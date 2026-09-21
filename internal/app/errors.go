package app

import (
	"errors"
	"fmt"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// Class is how a caller should answer. Every error leaving this package carries
// one, so an HTTP handler and an SQS consumer can map the same failure to their
// own vocabularies without either of them re-deriving what happened.
type Class string

const (
	// Invalid is a malformed submission. Nothing was persisted and the same
	// idempotency key is still free, so a corrected payload may be resubmitted
	// under it.
	//
	// A correctable code found while judging a submission classifies here. The
	// same code found while reading stored state does not — see [Audit].
	Invalid Class = "INVALID"
	// Rejected is a business rule settling an operation. A row was persisted, an
	// event was emitted and the idempotency key is bound to that payload for
	// good. It never arrives on an error — see below.
	Rejected Class = "REJECTED"
	// Conflict is a submission refused because it contradicts one already
	// recorded. Nothing new is persisted. It carries a code when the catalogue
	// has one for it, and none when it does not.
	Conflict Class = "CONFLICT"
	// Audit is a finding about stored state rather than about a submission.
	// Nothing was in flight for it to settle, and an operator, not a provider,
	// is the audience.
	//
	// Every failure.Code.Audit code classifies here, but the converse does not
	// hold and must not be assumed: what makes a finding an audit finding is
	// where it was found, not which code it carries. Reconciliation reads
	// stored rows and can find a duplicated ledger entry or an entry in the
	// wrong currency, which carry correctable codes because the same codes
	// describe a malformed submission elsewhere. They are still Audit — see
	// auditFinding, which sets the class explicitly for exactly that reason.
	Audit Class = "AUDIT"
	// Unauthorized is the caller not being permitted to do this at all. No
	// financial effect, no data.
	Unauthorized Class = "UNAUTHORIZED"
	// NotFound is the resource not existing, or not existing as far as this
	// caller is concerned.
	NotFound Class = "NOT_FOUND"
	// Retryable is an infrastructure failure that recorded nothing and may
	// succeed if sent again: a lost connection, a timeout, a serialization or
	// lock conflict.
	Retryable Class = "RETRYABLE"
	// Unretryable is everything else. Sending it again will fail the same way.
	Unretryable Class = "UNRETRYABLE"
)

// Error is the classified failure this layer reports.
//
// Class is what the caller acts on. Code is present when the failure names a
// catalogued reason, and absent when it does not — not every refusal has one,
// and inventing a code for those would put entries in the external contract that
// describe nothing a provider can act on.
type Error struct {
	// Class is how the caller should answer.
	Class Class
	// Code is the catalogued reason, when there is one.
	Code failure.Code
	// Field names the offending input, in the external contract's spelling.
	Field string

	msg     string
	wrapped error
}

// Error implements the error interface.
//
// A nil receiver renders rather than panicking. Error is exported with exported
// fields, so an adapter can declare a *Error, leave it nil and return it as a
// non-nil error — the nil interface trap — and a classifier that crashed on one
// would turn a caller's mistake into an outage on the path whose whole job is to
// report failures calmly.
func (e *Error) Error() string {
	if e == nil {
		return "<nil app.Error>"
	}
	head := string(e.Class)
	if e.Code != "" {
		head += " " + string(e.Code)
	}
	if e.Field != "" {
		head += " [" + e.Field + "]"
	}
	if e.msg == "" {
		return head
	}
	return head + ": " + e.msg
}

// Unwrap exposes the cause, which is what keeps errors.Is and failure.Is working
// through a classified error: an app.Error built over a domain refusal still
// matches failure.Is for the code the domain raised.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.wrapped
}

// ClassOf reports how a caller should answer err.
//
// The order is the whole error model, and each step is there because the step
// after it would answer wrongly:
//
//  1. An app.Error answers for itself. It was classified where the context to do
//     so existed, and re-deriving it here from a code would discard that.
//  2. An audit code is Audit. It has to be tested before "definitive", because
//     every audit code is definitive and the definitive branch would swallow it.
//  3. A correctable code is Invalid.
//  4. WalletAlreadyExists and IdempotencyPayloadConflict are Conflict. Both are
//     definitive, and both mean "something already recorded says otherwise",
//     which is not the same answer as the branch below.
//  5. Any remaining definitive code is Unretryable.
//  6. Anything carrying no code at all is Unretryable. An unclassified error is
//     never an invitation to retry, for the same reason failure.Correctable says
//     no to one: a failure nobody recognised is not one anybody has established
//     is safe to repeat.
//
// Rejected is not in that list, and its absence is the point. A rejection is an
// outcome, never an error: it exists only when the processor returned a nil error
// and the transaction it carries reached wagering.Rejected. A definitive code
// arriving on an error path means the operation never became anything — and
// LEDGER_BALANCE_MISMATCH is the proof, because it is definitive, is exactly what
// Reconcile produces, and could never settle a transaction.
func ClassOf(err error) Class {
	if err == nil {
		return ""
	}
	if e, ok := errors.AsType[*Error](err); ok && e != nil {
		return e.Class
	}
	code, ok := failure.CodeOf(err)
	if !ok {
		return Unretryable
	}
	switch {
	case code.Audit():
		return Audit
	case code.Correctable():
		return Invalid
	case code == failure.WalletAlreadyExists, code == failure.IdempotencyPayloadConflict:
		return Conflict
	default:
		return Unretryable
	}
}

// Is reports whether err classifies as class.
func Is(err error, class Class) bool { return ClassOf(err) == class }

// CodeOf returns the catalogued reason err carries, looking through the
// classification to the domain refusal underneath.
func CodeOf(err error) (failure.Code, bool) {
	if e, ok := errors.AsType[*Error](err); ok && e != nil && e.Code != "" {
		return e.Code, true
	}
	return failure.CodeOf(err)
}

// AsRetryable marks err as safe to send again: it recorded nothing and the
// condition that caused it is expected to pass. Adapters call this; nothing in
// this package can tell a dropped connection from a dead one.
//
// An error that is already classified is returned untouched, for the same reason
// [classify] leaves one alone: the first layer that knew what the failure meant
// is the one that should have said so. Without that guard an adapter wrapping
// everything it returns would relabel a malformed submission as retryable
// infrastructure — Class would say "send it again" while [CodeOf] still returned
// the correctable code saying "repair the payload first", and the two answers
// cannot both be acted on.
//
// The test is whether err IS classified, not whether anything in its chain is.
// An adapter calling this is asserting something about the error in its hand,
// and an infrastructure failure that happens to wrap a classified one is still
// an infrastructure failure it is entitled to classify. Only an error that
// already answers for itself is left to.
func AsRetryable(err error) error {
	if err == nil || isClassified(err) {
		return err
	}
	return &Error{Class: Retryable, msg: err.Error(), wrapped: err}
}

// AsUnretryable marks err as failing the same way every time. As with
// [AsRetryable], an error that is already classified is returned untouched.
func AsUnretryable(err error) error {
	if err == nil || isClassified(err) {
		return err
	}
	return &Error{Class: Unretryable, msg: err.Error(), wrapped: err}
}

// isClassified reports whether err is itself an [Error], rather than merely
// carrying one somewhere in its chain.
//
// The comparison is on the rendered form, which is the same test [describe]
// makes and for the same reason: a wrapper that added context says so in its
// message, and one that added none has nothing of its own to lose. Comparing
// the error values with == would read more directly and is exactly what this
// package's lint gate forbids, because that comparison is wrong everywhere else
// an error is examined here.
func isClassified(err error) bool {
	e, ok := errors.AsType[*Error](err)
	return ok && e != nil && e.Error() == err.Error()
}

// classify turns a domain refusal into a classified error, leaving anything
// already classified alone.
//
// Preserving an existing classification matters on the resume path, where an
// error crosses several layers before it is reported: the first layer that knew
// what the failure meant is the one that should have said so.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*Error](err); ok && e != nil {
		return err
	}
	code, field, msg := describe(err)
	return &Error{Class: ClassOf(err), Code: code, Field: field, msg: msg, wrapped: err}
}

// auditFinding classifies a finding about stored state.
//
// The class is Audit whatever code the finding carries, and that is the whole
// point of the constructor: an audit finding is about stored state rather than
// about a submission, so a correctable code on one must not be read as a
// malformed payload a provider could repair.
//
// The code and the field, in contrast, are the finding's own. Stamping every
// finding with one catalogue entry would name a failure that did not happen —
// an operator paged for a balance that is out goes looking for missing money,
// which is the wrong search when what was actually found is a duplicated ledger
// entry.
func auditFinding(finding error) *Error {
	code, field, msg := describe(finding)
	return &Error{Class: Audit, Code: code, Field: field, msg: msg, wrapped: finding}
}

// describe reads what a cause can tell a classified error about itself: the
// catalogued code, the field at fault, and the words to print after the head.
//
// The message is the refusal's own, not its rendered form. [Error.Error] already
// states the code and the field in the head it prints, so copying the cause's
// rendering would print both of them twice — "INSUFFICIENT_FUNDS [amount]:
// INSUFFICIENT_FUNDS: amount: ...". The cause stays reachable through Unwrap
// either way, so nothing is lost by not restating it.
//
// The rendering is only trimmed when the failure IS the cause rather than
// something in its chain. A refusal wrapped in context has that context in its
// rendered form and nowhere else, and dropping to the innermost message would
// discard it.
func describe(cause error) (code failure.Code, field, msg string) {
	msg = cause.Error()
	ferr, ok := errors.AsType[*failure.Error](cause)
	if !ok {
		return "", "", msg
	}
	if ferr.Error() == msg {
		msg = ferr.Message()
	}
	return ferr.Code, ferr.Field, msg
}

func newError(class Class, code failure.Code, wrapped error, format string, a ...any) *Error {
	return &Error{Class: class, Code: code, msg: fmt.Sprintf(format, a...), wrapped: wrapped}
}

func invalidField(field string, code failure.Code, format string, a ...any) *Error {
	e := newError(Invalid, code, nil, format, a...)
	e.Field = field
	return e
}

func unauthorized(format string, a ...any) *Error {
	return newError(Unauthorized, "", nil, format, a...)
}

func notFound(format string, a ...any) *Error {
	return newError(NotFound, "", nil, format, a...)
}

// ErrForeignOperation marks a read refused because the operation belongs to
// another provider.
//
// It never changes what the caller is told. The class, the code and the message
// are those of an ordinary miss, deliberately and unavoidably: anything else
// would confirm the operation exists, and an existence oracle is what scoping a
// read is meant to deny.
//
// It rides in the error chain instead, where [errors.Is] finds it and a rendered
// message does not show it, so that the two cases an operator needs to tell
// apart — a provider walking another provider's identifiers, and a caller asking
// for something that is simply not there — are distinguishable on the inside
// while staying identical on the outside.
//
// # What an adapter must not do with it
//
// Everything above holds only while the distinction stays inside. An adapter
// that renders an error chain to the caller — %+v, a cause list in a JSON body,
// an unwrapped detail field — publishes exactly the fact this sentinel exists to
// keep private, and turns a scoped read back into the existence oracle it was
// built to deny. Match it, count it, log it; never answer with it.
var ErrForeignOperation = errors.New("app: operation belongs to another provider")

// notFoundWrapping is notFound carrying a cause the caller cannot see. The
// rendered message is the one notFound would have produced, because Error
// prints the head and the message and never the cause.
func notFoundWrapping(wrapped error, format string, a ...any) *Error {
	return newError(NotFound, "", wrapped, format, a...)
}

func conflict(code failure.Code, wrapped error, format string, a ...any) *Error {
	return newError(Conflict, code, wrapped, format, a...)
}

// defect reports a condition this layer believes it has already made
// unreachable. It is Unretryable because repeating it cannot help: the fault is
// in the code, not in the world.
func defect(format string, a ...any) *Error {
	return newError(Unretryable, "", nil, format, a...)
}
