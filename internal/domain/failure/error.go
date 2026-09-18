package failure

import (
	"errors"
	"fmt"
)

// Error is the error every domain refusal is reported through. It always
// carries a [Code], optionally names the field at fault, and may wrap a
// lower-level cause.
//
// Two errors are considered equal by [errors.Is] when they carry the same code,
// so callers may write any of:
//
//	failure.Is(err, failure.InsufficientFunds)
//
//	if ferr, ok := errors.AsType[*failure.Error](err); ok { ... ferr.Code ... }
//
//	errors.Is(err, failure.New(failure.InsufficientFunds, ""))
//
// A refusal is never reported by panicking. Panics in this domain would signal
// a defect in the runtime, never a business outcome.
type Error struct {
	// Code is the stable reason for the refusal.
	Code Code
	// Field names the offending input when one is identifiable, using the
	// external contract's spelling (for example "referenceExternalTransactionId").
	Field string

	msg     string
	wrapped error
}

// New builds an Error carrying code and a formatted message.
func New(code Code, format string, a ...any) *Error {
	return &Error{Code: code, msg: fmt.Sprintf(format, a...)}
}

// Wrap builds an Error carrying code and wrapping cause, so the underlying
// error stays reachable through [errors.Unwrap].
func Wrap(cause error, code Code, format string, a ...any) *Error {
	return &Error{Code: code, msg: fmt.Sprintf(format, a...), wrapped: cause}
}

// WithField returns a copy of e naming the offending field. The receiver is
// left untouched, so package-level errors can be annotated at the call site
// without being mutated.
func (e *Error) WithField(field string) *Error {
	if e == nil {
		return nil
	}
	clone := *e
	clone.Field = field
	return &clone
}

// Error implements the error interface.
func (e *Error) Error() string {
	switch {
	case e.Field != "" && e.msg != "":
		return fmt.Sprintf("%s: %s: %s", e.Code, e.Field, e.msg)
	case e.Field != "":
		return fmt.Sprintf("%s: %s", e.Code, e.Field)
	case e.msg != "":
		return fmt.Sprintf("%s: %s", e.Code, e.msg)
	default:
		return string(e.Code)
	}
}

// Unwrap returns the wrapped cause, if any.
func (e *Error) Unwrap() error { return e.wrapped }

// Is reports whether target is an Error carrying the same code, which is what
// makes errors.Is match on the reason rather than on identity.
func (e *Error) Is(target error) bool {
	t, ok := errors.AsType[*Error](target)
	if !ok {
		return false
	}
	return t.Code == e.Code
}

// Is reports whether err, or anything it wraps, is an [Error] carrying code.
func Is(err error, code Code) bool {
	got, ok := CodeOf(err)
	return ok && got == code
}

// CodeOf returns the code carried by err, or by the first [Error] it wraps.
func CodeOf(err error) (Code, bool) {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Code, true
	}
	return "", false
}

// Correctable reports whether err is a correctable refusal, meaning nothing was
// persisted and the same idempotency key may be reused. An error that is not an
// [Error] is not correctable: an unrecognised failure is never an invitation to
// retry.
func Correctable(err error) bool {
	code, ok := CodeOf(err)
	return ok && code.Correctable()
}
