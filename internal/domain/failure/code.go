// Package failure defines the stable catalogue of reasons a wagering
// operation can be refused, together with the error type that carries them.
//
// Every refusal in the domain names a [Code]. Codes are split along a single
// axis, reported by [Code.Correctable]:
//
//   - Correctable codes describe a malformed submission. No wager transaction
//     is persisted, so a provider may repair the payload and resubmit it under
//     the same idempotency key.
//   - Definitive codes describe a settled business outcome. A rejected wager
//     transaction is persisted and an event is emitted, which binds the
//     idempotency key to that payload permanently; a repaired resubmission
//     under the same key is then a conflict.
//
// That split is not cosmetic. It is exactly the "did we persist a payload hash
// under this key?" split, and it decides whether a provider can retry.
//
// A small number of codes report on stored state rather than on a submission,
// and [Code.Audit] marks them. They are definitive, since there is no payload
// to repair, but they cannot settle a wager transaction: no operation was in
// flight for them to settle. That second axis exists so the first one stays
// honest — without it, "definitive" would have to mean both "binds the
// idempotency key" and "does not", depending on the code.
//
// The package has no dependencies, so every layer of the domain can name a code
// without importing anything else.
package failure

import (
	"maps"
	"slices"
)

// Code is the stable, documented identifier for a refusal. Its string form is
// part of the external contract and must not change once published.
type Code string

// Correctable codes. The submission was malformed; nothing is persisted, and
// the same idempotency key remains free for a corrected resubmission.
const (
	// UninitializedValue reports a domain value that was never constructed
	// through its validating constructor, such as a zero-valued Money or a nil
	// identifier.
	UninitializedValue Code = "UNINITIALIZED_VALUE"

	// InvalidAmountFormat reports an amount that is not a plain non-negative
	// decimal: signs, scientific notation, NaN, Infinity, leading zeros,
	// whitespace, a missing integer or fraction part, or any non-ASCII digit.
	InvalidAmountFormat Code = "INVALID_AMOUNT_FORMAT"

	// InvalidAmountScale reports a well-formed decimal carrying the wrong
	// number of fraction digits, whether too few ("25", "25.0") or too many
	// ("25.000").
	InvalidAmountScale Code = "INVALID_AMOUNT_SCALE"

	// AmountOutOfRange reports an amount that cannot be represented, or an
	// arithmetic result that would overflow the minor-unit representation.
	AmountOutOfRange Code = "AMOUNT_OUT_OF_RANGE"

	// InvalidAmountForKind reports an amount that is well-formed but forbidden
	// for the transaction kind: zero for BET, WIN, REFUND or ROLLBACK, or
	// non-zero for LOSS.
	InvalidAmountForKind Code = "INVALID_AMOUNT_FOR_KIND"

	// UnsupportedCurrency reports a currency code that is not three uppercase
	// ASCII letters.
	UnsupportedCurrency Code = "UNSUPPORTED_CURRENCY"

	// MissingRequiredField reports a field the kind requires but that was
	// absent or empty.
	MissingRequiredField Code = "MISSING_REQUIRED_FIELD"

	// InvalidFieldFormat reports a field that is present but malformed, such as
	// surrounding whitespace, an over-long identifier or an unknown enum value.
	InvalidFieldFormat Code = "INVALID_FIELD_FORMAT"

	// UnsupportedTransactionKind reports an OPENING submitted as an external
	// operation. Openings are raised only by the system when a wallet is
	// created.
	UnsupportedTransactionKind Code = "UNSUPPORTED_TRANSACTION_KIND"

	// ReferenceRequired reports a REFUND or ROLLBACK submitted without the
	// reference it must reverse.
	ReferenceRequired Code = "REFERENCE_REQUIRED"

	// ReferenceNotApplicable reports a reference supplied on a kind that cannot
	// carry one.
	ReferenceNotApplicable Code = "REFERENCE_NOT_APPLICABLE"

	// InvalidStateTransition reports an attempt to move a wager transaction to
	// a status it cannot reach, including any transition out of a terminal
	// status. This is a caller defect rather than a provider defect, and is
	// reported as an error rather than a panic.
	InvalidStateTransition Code = "INVALID_STATE_TRANSITION"
)

// Definitive codes. A business rule settled the operation; a rejected wager
// transaction is persisted and the idempotency key is bound to its payload.
const (
	// InsufficientFunds reports a BET whose debit would take the wallet below
	// zero.
	InsufficientFunds Code = "INSUFFICIENT_FUNDS"

	// ReversalInsufficientFunds reports a reversal whose debit would take the
	// wallet below zero. It is deliberately distinct from InsufficientFunds:
	// a player betting beyond their balance is routine, whereas a reversal that
	// cannot be applied means money has already left the wallet and needs
	// operator attention.
	ReversalInsufficientFunds Code = "REVERSAL_INSUFFICIENT_FUNDS"

	// BalanceOutOfRange reports a movement that cannot be applied because the
	// resulting balance would not be representable.
	//
	// It is the ceiling to InsufficientFunds' floor: both are limits on what a
	// wallet can hold rather than faults in the submission, so both settle the
	// operation. It is deliberately distinct from AmountOutOfRange, which is
	// correctable and describes an amount that could be corrected and resent —
	// here the amount is fine and the balance is what it is, so the same payload
	// resubmitted would overflow again.
	BalanceOutOfRange Code = "BALANCE_OUT_OF_RANGE"

	// CurrencyMismatch reports money whose currency differs from the wallet it
	// would move, or arithmetic attempted across two currencies.
	CurrencyMismatch Code = "CURRENCY_MISMATCH"

	// ReferenceNotFound reports a reference that never arrived before the wait
	// budget was exhausted.
	ReferenceNotFound Code = "REFERENCE_NOT_FOUND"

	// ReferenceNotProcessed reports a reference that exists but ended
	// unsuccessfully, so there is nothing to reverse.
	ReferenceNotProcessed Code = "REFERENCE_NOT_PROCESSED"

	// ReferenceNotReversible reports a reference whose kind cannot be reversed
	// by the submitted kind.
	ReferenceNotReversible Code = "REFERENCE_NOT_REVERSIBLE"

	// ReferenceAlreadyReversed reports a reference that already carries an
	// active reversal, which would otherwise return the same money twice.
	ReferenceAlreadyReversed Code = "REFERENCE_ALREADY_REVERSED"

	// ReferenceMismatch reports a reference that disagrees with the operation
	// on provider, player, wallet, currency or round.
	ReferenceMismatch Code = "REFERENCE_MISMATCH"

	// ReversalAmountMismatch reports a reversal whose amount differs from the
	// reference it reverses. Partial reversals do not exist.
	ReversalAmountMismatch Code = "REVERSAL_AMOUNT_MISMATCH"

	// IdempotencyPayloadConflict reports an idempotency key reused for a
	// different set of business fields.
	IdempotencyPayloadConflict Code = "IDEMPOTENCY_PAYLOAD_CONFLICT"

	// WalletAlreadyExists reports a second wallet opened for a player and
	// currency that already has one.
	WalletAlreadyExists Code = "WALLET_ALREADY_EXISTS"
)

// Audit codes. These report corruption discovered in stored state rather than
// the outcome of a submission, and [Code.Audit] identifies them.
//
// They are classified definitive because nothing about them invites a retry:
// there is no payload to repair and no idempotency key in play. They are
// nonetheless kept out of a wager transaction's failure code, because the
// operation they describe is not one that was ever in flight.
const (
	// LedgerBalanceMismatch reports a wallet whose stored balance does not
	// equal its ledger summed — credits less debits, including the opening.
	// It is a finding for an operator rather than a refusal a provider can act
	// on: the wagering package reports it through ReconciliationError, which
	// also names the two balances that disagree.
	LedgerBalanceMismatch Code = "LEDGER_BALANCE_MISMATCH"
)

// correctable classifies every declared code. A code absent from this table is
// a programming error, caught by the totality test rather than defaulted, so
// that a new code cannot silently inherit a classification.
var correctable = map[Code]bool{
	UninitializedValue:         true,
	InvalidAmountFormat:        true,
	InvalidAmountScale:         true,
	AmountOutOfRange:           true,
	InvalidAmountForKind:       true,
	UnsupportedCurrency:        true,
	MissingRequiredField:       true,
	InvalidFieldFormat:         true,
	UnsupportedTransactionKind: true,
	ReferenceRequired:          true,
	ReferenceNotApplicable:     true,
	InvalidStateTransition:     true,

	InsufficientFunds:          false,
	ReversalInsufficientFunds:  false,
	BalanceOutOfRange:          false,
	CurrencyMismatch:           false,
	ReferenceNotFound:          false,
	ReferenceNotProcessed:      false,
	ReferenceNotReversible:     false,
	ReferenceAlreadyReversed:   false,
	ReferenceMismatch:          false,
	ReversalAmountMismatch:     false,
	IdempotencyPayloadConflict: false,
	WalletAlreadyExists:        false,

	LedgerBalanceMismatch: false,
}

// audit lists the codes that report on stored state rather than on a
// submission. Membership is explicit rather than inferred: a code is a refusal
// unless it is listed here, so a new one cannot become an audit finding by
// accident.
var audit = map[Code]bool{
	LedgerBalanceMismatch: true,
}

// All returns every declared code, in no particular order. It exists so tests
// can assert that the catalogue and its classification stay in step.
//
// The slice is sized from the table before it is filled. [slices.Collect] would
// have to grow it from nothing, since an iterator carries no count, and the
// count is sitting right there.
func All() []Code {
	return slices.AppendSeq(make([]Code, 0, len(correctable)), maps.Keys(correctable))
}

// Known reports whether c is a declared code.
func (c Code) Known() bool {
	_, ok := correctable[c]
	return ok
}

// Correctable reports whether a provider can repair the submission and resubmit
// it under the same idempotency key. It is false for undeclared codes, so an
// unrecognised code is treated as settled rather than as an invitation to
// retry.
func (c Code) Correctable() bool {
	return correctable[c]
}

// Definitive reports whether the code settled the operation, persisting a
// rejected wager transaction and binding the idempotency key to its payload.
func (c Code) Definitive() bool {
	correctableCode, declared := correctable[c]
	return declared && !correctableCode
}

// Audit reports whether the code describes corruption found in stored state
// rather than the outcome of a submission.
//
// An audit code is definitive — there is nothing to retry — but it never stands
// as a wager transaction's failure code, because no operation was in flight for
// it to settle. A caller deciding whether a code may settle a transaction needs
// both halves: definitive and not an audit code.
func (c Code) Audit() bool { return audit[c] }

// String returns the wire form of the code.
func (c Code) String() string { return string(c) }
