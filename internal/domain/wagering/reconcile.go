package wagering

import (
	"fmt"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// ReconciliationError reports a wallet whose stored balance disagrees with its
// ledger.
//
// It is a finding, not a correction. Nothing in this package will adjust a
// balance to make it agree: a disagreement means either the ledger or the
// balance is wrong, and which one is a question for an operator.
//
// It reports under [failure.LedgerBalanceMismatch], reachable the same two ways
// as every other refusal the domain raises — [failure.Is] and errors.Is match
// on the code, errors.As finds the [failure.Error] — while errors.As on
// *ReconciliationError still yields the two balances that disagree. Carrying a
// code matters because [Reconcile] returns four other findings that all have
// one: without it, the single finding the function exists to produce would be
// the only one a caller classifying by code had to special-case.
//
// Its fields are unexported for the same reason every other type here keeps
// theirs that way. A finding asserts that a wallet's books do not balance, which
// is the sort of claim that starts an investigation; were the fields exported,
// any package could write a fully populated one naming any wallet and any two
// amounts, and nothing about the value would distinguish it from a finding this
// package actually made. [NewReconciliationError] is the only way to populate
// one, so the worst an outside package can now produce is the empty value, which
// says so when read.
type ReconciliationError struct {
	walletID WalletID
	expected money.Money
	actual   money.Money
}

// NewReconciliationError records a disagreement between a wallet's balance and
// its ledger.
//
// It refuses a finding that does not describe one: an unnamed wallet, a balance
// that was never constructed, two amounts in different currencies, or — the
// point of the check — two amounts that agree, which would assert a
// disagreement that is not there.
func NewReconciliationError(walletID WalletID, expected, actual money.Money) (*ReconciliationError, error) {
	if walletID.IsZero() {
		return nil, missing("walletId")
	}
	switch {
	case expected.IsUninitialized():
		return nil, uninitialized("expected")
	case actual.IsUninitialized():
		return nil, uninitialized("actual")
	}
	if expected.Currency() != actual.Currency() {
		return nil, failure.New(failure.CurrencyMismatch,
			"the ledger sums to %s but the wallet holds %s, which are not comparable",
			expected.Currency(), actual.Currency()).WithField("expected")
	}
	if expected.Equal(actual) {
		return nil, failure.New(failure.InvalidFieldFormat,
			"%s and %s agree, so there is no disagreement to report",
			expected, actual).WithField("expected")
	}
	return &ReconciliationError{walletID: walletID, expected: expected, actual: actual}, nil
}

// WalletID returns the wallet whose books do not balance.
func (e *ReconciliationError) WalletID() WalletID { return e.walletID }

// Expected returns the balance the ledger implies.
func (e *ReconciliationError) Expected() money.Money { return e.expected }

// Actual returns the balance the wallet holds.
func (e *ReconciliationError) Actual() money.Money { return e.actual }

// Error implements the error interface, naming the code the way every other
// domain error does.
func (e *ReconciliationError) Error() string {
	return fmt.Sprintf("%s: %s", failure.LedgerBalanceMismatch, e.detail())
}

// Unwrap exposes the finding as a coded [failure.Error], which is what puts it
// within reach of errors.Is, errors.As and [failure.CodeOf].
//
// The code is derived on demand rather than stored in a field, so that it is a
// property of the type rather than of how a particular value was built.
func (e *ReconciliationError) Unwrap() error {
	return failure.New(failure.LedgerBalanceMismatch, "%s", e.detail())
}

// detail is the human half of the message, shared so the error string and the
// code it unwraps to cannot drift apart.
//
// The empty value reads as what it is. It is the one value [NewReconciliationError]
// cannot stop another package from writing, and an error that renders as a
// disagreement between two blanks would be read as a real finding with missing
// data rather than as no finding at all.
func (e *ReconciliationError) detail() string {
	if e.walletID.IsZero() {
		return "a reconciliation finding that was never constructed"
	}
	return fmt.Sprintf("wallet %s holds %s but its ledger sums to %s", e.walletID, e.actual, e.expected)
}

// LedgerBalance sums a set of ledger entries: credits less debits.
//
// The sum is order-independent, which is why the ledger needs no ordering to be
// reconcilable. The currency is passed in rather than taken from the first
// entry, so an empty ledger correctly sums to zero in a known currency instead
// of having nothing to infer from.
func LedgerBalance(c money.Currency, entries []WalletLedgerEntry) (money.Money, error) {
	total, err := money.Zero(c)
	if err != nil {
		return money.Money{}, err
	}
	for _, entry := range entries {
		if entry.IsZero() {
			return money.Money{}, failure.New(failure.UninitializedValue,
				"ledger contains an entry that was never constructed")
		}
		if entry.Amount().Currency() != c {
			return money.Money{}, failure.New(failure.CurrencyMismatch,
				"entry %s is in %s, expected %s", entry.ID(), entry.Amount().Currency(), c)
		}
		total, err = applyDirection(entry.Direction(), total, entry.Amount())
		if err != nil {
			return money.Money{}, err
		}
	}
	return total, nil
}

// Reconcile checks a wallet's stored balance against its ledger.
//
// It verifies that every entry belongs to this wallet, that no transaction
// appears twice, that every entry is in the wallet's currency, and that credits
// less debits equal what the wallet holds — including the opening, when there
// was one.
//
// Entries may be passed in any order. Reconcile never writes: it reports a
// [ReconciliationError] and leaves the wallet exactly as it found it.
//
// The uniqueness of an entry per transaction is guaranteed upstream by
// construction — [Processor.Submit] returns at most one entry per operation,
// and a wallet has no other way to move — so finding a duplicate here means a
// defect somewhere else, which is why it is worth checking rather than assuming.
func Reconcile(w *Wallet, entries []WalletLedgerEntry) error {
	if w == nil {
		return failure.New(failure.UninitializedValue, "wallet must be present").WithField("wallet")
	}

	seen := make(map[TransactionID]LedgerEntryID, len(entries))
	for _, entry := range entries {
		if entry.IsZero() {
			return failure.New(failure.UninitializedValue,
				"ledger contains an entry that was never constructed")
		}
		if entry.WalletID() != w.ID() {
			return failure.New(failure.ReferenceMismatch,
				"entry %s belongs to wallet %s, not %s", entry.ID(), entry.WalletID(), w.ID())
		}
		if previous, duplicate := seen[entry.TransactionID()]; duplicate {
			return failure.New(failure.InvalidFieldFormat,
				"transaction %s has two ledger entries, %s and %s",
				entry.TransactionID(), previous, entry.ID()).WithField("transactionId")
		}
		seen[entry.TransactionID()] = entry.ID()
	}

	expected, err := LedgerBalance(w.Currency(), entries)
	if err != nil {
		return err
	}
	if !expected.Equal(w.Balance()) {
		mismatch, err := NewReconciliationError(w.ID(), expected, w.Balance())
		if err != nil {
			return err
		}
		return mismatch
	}
	return nil
}
