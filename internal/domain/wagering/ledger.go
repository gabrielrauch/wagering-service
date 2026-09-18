package wagering

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// WalletLedgerEntry is the immutable record of one balance change.
//
// An entry is never edited or deleted. A financial correction is made by adding
// further entries, which is what makes the ledger an append-only source of
// truth: the stored balance of a wallet must always equal the sum of its
// credits less its debits, including the opening.
//
// Losses and rejected operations write no entry, because no money moved.
//
// The entry carries the wallet version its change produced. The brief's field
// list does not include it, but without it a ledger has no ordering — two
// entries can share a creation timestamp, and a wallet's version is not
// recoverable from a count of entries, since a wallet opened at zero has a
// version but no opening entry. With it, (walletId, walletVersion) orders the
// ledger exactly and reconciliation can follow the chain as well as sum it.
type WalletLedgerEntry struct {
	id            LedgerEntryID
	walletID      WalletID
	transactionID TransactionID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion uint64
	createdAt     time.Time
}

// LedgerEntryInput carries everything needed to record a balance change.
type LedgerEntryInput struct {
	ID            LedgerEntryID
	WalletID      WalletID
	TransactionID TransactionID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion uint64
	CreatedAt     time.Time
}

// NewWalletLedgerEntry records a balance change, refusing any entry whose
// arithmetic does not hold.
//
// The entry must satisfy balanceAfter = balanceBefore ± amount according to the
// direction. That check is the reason this constructor exists: it makes an
// entry that misreports a movement unrepresentable rather than merely unlikely.
func NewWalletLedgerEntry(in LedgerEntryInput) (WalletLedgerEntry, error) {
	switch {
	case in.ID.IsZero():
		return WalletLedgerEntry{}, missing("id")
	case in.WalletID.IsZero():
		return WalletLedgerEntry{}, missing("walletId")
	case in.TransactionID.IsZero():
		return WalletLedgerEntry{}, missing("transactionId")
	case in.WalletVersion == 0:
		return WalletLedgerEntry{}, failure.New(failure.InvalidFieldFormat, "must be at least 1").WithField("walletVersion")
	case in.CreatedAt.IsZero():
		return WalletLedgerEntry{}, missing("createdAt")
	}
	if !in.Direction.Known() {
		return WalletLedgerEntry{}, unknownDirection(in.Direction)
	}

	// Checked in a fixed order rather than by ranging a map, so an entry with
	// more than one missing amount always names the same field.
	switch {
	case in.Amount.IsUninitialized():
		return WalletLedgerEntry{}, uninitialized("money")
	case in.BalanceBefore.IsUninitialized():
		return WalletLedgerEntry{}, uninitialized("balanceBefore")
	case in.BalanceAfter.IsUninitialized():
		return WalletLedgerEntry{}, uninitialized("balanceAfter")
	}

	// An entry exists because money moved; a zero-amount entry would be a
	// movement that did not happen.
	if !in.Amount.IsPositive() {
		return WalletLedgerEntry{}, failure.New(failure.InvalidAmountForKind,
			"a ledger entry must move a positive amount, got %s", in.Amount).WithField("money")
	}
	// Balances are what the wallet held, and a wallet never holds less than
	// nothing.
	if in.BalanceBefore.IsNegative() || in.BalanceAfter.IsNegative() {
		return WalletLedgerEntry{}, failure.New(failure.InsufficientFunds,
			"balances must not be negative, got %s then %s", in.BalanceBefore, in.BalanceAfter)
	}

	expected, err := applyDirection(in.Direction, in.BalanceBefore, in.Amount)
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	if !expected.Equal(in.BalanceAfter) {
		return WalletLedgerEntry{}, failure.New(failure.InvalidFieldFormat,
			"%s of %s against %s gives %s, but balanceAfter is %s",
			in.Direction, in.Amount, in.BalanceBefore, expected, in.BalanceAfter).WithField("balanceAfter")
	}

	return WalletLedgerEntry{
		id:            in.ID,
		walletID:      in.WalletID,
		transactionID: in.TransactionID,
		direction:     in.Direction,
		amount:        in.Amount,
		balanceBefore: in.BalanceBefore,
		balanceAfter:  in.BalanceAfter,
		walletVersion: in.WalletVersion,
		createdAt:     in.CreatedAt,
	}, nil
}

// RehydrateWalletLedgerEntry rebuilds an entry read from storage.
//
// It applies exactly the same validation as [NewWalletLedgerEntry], because an
// entry is immutable: there is no distinction between a valid new entry and a
// valid stored one, and a stored entry whose arithmetic no longer holds is
// corruption that must be reported rather than loaded.
func RehydrateWalletLedgerEntry(in LedgerEntryInput) (WalletLedgerEntry, error) {
	return NewWalletLedgerEntry(in)
}

// ID returns the entry's identifier.
func (e WalletLedgerEntry) ID() LedgerEntryID { return e.id }

// WalletID returns the wallet whose balance changed.
func (e WalletLedgerEntry) WalletID() WalletID { return e.walletID }

// TransactionID returns the transaction that caused the change.
func (e WalletLedgerEntry) TransactionID() TransactionID { return e.transactionID }

// Direction returns which way the money moved.
func (e WalletLedgerEntry) Direction() Direction { return e.direction }

// Amount returns how much moved.
func (e WalletLedgerEntry) Amount() money.Money { return e.amount }

// BalanceBefore returns the balance the wallet held before the change.
func (e WalletLedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter returns the balance the wallet held after the change.
func (e WalletLedgerEntry) BalanceAfter() money.Money { return e.balanceAfter }

// WalletVersion returns the wallet version this change produced.
func (e WalletLedgerEntry) WalletVersion() uint64 { return e.walletVersion }

// CreatedAt returns when the entry was recorded.
func (e WalletLedgerEntry) CreatedAt() time.Time { return e.createdAt }

// IsZero reports whether the entry was never constructed.
func (e WalletLedgerEntry) IsZero() bool { return e.id.IsZero() }
