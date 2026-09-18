package wagering

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// Wallet is a player's balance in a single currency, and the root of the
// financial aggregate.
//
// It is the only thing that may change its own balance. Every change goes
// through [Wallet.Debit] or [Wallet.Credit], which return the ledger entry that
// records it — so a balance change without a ledger entry is not something the
// domain declines to do, it is something it cannot do.
//
// The wallet keeps three invariants:
//
//   - the balance never goes below zero;
//   - every movement is in the wallet's own currency;
//   - the version starts at 1 and advances only when the balance changes.
//
// The wallet has no separate currency field. Its currency is the one its
// balance is denominated in, which a zero balance carries just as well as a
// positive one. A second field would be a copy that can drift from the value it
// claims to describe.
type Wallet struct {
	id        WalletID
	playerID  PlayerID
	balance   money.Money
	version   uint64
	createdAt time.Time
	updatedAt time.Time
}

// WalletKey is what makes a wallet unique: one wallet per player per currency.
//
// A player may hold wallets in several currencies, so it is the pair that is
// unique, never the player alone.
//
// The domain cannot look a wallet up, so [OpenWallet] is given whatever already
// exists for the key as a value and decides from that — the same way a reversal
// is given its [ReferenceView] rather than fetching one. A check against a value
// cannot win a race on its own, so the layer that stores wallets still needs a
// unique constraint on the pair; what this buys is that the rule, its failure
// code and its tests all live with the model rather than being reinvented by
// each caller.
type WalletKey struct {
	PlayerID PlayerID
	Currency money.Currency
}

// OpenWalletInput carries everything needed to create a wallet.
//
// TransactionID and LedgerEntryID record the opening. They are required exactly
// when InitialBalance is positive, because that is exactly when an opening
// exists to record.
type OpenWalletInput struct {
	WalletID       WalletID
	PlayerID       PlayerID
	InitialBalance money.Money
	TransactionID  TransactionID
	LedgerEntryID  LedgerEntryID
}

// OpenWallet creates a wallet, recording its starting balance if it has one.
//
// A wallet opened with money in it produces an [Opening] transaction in
// [Processed], the credit entry that records it, and both
// [WagerTransactionProcessed] and [WalletBalanceChanged]. Its version is 1: the
// opening is part of creating the wallet, not a change to one that already
// existed.
//
// A wallet opened at zero produces no opening, no entry and no events. It still
// has a version of 1 and a currency, taken from the zero balance it was given.
// The returned [Outcome] is therefore the zero one, and it is the only outcome
// in the domain that carries no transaction alongside a nil error: test it with
// [Outcome.Recorded] rather than dereferencing Transaction.
//
// The existing argument is whatever the caller already holds for this
// [WalletKey], and nil when there is none. A non-nil one means the pair is taken
// and the request is refused with [failure.WalletAlreadyExists].
func OpenWallet(in OpenWalletInput, existing *Wallet, now time.Time) (*Wallet, Outcome, error) {
	switch {
	case in.WalletID.IsZero():
		return nil, Outcome{}, missing("walletId")
	case in.PlayerID == "":
		return nil, Outcome{}, missing("playerId")
	case in.InitialBalance.IsUninitialized():
		return nil, Outcome{}, uninitialized("initialBalance")
	case in.InitialBalance.IsNegative():
		return nil, Outcome{}, failure.New(failure.InvalidAmountForKind,
			"a wallet cannot open with a negative balance, got %s", in.InitialBalance).WithField("initialBalance")
	case now.IsZero():
		return nil, Outcome{}, missing("now")
	}
	if _, err := NewPlayerID(string(in.PlayerID)); err != nil {
		return nil, Outcome{}, err
	}

	key := WalletKey{PlayerID: in.PlayerID, Currency: in.InitialBalance.Currency()}
	if existing != nil {
		// The caller is meant to hand over what it found for this key. A wallet
		// under some other key says the lookup and the request disagree, which
		// is a caller defect rather than a conflict to report to anyone.
		if existing.Key() != key {
			return nil, Outcome{}, failure.New(failure.ReferenceMismatch,
				"the existing wallet is %q in %s, not %q in %s",
				existing.PlayerID(), existing.Currency(), key.PlayerID, key.Currency).WithField("existing")
		}
		return nil, Outcome{}, failure.New(failure.WalletAlreadyExists,
			"%q already holds a wallet in %s", key.PlayerID, key.Currency)
	}

	zero, err := money.Zero(in.InitialBalance.Currency())
	if err != nil {
		return nil, Outcome{}, err
	}
	w := &Wallet{
		id:        in.WalletID,
		playerID:  in.PlayerID,
		balance:   zero,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}

	if in.InitialBalance.IsZero() {
		if !in.TransactionID.IsZero() || !in.LedgerEntryID.IsZero() {
			return nil, Outcome{}, failure.New(failure.ReferenceNotApplicable,
				"a wallet opened at zero records no opening").WithField("transactionId")
		}
		return w, Outcome{}, nil
	}

	switch {
	case in.TransactionID.IsZero():
		return nil, Outcome{}, failure.New(failure.MissingRequiredField,
			"a wallet opened with a balance records an opening").WithField("transactionId")
	case in.LedgerEntryID.IsZero():
		return nil, Outcome{}, failure.New(failure.MissingRequiredField,
			"a wallet opened with a balance records a ledger entry").WithField("ledgerEntryId")
	}

	tx, err := newOpeningTransaction(in, now)
	if err != nil {
		return nil, Outcome{}, err
	}

	// The opening credit is part of creating the wallet rather than a change to
	// one that already existed, so it must leave the version at 1 and its entry
	// must record version 1. The wallet is therefore credited from version 0,
	// which lands both on 1 — rather than applying the credit by hand, so that
	// this entry still comes from the one code path able to produce one.
	w.version = 0
	entry, err := w.Credit(Movement{
		EntryID:       in.LedgerEntryID,
		TransactionID: in.TransactionID,
		Amount:        in.InitialBalance,
		At:            now,
	})
	if err != nil {
		return nil, Outcome{}, err
	}

	processed, err := NewWagerTransactionProcessed(tx)
	if err != nil {
		return nil, Outcome{}, err
	}
	changed, err := NewWalletBalanceChanged(entry)
	if err != nil {
		return nil, Outcome{}, err
	}
	return w, Outcome{
		Transaction: tx,
		LedgerEntry: &entry,
		Events:      []Event{processed, changed},
	}, nil
}

// WalletSnapshot is the stored shape of a wallet.
type WalletSnapshot struct {
	ID        WalletID
	PlayerID  PlayerID
	Balance   money.Money
	Version   uint64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RehydrateWallet rebuilds a wallet from storage.
//
// It validates structure only: identifiers, a usable balance, a version that
// has at least been opened, and timestamps that agree with each other. It
// applies no movement, runs no transition and emits no event — there is no code
// path here that could.
func RehydrateWallet(s WalletSnapshot) (*Wallet, error) {
	switch {
	case s.ID.IsZero():
		return nil, missing("id")
	case s.Balance.IsUninitialized():
		return nil, uninitialized("balance")
	case s.Balance.IsNegative():
		return nil, failure.New(failure.InsufficientFunds,
			"a stored balance must not be negative, got %s", s.Balance).WithField("balance")
	case s.Version == 0:
		return nil, failure.New(failure.InvalidFieldFormat, "must be at least 1").WithField("version")
	case s.CreatedAt.IsZero():
		return nil, missing("createdAt")
	case s.UpdatedAt.IsZero():
		return nil, missing("updatedAt")
	case s.UpdatedAt.Before(s.CreatedAt):
		return nil, failure.New(failure.InvalidFieldFormat, "must not precede createdAt").WithField("updatedAt")
	}
	if _, err := NewPlayerID(string(s.PlayerID)); err != nil {
		return nil, err
	}
	return &Wallet{
		id:        s.ID,
		playerID:  s.PlayerID,
		balance:   s.Balance,
		version:   s.Version,
		createdAt: s.CreatedAt,
		updatedAt: s.UpdatedAt,
	}, nil
}

// ID returns the wallet's identifier.
func (w *Wallet) ID() WalletID { return w.id }

// PlayerID returns the player who holds the wallet.
func (w *Wallet) PlayerID() PlayerID { return w.playerID }

// Balance returns what the wallet currently holds.
func (w *Wallet) Balance() money.Money { return w.balance }

// Currency returns the currency the wallet is denominated in.
func (w *Wallet) Currency() money.Currency { return w.balance.Currency() }

// Version returns how many times the balance has stood, starting at 1.
func (w *Wallet) Version() uint64 { return w.version }

// CreatedAt returns when the wallet was opened.
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }

// UpdatedAt returns when the balance last changed.
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

// Key returns the pair that identifies the wallet uniquely.
func (w *Wallet) Key() WalletKey {
	return WalletKey{PlayerID: w.playerID, Currency: w.Currency()}
}

// Movement is one balance change a wallet is asked to record.
//
// The identifiers are named rather than positional because EntryID and
// TransactionID share an underlying type: passed in order they can be
// transposed without the compiler noticing, and for a ledger that is the worst
// kind of silent error.
type Movement struct {
	EntryID       LedgerEntryID
	TransactionID TransactionID
	Amount        money.Money
	At            time.Time
}

// Debit takes money out of the wallet and returns the entry recording it.
//
// It refuses any debit that would take the balance below zero, reporting
// [failure.InsufficientFunds]. A caller that needs to distinguish a player
// betting beyond their balance from a reversal that can no longer be applied
// translates that code itself; the wallet's concern is only the invariant.
func (w *Wallet) Debit(m Movement) (WalletLedgerEntry, error) { return w.move(Debit, m) }

// Credit puts money into the wallet and returns the entry recording it.
func (w *Wallet) Credit(m Movement) (WalletLedgerEntry, error) { return w.move(Credit, m) }

// move applies a balance change and records it.
//
// The entry is built before the wallet is touched, so a change that cannot be
// recorded does not happen: there is no path by which the balance moves and the
// ledger does not.
func (w *Wallet) move(d Direction, m Movement) (WalletLedgerEntry, error) {
	amount, now := m.Amount, m.At
	switch {
	case m.EntryID.IsZero():
		return WalletLedgerEntry{}, missing("ledgerEntryId")
	case m.TransactionID.IsZero():
		return WalletLedgerEntry{}, missing("transactionId")
	case amount.IsUninitialized():
		return WalletLedgerEntry{}, uninitialized("money")
	case !amount.IsPositive():
		return WalletLedgerEntry{}, failure.New(failure.InvalidAmountForKind,
			"a movement must be positive, got %s", amount).WithField("money")
	case now.IsZero():
		return WalletLedgerEntry{}, missing("now")
	}
	if amount.Currency() != w.Currency() {
		return WalletLedgerEntry{}, failure.New(failure.CurrencyMismatch,
			"wallet holds %s but the movement is in %s", w.Currency(), amount.Currency()).WithField("money")
	}

	before := w.balance
	after, err := applyDirection(d, before, amount)
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	if after.IsNegative() {
		return WalletLedgerEntry{}, failure.New(failure.InsufficientFunds,
			"balance %s cannot cover a %s of %s", before, d, amount)
	}

	entry, err := NewWalletLedgerEntry(LedgerEntryInput{
		ID:            m.EntryID,
		WalletID:      w.id,
		TransactionID: m.TransactionID,
		Direction:     d,
		Amount:        amount,
		BalanceBefore: before,
		BalanceAfter:  after,
		WalletVersion: w.version + 1,
		CreatedAt:     now,
	})
	if err != nil {
		return WalletLedgerEntry{}, err
	}

	w.balance = after
	w.version++
	w.updatedAt = now
	return entry, nil
}
