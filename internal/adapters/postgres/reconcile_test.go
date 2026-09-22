//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestReconciliationAgreesWithWhatThisSystemWrote is the baseline: a wallet
// moved only through the adapter's own door reconciles.
//
// It is worth having as a test rather than assuming, because the door writes
// the wallet's new state out of the ledger entry — so if that reading were
// wrong, every wallet in the system would diverge and nothing else here would
// notice.
func TestReconciliationAgreesWithWhatThisSystemWrote(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-reconciled", "100.00", "BRL")
	w.apply(t, command(t, wagering.Bet, "player-reconciled", "ext-r-1", "30.00", "BRL"), at(1))
	w.apply(t, command(t, wagering.Win, "player-reconciled", "ext-r-2", "5.00", "BRL"), at(2))
	// A loss moves nothing and writes no entry, which is exactly the shape that
	// would break a reconciliation derived from counting operations.
	w.apply(t, command(t, wagering.Loss, "player-reconciled", "ext-r-3", "0.00", "BRL"), at(3))

	stored, entries := w.snapshotOfWallet(t, wallet.ID())
	if finding := wagering.Reconcile(stored, entries); finding != nil {
		t.Fatalf("a wallet this system wrote does not reconcile: %v", finding)
	}
	if got := stored.Balance().Amount(); got != "75.00" {
		t.Fatalf("the balance is %s, wanted 75.00", got)
	}
	if len(entries) != 3 {
		t.Fatalf("the ledger holds %d entries, wanted 3", len(entries))
	}
}

// TestReconciliationFindsADivergence proves the read reports what is there
// rather than what should be.
//
// The divergence has to be written with the table's triggers off, because every
// path the application has is guarded and the guards work. That is the point of
// reconciliation: it exists for the state this system cannot produce — a bad
// migration, a manual UPDATE, a bug in another service — and a test of it has
// to produce that state the same way.
func TestReconciliationFindsADivergence(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-diverged", "100.00", "BRL")
	w.apply(t, command(t, wagering.Bet, "player-diverged", "ext-d-1", "30.00", "BRL"), at(1))

	w.corrupt(t, "wagering.wallet",
		`UPDATE wagering.wallet SET balance_minor = 9999 WHERE id = $1`, uuidOf(wallet.ID()))

	stored, entries := w.snapshotOfWallet(t, wallet.ID())
	finding := wagering.Reconcile(stored, entries)
	if finding == nil {
		t.Fatal("a wallet holding money its ledger does not explain reconciled")
	}
	mismatch, ok := errors.AsType[*wagering.ReconciliationError](finding)
	if !ok {
		t.Fatalf("reported %v, wanted a balance mismatch", finding)
	}
	if got := mismatch.Actual().Amount(); got != "99.99" {
		t.Fatalf("the stored balance is %s, wanted 99.99", got)
	}
	if got := mismatch.Expected().Amount(); got != "70.00" {
		t.Fatalf("the ledger sums to %s, wanted 70.00", got)
	}
}

// TestAReconciliationReadsOneInstant is why the snapshot is REPEATABLE READ.
//
// The balance and the entries it is compared against have to be the same
// instant. Under READ COMMITTED the wallet would be read before a movement
// commits and the ledger after it, and the reader would report a divergence
// that never existed — waking an operator for money that is exactly where it
// should be.
func TestAReconciliationReadsOneInstant(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-instant", "100.00", "BRL")

	var (
		stored  *wagering.Wallet
		entries []wagering.WalletLedgerEntry
	)
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		var err error
		if stored, err = r.Wallets.ByID(ctx, wallet.ID()); err != nil {
			return err
		}
		// A whole movement commits between the two reads, on another
		// connection. Its entry is the one the ledger read below must not see.
		w.apply(t, command(t, wagering.Bet, "player-instant", "ext-i-1", "30.00", "BRL"), at(1))

		entries, err = r.Ledger.All(ctx, wallet.ID())
		return err
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if got := len(entries); got != 1 {
		t.Fatalf("the snapshot saw %d entries, wanted the 1 that existed when it opened", got)
	}
	if got := stored.Balance().Amount(); got != "100.00" {
		t.Fatalf("the snapshot saw a balance of %s, wanted 100.00", got)
	}
	if finding := wagering.Reconcile(stored, entries); finding != nil {
		t.Fatalf("a movement committed mid-read was reported as a divergence: %v", finding)
	}

	// And the movement really did commit, so the test is not passing because
	// nothing happened.
	after, afterEntries := w.snapshotOfWallet(t, wallet.ID())
	if got := after.Balance().Amount(); got != "70.00" {
		t.Fatalf("after the snapshot the balance is %s, wanted 70.00", got)
	}
	if got := len(afterEntries); got != 2 {
		t.Fatalf("after the snapshot the ledger holds %d entries, wanted 2", got)
	}
}

// snapshotOfWallet reads a wallet and its whole ledger in one consistent view,
// which is what reconciliation does.
func (w *world) snapshotOfWallet(
	t *testing.T,
	id wagering.WalletID,
) (*wagering.Wallet, []wagering.WalletLedgerEntry) {
	t.Helper()
	var (
		wallet  *wagering.Wallet
		entries []wagering.WalletLedgerEntry
	)
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		var err error
		if wallet, err = r.Wallets.ByID(ctx, id); err != nil {
			return err
		}
		entries, err = r.Ledger.All(ctx, id)
		return err
	})
	if err != nil {
		t.Fatalf("read wallet %s and its ledger: %v", id, err)
	}
	if wallet == nil {
		t.Fatalf("no wallet %s", id)
	}
	return wallet, entries
}
