//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestTwoMovementsOnOneWalletSerialise is the concurrency design in one test.
//
// The wallet row lock, and nothing else, is what makes two commands on one
// wallet happen one after the other. The isolation level is READ COMMITTED and
// would not serialise them; the balance check is made against a value and could
// not.
func TestTwoMovementsOnOneWalletSerialise(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-serialise", "100.00", "BRL")

	held := make(chan struct{})
	release := make(chan struct{})
	entered := make(chan time.Time, 1)
	first := make(chan error, 1)

	go func() {
		first <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	second := make(chan error, 1)
	go func() {
		second <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
				return err
			}
			entered <- time.Now()
			return nil
		})
	}()

	// Long enough that a lock which did not serialise would have let the second
	// command through, and short enough to stay inside the lock timeout — a
	// waiter cut off at 750ms would prove nothing about serialisation.
	time.Sleep(200 * time.Millisecond)
	select {
	case got := <-entered:
		t.Fatalf("the second movement took the wallet at %s while the first still held it", got)
	default:
	}

	releasedAt := time.Now()
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("the first movement failed: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("the second movement failed: %v", err)
	}
	if got := <-entered; got.Before(releasedAt) {
		t.Fatalf("the second movement entered at %s, before the first released at %s",
			got, releasedAt)
	}
}

// TestMovementsOnDifferentWalletsDoNotWaitForEachOther is the other half of the
// same claim: the serialisation is per wallet, not global.
//
// Without it, "take the wallet lock first" would be indistinguishable from a
// single queue through the whole service, and the throughput of the system
// would be one command at a time.
func TestMovementsOnDifferentWalletsDoNotWaitForEachOther(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	one := w.openWallet(t, "player-parallel-one", "100.00", "BRL")
	two := w.openWallet(t, "player-parallel-two", "100.00", "BRL")

	// Each holds its own wallet and then waits for the other to have taken
	// theirs. If the two queued, neither would ever signal and this would
	// deadlock — which the test's own timeout reports.
	first, second := make(chan struct{}), make(chan struct{})
	done := make(chan error, 2)

	hold := func(id wagering.WalletID, taken, other chan struct{}) {
		done <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			if _, err := r.Wallets.LockByID(ctx, id); err != nil {
				return err
			}
			close(taken)
			<-other
			return nil
		})
	}
	go hold(one.ID(), first, second)
	go hold(two.ID(), second, first)

	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("a movement failed: %v", err)
		}
	}
}

// TestALockedWalletDoesNotBlockARowThatReferencesIt is what FOR NO KEY UPDATE
// buys.
//
// A wager transaction names its wallet through a foreign key, so inserting one
// takes FOR KEY SHARE on the wallet row. FOR NO KEY UPDATE does not conflict
// with that; FOR UPDATE does. Taken with FOR UPDATE, a wallet in the middle of
// a movement would block every insert that merely mentions it — including the
// outbox's aggregate key and another command's ledger entry — and the lock
// taken to serialise money would have become a lock on naming it.
func TestALockedWalletDoesNotBlockARowThatReferencesIt(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-fk", "100.00", "BRL")

	held := make(chan struct{})
	release := make(chan struct{})
	holder := make(chan error, 1)

	go func() {
		holder <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	// A plain insert naming the locked wallet, with no interest in its balance.
	cmd := command(t, wagering.Bet, "player-fk", "ext-fk", "10.00", "BRL")
	started := time.Now()
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		tx, err := wagering.NewExternalTransaction(cmd, wallet.ID(), at(1))
		if err != nil {
			return err
		}
		return r.Transactions.Record(ctx, tx, "fk")
	})
	took := time.Since(started)
	close(release)
	if holderErr := <-holder; holderErr != nil {
		t.Fatalf("the holder failed: %v", holderErr)
	}

	if err != nil {
		t.Fatalf("an insert referencing a locked wallet was blocked: %v", err)
	}
	if took >= testLockTimeout {
		t.Fatalf("the insert took %s, which is the lock timeout: it waited on the wallet", took)
	}
	const bets = `SELECT count(*) FROM wagering.wager_transaction WHERE kind = 'BET'`
	if got := w.count(t, bets); got != 1 {
		t.Fatalf("committed %d bets, wanted 1", got)
	}
}

// TestAMovementComputedFromAStaleWalletIsRefused is the lost-update detector.
//
// The wallet lock should make this unreachable: two movements on one wallet
// queue, so the second reads the balance the first left. This one reaches it
// anyway, by computing a movement from a wallet read outside the lock and
// settling it after another movement has committed — which is what a caller
// that skipped the lock would do.
//
// The write is refused rather than applied, and the answer is Retryable: the
// same operation sent again reads the current balance and gets the right
// answer, where this one would have overwritten somebody else's money.
func TestAMovementComputedFromAStaleWalletIsRefused(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-stale", "100.00", "BRL")

	// Read outside any lock, which is the whole of the mistake being made.
	key := wagering.WalletKey{
		PlayerID: mustPlayer(t, "player-stale"),
		Currency: mustMoney(t, "0.00", "BRL").Currency(),
	}
	var stale *wagering.Wallet
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		var err error
		stale, err = r.Wallets.ByKey(ctx, key)
		return err
	})
	if err != nil || stale == nil {
		t.Fatalf("read the wallet: %v", err)
	}

	// Somebody else moves the wallet properly, through the lock.
	w.apply(t, command(t, wagering.Bet, "player-stale", "ext-winner", "10.00", "BRL"), at(1))

	// And now the stale movement is settled.
	cmd := command(t, wagering.Bet, "player-stale", "ext-loser", "20.00", "BRL")
	err = w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		tx, err := wagering.NewExternalTransaction(cmd, stale.ID(), at(2))
		if err != nil {
			return err
		}
		if err := r.Transactions.Record(ctx, tx, "stale"); err != nil {
			return err
		}
		outcome, err := processor(t).Continue(tx, cmd, stale, nil, at(2))
		if err != nil {
			return err
		}
		return r.Settle(ctx, outcome, time.Time{})
	})

	if !errors.Is(err, ErrLostUpdate) {
		t.Fatalf("settled a stale movement with %v, wanted a lost update", err)
	}
	classifies(t, err, app.Retryable)

	// Nothing of the refused movement survived, including the transaction row
	// it had already inserted.
	if got := w.count(t,
		`SELECT count(*) FROM wagering.wager_transaction WHERE external_transaction_id = $1`,
		"ext-loser"); got != 0 {
		t.Fatalf("%d rows of the refused movement survived, wanted 0", got)
	}
	var balance int64
	if err := w.owner.QueryRow(t.Context(),
		`SELECT balance_minor FROM wagering.wallet WHERE id = $1`,
		uuidOf(stale.ID())).Scan(&balance); err != nil {
		t.Fatalf("read the balance: %v", err)
	}
	if balance != 9000 {
		t.Fatalf("balance is %d minor units, wanted 9000", balance)
	}
}
