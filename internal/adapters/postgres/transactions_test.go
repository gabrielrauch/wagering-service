//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestConcurrentIdenticalSubmissionsProduceOneRow is idempotency decided by the
// database rather than by a read.
//
// Every attempt inserts without looking first, so there is no window between
// finding the keys free and claiming them. PostgreSQL makes the losers WAIT on
// the winner's uncommitted row — it cannot know whether they conflict until it
// knows whether that row will exist — so each one observes the winner's
// decision rather than guessing at it, and the two assertions below are the two
// halves of that: exactly one row, and no loser answered before the winner
// could have committed.
func TestConcurrentIdenticalSubmissionsProduceOneRow(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-race", "1000.00", "BRL")
	cmd := command(t, wagering.Bet, "player-race", "ext-race", "10.00", "BRL")

	// No wallet lock in any attempt. The lock would serialise them on the
	// wallet and the unique keys would never be contended, which is the thing
	// under test.
	record := func() error {
		return w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			attempt := cmd
			attempt.TransactionID = wagering.NewTransactionID()
			tx, err := wagering.NewExternalTransaction(attempt, wallet.ID(), at(1))
			if err != nil {
				return err
			}
			return r.Transactions.Record(ctx, tx, "race")
		})
	}

	const losers = 6
	recorded := make(chan struct{})
	release := make(chan struct{})
	winner := make(chan error, 1)

	go func() {
		winner <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			attempt := cmd
			attempt.TransactionID = wagering.NewTransactionID()
			tx, err := wagering.NewExternalTransaction(attempt, wallet.ID(), at(1))
			if err != nil {
				return err
			}
			if err := r.Transactions.Record(ctx, tx, "race"); err != nil {
				return err
			}
			close(recorded)
			<-release
			return nil
		})
	}()
	<-recorded

	type answer struct {
		err        error
		answeredAt time.Time
	}
	answers := make(chan answer, losers)
	for range losers {
		go func() {
			err := record()
			answers <- answer{err: err, answeredAt: time.Now()}
		}()
	}

	// Long enough that an attempt which did not block would have finished, and
	// well inside the lock timeout, which bounds a wait on another
	// transaction's uncommitted key as much as it bounds a wait on a row lock.
	time.Sleep(contentionPause)
	releasedAt := time.Now()
	close(release)

	if err := <-winner; err != nil {
		t.Fatalf("the winning submission failed: %v", err)
	}
	for range losers {
		got := <-answers
		if !errors.Is(got.err, app.ErrDuplicateSubmission) {
			t.Fatalf("a losing submission reported %v, wanted ErrDuplicateSubmission", got.err)
		}
		if got.answeredAt.Before(releasedAt) {
			t.Fatalf("a losing submission answered at %s, before the winner could commit at %s",
				got.answeredAt, releasedAt)
		}
	}

	if got := w.count(t,
		`SELECT count(*) FROM wagering.wager_transaction WHERE external_transaction_id = $1`,
		"ext-race"); got != 1 {
		t.Fatalf("%d rows recorded, wanted 1", got)
	}
	// And the row the losers can now read is the one that won.
	var stored *app.StoredTransaction
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		var err error
		stored, err = r.Transactions.ByIdempotencyKey(ctx, cmd.Provider, cmd.IdempotencyKey)
		return err
	})
	if err != nil {
		t.Fatalf("read the winner back: %v", err)
	}
	if stored == nil {
		t.Fatal("the winning submission cannot be read back by its idempotency key")
	}
	if got, _ := stored.Transaction.ExternalTransactionID(); got != "ext-race" {
		t.Fatalf("read back %q, wanted ext-race", got)
	}
}

// TestAReadIsScopedToItsProvider proves the scoping is in the SQL rather than
// in a filter applied afterwards.
//
// The provider is half of both keys, so another provider's row is never read,
// never mind never returned — which is what makes the answer to "does this
// operation exist?" the same for a provider walking somebody else's identifiers
// as for one asking about nothing at all.
func TestAReadIsScopedToItsProvider(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-scoped", "100.00", "BRL")
	cmd := command(t, wagering.Bet, "player-scoped", "ext-scoped", "10.00", "BRL")
	w.apply(t, cmd, at(1))

	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		mine, err := r.Transactions.ByExternal(ctx, cmd.Provider, cmd.ExternalTransactionID)
		if err != nil {
			return err
		}
		if mine == nil {
			t.Fatal("the provider cannot read its own operation")
		}
		theirs, err := r.Transactions.ByExternal(ctx, "other", cmd.ExternalTransactionID)
		if err != nil {
			return err
		}
		if theirs != nil {
			t.Fatalf("another provider read operation %q", cmd.ExternalTransactionID)
		}
		byKey, err := r.Transactions.ByIdempotencyKey(ctx, "other", cmd.IdempotencyKey)
		if err != nil {
			return err
		}
		if byKey != nil {
			t.Fatalf("another provider read key %q", cmd.IdempotencyKey)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
}

// TestTheReferenceViewCarriesWhateverHoldsTheReference proves the view the
// domain reasons over is built from active_reversal.
//
// The holder is rehydrated as a real transaction rather than signalled by a
// flag. ReferenceView.ActiveReversal skips a view whose Transaction is nil, so
// a synthetic holder would make REFERENCE_ALREADY_REVERSED unreachable in the
// domain and leave the rule living only in the schema.
func TestTheReferenceViewCarriesWhateverHoldsTheReference(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-view", "100.00", "BRL")
	bet := w.apply(t, command(t, wagering.Bet, "player-view", "ext-bet", "80.00", "BRL"), at(1))

	view := w.referenceFor(t, "acme", "ext-bet")
	if !view.Found() {
		t.Fatal("the bet was not found")
	}
	if got := view.Transaction.ID(); got != bet.Transaction.ID() {
		t.Fatalf("found transaction %s, wanted %s", got, bet.Transaction.ID())
	}
	if _, held := view.ActiveReversal(); held {
		t.Fatal("an unreversed bet reports a reversal holding it")
	}

	refund := reversingCommand(
		command(t, wagering.Refund, "player-view", "ext-refund", "80.00", "BRL"), "ext-bet")
	settled := w.apply(t, refund, at(2))

	view = w.referenceFor(t, "acme", "ext-bet")
	holder, held := view.ActiveReversal()
	if !held {
		t.Fatal("a refunded bet reports no reversal holding it")
	}
	if holder.ID() != settled.Transaction.ID() {
		t.Fatalf("held by %s, wanted the refund %s", holder.ID(), settled.Transaction.ID())
	}
	if holder.Kind() != wagering.Refund {
		t.Fatalf("held by a %s, wanted a REFUND", holder.Kind())
	}

	if got := w.referenceFor(t, "acme", "ext-nothing"); got != nil {
		t.Fatalf("a reference that does not exist came back as %v, wanted nil", got)
	}
	if got := w.referenceFor(t, "other", "ext-bet"); got != nil {
		t.Fatal("another provider read a reference of ours")
	}
}

// TestTheReferenceViewCarriesAReleasedReversalToo is what the per-kind rule
// needs from the adapter, and what a view read from active_reversal alone
// cannot give it.
//
// Rolling back a refund deletes the refund's hold on the bet rather than
// marking it, so "what holds this bet?" answers nothing — which is right for
// the active rule and wrong for the other one: the bet has still been refunded
// once, successfully, and must not be refunded again. So the view carries every
// processed reversal that resolved to the reference, and active_reversal
// answers only whether each one still holds, which is the Reversed flag.
func TestTheReferenceViewCarriesAReleasedReversalToo(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-released", "100.00", "BRL")
	w.apply(t, command(t, wagering.Bet, "player-released", "ext-bet", "80.00", "BRL"), at(1))
	refund := w.apply(t, reversingCommand(
		command(t, wagering.Refund, "player-released", "ext-refund", "80.00", "BRL"), "ext-bet"), at(2))
	undo := w.apply(t, reversingCommand(
		command(t, wagering.Rollback, "player-released", "ext-undo", "80.00", "BRL"), "ext-refund"), at(3))

	view := w.referenceFor(t, "acme", "ext-bet")
	if _, held := view.ActiveReversal(); held {
		t.Fatal("a bet whose refund was rolled back still reports a reversal holding it")
	}
	if len(view.Reversals) != 1 {
		t.Fatalf("the view carries %d reversals, wanted the released refund alone", len(view.Reversals))
	}
	released := view.Reversals[0]
	if released.Transaction == nil || released.Transaction.ID() != refund.Transaction.ID() {
		t.Fatalf("the view carries %v, wanted the refund %s", released.Transaction, refund.Transaction.ID())
	}
	if !released.Reversed {
		t.Fatal("the refund that was rolled back is not flagged as reversed")
	}
	if !view.HasSuccessfulReversalOfKind(wagering.Refund) {
		t.Fatal("a refunded bet does not report a successful refund once the refund is undone")
	}
	if view.HasSuccessfulReversalOfKind(wagering.Rollback) {
		t.Fatal("the bet reports a successful rollback it never received")
	}

	// The refund's own view: held by the rollback, which still stands.
	view = w.referenceFor(t, "acme", "ext-refund")
	holder, held := view.ActiveReversal()
	if !held || holder.ID() != undo.Transaction.ID() {
		t.Fatalf("the refund is held by %v, wanted the rollback %s", holder, undo.Transaction.ID())
	}
	if len(view.Reversals) != 1 || view.Reversals[0].Reversed {
		t.Fatalf("the refund's view carries %v, wanted one standing rollback", view.Reversals)
	}
}

// TestWakingWaitersIsScopedToTheWallet pins the rule ADR-0011 states: an
// operation that becomes available wakes only the waiters on its own wallet.
//
// Waking one elsewhere would write outside this transaction's declared write
// set, which is the whole thing the lock order exists to prevent — and waiters
// on other wallets are found by their own schedule, so nothing is lost.
func TestWakingWaitersIsScopedToTheWallet(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	mine := w.openWallet(t, "player-woken", "100.00", "BRL")
	w.openWallet(t, "player-elsewhere", "100.00", "BRL")

	parked := w.park(t, "player-woken", "ext-wait-1", "ext-target", at(1))
	elsewhere := w.park(t, "player-elsewhere", "ext-wait-2", "ext-target", at(2))

	var woke int
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		if _, err := r.Wallets.LockByID(ctx, mine.ID()); err != nil {
			return err
		}
		var err error
		woke, err = r.Transactions.MakeDue(ctx, app.SettledOperation{
			WalletID: mine.ID(),
			Provider: "acme",
			External: "ext-target",
		}, at(5))
		return err
	})
	if err != nil {
		t.Fatalf("wake the waiters: %v", err)
	}
	if woke != 1 {
		t.Fatalf("woke %d operations, wanted 1", woke)
	}
	if got := w.scheduleOf(t, parked); !got.Equal(at(5)) {
		t.Fatalf("the waiter on this wallet is scheduled for %s, wanted %s", got, at(5))
	}
	if got := w.scheduleOf(t, elsewhere); got.Equal(at(5)) {
		t.Fatalf("a waiter on another wallet was woken, scheduled for %s", got)
	}
}

// TestTheNextDueOperationIsTheOneWaitingLongest pins the claim order, and that
// nothing due is an answer rather than a failure.
func TestTheNextDueOperationIsTheOneWaitingLongest(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-due", "100.00", "BRL")
	later := w.park(t, "player-due", "ext-later", "ext-target-a", at(1))
	earlier := w.park(t, "player-due", "ext-earlier", "ext-target-b", at(2))

	w.rescheduleOperation(t, later, at(40))
	w.rescheduleOperation(t, earlier, at(20))

	if got := w.nextDue(t, at(10)); got != nil {
		t.Fatalf("claimed %s before anything was due", got.TransactionID)
	}
	got := w.nextDue(t, at(30))
	if got == nil {
		t.Fatal("nothing was due, wanted the operation scheduled first")
	}
	if got.TransactionID != earlier {
		t.Fatalf("claimed %s, wanted the earlier %s", got.TransactionID, earlier)
	}
}

// TestTiedSchedulesAreBrokenByTransactionID is the trailing column of
// wager_transaction_due_idx, tested on the case that makes it load-bearing.
//
// MakeDue stamps every operation it wakes with ONE instant, so a commit that
// wakes two leaves them due at exactly the same time. Ordering on the schedule
// alone would then leave the choice to whatever order the rows came back in —
// which is stable enough in a test to look deliberate and is not a promise the
// database makes. The two candidates here are deliberately given no other way
// to be told apart.
func TestTiedSchedulesAreBrokenByTransactionID(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-tied", "100.00", "BRL")
	first := w.park(t, "player-tied", "ext-tied-1", "ext-target", at(1))
	second := w.park(t, "player-tied", "ext-tied-2", "ext-target", at(2))

	var woke int
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
			return err
		}
		var err error
		woke, err = r.Transactions.MakeDue(ctx, app.SettledOperation{
			WalletID: wallet.ID(),
			Provider: "acme",
			External: "ext-target",
		}, at(5))
		return err
	})
	if err != nil {
		t.Fatalf("wake the waiters: %v", err)
	}
	if woke != 2 {
		t.Fatalf("woke %d operations, wanted both", woke)
	}
	if a, b := w.scheduleOf(t, first), w.scheduleOf(t, second); !a.Equal(b) {
		t.Fatalf("the two were woken at %s and %s, wanted one instant", a, b)
	}

	lower := first
	if uuid.UUID(second).Compare(uuid.UUID(first)) < 0 {
		lower = second
	}
	// Twice, because a tie broken by nothing would still answer consistently
	// often enough to pass once.
	for turn := range 2 {
		got := w.nextDue(t, at(6))
		if got == nil {
			t.Fatalf("turn %d claimed nothing with two operations due", turn)
		}
		if got.TransactionID != lower {
			t.Fatalf("turn %d claimed %s, wanted the lower id %s", turn, got.TransactionID, lower)
		}
	}
}

// TestOneParkedOperationIsClaimedByOneWorker is the no-double-claim rule, and
// its other half: a claim held by a transaction that ended goes back.
//
// That asymmetry is what CONTEXT.md draws between the two kinds of claim. A
// publisher's claim on an outbox event is stored and expires by the clock; a
// worker's claim on a waiting operation lasts only as long as the transaction
// that took it, so there is nothing to expire and nothing to recover — it is
// simply gone.
func TestOneParkedOperationIsClaimedByOneWorker(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-claim", "100.00", "BRL")
	parked := w.park(t, "player-claim", "ext-parked", "ext-target", at(1))
	w.rescheduleOperation(t, parked, at(2))

	t.Run("a second worker finds nothing once the first has moved it", func(t *testing.T) {
		claimed := make(chan struct{})
		release := make(chan struct{})
		first := make(chan error, 1)

		go func() {
			first <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
				if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
					return err
				}
				stored, err := r.Transactions.ClaimForUpdate(ctx, parked, at(5))
				if err != nil {
					return err
				}
				if stored == nil {
					t.Error("the first worker claimed nothing")
					return nil
				}
				// Pushed out of dueness, which is what carrying an operation
				// forward and failing to settle it leaves behind.
				if err := r.Transactions.Reschedule(ctx, parked, at(100)); err != nil {
					return err
				}
				close(claimed)
				<-release
				return nil
			})
		}()
		<-claimed

		started := make(chan struct{})
		finished := make(chan time.Time, 1)
		second := make(chan error, 1)
		go func() {
			second <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
				close(started)
				// Waits on the wallet, which is where the two workers meet:
				// the lock order says the wallet comes before the row.
				if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
					return err
				}
				stored, err := r.Transactions.ClaimForUpdate(ctx, parked, at(5))
				if err != nil {
					return err
				}
				if stored != nil {
					t.Errorf("two workers claimed operation %s", parked)
				}
				finished <- time.Now()
				return nil
			})
		}()
		<-started

		// Released only once the second worker is genuinely queued behind the
		// first. Without this the two might never contend at all, and the
		// assertion above would hold for the uninteresting reason that the
		// second worker ran after the first had finished.
		time.Sleep(contentionPause)
		select {
		case got := <-finished:
			t.Fatalf("the second worker finished at %s while the first still held the wallet", got)
		default:
		}

		releasedAt := time.Now()
		close(release)
		if err := <-first; err != nil {
			t.Fatalf("the first worker failed: %v", err)
		}
		if err := <-second; err != nil {
			t.Fatalf("the second worker failed: %v", err)
		}
		if got := <-finished; got.Before(releasedAt) {
			t.Fatalf("the second worker finished at %s, before the first released at %s",
				got, releasedAt)
		}
	})

	t.Run("a claim whose transaction ended is claimable again", func(t *testing.T) {
		// Back to due, and then claimed by a transaction that rolls back.
		w.rescheduleOperation(t, parked, at(2))
		abandoned := errors.New("the worker gave up")
		err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
				return err
			}
			stored, err := r.Transactions.ClaimForUpdate(ctx, parked, at(5))
			if err != nil {
				return err
			}
			if stored == nil {
				t.Fatal("the abandoning worker claimed nothing")
			}
			return abandoned
		})
		if !errors.Is(err, abandoned) {
			t.Fatalf("returned %v, wanted the worker's own error", err)
		}

		got := w.nextDue(t, at(5))
		if got == nil || got.TransactionID != parked {
			t.Fatalf("the abandoned operation was not claimable again, got %v", got)
		}
	})
}

// TestReschedulingMovesOnlyTheSchedule pins what Reschedule must not do.
//
// It is called when carrying an operation forward failed, and nothing was
// spent: counting an attempt there would consume a budget that exists to bound
// how long a reference may take to arrive, so a bug that shipped for ten
// minutes would reject operations for having run out of time they never used.
func TestReschedulingMovesOnlyTheSchedule(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-resched", "100.00", "BRL")
	parked := w.park(t, "player-resched", "ext-resched", "ext-target", at(1))

	before := w.rowOf(t, parked)
	w.rescheduleOperation(t, parked, at(90))
	after := w.rowOf(t, parked)

	if !w.scheduleOf(t, parked).Equal(at(90)) {
		t.Fatalf("the schedule is %s, wanted %s", w.scheduleOf(t, parked), at(90))
	}
	if after.attempts != before.attempts {
		t.Errorf("attempts went from %d to %d", before.attempts, after.attempts)
	}
	if !after.updatedAt.Equal(before.updatedAt) {
		t.Errorf("updated_at went from %s to %s", before.updatedAt, after.updatedAt)
	}
	if !timeFrom(after.deadline).Equal(timeFrom(before.deadline)) {
		t.Errorf("the deadline went from %s to %s",
			timeFrom(before.deadline), timeFrom(after.deadline))
	}
	if after.status != before.status {
		t.Errorf("the status went from %s to %s", before.status, after.status)
	}
}

// TestFailingAParkedOperationTakesItOutOfTheSchedule pins the equivalence
// wager_transaction_only_waiting_is_scheduled states: settled work leaves the
// scheduler's index, in the same statement that settles it.
func TestFailingAParkedOperationTakesItOutOfTheSchedule(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-failed", "100.00", "BRL")
	parked := w.park(t, "player-failed", "ext-failed", "ext-target", at(1))

	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		stored, err := r.Transactions.ByID(ctx, parked)
		if err != nil {
			return err
		}
		if err := stored.Transaction.Fail(at(9)); err != nil {
			return err
		}
		return r.Transactions.Fail(ctx, stored.Transaction)
	})
	if err != nil {
		t.Fatalf("fail a parked operation: %v", err)
	}

	row := w.rowOf(t, parked)
	if row.status != string(wagering.Failed) {
		t.Fatalf("the operation is %s, wanted FAILED", row.status)
	}
	if row.scheduled.Valid {
		t.Fatalf("a failed operation is still scheduled for %s", row.scheduled.Time)
	}
	if got := w.nextDue(t, at(100)); got != nil {
		t.Fatalf("a failed operation is still claimable as %s", got.TransactionID)
	}
}
