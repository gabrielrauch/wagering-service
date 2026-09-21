package app_test

import (
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// resume runs one turn of the worker.
func (f *fixture) resume(t *testing.T) app.ResumeOutcome {
	t.Helper()
	out, err := f.wagers.Resume(t.Context(), servicePrincipal(t))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	return out
}

// park submits an operation whose reference has not arrived, and returns it.
func (f *fixture) park(t *testing.T, external, key string) app.OperationResult {
	t.Helper()
	result := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: external,
		Key:      key,
		Amount:   "25.00", Reference: "never-arrived",
	}))
	if result.Status != wagering.PendingReference {
		t.Fatalf("status %s, want %s", result.Status, wagering.PendingReference)
	}
	return result
}

func (f *fixture) attempts(t *testing.T, id wagering.TransactionID) int {
	t.Helper()
	return f.row(t, id).snap.ReferenceAttempts
}

func (f *fixture) nextAttemptAt(t *testing.T, id wagering.TransactionID) time.Time {
	t.Helper()
	row := f.row(t, id)
	if row.nextAttemptAt == nil {
		t.Fatalf("transaction %s has no schedule", id)
	}
	return *row.nextAttemptAt
}

// waitForNextAttempt moves the clock to just past when an operation is next due,
// so that a test exercising the retry count does not also spend its TTL.
func (f *fixture) waitForNextAttempt(t *testing.T, id wagering.TransactionID) {
	t.Helper()
	f.clock.advance(f.nextAttemptAt(t, id).Sub(f.clock.Now()) + time.Second)
}

// The only non-terminal state that is ever committed, and it is committed with a
// schedule: the two are an equivalence, so a parked row that nothing would ever
// look at again is not representable.
func TestParkingRecordsAScheduleAndAnnouncesTheWait(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	parked := f.park(t, "ext-refund", "key-1")

	if got := f.attempts(t, parked.TransactionID); got != 1 {
		t.Errorf("attempts %d, want 1", got)
	}
	if at := f.nextAttemptAt(t, parked.TransactionID); !at.After(f.clock.Now()) {
		t.Errorf("next attempt at %s, want after now (%s)", at, f.clock.Now())
	}
	assertEvents(t, f.db, "WagerTransactionPendingReference")
}

// One retry, one attempt. The domain counts it when it parks the operation, so
// this layer writes back what the domain left and never adds to it — counting in
// both places would halve the budget.
func TestEachRetryCountsExactlyOneAttempt(t *testing.T) {
	// A budget wide enough that only the counting is under test here: running
	// out of time would settle the operation and stop the count for a reason
	// this test is not about.
	f := newFixtureWith(t, 10, 24*time.Hour,
		app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: time.Hour})
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	parked := f.park(t, "ext-refund", "key-1")

	for want := 2; want <= 4; want++ {
		f.waitForNextAttempt(t, parked.TransactionID)
		out := f.resume(t)
		if !out.Claimed {
			t.Fatalf("attempt %d: nothing claimed", want)
		}
		if got := f.attempts(t, parked.TransactionID); got != want {
			t.Fatalf("attempts %d, want %d", got, want)
		}
	}
}

// Initial * Factor^(n-1), capped at Max. The jitter is a tenth at most, and is
// derived from the operation's own identifier so the schedule is reproducible.
func TestBackoffGrowsGeometrically(t *testing.T) {
	f := newFixtureWith(t, 10, 24*time.Hour,
		app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: 10 * time.Minute})
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	parked := f.park(t, "ext-refund", "key-1")

	waits := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute}
	for i, want := range waits {
		if i > 0 {
			f.waitForNextAttempt(t, parked.TransactionID)
			if out := f.resume(t); !out.Claimed {
				t.Fatalf("attempt %d: nothing claimed", i+1)
			}
		}
		got := f.nextAttemptAt(t, parked.TransactionID).Sub(f.clock.Now())
		if got < want || got > want+want/10 {
			t.Errorf("attempt %d waits %s, want %s (+at most a tenth)", i+1, got, want)
		}
	}
}

// Waiting past the point where the domain will refuse to park again would mean
// an operation whose budget expired at noon is not looked at until one, and is
// then rejected for having run out of time an hour earlier.
func TestBackoffIsClampedToTheDeadline(t *testing.T) {
	f := newFixtureWith(t, 10, time.Hour,
		app.BackoffPolicy{Initial: 6 * time.Hour, Factor: 2, Max: 12 * time.Hour})
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	parked := f.park(t, "ext-refund", "key-1")

	at := f.nextAttemptAt(t, parked.TransactionID)
	deadline := f.clock.Now().Add(time.Hour)
	if !at.Equal(deadline) {
		t.Errorf("next attempt at %s, want the deadline %s", at, deadline)
	}
}

func TestTheWaitBudgetEndsInARejection(t *testing.T) {
	t.Run("when the attempts run out", func(t *testing.T) {
		f := newFixtureWith(t, 2, 24*time.Hour,
			app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: time.Hour})
		f.db.seedWallet(t, "player-1", "100.00", "BRL")
		parked := f.park(t, "ext-refund", "key-1")

		f.waitForNextAttempt(t, parked.TransactionID)
		f.resume(t) // attempts 2
		f.waitForNextAttempt(t, parked.TransactionID)
		out := f.resume(t)

		assertStatus(t, out.Result, wagering.Rejected, failure.ReferenceNotFound)
		if out.Rescheduled {
			t.Error("a settled operation carries no schedule")
		}
		if row := f.row(t, parked.TransactionID); row.nextAttemptAt != nil {
			t.Error("leaving PENDING_REFERENCE must clear the schedule in the same write")
		}
		if got := f.db.eventTypes(); got[len(got)-1] != "WagerTransactionRejected" {
			t.Errorf("last event %s, want WagerTransactionRejected", got[len(got)-1])
		}
	})

	t.Run("when the time runs out, even with attempts to spare", func(t *testing.T) {
		f := newFixtureWith(t, 100, time.Hour,
			app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: time.Hour})
		f.db.seedWallet(t, "player-1", "100.00", "BRL")
		parked := f.park(t, "ext-refund", "key-1")

		f.clock.advance(2 * time.Hour)
		out := f.resume(t)

		assertStatus(t, out.Result, wagering.Rejected, failure.ReferenceNotFound)
		if got := f.attempts(t, parked.TransactionID); got >= 100 {
			t.Errorf("attempts %d — the budget ended on time, not on count", got)
		}
	})
}

// An operation becoming available and the operations waiting on it becoming due
// are one fact, so they are one commit.
func TestAProcessedReferenceWakesItsWaitersInTheSameCommit(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	waiting := f.submit(t, fields(acme, submission{
		Kind:     "WIN",
		External: "ext-win",
		Key:      "key-1",
		Amount:   "50.00", Reference: "ext-bet",
	}))
	if at := f.nextAttemptAt(t, waiting.TransactionID); !at.After(f.clock.Now()) {
		t.Fatalf("fixture: the waiter is already due")
	}

	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-2", Amount: "25.00"}))

	if at := f.nextAttemptAt(t, waiting.TransactionID); at.After(f.clock.Now()) {
		t.Errorf("waiter is next looked at %s, want due now (%s)", at, f.clock.Now())
	}
	// And it really is claimable, without the clock moving at all.
	if out := f.resume(t); !out.Claimed {
		t.Error("the woken operation was not claimable in the very next turn")
	}
}

// MakeDue is scoped to the wallet the caller holds the lock on. Waking a row on
// another wallet writes outside this transaction's declared write set, which is
// what the lock order exists to prevent.
func TestWakingIsScopedToTheLockedWallet(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.db.seedWallet(t, "player-2", "100.00", "BRL")

	other := fields(acme, submission{
		Kind: "WIN", External: "ext-win-2", Key: "key-1", Amount: "50.00", Reference: "ext-bet",
	})
	other.PlayerID = "player-2"
	waiting := f.submit(t, other)
	before := f.nextAttemptAt(t, waiting.TransactionID)

	// The bet lands on player-1's wallet.
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-2", Amount: "25.00"}))

	if at := f.nextAttemptAt(t, waiting.TransactionID); !at.Equal(before) {
		t.Errorf("a waiter on another wallet was woken: %s -> %s", before, at)
	}
}

// Between the unlocked SELECT and the locked re-read, the row may have settled or
// been taken. Neither is a failure, and a worker that alerted on it would alert
// on its own normal operation.
func TestAClaimThatIsNoLongerDueIsSkipped(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	parked := f.park(t, "ext-refund", "key-1")
	f.clock.advance(time.Hour)

	f.db.afterNextDue = func(st *state) {
		row := st.txns[parked.TransactionID]
		later := f.clock.Now().Add(time.Hour)
		row.nextAttemptAt = &later
		st.txns[parked.TransactionID] = row
	}

	out := f.resume(t)

	if out.Claimed {
		t.Error("claimed a row that stopped being due")
	}
	if got := f.attempts(t, parked.TransactionID); got != 1 {
		t.Errorf("attempts %d, want 1 — a skipped row is not an attempt", got)
	}
}

// This system failing to rebuild its own operation is not a business outcome.
// The row stays parked, no attempt is counted, and somebody is told.
func TestAnOperationThatCannotBeCarriedForwardParksWithoutSpendingTheBudget(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	parked := f.park(t, "ext-refund", "key-1")
	f.db.corruptPayloadHash(t, parked.TransactionID)
	f.clock.advance(time.Minute * 5)

	out := f.resume(t)

	if !out.Claimed || !out.Rescheduled {
		t.Errorf("claimed=%t rescheduled=%t, want both", out.Claimed, out.Rescheduled)
	}
	if row := f.row(t, parked.TransactionID); row.snap.Status != wagering.PendingReference {
		t.Errorf("status %s, want it left parked", row.snap.Status)
	}
	if got := f.attempts(t, parked.TransactionID); got != 1 {
		t.Errorf("attempts %d, want 1 — nothing was spent, so nothing is counted", got)
	}
	if len(f.observer.defects) != 1 || f.observer.defects[0] != parked.TransactionID {
		t.Errorf("defects %v, want exactly %s", f.observer.defects, parked.TransactionID)
	}
	if !f.nextAttemptAt(t, parked.TransactionID).After(f.clock.Now()) {
		t.Error("the next attempt was not pushed out")
	}
}

// FAILED is terminal, carries no code and no balance, and emits no event, so it
// is the last resort — reached only once waiting has run out anyway.
func TestAnOperationStillUnrebuildableAtTheDeadlineFails(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	parked := f.park(t, "ext-refund", "key-1")
	f.db.corruptPayloadHash(t, parked.TransactionID)
	f.clock.advance(2 * time.Hour)

	before := len(f.db.eventTypes())
	out := f.resume(t)

	if out.Result.Status != wagering.Failed {
		t.Errorf("status %s, want %s", out.Result.Status, wagering.Failed)
	}
	if out.Rescheduled {
		t.Error("a failed operation carries no schedule")
	}
	if row := f.row(t, parked.TransactionID); row.nextAttemptAt != nil {
		t.Error("leaving PENDING_REFERENCE must clear the schedule")
	}
	if got := len(f.db.eventTypes()); got != before {
		t.Errorf("published %d events, want none: FAILED is for the audit trail, not for subscribers", got-before)
	}
}
