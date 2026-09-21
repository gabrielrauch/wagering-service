//go:build multi

// Two wallets, two instances, at the same time.
package multi

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// interval is when one submission started and when it came back.
type interval struct {
	started  time.Time
	finished time.Time
	got      answer
	err      error
}

// overlaps reports whether this submission was entirely inside another's.
func (i interval) inside(other interval) bool {
	return i.started.After(other.started) && i.finished.Before(other.finished)
}

// TestTwoWalletsAreProcessedAtTheSameTimeAcrossTwoInstances proves that one
// wallet's operation does not stop another wallet's.
//
// # Why this is not a stopwatch
//
// The obvious version of this scenario sends two submissions at once, measures
// how long they took, and concludes they overlapped because the total was less
// than twice one of them. That version passes on a serialised system as soon as
// the machine is fast enough, which is to say it passes always and means
// nothing. This suite has shipped four tests that passed with the behaviour
// deleted; this is where the fifth would have been.
//
// So the overlap is MANUFACTURED and then observed. A session outside the
// service takes wallet A's row with SELECT FOR UPDATE and holds it. The
// submission against wallet A then cannot finish, and the test does not guess
// that — it waits until PostgreSQL itself reports a backend blocked by that
// exact session, through pg_blocking_pids, which is a fact about the server
// rather than about pool sizing or about how long anything took. Only then does
// it submit against wallet B, through a different instance, and require that
// submission to complete while the first is still visibly queued.
//
// What would fail if the concurrency were removed is therefore not a timing
// margin. A service that serialised wallets behind one lock would leave B's
// submission blocked too, the second count would be two rather than one, and
// B's answer would never arrive.
func TestTwoWalletsAreProcessedAtTheSameTimeAcrossTwoInstances(t *testing.T) {
	requireStack(t)
	ctx := context.Background()

	held := scoped("player-held")
	free := scoped("player-free")
	heldWallet := openWallet(t, instances[0], held, "100.00")
	freeWallet := openWallet(t, instances[0], free, "100.00")

	// A connection of its own rather than one from the pool, because this one
	// keeps a transaction open and a pooled connection handed back mid
	// transaction is a pool with a poisoned member in it.
	holder, err := pgx.Connect(ctx, composeDSN)
	if err != nil {
		t.Fatalf("open the session that holds the lock: %v", err)
	}
	defer func() { _ = holder.Close(ctx) }()

	var holderPID int
	if err := holder.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("read the holding session's backend id: %v", err)
	}
	locking, err := holder.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the holding transaction: %v", err)
	}
	// FOR UPDATE rather than FOR NO KEY UPDATE, which is what the service takes:
	// the stronger mode conflicts with the weaker one, so this blocks the
	// service where the weaker one would let it through.
	if _, err := locking.Exec(ctx,
		"SELECT id FROM wagering.wallet WHERE id = $1 FOR UPDATE", heldWallet.WalletID); err != nil {
		t.Fatalf("take wallet %s: %v", heldWallet.WalletID, err)
	}
	released := false
	defer func() {
		if !released {
			_ = locking.Rollback(ctx)
		}
	}()

	credential := token(t, providerA)
	// Rendered before the goroutine starts, because encode fails the test and a
	// test may only be failed from the goroutine running it.
	heldBody := encode(t, bet(providerA, scoped("held-bet"), held, "10.00"))
	blocked := make(chan interval, 1)
	go func() {
		span := interval{started: time.Now()}
		span.got, span.err = attempt(call{
			base:           instances[0],
			method:         http.MethodPost,
			path:           "/wagering/transactions",
			body:           heldBody,
			token:          credential,
			idempotencyKey: scoped("held-key"),
		})
		span.finished = time.Now()
		blocked <- span
	}()

	// Exactly one, and nothing proceeds until there is exactly one. "At least
	// one" would pass on a second backend queued behind the first, which is
	// what a service that serialised every wallet would produce.
	awaitBlockedBy(t, holderPID, 1, "the submission against the held wallet to queue behind it")

	other := interval{started: time.Now()}
	other.got, other.err = attempt(call{
		base:           instances[1],
		method:         http.MethodPost,
		path:           "/wagering/transactions",
		body:           encode(t, bet(providerA, scoped("free-bet"), free, "10.00")),
		token:          credential,
		idempotencyKey: scoped("free-key"),
	})
	other.finished = time.Now()

	// Still exactly one, read after the second submission came back. This is
	// the assertion: the first operation was in flight for the whole of the
	// second, which a system that ran them one after another could not produce.
	if blockedNow := blockedBy(t, holderPID); blockedNow != 1 {
		t.Errorf("%d backends were blocked by the holding session once the second wallet's "+
			"submission had been answered, wanted 1 — the first operation was supposed to "+
			"still be in flight", blockedNow)
	}

	if err := locking.Rollback(ctx); err != nil {
		t.Fatalf("release the held wallet: %v", err)
	}
	released = true

	first := <-blocked
	if first.err != nil {
		t.Fatalf("the submission against the held wallet failed: %v", first.err)
	}
	if other.err != nil {
		t.Fatalf("the submission against the free wallet failed: %v", other.err)
	}
	if !other.inside(first) {
		t.Errorf("the free wallet's submission ran %s..%s and the held wallet's ran %s..%s: "+
			"they did not overlap", other.started, other.finished, first.started, first.finished)
	}

	for _, answered := range []struct {
		what string
		got  answer
	}{{"the held wallet", first.got}, {"the free wallet", other.got}} {
		if answered.got.status != http.StatusOK {
			t.Fatalf("%s was answered %s, wanted 200", answered.what, answered.got)
		}
		if op := operationOf(t, answered.got); op.Status != processed {
			t.Errorf("%s is %s (%s), wanted %s", answered.what, op.Status,
				op.FailureCode, processed)
		}
	}
	for _, wallet := range []string{heldWallet.WalletID, freeWallet.WalletID} {
		if got, want := balanceOf(t, owner, wallet), minor(t, "90.00"); got != want {
			t.Errorf("wallet %s holds %d minor units, want %d", wallet, got, want)
		}
	}
	reconciled(t, instances[2], heldWallet.WalletID, freeWallet.WalletID)
}

// blockedBy is how many backends PostgreSQL says are waiting on a lock the
// session named holds.
//
// pg_blocking_pids rather than a count of everything in wait_event_type 'Lock':
// the deployment has five processes and two of them poll, so a count of every
// waiting backend would be a count of whatever else was happening. This one
// names the blocker.
func blockedBy(t *testing.T, holder int) int {
	t.Helper()
	return countRows(t, owner,
		`SELECT count(*) FROM pg_stat_activity `+
			`WHERE datname = current_database() AND wait_event_type = 'Lock' `+
			`AND $1 = ANY(pg_blocking_pids(pid))`, holder)
}

// awaitBlockedBy refuses to go on until exactly the expected number of backends
// are queued behind the holding session.
func awaitBlockedBy(t *testing.T, holder, want int, what string) {
	t.Helper()
	eventually(t, settleBudget, what, func() error {
		if got := blockedBy(t, holder); got != want {
			return fmt.Errorf("%d backends are blocked by the holding session, want %d",
				got, want)
		}
		return nil
	})
}
