//go:build integration

// The two things that happen to a message nobody finished with: a transient
// failure hands it back for later, and a shutdown hands it back at once.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/workers"
)

// TestATransientFailureLeavesTheMessageVisibleAgain makes every attempt at one
// wallet fail for a reason that will pass, and watches what becomes of the
// message.
//
// Both failures below are the database's own and neither is injected into the
// worker: one is the wallet's row lock held by another movement, which is the
// first failure the consumer's own documentation names as worth retrying, and
// the other is the connection carrying the transaction being ended underneath
// it — a failover, a restart, an operator, all of which arrive here as exactly
// this. A manufactured error would prove that the consumer branches on a class,
// which its unit tests already prove, and not that a real outage is one.
//
// What is asserted is that the message came back SOONER than the queue would
// have produced unaided. The queue's visibility timeout is twenty seconds and
// the consumer's backoff is two, so three deliveries inside one visibility
// window cannot be the queue timing the message out — unaided it would need
// forty seconds to deliver three times, and the second delivery alone would
// have taken twenty. That comparison is the only thing separating "the consumer
// handed the message back" from "the consumer did nothing and the message
// returned anyway", and a suite without it would pass against a
// ChangeVisibility that was never called.
//
// Twenty rather than the ten it was first written with, and the margin is the
// reason. The row-lock case pays its one-second lock timeout on each of three
// attempts, so its span is about 7.9 seconds and inflates by a fifth when the
// whole tree's integration run is in flight — 2.1 seconds of headroom under a
// ten-second ceiling, which is the one place in this suite where a slower
// machine reddens a correct test. Raising the ceiling STRENGTHENS the claim
// rather than loosening it: the bound a mutant has to beat goes up with it.
//
// Each case also asserts WHICH failure it produced, by the SQLSTATE that
// reached the consumer. Without that the two are indistinguishable — a
// termination that never landed would leave the transaction waiting on the lock
// and fail transiently anyway, and the case would pass having tested its
// neighbour.
func TestATransientFailureLeavesTheMessageVisibleAgain(t *testing.T) {
	t.Parallel()

	const (
		// Every timing assertion below is stated against this: a redelivery
		// sooner than this is one the consumer asked for.
		visibility = 20 * time.Second
		// The consumer's own backoff, in whole seconds because that is the
		// resolution a visibility timeout has.
		backoff = 2 * time.Second
		// How many deliveries are watched. Three rather than two, so that the
		// span covers two redeliveries and no single coincidence of timing can
		// account for it.
		wanted = 3
	)

	cases := []struct {
		name string
		// lockTimeout is how long the service waits for the wallet's row lock.
		// It is what ends the attempt in the first case and what must NOT end
		// it in the second.
		lockTimeout time.Duration
		// obstruct installs the failure and returns the call that removes it.
		obstruct func(t *testing.T, s *stack, wallet string) func()
		// expected reports whether an attempt failed the way this case meant
		// it to.
		expected func(err error) bool
	}{
		{
			name:        "the wallet's row lock is held by another movement",
			lockTimeout: time.Second,
			obstruct: func(t *testing.T, s *stack, wallet string) func() {
				return holdWallet(t, s, wallet)
			},
			expected: refusedWith(pgerrcode.LockNotAvailable),
		},
		{
			name: "the connection carrying the transaction is ended",
			// The default, so that the lock wait outlasts everything and the
			// termination below is what actually ends each attempt.
			lockTimeout: stackLockTimeout,
			obstruct: func(t *testing.T, s *stack, wallet string) func() {
				held := holdWallet(t, s, wallet)
				ended := endBlockedConnections(t, s)
				return func() {
					ended()
					held()
				}
			},
			expected: lostConnection,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			const (
				player    = "player-transient"
				external  = "ext-transient"
				messageID = "msg-transient"
			)
			s := newStack(t, withLockTimeout(c.lockTimeout))
			wallet := s.openWallet(t, player, "100.00")
			// No dead-letter queue behind it: this message has to survive as
			// many deliveries as the scenario watches, and the redrive policy
			// is another scenario's subject.
			name := inbound(t, visibility)

			queue := watch(openQueue(t, name))
			submitter := follow(s.wagering)
			clear := c.obstruct(t, s, wallet)

			raw := body(t, message(messageID, operationOf("BET", external, player, wallet, "25.00")))
			sent := put(t, name, raw, wallet, "dedupe-"+messageID)
			consumer := startConsumer(t, s, queue, submitter, consumerSettings{
				name:    consumerName,
				backoff: workers.Backoff{Initial: backoff, Factor: 1, Max: backoff},
			})

			s.logs.await(t, logDeferred, wanted, settleBudget)
			// The line is written before the message is handed back, so the
			// call has to be waited for separately or this reads the queue one
			// visibility change short of what it just watched happen.
			eventually(t, settleBudget, "the deferred messages to be handed back", func() error {
				if got := len(queue.hiddenCalls()); got < wanted {
					return fmt.Errorf("%d visibility changes for %d deferrals", got, wanted)
				}
				return nil
			})

			deliveries := queue.delivered(sent)
			if len(deliveries) < wanted {
				t.Fatalf("%d deliveries, want at least %d: %+v", len(deliveries), wanted,
					deliveries)
			}
			// Reported on the way past, not only on failure. This is the one
			// upper bound in the suite, and the margin it is passing by is
			// what somebody moving these numbers needs to see.
			span := deliveries[wanted-1].at.Sub(deliveries[0].at)
			t.Logf("%d deliveries spanned %s against a %s visibility timeout", wanted, span,
				visibility)
			if span >= visibility {
				t.Errorf("%d deliveries took %s, which the queue's own %s visibility timeout "+
					"could have produced unaided — the backoff proved nothing", wanted, span,
					visibility)
			}
			for i, d := range deliveries[:wanted] {
				if d.receiveCount != i+1 {
					t.Errorf("delivery %d reports receive count %d, want %d", i+1,
						d.receiveCount, i+1)
				}
			}

			// It was handed back rather than deleted or released, and for the
			// backoff it was configured with.
			for _, call := range queue.hiddenCalls() {
				if call.in != backoff {
					t.Errorf("a message was hidden for %s, want the configured backoff of %s",
						call.in, backoff)
				}
			}
			if len(queue.deleted()) != 0 || len(queue.released()) != 0 {
				t.Errorf("the consumer deleted %d and released %d messages it had not applied",
					len(queue.deleted()), len(queue.released()))
			}

			// The failure really was the one this case engineered, and it was
			// classified as one worth another delivery.
			calls := submitter.submissions()
			if len(calls) < wanted {
				t.Fatalf("%d submissions, want at least %d", len(calls), wanted)
			}
			for i, call := range calls[:wanted] {
				if class := app.ClassOf(call.err); class != app.Retryable {
					t.Errorf("submission %d failed as %s (%v), want %s", i+1, class, call.err,
						app.Retryable)
				}
				if !c.expected(call.err) {
					t.Errorf("submission %d failed with %v, which is not the failure this case "+
						"arranged", i+1, call.err)
				}
			}

			// Take the obstruction away. The same message, on its next
			// delivery, is applied — which is what makes the failure transient
			// rather than fatal.
			clear()
			s.logs.await(t, logApplied, 1, settleBudget)
			finished(t, consumer)

			if got, want := s.balance(t, player), minor(t, "75.00"); got != want {
				t.Errorf("balance = %d minor units, want %d", got, want)
			}
			op := s.operationRow(t, external)
			if op.status != wagering.Processed.String() {
				t.Errorf("the operation is %s, want %s", op.status, wagering.Processed)
			}
			inbox := s.inboxRows(t)
			if len(inbox) != 1 || inbox[0].messageID != messageID {
				t.Errorf("inbox = %+v, want one row for %s: the deferred deliveries recorded "+
					"nothing", inbox, messageID)
			}
			if deleted := queue.messagesFor(queue.deleted()); len(deleted) != 1 ||
				deleted[0] != sent {
				t.Errorf("deleted %v, want exactly %s once it had been applied", deleted, sent)
			}
		})
	}
}

// refusedWith reports whether a failure carries one particular SQLSTATE.
func refusedWith(code string) func(error) bool {
	return func(err error) bool {
		pgErr, ok := errors.AsType[*pgconn.PgError](err)
		return ok && pgErr.Code == code
	}
}

// connectionExceptionClass is SQLSTATE class 08, every member of which is the
// connection failing rather than the statement being wrong. Restated here
// because the adapter's copy is unexported.
const connectionExceptionClass = "08"

// lostConnection reports whether a failure is the connection going away rather
// than a statement being refused.
//
// Two shapes reach here and both are the same event. The server may get its
// FATAL out first, which arrives as admin_shutdown or somewhere in class 08; or
// the socket may simply end, which pgx reports with no SQLSTATE at all — as an
// unexpected EOF, as a net.OpError on the read, or as a use of a closed
// connection.
//
// Those three are named rather than answered with "anything that is not a
// SQLSTATE", which is what this used to do and which is not a predicate at all:
// it accepted a pool error and a bare deadline just as readily, so the only
// thing it actually rejected was the neighbouring case's lock_not_available.
// The point of this function is to say which of the two failures the table
// arranged actually happened, and a predicate that accepts everything else
// cannot.
func lostConnection(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == pgerrcode.AdminShutdown ||
			strings.HasPrefix(pgErr.Code, connectionExceptionClass)
	}
	var opErr *net.OpError
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.As(err, &opErr)
}

// endBlockedConnections keeps ending the connections that are waiting on a lock
// in this test's database, and returns the call that stops doing so.
//
// It targets waiting backends rather than every backend, so that the
// transaction holding the lock — this test's own — survives to keep holding it.
// The sweep repeats because each attempt the consumer makes opens a new
// connection, and the scenario needs several attempts to fail the same way.
//
// It is blunt within its own database: every backend waiting on a lock there is
// ended, not only the consumer's. Nothing else is waiting on one, and the
// datname predicate is what keeps the bluntness off the tests running beside
// this one — each of which has a database of its own.
func endBlockedConnections(t *testing.T, s *stack) func() {
	t.Helper()
	database := databaseOf(t, s.dsn)
	done := make(chan struct{})
	swept := make(chan struct{})

	go func() {
		defer close(swept)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity `+
				`WHERE datname = $1 AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()`,
				database)
			cancel()
			select {
			case <-done:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()

	return sync.OnceFunc(func() {
		close(done)
		<-swept
	})
}

// TestAGracefulShutdownReleasesAMessageStillInFlight stops a consumer while one
// message is in the middle of being applied.
//
// The message is held there deliberately: this test takes the wallet's row lock
// first, so the submission parks on it exactly as it would behind another
// movement and is still in hand when the drain deadline passes. What the
// consumer then owes the queue is the message back AT ONCE — not at the end of
// a visibility timeout that exists for the case where nobody gave it back.
//
// The queue's timeout is the deployed thirty seconds, and it has to be: the
// whole claim is that a clean shutdown does not look like a crash to the queue,
// and a fixture with a two-second timeout would make the two
// indistinguishable.
func TestAGracefulShutdownReleasesAMessageStillInFlight(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-shutdown"
		external   = "ext-shutdown"
		messageID  = "msg-shutdown"
		visibility = 30 * time.Second
		drain      = time.Second
		// How long the released message is given to come back. Far below the
		// visibility timeout above, which is the point.
		promptly = 10 * time.Second
	)
	s := newStack(t)
	wallet := s.openWallet(t, player, "100.00")
	name := inbound(t, visibility)

	queue := watch(openQueue(t, name))
	submitter := follow(s.wagering)

	release := holdWallet(t, s, wallet)

	raw := body(t, message(messageID, operationOf("BET", external, player, wallet, "25.00")))
	sent := put(t, name, raw, wallet, "dedupe-"+messageID)
	consumer := startConsumer(t, s, queue, submitter,
		consumerSettings{name: consumerName, drain: drain})

	// In the consumer's hands. The submission that follows the delivery is
	// parked on the lock this test holds, and will be there when the drain
	// deadline passes a second later.
	eventually(t, settleBudget, "the message to reach the consumer", func() error {
		if len(queue.delivered(sent)) == 0 {
			return errors.New("nothing has been delivered")
		}
		return nil
	})

	stopped := time.Now()
	err := consumer.Stop(context.Background())
	if err == nil {
		t.Fatal("the drain deadline passed with a message in flight and Stop reported success")
	}
	if released := queue.messagesFor(queue.released()); len(released) != 1 ||
		released[0] != sent {
		t.Fatalf("released %v, want exactly %s: %v", released, sent, err)
	}
	release()

	back := awaitMessages(t, name, 1, promptly, "the message the shutdown gave back")
	if elapsed := time.Since(stopped); elapsed >= visibility {
		t.Errorf("the message came back %s after the shutdown, which the queue's own %s "+
			"visibility timeout would have produced unaided", elapsed, visibility)
	}
	if back[0].body != raw {
		t.Errorf("the queue holds %s, want the message that was in flight", back[0].body)
	}
	if back[0].receiveCount != 2 {
		t.Errorf("the redelivery reports receive count %d, want 2 — a second delivery of the "+
			"same message rather than a different one", back[0].receiveCount)
	}

	// The transaction the deadline cancelled rolled back whole: no operation,
	// no inbox row claiming the message was handled, no movement.
	if got := s.rowCount(t, "wager_transaction", "external_transaction_id = $1", external); got != 0 {
		t.Errorf("%d wager transactions for an operation that never committed", got)
	}
	if got := len(s.inboxRows(t)); got != 0 {
		t.Errorf("%d inbox rows for a message that was given back unhandled", got)
	}
	if got, want := s.balance(t, player), minor(t, "100.00"); got != want {
		t.Errorf("balance = %d minor units, want %d untouched", got, want)
	}
}

// holdWallet takes the row lock a movement on this wallet needs, and returns
// the call that gives it back.
//
// FOR NO KEY UPDATE is the mode the adapter's own LockForMovement takes, so a
// submission queues behind this exactly as it queues behind another movement —
// which is the point: the delay is the service's own contention rather than
// something arranged around it.
//
// The transaction runs on a context of its own rather than the test's, because
// it has to outlive a shutdown the test is watching and t.Context ends with the
// test.
func holdWallet(t *testing.T, s *stack, wallet string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	tx, err := s.owner.Begin(ctx)
	if err != nil {
		cancel()
		t.Fatalf("begin a transaction to hold %s: %v", wallet, err)
	}
	give := sync.OnceFunc(func() {
		_ = tx.Rollback(ctx)
		cancel()
	})
	t.Cleanup(give)

	var held string
	if err := tx.QueryRow(ctx,
		`SELECT id::text FROM wagering.wallet WHERE id = $1 FOR NO KEY UPDATE`,
		wallet).Scan(&held); err != nil {
		give()
		t.Fatalf("hold the wallet %s: %v", wallet, err)
	}
	return give
}
