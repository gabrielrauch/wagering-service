//go:build integration

// The reference worker, on the two ends a parked operation can reach: the
// reference arrives, or the budget for waiting runs out.
package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// referenceSchedule is how long a parked operation waits between attempts in
// these two scenarios.
//
// Fast, because it is the application layer's schedule and has no business
// meaning — the point of keeping it apart from the domain's wait budget is
// exactly that tuning it cannot change whether an operation is eventually
// settled, which is what makes shortening it here legitimate rather than a
// thumb on the scale.
var referenceSchedule = app.BackoffPolicy{
	Initial: 200 * time.Millisecond, Factor: 2, Max: time.Second,
}

// TestARefundDeliveredBeforeItsBetIsCarriedForwardWhenTheBetArrives sends a
// reversal for an operation that has not been submitted yet.
//
// Out of order is the ordinary case on this path rather than a fault: a
// provider's two messages can be produced in either order, and the domain's
// answer is to park the reversal rather than refuse it. What is asserted is
// that the refund really did park — its attempt count says so, and a refund
// that arrived after its bet would never have one — and that the worker then
// carried it forward once the bet landed, leaving the wallet where it started
// and the reference resolved to the operation it names.
//
// Both messages go into one FIFO group, which is the wallet, so the order this
// scenario is about is the queue's guarantee rather than the test's hope.
func TestARefundDeliveredBeforeItsBetIsCarriedForwardWhenTheBetArrives(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-early-refund"
		bet        = "ext-bet-late"
		refund     = "ext-refund-early"
		round      = "round-shared"
		visibility = 5 * time.Second
	)
	// A budget nothing here reaches: this scenario is about the reference
	// arriving, and the one that is about the budget is below.
	s := newStack(t,
		withReferenceBudget(100, 2*time.Minute),
		withReferenceSchedule(referenceSchedule))
	wallet := s.openWallet(t, player, "100.00")
	name := inbound(t, visibility)

	queue := watch(openQueue(t, name))
	submitter := follow(s.wagering)
	consumer := startConsumer(t, s, queue, submitter, consumerSettings{name: consumerName})
	startReferenceWorker(t, s)

	put(t, name, body(t, message("msg-refund",
		operationOf("REFUND", refund, player, wallet, "25.00").against(bet).inRound(round))),
		wallet, "dedupe-refund")

	eventually(t, settleBudget, "the refund to be parked on the bet it names", func() error {
		op, ok := s.findOperation(t, refund)
		if !ok {
			return errors.New("the refund has not been recorded yet")
		}
		if op.status != wagering.PendingReference.String() {
			return fmt.Errorf("the refund is %s", op.status)
		}
		return nil
	})
	// Nothing moved while it waited, and the wallet is still whole.
	if got, want := s.balance(t, player), minor(t, "100.00"); got != want {
		t.Errorf("balance = %d minor units while the refund waited, want %d", got, want)
	}

	put(t, name, body(t, message("msg-bet",
		operationOf("BET", bet, player, wallet, "25.00").inRound(round))),
		wallet, "dedupe-bet")

	s.logs.await(t, logCarried, 1, settleBudget)
	eventually(t, settleBudget, "the refund to be carried forward", func() error {
		op, ok := s.findOperation(t, refund)
		if !ok {
			return errors.New("the refund has not been recorded yet")
		}
		if op.status != wagering.Processed.String() {
			return fmt.Errorf("the refund is %s", op.status)
		}
		return nil
	})

	finished(t, consumer)

	settled := s.operationRow(t, refund)
	staked := s.operationRow(t, bet)
	if settled.referenceAttempts < 1 {
		t.Errorf("the refund reports %d attempts at its reference: it never waited, so it did "+
			"not arrive first", settled.referenceAttempts)
	}
	if settled.resolvedReference == nil || *settled.resolvedReference != staked.id {
		t.Errorf("the refund resolved to %v, want the bet %s", settled.resolvedReference,
			staked.id)
	}
	if staked.status != wagering.Processed.String() {
		t.Errorf("the bet is %s, want %s", staked.status, wagering.Processed)
	}

	// The stake went out and came back: three entries, and a balance where it
	// started.
	if got, want := s.balance(t, player), minor(t, "100.00"); got != want {
		t.Errorf("balance = %d minor units, want %d", got, want)
	}
	if got := s.rowCount(t, "wallet_ledger_entry", "wallet_id = $1", wallet); got != 3 {
		t.Errorf("%d ledger entries, want 3 — the opening, the bet and the refund", got)
	}

	// Both messages are finished with. A parked operation is a settled message:
	// the consumer's work on it is done and the worker owns what happens next.
	if deleted := queue.messagesFor(queue.deleted()); len(deleted) != 2 {
		t.Errorf("deleted %v, want both the refund's message and the bet's", deleted)
	}
	if got := len(s.inboxRows(t)); got != 2 {
		t.Errorf("%d inbox rows, want 2", got)
	}
}

// TestARefundWhoseBetNeverArrivesIsRejectedWhenTheBudgetRunsOut is the other
// end of the same wait.
//
// The bet is never sent, so the refund is parked, looked at again, parked
// again, and finally settled — rejected for the reference it waited for, with a
// row and an event, because that is an outcome and not a failure. The worker
// sees it as an ordinary settled turn, which is what it is.
//
// The rejection is asserted to have taken at least the wait budget. Without
// that the test would also pass against a domain that refused the first attempt
// outright, which is the opposite of the behaviour: an operation that gives up
// immediately and one that waits its budget out reach the same row.
//
// The event is then followed onto the wire. A publisher runs beside the two
// workers, and once the outbox has drained the outbound queue is read back:
// exactly one WagerTransactionRejected for the refund's transaction, carrying
// REFERENCE_NOT_FOUND. The outbox row is what the application wrote; the
// message is what a subscriber gets, and a rejection that reached the table and
// not the queue would be an outcome nobody was told about.
func TestARefundWhoseBetNeverArrivesIsRejectedWhenTheBudgetRunsOut(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-lost-refund"
		missing    = "ext-bet-never-sent"
		refund     = "ext-refund-orphan"
		visibility = 5 * time.Second
		// The domain's wait budget. Short, because what is being asserted is
		// that it is enforced and not how long it is.
		ttl = 3 * time.Second
	)
	// The attempt count is generous so that the TTL is what ends the wait. The
	// two bounds are separate rules and only one of them is this scenario's.
	s := newStack(t,
		withReferenceBudget(100, ttl),
		withReferenceSchedule(referenceSchedule))
	wallet := s.openWallet(t, player, "100.00")
	name := inbound(t, visibility)
	events := outbound(t)

	queue := watch(openQueue(t, name))
	submitter := follow(s.wagering)
	consumer := startConsumer(t, s, queue, submitter, consumerSettings{name: consumerName})
	startReferenceWorker(t, s)
	startPublisher(t, s, openQueue(t, events),
		publisherSettings{name: "publisher-orphan", hold: 30 * time.Second})

	sent := time.Now()
	put(t, name, body(t, message("msg-orphan",
		operationOf("REFUND", refund, player, wallet, "25.00").against(missing))),
		wallet, "dedupe-orphan")

	eventually(t, settleBudget, "the refund to run out of budget", func() error {
		op, ok := s.findOperation(t, refund)
		if !ok {
			return errors.New("the refund has not been recorded yet")
		}
		if op.status != wagering.Rejected.String() {
			return fmt.Errorf("the refund is %s after %s", op.status, time.Since(sent))
		}
		return nil
	})
	if waited := time.Since(sent); waited < ttl {
		t.Errorf("the refund was rejected after %s, inside its %s wait budget: it gave up "+
			"rather than waiting", waited, ttl)
	}

	finished(t, consumer)

	settled := s.operationRow(t, refund)
	if settled.failureCode == nil || *settled.failureCode != failure.ReferenceNotFound.String() {
		t.Errorf("the refund is coded %v, want %s", settled.failureCode,
			failure.ReferenceNotFound)
	}
	if settled.referenceAttempts < 2 {
		t.Errorf("the refund reports %d attempts, want more than one: it was parked and looked "+
			"at again before it was given up on", settled.referenceAttempts)
	}
	if settled.resolvedReference != nil {
		t.Errorf("the refund resolved to %s, and the operation it named was never submitted",
			*settled.resolvedReference)
	}

	// Nothing moved, and the rejection is published like any other outcome: the
	// opening's two events plus the refund's parking and its rejection.
	if got, want := s.balance(t, player), minor(t, "100.00"); got != want {
		t.Errorf("balance = %d minor units, want %d untouched", got, want)
	}
	if got := s.rowCount(t, "wallet_ledger_entry", "wallet_id = $1", wallet); got != 1 {
		t.Errorf("%d ledger entries, want only the opening's", got)
	}
	if got := s.rowCount(t, "outbox", "event_type = $1", "WagerTransactionRejected"); got != 1 {
		t.Errorf("%d rejection events, want 1: a rejection is an outcome and is published",
			got)
	}

	// The message was deleted on the delivery that parked the operation. What
	// happened afterwards was the worker's, and the queue had no further part
	// in it.
	if deleted := queue.messagesFor(queue.deleted()); len(deleted) != 1 {
		t.Errorf("deleted %v, want the one message that carried the refund", deleted)
	}
	if calls := submitter.submissions(); len(calls) != 1 {
		t.Errorf("%d submissions, want 1: the worker resumes, it does not resubmit", len(calls))
	}

	empty(t, name, visibility+3*time.Second, "after the refund was given up on")

	// And the rejection reached the wire, once. The outbox holds the opening's
	// two events, one parking event per attempt and the rejection; every one
	// of them is published before the queue is read, so that the count below
	// is over the whole stream and not over whatever had gone out so far.
	pending := s.outboxRows(t)
	eventually(t, settleBudget, "every event to be published", func() error {
		if got := len(s.publishedEvents(t)); got != len(pending) {
			return fmt.Errorf("%d of %d events are marked published", got, len(pending))
		}
		return nil
	})
	rejections := 0
	for _, m := range awaitMessages(t, events, len(pending), settleBudget, "the published events") {
		var published struct {
			EventType string `json:"eventType"`
			Data      struct {
				TransactionID string `json:"transactionId"`
				FailureCode   string `json:"failureCode"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(m.body), &published); err != nil {
			t.Fatalf("read a published envelope: %v\n%s", err, m.body)
		}
		if published.Data.TransactionID != settled.id {
			continue
		}
		switch published.EventType {
		case "WagerTransactionRejected":
			rejections++
			if published.Data.FailureCode != failure.ReferenceNotFound.String() {
				t.Errorf("the rejection on the wire carries %q, want %s",
					published.Data.FailureCode, failure.ReferenceNotFound)
			}
		case "WagerTransactionPendingReference":
			// One per attempt, and the operation waited more than once.
		default:
			t.Errorf("the refund put a %s on the wire, and it was never processed",
				published.EventType)
		}
	}
	if rejections != 1 {
		t.Errorf("%d WagerTransactionRejected events for %s reached %s, want exactly 1",
			rejections, settled.id, events)
	}
}
