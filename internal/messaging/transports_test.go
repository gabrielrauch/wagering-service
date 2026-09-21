//go:build integration

// One operation, submitted over both transports.
package messaging

import (
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestOneOperationOverBothTransportsMovesMoneyOnceAndReplaysOnce sends one
// submission down both doors, in both orders.
//
// The two transports are deliberately different in exactly one way that
// matters: the queue carries an inbox identity and the application's own door
// carries none. Everything else — the provider, the external id, the
// idempotency key and the payload the hash is taken over — is the same
// submission, because that is the condition under which the idempotency
// decision has to hold. Whichever arrives second is answered from what the
// first recorded, and the wallet moves once.
//
// Both orders, because they exercise different code. The transport that arrives
// first takes the wallet lock and writes; the one that arrives second is
// answered by the replay lookup before the lock is ever reached, and only the
// queue's side of that also writes an inbox row.
func TestOneOperationOverBothTransportsMovesMoneyOnceAndReplaysOnce(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// queueFirst says which transport arrives first.
		queueFirst bool
	}{
		{name: "the queue arrives first", queueFirst: true},
		{name: "the application's own door arrives first", queueFirst: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			const (
				player     = "player-both"
				external   = "ext-both"
				messageID  = "msg-both"
				visibility = 2 * time.Second
			)
			s := newStack(t)
			wallet := s.openWallet(t, player, "100.00")
			name := inbound(t, visibility)

			queue := watch(openQueue(t, name))
			submitter := follow(s.wagering)
			consumer := startConsumer(t, s, queue, submitter,
				consumerSettings{name: consumerName})

			op := operationOf("BET", external, player, "25.00")
			var direct app.OperationResult
			if c.queueFirst {
				put(t, name, body(t, message(messageID, op)), wallet, "dedupe-"+messageID)
				s.logs.await(t, logApplied, 1, settleBudget)
				direct = s.submitDirectly(t, op, "correlation-direct")
			} else {
				direct = s.submitDirectly(t, op, "correlation-direct")
				put(t, name, body(t, message(messageID, op)), wallet, "dedupe-"+messageID)
				s.logs.await(t, logApplied, 1, settleBudget)
			}

			finished(t, consumer)

			calls := submitter.submissions()
			if len(calls) != 1 {
				t.Fatalf("%d submissions reached the consumer, want 1: %+v", len(calls), calls)
			}
			queued := calls[0].result

			// Exactly one of the two was a replay, and it was the one that
			// arrived second.
			first, second := queued, direct
			if !c.queueFirst {
				first, second = direct, queued
			}
			if first.IdempotentReplay {
				t.Errorf("the transport that arrived first was answered as a replay: %+v", first)
			}
			if !second.IdempotentReplay {
				t.Errorf("the transport that arrived second applied the operation again "+
					"rather than replaying it: %+v", second)
			}
			// Both answered for the same operation, which is what makes the
			// replay a replay rather than a second operation that happens to
			// look alike.
			if first.TransactionID != second.TransactionID {
				t.Errorf("the two transports named operations %s and %s, want one",
					first.TransactionID, second.TransactionID)
			}
			if first.Status != wagering.Processed || second.Status != wagering.Processed {
				t.Errorf("statuses %s and %s, want both %s", first.Status, second.Status,
					wagering.Processed)
			}

			// One movement, in every table that records one.
			if got, want := s.balance(t, player), minor(t, "75.00"); got != want {
				t.Errorf("balance = %d minor units, want %d: the operation moved money twice",
					got, want)
			}
			stored := s.operationRow(t, external)
			if stored.id != first.TransactionID.String() {
				t.Errorf("the stored operation is %s, want %s", stored.id, first.TransactionID)
			}
			if got := s.rowCount(t, "wallet_ledger_entry", "wallet_id = $1", wallet); got != 2 {
				t.Errorf("%d ledger entries, want 2 — the opening's and the bet's", got)
			}
			if got := len(s.outboxRows(t)); got != 4 {
				t.Errorf("%d outbox rows, want 4: a replay publishes nothing", got)
			}

			// The inbox is the queue's alone. The other transport has no message
			// to record, which is exactly why the two cannot be one mechanism.
			inbox := s.inboxRows(t)
			if len(inbox) != 1 || inbox[0].messageID != messageID {
				t.Errorf("inbox = %+v, want one row for %s", inbox, messageID)
			}

			empty(t, name, visibility+3*time.Second, "after both transports had submitted")
		})
	}
}
