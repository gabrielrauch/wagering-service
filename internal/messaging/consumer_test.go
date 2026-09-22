//go:build integration

// What the consumer does with a message: applies it once, replays a
// redelivery, and hands to the redrive policy the two bodies nothing can
// decide about.
package messaging

import (
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// consumerName is the inbox consumer every scenario here runs under.
//
// One name across the suite, not because these tests share an inbox — each has
// a database of its own — but because the name is what makes a redelivery to a
// different replica a replay, and a suite that varied it per test would be
// demonstrating the opposite of the contract.
const consumerName = "wager-consumer"

// settleBudget is how long a scenario waits for a worker loop to reach an
// outcome.
//
// Generous, and deliberately not tuned down. Every wait here is for something
// that normally happens in milliseconds; the budget is the point at which the
// suite stops waiting and says what was still wrong, and making it tight buys
// nothing but flakes on a loaded machine.
const settleBudget = 30 * time.Second

// TestTheConsumerAppliesAnOperationAndDeletesTheMessage is the happy path,
// stated in the four tables a movement lands in and in the message no longer
// being on the queue.
//
// The delete is asserted PAST the visibility window and not inside it. A queue
// asked for messages while a delivery is still hidden answers empty whether the
// delete happened or not, so a probe inside the window passes against a Delete
// that does nothing — which is not hypothetical, it is the assertion the
// preceding task shipped. The consumer is stopped before the probe for the same
// family of reason: a consumer still running would receive the message that
// came back and delete it, and the probe would find the queue empty because of
// the second delete rather than the first.
func TestTheConsumerAppliesAnOperationAndDeletesTheMessage(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-applies"
		external   = "ext-applies"
		messageID  = "msg-applies"
		visibility = 2 * time.Second
	)
	s := newStack(t)
	wallet := s.openWallet(t, player, "100.00")
	name := inbound(t, visibility)

	queue := watch(openQueue(t, name))
	submitter := follow(s.wagering)
	consumer := startConsumer(t, s, queue, submitter, consumerSettings{name: consumerName})

	raw := body(t, message(messageID, operationOf("BET", external, player, wallet, "25.00")))
	sent := put(t, name, raw, wallet, "dedupe-"+messageID)

	s.logs.await(t, logApplied, 1, settleBudget)
	finished(t, consumer)

	// The movement, in every table it touches.
	if got, want := s.balance(t, player), minor(t, "75.00"); got != want {
		t.Errorf("balance = %d minor units, want %d", got, want)
	}
	op := s.operationRow(t, external)
	if op.status != wagering.Processed.String() {
		t.Errorf("the operation is %s, want %s", op.status, wagering.Processed)
	}
	if op.resultBalance == nil || *op.resultBalance != minor(t, "75.00") {
		t.Errorf("the operation reported balance %v, want %d", op.resultBalance,
			minor(t, "75.00"))
	}
	// Two entries: the opening's and the bet's. The opening is the wallet's
	// starting balance and is a movement like any other.
	if got := s.rowCount(t, "wallet_ledger_entry", "wallet_id = $1", wallet); got != 2 {
		t.Errorf("%d ledger entries, want 2", got)
	}
	// Four events: the opening and its balance change, then the bet's.
	if got := len(s.outboxRows(t)); got != 4 {
		t.Errorf("%d outbox rows, want 4", got)
	}
	inbox := s.inboxRows(t)
	if len(inbox) != 1 {
		t.Fatalf("%d inbox rows, want exactly one: %+v", len(inbox), inbox)
	}
	if inbox[0] != (inboxRow{consumer: consumerName, messageID: messageID, bodyHash: hashOf(raw)}) {
		t.Errorf("inbox row = %+v, want %s/%s fingerprinted as the body that arrived",
			inbox[0], consumerName, messageID)
	}

	// One delivery, one submission, one delete — and the delete for the message
	// that was delivered rather than for some handle.
	if deliveries := queue.delivered(sent); len(deliveries) != 1 {
		t.Errorf("%d deliveries of %s, want 1: %+v", len(deliveries), sent, deliveries)
	}
	if calls := submitter.submissions(); len(calls) != 1 || calls[0].result.IdempotentReplay {
		t.Errorf("submissions = %+v, want one that was not a replay", calls)
	}
	if deleted := queue.messagesFor(queue.deleted()); len(deleted) != 1 || deleted[0] != sent {
		t.Errorf("deleted %v, want exactly %s", deleted, sent)
	}

	empty(t, name, visibility+3*time.Second, "after the operation was applied")
}

// TestARedeliveredMessageIsReplayedFromTheInboxAndDeleted sends one envelope
// message id twice, under two deduplication ids so that the queue delivers both
// rather than collapsing them.
//
// What it proves is that the SECOND delivery happened and was answered as a
// replay. A test that only read the balance back would pass against a consumer
// that never saw the duplicate at all, which is the same test as no test: the
// interesting failure here is a second application, and the interesting
// evidence is that there was a second chance to make one.
func TestARedeliveredMessageIsReplayedFromTheInboxAndDeleted(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-replay"
		external   = "ext-replay"
		messageID  = "msg-replay"
		visibility = 2 * time.Second
	)
	s := newStack(t)
	wallet := s.openWallet(t, player, "100.00")
	name := inbound(t, visibility)

	queue := watch(openQueue(t, name))
	submitter := follow(s.wagering)
	consumer := startConsumer(t, s, queue, submitter, consumerSettings{name: consumerName})

	raw := body(t, message(messageID, operationOf("BET", external, player, wallet, "25.00")))
	first := put(t, name, raw, wallet, "dedupe-first")
	s.logs.await(t, logApplied, 1, settleBudget)

	// The same envelope, byte for byte, under a deduplication id the queue has
	// not seen. SQS would otherwise answer the second send with the first
	// message's id and nothing would be delivered again.
	second := put(t, name, raw, wallet, "dedupe-second")
	s.logs.await(t, logApplied, 2, settleBudget)
	finished(t, consumer)

	if first == second {
		t.Fatalf("the queue collapsed the two sends into one message %s; there was no "+
			"redelivery to replay", first)
	}
	if got := len(queue.delivered(second)); got != 1 {
		t.Errorf("%d deliveries of the duplicate, want 1", got)
	}

	// Two submissions, one of them answered from what was already recorded.
	calls := submitter.submissions()
	if len(calls) != 2 {
		t.Fatalf("%d submissions, want 2: %+v", len(calls), calls)
	}
	if calls[0].result.IdempotentReplay {
		t.Errorf("the first delivery was answered as a replay; nothing had been recorded yet")
	}
	if !calls[1].result.IdempotentReplay {
		t.Errorf("the duplicate was applied rather than replayed: %+v", calls[1].result)
	}
	if calls[0].result.TransactionID != calls[1].result.TransactionID {
		t.Errorf("the replay named operation %s, want the original's %s",
			calls[1].result.TransactionID, calls[0].result.TransactionID)
	}

	// One movement, whatever the queue did.
	if got, want := s.balance(t, player), minor(t, "75.00"); got != want {
		t.Errorf("balance = %d minor units, want %d — the duplicate moved money", got, want)
	}
	s.operationRow(t, external)
	if got := s.rowCount(t, "wallet_ledger_entry", "wallet_id = $1", wallet); got != 2 {
		t.Errorf("%d ledger entries, want 2 — the opening's and the bet's", got)
	}
	if got := len(s.inboxRows(t)); got != 1 {
		t.Errorf("%d inbox rows, want 1: one message handled once", got)
	}

	// Both messages deleted, the duplicate included: a replay is an outcome and
	// the message that carried it is finished with.
	if deleted := queue.messagesFor(queue.deleted()); len(deleted) != 2 {
		t.Errorf("deleted %v, want both %s and %s", deleted, first, second)
	}
	empty(t, name, visibility+3*time.Second, "after the duplicate was replayed")
}

// TestAMessageIdReusedForADifferentBodyReachesTheDeadLetterQueue sends one
// envelope message id twice with two different bodies.
//
// Nothing can decide which of them is real — the inbox holds one fingerprint
// per message id and the second body does not match it — and sending it again
// will not change that, so the consumer leaves the message alone and the
// redrive policy ends it. The assertion is that it ends on the dead-letter
// queue after the deployed number of deliveries, and not merely that it stopped
// appearing.
//
// The two bodies below differ in what they are for. The first says something
// different and is the one a provider would most regret having applied; the
// second says exactly the same thing in different bytes, and it is the case
// that isolates the mechanism. The inbox fingerprints the body AS IT ARRIVED
// rather than the fields it parsed to, so a re-serialisation is a different
// body — and it is the only one of the two that the idempotency key would let
// through as an ordinary replay if the inbox were not looking. Without it this
// test would pass against an inbox that had stopped comparing anything.
//
// The visibility timeout is a second rather than the deployed thirty. Five
// deliveries have to fit inside a test, and what is being asserted is the
// redrive policy's count rather than its timing.
func TestAMessageIdReusedForADifferentBodyReachesTheDeadLetterQueue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// forge renders the second body from the first.
		forge func(t *testing.T, player, external, messageID, wallet, honest string) string
	}{
		{
			name: "the second body says something different",
			forge: func(t *testing.T, player, external, messageID, wallet, _ string) string {
				return body(t, message(messageID,
					operationOf("BET", external, player, wallet, "90.00")))
			},
		},
		{
			name: "the second body says the same thing in different bytes",
			forge: func(t *testing.T, _, _, _, _, honest string) string {
				// One space after the opening brace: the same document, the
				// same fields, the same idempotency key and the same payload
				// hash — and a different fingerprint, because the inbox hashes
				// what arrived.
				return "{ " + honest[1:]
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			const (
				player     = "player-conflict"
				external   = "ext-conflict"
				messageID  = "msg-conflict"
				visibility = time.Second
			)
			s := newStack(t)
			wallet := s.openWallet(t, player, "100.00")
			source, dead := inboundPair(t, visibility)

			queue := watch(openQueue(t, source))
			submitter := follow(s.wagering)
			consumer := startConsumer(t, s, queue, submitter,
				consumerSettings{name: consumerName})

			honest := body(t, message(messageID,
				operationOf("BET", external, player, wallet, "25.00")))
			put(t, source, honest, wallet, "dedupe-honest")
			s.logs.await(t, logApplied, 1, settleBudget)

			forged := c.forge(t, player, external, messageID, wallet, honest)
			if forged == honest {
				t.Fatal("the two bodies are identical; there is no fingerprint conflict")
			}
			second := put(t, source, forged, wallet, "dedupe-forged")

			dropped := awaitMessages(t, dead, 1, settleBudget, "the reused message id")
			finished(t, consumer)
			if len(dropped) != 1 {
				t.Fatalf("%d messages on the dead-letter queue, want 1: %+v", len(dropped),
					dropped)
			}
			if dropped[0].body != forged {
				t.Errorf("the dead-letter queue holds %s, want the second body %s",
					dropped[0].body, forged)
			}

			// It got there by spending the redrive budget, not by being
			// deleted. The consumer leaves a poisoned message untouched
			// precisely so that the policy is what decides, and the delivery
			// count is what shows the policy decided.
			if got := len(queue.delivered(second)); got != maxReceives {
				t.Errorf("%d deliveries of the conflicting body, want the deployed limit of %d",
					got, maxReceives)
			}
			if deleted := queue.messagesFor(queue.deleted()); len(deleted) != 1 {
				t.Errorf("deleted %v, want only the first message", deleted)
			}

			// Nothing the second body said was applied, and the inbox still
			// carries the first body's fingerprint.
			if got, want := s.balance(t, player), minor(t, "75.00"); got != want {
				t.Errorf("balance = %d minor units, want %d", got, want)
			}
			inbox := s.inboxRows(t)
			if len(inbox) != 1 || inbox[0].bodyHash != hashOf(honest) {
				t.Errorf("inbox = %+v, want one row fingerprinting the body that was handled",
					inbox)
			}
			if got := s.rowCount(t, "wager_transaction",
				"external_transaction_id = $1", external); got != 1 {
				t.Errorf("%d wager transactions for %s, want 1", got, external)
			}

			empty(t, source, visibility+3*time.Second,
				"after the conflicting body was redriven")
		})
	}
}

// TestAnUnreadableMessageReachesTheDeadLetterQueueAfterTheRetryLimit is the
// other half of the same policy, for the failure that never reaches a
// transaction at all.
//
// A body this envelope cannot read is refused identically however often it is
// delivered, so the consumer neither deletes it nor pushes it back: deleting
// would discard an operation a provider believes it submitted, and releasing it
// would spend the whole redrive budget in one burst. What is asserted is that
// the message is on wager-transactions-dlq.fifo's stand-in after exactly the
// deployed number of deliveries, that nothing was submitted, and that the
// source queue is finished with it.
func TestAnUnreadableMessageReachesTheDeadLetterQueueAfterTheRetryLimit(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-unreadable"
		visibility = time.Second
	)
	s := newStack(t)
	wallet := s.openWallet(t, player, "100.00")
	source, dead := inboundPair(t, visibility)

	queue := watch(openQueue(t, source))
	submitter := follow(s.wagering)
	consumer := startConsumer(t, s, queue, submitter, consumerSettings{name: consumerName})

	// A well-formed JSON document naming a type this consumer does not handle.
	// Chosen over malformed bytes because it is the failure a producer actually
	// has — a queue shared with another message type, or an envelope that moved
	// on — and because it proves the refusal is the envelope's own rather than
	// the decoder giving up.
	unreadable := `{"messageId":"msg-unreadable","type":"SomethingElseEntirely",` +
		`"occurredAt":"2026-09-21T12:00:00Z","data":{"providerId":"provider-a",` +
		`"externalTransactionId":"ext-unreadable","idempotencyKey":"key-unreadable",` +
		`"playerId":"player-unreadable","walletId":"` + wallet + `","roundId":"round-1","gameId":"game-1",` +
		`"kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`
	sent := put(t, source, unreadable, wallet, "dedupe-unreadable")

	dropped := awaitMessages(t, dead, 1, settleBudget, "the unreadable message")
	finished(t, consumer)
	if len(dropped) != 1 || dropped[0].body != unreadable {
		t.Fatalf("the dead-letter queue holds %+v, want the one unreadable body", dropped)
	}
	if got := len(queue.delivered(sent)); got != maxReceives {
		t.Errorf("%d deliveries before the dead-letter queue, want the deployed limit of %d",
			got, maxReceives)
	}

	// It never reached a transaction, and the consumer never gave it back
	// early: a release would have spent the budget in one burst and written the
	// same line five times in a second.
	if calls := submitter.submissions(); len(calls) != 0 {
		t.Errorf("%d submissions for a message that could not be read: %+v", len(calls), calls)
	}
	if len(queue.deleted()) != 0 || len(queue.released()) != 0 || len(queue.hiddenCalls()) != 0 {
		t.Errorf("the consumer touched the message it was meant to leave alone: "+
			"%d deletes, %d releases, %d hides", len(queue.deleted()), len(queue.released()),
			len(queue.hiddenCalls()))
	}
	if got := len(s.inboxRows(t)); got != 0 {
		t.Errorf("%d inbox rows for a message no transaction ever claimed", got)
	}
	if got, want := s.balance(t, player), minor(t, "100.00"); got != want {
		t.Errorf("balance = %d minor units, want %d untouched", got, want)
	}

	empty(t, source, visibility+3*time.Second, "after the unreadable message was redriven")
}

// TestTheSpecificationsEnvelopeIsAppliedOnce sends the specification's own
// message shape — type WagerTransactionRequested, data.providerId and
// data.walletId, an occurredAt carrying milliseconds — twice, under two
// deduplication ids so that the queue delivers both. It is applied once and
// the second delivery is answered as a replay.
//
// The body is written out rather than built from this suite's [envelope]
// type, so that the spelling asserted is the specification's and not this
// suite's reading of it.
func TestTheSpecificationsEnvelopeIsAppliedOnce(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-specification"
		external   = "transaction-specification"
		messageID  = "msg-specification"
		visibility = 2 * time.Second
	)
	s := newStack(t)
	wallet := s.openWallet(t, player, "100.00")
	name := inbound(t, visibility)

	queue := watch(openQueue(t, name))
	submitter := follow(s.wagering)
	consumer := startConsumer(t, s, queue, submitter, consumerSettings{name: consumerName})

	raw := `{"messageId":"` + messageID + `","type":"WagerTransactionRequested",` +
		`"occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"` + provider + `",` +
		`"externalTransactionId":"` + external + `","idempotencyKey":"` + provider + `:` + external + `",` +
		`"playerId":"` + player + `","walletId":"` + wallet + `","roundId":"round-987",` +
		`"gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`
	first := put(t, name, raw, wallet, "dedupe-specification-first")
	s.logs.await(t, logApplied, 1, settleBudget)
	second := put(t, name, raw, wallet, "dedupe-specification-second")
	s.logs.await(t, logApplied, 2, settleBudget)
	finished(t, consumer)

	if first == second {
		t.Fatalf("the queue collapsed the two sends into one message %s", first)
	}
	calls := submitter.submissions()
	if len(calls) != 2 {
		t.Fatalf("%d submissions, want 2: %+v", len(calls), calls)
	}
	if calls[0].result.IdempotentReplay || !calls[1].result.IdempotentReplay {
		t.Errorf("replay flags = %v then %v, want false then true",
			calls[0].result.IdempotentReplay, calls[1].result.IdempotentReplay)
	}
	if got, want := s.balance(t, player), minor(t, "75.00"); got != want {
		t.Errorf("balance = %d minor units, want %d — the wallet the message named moved once", got, want)
	}
	op := s.operationRow(t, external)
	if op.status != wagering.Processed.String() {
		t.Errorf("the operation is %s, want %s", op.status, wagering.Processed)
	}
	if got := s.rowCount(t, "wallet_ledger_entry", "wallet_id = $1", wallet); got != 2 {
		t.Errorf("%d ledger entries, want 2 — the opening's and the bet's", got)
	}
	if got := len(s.inboxRows(t)); got != 1 {
		t.Errorf("%d inbox rows, want 1", got)
	}
	if deleted := queue.messagesFor(queue.deleted()); len(deleted) != 2 {
		t.Errorf("deleted %v, want both %s and %s", deleted, first, second)
	}
}

// TestTheSupersededEnvelopeSpellingIsUnreadable sends the message shape this
// consumer accepted before the specification was in the tree — type
// WagerTransactionSubmitted, data.provider, no walletId — and proves it is
// refused as unreadable rather than quietly applied: it reaches the
// dead-letter queue after the deployed number of deliveries and no submission
// was ever made for it.
func TestTheSupersededEnvelopeSpellingIsUnreadable(t *testing.T) {
	t.Parallel()

	const (
		player     = "player-superseded"
		visibility = time.Second
	)
	s := newStack(t)
	wallet := s.openWallet(t, player, "100.00")
	source, dead := inboundPair(t, visibility)

	queue := watch(openQueue(t, source))
	submitter := follow(s.wagering)
	consumer := startConsumer(t, s, queue, submitter, consumerSettings{name: consumerName})

	superseded := `{"messageId":"msg-superseded","type":"WagerTransactionSubmitted",` +
		`"occurredAt":"2026-09-21T12:00:00Z","data":{"provider":"` + provider + `",` +
		`"externalTransactionId":"ext-superseded","idempotencyKey":"key-superseded",` +
		`"playerId":"` + player + `","roundId":"round-1","gameId":"game-1",` +
		`"kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`
	sent := put(t, source, superseded, wallet, "dedupe-superseded")

	dropped := awaitMessages(t, dead, 1, settleBudget, "the superseded message")
	finished(t, consumer)
	if len(dropped) != 1 || dropped[0].body != superseded {
		t.Fatalf("the dead-letter queue holds %+v, want the one superseded body", dropped)
	}
	if got := len(queue.delivered(sent)); got != maxReceives {
		t.Errorf("%d deliveries before the dead-letter queue, want the deployed limit of %d",
			got, maxReceives)
	}
	if calls := submitter.submissions(); len(calls) != 0 {
		t.Errorf("%d submissions for a message in the superseded spelling: %+v", len(calls), calls)
	}
	if got, want := s.balance(t, player), minor(t, "100.00"); got != want {
		t.Errorf("balance = %d minor units, want %d untouched", got, want)
	}
}
