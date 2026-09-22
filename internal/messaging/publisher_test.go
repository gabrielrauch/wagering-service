//go:build integration

// What the outbox publisher puts on the wire, and what two of them do to each
// other while doing it.
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
)

// TestTwoPublishersNeverPutOneEventOnTheQueueTwice runs two publishers over one
// outbox and counts what each of them offered to the queue.
//
// Counting what was OFFERED rather than what arrived is the whole of the
// design. The outbound queue deduplicates on the event id, so a second send of
// one event is answered with the message SQS already has and leaves no trace on
// the queue at all — which means a suite that drained the queue would report a
// clean run against two publishers doing everything twice. A middleware on each
// publisher's own client is the only place the second send is visible, and it
// is a middleware rather than a stand-in: the call still goes to LocalStack and
// the answer still comes back from it.
//
// The contention is real and is arranged to be, in three ways. The outbox is
// head-of-line per wallet, so a backlog on ONE wallet would serialise the two
// publishers by construction and prove nothing — hence twenty wallets, which
// make twenty rows claimable at once. The batch is two rather than the default
// ten, so draining them takes twenty turns instead of four and the two
// publishers are claiming from the same candidate set for the whole of it;
// SKIP LOCKED is what then keeps them off each other. And both queues are
// resolved before either publisher starts, so neither gets a head start worth
// the name.
//
// Both publishers are asserted to have done some of the work, because a run
// where one of them did all of it would be a test of one publisher wearing two
// names.
func TestTwoPublishersNeverPutOneEventOnTheQueueTwice(t *testing.T) {
	t.Parallel()

	// Twenty funded wallets, each of which records an opening and the balance
	// change it caused: forty events on twenty aggregates.
	const wallets = 20
	s := newStack(t)
	for i := range wallets {
		s.openWallet(t, fmt.Sprintf("player-contended-%02d", i), "100.00")
	}
	pending := s.outboxRows(t)
	if len(pending) != 2*wallets {
		t.Fatalf("%d outbox rows, want %d", len(pending), 2*wallets)
	}

	name := outbound(t)
	var one, two wire
	// Both resolved first, then both started, against an outbox that is already
	// full: the first claim each of them makes is made in the same instant as
	// the other's.
	queueOne := openQueueOn(t, name, recordSends(t, &one))
	queueTwo := openQueueOn(t, name, recordSends(t, &two))
	const contendedBatch = 2
	startPublisher(t, s, queueOne,
		publisherSettings{name: "publisher-one", hold: 30 * time.Second, batch: contendedBatch})
	startPublisher(t, s, queueTwo,
		publisherSettings{name: "publisher-two", hold: 30 * time.Second, batch: contendedBatch})

	eventually(t, settleBudget, "every event to be published", func() error {
		if got := len(s.publishedEvents(t)); got != len(pending) {
			return fmt.Errorf("%d of %d events are marked published", got, len(pending))
		}
		return nil
	})

	for _, row := range pending {
		if got := one.count(row.eventID) + two.count(row.eventID); got != 1 {
			t.Errorf("event %s (%s, sequence %d) was offered to the queue %d times, want once",
				row.eventID, row.eventType, row.sequence, got)
		}
	}
	if one.total() == 0 || two.total() == 0 {
		t.Errorf("publisher-one sent %d events and publisher-two sent %d: one of them did all "+
			"the work, so nothing here contended", one.total(), two.total())
	}
	if got := one.total() + two.total(); got != len(pending) {
		t.Errorf("%d events reached the queue in total, want %d", got, len(pending))
	}

	// Every event is on the queue exactly once, which is the same claim read
	// from the other end.
	published := awaitMessages(t, name, len(pending), settleBudget, "the published events")
	if len(published) != len(pending) {
		t.Errorf("%d messages on %s, want %d", len(published), name, len(pending))
	}
}

// TestAnExpiredClaimIsTakenUpByAnotherPublisher leaves a claim standing with
// nothing on the wire, which is what a publisher killed between taking work and
// sending it leaves behind.
//
// Two things are asserted and the second is the one that makes the first mean
// anything. The event IS eventually published by the publisher that is still
// running — and it is published no sooner than the hold, because a publisher
// that ignored the claim rather than waiting for it to expire would also
// eventually publish, and the two are indistinguishable without the clock.
//
// The claim is taken through the adapter the publisher itself claims with, so
// what is arranged is a publisher's claim and not an approximation of one.
func TestAnExpiredClaimIsTakenUpByAnotherPublisher(t *testing.T) {
	t.Parallel()

	// Long enough that a publisher polling ten times a second has unmistakably
	// declined to take the row, short enough not to dominate the suite.
	const hold = 3 * time.Second

	s := newStack(t)
	s.openWallet(t, "player-stalled", "100.00")
	pending := s.outboxRows(t)
	if len(pending) != 2 {
		t.Fatalf("%d outbox rows, want 2", len(pending))
	}

	taken := time.Now().UTC()
	stalled, err := s.claims.Claim(t.Context(), postgres.ClaimRequest{
		By: "publisher-stalled", Limit: 10, At: taken, Hold: hold,
	})
	if err != nil {
		t.Fatalf("take a claim that will be abandoned: %v", err)
	}
	// One, not two: the outbox is head-of-line per wallet, so this wallet's
	// second event is not claimable while its first is unpublished. That is why
	// an abandoned claim is worth recovering at all — it holds up a stream and
	// not just a row.
	if len(stalled) != 1 {
		t.Fatalf("%d events claimed, want the one at the head of the wallet's stream",
			len(stalled))
	}
	held := stalled[0].EventID.String()

	name := outbound(t)
	var sent wire
	startPublisher(t, s, openQueueOn(t, name, recordSends(t, &sent)),
		publisherSettings{name: "publisher-live", hold: 30 * time.Second})

	eventually(t, settleBudget, "the abandoned claim to be taken up and published", func() error {
		published := s.publishedEvents(t)
		if !published[held] {
			return fmt.Errorf("event %s is still unpublished", held)
		}
		return nil
	})
	if waited := time.Since(taken); waited < hold {
		t.Errorf("the event was published %s after the claim was taken, inside its %s hold: "+
			"the claim was passed over rather than waited out", waited, hold)
	}
	if got := sent.count(held); got != 1 {
		t.Errorf("the recovered event was offered to the queue %d times, want once", got)
	}

	// The row itself says the recovery happened: it was claimed twice, once by
	// the publisher that stopped and once by the one that took over.
	for _, row := range s.outboxRows(t) {
		if row.eventID != held {
			continue
		}
		if row.attempts != 2 {
			t.Errorf("the recovered event reports %d attempts, want 2 — one claim abandoned "+
				"and one that published it", row.attempts)
		}
		if row.claimedBy != nil {
			t.Errorf("the published event still names %q as its holder", *row.claimedBy)
		}
	}

	// And the stream behind it moved: the wallet's second event was blocked by
	// the abandoned claim and is published once the first one is.
	eventually(t, settleBudget, "the rest of the wallet's stream to follow", func() error {
		if got := len(s.publishedEvents(t)); got != len(pending) {
			return fmt.Errorf("%d of %d events are marked published", got, len(pending))
		}
		return nil
	})
}

// TestPublishedEventsKeepTheirEventIdAcrossRepublication reads the published
// envelope back off the queue and then has it published again.
//
// The event id is the whole of what makes the second publication safe: it is
// the deduplication id SQS is given, so a publisher that sent an event and died
// before marking the row puts no second copy on the queue when it comes back.
// That state — on the wire, unmarked — is reproduced here by putting the rows
// back. The other way into it is killing the process at the fault point the
// production path marks, faults.AfterPublishBeforeMark, which is what
// internal/multi/publishers_test.go does with a real binary; this suite holds
// the same guarantee without a process to kill, on the adapter's own path.
func TestPublishedEventsKeepTheirEventIdAcrossRepublication(t *testing.T) {
	t.Parallel()

	s := newStack(t)
	wallet := s.openWallet(t, "player-eventid", "100.00")
	pending := s.outboxRows(t)
	if len(pending) != 2 {
		t.Fatalf("%d outbox rows, want 2", len(pending))
	}

	name := outbound(t)
	var sent wire
	publisher := startPublisher(t, s, openQueueOn(t, name, recordSends(t, &sent)),
		publisherSettings{name: "publisher-eventid", hold: 30 * time.Second})

	eventually(t, settleBudget, "the wallet's events to be published", func() error {
		if got := len(s.publishedEvents(t)); got != len(pending) {
			return fmt.Errorf("%d of %d events are marked published", got, len(pending))
		}
		return nil
	})

	first := awaitMessages(t, name, len(pending), settleBudget, "the published events")
	byEvent := map[string]seen{}
	for _, m := range first {
		var published struct {
			EventID       string `json:"eventId"`
			AggregateID   string `json:"aggregateId"`
			AggregateType string `json:"aggregateType"`
		}
		if err := json.Unmarshal([]byte(m.body), &published); err != nil {
			t.Fatalf("read a published envelope: %v\n%s", err, m.body)
		}
		// The three halves of the outbound contract: the body's own identity,
		// the deduplication id that makes a republication the same event, and
		// the group that orders a wallet's stream.
		if m.deduplicationID != published.EventID {
			t.Errorf("the message was deduplicated on %q and its envelope says %q",
				m.deduplicationID, published.EventID)
		}
		if m.groupID != published.AggregateID || m.groupID != wallet {
			t.Errorf("the message is grouped under %q, the envelope names aggregate %q, and "+
				"the wallet is %q — all three should be one", m.groupID, published.AggregateID,
				wallet)
		}
		byEvent[published.EventID] = m
	}
	// Given straight back, so that the second read below is waiting for the
	// republication rather than for this probe's own visibility timeout.
	unhide(t, name, first)
	for _, row := range pending {
		if _, ok := byEvent[row.eventID]; !ok {
			t.Errorf("event %s is marked published and no message on %s carries its id",
				row.eventID, name)
		}
	}

	// Stopped before the rows are put back, so that the republication below is
	// a publisher starting on work it believes is outstanding rather than a
	// race with the one that just finished it.
	if err := publisher.Stop(context.Background()); err != nil {
		t.Fatalf("stop the publisher: %v", err)
	}

	// The state a publisher killed between the send and the mark leaves: on the
	// wire, and no row says so.
	//
	// One notch off, and worth naming. A publisher that died holding the row
	// leaves claimed_by set and claim_expires_at in the past — a claim that has
	// expired — where this leaves both NULL, which is a row that was never
	// claimed. Both are claimable by the next turn and the scenario turns on
	// what the send does with the event id rather than on who held it; the
	// claim's own expiry is asserted by TestAnExpiredClaimIsTakenUpByAnother-
	// Publisher, which arranges it through the adapter rather than by hand.
	s.exec(t, `UPDATE wagering.outbox SET published_at = NULL, claimed_by = NULL, `+
		`claimed_at = NULL, claim_expires_at = NULL, next_attempt_at = $1`, time.Now().UTC())

	var again wire
	startPublisher(t, s, openQueueOn(t, name, recordSends(t, &again)),
		publisherSettings{name: "publisher-recovered", hold: 30 * time.Second})
	eventually(t, settleBudget, "the events to be published a second time", func() error {
		if got := len(s.publishedEvents(t)); got != len(pending) {
			return fmt.Errorf("%d of %d events are marked published again", got, len(pending))
		}
		return nil
	})
	for _, row := range pending {
		if got := again.count(row.eventID); got != 1 {
			t.Errorf("event %s was offered %d times on the second pass, want once", row.eventID,
				got)
		}
	}

	// The queue still holds one message per event, and they are the SAME
	// messages: the deduplication id being the event id is what makes the
	// second send the first message rather than a second one.
	second := awaitMessages(t, name, len(pending), settleBudget, "the republished events")
	if len(second) != len(pending) {
		t.Fatalf("%d messages on %s after republication, want %d", len(second), name,
			len(pending))
	}
	for _, m := range second {
		before, ok := byEvent[m.deduplicationID]
		if !ok {
			t.Errorf("a message deduplicated on %q appeared that was not there before",
				m.deduplicationID)
			continue
		}
		if m.messageID != before.messageID {
			t.Errorf("event %s is now message %s and was message %s: the republication put a "+
				"second copy on the queue", m.deduplicationID, m.messageID, before.messageID)
		}
	}
}

// wire is what a publisher OFFERED to SQS, recorded by a middleware on the real
// SDK client.
//
// A middleware rather than a stand-in for the queue: the call still goes to
// LocalStack and the answer still comes back from it. What is added is a count
// per deduplication id — which for everything this service publishes is the
// event id — and that count is the one thing the queue itself cannot be asked
// about, because deduplication is exactly the mechanism that hides a second
// send.
//
// Offered, not sent, and the distinction is deliberate: the middleware sits at
// the Initialize step, which is above the SDK's retry loop, so a call the SDK
// retried is counted once. That is the right level for the property being
// asserted — whether the PUBLISHER decided to send an event twice — and it
// would be the wrong level for a question about packets.
type wire struct {
	mu      sync.Mutex
	offered map[string]int
	sent    int
}

func (w *wire) record(entries []types.SendMessageBatchRequestEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.offered == nil {
		w.offered = map[string]int{}
	}
	for _, entry := range entries {
		if entry.MessageDeduplicationId == nil {
			continue
		}
		w.offered[*entry.MessageDeduplicationId]++
		w.sent++
	}
}

// count reports how many times one deduplication id was offered to the queue.
func (w *wire) count(id string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offered[id]
}

// total reports how many entries this client offered in all.
func (w *wire) total() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sent
}

// recordSends builds a client that records every batch entry it carries.
func recordSends(t *testing.T, into *wire) *awssqs.Client {
	t.Helper()
	requireQueues(t)
	record := func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("recordSendMessageBatch",
			func(ctx context.Context, in middleware.InitializeInput,
				next middleware.InitializeHandler,
			) (middleware.InitializeOutput, middleware.Metadata, error) {
				if batch, ok := in.Parameters.(*awssqs.SendMessageBatchInput); ok {
					into.record(batch.Entries)
				}
				return next.HandleInitialize(ctx, in)
			}), middleware.Before)
	}
	return awssqs.New(sharedSDK.Options(), func(o *awssqs.Options) {
		o.APIOptions = append(o.APIOptions, record)
	})
}
