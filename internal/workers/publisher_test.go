package workers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
)

// storedEnvelope is what an outbox row's payload looks like once jsonb has
// normalised it: one object, already the envelope, with nothing left to
// assemble.
func storedEnvelope(correlation, causation string) []byte {
	return fmt.Appendf(nil, `{"aggregateId":"wallet","causationId":%q,"correlationId":%q,`+
		`"data":{"transactionId":"transaction"},"eventId":"event","eventType":`+
		`"WagerTransactionProcessed","occurredAt":"2026-09-21T09:00:00Z","version":1}`,
		causation, correlation)
}

// publisherOver builds a publisher with the test defaults filled in and stops
// it when the test ends.
func publisherOver(t *testing.T, ctx context.Context, cfg PublisherConfig) *Publisher {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "publisher-1"
	}
	if cfg.Logger == nil {
		cfg.Logger = discard()
	}
	if cfg.Clock == nil {
		cfg.Clock = fixedClock{at: testTime()}
	}
	if cfg.Interval == 0 {
		cfg.Interval = time.Hour
	}
	if cfg.Backoff == (Backoff{}) {
		cfg.Backoff = Backoff{Initial: 2 * time.Second, Factor: 2, Max: 30 * time.Second}
	}
	publisher, err := NewPublisher(cfg)
	if err != nil {
		t.Fatalf("build a publisher: %v", err)
	}
	if err := publisher.Start(ctx); err != nil {
		t.Fatalf("start the publisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Stop(context.WithoutCancel(ctx)) })
	return publisher
}

// An event is marked published exactly where the queue accepted it. Marking one
// it refused loses the event; not marking one it took sends it twice.
func TestThePublisherMarksExactlyWhatWasSent(t *testing.T) {
	events := []postgres.ClaimedEvent{
		event(1, storedEnvelope("thread-1", "")),
		event(1, storedEnvelope("thread-2", "")),
		event(1, storedEnvelope("thread-3", "")),
	}

	cases := []struct {
		name string
		// results says what became of each of the three, positionally.
		results []sqs.SendResult
		// want names the indexes that must be marked; every other index must be
		// rescheduled instead.
		marked []int
	}{
		{
			name: "a batch the queue took entirely",
			results: []sqs.SendResult{
				{MessageID: "q-1"}, {MessageID: "q-2"}, {MessageID: "q-3"},
			},
			marked: []int{0, 1, 2},
		},
		{
			name: "a partial failure marks only the successes",
			results: []sqs.SendResult{
				{MessageID: "q-1"},
				{Err: app.AsRetryable(errors.New("the queue refused this one"))},
				{MessageID: "q-3"},
			},
			marked: []int{0, 2},
		},
		{
			name: "a batch the queue refused entirely",
			results: []sqs.SendResult{
				{Err: app.AsRetryable(errors.New("throttled"))},
				{Err: app.AsRetryable(errors.New("throttled"))},
				{Err: app.AsRetryable(errors.New("throttled"))},
			},
		},
		{
			name: "entries a stopped send never offered are put back, not marked",
			results: []sqs.SendResult{
				{Err: app.AsRetryable(errors.New("the call failed"))},
				{Err: fmt.Errorf("%w: the call failed", sqs.ErrSendAborted)},
				{Err: fmt.Errorf("%w: the call failed", sqs.ErrSendAborted)},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			outbox := newFakeOutbox(events)
			sender := &fakeSender{
				answer: func(int, []sqs.Outbound) ([]sqs.SendResult, error) {
					return c.results, nil
				},
			}
			publisherOver(t, t.Context(), PublisherConfig{Outbox: outbox, Queue: sender})
			outbox.settled(t, len(events))

			wantMarked := map[int]bool{}
			for _, i := range c.marked {
				wantMarked[i] = true
			}
			marked := map[app.EventID]bool{}
			for _, m := range outbox.marked() {
				marked[m.id] = true
			}
			put := map[app.EventID]bool{}
			for _, r := range outbox.put() {
				put[r.id] = true
			}
			for i, e := range events {
				switch {
				case wantMarked[i] && !marked[e.EventID]:
					t.Errorf("event %d reached the queue and was not marked published", i)
				case !wantMarked[i] && marked[e.EventID]:
					t.Errorf("event %d never reached the queue and was marked published", i)
				case !wantMarked[i] && !put[e.EventID]:
					t.Errorf("event %d was neither published nor rescheduled", i)
				case wantMarked[i] && put[e.EventID]:
					t.Errorf("event %d was published and rescheduled", i)
				}
			}
		})
	}
}

// A send that could not be attempted at all is reported as an error rather than
// per entry, and every event goes back.
func TestASendThatWasNeverAttemptedPutsEveryEventBack(t *testing.T) {
	events := []postgres.ClaimedEvent{
		event(1, storedEnvelope("thread-1", "")),
		event(1, storedEnvelope("thread-2", "")),
	}
	outbox := newFakeOutbox(events)
	sender := &fakeSender{
		answer: func(int, []sqs.Outbound) ([]sqs.SendResult, error) {
			return nil, app.AsUnretryable(sqs.ErrNotResolved)
		},
	}
	publisherOver(t, t.Context(), PublisherConfig{Outbox: outbox, Queue: sender})
	outbox.settled(t, len(events))

	if got := outbox.marked(); len(got) != 0 {
		t.Errorf("marked = %d events published, want none: nothing was sent", len(got))
	}
	if got := outbox.put(); len(got) != len(events) {
		t.Errorf("rescheduled %d events, want all %d back", len(got), len(events))
	}
}

// A result slice that does not line up with the batch is a defect, and the only
// safe answer is to mark nothing.
func TestAMisalignedResultMarksNothing(t *testing.T) {
	events := []postgres.ClaimedEvent{
		event(1, storedEnvelope("thread-1", "")),
		event(1, storedEnvelope("thread-2", "")),
	}
	outbox := newFakeOutbox(events)
	sender := &fakeSender{
		answer: func(int, []sqs.Outbound) ([]sqs.SendResult, error) {
			return []sqs.SendResult{{MessageID: "q-1"}}, nil
		},
	}
	log := &recorder{}
	publisherOver(t, t.Context(), PublisherConfig{
		Outbox: outbox, Queue: sender, Logger: log.logger(),
	})
	outbox.settled(t, 2)

	if got := outbox.marked(); len(got) != 0 {
		t.Errorf("marked %d events on a result nobody could align, want none", len(got))
	}
	if got := outbox.put(); len(got) != 2 {
		t.Errorf("rescheduled %d events, want both", len(got))
	}
	log.await(t, "the queue answered for a different number of events")
}

// The wire form is the stored payload, byte for byte, under the group and
// deduplication ids that make ordering and republication work.
func TestTheEnvelopeGoesOnTheWireExactlyAsStored(t *testing.T) {
	payload := storedEnvelope("thread-1", "cause-1")
	claimed := event(1, payload)
	outbox := newFakeOutbox([]postgres.ClaimedEvent{claimed})
	sender := &fakeSender{}

	publisherOver(t, t.Context(), PublisherConfig{Outbox: outbox, Queue: sender})
	outbox.settled(t, 1)

	batches := sender.batches()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("sent %v, want exactly one message", batches)
	}
	message := batches[0][0]
	if string(message.Body) != string(payload) {
		t.Errorf("body = %s, want the stored payload unaltered", message.Body)
	}
	if message.GroupID != claimed.AggregateID.String() {
		t.Errorf("group = %q, want the aggregate id %q", message.GroupID, claimed.AggregateID)
	}
	if message.DeduplicationID != claimed.EventID.String() {
		t.Errorf("deduplication id = %q, want the event id %q, which is what makes a "+
			"republication the same event", message.DeduplicationID, claimed.EventID)
	}
	if got := message.Attributes[correlationAttribute]; got != "thread-1" {
		t.Errorf("correlation attribute = %q, want the envelope's correlation", got)
	}
	if got := message.Attributes[causationAttribute]; got != "cause-1" {
		t.Errorf("causation attribute = %q, want the envelope's causation", got)
	}
}

func TestTraceAttributesAreOmittedWhenThereIsNothingToCarry(t *testing.T) {
	for _, c := range []struct {
		name    string
		payload []byte
	}{
		{name: "no causation to name", payload: storedEnvelope("thread-1", "")},
		{name: "a payload that cannot be read", payload: []byte("{not json")},
	} {
		t.Run(c.name, func(t *testing.T) {
			attributes := traceOf(c.payload)
			if _, carried := attributes[causationAttribute]; carried {
				t.Errorf("attributes = %v, want no causation", attributes)
			}
		})
	}
	if traceOf([]byte("{not json")) != nil {
		t.Error("an unreadable payload produced attributes")
	}
}

// The reschedule is the publisher's backoff applied to the row's own attempt
// count, capped, and written against the publisher holding the claim.
func TestTheRescheduleBacksOffWithTheAttempts(t *testing.T) {
	for _, c := range []struct {
		attempts int
		want     time.Duration
	}{
		{attempts: 1, want: 2 * time.Second},
		{attempts: 2, want: 4 * time.Second},
		{attempts: 3, want: 8 * time.Second},
		{attempts: 20, want: 30 * time.Second},
	} {
		t.Run(fmt.Sprintf("attempt %d", c.attempts), func(t *testing.T) {
			claimed := event(c.attempts, storedEnvelope("thread-1", ""))
			outbox := newFakeOutbox([]postgres.ClaimedEvent{claimed})
			sender := &fakeSender{
				answer: func(int, []sqs.Outbound) ([]sqs.SendResult, error) {
					return []sqs.SendResult{
						{Err: app.AsRetryable(errors.New("throttled"))},
					}, nil
				},
			}
			publisherOver(t, t.Context(), PublisherConfig{
				Outbox: outbox, Queue: sender, Name: "publisher-1",
			})
			outbox.settled(t, 1)

			put := outbox.put()
			if len(put) != 1 {
				t.Fatalf("rescheduled %d events, want one", len(put))
			}
			if want := testTime().Add(c.want); !put[0].at.Equal(want) {
				t.Errorf("next attempt at %s, want %s (%s after this turn)",
					put[0].at, want, c.want)
			}
			if put[0].by != "publisher-1" {
				t.Errorf("rescheduled by %q, want the publisher holding the claim", put[0].by)
			}
		})
	}
}

// An event the queue will refuse identically forever cannot be parked — there
// is no door onto the outbox that does that — so it has to be said loudly.
func TestAnEventTheQueueWillNeverAcceptIsReported(t *testing.T) {
	outbox := newFakeOutbox([]postgres.ClaimedEvent{event(3, storedEnvelope("thread-1", ""))})
	sender := &fakeSender{
		answer: func(int, []sqs.Outbound) ([]sqs.SendResult, error) {
			return []sqs.SendResult{
				{Err: app.AsUnretryable(errors.New("sqs: InvalidMessageContents"))},
			}, nil
		},
	}
	log := &recorder{}
	publisherOver(t, t.Context(), PublisherConfig{
		Outbox: outbox, Queue: sender, Logger: log.logger(),
	})
	outbox.settled(t, 1)

	record := log.await(t,
		"the queue refuses this event and will refuse it again; it needs an operator")
	if got, _ := attr(record, "eventId"); got == "" {
		t.Error("the line does not name the event")
	}
}

// The claim names the publisher, bounds the batch, reads one clock and states
// its own hold.
func TestTheClaimStatesWhoTookItAndForHowLong(t *testing.T) {
	outbox := newFakeOutbox()
	publisherOver(t, t.Context(), PublisherConfig{
		Outbox: outbox, Queue: &fakeSender{}, Name: "publisher-7",
		Batch: 4, Hold: 45 * time.Second,
	})
	outbox.drained(t)

	claims := outbox.claimed()
	if len(claims) == 0 {
		t.Fatal("the publisher never claimed")
	}
	got := claims[0].req
	if got.By != "publisher-7" {
		t.Errorf("claimed by %q, want the publisher's name", got.By)
	}
	if got.Limit != 4 {
		t.Errorf("limit = %d, want the configured batch", got.Limit)
	}
	if got.Hold != 45*time.Second {
		t.Errorf("hold = %s, want the configured hold", got.Hold)
	}
	if !got.At.Equal(testTime()) {
		t.Errorf("at = %s, want the clock's instant %s", got.At, testTime())
	}
}

// A turn that filled its batch goes straight round again; a full claim is
// evidence there is more work now rather than in an interval's time.
func TestAFullBatchIsClaimedAgainWithoutWaiting(t *testing.T) {
	full := []postgres.ClaimedEvent{
		event(1, storedEnvelope("thread-1", "")),
		event(1, storedEnvelope("thread-2", "")),
	}
	outbox := newFakeOutbox(full, full)

	// An interval no test could wait out, so that a third claim can only have
	// happened because the publisher did not wait.
	publisherOver(t, t.Context(), PublisherConfig{
		Outbox: outbox, Queue: &fakeSender{}, Batch: 2, Interval: time.Hour,
	})
	outbox.drained(t)

	if got := len(outbox.claimed()); got < 3 {
		t.Errorf("claims = %d, want the publisher to have claimed both full batches and "+
			"then found the outbox empty", got)
	}
}

// A replica that stops cleanly hands its claims back rather than leaving them to
// wait out a hold that exists for the case where it did not.
func TestClaimsAreReleasedOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	outbox := newFakeOutbox()
	publisher, err := NewPublisher(PublisherConfig{
		Outbox: outbox, Queue: &fakeSender{}, Clock: fixedClock{at: testTime()},
		Name: "publisher-1", Logger: discard(), Interval: time.Hour,
	})
	if err != nil {
		t.Fatalf("build a publisher: %v", err)
	}
	if err := publisher.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	outbox.drained(t)

	if err := publisher.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got := outbox.releaseCount(); got != 1 {
		t.Errorf("released %d times, want exactly once", got)
	}
	if err := publisher.Stop(ctx); err != nil {
		t.Errorf("stopping twice reported %v", err)
	}
	if got := outbox.releaseCount(); got != 1 {
		t.Errorf("released %d times after a second stop, want still once", got)
	}
}

// A release that fails is reported rather than swallowed: those rows are a
// wallet's whole stream waiting for a hold to expire.
func TestAFailedReleaseIsReported(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	outbox := newFakeOutbox()
	outbox.releaseErr = app.AsRetryable(errors.New("the connection was lost"))
	publisher, err := NewPublisher(PublisherConfig{
		Outbox: outbox, Queue: &fakeSender{}, Clock: fixedClock{at: testTime()},
		Name: "publisher-1", Logger: discard(), Interval: time.Hour,
	})
	if err != nil {
		t.Fatalf("build a publisher: %v", err)
	}
	if err := publisher.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	outbox.drained(t)

	if err := publisher.Stop(ctx); err == nil {
		t.Error("a shutdown that could not release its claims reported success")
	}
}

// A claim that failed does not end the loop: the publisher says so, waits its
// backoff and claims again.
func TestAFailedClaimDoesNotEndTheLoop(t *testing.T) {
	claimed := event(1, storedEnvelope("thread-1", ""))
	outbox := newFakeOutbox()
	outbox.claimAnswer = func(n int) ([]postgres.ClaimedEvent, error) {
		switch n {
		case 0:
			return nil, app.AsRetryable(errors.New("the connection was lost"))
		case 1:
			return []postgres.ClaimedEvent{claimed}, nil
		default:
			return nil, nil
		}
	}
	log := &recorder{}
	publisherOver(t, t.Context(), PublisherConfig{
		Outbox: outbox, Queue: &fakeSender{}, Logger: log.logger(),
		Backoff: Backoff{Initial: time.Millisecond, Factor: 2, Max: 10 * time.Millisecond},
	})
	outbox.settled(t, 1)

	log.await(t, "could not claim outbox events")
	marked := outbox.marked()
	if len(marked) != 1 || marked[0].id != claimed.EventID {
		t.Errorf("marked = %v, want the publisher to have carried on to the next claim", marked)
	}
}
