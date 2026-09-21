package workers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// watched is telemetry over the real SDK, with everything it produces kept in
// memory. The real SDK, because what these tests assert is what it ends up
// holding: a span's parent, a counter's attribute set.
type watched struct {
	*telemetry.Telemetry
	spans   *tracetest.SpanRecorder
	metrics *sdkmetric.ManualReader
}

func watch(t *testing.T) *watched {
	t.Helper()
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	built, err := telemetry.New(telemetry.Config{
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		MeterProvider:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		t.Fatalf("build the telemetry: %v", err)
	}
	return &watched{Telemetry: built, spans: spans, metrics: reader}
}

// awaitNamed waits for a span with that name to finish and returns it.
//
// A wait rather than a read, because a batch turn's span ends AFTER the events
// inside it have been marked — which is the barrier a publisher test has —
// so reading straight after that barrier is reading a span that has not closed
// yet.
func (w *watched) awaitNamed(t *testing.T, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, span := range w.spans.Ended() {
			if span.Name() == name {
				return span
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no span named %q finished", name)
	return nil
}

// named is the one finished span with that name, and fails when there is not
// exactly one.
func (w *watched) named(t *testing.T, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found []sdktrace.ReadOnlySpan
	for _, span := range w.spans.Ended() {
		if span.Name() == name {
			found = append(found, span)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d spans named %q finished, wanted one", len(found), name)
	}
	return found[0]
}

// counted is the sum one counter holds under the attributes given.
func (w *watched) counted(t *testing.T, name string, want map[string]string) int64 {
	t.Helper()
	var into metricdata.ResourceMetrics
	if err := w.metrics.Collect(context.Background(), &into); err != nil {
		t.Fatalf("collect the measurements: %v", err)
	}
	for _, scope := range into.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is a %T, wanted an int64 sum", name, m.Data)
			}
			for _, point := range sum.DataPoints {
				if sameAttributes(point.Attributes.ToSlice(), want) {
					return point.Value
				}
			}
		}
	}
	return 0
}

func sameAttributes(got []attribute.KeyValue, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, attr := range got {
		value, named := want[string(attr.Key)]
		if !named || attr.Value.String() != value {
			return false
		}
	}
	return true
}

// aTraceparent is a well-formed W3C trace context, as an operation's
// transaction would have written it into its outbox row.
const (
	aTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	aSpanID      = "00f067aa0ba902b7"
	aTraceparent = "00-" + aTraceID + "-" + aSpanID + "-01"
)

// TestAPublishedEventStaysInTheTraceItsOperationWasSubmittedUnder is the whole
// point of the outbox carrying trace context.
//
// The requirement is ONE trace spanning the request that submitted an operation
// and the event that was published for it — not two traces sharing a
// correlation attribute. The outbox row is the only durable thing the publisher
// reads, so the trace it carries is the only way the publisher can know which
// trace it is continuing, minutes later, in another process.
//
// Three things have to hold together and each is asserted: the event's span is
// in the operation's trace, its parent is the span that wrote the row, and the
// traceparent that goes onto the queue is that span's — so a consumer that
// continues from the message attributes lands in the same trace again.
func TestAPublishedEventStaysInTheTraceItsOperationWasSubmittedUnder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	w := watch(t)

	claimed := event(1, storedEnvelope("thread-1", ""))
	claimed.Trace = map[string]string{"traceparent": aTraceparent}
	sender := &fakeSender{}
	outbox := newFakeOutbox([]postgres.ClaimedEvent{claimed})

	publisherOver(t, ctx, PublisherConfig{
		Outbox: outbox, Queue: sender, Telemetry: w.Telemetry,
	})
	outbox.settled(t, 1)

	published := w.named(t, telemetry.SpanPublishEvent)
	if got := published.SpanContext().TraceID().String(); got != aTraceID {
		t.Fatalf("the published event is in trace %s, wanted the operation's %s", got, aTraceID)
	}
	if got := published.Parent().SpanID().String(); got != aSpanID {
		t.Errorf("the published event's parent is %s, wanted the span that wrote the row, %s",
			got, aSpanID)
	}

	message := sender.batches()[0][0]
	carried := message.Attributes["traceparent"]
	if carried == "" {
		t.Fatalf("the message carries no traceparent: %v", message.Attributes)
	}
	continued := oteltrace.SpanContextFromContext(
		w.Extract(ctx, message.Attributes))
	if got := continued.TraceID().String(); got != aTraceID {
		t.Errorf("a consumer reading this message lands in trace %s, wanted %s", got, aTraceID)
	}
	if got := continued.SpanID(); got != published.SpanContext().SpanID() {
		t.Errorf("the message names span %s, wanted the publishing span %s",
			got, published.SpanContext().SpanID())
	}

	// And the envelope's own thread still rides beside the body, because a
	// reader following a correlation must not have to open a financial payload.
	if got := message.Attributes[correlationAttribute]; got != "thread-1" {
		t.Errorf("the message carries correlation %q, wanted thread-1", got)
	}

	// The batch turn that happened to send it is a LINK and not the parent. It
	// cannot be the parent: one turn sends up to ten events belonging to up to
	// ten traces, and making it the parent of any of them would be choosing
	// which operation the batch belongs to. The link is how an operator gets
	// from the operation's trace to the publisher's turn and back.
	batch := w.awaitNamed(t, telemetry.SpanPublishBatch)
	if len(published.Links()) != 1 {
		t.Fatalf("the published event carries %d links, wanted the batch turn",
			len(published.Links()))
	}
	if got := published.Links()[0].SpanContext.SpanID(); got != batch.SpanContext().SpanID() {
		t.Errorf("the link points at %s, wanted the batch turn %s",
			got, batch.SpanContext().SpanID())
	}
}

// TestAnEventWithNoCarriedTraceStartsOneUnderTheBatch pins the honest fallback.
//
// An event written before any of this existed, or by a process with telemetry
// switched off, names no trace. It gets a span under the batch turn's own trace
// rather than a fabricated parent — there is no earlier trace to join, and
// inventing one would put the event in a trace nothing else is in.
func TestAnEventWithNoCarriedTraceStartsOneUnderTheBatch(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	w := watch(t)

	sender := &fakeSender{}
	outbox := newFakeOutbox([]postgres.ClaimedEvent{event(1, storedEnvelope("thread-1", ""))})

	publisherOver(t, ctx, PublisherConfig{
		Outbox: outbox, Queue: sender, Telemetry: w.Telemetry,
	})
	outbox.settled(t, 1)

	published := w.named(t, telemetry.SpanPublishEvent)
	batch := w.awaitNamed(t, telemetry.SpanPublishBatch)
	if published.SpanContext().TraceID() != batch.SpanContext().TraceID() {
		t.Errorf("an untraced event is in trace %s and its batch in %s",
			published.SpanContext().TraceID(), batch.SpanContext().TraceID())
	}
}

// TestEveryEventOfferedToTheQueueIsCountedByWhatBecameOfIt pins the two counts
// a publisher dashboard is built from.
//
// Published and refused are counted apart because the ratio is the alert: a
// publisher that is claiming and refusing is a queue outage, and one that is
// claiming nothing is an empty outbox. A single "attempts" counter cannot tell
// those apart.
func TestEveryEventOfferedToTheQueueIsCountedByWhatBecameOfIt(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	w := watch(t)

	sender := &fakeSender{answer: func(int, []sqs.Outbound) ([]sqs.SendResult, error) {
		return []sqs.SendResult{
			{MessageID: "queue-1"},
			{Err: sqs.ErrSendAborted},
		}, nil
	}}
	outbox := newFakeOutbox([]postgres.ClaimedEvent{
		event(1, storedEnvelope("thread-1", "")),
		event(1, storedEnvelope("thread-2", "")),
	})

	publisherOver(t, ctx, PublisherConfig{
		Outbox: outbox, Queue: sender, Telemetry: w.Telemetry,
	})
	outbox.settled(t, 2)

	if got := w.counted(t, telemetry.MetricPublishAttempts, map[string]string{
		"outcome": telemetry.OutcomeRefused,
	}); got != 1 {
		t.Errorf("%s counted %d refused, wanted 1", telemetry.MetricPublishAttempts, got)
	}
	if got := w.counted(t, telemetry.MetricPublishAttempts, map[string]string{
		"outcome": telemetry.OutcomePublished,
	}); got != 1 {
		t.Errorf("%s counted %d published, wanted 1", telemetry.MetricPublishAttempts, got)
	}
}

// TestAConsumedMessageContinuesTheTraceItArrivedCarrying is the other end of
// the same thread.
//
// A message this service published carries the operation's trace in its
// attributes, so the work a consumer does for it belongs in that trace rather
// than in a new one beside it. It is also what makes the queue path and the
// HTTP path one picture: the same operation submitted either way produces one
// trace.
func TestAConsumedMessageContinuesTheTraceItArrivedCarrying(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	w := watch(t)

	m := message("handle-1", validBody(t, nil), 1)
	m.Attributes = map[string]string{"traceparent": aTraceparent}
	queue := newFakeQueue([]sqs.Message{m})

	consumerOver(t, ctx, ConsumerConfig{
		Queue: queue, Wagering: &fakeSubmitter{}, Telemetry: w.Telemetry,
	})
	queue.handled(t)

	consumed := w.named(t, telemetry.SpanConsume)
	if got := consumed.SpanContext().TraceID().String(); got != aTraceID {
		t.Errorf("the message was consumed in trace %s, wanted the one it carried, %s",
			got, aTraceID)
	}
	if got := consumed.Parent().SpanID().String(); got != aSpanID {
		t.Errorf("the consume span's parent is %s, wanted %s", got, aSpanID)
	}
}

// TestAReplayedMessageIsCountedAsAnInboxDuplicate pins the count that says how
// often the QUEUE is redelivering.
//
// It overlaps the replay count on purpose and answers a different question:
// replays by source say how much of this service's work idempotency is
// absorbing across all three doors, and inbox duplicates say the queue handed
// back something already applied — which is a fact about the queue's
// configuration and this consumer's drains.
func TestAReplayedMessageIsCountedAsAnInboxDuplicate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	w := watch(t)

	queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 2)})
	replayed := processed()
	replayed.IdempotentReplay = true
	submitter := &fakeSubmitter{
		answer: func(context.Context, int, app.SubmitOperation) (app.OperationResult, error) {
			return replayed, nil
		},
	}

	consumerOver(t, ctx, ConsumerConfig{
		Queue: queue, Wagering: submitter, Telemetry: w.Telemetry, Name: "wager-consumer",
	})
	queue.handled(t)

	if got := w.counted(t, telemetry.MetricInboxDuplicates,
		map[string]string{"consumer": "wager-consumer"}); got != 1 {
		t.Errorf("%s counted %d, wanted the duplicate", telemetry.MetricInboxDuplicates, got)
	}
	if got := w.counted(t, telemetry.MetricReplays, map[string]string{
		"source": telemetry.SourceQueue, "kind": replayed.Kind.String(),
	}); got != 1 {
		t.Errorf("%s counted %d, wanted the replay", telemetry.MetricReplays, got)
	}
	if got := w.counted(t, telemetry.MetricTransactions, map[string]string{
		"source": telemetry.SourceQueue, "kind": replayed.Kind.String(),
		"status": replayed.Status.String(), "failureCode": telemetry.NoFailureCode,
	}); got != 1 {
		t.Errorf("%s counted %d, wanted the operation", telemetry.MetricTransactions, got)
	}
}

// TestWhatBecameOfAMessageIsCounted pins the queue numbers a dashboard alerts
// on, and — for every one of them — the paths on which they must NOT move.
//
// The second half is the half that was missing, and its absence was not
// academic: with only positive cases, moving RecordDeadLetter out of its
// lastDelivery guard passed the whole suite, and every poisoned message would
// have counted as a message about to leave the system. A counter that fires on
// every path is worse than one that never fires, because it is a page that
// stops meaning anything.
//
// So each case states what every instrument must hold, including the zeroes.
// The three are different incidents and have to stay that way: a rising retry
// rate is a database under pressure and it recovers, a message on its last
// delivery is work about to be lost, and an inbox duplicate is the queue
// redelivering something already applied.
func TestWhatBecameOfAMessageIsCounted(t *testing.T) {
	t.Parallel()

	const consumer = "wager-consumer"
	retries := map[string]string{"consumer": consumer, "class": string(app.Retryable)}
	deadLetters := map[string]string{"consumer": consumer}
	duplicates := map[string]string{"consumer": consumer}
	applied := map[string]string{
		"source": telemetry.SourceQueue, "kind": "BET",
		"status": "PROCESSED", "failureCode": telemetry.NoFailureCode,
	}
	replays := map[string]string{"source": telemetry.SourceQueue, "kind": "BET"}

	cases := []struct {
		name string
		// deliveries is the message's ApproximateReceiveCount.
		deliveries int
		// body is what the message carries; an unreadable one is permanent.
		body func(*testing.T) []byte
		// answer is what the application layer says.
		answer func(context.Context, int, app.SubmitOperation) (app.OperationResult, error)
		// counts is every instrument this case has an opinion about, including
		// the ones that must not have moved.
		counts []count
	}{
		{
			name:       "a transient failure, handed back for another delivery",
			deliveries: 1,
			body:       func(t *testing.T) []byte { return validBody(t, nil) },
			answer: func(context.Context, int, app.SubmitOperation) (app.OperationResult, error) {
				return app.OperationResult{}, app.AsRetryable(errUnavailable)
			},
			counts: []count{
				{telemetry.MetricQueueRetries, retries, 1},
				{telemetry.MetricQueueDeadLetters, deadLetters, 0},
				{telemetry.MetricInboxDuplicates, duplicates, 0},
				{telemetry.MetricTransactions, applied, 0},
			},
		},
		{
			name:       "a body nobody can read, on its last delivery",
			deliveries: testMaxReceives,
			body:       func(*testing.T) []byte { return []byte("{") },
			counts: []count{
				{telemetry.MetricQueueDeadLetters, deadLetters, 1},
				{telemetry.MetricQueueRetries, retries, 0},
				{telemetry.MetricInboxDuplicates, duplicates, 0},
				{telemetry.MetricTransactions, applied, 0},
			},
		},
		{
			name: "the same body, with deliveries still to come",
			// The message is just as poisoned and is left for the redrive
			// policy exactly as above. What it is NOT is a dead letter: the
			// queue will deliver it four more times, and counting it now would
			// make the number an operator pages on five times per message.
			deliveries: testMaxReceives - 1,
			body:       func(*testing.T) []byte { return []byte("{") },
			counts: []count{
				{telemetry.MetricQueueDeadLetters, deadLetters, 0},
				{telemetry.MetricQueueRetries, retries, 0},
			},
		},
		{
			name:       "an operation applied for the first time",
			deliveries: 1,
			body:       func(t *testing.T) []byte { return validBody(t, nil) },
			counts: []count{
				{telemetry.MetricTransactions, applied, 1},
				{telemetry.MetricInboxDuplicates, duplicates, 0},
				{telemetry.MetricReplays, replays, 0},
				{telemetry.MetricQueueRetries, retries, 0},
				{telemetry.MetricQueueDeadLetters, deadLetters, 0},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			w := watch(t)

			queue := newFakeQueue([]sqs.Message{message("handle-1", c.body(t), c.deliveries)})
			consumerOver(t, ctx, ConsumerConfig{
				Queue:           queue,
				Wagering:        &fakeSubmitter{answer: c.answer},
				Telemetry:       w.Telemetry,
				Name:            consumer,
				MaxReceiveCount: testMaxReceives,
			})
			queue.handled(t)

			for _, want := range c.counts {
				if got := w.counted(t, want.instrument, want.attributes); got != want.want {
					t.Errorf("%s counted %d under %v, wanted %d",
						want.instrument, got, want.attributes, want.want)
				}
			}
		})
	}
}

// count is one instrument, one attribute set, and what it must hold — including
// nought, which is what half of these assertions are for.
type count struct {
	instrument string
	attributes map[string]string
	want       int64
}

// TestAPoisonedMessageIsLeftForTheRedrivePolicyWhicheverDeliveryItIs keeps the
// case above from being vacuous.
//
// "The dead-letter counter did not move" is only worth asserting if the message
// was genuinely poisoned, so this asserts the disposition itself: not deleted,
// not hidden, left exactly where it was for the redrive policy to decide.
func TestAPoisonedMessageIsLeftForTheRedrivePolicyWhicheverDeliveryItIs(t *testing.T) {
	t.Parallel()

	for _, deliveries := range []int{1, testMaxReceives - 1, testMaxReceives} {
		t.Run(fmt.Sprintf("delivery %d", deliveries), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			w := watch(t)

			queue := newFakeQueue([]sqs.Message{message("handle-1", []byte("{"), deliveries)})
			consumerOver(t, ctx, ConsumerConfig{
				Queue: queue, Wagering: &fakeSubmitter{}, Telemetry: w.Telemetry,
				MaxReceiveCount: testMaxReceives,
			})
			queue.handled(t)

			if got := queue.deletedHandles(); len(got) != 0 {
				t.Errorf("a message nobody can read was deleted: %v", got)
			}
			if got := queue.changes(); len(got) != 0 {
				t.Errorf("a message nobody can read had its visibility changed: %v", got)
			}
		})
	}
}

// testMaxReceives is the redrive policy these tests state, and is the number
// the deployment provisions.
const testMaxReceives = 5

// errUnavailable stands in for a database that declined this attempt.
var errUnavailable = errors.New("the database declined this attempt")

// TestAParkedOperationIsCountedOnceItIsSettledAndNotBefore pins the count that
// would otherwise be twenty times too high.
//
// A parked operation is looked at again and again until its reference lands or
// its budget runs out — twenty times, on this service's own default. Counting
// each turn would count one operation once per attempt, and "how many
// operations did this service perform" would be a number about the reference
// worker's poll interval. It is counted exactly where it reaches an answer.
func TestAParkedOperationIsCountedOnceItIsSettledAndNotBefore(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	w := watch(t)

	settled := processed()
	turns := make(chan struct{}, 8)
	resumer := newFakeResumer(func(n int) (app.ResumeOutcome, error) {
		defer func() {
			select {
			case turns <- struct{}{}:
			default:
			}
		}()
		switch n {
		case 0, 1:
			// Still waiting: parked again, with a later attempt written.
			return app.ResumeOutcome{
				Claimed:       true,
				Rescheduled:   true,
				Result:        app.OperationResult{Kind: settled.Kind, Status: wagering.PendingReference},
				NextAttemptAt: testTime().Add(time.Second),
			}, nil
		case 2:
			return app.ResumeOutcome{Claimed: true, Result: settled}, nil
		default:
			return app.ResumeOutcome{}, nil
		}
	})

	workerOver(t, ctx, ReferenceConfig{Wagering: resumer, Telemetry: w.Telemetry})
	for range 4 {
		select {
		case <-turns:
		case <-time.After(2 * time.Second):
			t.Fatal("the worker never made four turns")
		}
	}

	if got := w.counted(t, telemetry.MetricTransactions, map[string]string{
		"source": telemetry.SourceReference, "kind": settled.Kind.String(),
		"status": settled.Status.String(), "failureCode": telemetry.NoFailureCode,
	}); got != 1 {
		t.Errorf("%s counted %d settlements, wanted the one", telemetry.MetricTransactions, got)
	}
	if got := w.counted(t, telemetry.MetricTransactions, map[string]string{
		"source": telemetry.SourceReference, "kind": settled.Kind.String(),
		"status": wagering.PendingReference.String(), "failureCode": telemetry.NoFailureCode,
	}); got != 0 {
		t.Errorf("%s counted %d operations that are still waiting",
			telemetry.MetricTransactions, got)
	}
}

// TestEveryDoorProducesTheSameSpanTree pins the shape a reader depends on.
//
// One operation submitted over the queue and the same operation carried forward
// by the reference worker have to read the same way: the entry span, the use
// case inside it, and whatever the adapters open below that. A tree that
// differed per door would mean an operator learning three shapes and a
// dashboard built on span names that exist on only one of them.
func TestEveryDoorProducesTheSameSpanTree(t *testing.T) {
	t.Parallel()

	t.Run("a message off the queue", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		w := watch(t)

		queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 1)})
		consumerOver(t, ctx, ConsumerConfig{
			Queue: queue, Wagering: &fakeSubmitter{}, Telemetry: w.Telemetry,
		})
		queue.handled(t)

		useCase := w.awaitNamed(t, telemetry.SpanSubmit)
		consumed := w.awaitNamed(t, telemetry.SpanConsume)
		if got := useCase.Parent().SpanID(); got != consumed.SpanContext().SpanID() {
			t.Errorf("%s hangs off %s, wanted the %s span %s", telemetry.SpanSubmit, got,
				telemetry.SpanConsume, consumed.SpanContext().SpanID())
		}
	})

	t.Run("a parked operation carried forward", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		w := watch(t)

		resumer := newFakeResumer(func(n int) (app.ResumeOutcome, error) {
			if n == 0 {
				return app.ResumeOutcome{Claimed: true, Result: processed()}, nil
			}
			return app.ResumeOutcome{}, nil
		})
		workerOver(t, ctx, ReferenceConfig{Wagering: resumer, Telemetry: w.Telemetry})

		useCase := w.awaitNamed(t, telemetry.SpanResumeUseCase)
		turn := w.awaitNamed(t, telemetry.SpanResume)
		if got := useCase.Parent().SpanID(); got != turn.SpanContext().SpanID() {
			t.Errorf("%s hangs off %s, wanted the %s span %s", telemetry.SpanResumeUseCase,
				got, telemetry.SpanResume, turn.SpanContext().SpanID())
		}
	})
}
