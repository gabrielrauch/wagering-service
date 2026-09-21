package workers

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/faults"
)

// What a [PublisherConfig] leaves open.
const (
	// defaultClaimBatch is how many events one turn takes. Ten, which is the
	// most one SendMessageBatch may carry, so a full claim is one round trip to
	// the queue and one to the database rather than a claim the send then has
	// to split.
	defaultClaimBatch = 10
	// defaultClaimHold is how long a claim stands before the row returns to the
	// pool. It has to outlast a publication attempt and must not outlast a
	// deployment: too short and a slow send loses its work to another
	// publisher mid-flight, too long and work abandoned by a publisher that
	// died sits unpublished for as long as the hold.
	defaultClaimHold = 30 * time.Second
	// defaultPollInterval is how long an idle publisher waits before looking
	// again. A second: the outbox is on the latency path of every event this
	// service emits, and a busier interval buys nothing because a turn that
	// filled its batch does not wait at all.
	defaultPollInterval = time.Second

	// The default backoff: doubling from two seconds to five minutes. Unlike
	// the consumer's, this one has no redrive policy behind it to end the
	// work, so the cap is what bounds it — and it is five minutes rather than
	// longer because the outbox is head-of-line per wallet, so an event waiting
	// is that wallet's whole stream waiting.
	defaultPublisherInitial = 2 * time.Second
	defaultPublisherFactor  = 2
	defaultPublisherMax     = 5 * time.Minute
)

// PublisherConfig is everything the outbox publisher is built from.
type PublisherConfig struct {
	// Outbox is the publisher's side of the outbox. Required.
	Outbox OutboxClaims
	// Queue is the outbound queue. Required.
	Queue OutboundQueue
	// Clock is where the publisher reads the time, so that a publisher and its
	// tests read one clock. Required.
	Clock app.Clock
	// Name identifies this publisher in the claims it takes, and must be
	// distinct per process: it is what an operator sees in claimed_by, and it
	// is what scopes a reschedule to the publisher that holds the row.
	// Required.
	//
	// Two publishers sharing a name could reschedule each other's work, which
	// is the one thing the claim's scoping exists to prevent — see
	// postgres.OutboxClaims.Reschedule.
	Name string
	// Batch is how many events one turn claims. Zero means
	// [defaultClaimBatch].
	Batch int
	// Hold is how long a claim stands. Zero means [defaultClaimHold].
	Hold time.Duration
	// Interval is how long an idle publisher waits before claiming again. Zero
	// means [defaultPollInterval]. A turn that filled its batch does not wait,
	// because a full batch is evidence there is more.
	Interval time.Duration
	// Backoff is how long an event waits after a send it did not survive. The
	// zero value means the documented default; anything else is taken as given
	// and checked in full.
	Backoff Backoff
	// Logger is where the publisher reports what it did. Required.
	Logger *slog.Logger
}

// Publisher moves events from the outbox onto the outbound queue.
//
// One turn claims a bounded batch of due, unpublished, head-of-line events,
// sends them, and then records each one individually: marked published where the
// queue accepted it, rescheduled where it did not. Marking is driven by
// [sqs.SendResult.Sent] and by nothing else, because the two ways of getting
// that wrong are marking an event nobody has seen and sending one twice — and
// only one of those is recoverable.
//
// Several publishers may run at once. They do not coordinate: the claim is
// taken with SKIP LOCKED and expires by wall clock, so a publisher that stops
// mid-turn has its work taken up by another rather than holding it until a
// connection closes. A publisher killed between the send and the mark
// republishes when it comes back, and the deduplication id being the event id is
// what keeps that from becoming a second event on the wire.
type Publisher struct {
	outbox   OutboxClaims
	queue    OutboundQueue
	clock    app.Clock
	name     string
	batch    int
	hold     time.Duration
	interval time.Duration
	backoff  Backoff
	logger   *slog.Logger

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewPublisher wires the publisher. It performs no I/O and claims nothing;
// publishing begins at [Publisher.Start].
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	switch {
	case cfg.Outbox == nil:
		return nil, errors.New("workers: the publisher needs the outbox")
	case cfg.Queue == nil:
		return nil, errors.New("workers: the publisher needs an outbound queue")
	case cfg.Clock == nil:
		return nil, errors.New("workers: the publisher needs a clock")
	case cfg.Logger == nil:
		return nil, errors.New("workers: the publisher needs a logger")
	case cfg.Name == "":
		return nil, errors.New("workers: the publisher needs a name to claim under")
	case cfg.Batch < 0:
		return nil, fmt.Errorf("workers: the publisher needs a positive batch, got %d", cfg.Batch)
	case cfg.Hold < 0:
		return nil, fmt.Errorf("workers: the publisher needs a positive claim hold, got %s", cfg.Hold)
	case cfg.Interval < 0:
		return nil, fmt.Errorf("workers: the publisher needs a positive poll interval, got %s",
			cfg.Interval)
	}

	backoff := cfg.Backoff
	if backoff == (Backoff{}) {
		backoff = Backoff{
			Initial: defaultPublisherInitial,
			Factor:  defaultPublisherFactor,
			Max:     defaultPublisherMax,
		}
	}
	if err := backoff.validate("publisher"); err != nil {
		return nil, err
	}

	return &Publisher{
		outbox:   cfg.Outbox,
		queue:    cfg.Queue,
		clock:    cfg.Clock,
		name:     cfg.Name,
		batch:    orDefault(cfg.Batch, defaultClaimBatch),
		hold:     orDefaultDuration(cfg.Hold, defaultClaimHold),
		interval: orDefaultDuration(cfg.Interval, defaultPollInterval),
		backoff:  backoff,
		logger:   cfg.Logger,
	}, nil
}

// Start begins publishing. A publisher is started once.
func (p *Publisher) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.stopped:
		return errors.New("workers: the publisher has already been stopped")
	case p.started:
		return errors.New("workers: the publisher is already running")
	}

	runCtx, cancel := context.WithCancel(ctx)
	p.cancel, p.started = cancel, true
	p.wg.Add(1)
	go p.run(runCtx)

	p.logger.InfoContext(ctx, "publishing wallet events",
		slog.String("publisher", p.name),
		slog.Int("batch", p.batch),
		slog.Duration("interval", p.interval),
		slog.Duration("hold", p.hold))
	return nil
}

// Stop stops claiming and hands back everything this publisher holds.
//
// The release is the whole point of stopping cleanly. Without it a replica that
// was asked to stop leaves its claimed rows waiting out a hold that exists for
// the case where it was not — and because the outbox is head-of-line per wallet,
// each of those rows is a wallet's whole stream waiting with it.
func (p *Publisher) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.started || p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	cancel := p.cancel
	p.mu.Unlock()

	cancel()
	p.wg.Wait()

	// The caller's cancellation is stripped, for the reason the consumer's
	// release sweep strips it: claims are handed back while the process is
	// shutting down, which is exactly when a shutdown hook's context is likely
	// to be gone.
	releaseCtx, done := context.WithTimeout(context.WithoutCancel(ctx), releaseBudget)
	defer done()

	released, err := p.outbox.ReleaseClaims(releaseCtx, p.name)
	if err != nil {
		p.logger.ErrorContext(ctx, "the publisher's claims could not be released",
			slog.String("publisher", p.name),
			slog.String("error", err.Error()))
		return fmt.Errorf("workers: release the publisher's outbox claims: %w", err)
	}
	p.logger.InfoContext(ctx, "stopped publishing",
		slog.String("publisher", p.name),
		slog.Int("released", released))
	return nil
}

// run is the publisher's loop.
//
// A turn that filled its batch goes straight round again: the claim is bounded,
// so a full one is evidence that there is more work now rather than in an
// interval's time. Anything less waits the interval, because the next claim
// would find the same nothing.
//
// A turn that could not claim at all waits the backoff instead, for however
// many turns have failed in a row. A publisher polling a database that is not
// answering once a second writes one error line a second and asks a struggling
// database a question a second, which is how a difficulty becomes an outage.
//
// The consecutive-failure count is in memory, and that is not the in-memory
// state this system forbids: it decides when this process next asks a question,
// never what has been published. What has been published is in the outbox.
func (p *Publisher) run(ctx context.Context) {
	defer p.wg.Done()

	failures := 0
	for ctx.Err() == nil {
		claimed, err := p.turn(ctx)
		switch {
		case err != nil:
			failures++
			if !wait(ctx, p.backoff.after(failures)) {
				return
			}
		case claimed >= p.batch:
			failures = 0
		default:
			failures = 0
			if !wait(ctx, p.interval) {
				return
			}
		}
	}
}

// turn claims, publishes and records one batch, and reports how many events it
// claimed.
//
// The error is the claim's alone. A send that failed is not a failure of the
// turn: every event it touched has been put back on its own schedule, so the
// next claim will not see them and there is nothing for the loop to slow down
// on.
func (p *Publisher) turn(ctx context.Context) (int, error) {
	at := p.clock.Now()
	claimed, err := p.outbox.Claim(ctx, postgres.ClaimRequest{
		By:    p.name,
		Limit: p.batch,
		At:    at,
		Hold:  p.hold,
	})
	if err != nil {
		if ctx.Err() == nil {
			p.logger.ErrorContext(ctx, "could not claim outbox events",
				slog.String("publisher", p.name),
				slog.String("class", string(app.ClassOf(err))),
				slog.String("error", err.Error()))
		}
		return 0, err
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	// The claim is held and nothing has been sent. A process killed here must
	// not hold the event hostage; the claim expiring by wall clock is what
	// makes that true.
	faults.Hit(faults.AfterClaimBeforePublish)

	results, err := p.queue.SendBatch(ctx, outboundOf(claimed))
	if err != nil {
		// Nothing was attempted at all — the adapter reports per entry
		// otherwise — so every event goes back with the same cause.
		p.logger.ErrorContext(ctx, "could not send a batch of events",
			slog.String("publisher", p.name),
			slog.Int("events", len(claimed)),
			slog.String("class", string(app.ClassOf(err))),
			slog.String("error", err.Error()))
		p.rescheduleAll(ctx, claimed, err)
		return len(claimed), nil
	}
	if len(results) != len(claimed) {
		// Unreachable: the adapter's contract is one positionally aligned
		// result per message. Asserting it anyway, because the alternative to
		// noticing is marking an event published on the strength of another
		// event's result.
		p.logger.ErrorContext(ctx, "the queue answered for a different number of events",
			slog.String("publisher", p.name),
			slog.Int("events", len(claimed)),
			slog.Int("results", len(results)))
		p.rescheduleAll(ctx, claimed,
			errors.New("workers: the send reported a different number of results"))
		return len(claimed), nil
	}

	// The events the queue accepted are on the wire and no row says so. A
	// process killed here republishes when it comes back, and the deduplication
	// id being the event id is what keeps that from becoming a second event.
	faults.Hit(faults.AfterPublishBeforeMark)

	done := p.clock.Now()
	for i, result := range results {
		if result.Sent() {
			p.mark(ctx, claimed[i], done)
			continue
		}
		p.reschedule(ctx, claimed[i], done, result.Err)
	}
	return len(claimed), nil
}

// mark records one event as published.
//
// Individually, one statement per event, rather than as a batch keyed on
// whatever the send returned as a whole: the queue accepts some entries and
// refuses others in one response, and a mark that covered the batch would either
// claim events the queue refused or republish events it took.
func (p *Publisher) mark(ctx context.Context, event postgres.ClaimedEvent, at time.Time) {
	marked, err := p.outbox.MarkPublished(ctx, event.EventID, at)
	if err != nil {
		// The event is on the queue and its row does not know it. Nothing is
		// lost: the claim expires, the row is claimed again, and the event is
		// sent again under the same deduplication id, which SQS answers with
		// the message it already has.
		p.logger.ErrorContext(ctx, "an event was published but could not be marked",
			p.about(event, slog.String("error", err.Error()))...)
		return
	}
	if !marked {
		// A claim expires by wall clock, so a publisher that was slow can find
		// its work already done. That is the recovery path working, and the
		// only thing worth doing with it is counting it.
		p.logger.InfoContext(ctx, "an event was already marked published by another publisher",
			p.about(event)...)
	}
}

// reschedule puts one event back with a later next attempt.
func (p *Publisher) reschedule(
	ctx context.Context, event postgres.ClaimedEvent, at time.Time, cause error,
) {
	delay := p.backoff.after(event.Attempts)
	moved, err := p.outbox.Reschedule(ctx, p.name, event.EventID, at.Add(delay))
	class := app.ClassOf(cause)
	attrs := p.about(event,
		slog.String("class", string(class)),
		slog.Duration("retryIn", delay),
		slog.String("error", cause.Error()))
	switch {
	case err != nil:
		// The row keeps the claim until it expires, and is then taken by
		// whoever claims next. Worth a line; nothing else to do about it.
		p.logger.ErrorContext(ctx, "an event could not be rescheduled",
			append(attrs, slog.String("rescheduleError", err.Error()))...)
	case !moved:
		p.logger.InfoContext(ctx, "an event was no longer this publisher's to reschedule",
			attrs...)
	case class == app.Unretryable:
		// The queue will refuse this event the same way every time, and there
		// is no door onto the outbox that parks a row permanently. It will be
		// claimed, refused and rescheduled until somebody acts on it, holding
		// the head of its wallet's stream while it does — which is why this is
		// an error and not a warning.
		p.logger.ErrorContext(ctx,
			"the queue refuses this event and will refuse it again; it needs an operator",
			attrs...)
	default:
		p.logger.WarnContext(ctx, "an event was not published and will be sent again", attrs...)
	}
}

// rescheduleAll puts a whole batch back, for the two failures that are about the
// call rather than about any one event.
func (p *Publisher) rescheduleAll(
	ctx context.Context, claimed []postgres.ClaimedEvent, cause error,
) {
	at := p.clock.Now()
	for _, event := range claimed {
		p.reschedule(ctx, event, at, cause)
	}
}

// about is the attributes every line about one event carries. The envelope's
// data is never among them: it is a financial payload, and a log is not where
// one belongs.
func (p *Publisher) about(event postgres.ClaimedEvent, extra ...slog.Attr) []any {
	attrs := []any{
		slog.String("publisher", p.name),
		slog.String("eventId", event.EventID.String()),
		slog.String("eventType", event.EventType),
		slog.String("aggregateId", event.AggregateID.String()),
		slog.Int64("aggregateSequence", event.AggregateSequence),
		slog.Int("attempts", event.Attempts),
	}
	for _, attr := range extra {
		attrs = append(attrs, attr)
	}
	return attrs
}

// outboundOf is the batch as the queue takes it.
//
// The body is the stored payload, byte for byte. The outbox row's payload IS the
// envelope — it is written as one object and read back as one — so there is
// nothing here to assemble and nothing to re-render: jsonb has already
// normalised the key order and the spacing, and re-marshalling would publish a
// third spelling of a document this service has two of already.
//
// The group is the aggregate id and the deduplication id is the event id. That
// pairing is the whole of the outbound contract: the group is what orders a
// wallet's events for a consumer rebuilding its history, and the deduplication
// id is what makes a republication the same event rather than a second one.
func outboundOf(claimed []postgres.ClaimedEvent) []sqs.Outbound {
	messages := make([]sqs.Outbound, len(claimed))
	for i, event := range claimed {
		messages[i] = sqs.Outbound{
			Body:            event.Payload,
			GroupID:         event.AggregateID.String(),
			DeduplicationID: event.EventID.String(),
			Attributes:      traceOf(event.Payload),
		}
	}
	return messages
}

// storedTrace is the part of a stored envelope the publisher reads for itself.
//
// Only these two members are named, and unknown ones are ignored rather than
// refused — the opposite of how the inbound envelope is read, and for the
// opposite reason. This document is this service's own and is not being
// validated; it is being looked in, for the two values that also belong beside
// the body.
type storedTrace struct {
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId"`
}

// traceOf reads the trace context out of a stored envelope, so that it can ride
// in the message attributes as well as in the body.
//
// A payload it cannot read returns no attributes rather than failing the send.
// The trace is already inside the envelope, so the consequence is a consumer
// having to open the body to follow a thread — which is a worse morning for
// somebody, not a lost event.
func traceOf(payload []byte) map[string]string {
	var trace storedTrace
	if err := json.Unmarshal(payload, &trace); err != nil {
		return nil
	}
	attributes := make(map[string]string, 2)
	if trace.CorrelationID != "" {
		attributes[correlationAttribute] = trace.CorrelationID
	}
	if trace.CausationID != "" {
		attributes[causationAttribute] = trace.CausationID
	}
	if len(attributes) == 0 {
		return nil
	}
	return attributes
}
