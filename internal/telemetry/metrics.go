package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The instruments this service records, by name.
//
// Dotted, lower case, singular-noun-then-plural-thing, which is
// OpenTelemetry's convention and is what the Prometheus exporter turns into
// wagering_transactions_total and friends by replacing the dots and appending
// the unit's suffix. They are declared as constants so that a dashboard is
// built from this file rather than from a running process — and so that
// renaming one is a compile-time event here and a visible diff there.
//
// Every one of them is prefixed wagering. rather than left bare. The collector
// exports several services into one Prometheus, and a bare `transactions_total`
// belongs to whoever registered it first.
const (
	// MetricTransactions counts operations that reached an answer, whichever
	// door they came in by. It does NOT count failures: an operation that could
	// not be applied has no kind, no status and no failure code, and counting it
	// as one would put a database outage in the same series as a rejected bet.
	MetricTransactions = "wagering.transactions"
	// MetricReplays counts operations answered from records already written.
	MetricReplays = "wagering.idempotent_replays"
	// MetricInboxDuplicates counts messages the inbox had already seen.
	MetricInboxDuplicates = "wagering.inbox.duplicates"
	// MetricProcessing is how long one operation took, measured at the door it
	// came in by — so it includes the transaction, the domain and the wait for
	// a wallet lock, and excludes the queue it was sitting on.
	MetricProcessing = "wagering.processing.duration"

	// MetricQueueRetries counts messages handed back for another delivery.
	MetricQueueRetries = "wagering.sqs.retries"
	// MetricQueueDeadLetters counts messages left on what the redrive policy
	// says is their last delivery. It is an estimate for the reason
	// ApproximateReceiveCount is: the count SQS reports is approximate, and the
	// policy is judged on the same approximate number.
	MetricQueueDeadLetters = "wagering.sqs.dead_letters"

	// MetricLockTimeouts counts transactions that gave up waiting for a row
	// lock, and MetricVersionConflicts counts wallet writes refused because the
	// balance they were computed from was not the balance the wallet held.
	//
	// They are kept apart although both are contention, because they mean
	// opposite things about this service: a lock timeout is the lock working
	// and the wait being too long, a version conflict is the lock NOT having
	// been held, which should be unreachable.
	MetricLockTimeouts     = "wagering.lock_timeouts"
	MetricVersionConflicts = "wagering.version_conflicts"

	// MetricOutboxLag is how old the oldest unpublished event is. It is the one
	// number that says whether the outbox is draining, and because the outbox is
	// head-of-line per wallet it is also an upper bound on how far behind any
	// one wallet's event stream has fallen.
	MetricOutboxLag = "wagering.outbox.lag"
	// MetricPublishAttempts counts events offered to the queue, by what became
	// of each.
	//
	// It does NOT name the publisher, although every span and every log line
	// about a publish does. A publisher's name must be distinct per process and
	// falls back to HOSTNAME, which on Kubernetes is a pod name with a random
	// suffix and on ECS a container id — both of which change on every rollout
	// and every restart. As a metric attribute that is an unbounded label: the
	// series count grows by two per pod lifetime for ever, and Prometheus does
	// not reclaim them inside its retention. The question this counter answers
	// — are publishes being refused, and at what rate — does not need to be
	// per-process, and the question that does (which replica) is answered where
	// per-process identity is cheap.
	MetricPublishAttempts = "wagering.outbox.publish_attempts"

	// MetricDivergences counts reconciliations that found a wallet's stored
	// balance disagreeing with its ledger. Nothing else in this system is
	// allowed to be non-zero.
	MetricDivergences = "wagering.reconciliation.divergences"
)

// The units, in UCUM, which is what the OpenTelemetry specification asks for.
//
// A counter's unit is the thing it counts in braces — annotation-only, so it
// contributes no suffix to a Prometheus name and no arithmetic to anything.
// Durations are seconds, "s", because that is what a Prometheus histogram is
// expected to be in and converting at the dashboard is how somebody ends up
// alerting on milliseconds labelled seconds.
const (
	unitTransaction = "{transaction}"
	unitReplay      = "{replay}"
	unitMessage     = "{message}"
	unitConflict    = "{conflict}"
	unitAttempt     = "{attempt}"
	unitWallet      = "{wallet}"
	unitSeconds     = "s"
)

// instruments is every measurement this service takes.
//
// One struct built once per process rather than an instrument created at each
// call site: creating one is not free, and a meter asked twice for the same
// name with a different description answers with a warning and the first
// definition, which is a drift nobody sees.
type instruments struct {
	transactions    metric.Int64Counter
	replays         metric.Int64Counter
	inboxDuplicates metric.Int64Counter
	processing      metric.Float64Histogram

	queueRetries     metric.Int64Counter
	queueDeadLetters metric.Int64Counter

	lockTimeouts     metric.Int64Counter
	versionConflicts metric.Int64Counter

	outboxLag       metric.Float64ObservableGauge
	publishAttempts metric.Int64Counter

	divergences metric.Int64Counter

	meter metric.Meter
}

// newInstruments creates every instrument, refusing at construction rather than
// at the first measurement.
func newInstruments(meter metric.Meter) (*instruments, error) {
	i := &instruments{meter: meter}
	var err error
	fail := func(name string, cause error) error {
		return fmt.Errorf("telemetry: create the instrument %s: %w", name, cause)
	}

	if i.transactions, err = meter.Int64Counter(MetricTransactions,
		metric.WithUnit(unitTransaction),
		metric.WithDescription("Wager operations that reached an answer."),
	); err != nil {
		return nil, fail(MetricTransactions, err)
	}
	if i.replays, err = meter.Int64Counter(MetricReplays,
		metric.WithUnit(unitReplay),
		metric.WithDescription("Operations answered from records already written."),
	); err != nil {
		return nil, fail(MetricReplays, err)
	}
	if i.inboxDuplicates, err = meter.Int64Counter(MetricInboxDuplicates,
		metric.WithUnit(unitMessage),
		metric.WithDescription("Queue messages this consumer had already handled."),
	); err != nil {
		return nil, fail(MetricInboxDuplicates, err)
	}
	if i.processing, err = meter.Float64Histogram(MetricProcessing,
		metric.WithUnit(unitSeconds),
		metric.WithDescription("How long one operation took at the door it arrived by."),
	); err != nil {
		return nil, fail(MetricProcessing, err)
	}
	if i.queueRetries, err = meter.Int64Counter(MetricQueueRetries,
		metric.WithUnit(unitMessage),
		metric.WithDescription("Messages hidden for a backoff and delivered again."),
	); err != nil {
		return nil, fail(MetricQueueRetries, err)
	}
	if i.queueDeadLetters, err = meter.Int64Counter(MetricQueueDeadLetters,
		metric.WithUnit(unitMessage),
		metric.WithDescription("Messages left on their last delivery before the dead-letter queue."),
	); err != nil {
		return nil, fail(MetricQueueDeadLetters, err)
	}
	if i.lockTimeouts, err = meter.Int64Counter(MetricLockTimeouts,
		metric.WithUnit(unitConflict),
		metric.WithDescription("Transactions that gave up waiting for a row lock."),
	); err != nil {
		return nil, fail(MetricLockTimeouts, err)
	}
	if i.versionConflicts, err = meter.Int64Counter(MetricVersionConflicts,
		metric.WithUnit(unitConflict),
		metric.WithDescription("Wallet writes refused because the balance had moved under them."),
	); err != nil {
		return nil, fail(MetricVersionConflicts, err)
	}
	if i.outboxLag, err = meter.Float64ObservableGauge(MetricOutboxLag,
		metric.WithUnit(unitSeconds),
		metric.WithDescription("Age of the oldest unpublished outbox event."),
	); err != nil {
		return nil, fail(MetricOutboxLag, err)
	}
	if i.publishAttempts, err = meter.Int64Counter(MetricPublishAttempts,
		metric.WithUnit(unitAttempt),
		metric.WithDescription("Events offered to the outbound queue, by what became of each."),
	); err != nil {
		return nil, fail(MetricPublishAttempts, err)
	}
	if i.divergences, err = meter.Int64Counter(MetricDivergences,
		metric.WithUnit(unitWallet),
		metric.WithDescription("Reconciliations that found a wallet disagreeing with its ledger."),
	); err != nil {
		return nil, fail(MetricDivergences, err)
	}
	return i, nil
}

// Operation is one operation that reached an answer, as the instruments read
// it.
//
// It carries no amount, no balance and no player, and never will: see the
// package documentation. What it carries is what a dashboard divides by.
type Operation struct {
	// Source is the door this operation came in by: [SourceHTTP],
	// [SourceQueue] or [SourceReference].
	Source string
	// Kind, Status and FailureCode are the operation's own vocabulary.
	// FailureCode is empty for anything that was not rejected, and is recorded
	// as [NoFailureCode].
	Kind        string
	Status      string
	FailureCode string
	// Replay reports that this result was read rather than produced.
	Replay bool
	// Took is how long the door was busy with it. Zero is not recorded, so a
	// caller that has no measurement records a count without distorting the
	// histogram.
	Took time.Duration
}

// RecordOperation counts one operation, its replay if it was one, and how long
// it took.
//
// Three measurements from one call rather than three calls, because they share
// an attribute set and the one that is easiest to get wrong is the third: a
// latency histogram whose attributes drifted from the counter's is two views
// that cannot be divided into each other.
func (t *Telemetry) RecordOperation(ctx context.Context, op Operation) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(
		Source(op.Source),
		Kind(op.Kind),
		Status(op.Status),
		FailureCode(op.FailureCode),
	)
	t.metrics.transactions.Add(ctx, 1, attrs)
	if op.Replay {
		t.metrics.replays.Add(ctx, 1, metric.WithAttributes(Source(op.Source), Kind(op.Kind)))
	}
	if op.Took > 0 {
		t.metrics.processing.Record(ctx, op.Took.Seconds(), attrs)
	}
}

// RecordInboxDuplicate counts a message this consumer had already handled.
//
// It overlaps [Telemetry.RecordOperation]'s replay count on purpose, and the
// two answer different questions. A replay by source says how much of this
// service's work is idempotency doing its job, across all three doors; an inbox
// duplicate says how often the QUEUE is redelivering, which is a fact about the
// queue's configuration and about this consumer's drains rather than about
// providers.
func (t *Telemetry) RecordInboxDuplicate(ctx context.Context, consumer string) {
	if t == nil {
		return
	}
	t.metrics.inboxDuplicates.Add(ctx, 1,
		metric.WithAttributes(attribute.String(KeyConsumer, consumer)))
}

// RecordQueueRetry counts a message handed back for another delivery, with the
// class of failure that sent it back.
func (t *Telemetry) RecordQueueRetry(ctx context.Context, consumer, class string) {
	if t == nil {
		return
	}
	t.metrics.queueRetries.Add(ctx, 1, metric.WithAttributes(
		attribute.String(KeyConsumer, consumer),
		Class(class),
	))
}

// RecordDeadLetter counts a message left on what the redrive policy says is its
// last delivery.
func (t *Telemetry) RecordDeadLetter(ctx context.Context, consumer string) {
	if t == nil {
		return
	}
	t.metrics.queueDeadLetters.Add(ctx, 1,
		metric.WithAttributes(attribute.String(KeyConsumer, consumer)))
}

// RecordLockTimeout counts a transaction that gave up waiting for a row lock.
func (t *Telemetry) RecordLockTimeout(ctx context.Context, transaction string) {
	if t == nil {
		return
	}
	t.metrics.lockTimeouts.Add(ctx, 1,
		metric.WithAttributes(attribute.String(KeyTransactionKind, transaction)))
}

// RecordVersionConflict counts a wallet write refused because the balance it
// was computed from was no longer the balance the wallet held.
func (t *Telemetry) RecordVersionConflict(ctx context.Context, transaction string) {
	if t == nil {
		return
	}
	t.metrics.versionConflicts.Add(ctx, 1,
		metric.WithAttributes(attribute.String(KeyTransactionKind, transaction)))
}

// RecordPublishAttempt counts one event offered to the outbound queue.
// outcome is [OutcomePublished] or [OutcomeRefused].
//
// It takes no publisher, deliberately. See [MetricPublishAttempts].
func (t *Telemetry) RecordPublishAttempt(ctx context.Context, outcome string) {
	if t == nil {
		return
	}
	t.metrics.publishAttempts.Add(ctx, 1,
		metric.WithAttributes(attribute.String(KeyOutcome, outcome)))
}

// RecordDivergence counts a reconciliation that found a wallet disagreeing with
// its ledger.
func (t *Telemetry) RecordDivergence(ctx context.Context) {
	if t == nil {
		return
	}
	t.metrics.divergences.Add(ctx, 1)
}

// ObserveOutboxLag registers the callback that reports how old the oldest
// unpublished event is, and returns the call that stops it.
//
// An observable rather than something the publisher records each turn, and the
// difference is the case that matters. A publisher whose sends are failing
// claims nothing new while its backlog ages, so a gauge written from a turn's
// own work would be silent in exactly the outage it exists to show. The
// callback runs on the meter's collection interval instead, asks the database,
// and answers whether or not anything is being published.
//
// A callback that fails records nothing for that interval rather than a zero. A
// zero would mean "the outbox is empty", which is the opposite of what a
// database that would not answer is evidence for.
func (t *Telemetry) ObserveOutboxLag(
	lag func(context.Context) (time.Duration, error),
) (func() error, error) {
	if t == nil {
		return Disabled().ObserveOutboxLag(lag)
	}
	if lag == nil {
		return nil, fmt.Errorf("telemetry: observing %s needs something to ask", MetricOutboxLag)
	}
	registration, err := t.metrics.meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			age, err := lag(ctx)
			if err != nil {
				return err
			}
			o.ObserveFloat64(t.metrics.outboxLag, age.Seconds())
			return nil
		},
		t.metrics.outboxLag,
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: observe %s: %w", MetricOutboxLag, err)
	}
	return registration.Unregister, nil
}
