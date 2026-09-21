package telemetry

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// errUnreachable stands in for a database that has stopped answering.
var errUnreachable = errors.New("the database is not answering")

// TestTheMetricCatalogueIsWhatADashboardIsBuiltFrom pins every instrument's
// name, unit and attribute keys.
//
// It is the one test in this tree written for a reader outside it. A dashboard
// and an alert are built from these names, in another repository, by somebody
// who cannot see this code — so a name that changed, a unit that changed from
// seconds to milliseconds, or an attribute key that gained a letter is a panel
// that goes blank and an alert that never fires again. None of those is a
// failure any other test here would notice: the service keeps working, and the
// only symptom is a dashboard nobody trusts.
//
// The attribute sets are compared EXACTLY. An attribute added to a counter
// splits its series, so "at least these" would pass on the change that breaks
// the sum a panel is drawing.
func TestTheMetricCatalogueIsWhatADashboardIsBuiltFrom(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// record makes the one measurement this case is about.
		record func(*recorded)
		// instrument, unit and attributes are the contract.
		instrument string
		unit       string
		attributes map[string]string
		// value is what the counter must hold, or nought for the instruments
		// checked another way.
		value int64
	}{
		{
			name: "an operation that reached an answer",
			record: func(r *recorded) {
				r.RecordOperation(t.Context(), Operation{
					Source: SourceHTTP, Kind: "BET", Status: "PROCESSED",
					Took: 250 * time.Millisecond,
				})
			},
			instrument: MetricTransactions,
			unit:       "{transaction}",
			attributes: map[string]string{
				"source": "http", "kind": "BET",
				"status": "PROCESSED", "failureCode": NoFailureCode,
			},
			value: 1,
		},
		{
			name: "a rejected operation, which keeps its reason",
			record: func(r *recorded) {
				r.RecordOperation(t.Context(), Operation{
					Source: SourceQueue, Kind: "BET", Status: "REJECTED",
					FailureCode: "INSUFFICIENT_FUNDS",
				})
			},
			instrument: MetricTransactions,
			unit:       "{transaction}",
			attributes: map[string]string{
				"source": "sqs", "kind": "BET",
				"status": "REJECTED", "failureCode": "INSUFFICIENT_FUNDS",
			},
			value: 1,
		},
		{
			name: "a replay, counted by the door it came in by",
			record: func(r *recorded) {
				r.RecordOperation(t.Context(), Operation{
					Source: SourceQueue, Kind: "WIN", Status: "PROCESSED", Replay: true,
				})
			},
			instrument: MetricReplays,
			unit:       "{replay}",
			attributes: map[string]string{"source": "sqs", "kind": "WIN"},
			value:      1,
		},
		{
			name:       "a message the inbox had already seen",
			record:     func(r *recorded) { r.RecordInboxDuplicate(t.Context(), "wager-consumer") },
			instrument: MetricInboxDuplicates,
			unit:       "{message}",
			attributes: map[string]string{"consumer": "wager-consumer"},
			value:      1,
		},
		{
			name: "a message handed back for another delivery",
			record: func(r *recorded) {
				r.RecordQueueRetry(t.Context(), "wager-consumer", "RETRYABLE")
			},
			instrument: MetricQueueRetries,
			unit:       "{message}",
			attributes: map[string]string{"consumer": "wager-consumer", "class": "RETRYABLE"},
			value:      1,
		},
		{
			name:       "a message on its last delivery",
			record:     func(r *recorded) { r.RecordDeadLetter(t.Context(), "wager-consumer") },
			instrument: MetricQueueDeadLetters,
			unit:       "{message}",
			attributes: map[string]string{"consumer": "wager-consumer"},
			value:      1,
		},
		{
			name:       "a transaction that gave up waiting for a lock",
			record:     func(r *recorded) { r.RecordLockTimeout(t.Context(), Movement) },
			instrument: MetricLockTimeouts,
			unit:       "{conflict}",
			attributes: map[string]string{"transaction": "movement"},
			value:      1,
		},
		{
			name:       "a wallet that moved under a movement",
			record:     func(r *recorded) { r.RecordVersionConflict(t.Context(), Movement) },
			instrument: MetricVersionConflicts,
			unit:       "{conflict}",
			attributes: map[string]string{"transaction": "movement"},
			value:      1,
		},
		{
			name:       "an event the queue took",
			record:     func(r *recorded) { r.RecordPublishAttempt(t.Context(), OutcomePublished) },
			instrument: MetricPublishAttempts,
			unit:       "{attempt}",
			attributes: map[string]string{"outcome": "published"},
			value:      1,
		},
		{
			name:       "an event the queue refused",
			record:     func(r *recorded) { r.RecordPublishAttempt(t.Context(), OutcomeRefused) },
			instrument: MetricPublishAttempts,
			unit:       "{attempt}",
			attributes: map[string]string{"outcome": "refused"},
			value:      1,
		},
		{
			name:       "a wallet that disagrees with its ledger",
			record:     func(r *recorded) { r.RecordDivergence(t.Context()) },
			instrument: MetricDivergences,
			unit:       "{wallet}",
			attributes: map[string]string{},
			value:      1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := record(t)
			c.record(r)

			instrument := r.instrument(t, c.instrument)
			if instrument.Unit != c.unit {
				t.Errorf("%s is in %q, wanted %q", c.instrument, instrument.Unit, c.unit)
			}
			if instrument.Description == "" {
				t.Errorf("%s has no description, and a dashboard shows one", c.instrument)
			}
			got, found := counted(t, instrument, c.attributes)
			if !found {
				t.Fatalf("%s carried no point with exactly %v; it holds %s",
					c.instrument, c.attributes, describe(instrument))
			}
			if got != c.value {
				t.Errorf("%s counted %d, wanted %d", c.instrument, got, c.value)
			}
		})
	}
}

// TestTheLatencyHistogramIsInSecondsAndSharesTheCounterAttributes pins the one
// instrument a dashboard divides by another.
//
// Seconds, because Prometheus histograms are expected in seconds and a panel
// that converts at the query is a panel somebody eventually forgets to convert.
// The same attribute set as the transaction counter, because "average latency
// of a rejected BET" is that division and it only works if the two series have
// the same labels.
func TestTheLatencyHistogramIsInSecondsAndSharesTheCounterAttributes(t *testing.T) {
	t.Parallel()
	r := record(t)

	r.RecordOperation(t.Context(), Operation{
		Source: SourceHTTP, Kind: "BET", Status: "PROCESSED", Took: 1500 * time.Millisecond,
	})

	instrument := r.instrument(t, MetricProcessing)
	if instrument.Unit != "s" {
		t.Errorf("%s is in %q, wanted seconds", MetricProcessing, instrument.Unit)
	}
	histogram, ok := instrument.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is a %T, wanted a float64 histogram", MetricProcessing, instrument.Data)
	}
	if len(histogram.DataPoints) != 1 {
		t.Fatalf("%s holds %d points, wanted one", MetricProcessing, len(histogram.DataPoints))
	}
	point := histogram.DataPoints[0]
	if point.Sum != 1.5 {
		t.Errorf("%s recorded %v seconds, wanted 1.5", MetricProcessing, point.Sum)
	}
	want := map[string]string{
		"source": "http", "kind": "BET", "status": "PROCESSED", "failureCode": NoFailureCode,
	}
	if !matches(point.Attributes.ToSlice(), want) {
		t.Errorf("%s is labelled %v, wanted the transaction counter's labels %v",
			MetricProcessing, point.Attributes.ToSlice(), want)
	}
}

// TestTheLatencyBucketsAreShapedForSeconds pins the boundaries themselves.
//
// It is the only thing that can. The SDK's default boundaries are 0, 5, 10,
// 25 … 10000, shaped for milliseconds, and against a value in seconds every
// observation this service will ever make lands in the first bucket — so the
// recording succeeds, the sum and the count are right, the unit says "s", and
// only the QUANTILES are nonsense. Nothing fails, nothing logs, and the panel
// reports a steady 2.5 seconds for six milliseconds of work for ever.
//
// So the assertion is on the list: that it is the declared unit's shape, that
// the whole of it lies where this service's work lies, and that an observation
// of a few milliseconds is distinguishable from one of a few hundred.
func TestTheLatencyBucketsAreShapedForSeconds(t *testing.T) {
	t.Parallel()
	r := record(t)

	// Two observations three orders of magnitude apart. A bucket list shaped
	// for the wrong unit puts both in the same bucket, which is the whole
	// failure.
	for _, took := range []time.Duration{6 * time.Millisecond, 900 * time.Millisecond} {
		r.RecordOperation(t.Context(), Operation{
			Source: SourceHTTP, Kind: "BET", Status: "PROCESSED", Took: took,
		})
	}

	histogram, ok := r.instrument(t, MetricProcessing).Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is not a float64 histogram", MetricProcessing)
	}
	point := histogram.DataPoints[0]

	if !slices.Equal(point.Bounds, processingBuckets) {
		t.Fatalf("%s draws its boundaries at %v, wanted %v",
			MetricProcessing, point.Bounds, processingBuckets)
	}
	// The SDK's defaults, which are what this instrument gets if the explicit
	// boundaries are ever dropped. Named here so that the revert is what fails.
	if point.Bounds[0] >= 1 {
		t.Errorf("%s starts at %v, which is a millisecond-shaped boundary on an "+
			"instrument declared in seconds", MetricProcessing, point.Bounds[0])
	}

	// And the two observations are told apart, which is what a quantile needs.
	var occupied int
	for _, count := range point.BucketCounts {
		if count > 0 {
			occupied++
		}
	}
	if occupied != 2 {
		t.Errorf("six milliseconds and nine hundred fell into %d buckets, wanted two: %v",
			occupied, point.BucketCounts)
	}
}

// TestAnOperationWithNoMeasurementDistortsNoHistogram pins the one case that
// would quietly ruin the latency panel.
//
// The reference worker counts operations and takes no measurement: its latency
// is a poll interval, which is a setting rather than a fact about an operation.
// Recording a zero for it would drag the histogram's every quantile toward
// nought, and the panel would say this service answers instantly while
// providers waited.
func TestAnOperationWithNoMeasurementDistortsNoHistogram(t *testing.T) {
	t.Parallel()
	r := record(t)

	r.RecordOperation(t.Context(), Operation{
		Source: SourceReference, Kind: "WIN", Status: "PROCESSED",
	})

	if got, _ := counted(t, r.instrument(t, MetricTransactions), map[string]string{
		"source": "reference", "kind": "WIN",
		"status": "PROCESSED", "failureCode": NoFailureCode,
	}); got != 1 {
		t.Fatalf("the operation was counted %d times, wanted once", got)
	}
	for _, scope := range r.collected(t).ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == MetricProcessing {
				t.Fatalf("%s recorded a point for an operation nobody timed", MetricProcessing)
			}
		}
	}
}

// TestTheOutboxLagIsObservedAndAFailureRecordsNothing pins the gauge and the
// one thing it must never do.
//
// A callback that fails records NOTHING rather than a zero, because zero means
// "the outbox is clear" — so a database that has stopped answering would show
// as a perfectly drained outbox at exactly the moment nothing is draining.
func TestTheOutboxLagIsObservedAndAFailureRecordsNothing(t *testing.T) {
	t.Parallel()

	t.Run("an outbox with a backlog", func(t *testing.T) {
		t.Parallel()
		r := record(t)
		stop, err := r.ObserveOutboxLag(func(ctx context.Context) (time.Duration, error) {
			return 90 * time.Second, nil
		})
		if err != nil {
			t.Fatalf("observe the lag: %v", err)
		}
		t.Cleanup(func() { _ = stop() })

		instrument := r.instrument(t, MetricOutboxLag)
		if instrument.Unit != "s" {
			t.Errorf("%s is in %q, wanted seconds", MetricOutboxLag, instrument.Unit)
		}
		gauge, ok := instrument.Data.(metricdata.Gauge[float64])
		if !ok {
			t.Fatalf("%s is a %T, wanted a float64 gauge", MetricOutboxLag, instrument.Data)
		}
		if len(gauge.DataPoints) != 1 || gauge.DataPoints[0].Value != 90 {
			t.Fatalf("%s observed %v, wanted 90 seconds", MetricOutboxLag, gauge.DataPoints)
		}
	})

	t.Run("a database that would not answer", func(t *testing.T) {
		t.Parallel()
		r := record(t)
		stop, err := r.ObserveOutboxLag(func(ctx context.Context) (time.Duration, error) {
			return 0, errUnreachable
		})
		if err != nil {
			t.Fatalf("observe the lag: %v", err)
		}
		t.Cleanup(func() { _ = stop() })

		// Collected directly, because a callback that fails makes the whole
		// collection report an error — which is how the failure reaches an
		// operator through the SDK's error handler. What must not happen is a
		// point with a value in it.
		var into metricdata.ResourceMetrics
		if err := r.metrics.Collect(t.Context(), &into); !errors.Is(err, errUnreachable) {
			t.Fatalf("the collection reported %v, wanted the callback's own failure", err)
		}
		for _, scope := range into.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != MetricOutboxLag {
					continue
				}
				gauge, ok := m.Data.(metricdata.Gauge[float64])
				if ok && len(gauge.DataPoints) > 0 {
					t.Fatalf("%s reported %v for a database that would not answer",
						MetricOutboxLag, gauge.DataPoints)
				}
			}
		}
	})

	t.Run("observing needs something to ask", func(t *testing.T) {
		t.Parallel()
		if _, err := record(t).ObserveOutboxLag(nil); err == nil {
			t.Error("a nil callback was accepted, so the gauge would report nothing for ever")
		}
	})
}

// TestDisabledTelemetryRecordsNothingAndRefusesNothing pins the shape every
// component depends on: nil is a working value.
func TestDisabledTelemetryRecordsNothingAndRefusesNothing(t *testing.T) {
	t.Parallel()

	for _, t_ := range []*Telemetry{nil, Disabled()} {
		ctx, span := t_.Start(t.Context(), "anything")
		t_.Failed(span, "SOMETHING")
		span.End()

		t_.RecordOperation(ctx, Operation{Source: SourceHTTP, Kind: "BET", Took: time.Second})
		t_.RecordInboxDuplicate(ctx, "consumer")
		t_.RecordQueueRetry(ctx, "consumer", "RETRYABLE")
		t_.RecordDeadLetter(ctx, "consumer")
		t_.RecordLockTimeout(ctx, Movement)
		t_.RecordVersionConflict(ctx, Movement)
		t_.RecordPublishAttempt(ctx, OutcomePublished)
		t_.RecordDivergence(ctx)

		if carried := t_.Inject(ctx); carried != nil {
			t.Errorf("disabled telemetry injected %v, wanted nothing to carry", carried)
		}
	}
	// Two calls, kept in variables so that the comparison is between two
	// results rather than between one expression and itself — which is both
	// what the memoisation claim means and what a linter will accept.
	first, second := Disabled(), Disabled()
	if first != second {
		t.Error("Disabled built two values, so two components would hold different instruments")
	}
}

// describe renders one instrument's points for a failure message.
func describe(m metricdata.Metrics) string {
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return "no int64 points"
	}
	rendered := make([]string, 0, len(sum.DataPoints))
	for _, point := range sum.DataPoints {
		rendered = append(rendered, point.Attributes.Encoded(nil))
	}
	slices.Sort(rendered)
	return "[" + strings.Join(rendered, " | ") + "]"
}
