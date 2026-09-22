package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// recorded is a telemetry built over the real SDK, with everything it produces
// kept in memory.
//
// The real SDK rather than a fake, because what these tests are about is what
// the SDK ends up holding: a span's parent, a counter's attribute set, a
// histogram's unit. A fake would be this package asserting that it called
// itself.
type recorded struct {
	*Telemetry
	spans   *tracetest.SpanRecorder
	metrics *sdkmetric.ManualReader
}

// record builds one.
func record(t *testing.T) *recorded {
	t.Helper()

	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	telemetry, err := New(Config{
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		MeterProvider:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		t.Fatalf("build the telemetry: %v", err)
	}
	return &recorded{Telemetry: telemetry, spans: spans, metrics: reader}
}

// ended is every span that has finished, in the order they finished.
func (r *recorded) ended(t *testing.T) []sdktrace.ReadOnlySpan {
	t.Helper()
	return r.spans.Ended()
}

// only is the one span that finished, and fails when there is not exactly one.
func (r *recorded) only(t *testing.T) sdktrace.ReadOnlySpan {
	t.Helper()
	ended := r.ended(t)
	if len(ended) != 1 {
		t.Fatalf("%d spans finished, wanted exactly one", len(ended))
	}
	return ended[0]
}

// collected is every measurement the meter is holding, collected once.
func (r *recorded) collected(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var into metricdata.ResourceMetrics
	if err := r.metrics.Collect(context.Background(), &into); err != nil {
		t.Fatalf("collect the measurements: %v", err)
	}
	return into
}

// instrument finds one instrument by name, and fails when nothing recorded it.
func (r *recorded) instrument(t *testing.T, name string) metricdata.Metrics {
	t.Helper()
	collected := r.collected(t)
	var recorded []string
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			recorded = append(recorded, m.Name)
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("nothing recorded %s; the meter is holding %v", name, recorded)
	return metricdata.Metrics{}
}

// counted is the sum one counter holds under the attributes given, and reports
// whether any data point carried them.
func counted(t *testing.T, m metricdata.Metrics, want map[string]string) (int64, bool) {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is a %T, wanted an int64 sum", m.Name, m.Data)
	}
	for _, point := range sum.DataPoints {
		if matches(point.Attributes.ToSlice(), want) {
			return point.Value, true
		}
	}
	return 0, false
}

// matches reports whether an attribute set carries exactly the pairs wanted.
//
// Exactly, not at least. A counter's attribute set IS its time series, so a
// test that accepted a superset would pass with an attribute nobody meant to
// add — which is the way a dashboard's query silently stops matching.
func matches(got []attribute.KeyValue, want map[string]string) bool {
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
