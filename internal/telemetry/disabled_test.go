package telemetry

import (
	"context"
	"testing"
	"time"
)

// The benchmarks behind the sentence in the package documentation that says
// what switched-off telemetry costs.
//
// They exist because the obvious reading of "records into nothing" is "costs
// nothing", and that reading is wrong in one place that matters:
// metric.WithAttributes builds an attribute.Set eagerly, at the call site,
// before any instrument — no-op or not — has had the chance to discard it. The
// number is small against a database transaction and is not worth engineering
// away; it is worth not claiming.
//
// Run: go test ./internal/telemetry/ -bench . -benchmem -run ^$

func BenchmarkDisabledStart(b *testing.B) {
	t := Disabled()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, span := t.Start(ctx, SpanConsume)
		span.End()
	}
}

func BenchmarkDisabledRecordOperation(b *testing.B) {
	t := Disabled()
	ctx := context.Background()
	op := Operation{
		Source: SourceHTTP, Kind: "BET", Status: "PROCESSED", Took: 250 * time.Millisecond,
	}
	b.ReportAllocs()
	for b.Loop() {
		t.RecordOperation(ctx, op)
	}
}

func BenchmarkDisabledInject(b *testing.B) {
	t := Disabled()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_ = t.Inject(ctx)
	}
}
