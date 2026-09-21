package httpapi

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// Where an attribute goes, which is one rule and worth stating once.
//
// Identifiers go on the ENTRY span — the request's — and outcomes go on the
// span that decided them. A request is what somebody searches for, so
// correlationId, walletId, providerId and transactionId have to be findable
// from the top of the trace rather than on a child three levels down; and the
// use case's span is the one that knows what the call came to, so kind, status
// and failure code belong there.
//
// Nothing is written twice. An attribute on both would be an attribute that can
// disagree with itself, and a search that returned the same trace under two
// names.

// requestSpan is the span otelhttp opened for this request, or a span that
// records nothing when there is none — which is every test that drives the API
// directly and every process with telemetry switched off.
func requestSpan(ctx context.Context) trace.Span { return trace.SpanFromContext(ctx) }

// describe names on the request's span what this request turned out to be
// about.
//
// It is called as each identifier becomes known rather than once at the end,
// because the end is not reached for a request that was refused — and a refused
// request is the one somebody is searching for.
func describe(r *http.Request, attrs ...attribute.KeyValue) {
	kept := telemetry.Some(attrs...)
	if len(kept) == 0 {
		return
	}
	requestSpan(r.Context()).SetAttributes(kept...)
}

// usecase opens the child span one application call runs in, and returns the
// call that closes it.
//
// The closer takes the error rather than reading it from somewhere, so that a
// span is marked exactly once and by the code that knows what happened. It
// reports how long the call took, which is what the processing histogram is
// measured over: the door was busy with this operation for that long, including
// the transaction and the wait for a wallet lock, and excluding everything this
// package does before and after.
//
// time.Since rather than a clock port, deliberately. Every other instant in
// this tree comes from app.Clock so that a test can fix it; this one is a
// MONOTONIC interval, which a fixed clock would render as zero — and a latency
// histogram of zeros is worse than none.
func (a *API) usecase(
	r *http.Request, name string, attrs ...attribute.KeyValue,
) (context.Context, func(error) time.Duration) {
	ctx, span := a.telemetry.Start(r.Context(), name,
		trace.WithSpanKind(trace.SpanKindInternal))
	started := time.Now()
	return ctx, func(err error) time.Duration {
		took := time.Since(started)
		if err != nil {
			// The class and the catalogued code, never the message. A refusal's
			// own words are the caller's business or nobody's — see
			// [telemetry.Telemetry.Failed], and see [record] for the one
			// failure that is named in the log and nowhere else.
			class := app.ClassOf(err)
			span.SetAttributes(telemetry.Class(string(class)))
			a.telemetry.Failed(span, codeFor(err, class))
		}
		span.End()
		return took
	}
}

// applied counts one operation that reached an answer and names what it came
// to on the span that decided it.
//
// Only a SUBMISSION reaches here. A read returns the same shape and has decided
// nothing, so counting one would put "how many operations did this service
// perform" and "how many times did somebody look at one" in the same series.
func (a *API) applied(ctx context.Context, result app.OperationResult, took time.Duration) {
	trace.SpanFromContext(ctx).SetAttributes(
		telemetry.Kind(result.Kind.String()),
		telemetry.Status(result.Status.String()),
		telemetry.FailureCode(result.FailureCode.String()),
		telemetry.Replay(result.IdempotentReplay),
	)
	a.telemetry.RecordOperation(ctx, telemetry.Operation{
		Source:      telemetry.SourceHTTP,
		Kind:        result.Kind.String(),
		Status:      result.Status.String(),
		FailureCode: result.FailureCode.String(),
		Replay:      result.IdempotentReplay,
		Took:        took,
	})
}
