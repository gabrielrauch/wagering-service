package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// The two members a log line carries so that it and a span can be put beside
// each other.
//
// Spelled as Tempo and Grafana spell them in a derived field, rather than as
// OpenTelemetry's trace_id and span_id: the log lines this service writes are
// camelCase throughout — correlationId, messageId, transactionId — and one line
// carrying two conventions is one that has to be read twice.
const (
	KeyTraceID = "traceId"
	KeySpanID  = "spanId"
)

// correlated is a handler that adds the current trace to every line.
//
// A handler rather than something each call site does, and that is the whole
// design: there are around fifty log calls in this tree and each of them
// already passes a context, so the one thing that could go wrong is somebody
// forgetting. Here there is nothing to forget — a line written under a span
// carries the span, and a line written without one carries neither member
// rather than two empty strings.
type correlated struct{ slog.Handler }

// Correlate wraps a handler so that every line it writes names the trace it was
// written under.
//
// It refuses nothing and defaults nothing: a nil handler is returned as nil, so
// that a caller building a logger sees its own mistake rather than a logger
// that writes into a wrapper around nothing.
func Correlate(h slog.Handler) slog.Handler {
	if h == nil {
		return nil
	}
	return correlated{Handler: h}
}

// Handle adds traceId and spanId when the context is carrying a recorded span.
//
// The attributes are added to the record rather than to the handler, because a
// handler is built once per process and a trace changes per line.
//
// An unrecorded span — telemetry switched off, or a sampler that declined this
// trace — adds nothing. That is deliberate rather than an omission: an
// identifier for a span nobody exported is an identifier that resolves to
// nothing in Tempo, and a link that is always broken is worse than no link.
func (c correlated) Handle(ctx context.Context, record slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		record.AddAttrs(
			slog.String(KeyTraceID, sc.TraceID().String()),
			slog.String(KeySpanID, sc.SpanID().String()),
		)
	}
	return c.Handler.Handle(ctx, record)
}

// WithAttrs and WithGroup keep the wrapper on, which is what makes
// logger.With(...) — which every component here is built with — still
// correlate.
//
// Without them, embedding would hand back the WRAPPED handler and the very
// first With in this tree would quietly take the trace off every line after it.
// newLogger does exactly one, for the service name, so this is not a corner:
// it is every line the process writes.
//
// # What WithGroup costs
//
// A group opened here puts traceId and spanId inside it, because the members
// are added to the record and the record is rendered under whatever groups the
// handler is holding. Nothing in this tree opens a group — the lines are flat
// by design, so that one field is one query — and the correct fix is to
// remember the group state and add the two members outside it, which is
// bookkeeping for a case that does not exist. It is written down rather than
// left to be discovered.
func (c correlated) WithAttrs(attrs []slog.Attr) slog.Handler {
	return correlated{Handler: c.Handler.WithAttrs(attrs)}
}

func (c correlated) WithGroup(name string) slog.Handler {
	if name == "" {
		return c
	}
	return correlated{Handler: c.Handler.WithGroup(name)}
}
