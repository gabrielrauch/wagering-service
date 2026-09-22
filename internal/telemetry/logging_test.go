package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// lines is a logger writing JSON into memory, wrapped the way newLogger wraps
// the real one.
type lines struct {
	buffer *bytes.Buffer
	logger *slog.Logger
}

func logging() *lines {
	buffer := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buffer, &slog.HandlerOptions{Level: slog.LevelDebug})
	return &lines{buffer: buffer, logger: slog.New(Correlate(handler))}
}

// only reads back the one line that was written.
func (l *lines) only(t *testing.T) map[string]any {
	t.Helper()
	written := strings.TrimSpace(l.buffer.String())
	if written == "" {
		t.Fatal("nothing was written")
	}
	if strings.Contains(written, "\n") {
		t.Fatalf("%d lines were written, wanted one:\n%s",
			strings.Count(written, "\n")+1, written)
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(written), &record); err != nil {
		t.Fatalf("the line is not JSON: %v\n%s", err, written)
	}
	return record
}

// TestALineWrittenUnderASpanNamesIt pins the whole of the log-to-trace link.
//
// traceId and spanId go on from the CONTEXT rather than from an argument at
// each of the fifty-odd log calls in this tree, so there is nothing anybody can
// forget. The second half is as load-bearing as the first: a line written with
// no span carries neither member, because an identifier for a span nobody
// exported resolves to nothing in Tempo and a link that is always broken is
// worse than no link.
func TestALineWrittenUnderASpanNamesIt(t *testing.T) {
	t.Parallel()

	t.Run("under a span", func(t *testing.T) {
		t.Parallel()
		r := record(t)
		l := logging()

		ctx, span := r.Start(t.Context(), "submit an operation")
		l.logger.InfoContext(ctx, "the operation was applied")
		span.End()

		ended := r.only(t)
		written := l.only(t)
		if got := written[KeyTraceID]; got != ended.SpanContext().TraceID().String() {
			t.Errorf("the line names trace %v, wanted %s",
				got, ended.SpanContext().TraceID())
		}
		if got := written[KeySpanID]; got != ended.SpanContext().SpanID().String() {
			t.Errorf("the line names span %v, wanted %s",
				got, ended.SpanContext().SpanID())
		}
	})

	t.Run("with no span at all", func(t *testing.T) {
		t.Parallel()
		l := logging()

		l.logger.InfoContext(context.Background(), "stopped")

		written := l.only(t)
		if _, named := written[KeyTraceID]; named {
			t.Errorf("a line with no span named a trace: %v", written)
		}
		if _, named := written[KeySpanID]; named {
			t.Errorf("a line with no span named a span: %v", written)
		}
	})
}

// TestWithKeepsTheCorrelationOn pins the one way this wrapper silently stops
// working.
//
// newLogger does logger.With(service) before anything else sees it, and every
// worker and adapter in this tree logs through that logger. Embedding alone
// would hand back the WRAPPED handler from With, so the trace would be on
// exactly the lines nobody writes and off every line the service does.
func TestWithKeepsTheCorrelationOn(t *testing.T) {
	t.Parallel()
	r := record(t)
	l := logging()

	ctx, span := r.Start(t.Context(), "submit an operation")
	l.logger.With(slog.String("service", "wagering")).
		With(slog.String("consumer", "wager-consumer")).
		InfoContext(ctx, "the operation was applied")
	span.End()

	written := l.only(t)
	if got := written[KeyTraceID]; got != r.only(t).SpanContext().TraceID().String() {
		t.Errorf("a line written through With names trace %v, wanted the span's", got)
	}
	if written["service"] != "wagering" || written["consumer"] != "wager-consumer" {
		t.Errorf("the wrapper dropped the attributes it was given: %v", written)
	}
}

// TestCorrelateRefusesNothingAndInventsNothing pins the two edges.
//
// A nil handler comes back nil rather than as a wrapper around nothing, so a
// caller building a logger sees its own mistake at the point it made it.
func TestCorrelateRefusesNothingAndInventsNothing(t *testing.T) {
	t.Parallel()
	if Correlate(nil) != nil {
		t.Error("a nil handler became a wrapper around nothing")
	}
}
