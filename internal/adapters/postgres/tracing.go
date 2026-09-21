package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// The attributes one statement's span carries, in OpenTelemetry's own spelling
// because they are about the database rather than about this domain.
const (
	keyDBSystem = "db.system"
	keyDBQuery  = "db.query.text"
	dbSystem    = "postgresql"
)

// queryTracer puts one span around each statement, inside the transaction's.
//
// # What it never records
//
// The ARGUMENTS. pgx offers them and this deliberately drops every one: they
// are the amounts, the balances, the player identifiers and the idempotency
// keys of every operation this service performs, and a trace is read by more
// people than the database is. That is the single rule of this file, and it is
// why the tracer is written here rather than taken from a library — the
// available ones log arguments, some of them by default, and the setting that
// turns it off is one deployment away from being the setting somebody turns on
// while debugging.
//
// The statement TEXT is recorded, and that is safe for the opposite reason:
// every statement this package runs is a package-level constant with numbered
// placeholders, so the text carries no value anybody submitted. A tree that
// started composing SQL from input would have to revisit this.
type queryTracer struct{ telemetry *telemetry.Telemetry }

// queryContext is where the span travels between the two halves of pgx's
// tracer interface. It is an unexported key type, so nothing outside this
// package can set or shadow it.
type queryContext struct{}

// TraceQueryStart opens the statement's span.
//
// The span is a child of whatever is in the context, which inside a command is
// the transaction's — so a trace reads request, transaction, statement, in that
// order, and the statement that was slow is the one with the long bar.
func (q queryTracer) TraceQueryStart(
	ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	ctx, span := q.telemetry.Start(ctx, "postgres.query",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String(keyDBSystem, dbSystem),
			attribute.String(keyDBQuery, data.SQL),
		))
	return context.WithValue(ctx, queryContext{}, span)
}

// TraceQueryEnd closes it, and records the CLASS of a failure rather than its
// message.
//
// The class is what a reader of a trace can act on — may this be tried again —
// and it is also the one thing that is certainly safe to show: a driver's
// message carries whatever the server put in it, which on a constraint
// violation is the row that violated it.
func (q queryTracer) TraceQueryEnd(
	ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData,
) {
	span, ok := ctx.Value(queryContext{}).(trace.Span)
	if !ok {
		return
	}
	defer span.End()
	if data.Err != nil {
		q.telemetry.Failed(span, string(app.ClassOf(data.Err)))
	}
}
