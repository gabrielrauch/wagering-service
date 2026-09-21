package telemetry

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// ScopeName is the instrumentation scope every span and every instrument in
// this service is recorded under.
//
// The module path, which is what OpenTelemetry's own convention asks for: a
// scope names the code that produced the telemetry rather than the service
// that ran it, so that a collector handling several services can still tell
// which library emitted a span.
const ScopeName = "github.com/gabrielrauch/wagering-service"

// The spans this service opens, by name.
//
// They are constants rather than literals at each call site because a span
// name is a query somebody writes in Tempo, and a name that differs by a word
// between two code paths is a query that silently misses half of them. Four of
// them are entry points — an HTTP request, a consumed message, a publish batch
// and a resume turn — and the rest are children.
const (
	// SpanRequest is the name an HTTP request's span is opened under, before
	// the mux has matched a route. It is replaced with "METHOD /the/{route}"
	// the moment a route is known, so a span carrying this name is one that
	// never reached a handler.
	SpanRequest = "http.request"
	// SpanConsume covers one message: reading it, applying it, and deciding
	// what becomes of it.
	SpanConsume = "consume wager operation"
	// SpanPublishBatch covers one turn of the publisher: the claim, the send
	// and the marking.
	SpanPublishBatch = "publish outbox batch"
	// SpanPublishEvent covers one event within that batch, and it is the span
	// that rejoins the trace the operation was submitted under — see
	// [Telemetry.Link].
	SpanPublishEvent = "publish wallet event"
	// SpanResume covers one reference worker turn.
	SpanResume = "resume parked operation"

	// SpanMovement and SpanSnapshot are the two SQL transactions, named for
	// what they are allowed to do rather than for the statements inside them.
	SpanMovement = "postgres movement"
	SpanSnapshot = "postgres snapshot"

	// SpanSubmit, SpanResumeUseCase and the rest are the application layer's
	// own doors, named as that package names them.
	SpanSubmit                  = "Wagering.Submit"
	SpanResumeUseCase           = "Wagering.Resume"
	SpanTransactionByID         = "Wagering.TransactionByID"
	SpanTransactionByExternalID = "Wagering.TransactionByExternalID"
	SpanOpenWallet              = "Wallets.Open"
	SpanWalletByID              = "Wallets.ByID"
	SpanLedger                  = "Wallets.Ledger"
	SpanReconcile               = "Wallets.Reconcile"
)

// Config is what a [Telemetry] is built from.
//
// Every field is optional and a nil one becomes the corresponding no-op, so a
// partly configured process — traces exported, metrics not — is a thing that
// can be built rather than a start-up failure. Which of them are supplied is
// the composition root's decision and is made from the environment.
type Config struct {
	// TracerProvider is where spans go. Nil means nowhere.
	TracerProvider trace.TracerProvider
	// MeterProvider is where measurements go. Nil means nowhere.
	MeterProvider metric.MeterProvider
	// Propagator is how a trace crosses a boundary. Nil means it does not: an
	// incoming traceparent is ignored and nothing is injected.
	Propagator propagation.TextMapPropagator
}

// Telemetry is the tracer, the propagator and the instruments one component
// reports through.
//
// Safe for concurrent use, and every method is safe on a nil receiver — a
// component handed no telemetry behaves exactly as one handed [Disabled],
// which is what keeps the branch out of every call site. See the package
// documentation.
type Telemetry struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	metrics    *instruments
}

// New builds the telemetry one process reports through.
//
// It fails only if an instrument cannot be created, which a meter provider
// does by refusing a malformed name — and that is a defect in this package
// rather than a condition, so it is reported at construction where the process
// has not started yet.
func New(cfg Config) (*Telemetry, error) {
	tracers := cfg.TracerProvider
	if tracers == nil {
		tracers = tracenoop.NewTracerProvider()
	}
	meters := cfg.MeterProvider
	if meters == nil {
		meters = metricnoop.NewMeterProvider()
	}
	propagator := cfg.Propagator
	if propagator == nil {
		// An empty composite, not nil: it satisfies the interface, injects
		// nothing and extracts nothing, so the Inject and Extract paths below
		// need no guard of their own.
		propagator = propagation.NewCompositeTextMapPropagator()
	}
	instruments, err := newInstruments(meters.Meter(ScopeName))
	if err != nil {
		return nil, err
	}
	return &Telemetry{
		tracer:     tracers.Tracer(ScopeName),
		propagator: propagator,
		metrics:    instruments,
	}, nil
}

// disabled is the one value every component that was handed nothing shares.
//
// Memoised, because it is asked for once per construction and there is nothing
// in it that a second copy would keep apart: a no-op instrument holds no state.
var disabled = sync.OnceValue(func() *Telemetry {
	t, err := New(Config{})
	if err != nil {
		// Unreachable: New only fails on an instrument name a no-op meter does
		// not check. Panicking rather than returning an error keeps every
		// caller of Disabled free of a branch that cannot be taken.
		panic("telemetry: the no-op instruments could not be built: " + err.Error())
	}
	return t
})

// Disabled is telemetry that records nothing.
//
// It is a real value rather than nil and a real value rather than an
// interface, so that switching telemetry off changes what is recorded and
// nothing about what runs. A component given nil uses it — see [Or].
func Disabled() *Telemetry { return disabled() }

// Or is what a constructor calls on a telemetry field it was not given.
//
// The alternative — refusing nil the way every other dependency here is
// refused — would make telemetry a thing every test, every fake and every
// hand-built component had to supply in order to compile, which is precisely
// the tax that makes instrumentation get skipped.
func Or(t *Telemetry) *Telemetry {
	if t == nil {
		return Disabled()
	}
	return t
}

// Start opens a span, and returns the context the work below it runs in.
//
// The caller ends it. There is no variant here that ends it for you: a span
// that closed at the end of the function that opened it would be the wrong
// span in every case this service has, because each of them ends where a
// transaction commits or a message is decided about rather than where a Go
// function returns.
func (t *Telemetry) Start(
	ctx context.Context, name string, opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	if t == nil {
		return Disabled().Start(ctx, name, opts...)
	}
	return t.tracer.Start(ctx, name, opts...)
}

// Failed marks a span as failed, naming a CODE and never an error.
//
// This is the one door, and it takes no error on purpose. app.ErrForeignOperation
// must never be rendered to anybody, a driver's message carries whatever a
// query put in it, and a span is read by more people than a log is — so the
// rule this package enforces is that no rendered error reaches a span at all.
// What reaches one is a failure.Code, an app.Class or one of this package's own
// words, all of which are published vocabularies. The message is still written
// once, to the log, where the adapter that owns the refusal decides what may be
// said.
func (t *Telemetry) Failed(span trace.Span, code string, attrs ...attribute.KeyValue) {
	if span == nil {
		return
	}
	span.SetStatus(codes.Error, code)
	span.SetAttributes(append(Some(attrs...), attribute.String(KeyCode, code))...)
}

// Inject renders the context's trace as the carriage a queue message or an
// outbox row travels with.
//
// It answers nil when there is nothing to carry — telemetry switched off, or a
// context with no span in it — so that a caller writing the result beside a
// message adds no attribute rather than an empty one, and a row stores no
// member rather than an empty object.
func (t *Telemetry) Inject(ctx context.Context) map[string]string {
	if t == nil {
		return nil
	}
	carrier := propagation.MapCarrier{}
	t.propagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

// Extract reads the trace a message arrived carrying, so that what this
// service does next is part of it rather than a second trace beside it.
//
// A carrier that names no trace, or names one this process cannot parse,
// returns the context unchanged — which is a new trace rooted here, and is the
// right answer for a message somebody put on the queue by hand.
func (t *Telemetry) Extract(ctx context.Context, carried map[string]string) context.Context {
	if t == nil || len(carried) == 0 {
		return ctx
	}
	return t.propagator.Extract(ctx, propagation.MapCarrier(carried))
}

// Link points a span at one it is caused by but is not inside.
//
// One place uses it and it is worth naming: the publisher's per-event span is a
// CHILD of the trace the operation was submitted under — that is the whole
// point, one trace from the request to the published event — and a LINK back to
// the batch turn that happened to send it. It cannot be a child of both, and
// the trace an operator follows is the operation's; the batch is the operational
// fact, reachable from either end.
//
// A context with no span in it yields a link the SDK discards, so a publisher
// running with telemetry switched off needs no branch here either.
func Link(ctx context.Context, attrs ...attribute.KeyValue) trace.SpanStartOption {
	return trace.WithLinks(trace.LinkFromContext(ctx, attrs...))
}

// TraceID and SpanID are the identifiers a log line carries so that a line and
// a span can be put beside each other. They answer "" for a context carrying no
// recorded span, which is what a disabled process has everywhere.
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

func SpanID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.SpanID().String()
}
