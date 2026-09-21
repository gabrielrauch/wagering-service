package fxmod

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// Telemetry is the module every other one reports through: the logger, the
// OpenTelemetry SDK, and the position of the hooks that hand observability back
// before the process exits.
//
// # Why the invoke is here
//
// Fx builds what something asks for, when something asks for it, and it appends
// OnStop hooks in that order — so where a hook sits in the shutdown is decided
// by who asked for its constructor first. [newLogger] needs no help with that:
// fx.WithLogger forces it during fx.New, before any invoke runs, so its hook is
// the first appended and the last run, unconditionally.
//
// The SDK's providers have no such forcing, and two things hold them in place
// instead. Neither alone would be worth relying on, which is why both are here.
//
// The first is the dependency graph, and today it is sufficient: [newPool]
// takes a [telemetry.Telemetry], so the SDK is built before the pool and
// therefore before everything built from the pool — which is every remaining
// hook in this package. That is the same argument the pool's own position rests
// on, made one level further in.
//
// The second is [installTelemetry], and it is what keeps the first from being a
// coincidence. It is the device [checkDatabase] already is — an invoke whose
// only job is to make a constructor run — and it works because module invokes
// run in the order the modules are declared and this module is declared first
// in both [API] and [Worker], which is the property [checks] already rests on.
// It matters on the day somebody adds a hook-appending constructor that does
// NOT take telemetry: four constructors in this package already take no logger,
// so that day is not hypothetical, and without this the new hook would be
// appended ahead of the SDK's and the SDK shut down while it was still
// draining.
//
// Both are asserted. TestTelemetryIsFlushedAfterEverythingThatCouldStillEmit
// isolates the invoke on a graph shaped like this one, and
// TestTheSDKIsShutDownAfterThePoolOnTheRealGraph asserts the property itself
// against the whole worker.
//
// # What is exported, and when nothing is
//
// TELEMETRY_SHUTDOWN_TIMEOUT is the budget every Shutdown here runs under, and
// it is applied by each hook rather than inherited, for the reason
// [telemetry.flush] gives: the rollback after a start-up timeout is handed the
// context that has just expired.
//
// A process with no OTEL_EXPORTER_OTLP_ENDPOINT, or with OTEL_SDK_DISABLED,
// builds no exporter and no SDK provider at all. It gets OpenTelemetry's own
// no-op providers, appends no shutdown hook, and says so once at start-up.
// That is the difference between "switched off" and "misconfigured": a
// disabled process makes no network call and writes no error, where a process
// pointed at a collector that is not answering keeps running and reports each
// failed export through [otelErrors].
//
// # Sampling: there is none here, and what that costs
//
// The provider is built with no WithSampler, so it is OpenTelemetry's default —
// ParentBased(AlwaysSample). Every trace this service starts is recorded, and a
// trace arriving with a sampled traceparent is honoured.
//
// That is a deliberate deferral rather than an oversight, and the number it
// defers is large enough to write down. An IDLE worker replica, at the defaults
// in .env.example, exports of the order of eight hundred thousand spans a day:
//
//   - the reference loop, REFERENCE_WORKER_INTERVAL=1s, seven spans a turn —
//     app.Wagering.Resume opens a transaction whether or not anything is due,
//     so a turn that found nothing is the turn, the use case, the movement and
//     four statements — about six hundred thousand a day;
//   - the outbox loop, PUBLISHER_INTERVAL=1s, two spans a turn, about a hundred
//     and seventy thousand a day;
//   - the consumer's long polls, four receivers at twenty seconds, about
//     seventeen thousand a day.
//
// An idle API replica exports none: the only spans it opens are per request,
// and the two health endpoints are filtered out before otelhttp sees them —
// see [notAProbe].
//
// Sampling is not done here for the reason it is usually not: a head sampler in
// the process throws away the traces an operator most wants, because whether a
// trace is interesting is known at its END. The collector is where that
// decision belongs — tail sampling keeps the errors and the slow ones and drops
// the rest — and the collector is not this repository's. What this package owes
// that decision is the arithmetic above, so that whoever configures it knows
// what they are bounding. The three intervals are the other knob and they are
// environment variables, so a deployment that wants fewer spans and no
// collector rule can slow the loops down instead.
func Telemetry() fx.Option {
	return fx.Module("telemetry",
		fx.Provide(
			newLogger,
			newResource,
			newPropagator,
			newTracerProvider,
			newMeterProvider,
			newTelemetry,
		),
		fx.WithLogger(newEventLogger),
		fx.Invoke(installTelemetry),
	)
}

// installTelemetry exists to make the SDK exist, early.
//
// See [Telemetry] for why that matters and [checkDatabase] for the same device
// used for the same reason.
func installTelemetry(*telemetry.Telemetry) {}

// newLogger builds the logger every component in this process writes to.
//
// It also appends the hook that ends the process, and it does that here rather
// than from an invoke on purpose. An invoke's position depends on the order the
// modules were listed in, which is a convention somebody has to maintain; a
// constructor's position depends on what depends on it, which the compiler
// maintains. fx.WithLogger forces this one during fx.New, so it is first
// appended and last run whatever else the graph contains.
//
// The handler is wrapped so that every line names the trace it was written
// under. That is a wrapper rather than an argument each of the fifty or so log
// calls in this tree passes, because every one of them already carries a
// context and the only thing that could go wrong is somebody forgetting — see
// [telemetry.Correlate].
func newLogger(lc fx.Lifecycle, cfg config.Telemetry) *slog.Logger {
	options := &slog.HandlerOptions{Level: cfg.LogLevel}

	var handler slog.Handler
	switch cfg.LogFormat {
	case config.LogText:
		handler = slog.NewTextHandler(os.Stderr, options)
	case config.LogJSON:
		handler = slog.NewJSONHandler(os.Stderr, options)
	default:
		// Unreachable: config refuses anything else. Written as the JSON case
		// rather than as a panic, because a process that has already been told
		// what to log should not fail to start over how to format it.
		handler = slog.NewJSONHandler(os.Stderr, options)
	}

	// Stderr rather than stdout, and unbuffered. Stdout is where cmd/migrate
	// writes its one answer, and a container runtime interleaves the two; a
	// buffered writer would make the flush below load-bearing for ordinary log
	// lines, which is a far worse thing to depend on a deadline for.
	logger := slog.New(telemetry.Correlate(handler)).
		With(slog.String("service", cfg.ServiceName))

	lc.Append(fx.Hook{OnStop: (&lastWord{logger: logger, budget: cfg.ShutdownTimeout}).flush})
	return logger
}

// lastWord is what this process says on its way out, and the budget it says it
// under.
//
// Named for what it does rather than for what it is about, which is also how it
// avoids colliding with the internal/telemetry package this file imports under
// its own name.
type lastWord struct {
	logger *slog.Logger
	budget time.Duration
}

// flush is the last thing this process does.
//
// One line, and the line is not decoration: a shutdown that reached here is one
// where every hook before it returned, so its presence is how an operator tells
// a drain that completed from a process that was killed part way through one.
// The SDK's own Shutdown calls run immediately before it — they were appended
// immediately after this hook, and OnStop runs in reverse — so by the time this
// line is written there is nothing left that could still be exported.
//
// The budget replaces the caller's cancellation rather than sitting on top of
// it, and this is the one hook where that is right. It is the last thing that
// runs, including on the rollback Fx performs when a START hook fails — and a
// rollback after a start TIMEOUT is handed the context that just expired, so a
// budget derived from it would be no budget at all and the last word of a
// failed deployment would be the one thing not written. Values are kept, for
// the reason [stopContext] keeps them.
func (t *lastWord) flush(ctx context.Context) error {
	ctx, cancel := stopContext(ctx, t.budget)
	defer cancel()

	t.logger.InfoContext(ctx, "stopped")
	return nil
}

// newEventLogger routes Fx's own lifecycle events into this service's logger.
//
// At debug, because they are a hook-by-hook account of start-up and shutdown:
// invaluable when a process will not start, noise when one is running. Errors
// keep their own level, so a hook that failed is reported whatever the level is
// set to.
//
// Without this, Fx writes its events to stderr in its own format, so a
// deployment collecting structured logs would have one component writing
// something else — and that component is the one that reports why the process
// did not start.
func newEventLogger(logger *slog.Logger) fxevent.Logger {
	events := &fxevent.SlogLogger{Logger: logger}
	events.UseLogLevel(slog.LevelDebug)
	events.UseErrorLevel(slog.LevelError)
	return events
}

// newResource is what every span and every measurement this process emits is
// attributed to.
//
// service.name comes from the same SERVICE_NAME the logger reports under, which
// is the point: a log line and a span that disagree about which service wrote
// them are two halves of an incident nobody joins up. It is also the attribute
// the collector's Prometheus exporter turns into the `job` label — and
// Prometheus renames that to `exported_job` where it collides with its own
// scrape job, which is what a dashboard has to select on.
//
// # service.instance.id, and why a metric is wrong without it
//
// It says WHICH process emitted this, and its absence is not a missing detail
// — it is silent data loss. The collector's Prometheus exporter identifies a
// series by its labels, so five processes of one service emitting
// wagering.transactions with identical labels are not five series that sum:
// they are one series that each of them overwrites in turn. Measured on the
// running stack before this existed: three bets, one to each of three API
// replicas, moved the counter by ONE. Two vanished, with nothing logged
// anywhere — not by the collector, not by Prometheus, not by this service.
// Every counter in the catalogue was under-reporting by about the replica
// count, and the dashboard looked entirely plausible while doing it.
//
// Traces were never affected, which is why the end-to-end trace verification
// did not find this: a trace is identified by its trace id and does not care
// which process wrote a span.
//
// # The tension with dropping `publisher` from a counter, which is only apparent
//
// internal/telemetry deliberately removed a per-process name from
// wagering.outbox.publish_attempts because a pod name with a random suffix
// multiplies the series of a business counter for ever. This adds what is
// usually the same string. Both are right, because they are different places:
// on an INSTRUMENT, per-process identity multiplies every question that
// instrument answers by the number of processes that ever ran; on the RESOURCE,
// it IS the identity of the emitter, it is where Prometheus expects it — as the
// `instance` label — and without it the emitters are indistinguishable and
// therefore lossy. A dashboard sums over instances and asks its business
// question once; an operator drills into one instance when they need to.
//
// # Where the value comes from
//
// The operating system, not the configuration. It is the same string
// PUBLISHER_NAME falls back to and for the same reason — every container
// runtime sets the hostname to something distinct per replica, a pod name under
// Kubernetes and a container id under compose — but it is asked of the host
// rather than of the environment, because it is a fact about this process
// rather than a thing an operator tunes, and because a second variable to set
// is a second variable to forget.
//
// A host that will not say its own name loses the attribute rather than getting
// an empty one: an empty service.instance.id is not "unknown", it is every
// process claiming the same identity, which is the exact failure this exists to
// prevent. It is warned about, because the consequence is quiet.
//
// Merged with the SDK's default resource, which contributes the telemetry SDK's
// own name and version, and merged from a SCHEMALESS resource so that the merge
// cannot fail on a schema URL conflict. The alternative — naming a schema URL
// here — makes upgrading the SDK a change to this line, and the failure mode is
// a process that will not start over a version string.
func newResource(cfg config.Telemetry, logger *slog.Logger) (*resource.Resource, error) {
	attributes := []attribute.KeyValue{semconv.ServiceName(cfg.ServiceName)}

	host, err := os.Hostname()
	switch {
	case err != nil:
		logger.Warn("this host will not say its own name, so every process of this "+
			"service reports under one identity and their measurements overwrite "+
			"rather than sum",
			slog.String("attribute", string(semconv.ServiceInstanceIDKey)),
			slog.String("error", err.Error()))
	case host == "":
		logger.Warn("this host has no name, so every process of this service reports "+
			"under one identity and their measurements overwrite rather than sum",
			slog.String("attribute", string(semconv.ServiceInstanceIDKey)))
	default:
		attributes = append(attributes, semconv.ServiceInstanceID(host))
	}

	return resource.Merge(resource.Default(), resource.NewSchemaless(attributes...))
}

// newPropagator is how a trace crosses a boundary: an HTTP header, an outbox
// row, an SQS message attribute.
//
// W3C trace context and W3C baggage, in that order, and nothing else. No B3, no
// Jaeger: a service that accepted several formats would have to decide which
// one to emit, and the one it emitted would be the only one anything
// downstream had to understand anyway.
func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// newTracerProvider builds the trace provider and arranges for it to be flushed
// last.
//
// The exporter is built with a context of this constructor's own rather than
// the lifecycle's, because Fx runs constructors while the app is being built
// and there is no start context yet. It is bounded by the configured export
// timeout, and the bound is nearly ceremonial: otlptracegrpc dials lazily, so a
// collector that is not answering costs this call nothing and is discovered by
// the first export instead. That is exactly the behaviour this service wants —
// a process whose collector is down starts, works, and says so.
//
// The hook is appended here, where the provider is built, and that placement is
// the shutdown ordering. See [Telemetry].
func newTracerProvider(
	lc fx.Lifecycle, cfg config.Telemetry, res *resource.Resource, logger *slog.Logger,
) (trace.TracerProvider, error) {
	if !cfg.Exporting() {
		reportDisabled(logger, cfg, "traces")
		return tracenoop.NewTracerProvider(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ExportTimeout)
	defer cancel()

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpointURL(cfg.OTLPEndpoint),
		otlptracegrpc.WithTimeout(cfg.ExportTimeout),
	)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter, sdktrace.WithExportTimeout(cfg.ExportTimeout)),
	)
	lc.Append(fx.Hook{
		OnStop: shutdownWithin(cfg.ShutdownTimeout, logger, "traces", provider.Shutdown),
	})
	return provider, nil
}

// newMeterProvider builds the meter provider and arranges for it to be flushed
// last.
//
// The reader is periodic, and its interval is what decides how often the
// observable instruments are asked — the outbox lag among them, which is why
// that interval is documented next to the lag rather than only here.
func newMeterProvider(
	lc fx.Lifecycle, cfg config.Telemetry, res *resource.Resource, logger *slog.Logger,
) (metric.MeterProvider, error) {
	if !cfg.Exporting() {
		reportDisabled(logger, cfg, "metrics")
		return metricnoop.NewMeterProvider(), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ExportTimeout)
	defer cancel()

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpointURL(cfg.OTLPEndpoint),
		otlpmetricgrpc.WithTimeout(cfg.ExportTimeout),
	)
	if err != nil {
		return nil, err
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(cfg.MetricInterval),
			sdkmetric.WithTimeout(cfg.ExportTimeout),
		)),
	)
	lc.Append(fx.Hook{
		OnStop: shutdownWithin(cfg.ShutdownTimeout, logger, "metrics", provider.Shutdown),
	})
	return provider, nil
}

// newTelemetry is what every component in this tree is handed.
//
// It also installs the SDK's error handler, and this is the one place a global
// is touched. OpenTelemetry reports an export that failed through a
// process-wide handler and offers no per-provider alternative, so a service
// that wants "export failures are logged, not fatal" has to set it — and the
// default handler writes to the standard library's log package, which is
// precisely the component writing something other than this service's JSON.
//
// Nothing else global is set. otel.SetTracerProvider and its siblings would be
// a second way to reach objects every component is already handed explicitly,
// and a second way is a way for a test to be given one thing and instrument
// another.
func newTelemetry(
	tracers trace.TracerProvider,
	meters metric.MeterProvider,
	propagator propagation.TextMapPropagator,
	logger *slog.Logger,
) (*telemetry.Telemetry, error) {
	otel.SetErrorHandler(otelErrors{logger: logger})
	return telemetry.New(telemetry.Config{
		TracerProvider: tracers,
		MeterProvider:  meters,
		Propagator:     propagator,
	})
}

// otelErrors is where OpenTelemetry's own failures are reported.
//
// Warn rather than error, deliberately. What arrives here is an export that did
// not land — a collector restarting, a batch dropped because the queue was
// full — and none of it is a failure of the work this service exists to do. A
// deployment that paged on it would page on its observability rather than on
// its wagering.
type otelErrors struct{ logger *slog.Logger }

// Handle writes one failure. It has no context to write under, which is
// OpenTelemetry's interface rather than a choice here, so the line carries no
// trace of its own.
func (o otelErrors) Handle(err error) {
	o.logger.Warn("telemetry could not be exported",
		slog.String("error", err.Error()))
}

// reportDisabled says once, at start-up, that nothing will be exported.
//
// It is said rather than left to be noticed, because the failure it prevents is
// somebody looking for a trace that was never going to exist. The two reasons
// are told apart, because they need different actions: switched off is a
// decision, and no endpoint is a variable nobody set.
func reportDisabled(logger *slog.Logger, cfg config.Telemetry, what string) {
	if cfg.Disabled {
		logger.Info("telemetry is switched off, so no "+what+" are exported",
			slog.String("variable", "OTEL_SDK_DISABLED"))
		return
	}
	logger.Info("no collector is configured, so no "+what+" are exported",
		slog.String("variable", "OTEL_EXPORTER_OTLP_ENDPOINT"))
}

// shutdownWithin flushes one provider inside the telemetry budget, and reports
// a flush that did not finish without failing the shutdown.
//
// The error is swallowed on purpose, and it is the only place in this package
// that swallows one. Every other OnStop error here becomes a non-zero exit
// because it means work was abandoned; a batch of spans that did not reach a
// collector is not work, and a deployment that exited non-zero over it would be
// a deployment that fails when its observability does.
//
// The budget replaces the caller's cancellation for the reason [lastWord.flush]
// gives: this runs last, including on the rollback after a start-up timeout,
// where the context it would inherit has already expired.
func shutdownWithin(
	budget time.Duration, logger *slog.Logger, what string,
	shutdown func(context.Context) error,
) func(context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := stopContext(ctx, budget)
		defer cancel()

		if err := shutdown(ctx); err != nil {
			logger.WarnContext(ctx, "the last "+what+" could not be exported",
				slog.String("error", err.Error()))
		}
		return nil
	}
}
