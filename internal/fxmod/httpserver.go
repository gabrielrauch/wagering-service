package fxmod

import (
	"log/slog"
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	httpapi "github.com/gabrielrauch/wagering-service/internal/adapters/http"
	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// HTTPServer is the API and the server that carries it. cmd/api only.
func HTTPServer() fx.Option {
	return fx.Module("httpserver",
		fx.Provide(
			newReadiness,
			newAPI,
			newServer,
		),
		fx.Invoke(serve),
	)
}

// newReadiness names the dependencies /health/ready reports on.
//
// The keys are what a caller reads in the body, so they are the two words an
// operator would use. Both checks bound themselves, and the endpoint bounds the
// pair again — a probe that inherited its caller's deadline would answer
// neither ready nor unready to an orchestrator that set none, which is the one
// answer nothing can act on.
func newReadiness(
	database *postgres.Health, queue queueReadiness,
) map[string]httpapi.ReadinessCheck {
	return map[string]httpapi.ReadinessCheck{
		"postgres": database,
		"sqs":      queue,
	}
}

// newAPI wires the router and the handlers.
func newAPI(
	wagering *app.Wagering,
	wallets *app.Wallets,
	authenticator *oidc.Authenticator,
	readiness map[string]httpapi.ReadinessCheck,
	cfg config.HTTP,
	reporting *telemetry.Telemetry,
	logger *slog.Logger,
) (*httpapi.API, error) {
	return httpapi.New(httpapi.Config{
		Wagering:         wagering,
		Wallets:          wallets,
		Authenticator:    authenticator,
		Readiness:        readiness,
		ReadinessTimeout: cfg.ReadinessTimeout,
		MaxBodyBytes:     cfg.MaxBodyBytes,
		Logger:           logger,
		Telemetry:        reporting,
	})
}

// newServer wires the server, with the API wrapped so that every request is a
// span. It binds nothing; [serve] does that.
//
// The wrapping is here rather than inside internal/adapters/http, and that is
// the same decision this package makes everywhere: the adapter takes a
// [telemetry.Telemetry] and describes its own work, and which SDK carries it is
// the composition root's business. It also keeps otelhttp out of every handler
// test, which would otherwise need a tracer provider to build an API.
//
// The providers are passed explicitly rather than left to otelhttp's defaults,
// which read the process-wide globals. Nothing here sets those — see
// [newTelemetry] — so the default would be a no-op provider and every request
// span would silently not exist.
//
// The span is opened under one fixed name and renamed once the mux has matched
// a route; see [httpapi.dispatched], which is the only place the route pattern
// is known.
func newServer(
	api *httpapi.API,
	cfg config.HTTP,
	tracers trace.TracerProvider,
	meters metric.MeterProvider,
	propagator propagation.TextMapPropagator,
	logger *slog.Logger,
) (*httpapi.Server, error) {
	return httpapi.NewServer(httpapi.ServerConfig{
		Addr: cfg.Addr,
		Handler: otelhttp.NewHandler(api, telemetry.SpanRequest,
			otelhttp.WithTracerProvider(tracers),
			otelhttp.WithMeterProvider(meters),
			otelhttp.WithPropagators(propagator),
			otelhttp.WithFilter(notAProbe),
		),
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ShutdownTimeout:   cfg.ShutdownTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
		Logger:            logger,
	})
}

// notAProbe keeps the health endpoints out of the traces.
//
// An orchestrator polls /health/live and /health/ready every few seconds per
// replica, for ever, and each poll would be a trace containing one span that
// says a probe answered. That is more spans than this service's actual traffic
// on a quiet deployment, it costs storage in Tempo for as long as the retention
// is, and nothing is ever looked up by it: a probe that FAILS is visible in the
// readiness body, in the log line the check writes, and in the orchestrator's
// own events.
//
// Filtered here rather than sampled at the collector, because a filter is a
// sentence somebody can read in the code that serves them and a sampler is a
// rule in another repository's configuration.
func notAProbe(r *http.Request) bool {
	return !strings.HasPrefix(r.URL.Path, "/health/")
}

// serve binds the listener at start-up and drains it at shutdown.
//
// Start binds synchronously and only then serves, so an address already in use
// is a start-up failure with the rest of the lifecycle rolled back — rather than
// a process that reported itself up and is listening on nothing. It is appended
// last of everything in this binary, because the server is built from every
// other component, so it is the first hook Stop runs: requests in flight are
// drained while the database pool they are using is still open.
//
// Stop reports a drain that hit its deadline, and that error is carried out
// rather than swallowed, so a deployment that cut requests off does not exit
// zero.
func serve(lc fx.Lifecycle, server *httpapi.Server) {
	lc.Append(fx.Hook{OnStart: server.Start, OnStop: server.Stop})
}
