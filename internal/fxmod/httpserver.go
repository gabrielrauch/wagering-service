package fxmod

import (
	"log/slog"

	"go.uber.org/fx"

	httpapi "github.com/gabrielrauch/wagering-service/internal/adapters/http"
	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/config"
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
	})
}

// newServer wires the server. It binds nothing; [serve] does that.
func newServer(
	api *httpapi.API, cfg config.HTTP, logger *slog.Logger,
) (*httpapi.Server, error) {
	return httpapi.NewServer(httpapi.ServerConfig{
		Addr:              cfg.Addr,
		Handler:           api,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ShutdownTimeout:   cfg.ShutdownTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
		Logger:            logger,
	})
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
