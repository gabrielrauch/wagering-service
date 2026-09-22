// Command api serves the wagering HTTP API.
//
// It takes no arguments. Everything it needs comes from the environment and is
// checked before anything is built from it; .env.example lists every variable
// with a value that works against the local stack in deploy/.
//
// Exit codes. Nought, one and two are cmd/migrate's:
//
//	0  started, ran and stopped cleanly
//	1  the graph would not build, or a dependency could not be reached at
//	   start-up
//	2  the environment is not one this service can be configured from
//	3  it ran, and then failed to give its work back inside STOP_TIMEOUT
//
// An interrupt or a SIGTERM begins the shutdown: the listener drains within
// HTTP_SHUTDOWN_TIMEOUT, then the database pool closes, then telemetry is
// flushed — in that order, and the whole of it within STOP_TIMEOUT.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/fxmod"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(fxmod.Run(ctx, os.Stderr, "api", config.Environment, fxmod.API))
}
