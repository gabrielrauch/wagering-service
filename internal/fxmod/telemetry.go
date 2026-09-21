package fxmod

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/gabrielrauch/wagering-service/internal/config"
)

// Telemetry is the module every other one reports through.
//
// # What it is today
//
// The logger, and the position of the hook that hands observability back before
// the process exits. The OpenTelemetry SDK is a later step and nothing here
// anticipates its shape: there is no exporter interface, no provider wrapper
// and no no-op implementation of anything. Inventing one now would mean
// designing an abstraction against a dependency nobody has added, and the first
// real exporter would be built through it or around it with equal probability.
//
// # Where the SDK goes
//
// Into this module, as two more providers — a trace provider and a meter
// provider — each of which appends its own Shutdown as an OnStop hook from its
// own constructor, exactly as [newLogger] appends [telemetry.flush]. That
// position is the whole of the seam, and it is not arbitrary: a constructor
// here runs before every constructor that depends on the logger, which is every
// constructor in this package that registers a hook at all, so a hook appended
// here is the first appended and therefore the last run. Whatever the SDK is
// told to flush, it is flushed after the server has drained, after the loops
// have stopped and after the pool has closed — which is to say after everything
// that could still emit a span has finished emitting.
//
// TELEMETRY_SHUTDOWN_TIMEOUT is the budget that flush runs under, and it is
// already applied. An exporter that is not answering must not hold a deployment
// open.
func Telemetry() fx.Option {
	return fx.Module("telemetry",
		fx.Provide(newLogger),
		fx.WithLogger(newEventLogger),
	)
}

// newLogger builds the logger every component in this process writes to.
//
// It also appends the hook that ends the process, and it does that here rather
// than from an invoke on purpose. An invoke's position depends on the order the
// modules were listed in, which is a convention somebody has to maintain; a
// constructor's position depends on what depends on it, which the compiler
// maintains. Every other hook in this package is appended by something that
// takes this logger, so this one is first, and first appended is last run.
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
	logger := slog.New(handler).With(slog.String("service", cfg.ServiceName))

	lc.Append(fx.Hook{OnStop: (&telemetry{logger: logger, budget: cfg.ShutdownTimeout}).flush})
	return logger
}

// telemetry is what this process has to hand back before it exits.
type telemetry struct {
	logger *slog.Logger
	budget time.Duration
}

// flush is the last thing this process does.
//
// Today that is one line, and the line is not decoration: a shutdown that
// reached here is one where every hook before it returned, so its presence is
// how an operator tells a drain that completed from a process that was killed
// part way through one. The OpenTelemetry SDK's Shutdown calls join it here —
// see [Telemetry].
//
// The budget is applied on top of the caller's context rather than instead of
// it, so a lifecycle that was given less keeps it.
func (t *telemetry) flush(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, t.budget)
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
