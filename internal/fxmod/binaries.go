package fxmod

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/config"
)

// The exit codes both binaries use, kept the same as cmd/migrate's so that a
// script driving any of the three reads one table.
const (
	// ExitOK is a process that started, ran and stopped cleanly.
	ExitOK = 0
	// ExitFailed is a graph that would not build, a dependency that could not
	// be reached at start-up, or a shutdown that left work behind.
	ExitFailed = 1
	// ExitConfig is an environment this service cannot be configured from. It
	// is told apart from ExitFailed because nothing about the deployment is
	// wrong — a variable is — and a supervisor that restarts on one should not
	// restart on the other.
	ExitConfig = 2
)

// API is the graph cmd/api runs: the HTTP routes, the two application services
// beneath them, and the three dependencies they need.
//
// It runs no loops. A replica serving providers and a replica draining queues
// scale on different numbers — requests against wallets, messages against
// wallets — and one binary doing both would be scaled on whichever of the two
// somebody remembered.
func API(cfg config.Config) fx.Option {
	return fx.Options(
		lifecycle(cfg),
		Telemetry(),
		Config(cfg),
		Postgres(),
		SQS(),
		OIDC(),
		App(),
		HTTPServer(),
	)
}

// Worker is the graph cmd/worker runs: whichever of the three loops the
// environment switched on.
//
// A loop that is switched off is not in the graph rather than present and
// idle, which is what makes the queues conditional too: Fx builds a queue only
// for something that asks for one, so a worker running only the reference loop
// resolves no queue at start-up and needs no SQS at all.
//
// There is no identity provider here. Nothing a worker does is authenticated: a
// message on the inbound queue was authorised by whatever put it there, and the
// reference worker acts as the service itself.
func Worker(cfg config.Config) fx.Option {
	loops := make([]fx.Option, 0, 3)
	if cfg.Consumer.Enabled {
		loops = append(loops, Consumer())
	}
	if cfg.Publisher.Enabled {
		loops = append(loops, Outbox())
	}
	if cfg.Reference.Enabled {
		loops = append(loops, Reference())
	}
	if len(loops) == 0 {
		// Refused rather than run as a process that holds a pool open and does
		// nothing. A worker with every loop switched off is always a mistake,
		// and it is one that looks exactly like a healthy deployment: the
		// process starts, reports itself up and stays that way while its
		// queues fill.
		return fx.Error(errors.New(
			"fxmod: the worker was told to run no loops; set at least one of " +
				"CONSUMER_ENABLED, PUBLISHER_ENABLED or REFERENCE_WORKER_ENABLED"))
	}

	return fx.Options(append([]fx.Option{
		lifecycle(cfg),
		Telemetry(),
		Config(cfg),
		Postgres(),
		SQS(),
		App(),
	}, loops...)...)
}

// lifecycle bounds start-up and shutdown as a whole.
//
// These are the outer bounds and not the only ones. Every hook that reaches
// anything applies its own timeout as well, so a start that fails says which
// dependency it could not reach rather than only that thirty seconds passed.
func lifecycle(cfg config.Config) fx.Option {
	return fx.Options(
		fx.StartTimeout(cfg.Lifecycle.StartTimeout),
		fx.StopTimeout(cfg.Lifecycle.StopTimeout),
	)
}

// Run is the whole of both binaries: read the environment, build the graph,
// start it, wait, stop it.
//
// It is shared rather than written twice because the two would be the same
// fifty lines differing in one call, and the parts worth getting right — that a
// configuration failure exits differently from a start-up failure, and that the
// shutdown budget does not come from the context a signal just cancelled — are
// the parts that get copied wrongly.
//
// name prefixes every message, so a container's logs say which binary refused.
func Run(
	ctx context.Context,
	stderr io.Writer,
	name string,
	lookup config.Lookup,
	graph func(config.Config) fx.Option,
) int {
	cfg, err := config.Load(lookup)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: %v\n", name, err)
		return ExitConfig
	}

	application := fx.New(graph(cfg))
	if err := application.Err(); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: build the application: %v\n", name, err)
		return ExitFailed
	}

	startCtx, cancelStart := context.WithTimeout(ctx, application.StartTimeout())
	defer cancelStart()
	if err := application.Start(startCtx); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: start: %v\n", name, err)
		return ExitFailed
	}

	<-ctx.Done()

	stopCtx, cancelStop := stopContext(ctx, application.StopTimeout())
	defer cancelStop()
	if err := application.Stop(stopCtx); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: stop: %v\n", name, err)
		return ExitFailed
	}
	return ExitOK
}

// stopContext is the budget a shutdown runs under.
//
// Deliberately NOT derived from ctx, although it keeps its values. ctx is the
// one a signal has just cancelled — it is what woke the shutdown — so a budget
// taken from it is no budget at all: every drain would be handed a deadline
// that had already passed, the server would cut off the requests in flight and
// each worker would abandon its work, which is exactly the shutdown the drains
// exist to avoid. The values are kept because a trace or a correlation
// established at start-up belongs on the lines the shutdown writes.
func stopContext(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), budget)
}
