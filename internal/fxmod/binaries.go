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
	// ExitUnclean is a process that started, ran, and then failed to give its
	// work back inside the shutdown budget.
	//
	// Its own code because it needs two different audiences to do two different
	// things. A supervisor should restart it, exactly as it would on
	// ExitFailed; an operator should also look, because messages were abandoned
	// mid-flight or outbox claims were left held, and that is a latency
	// incident somebody will otherwise meet as a mystery. Collapsed into
	// ExitFailed it is invisible, because a failed start and an abandoned drain
	// then look identical from outside.
	ExitUnclean = 3
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
		// It has somewhere to report an outage, so an outage does not stop it
		// starting. See [queueStartUp].
		fx.Supply(queueStartUp{tolerateOutage: true}),
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
		// It has no readiness endpoint to shed traffic through and no buffer of
		// its own, so a queue it cannot reach stops it. See [queueStartUp].
		fx.Supply(queueStartUp{tolerateOutage: false}),
		Telemetry(),
		Config(cfg),
		Postgres(),
		SQS(),
		App(),
		// Before the loops, deliberately. See [checks].
		checks(cfg),
	}, loops...)...)
}

// checks forces every start-up validation this binary makes to be registered
// before any loop's Start hook is.
//
// Without it the order is the order Fx happens to construct things in, and that
// interleaves: the consumer module's invoke builds the inbound queue and then
// appends the consumer's Start, so the outbound queue is resolved AFTER the
// consumer has begun handling messages. A worker with a wrong
// SQS_OUTBOUND_QUEUE would consume operations, commit transactions and write
// outbox rows and only then fail to start. Nothing is corrupted by that — the
// drain releases what is held, the outbox is durable and the inbox absorbs the
// replays — but it is not "nothing starts half up", which is what this package
// promises.
//
// It is conditional on the same flags [Worker] is, so it forces exactly the
// queues this binary will use and no others: a worker running only the
// reference loop still resolves nothing.
//
// A module rather than bare invokes, and that is load-bearing rather than
// tidiness. Fx runs every CHILD module's invokes before the parent's own, so a
// root-level fx.Invoke listed here would run last of all — after every loop had
// started, which is the ordering this exists to prevent. Inside a module it
// runs in declaration order with the rest.
func checks(cfg config.Config) fx.Option {
	forced := make([]fx.Option, 0, 2)
	if cfg.Consumer.Enabled {
		forced = append(forced, fx.Invoke(resolveInboundFirst))
	}
	if cfg.Publisher.Enabled {
		forced = append(forced, fx.Invoke(resolveOutboundFirst))
	}
	return fx.Module("checks", forced...)
}

// The two invokes that do nothing but exist earlier than a loop does.
func resolveInboundFirst(inboundQueue)   {}
func resolveOutboundFirst(outboundQueue) {}

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

	// Two ways to end, not one. A signal is the ordinary one; the other is a
	// component deciding this process can no longer do its job and asking for
	// it through the fx.Shutdowner every graph here is given. Without the
	// second there is no way for a graph that has stopped working to say so,
	// and the only failure mode left is the worst one — a process that is up,
	// healthy-looking and doing nothing.
	select {
	case <-ctx.Done():
	case <-application.Wait():
	}

	stopCtx, cancelStop := stopContext(ctx, application.StopTimeout())
	defer cancelStop()
	if err := application.Stop(stopCtx); err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: stop: %v\n", name, err)
		return ExitUnclean
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
