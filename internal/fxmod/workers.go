package fxmod

import (
	"context"
	"fmt"
	"log/slog"

	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/workers"
)

// Consumer is the loop that takes wager operations off the inbound queue.
func Consumer() fx.Option {
	return fx.Module("consumer",
		fx.Provide(newConsumer),
		fx.Invoke(runConsumer),
	)
}

// Outbox is the loop that moves committed events onto the outbound queue.
//
// It is named for the table rather than for the loop, because that is what it
// is about: nothing is published before its transaction commits, so the queue
// only ever sees rows the database already agreed to.
func Outbox() fx.Option {
	return fx.Module("outbox",
		fx.Provide(newPublisher),
		fx.Invoke(runPublisher),
	)
}

// Reference is the loop that carries parked operations forward.
func Reference() fx.Option {
	return fx.Module("reference",
		fx.Provide(newReferenceWorker),
		fx.Invoke(runReferenceWorker),
	)
}

// newConsumer wires the consumer.
//
// The redrive policy is configuration rather than something read back off the
// queue, and it has to match what deploy/localstack/01-queues.sh provisions: it
// is how the consumer knows that a delivery is the last one before the
// dead-letter queue, which is the line an operator most wants in the log. Zero
// would mean it says nothing about it.
//
// The drain timeout should stay under the queue's visibility timeout, and that
// relationship is a convention this package does not enforce — internal/workers
// declines to be told the visibility timeout on the grounds that a second place
// to configure one is a place for the two to disagree, and overruling that here
// would create exactly the second place. What a drain past the visibility
// timeout buys is a release that quietly fails because the message came back on
// its own; .env.example says so next to both values.
func newConsumer(
	cfg config.Consumer,
	queues config.SQS,
	queue inboundQueue,
	wagering *app.Wagering,
	logger *slog.Logger,
) (*workers.Consumer, error) {
	settings := consumerSettings(cfg, queues)
	settings.Queue, settings.Wagering, settings.Logger = queue, wagering, logger
	return workers.NewConsumer(settings)
}

// consumerSettings is the configuration half of the consumer's wiring, apart
// from its dependencies.
//
// Apart, so that what the environment becomes can be asserted without a queue,
// a database and a container. Every field here is one a wrong value makes the
// system quietly rather than loudly wrong — a name that varied per replica, a
// redrive count of zero, a backoff that was dropped — and none of them shows up
// in a running process until the day it matters.
func consumerSettings(cfg config.Consumer, queues config.SQS) workers.ConsumerConfig {
	return workers.ConsumerConfig{
		Name:            cfg.Name,
		Concurrency:     cfg.Concurrency,
		Backoff:         backoffOf(cfg.Backoff),
		DrainTimeout:    cfg.DrainTimeout,
		MaxReceiveCount: queues.MaxReceiveCount,
	}
}

// runConsumer starts the loop and stops it within its drain deadline.
func runConsumer(lc fx.Lifecycle, consumer *workers.Consumer) {
	lc.Append(fx.Hook{
		OnStart: consumer.Start,
		OnStop:  stopWorker("consumer", consumer.Stop),
	})
}

// newPublisher wires the publisher.
//
// Name is the one value in this configuration that must differ between two
// processes running the same deployment. It lands in outbox.claimed_by, and a
// reschedule is scoped to the publisher holding the row, so two publishers
// sharing a name put each other's claims back — and because the outbox is
// head-of-line per wallet, a claim put back by the wrong process is a wallet's
// event stream stalled behind it. config.Publisher.Name is PUBLISHER_NAME or
// HOSTNAME and has no fixed default, which is what keeps that from happening
// silently on the day a second replica is deployed.
func newPublisher(
	cfg config.Publisher,
	claims *postgres.OutboxClaims,
	queue outboundQueue,
	clock app.Clock,
	logger *slog.Logger,
) (*workers.Publisher, error) {
	settings := publisherSettings(cfg)
	settings.Outbox, settings.Queue = claims, queue
	settings.Clock, settings.Logger = clock, logger
	return workers.NewPublisher(settings)
}

// publisherSettings is the configuration half of the publisher's wiring.
func publisherSettings(cfg config.Publisher) workers.PublisherConfig {
	return workers.PublisherConfig{
		Name:         cfg.Name,
		Batch:        cfg.Batch,
		Hold:         cfg.Hold,
		Interval:     cfg.Interval,
		Backoff:      backoffOf(cfg.Backoff),
		DrainTimeout: cfg.DrainTimeout,
	}
}

// runPublisher starts the loop and stops it within its drain deadline.
//
// Stop hands back every claim this publisher holds, through the pool — which is
// why the pool's close hook is appended where it is. See [newPool].
func runPublisher(lc fx.Lifecycle, publisher *workers.Publisher) {
	lc.Append(fx.Hook{
		OnStart: publisher.Start,
		OnStop:  stopWorker("publisher", publisher.Stop),
	})
}

// newReferenceWorker wires the reference worker.
//
// Name is the subject the service principal acts under and is kept for the
// audit trail alone, so it is stable rather than per-process: a row's trail
// should name the job and not whichever replica happened to run the turn.
func newReferenceWorker(
	cfg config.Reference, wagering *app.Wagering, logger *slog.Logger,
) (*workers.ReferenceWorker, error) {
	settings := referenceSettings(cfg)
	settings.Wagering, settings.Logger = wagering, logger
	return workers.NewReferenceWorker(settings)
}

// referenceSettings is the configuration half of the reference worker's wiring.
func referenceSettings(cfg config.Reference) workers.ReferenceConfig {
	return workers.ReferenceConfig{
		Name:         cfg.Name,
		Interval:     cfg.Interval,
		Backoff:      backoffOf(cfg.Backoff),
		DrainTimeout: cfg.DrainTimeout,
	}
}

// runReferenceWorker starts the loop and stops it within its drain deadline.
func runReferenceWorker(lc fx.Lifecycle, worker *workers.ReferenceWorker) {
	lc.Append(fx.Hook{
		OnStart: worker.Start,
		OnStop:  stopWorker("reference worker", worker.Stop),
	})
}

// backoffOf is the one conversion internal/config's independence costs.
//
// That package depends on nothing in this tree so that it is testable without
// any of it, which means it states a schedule in its own shape and this is
// where the shape becomes a worker's. Three fields, written once.
func backoffOf(b config.Backoff) workers.Backoff {
	return workers.Backoff{Initial: b.Initial, Factor: b.Factor, Max: b.Max}
}

// stopWorker names the loop in whatever its Stop reported.
//
// Every worker's Stop reports what it did not finish — receivers still holding
// messages, a turn that overran — and that report is the difference between a
// drain that completed and one that ran out of time. Carrying it out of the
// hook rather than logging it and returning nil is what makes those two
// different exit codes: Fx joins the errors from every OnStop hook, and a
// process that abandoned work mid-flight should not look like a clean
// deployment.
//
// The loop is named because the error itself does not always name it. A
// publisher and a consumer both report "the drain deadline of 20s passed", and
// an operator reading one line needs to know which loop left work behind.
func stopWorker(what string, stop func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := stop(ctx); err != nil {
			return fmt.Errorf("fxmod: the %s did not stop cleanly: %w", what, err)
		}
		return nil
	}
}
