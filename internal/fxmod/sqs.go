package fxmod

import (
	"context"
	"errors"
	"log/slog"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// The two queues, as two types.
//
// Both are the same adapter — an SQS queue is symmetric and [sqs.Queue] can do
// everything either direction needs — so what distinguishes them is which one a
// component is handed, and that is a decision only this package makes. Naming
// them keeps it from being made by argument position: a consumer built on the
// outbound queue and a publisher built on the inbound one both compile
// perfectly, and the first would receive the events it was meant to send.
//
// Two named types rather than Fx's named values or an fx.In parameter struct.
// A named value has to be requested with fx.Annotate(..., fx.ParamTags(...)),
// and ParamTags is one tag per parameter in positional order — a long
// positional list standing in for exactly the distinction these two types make
// in the signature itself. An fx.In struct would avoid the positions and add a
// declared struct per constructor to say what two type names already say.
type (
	// inboundQueue is the FIFO queue wager operations arrive on.
	inboundQueue struct{ *sqs.Queue }
	// outboundQueue is the FIFO queue wallet events leave on.
	outboundQueue struct{ *sqs.Queue }
)

// queueStartUp is what a process does when a queue cannot be resolved while it
// is starting.
//
// The distinction it carries is not "how important is SQS" but "can this
// process tell anybody it is not working". A queue name nobody provisioned
// stops every process: the deployment is wrong, no amount of waiting fixes it,
// and an operator should see it now. A queue that is momentarily unreachable is
// a different question, and the answer differs by binary.
type queueStartUp struct {
	// tolerateOutage lets a transient failure through, for a process that has
	// somewhere to report the consequence.
	//
	// cmd/api sets it. The API never calls SQS on the request path — a
	// submission becomes a row and an outbox row, and a separate publisher
	// sends it — so the outbox is precisely the buffer an SQS outage is
	// supposed to be absorbed by, and an API that refuses to start throws that
	// buffer away. Worse, it crash-loops: when SQS comes back there is no warm
	// capacity to take the recovery, only a cold start. Starting and reporting
	// unready sheds the traffic while the outage lasts and takes it back
	// without a human, which is what /health/ready is for.
	//
	// cmd/worker does not set it, and the reason is the absence of that
	// endpoint. A worker serves no readiness probe and has no buffer of its
	// own: every loop it runs is the queue. A worker that started without one
	// would be a process holding a pool open and consuming nothing while
	// looking exactly like a healthy deployment — the state the "no loops is
	// refused" guard in [Worker] exists to prevent.
	tolerateOutage bool
}

// SQS is the client and the queues built on it.
//
// Nothing here is unconditional: Fx builds a queue only if something asks for
// one, so a worker running only the reference loop resolves no queue and a
// worker with no publisher never touches the outbound one. That is also why
// each queue's start-up resolution is appended by its own constructor rather
// than by an invoke — a hook registered from an invoke would run for a queue
// this binary does not use.
func SQS() fx.Option {
	return fx.Module("sqs",
		fx.Provide(
			newSQSClient,
			newInboundQueue,
			newOutboundQueue,
			newQueueReadiness,
		),
	)
}

// newSQSClient builds the client both queues are made from.
//
// It names no credential and cannot: the SDK's default chain supplies them, so
// a deployment provides them the way it provides them to everything else and
// this service's configuration has nothing in it worth stealing. The bound is
// there because that chain reaches the instance metadata service on an EC2
// host, and an address that is black-holed rather than refused would otherwise
// hang start-up with nothing to report.
func newSQSClient(cfg config.SQS, reporting *telemetry.Telemetry) (*awssqs.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ClientTimeout)
	defer cancel()

	return sqs.NewClient(ctx, sqs.ClientConfig{
		Region:    cfg.Region,
		Endpoint:  cfg.Endpoint,
		Telemetry: reporting,
	})
}

// newInboundQueue wires the queue the consumer receives on, and makes resolving
// its name a start-up check.
//
// Resolution is worth being precise about: GetQueueUrl fails for a queue that
// does not exist, in an account this process cannot reach, or in a region it
// was misconfigured with — which is every way the deployment can be wrong about
// a queue. Every other method on the adapter refuses to run until it has
// happened, so a hook that was skipped cannot become a receive against an empty
// URL.
func newInboundQueue(
	lc fx.Lifecycle, cfg config.SQS, client *awssqs.Client,
	policy queueStartUp, logger *slog.Logger,
) (inboundQueue, error) {
	queue, err := sqs.NewQueue(sqs.Config{
		Client:            client,
		Name:              cfg.InboundQueue,
		ResolveTimeout:    cfg.ResolveTimeout,
		MaxMessages:       cfg.MaxMessages,
		WaitTime:          cfg.WaitTime,
		VisibilityTimeout: cfg.VisibilityTimeout,
	})
	if err != nil {
		return inboundQueue{}, err
	}
	lc.Append(fx.Hook{OnStart: resolveQueue(queue, policy, logger)})
	return inboundQueue{Queue: queue}, nil
}

// newOutboundQueue wires the queue the publisher sends on.
//
// The receive bounds are left at the adapter's defaults: nothing receives from
// this one, and configuring a long poll for a queue nobody polls would be a
// number an operator could tune with no effect. The visibility timeout is left
// open for the same reason — it is a property of the queue, and saying nothing
// here means the queue's own provisioned value stands.
func newOutboundQueue(
	lc fx.Lifecycle, cfg config.SQS, client *awssqs.Client,
	policy queueStartUp, logger *slog.Logger,
) (outboundQueue, error) {
	queue, err := sqs.NewQueue(sqs.Config{
		Client:         client,
		Name:           cfg.OutboundQueue,
		ResolveTimeout: cfg.ResolveTimeout,
	})
	if err != nil {
		return outboundQueue{}, err
	}
	lc.Append(fx.Hook{OnStart: resolveQueue(queue, policy, logger)})
	return outboundQueue{Queue: queue}, nil
}

// resolveQueue is the start-up check, and the rule for what a failure means.
//
// The rule rests on a classification the adapter already makes, so nothing here
// has to guess: a queue that does not exist is [app.Unretryable], and a refused
// connection, a timeout or a throttle is [app.Retryable]. That is exactly the
// line between "the deployment is wrong" and "AWS is having a morning", and it
// is the line [queueStartUp] turns on.
//
// An unresolved queue does not stay unresolved for ever on the tolerant path.
// Queue.OnStart may be called again — it says so — and [queueReadiness] calls
// it, so a process that started during an outage becomes ready the moment the
// queue answers, without a restart and without a human.
func resolveQueue(
	queue *sqs.Queue, policy queueStartUp, logger *slog.Logger,
) func(context.Context) error {
	return func(ctx context.Context) error {
		err := queue.OnStart(ctx)
		switch {
		case err == nil:
			return nil
		case !policy.tolerateOutage, app.ClassOf(err) != app.Retryable:
			return err
		}
		logger.WarnContext(ctx,
			"could not reach the queue at start-up; starting anyway and reporting unready "+
				"until it answers",
			slog.String("queue", queue.Name()),
			slog.String("error", err.Error()))
		return nil
	}
}

// queueReadiness is the check /health/ready reports SQS through.
//
// It probes the outbound queue, and that is a choice worth stating. The API
// itself never calls SQS — a submission becomes a row and an outbox row, and a
// separate publisher sends it — so neither queue is on the request path and
// either would do as evidence that this process can reach the service. The
// outbound one is the one this process's own work leaves through, so an API
// replica reporting unready is reporting that what it accepts cannot get out.
//
// It resolves the queue when it finds it unresolved, and that is the other half
// of [queueStartUp.tolerateOutage]: without it, a process that started during an
// outage would report unready for the rest of its life, and "start and recover"
// would be "start and never work" with extra steps.
type queueReadiness struct {
	queue  *sqs.Queue
	health *sqs.Health
}

// Ready reports whether the queue is answering.
func (r queueReadiness) Ready(ctx context.Context) error {
	err := r.health.Ready(ctx)
	if !errors.Is(err, sqs.ErrNotResolved) {
		return err
	}
	// Only reached on a process that started without this queue. Resolution is
	// bounded by the adapter's own resolve timeout and the probe by its own, so
	// the retry cannot make a readiness answer take longer than the endpoint's
	// budget allows.
	if err := r.queue.OnStart(ctx); err != nil {
		return err
	}
	return r.health.Ready(ctx)
}

// newQueueReadiness wires that check.
func newQueueReadiness(queue outboundQueue, cfg config.SQS) (queueReadiness, error) {
	health, err := sqs.NewHealth(queue.Queue, cfg.HealthTimeout)
	if err != nil {
		return queueReadiness{}, err
	}
	return queueReadiness{queue: queue.Queue, health: health}, nil
}
