package fxmod

import (
	"context"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/config"
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
// Two named types rather than Fx's named values, because a named value has to
// be requested with fx.Annotate(..., fx.ParamTags(...)), and ParamTags is one
// tag per parameter in positional order — a long positional list standing in
// for exactly the distinction these two types make in the signature itself.
type (
	// inboundQueue is the FIFO queue wager operations arrive on.
	inboundQueue struct{ *sqs.Queue }
	// outboundQueue is the FIFO queue wallet events leave on.
	outboundQueue struct{ *sqs.Queue }
)

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
			newQueueHealth,
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
func newSQSClient(cfg config.SQS) (*awssqs.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ClientTimeout)
	defer cancel()

	return sqs.NewClient(ctx, sqs.ClientConfig{Region: cfg.Region, Endpoint: cfg.Endpoint})
}

// newInboundQueue wires the queue the consumer receives on, and makes resolving
// its name a start-up check.
//
// Resolution is the check the task asks for and it is worth being precise about
// what it proves: GetQueueUrl fails for a queue that does not exist, in an
// account this process cannot reach, or in a region it was misconfigured with —
// which is every way the deployment can be wrong about a queue. Every other
// method on the adapter refuses to run until it has happened, so a hook that
// was skipped cannot become a receive against an empty URL.
func newInboundQueue(
	lc fx.Lifecycle, cfg config.SQS, client *awssqs.Client,
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
	lc.Append(fx.Hook{OnStart: queue.OnStart})
	return inboundQueue{Queue: queue}, nil
}

// newOutboundQueue wires the queue the publisher sends on.
//
// The receive bounds are left at the adapter's defaults: nothing receives from
// this one, and configuring a long poll for a queue nobody polls would be a
// number an operator could tune with no effect. The visibility timeout is set
// for the same reason it is on the inbound queue — it is a property of the
// queue, and saying nothing here means the queue's own provisioned value stands.
func newOutboundQueue(
	lc fx.Lifecycle, cfg config.SQS, client *awssqs.Client,
) (outboundQueue, error) {
	queue, err := sqs.NewQueue(sqs.Config{
		Client:         client,
		Name:           cfg.OutboundQueue,
		ResolveTimeout: cfg.ResolveTimeout,
	})
	if err != nil {
		return outboundQueue{}, err
	}
	lc.Append(fx.Hook{OnStart: queue.OnStart})
	return outboundQueue{Queue: queue}, nil
}

// newQueueHealth wires the readiness probe /health/ready reports SQS through.
//
// It probes the outbound queue, and that is a choice worth stating. The API
// itself never calls SQS — a submission becomes a row and an outbox row, and a
// separate publisher sends it — so neither queue is on the request path and
// either would do as evidence that this process can reach the service. The
// outbound one is the one this process's own work leaves through, so an API
// replica reporting unready is reporting that what it accepts cannot get out.
func newQueueHealth(queue outboundQueue, cfg config.SQS) (*sqs.Health, error) {
	return sqs.NewHealth(queue.Queue, cfg.HealthTimeout)
}
