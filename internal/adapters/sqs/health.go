package sqs

import (
	"context"
	"errors"
	"fmt"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// readinessAttribute is the cheapest thing a queue can be asked that proves it
// is there and that this process is allowed to touch it.
//
// The ARN, deliberately, and not the message count. A probe that read
// ApproximateNumberOfMessages would look like a queue depth metric, and
// somebody would eventually make it one — at which point a backlog would report
// the service unready and take away the replicas that were working through it.
const readinessAttribute = types.QueueAttributeNameQueueArn

// Health answers whether this process can reach a queue.
//
// This is a READINESS probe and must not be wired to a liveness check, for the
// reason the PostgreSQL adapter gives: an unreachable dependency should shed
// load from this replica, not restart it, because restarting every replica at
// once removes the only capacity that was still working.
//
// It reports unready before [Queue.OnStart] has run, which is the honest
// answer: a queue whose URL is not resolved is one this process cannot serve
// from, whatever SQS would have said about it.
type Health struct {
	queue   *Queue
	timeout time.Duration
}

// NewHealth wires the readiness check for one queue.
//
// The timeout is required and bounds the probe itself. Without it a readiness
// endpoint inherits whatever deadline its caller happened to set, and an
// orchestrator that sets none would wait on an endpoint that accepts a
// connection and then says nothing — reporting neither ready nor unready, which
// is the one answer nothing can act on.
func NewHealth(queue *Queue, timeout time.Duration) (*Health, error) {
	switch {
	case queue == nil:
		return nil, errors.New("sqs: a readiness check needs a queue")
	case timeout <= 0:
		return nil, fmt.Errorf("sqs: a readiness check needs a positive timeout, got %s", timeout)
	}
	return &Health{queue: queue, timeout: timeout}, nil
}

// Ready reports whether the queue is reachable, within the configured timeout.
//
// The bound is applied on top of the caller's context rather than instead of
// it, so a caller that is already giving up is not made to wait for this.
func (h *Health) Ready(ctx context.Context) error {
	url, err := h.queue.resolved()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	_, err = h.queue.settings.client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       url,
		AttributeNames: []types.QueueAttributeName{readinessAttribute},
	})
	return fail("check readiness of "+h.queue.settings.name, err)
}
