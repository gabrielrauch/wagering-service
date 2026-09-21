package sqs

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// Queue is a handle on one SQS queue: what a consumer receives from, what a
// publisher sends to, and what a readiness probe asks about.
//
// Safe for concurrent use. Everything it holds after construction is read-only
// but the resolved URL, which is written once by [Queue.OnStart] and read
// atomically thereafter — a consumer that runs several receives at once and a
// readiness probe running beside them all read the same word.
type Queue struct {
	settings settings
	// url is nil until OnStart succeeds. A pointer rather than a string and a
	// flag, so that "resolved" and "the value" cannot disagree.
	url atomic.Pointer[string]
}

// NewQueue builds a handle on a queue from a checked configuration.
//
// It performs no I/O, so that a misconfigured queue is a refusal here and an
// unreachable one is a refusal in [Queue.OnStart] — two different mornings,
// which a constructor that did both would report identically.
func NewQueue(cfg Config) (*Queue, error) {
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return &Queue{settings: resolved}, nil
}

// OnStart resolves the queue's name to its URL, bounded by the configured
// resolve timeout.
//
// It exists so that a queue nobody provisioned, an endpoint nothing answers on
// and a credential with no permission on it are start-up failures rather than a
// consumer that receives nothing and reports nothing. The error is the
// underlying one, rendered: its audience is whoever is starting the process.
//
// Calling it twice is harmless and re-resolves; the URL a queue name maps to
// does not change under a running process, so nothing here tries to be clever
// about the second call.
func (q *Queue) OnStart(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, q.settings.resolveTimeout)
	defer cancel()

	found, err := q.settings.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{
		QueueName: aws.String(q.settings.name),
	})
	if err != nil {
		if missing(err) {
			return fail(fmt.Sprintf("find the queue %q", q.settings.name), err)
		}
		return fail(fmt.Sprintf("resolve the queue %q", q.settings.name), err)
	}
	// The URL is used as SQS gives it, host and all. SQS v2 carries it in the
	// request body rather than in the address, so a URL naming a host this
	// process cannot reach — which is what LocalStack behind a mapped port
	// returns — is still the right value to send.
	q.url.Store(found.QueueUrl)
	return nil
}

// Name is the queue this handle was built for. It is what a log line or a
// readiness map is keyed on; nothing in this package decides anything from it.
func (q *Queue) Name() string { return q.settings.name }

// resolved returns the queue URL, or the refusal a call made before
// [Queue.OnStart] is owed.
func (q *Queue) resolved() (*string, error) {
	url := q.url.Load()
	if url == nil {
		return nil, app.AsUnretryable(fmt.Errorf("%w: %s", ErrNotResolved, q.settings.name))
	}
	return url, nil
}
