package sqs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// The bounds SQS itself applies, restated here so that a value outside them is
// refused at construction with a sentence rather than at the first call with an
// InvalidParameterValue.
const (
	// maxReceiveBatch is the most messages one ReceiveMessage may return.
	maxReceiveBatch = 10
	// maxWaitTime is the longest a receive may hold the connection open
	// waiting. Twenty seconds is the ceiling SQS enforces on long polling.
	maxWaitTime = 20 * time.Second
	// maxVisibilityTimeout is the longest a message may be hidden, and the
	// longest any single unit of work may therefore take without extending it.
	maxVisibilityTimeout = 12 * time.Hour
)

// The defaults a [Config] leaves open.
const (
	// defaultMaxMessages is the full batch. A receive that asks for fewer pays
	// the same round trip for less work, and the consumer bounds its own
	// concurrency — how many it processes at once is its decision, not the
	// queue's.
	defaultMaxMessages = maxReceiveBatch
	// defaultWaitTime is the full long poll. Anything shorter is a consumer
	// paying for empty receives on an idle queue, and SQS bills per request.
	defaultWaitTime = maxWaitTime
	// defaultResolveTimeout bounds resolving the queue's URL at start-up. Five
	// seconds is long enough for a cold connection to a regional endpoint and
	// short enough that a process pointed at an address nothing answers on
	// fails while somebody is still watching it deploy.
	defaultResolveTimeout = 5 * time.Second
)

// ClientConfig is what the SQS client every [Queue] is built on is made from.
//
// There is no credential here and there deliberately cannot be one. Credentials
// come from the SDK's default chain, so a deployment supplies them the way
// every other AWS client in its environment does, and this service's own
// configuration has nothing in it worth stealing.
type ClientConfig struct {
	// Region is the AWS region the queues live in. Required: resolving it from
	// the ambient environment would let a process that was configured for
	// nothing at all start up and talk to the wrong account's queues.
	Region string
	// Endpoint overrides where requests are sent, for LocalStack and for
	// nothing else. Empty means the real regional endpoint.
	//
	// SQS v2 carries the queue URL in the request body rather than in the
	// address, so this alone is enough: a queue URL naming a host this process
	// cannot reach is still usable, because the request never goes there.
	Endpoint string
	// Telemetry is where each call against SQS is reported. Optional: nil is
	// [telemetry.Disabled], and a client built without it behaves exactly as it
	// did before there was any.
	Telemetry *telemetry.Telemetry
}

// NewClient builds the SQS client, resolving credentials from the default
// chain.
//
// It performs no call against SQS. Whether the queues exist is [Queue.OnStart]'s
// question, and whether they are still answering is [Health]'s.
func NewClient(ctx context.Context, cfg ClientConfig) (*awssqs.Client, error) {
	if cfg.Region == "" {
		return nil, errors.New("sqs: a client needs a region")
	}
	loaded, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		// Rendered, not wrapped with a credential in it: LoadDefaultConfig
		// reports which provider in the chain refused, never what it held.
		return nil, fmt.Errorf("sqs: load the AWS configuration: %w", err)
	}
	return awssqs.NewFromConfig(loaded, func(o *awssqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.APIOptions = append(o.APIOptions, traced(telemetry.Or(cfg.Telemetry)))
	}), nil
}

// Config is everything a [Queue] is built from. Named fields rather than a
// positional list, because four of these are durations and the compiler cannot
// tell a poll from a timeout.
type Config struct {
	// Client is the SQS client, from [NewClient] or built by the caller.
	// Required.
	Client *awssqs.Client
	// Name is the queue's name, not its URL. Required.
	//
	// A name rather than a URL because a URL is an account id and a host this
	// service should not be asked to know — and because resolving the name is
	// what proves at start-up that the queue exists.
	Name string
	// ResolveTimeout bounds [Queue.OnStart]. Zero means
	// [defaultResolveTimeout]; negative is refused.
	ResolveTimeout time.Duration
	// MaxMessages is how many messages one receive may return, 1 to 10. Zero
	// means [defaultMaxMessages].
	MaxMessages int
	// WaitTime is how long a receive waits for a message before answering
	// empty, from 1 to 20 seconds. Zero means [defaultWaitTime].
	//
	// There is deliberately no way to ask for short polling. A short poll
	// samples a subset of the queue's hosts and answers empty while messages
	// are waiting, so a consumer built on one looks like it has lost work; the
	// only reason to want it is a test that does not wish to wait, and a test
	// that does not wish to wait can ask for one second.
	//
	// Whole seconds, because that is the resolution SQS has. A wait of 500ms
	// would truncate to zero and silently become the short poll this refuses.
	WaitTime time.Duration
	// VisibilityTimeout is how long a received message is hidden from other
	// consumers. Zero means the queue's own configured timeout, which is where
	// it belongs: it is a property of how long the work takes, and the queue is
	// where an operator can change it without a deployment.
	//
	// Whole seconds, for the reason [Config.WaitTime] gives: anything under a
	// second would truncate to zero and quietly mean something else.
	VisibilityTimeout time.Duration
}

// settings is a Config that has been checked and had its defaults filled in, so
// that nothing downstream has to ask whether a value is present.
type settings struct {
	client            *awssqs.Client
	name              string
	resolveTimeout    time.Duration
	maxMessages       int32
	waitTime          int32
	visibilityTimeout *int32
}

// resolve checks the configuration and fills in what it left open.
func (c Config) resolve() (settings, error) {
	s := settings{
		client:         c.Client,
		name:           c.Name,
		resolveTimeout: c.ResolveTimeout,
	}
	switch {
	case s.client == nil:
		return settings{}, errors.New("sqs: a queue needs a client")
	case s.name == "":
		return settings{}, errors.New("sqs: a queue needs a name")
	}

	switch {
	case s.resolveTimeout < 0:
		return settings{}, fmt.Errorf("sqs: the resolve timeout must not be negative, got %s",
			s.resolveTimeout)
	case s.resolveTimeout == 0:
		s.resolveTimeout = defaultResolveTimeout
	}

	switch {
	case c.MaxMessages < 0 || c.MaxMessages > maxReceiveBatch:
		return settings{}, fmt.Errorf("sqs: a receive takes 1 to %d messages, got %d",
			maxReceiveBatch, c.MaxMessages)
	case c.MaxMessages == 0:
		s.maxMessages = defaultMaxMessages
	default:
		s.maxMessages = int32(c.MaxMessages)
	}

	switch {
	case c.WaitTime < 0 || c.WaitTime > maxWaitTime:
		return settings{}, fmt.Errorf("sqs: a receive waits up to %s, got %s", maxWaitTime,
			c.WaitTime)
	case c.WaitTime%time.Second != 0:
		return settings{}, fmt.Errorf("sqs: a receive waits whole seconds, got %s", c.WaitTime)
	case c.WaitTime == 0:
		s.waitTime = seconds(defaultWaitTime)
	default:
		s.waitTime = seconds(c.WaitTime)
	}

	switch {
	case c.VisibilityTimeout < 0 || c.VisibilityTimeout > maxVisibilityTimeout:
		return settings{}, fmt.Errorf("sqs: a visibility timeout runs to %s, got %s",
			maxVisibilityTimeout, c.VisibilityTimeout)
	case c.VisibilityTimeout%time.Second != 0:
		return settings{}, fmt.Errorf("sqs: a visibility timeout is whole seconds, got %s",
			c.VisibilityTimeout)
	case c.VisibilityTimeout > 0:
		s.visibilityTimeout = aws.Int32(seconds(c.VisibilityTimeout))
	}
	return s, nil
}

// seconds is a duration in the unit SQS states every one of its own bounds in.
//
// It is only ever reached with a duration that is a whole number of seconds
// inside the bound above it, so the conversion cannot lose anything or
// overflow — which is the property the checks in resolve exist to establish.
func seconds(d time.Duration) int32 { return int32(d / time.Second) }
