package sqs

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// stubClient is a client that has been built and never used.
//
// It is not a substitute for SQS and nothing here calls through it: every test
// in this file is about what happens before a request would be made. Building
// one takes no credentials and makes no connection.
func stubClient() *awssqs.Client {
	return awssqs.New(awssqs.Options{Region: "us-east-1"})
}

func TestNewQueueRefusals(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		because string
	}{
		{
			name:    "no client",
			cfg:     Config{Name: "wager-transactions.fifo"},
			because: "needs a client",
		},
		{
			name:    "no name",
			cfg:     Config{Client: stubClient()},
			because: "needs a name",
		},
		{
			name: "a negative resolve timeout",
			cfg: Config{Client: stubClient(), Name: "q.fifo",
				ResolveTimeout: -time.Second},
			because: "must not be negative",
		},
		{
			name:    "more messages than a receive can carry",
			cfg:     Config{Client: stubClient(), Name: "q.fifo", MaxMessages: 11},
			because: "1 to 10 messages",
		},
		{
			name:    "fewer than none",
			cfg:     Config{Client: stubClient(), Name: "q.fifo", MaxMessages: -1},
			because: "1 to 10 messages",
		},
		{
			name:    "a wait past what long polling allows",
			cfg:     Config{Client: stubClient(), Name: "q.fifo", WaitTime: 21 * time.Second},
			because: "waits up to 20s",
		},
		{
			name: "a wait SQS could not express",
			cfg: Config{Client: stubClient(), Name: "q.fifo",
				WaitTime: 500 * time.Millisecond},
			because: "whole seconds",
		},
		{
			name: "a visibility timeout past twelve hours",
			cfg: Config{Client: stubClient(), Name: "q.fifo",
				VisibilityTimeout: 13 * time.Hour},
			because: "runs to 12h0m0s",
		},
		{
			name: "a visibility timeout SQS could not express",
			cfg: Config{Client: stubClient(), Name: "q.fifo",
				VisibilityTimeout: 1500 * time.Millisecond},
			because: "whole seconds",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			queue, err := NewQueue(c.cfg)
			if err == nil {
				t.Fatalf("NewQueue(%#v) = %v, want a refusal", c.cfg, queue)
			}
			if queue != nil {
				t.Errorf("a refused queue came back as %v, want nil", queue)
			}
			if !strings.Contains(err.Error(), c.because) {
				t.Errorf("error = %q, want it to say %q", err, c.because)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	resolved, err := Config{Client: stubClient(), Name: "q.fifo"}.resolve()
	if err != nil {
		t.Fatalf("resolve a minimal config: %v", err)
	}
	if resolved.maxMessages != maxReceiveBatch {
		t.Errorf("max messages = %d, want the full batch of %d", resolved.maxMessages,
			maxReceiveBatch)
	}
	if want := seconds(maxWaitTime); resolved.waitTime != want {
		t.Errorf("wait time = %ds, want the full long poll of %ds", resolved.waitTime, want)
	}
	if resolved.resolveTimeout != defaultResolveTimeout {
		t.Errorf("resolve timeout = %s, want %s", resolved.resolveTimeout, defaultResolveTimeout)
	}
	// Absent rather than zero: zero would mean "make it visible again at once",
	// where absent means "use the queue's own", which is what the queue is
	// configured for.
	if resolved.visibilityTimeout != nil {
		t.Errorf("visibility timeout = %d, want it left to the queue",
			aws.ToInt32(resolved.visibilityTimeout))
	}
}

func TestConfigKeepsWhatItIsGiven(t *testing.T) {
	resolved, err := Config{
		Client:            stubClient(),
		Name:              "wager-transactions.fifo",
		ResolveTimeout:    9 * time.Second,
		MaxMessages:       3,
		WaitTime:          2 * time.Second,
		VisibilityTimeout: 45 * time.Second,
	}.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	switch {
	case resolved.maxMessages != 3:
		t.Errorf("max messages = %d, want 3", resolved.maxMessages)
	case resolved.waitTime != 2:
		t.Errorf("wait time = %d, want 2", resolved.waitTime)
	case resolved.resolveTimeout != 9*time.Second:
		t.Errorf("resolve timeout = %s, want 9s", resolved.resolveTimeout)
	case aws.ToInt32(resolved.visibilityTimeout) != 45:
		t.Errorf("visibility timeout = %d, want 45", aws.ToInt32(resolved.visibilityTimeout))
	}
}

func TestNewClientNeedsARegion(t *testing.T) {
	client, err := NewClient(t.Context(), ClientConfig{})
	if err == nil {
		t.Fatalf("NewClient with no region = %v, want a refusal", client)
	}
	if !strings.Contains(err.Error(), "needs a region") {
		t.Errorf("error = %q, want it to say the region is missing", err)
	}
}

func TestNewHealthRefusals(t *testing.T) {
	queue, err := NewQueue(Config{Client: stubClient(), Name: "q.fifo"})
	if err != nil {
		t.Fatalf("build a queue: %v", err)
	}
	cases := []struct {
		name    string
		queue   *Queue
		timeout time.Duration
		because string
	}{
		{name: "no queue", timeout: time.Second, because: "needs a queue"},
		{name: "no timeout", queue: queue, because: "positive timeout"},
		{name: "a negative timeout", queue: queue, timeout: -time.Second,
			because: "positive timeout"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			health, err := NewHealth(c.queue, c.timeout)
			if err == nil {
				t.Fatalf("NewHealth = %v, want a refusal", health)
			}
			if !strings.Contains(err.Error(), c.because) {
				t.Errorf("error = %q, want it to say %q", err, c.because)
			}
		})
	}
}

// TestUnresolvedQueueRefusesEveryCall is the guard on the two-phase
// construction: a queue whose OnStart has not run must refuse rather than send
// a request naming an empty URL, and it must refuse the same way everywhere.
func TestUnresolvedQueueRefusesEveryCall(t *testing.T) {
	queue, err := NewQueue(Config{Client: stubClient(), Name: "wallet-events.fifo"})
	if err != nil {
		t.Fatalf("build a queue: %v", err)
	}
	health, err := NewHealth(queue, time.Second)
	if err != nil {
		t.Fatalf("build a readiness check: %v", err)
	}

	calls := map[string]func() error{
		"receive": func() error {
			_, err := queue.Receive(t.Context())
			return err
		},
		"delete":            func() error { return queue.Delete(t.Context(), "handle") },
		"change visibility": func() error { return queue.ChangeVisibility(t.Context(), "h", 0) },
		"release":           func() error { return queue.Release(t.Context(), "handle") },
		"send": func() error {
			_, err := queue.SendBatch(t.Context(), []Outbound{{
				Body: []byte("{}"), GroupID: "g", DeduplicationID: "d"}})
			return err
		},
		"readiness": func() error { return health.Ready(t.Context()) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if !errors.Is(err, ErrNotResolved) {
				t.Fatalf("error = %v, want it to carry ErrNotResolved", err)
			}
			if got := app.ClassOf(err); got != app.Unretryable {
				t.Errorf("class = %s, want %s: calling again changes nothing", got,
					app.Unretryable)
			}
			if !strings.Contains(err.Error(), "wallet-events.fifo") {
				t.Errorf("error = %q, want it to name the queue", err)
			}
		})
	}
}

// TestSendBatchOfNothingIsNotAFailure holds the one case that answers before
// the queue is even consulted: a publisher whose claim came back empty.
func TestSendBatchOfNothingIsNotAFailure(t *testing.T) {
	queue, err := NewQueue(Config{Client: stubClient(), Name: "wallet-events.fifo"})
	if err != nil {
		t.Fatalf("build a queue: %v", err)
	}
	results, err := queue.SendBatch(t.Context(), nil)
	if err != nil {
		t.Fatalf("send nothing: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("results = %v, want none", results)
	}
}
