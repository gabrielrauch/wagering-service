package workers

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// testBackoff is short enough that a test never waits on it and predictable
// enough that a test can state what a visibility change should be.
func testBackoff() Backoff {
	return Backoff{Initial: 2 * time.Second, Factor: 2, Max: 30 * time.Second}
}

// consumerOver builds a consumer over the queue with the test defaults filled
// in, and stops it when the test ends.
func consumerOver(t *testing.T, ctx context.Context, cfg ConsumerConfig) *Consumer {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "wager-transactions"
	}
	if cfg.Logger == nil {
		cfg.Logger = discard()
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.Backoff == (Backoff{}) {
		cfg.Backoff = testBackoff()
	}
	consumer, err := NewConsumer(cfg)
	if err != nil {
		t.Fatalf("build a consumer: %v", err)
	}
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("start the consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Stop(context.WithoutCancel(ctx)) })
	return consumer
}

// A message is deleted only once the transaction that handled it has committed.
// Deleting first turns every crash in between into a lost operation.
func TestTheMessageIsDeletedOnlyAfterTheCommit(t *testing.T) {
	queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 1)})
	submitter := &fakeSubmitter{}
	submitter.answer = func(context.Context, int, app.SubmitOperation) (app.OperationResult, error) {
		if deleted := queue.deletedHandles(); len(deleted) > 0 {
			t.Errorf("the message was deleted before the commit: %v", deleted)
		}
		return processed(), nil
	}

	consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
	queue.handled(t)

	if got := queue.deletedHandles(); len(got) != 1 || got[0] != "handle-1" {
		t.Errorf("deleted = %v, want the message to have been deleted after the commit", got)
	}
}

// Every answer the application layer reports without an error is terminal for
// the message, including the two that are not successes. A rejection has a row
// and an event behind it and a parked operation has a schedule; redelivering
// either would be asking for work that is already done.
func TestWhatBecomesOfAMessage(t *testing.T) {
	rejected := app.OperationResult{
		TransactionID: wagering.NewTransactionID(),
		Kind:          wagering.Bet,
		Status:        wagering.Rejected,
		FailureCode:   failure.InsufficientFunds,
	}
	parked := app.OperationResult{
		TransactionID: wagering.NewTransactionID(),
		Kind:          wagering.Rollback,
		Status:        wagering.PendingReference,
	}

	cases := []struct {
		name   string
		result app.OperationResult
		err    error
		// want, in the three ways a message can leave a consumer's hands.
		deleted   bool
		hiddenFor time.Duration
	}{
		{
			name:    "a processed operation is deleted",
			result:  processed(),
			deleted: true,
		},
		{
			name:    "a business rejection is terminal for the message",
			result:  rejected,
			deleted: true,
		},
		{
			name:    "a pending reference is terminal for the message",
			result:  parked,
			deleted: true,
		},
		{
			name:      "a retryable failure hides the message for its backoff",
			err:       app.AsRetryable(errors.New("the connection was lost")),
			hiddenFor: 2 * time.Second,
		},
		{
			name:      "a wallet that is not open yet is worth another delivery",
			err:       &app.Error{Class: app.NotFound},
			hiddenFor: 2 * time.Second,
		},
		{
			name: "an unretryable failure is left for the redrive policy",
			err:  app.AsUnretryable(errors.New("this will fail the same way again")),
		},
		{
			name: "a malformed submission is left for the redrive policy",
			err:  &app.Error{Class: app.Invalid, Code: failure.InvalidFieldFormat},
		},
		{
			name: "a conflicting submission is left for the redrive policy",
			err:  &app.Error{Class: app.Conflict, Code: failure.IdempotencyPayloadConflict},
		},
		{
			name: "an unauthorized submission is left for the redrive policy",
			err:  &app.Error{Class: app.Unauthorized},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 1)})
			submitter := &fakeSubmitter{
				answer: func(context.Context, int, app.SubmitOperation) (app.OperationResult, error) {
					return c.result, c.err
				},
			}
			consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
			queue.handled(t)

			deleted := len(queue.deletedHandles()) == 1
			if deleted != c.deleted {
				t.Errorf("deleted = %v, want %v", deleted, c.deleted)
			}
			changes := queue.changes()
			switch {
			case c.hiddenFor == 0 && len(changes) != 0:
				t.Errorf("the message was hidden for %v, want it left alone", changes)
			case c.hiddenFor != 0 && len(changes) != 1:
				t.Fatalf("visibility changes = %v, want exactly one", changes)
			case c.hiddenFor != 0 && changes[0].in != c.hiddenFor:
				t.Errorf("hidden for %s, want %s", changes[0].in, c.hiddenFor)
			}
			if released := queue.releasedHandles(); len(released) != 0 {
				t.Errorf("released = %v, want nothing released while running", released)
			}
		})
	}
}

// The backoff is driven by ApproximateReceiveCount, which is the only attempt
// counter a redelivery carries.
func TestTheBackoffGrowsWithTheDeliveryCount(t *testing.T) {
	for _, c := range []struct {
		receiveCount int
		want         time.Duration
	}{
		{receiveCount: 1, want: 2 * time.Second},
		{receiveCount: 2, want: 4 * time.Second},
		{receiveCount: 3, want: 8 * time.Second},
		{receiveCount: 9, want: 30 * time.Second},
	} {
		t.Run(strings.TrimSpace(time.Duration(c.receiveCount).String()), func(t *testing.T) {
			queue := newFakeQueue([]sqs.Message{
				message("handle-1", validBody(t, nil), c.receiveCount),
			})
			submitter := &fakeSubmitter{
				answer: func(context.Context, int, app.SubmitOperation) (app.OperationResult, error) {
					return app.OperationResult{}, app.AsRetryable(errors.New("not now"))
				},
			}
			consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
			queue.handled(t)

			changes := queue.changes()
			if len(changes) != 1 {
				t.Fatalf("visibility changes = %v, want exactly one", changes)
			}
			if changes[0].in != c.want {
				t.Errorf("delivery %d hidden for %s, want %s", c.receiveCount, changes[0].in, c.want)
			}
		})
	}
}

// A body this consumer cannot read never reaches the application layer at all,
// and is left where the redrive policy will find it.
func TestAnUnreadableEnvelopeIsLeftForTheRedrivePolicy(t *testing.T) {
	for _, c := range []struct {
		name string
		body []byte
	}{
		{name: "invalid JSON", body: []byte("{not json")},
		{
			name: "an unknown type",
			body: validBody(t, map[string]any{"type": "SomethingElse"}),
		},
		{
			name: "a provider that is not an identifier",
			body: validBody(t, map[string]any{"data": validData(map[string]any{"provider": ""})}),
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			queue := newFakeQueue([]sqs.Message{message("handle-1", c.body, 1)})
			submitter := &fakeSubmitter{}

			consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
			queue.handled(t)

			if got := submitter.count(); got != 0 {
				t.Errorf("submissions = %d, want the message never to reach the application", got)
			}
			if got := queue.deletedHandles(); len(got) != 0 {
				t.Errorf("deleted = %v, want the message left for the redrive policy", got)
			}
			if got := queue.changes(); len(got) != 0 {
				t.Errorf("visibility changes = %v, want the message left alone", got)
			}
		})
	}
}

// A FIFO receive returns as many messages of one group as it can, so everything
// behind a message the consumer does not delete may be in its group. Handling it
// would apply a wallet's operations out of order.
func TestAnUndeletedMessageStopsItsBatch(t *testing.T) {
	batch := []sqs.Message{
		message("handle-1", validBody(t, map[string]any{"messageId": "message-1"}), 1),
		message("handle-2", validBody(t, map[string]any{"messageId": "message-2"}), 1),
		message("handle-3", validBody(t, map[string]any{"messageId": "message-3"}), 1),
	}

	t.Run("a transient failure hides the whole group for the same backoff", func(t *testing.T) {
		queue := newFakeQueue(batch)
		submitter := &fakeSubmitter{
			answer: func(context.Context, int, app.SubmitOperation) (app.OperationResult, error) {
				return app.OperationResult{}, app.AsRetryable(errors.New("the database is down"))
			},
		}
		consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
		queue.handled(t)

		if got := submitter.count(); got != 1 {
			t.Errorf("submissions = %d, want only the message at the head of the batch", got)
		}
		changes := queue.changes()
		if len(changes) != 3 {
			t.Fatalf("visibility changes = %v, want all three hidden together", changes)
		}
		for _, change := range changes {
			if change.in != 2*time.Second {
				t.Errorf("%s hidden for %s, want the same 2s as the message that failed",
					change.handle, change.in)
			}
		}
	})

	t.Run("a permanent failure leaves the whole group alone", func(t *testing.T) {
		queue := newFakeQueue(batch)
		submitter := &fakeSubmitter{
			answer: func(context.Context, int, app.SubmitOperation) (app.OperationResult, error) {
				return app.OperationResult{}, app.AsUnretryable(errors.New("never"))
			},
		}
		consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
		queue.handled(t)

		if got := submitter.count(); got != 1 {
			t.Errorf("submissions = %d, want only the message at the head of the batch", got)
		}
		if got := queue.changes(); len(got) != 0 {
			t.Errorf("visibility changes = %v, want the batch left to time out together", got)
		}
		if got := queue.deletedHandles(); len(got) != 0 {
			t.Errorf("deleted = %v, want nothing deleted", got)
		}
	})

	t.Run("messages ahead of the failure are still deleted", func(t *testing.T) {
		queue := newFakeQueue(batch)
		submitter := &fakeSubmitter{
			answer: func(_ context.Context, n int, _ app.SubmitOperation) (app.OperationResult, error) {
				if n == 0 {
					return processed(), nil
				}
				return app.OperationResult{}, app.AsUnretryable(errors.New("never"))
			},
		}
		consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
		queue.handled(t)

		if got := submitter.count(); got != 2 {
			t.Errorf("submissions = %d, want the first two only", got)
		}
		deleted := queue.deletedHandles()
		if len(deleted) != 1 || deleted[0] != "handle-1" {
			t.Errorf("deleted = %v, want only the message that committed", deleted)
		}
	})
}

// Concurrency is bounded per instance: that many messages are handled at once
// and no more, however much the queue offers.
func TestConcurrencyIsBounded(t *testing.T) {
	const bound = 3

	queue := newFakeQueue()
	var handles atomic64
	queue.stream = func(int) []sqs.Message {
		return []sqs.Message{
			message("handle-"+handles.next(), validBody(t, nil), 1),
		}
	}

	var (
		admitted = make(chan struct{}, 64)
		release  = make(chan struct{})
		once     sync.Once
	)
	submitter := &fakeSubmitter{
		answer: func(ctx context.Context, _ int, _ app.SubmitOperation) (app.OperationResult, error) {
			admitted <- struct{}{}
			select {
			case <-release:
				return processed(), nil
			case <-ctx.Done():
				return app.OperationResult{}, ctx.Err()
			}
		},
	}
	defer once.Do(func() { close(release) })

	consumerOver(t, t.Context(), ConsumerConfig{
		Queue: queue, Wagering: submitter, Concurrency: bound,
	})

	// Wait until the bound is reached, then give the loops long enough to
	// exceed it if they were going to.
	for range bound {
		select {
		case <-admitted:
		case <-time.After(settledWithin):
			t.Fatalf("fewer than %d messages were taken on at once", bound)
		}
	}
	select {
	case <-admitted:
		t.Fatalf("a %dth message was taken on while %d were still in flight", bound+1, bound)
	case <-time.After(100 * time.Millisecond):
	}
	if got := submitter.peakConcurrency(); got != bound {
		t.Errorf("peak concurrency = %d, want %d", got, bound)
	}
	once.Do(func() { close(release) })
}

// A clean shutdown finishes what it can inside the deadline and then hands back
// whatever it could not, so that the work is redelivered at once rather than
// waiting out a visibility timeout that exists for the case where nobody gave
// it back.
func TestShutdownDrainsAndThenReleases(t *testing.T) {
	t.Run("work that finishes inside the deadline is acknowledged", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 1)})
		consumer, err := NewConsumer(ConsumerConfig{
			Queue: queue, Wagering: &fakeSubmitter{}, Name: "wager-transactions",
			Logger: discard(), Concurrency: 1, DrainTimeout: settledWithin,
		})
		if err != nil {
			t.Fatalf("build a consumer: %v", err)
		}
		if err := consumer.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		queue.handled(t)

		if err := consumer.Stop(ctx); err != nil {
			t.Errorf("a shutdown with nothing in flight reported %v", err)
		}
		if got := queue.deletedHandles(); len(got) != 1 {
			t.Errorf("deleted = %v, want the message acknowledged", got)
		}
		if got := queue.releasedHandles(); len(got) != 0 {
			t.Errorf("released = %v, want nothing released", got)
		}
	})

	t.Run("work still running at the deadline is released and reported", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 1)})
		var (
			admitted = make(chan struct{})
			release  = make(chan struct{})
			once     sync.Once
		)
		defer once.Do(func() { close(release) })

		submitter := &fakeSubmitter{
			answer: func(ctx context.Context, _ int, _ app.SubmitOperation) (app.OperationResult, error) {
				close(admitted)
				select {
				case <-release:
					return processed(), nil
				case <-ctx.Done():
					return app.OperationResult{}, ctx.Err()
				}
			},
		}
		consumer, err := NewConsumer(ConsumerConfig{
			Queue: queue, Wagering: submitter, Name: "wager-transactions",
			Logger: discard(), Concurrency: 1, DrainTimeout: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("build a consumer: %v", err)
		}
		if err := consumer.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		<-admitted

		stopped := consumer.Stop(ctx)
		if stopped == nil {
			t.Fatal("a shutdown that ran out of time reported success")
		}
		if !strings.Contains(stopped.Error(), "receiver-0") {
			t.Errorf("the shutdown error %q does not name the receiver that did not finish",
				stopped)
		}
		released := queue.releasedHandles()
		if len(released) != 1 || released[0] != "handle-1" {
			t.Errorf("released = %v, want the message in flight handed straight back", released)
		}
		once.Do(func() { close(release) })
	})

	t.Run("messages a stopped batch never reached are handed back", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		queue := newFakeQueue([]sqs.Message{
			message("handle-1", validBody(t, map[string]any{"messageId": "message-1"}), 1),
			message("handle-2", validBody(t, map[string]any{"messageId": "message-2"}), 1),
		})
		var (
			admitted = make(chan struct{})
			release  = make(chan struct{})
			once     sync.Once
		)
		defer once.Do(func() { close(release) })

		submitter := &fakeSubmitter{
			answer: func(ctx context.Context, n int, _ app.SubmitOperation) (app.OperationResult, error) {
				if n == 0 {
					close(admitted)
					select {
					case <-release:
					case <-ctx.Done():
						return app.OperationResult{}, ctx.Err()
					}
				}
				return processed(), nil
			},
		}
		consumer, err := NewConsumer(ConsumerConfig{
			Queue: queue, Wagering: submitter, Name: "wager-transactions",
			Logger: discard(), Concurrency: 1, DrainTimeout: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("build a consumer: %v", err)
		}
		if err := consumer.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		<-admitted

		_ = consumer.Stop(ctx)
		released := queue.releasedHandles()
		if len(released) != 2 {
			t.Errorf("released = %v, want both the message in flight and the one behind it",
				released)
		}
		once.Do(func() { close(release) })
	})
}

// Stopping twice is not an error and does not release anything twice, because a
// lifecycle hook may run more than once and a receipt handle handed back twice
// is a message made visible after somebody else took it.
func TestStoppingTwiceIsHarmless(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	queue := newFakeQueue([]sqs.Message{message("handle-1", validBody(t, nil), 1)})
	consumer, err := NewConsumer(ConsumerConfig{
		Queue: queue, Wagering: &fakeSubmitter{}, Name: "wager-transactions",
		Logger: discard(), Concurrency: 1, DrainTimeout: settledWithin,
	})
	if err != nil {
		t.Fatalf("build a consumer: %v", err)
	}
	if err := consumer.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	queue.handled(t)

	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("first stop: %v", err)
	}
	if err := consumer.Stop(ctx); err != nil {
		t.Fatalf("second stop: %v", err)
	}
	if err := consumer.Start(ctx); err == nil {
		t.Error("a stopped consumer was started again")
	}
}

// The inbox identity is the envelope's message id and the hash of the body, and
// the consumer's own name is what scopes it — the queue's message id is not an
// identity the sender chose and is never used as one.
func TestTheSubmissionCarriesTheInboxIdentity(t *testing.T) {
	body := validBody(t, map[string]any{"messageId": "envelope-message-1"})
	queue := newFakeQueue([]sqs.Message{message("handle-1", body, 1)})
	submitter := &fakeSubmitter{}

	consumerOver(t, t.Context(), ConsumerConfig{
		Queue: queue, Wagering: submitter, Name: "wager-transactions",
	})
	queue.handled(t)

	submissions := submitter.submissions()
	if len(submissions) != 1 {
		t.Fatalf("submissions = %d, want one", len(submissions))
	}
	inbox := submissions[0].Inbox
	if inbox == nil {
		t.Fatal("the submission carried no inbox identity")
	}
	if inbox.Consumer != "wager-transactions" {
		t.Errorf("consumer = %q, want the consumer's own name", inbox.Consumer)
	}
	if inbox.MessageID != "envelope-message-1" {
		t.Errorf("messageId = %q, want the envelope's and not the queue's", inbox.MessageID)
	}
	if inbox.BodyHash != bodyHash(body) {
		t.Errorf("bodyHash = %q, want the hash of the body that arrived", inbox.BodyHash)
	}

	provider, ok := submissions[0].Principal.Provider()
	if !ok || provider.String() != "acme" {
		t.Errorf("principal provider = %q (%v), want the one the envelope named", provider, ok)
	}
	if got := submissions[0].Principal.Subject(); got != "wager-transactions" {
		t.Errorf("subject = %q, want the consumer that admitted the message", got)
	}
}

func TestTheCorrelationComesFromTheTraceAttributes(t *testing.T) {
	cases := []struct {
		name       string
		attributes map[string]string
		want       string
	}{
		{
			name:       "a usable correlation is carried through",
			attributes: map[string]string{correlationAttribute: "thread-1"},
			want:       "thread-1",
		},
		{
			name: "no attributes fall back to the envelope's message id",
			want: "envelope-message-1",
		},
		{
			name:       "a correlation carrying a newline is replaced",
			attributes: map[string]string{correlationAttribute: "thread\n1"},
			want:       "envelope-message-1",
		},
		{
			name: "a correlation the inbox could not store is replaced",
			attributes: map[string]string{
				correlationAttribute: strings.Repeat("t", maxCorrelationBytes+1),
			},
			want: "envelope-message-1",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := message("handle-1", validBody(t, map[string]any{
				"messageId": "envelope-message-1",
			}), 1)
			m.Attributes = c.attributes

			queue := newFakeQueue([]sqs.Message{m})
			submitter := &fakeSubmitter{}
			consumerOver(t, t.Context(), ConsumerConfig{Queue: queue, Wagering: submitter})
			queue.handled(t)

			submissions := submitter.submissions()
			if len(submissions) != 1 {
				t.Fatalf("submissions = %d, want one", len(submissions))
			}
			if submissions[0].Correlation != c.want {
				t.Errorf("correlation = %q, want %q", submissions[0].Correlation, c.want)
			}
		})
	}
}

// The delivery that is about to be redriven is the one an operator most wants
// to hear about, and ApproximateReceiveCount is the same number the redrive
// policy is judged on.
func TestTheLastDeliveryBeforeTheDeadLetterQueueIsSaidSo(t *testing.T) {
	const maxReceives = 5

	for _, c := range []struct {
		name         string
		receiveCount int
		want         string
	}{
		{
			name:         "an earlier delivery",
			receiveCount: maxReceives - 1,
			want:         "the message cannot be handled and was left for the redrive policy",
		},
		{
			name:         "the last delivery",
			receiveCount: maxReceives,
			want: "the message cannot be handled and this was its last delivery " +
				"before the dead-letter queue",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := &recorder{}
			queue := newFakeQueue([]sqs.Message{
				message("handle-1", []byte("{not json"), c.receiveCount),
			})
			consumerOver(t, t.Context(), ConsumerConfig{
				Queue: queue, Wagering: &fakeSubmitter{},
				Logger: log.logger(), MaxReceiveCount: maxReceives,
			})
			queue.handled(t)

			record := log.await(t, c.want)
			if got, ok := attr(record, "receiveCount"); !ok || got == "" {
				t.Error("the line does not say which delivery this was")
			}
		})
	}
}

// A receive that failed does not end the loop: the consumer says so and takes
// the next batch.
func TestAFailedReceiveDoesNotEndTheLoop(t *testing.T) {
	log := &recorder{}
	queue := newFakeQueue(nil, []sqs.Message{message("handle-1", validBody(t, nil), 1)})
	queue.errs = []error{app.AsRetryable(errors.New("the queue is throttling"))}
	submitter := &fakeSubmitter{}

	consumerOver(t, t.Context(), ConsumerConfig{
		Queue: queue, Wagering: submitter, Logger: log.logger(),
		Backoff: Backoff{Initial: time.Millisecond, Factor: 2, Max: time.Second},
	})
	queue.handled(t)

	log.await(t, "could not receive from the queue")
	if got := queue.deletedHandles(); len(got) != 1 {
		t.Errorf("deleted = %v, want the consumer to have carried on to the next batch", got)
	}
}

// atomic64 hands out distinct suffixes for a stream of generated messages.
type atomic64 struct {
	mu sync.Mutex
	n  int
}

func (a *atomic64) next() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
	return time.Duration(a.n).String()
}
