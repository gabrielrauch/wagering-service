package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/faults"
)

// What a [ConsumerConfig] leaves open.
const (
	// defaultConcurrency is how many messages one instance handles at once
	// when it is not told. Four rather than one, because a single receiver
	// leaves every other wallet waiting behind whichever one is slow; and
	// rather than something larger, because each of these holds a database
	// connection and a wallet lock for as long as it runs, and the pool is the
	// thing that runs out first.
	defaultConcurrency = 4
	// defaultDrainTimeout is how long [Consumer.Stop] waits for work in hand.
	// It is under the thirty-second visibility timeout the queues are
	// provisioned with, so a message this consumer gives up on is released
	// while its receipt handle is still worth something.
	defaultDrainTimeout = 20 * time.Second

	// The default backoff: doubling from two seconds to a minute. Two seconds
	// because the failures worth retrying here are a lock conflict, a
	// connection lost, a database restarting — measured in seconds; and a
	// minute because the redrive policy ends the message after a handful of
	// deliveries, so a cap beyond that would only be reached by a queue whose
	// policy was changed without changing this.
	defaultConsumerInitial = 2 * time.Second
	defaultConsumerFactor  = 2
	defaultConsumerMax     = 60 * time.Second
)

// maxVisibility is the longest SQS will hide a message, restated here because
// the queue adapter's copy is unexported. It bounds a consumer's backoff at
// construction, so that a policy nothing could ever apply is refused while an
// operator is watching rather than discovered by a message that failed.
const maxVisibility = 12 * time.Hour

// releaseBudget bounds the shutdown's release sweep.
//
// It is the sweep's own rather than the caller's, and that is the point: a
// message is released when the drain deadline has already passed, which is
// exactly the moment the context a shutdown hook was given is likely to be gone.
// Inheriting it would mean the messages that most need giving back are the ones
// that are not.
const releaseBudget = 10 * time.Second

// ConsumerConfig is everything the consumer is built from.
//
// Named fields rather than a positional list, because three of these are counts
// and two are durations and the compiler cannot tell one from another.
type ConsumerConfig struct {
	// Queue is the inbound queue. Required.
	Queue InboundQueue
	// Wagering is the application's write path. Required, and it is the same
	// use case the HTTP adapter calls.
	Wagering Submitter
	// Name identifies this consumer in the inbox, which holds one row per
	// consumer per message, and is the subject every message is submitted
	// under. Required, and bounded by what wagering.opaque_id will store.
	//
	// It names the consumer and not the replica. Two replicas of this service
	// are one consumer: they share the inbox rows that make a redelivery a
	// replay, and a name that varied per process would let the same message be
	// applied once by each of them.
	Name string
	// Concurrency is how many messages this instance handles at once. Zero
	// means [defaultConcurrency].
	//
	// It is bought as that many independent receivers rather than as a pool
	// over one receiver's batch, which is what keeps a wallet's operations in
	// order without this package ever naming a message group — see the package
	// documentation.
	Concurrency int
	// Backoff is how long a message waits after a transient failure. The zero
	// value means the documented default; anything else is taken as given and
	// checked in full.
	Backoff Backoff
	// DrainTimeout is how long [Consumer.Stop] waits for work in hand before
	// giving it back. Zero means [defaultDrainTimeout].
	DrainTimeout time.Duration
	// MaxReceiveCount is the queue's redrive policy, so that the consumer can
	// say when a delivery is the last one before the dead-letter queue — which
	// is the line an operator most wants in the log.
	//
	// Zero means the consumer does not know the policy and says nothing about
	// it. It is configuration rather than something read from the queue,
	// because reading it would be a call per message against a value that
	// changes when somebody deploys.
	MaxReceiveCount int
	// Logger is where the consumer reports what it did. Required: a message
	// that went to the dead-letter queue and a message that was deferred are
	// invisible otherwise, and an absent logger does not degrade that report,
	// it deletes it.
	Logger *slog.Logger
}

// Consumer takes wager operations off the inbound queue and applies them.
//
// One SQL transaction per message, opened by the application layer, and the
// message is deleted only once that transaction has committed. A process that
// dies in between sees the message again; the inbox row, written inside the same
// transaction, is what turns the redelivery into a replay rather than a second
// application.
//
// Start and Stop do not block, so a composition root can drive them from a
// lifecycle hook without owning a goroutine of its own. Nothing here imports a
// dependency-injection framework.
type Consumer struct {
	queue       InboundQueue
	wagering    Submitter
	name        string
	concurrency int
	backoff     Backoff
	drain       time.Duration
	maxReceives int
	logger      *slog.Logger

	mu        sync.Mutex
	started   bool
	stopped   bool
	stopPoll  context.CancelFunc
	stopWork  context.CancelFunc
	receivers []*receiver

	wg   sync.WaitGroup
	held heldMessages
}

// NewConsumer wires the consumer.
//
// Every dependency is refused when absent, because a consumer built without one
// would fail at the first message rather than at start-up — and a consumer that
// fails at the first message has already taken it off the queue.
//
// It performs no I/O and starts nothing. Receiving begins at [Consumer.Start].
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	switch {
	case cfg.Queue == nil:
		return nil, errors.New("workers: the consumer needs an inbound queue")
	case cfg.Wagering == nil:
		return nil, errors.New("workers: the consumer needs a wagering service")
	case cfg.Logger == nil:
		return nil, errors.New("workers: the consumer needs a logger")
	case cfg.Name == "":
		return nil, errors.New("workers: the consumer needs a name for its inbox rows")
	case !opaqueID(cfg.Name):
		return nil, fmt.Errorf("workers: the consumer name is not storable: at most %d bytes, "+
			"no control characters, no surrounding whitespace", maxOpaqueIDBytes)
	case cfg.Concurrency < 0:
		return nil, fmt.Errorf("workers: the consumer needs a positive concurrency, got %d",
			cfg.Concurrency)
	case cfg.DrainTimeout < 0:
		return nil, fmt.Errorf("workers: the consumer needs a positive drain timeout, got %s",
			cfg.DrainTimeout)
	case cfg.MaxReceiveCount < 0:
		return nil, fmt.Errorf("workers: a redrive policy receives at least once, got %d",
			cfg.MaxReceiveCount)
	}

	backoff := cfg.Backoff
	if backoff == (Backoff{}) {
		backoff = Backoff{
			Initial: defaultConsumerInitial,
			Factor:  defaultConsumerFactor,
			Max:     defaultConsumerMax,
		}
	}
	if err := backoff.validate("consumer"); err != nil {
		return nil, err
	}
	if backoff.Max > maxVisibility {
		return nil, fmt.Errorf("workers: a message may be hidden for at most %s, got %s",
			maxVisibility, backoff.Max)
	}

	return &Consumer{
		queue:       cfg.Queue,
		wagering:    cfg.Wagering,
		name:        cfg.Name,
		concurrency: orDefault(cfg.Concurrency, defaultConcurrency),
		backoff:     backoff,
		drain:       orDefaultDuration(cfg.DrainTimeout, defaultDrainTimeout),
		maxReceives: cfg.MaxReceiveCount,
		logger:      cfg.Logger,
		held:        heldMessages{handles: make(map[string]struct{})},
	}, nil
}

// Start begins receiving.
//
// The context it is given is the one every receiver polls under and the one
// every message is handled under, so a composition root that cancels it stops
// this consumer the way a crash would: nothing new is received, work in hand is
// abandoned, and whatever was in flight is redelivered once its visibility
// timeout runs out. [Consumer.Stop] is the graceful door, and it is the one that
// gives work back rather than leaving it to time out.
//
// A consumer is started once. Starting a stopped one is refused rather than
// silently doing nothing, because the caller believes work is being consumed.
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.stopped:
		return errors.New("workers: the consumer has already been stopped")
	case c.started:
		return errors.New("workers: the consumer is already running")
	}

	// Two cancellations, because stopping has two deadlines. Polling ends the
	// moment Stop is called — a long poll is cancelled rather than waited out —
	// while work already in hand keeps running until the drain deadline passes.
	pollCtx, stopPoll := context.WithCancel(ctx)
	workCtx, stopWork := context.WithCancel(ctx)
	c.stopPoll, c.stopWork, c.started = stopPoll, stopWork, true

	c.receivers = make([]*receiver, c.concurrency)
	for i := range c.receivers {
		r := &receiver{id: i}
		c.receivers[i] = r
		c.wg.Add(1)
		go c.receive(pollCtx, workCtx, r)
	}
	c.logger.InfoContext(ctx, "consuming wager operations",
		slog.String("consumer", c.name),
		slog.Int("receivers", c.concurrency))
	return nil
}

// Stop stops receiving, drains what is in hand, and gives back what is left.
//
// It reports whether the drain deadline was hit: an error naming the receivers
// still working means their messages were abandoned mid-flight and released for
// immediate redelivery, which is a fact an operator reading a deployment's logs
// needs and a silent success would hide.
//
// Everything this consumer had taken responsibility for and not finished
// deciding about has its visibility reset to zero, whether the drain finished or
// not. Without that a clean shutdown looks exactly like a crash to the queue,
// and the work waits out a timeout that exists for the case where nobody gave
// it back.
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.started || c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	stopPoll, stopWork, receivers := c.stopPoll, c.stopWork, c.receivers
	c.mu.Unlock()

	stopPoll()

	finished := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(finished)
	}()

	drainCtx, cancelDrain := context.WithTimeout(ctx, c.drain)
	defer cancelDrain()

	var late []string
	select {
	case <-finished:
	case <-drainCtx.Done():
		// Named before the work is cancelled. A receiver that finishes during
		// the cancellation would otherwise be reported or not depending on the
		// scheduler.
		late = stillWorking(receivers)
		stopWork()
		<-finished
	}
	stopWork()

	released := c.releaseHeld(ctx)
	if len(late) == 0 {
		c.logger.InfoContext(ctx, "stopped consuming",
			slog.String("consumer", c.name),
			slog.Int("released", released))
		return nil
	}
	c.logger.WarnContext(ctx, "the drain deadline passed with messages in flight",
		slog.String("consumer", c.name),
		slog.Any("receivers", late),
		slog.Int("released", released))
	return fmt.Errorf(
		"workers: the consumer's drain deadline of %s passed with %d receivers still working (%v); "+
			"%d messages were released for redelivery", c.drain, len(late), late, released)
}

// receiver is one of the loops a consumer runs, and the unit its concurrency is
// counted in.
type receiver struct {
	id int
	// finished is read by Stop to name the receivers that did not come back
	// inside the drain deadline, so a shutdown that ran out of time says which
	// loops it ran out of time on.
	finished atomic.Bool
}

func (r *receiver) name() string { return fmt.Sprintf("receiver-%d", r.id) }

// stillWorking names the receivers that have not returned.
func stillWorking(receivers []*receiver) []string {
	var late []string
	for _, r := range receivers {
		if !r.finished.Load() {
			late = append(late, r.name())
		}
	}
	return late
}

// receive is one receiver's loop: take a batch, handle it in order, take the
// next.
//
// pollCtx bounds the receiving and ends when Stop is called; workCtx bounds the
// handling and ends when the drain deadline passes. Handing the same context to
// both would make a graceful stop indistinguishable from a crash for whatever
// was in flight.
func (c *Consumer) receive(pollCtx, workCtx context.Context, r *receiver) {
	defer c.wg.Done()
	defer r.finished.Store(true)

	failures := 0
	for pollCtx.Err() == nil {
		messages, err := c.queue.Receive(pollCtx)
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			failures++
			c.logger.ErrorContext(pollCtx, "could not receive from the queue",
				slog.String("consumer", c.name),
				slog.String("receiver", r.name()),
				slog.Int("consecutiveFailures", failures),
				slog.String("error", err.Error()))
			// A queue that refused this receive will refuse the next one in the
			// same instant, and an unslowed loop against a throttled queue is
			// how a throttle becomes an outage. The same schedule a message
			// gets, for the same reason, counted in consecutive failures rather
			// than in deliveries — the count is in memory because it decides
			// when this process next asks a question and nothing else.
			if !wait(pollCtx, c.backoff.after(failures)) {
				return
			}
			continue
		}
		failures = 0
		if len(messages) == 0 {
			continue
		}
		// Taken before any of them is handled. A shutdown that arrives
		// mid-batch has to be able to give back the messages this receiver has
		// not reached yet, and it can only give back what it knows it holds.
		c.held.take(messages)
		c.handleBatch(workCtx, messages)
	}
}

// disposition is what the consumer decided about one message.
type disposition int

const (
	// settled means the operation reached an answer that will not change:
	// processed, rejected, parked on a reference, or replayed from a previous
	// delivery. All four are terminal for the message.
	settled disposition = iota
	// deferred means a transient failure. The message is kept and hidden for
	// the backoff; the redelivery finds the inbox empty and does the work
	// again.
	deferred
	// poisoned means a failure that will happen again identically. The message
	// is kept, untouched, for the redrive policy to move to the dead-letter
	// queue.
	poisoned
	// abandoned means the shutdown deadline arrived while this message was
	// being handled. It stays held, so that Stop releases it for immediate
	// redelivery rather than leaving it to time out.
	abandoned
)

// handleBatch handles one received batch, in order, and stops at the first
// message it does not delete.
//
// Stopping is the ordering rule, and it is not caution for its own sake. A FIFO
// receive returns as many messages of one group as it can, and the queue adapter
// does not carry MessageGroupId, so everything behind an undeleted message may
// belong to its group — handling it would be applying a wallet's operations out
// of the order the provider sent them.
//
// What happens to the rest depends on why the batch stopped. A transient failure
// hides them for the same backoff as the message that failed, so the whole group
// becomes visible together and SQS hands it back in order. A permanent one
// leaves them exactly as they are: they were received together, so they time out
// together, and pushing them back sooner than the message ahead of them is the
// one thing that would reorder the group.
func (c *Consumer) handleBatch(ctx context.Context, messages []sqs.Message) {
	for i, message := range messages {
		if ctx.Err() != nil {
			return
		}
		decided, in := c.handle(ctx, message)
		switch decided {
		case settled:
			c.acknowledge(ctx, message)
			continue
		case deferred:
			c.hide(ctx, message, in)
			for _, behind := range messages[i+1:] {
				c.hide(ctx, behind, in)
			}
		case poisoned:
			c.held.done(message.ReceiptHandle)
			for _, behind := range messages[i+1:] {
				c.held.done(behind.ReceiptHandle)
			}
		case abandoned:
			// Left held, along with everything behind it. Stop is what gives
			// them back, and it gives them back at once.
		}
		return
	}
}

// handle applies one message and says what should become of it.
//
// The returned duration is meaningful only for [deferred], where it is how long
// the message is hidden for.
func (c *Consumer) handle(ctx context.Context, m sqs.Message) (disposition, time.Duration) {
	e, err := parseEnvelope(m.Body)
	if err != nil {
		c.poison(ctx, m, "", err)
		return poisoned, 0
	}
	submission, err := c.submission(ctx, e, m)
	if err != nil {
		c.poison(ctx, m, e.MessageID, err)
		return poisoned, 0
	}

	result, err := c.wagering.Submit(ctx, submission)
	if err == nil {
		// The transaction has committed. Everything from here on is outside it,
		// which is why the fault point is exactly here: a process killed
		// between this line and the delete below must see the message again and
		// must not apply it twice, and the inbox row that committed with the
		// work is what makes that true.
		faults.Hit(faults.AfterCommitBeforeAck)
		c.applied(ctx, m, submission, result)
		return settled, 0
	}
	if ctx.Err() != nil {
		// The drain deadline, not the message. Classifying a cancellation as a
		// transient failure would spend a delivery of the redrive budget on a
		// deployment.
		return abandoned, 0
	}

	class := app.ClassOf(err)
	if !transient(class) {
		c.poison(ctx, m, e.MessageID, err)
		return poisoned, 0
	}
	in := wholeSeconds(c.backoff.after(m.ReceiveCount))
	c.logger.WarnContext(ctx, "the operation could not be applied and will be delivered again",
		c.about(m, submission.Correlation,
			slog.String("messageId", e.MessageID),
			slog.String("class", string(class)),
			slog.Duration("hiddenFor", in),
			slog.String("error", err.Error()))...)
	return deferred, in
}

// transient reports whether a class of failure is worth another delivery.
//
// [app.Retryable] is the obvious one: it recorded nothing and the condition that
// caused it is expected to pass.
//
// [app.NotFound] is the interesting one, and it is here deliberately. The only
// way this layer produces it is a submission for a player who holds no wallet in
// that currency — nothing is persisted, the idempotency key is still free, and
// the same message succeeds unchanged once the wallet is opened. Sending it
// straight to the dead-letter queue would make an ordering race between opening
// a wallet and the first operation on it into an operator's morning. It is still
// bounded: the redrive policy ends the message after its deliveries are spent,
// exactly as it would have.
//
// Everything else is permanent, including every business refusal. Those are not
// failures at all — a rejection is an outcome with a row and an event behind it,
// and it never reaches this function.
func transient(class app.Class) bool {
	return class == app.Retryable || class == app.NotFound
}

// submission turns a validated envelope into the command the application layer
// takes.
//
// The principal is built from the provider the message names, and the queue is
// what authorises that. Nothing on an SQS message can be authenticated the way a
// bearer token can, so the authorisation boundary is the queue itself: a message
// on it was put there by something the deployment's own policy allowed to write
// to it. The subject records which consumer admitted it, which is the only thing
// this system knows about where the authority came from, and the application
// layer keeps a subject for the audit trail and never for a decision.
func (c *Consumer) submission(
	ctx context.Context, e envelope, m sqs.Message,
) (app.SubmitOperation, error) {
	provider, err := wagering.NewProvider(e.Data.Provider)
	if err != nil {
		return app.SubmitOperation{}, err
	}
	principal, err := app.NewProviderPrincipal(provider, c.name)
	if err != nil {
		return app.SubmitOperation{}, err
	}
	correlation, replaced := c.correlationOf(e, m)
	if replaced {
		c.logger.WarnContext(ctx, "the message's correlation was not usable and was replaced",
			c.about(m, correlation, slog.String("messageId", e.MessageID))...)
	}
	return app.SubmitOperation{
		Principal:   principal,
		Correlation: correlation,
		Inbox: &app.InboxMessage{
			Consumer:  c.name,
			MessageID: e.MessageID,
			BodyHash:  bodyHash(m.Body),
		},
		Fields: e.fields(),
	}, nil
}

// correlationOf reads the thread this message belongs to, and reports whether
// the supplied one had to be replaced.
//
// The attribute is preferred because it is what a producer that is already
// tracing a request will have set. The envelope's message id is the fallback
// rather than a fresh identifier: it is present on every message this consumer
// accepts, it is already the inbox's key, and it ties the log lines of every
// delivery of one message together — which a value minted per delivery would
// not.
func (c *Consumer) correlationOf(e envelope, m sqs.Message) (string, bool) {
	supplied, carried := m.Attributes[correlationAttribute]
	if usableCorrelation(supplied) {
		return supplied, false
	}
	return e.MessageID, carried
}

// acknowledge deletes a message whose transaction has committed.
//
// A delete that fails is a warning and not a failure of the message. The work is
// recorded; only the acknowledgement was lost, so the redelivery that follows
// finds the inbox row and replays the settled result.
func (c *Consumer) acknowledge(ctx context.Context, m sqs.Message) {
	defer c.held.done(m.ReceiptHandle)
	if err := c.queue.Delete(ctx, m.ReceiptHandle); err != nil {
		c.logger.WarnContext(ctx, "the message was handled but could not be deleted",
			c.about(m, "", slog.String("error", err.Error()))...)
	}
}

// hide keeps a message and pushes its next delivery out by in.
func (c *Consumer) hide(ctx context.Context, m sqs.Message, in time.Duration) {
	defer c.held.done(m.ReceiptHandle)
	if err := c.queue.ChangeVisibility(ctx, m.ReceiptHandle, in); err != nil {
		// The message still comes back, at the queue's own visibility timeout
		// rather than at the backoff's. Worth a line, not worth failing over.
		c.logger.WarnContext(ctx, "the message could not be hidden for its backoff",
			c.about(m, "", slog.Duration("hiddenFor", in),
				slog.String("error", err.Error()))...)
	}
}

// poison records a message that will fail the same way however often it is
// delivered, and leaves it for the redrive policy.
//
// It is not deleted and its visibility is not touched. Deleting it would discard
// an operation a provider believes it submitted with no trace of it anywhere but
// a log line; releasing it would spend the whole redrive budget in one burst and
// write the same line five times in a second. The redrive policy is the
// mechanism that is meant to decide, and leaving the message alone is what lets
// it.
func (c *Consumer) poison(ctx context.Context, m sqs.Message, messageID string, cause error) {
	attrs := c.about(m, "",
		slog.String("messageId", messageID),
		slog.String("class", string(app.ClassOf(cause))),
		slog.String("error", cause.Error()))
	if c.lastDelivery(m) {
		c.logger.ErrorContext(ctx,
			"the message cannot be handled and this was its last delivery before the dead-letter queue",
			attrs...)
		return
	}
	c.logger.ErrorContext(ctx, "the message cannot be handled and was left for the redrive policy",
		attrs...)
}

// applied reports an operation that reached an answer.
//
// What it says is the operation's identity and what it came to, and nothing of
// what it was worth: an amount, a balance and a player are a financial payload,
// and a log is not where one belongs.
func (c *Consumer) applied(
	ctx context.Context, m sqs.Message, s app.SubmitOperation, result app.OperationResult,
) {
	c.logger.InfoContext(ctx, "the operation was applied",
		c.about(m, s.Correlation,
			slog.String("messageId", s.Inbox.MessageID),
			slog.String("transactionId", result.TransactionID.String()),
			slog.String("kind", result.Kind.String()),
			slog.String("status", result.Status.String()),
			slog.String("failureCode", result.FailureCode.String()),
			slog.Bool("replay", result.IdempotentReplay))...)
}

// about is the attributes every line about one message carries.
//
// The queue's own message id rather than the envelope's, because this identifies
// the delivery an operator would look for in the queue; the envelope's is added
// by the caller once it is known, which on a body that could not be read is
// never.
func (c *Consumer) about(m sqs.Message, correlation string, extra ...slog.Attr) []any {
	attrs := []any{
		slog.String("consumer", c.name),
		slog.String("queueMessageId", m.MessageID),
		slog.Int("receiveCount", m.ReceiveCount),
	}
	if correlation != "" {
		attrs = append(attrs, slog.String("correlationId", correlation))
	}
	for _, attr := range extra {
		attrs = append(attrs, attr)
	}
	return attrs
}

// lastDelivery reports whether this delivery is the last one the redrive policy
// allows.
//
// ApproximateReceiveCount is the same number the policy is judged on, so a
// message in hand for the fifth time against a maxReceiveCount of five is one
// delivery from the dead-letter queue. It is approximate, as SQS names it, which
// is why it decides a log line and a backoff here and nothing that has to be
// exact.
func (c *Consumer) lastDelivery(m sqs.Message) bool {
	return c.maxReceives > 0 && m.ReceiveCount >= c.maxReceives
}

// releaseHeld gives back every message this consumer has not finished deciding
// about, and reports how many it managed to release.
func (c *Consumer) releaseHeld(ctx context.Context) int {
	handles := c.held.drain()
	if len(handles) == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseBudget)
	defer cancel()

	released := 0
	for _, handle := range handles {
		if err := c.queue.Release(ctx, handle); err != nil {
			c.logger.WarnContext(ctx, "a message in flight could not be released",
				slog.String("consumer", c.name),
				slog.String("error", err.Error()))
			continue
		}
		released++
	}
	return released
}

// heldMessages is every message a consumer has taken responsibility for and has
// not finished deciding about.
//
// A message joins when its batch is received and leaves the moment it is
// deleted, hidden, or deliberately left — the three decisions, all of which are
// this consumer saying it is done with it. What remains is what a shutdown has
// to give back, which is the only question this set exists to answer.
type heldMessages struct {
	mu      sync.Mutex
	handles map[string]struct{}
}

func (h *heldMessages) take(messages []sqs.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range messages {
		h.handles[m.ReceiptHandle] = struct{}{}
	}
}

func (h *heldMessages) done(receiptHandle string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.handles, receiptHandle)
}

// drain empties the set and returns what was in it, so that a release sweep
// cannot hand the same message back twice.
func (h *heldMessages) drain() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	handles := make([]string, 0, len(h.handles))
	for handle := range h.handles {
		handles = append(handles, handle)
	}
	clear(h.handles)
	return handles
}

// orDefault and orDefaultDuration read a configuration field that means "the
// documented default" when it is zero.
func orDefault(v, fallback int) int {
	if v == 0 {
		return fallback
	}
	return v
}

func orDefaultDuration(v, fallback time.Duration) time.Duration {
	if v == 0 {
		return fallback
	}
	return v
}
