//go:build integration

// The service, assembled the way a composition root will assemble it, onto one
// test's database — and the two decorators this suite watches it through.
package messaging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/workers"
)

// The bounds this suite runs the service under.
//
// They are deployment numbers rather than test values, with one exception. The
// lock timeout is generous because the shutdown scenario parks a submission on
// a wallet lock deliberately and needs the wait to outlast the drain deadline
// it is watching — a shorter timeout there would end the submission before the
// shutdown had anything in flight to release.
const (
	stackLockTimeout      = 30 * time.Second
	stackStatementTimeout = 15 * time.Second
	stackConnectTimeout   = 5 * time.Second
	stackMaxConns         = 8
)

// The defaults a [stackOptions] leaves open: a wait budget no scenario reaches
// unless it asked to, and a schedule that looks at a parked operation again
// within a second.
const (
	defaultReferenceTries  = 20
	defaultReferenceBudget = time.Minute
)

// stack is one running service: the use cases a message reaches, the outbox a
// publisher claims from, and an owner connection this suite reads the tables
// with.
type stack struct {
	// dsn names this test's database, as the role that owns it.
	dsn string
	// owner reads the tables directly, as the role that can see all of them.
	//
	// Almost nothing here writes through it, and what does is named: a fixture
	// that reached past the use cases would be fixing up state the service is
	// supposed to have produced. The exception is the republication scenario,
	// which has to reproduce an outbox row that reached the queue and was never
	// marked — a state this service only gets into by dying between the two,
	// and the fault machinery that kills it belongs to a later task.
	owner    *pgxpool.Pool
	claims   *postgres.OutboxClaims
	wagering *app.Wagering
	wallets  *app.Wallets
	logs     *recorder
}

// stackOptions is what a scenario varies about the service it runs against.
type stackOptions struct {
	// referenceTries and referenceBudget are the domain's wait budget, and the
	// two numbers the reference scenarios are entirely about.
	referenceTries  int
	referenceBudget time.Duration
	// backoff is how long a parked operation waits between attempts. It is the
	// application layer's schedule, kept apart from the budget above so that
	// tuning it cannot change whether an operation is eventually settled.
	backoff app.BackoffPolicy
	// lockTimeout is how long a statement waits for a wallet's row lock. It is
	// varied by exactly one scenario, which needs the wait to END rather than
	// to outlast a shutdown.
	lockTimeout time.Duration
}

type stackOption func(*stackOptions)

// withReferenceBudget sets how many attempts a parked operation gets and how
// long it may wait at all.
func withReferenceBudget(tries int, ttl time.Duration) stackOption {
	return func(o *stackOptions) { o.referenceTries, o.referenceBudget = tries, ttl }
}

// withReferenceSchedule sets how long a parked operation waits between
// attempts.
func withReferenceSchedule(p app.BackoffPolicy) stackOption {
	return func(o *stackOptions) { o.backoff = p }
}

// withLockTimeout sets how long a statement waits for a wallet's row lock
// before PostgreSQL refuses it.
func withLockTimeout(d time.Duration) stackOption {
	return func(o *stackOptions) { o.lockTimeout = d }
}

// newStack wires the service onto a freshly migrated database.
//
// Nothing in it stands in for anything. The pool is opened by the adapter's own
// constructor as wagering_app, and the transaction manager, the outbox claims,
// the repositories and the two use cases are the real ones.
func newStack(t *testing.T, opts ...stackOption) *stack {
	t.Helper()
	requireCluster(t)
	requireQueues(t)

	settings := stackOptions{
		referenceTries:  defaultReferenceTries,
		referenceBudget: defaultReferenceBudget,
		backoff: app.BackoffPolicy{
			Initial: 500 * time.Millisecond, Factor: 2, Max: 5 * time.Second,
		},
		lockTimeout: stackLockTimeout,
	}
	for _, opt := range opts {
		opt(&settings)
	}

	dsn := migrated(t)
	pool, err := postgres.NewPool(t.Context(), postgres.PoolConfig{
		DSN:            asApplication(dsn),
		MaxConns:       stackMaxConns,
		ConnectTimeout: stackConnectTimeout,
	})
	if err != nil {
		t.Fatalf("open the service pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// Checked rather than assumed. A DSN option the server did not honour would
	// leave this suite connected as the owner, every privilege the schema
	// grants would be wider than the service's, and nothing below would notice
	// — a superuser passes every test an application role passes.
	var role string
	if err := pool.QueryRow(t.Context(), "SELECT current_user").Scan(&role); err != nil {
		t.Fatalf("read the role the service connected as: %v", err)
	}
	if role != "wagering_app" {
		t.Fatalf("the service connected as %q, wanted wagering_app", role)
	}

	manager, err := postgres.NewTxManager(postgres.TxConfig{
		Pool:             pool,
		LockTimeout:      settings.lockTimeout,
		StatementTimeout: stackStatementTimeout,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}
	claims, err := postgres.NewOutboxClaims(pool)
	if err != nil {
		t.Fatalf("wire the outbox claims: %v", err)
	}

	policy, err := wagering.NewReferencePolicy(settings.referenceTries, settings.referenceBudget)
	if err != nil {
		t.Fatalf("reference policy: %v", err)
	}
	processor, err := wagering.NewProcessor(policy)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	wagers, err := app.NewWagering(app.WageringDeps{
		Tx:        manager,
		Processor: processor,
		Clock:     wallClock{},
		IDs:       mintedIDs{},
		Backoff:   settings.backoff,
		Defects:   loggedDefects{t: t},
	})
	if err != nil {
		t.Fatalf("wire the wagering service: %v", err)
	}
	wallets, err := app.NewWallets(manager, wallClock{}, mintedIDs{}, nil)
	if err != nil {
		t.Fatalf("wire the wallets service: %v", err)
	}

	owner, err := newAdminPool(t.Context(), dsn, 4)
	if err != nil {
		t.Fatalf("open an owner pool: %v", err)
	}
	t.Cleanup(owner.Close)

	return &stack{
		dsn:      dsn,
		owner:    owner,
		claims:   claims,
		wagering: wagers,
		wallets:  wallets,
		logs:     newRecorder(t),
	}
}

// wallClock is the clock the service reads, at the resolution [app.Clock] asks
// for.
//
// The real one rather than a settable fixture, because two movements on one
// wallet have to be stamped with instants that move strictly forward and the
// wallet lock already serialises them by far more than a microsecond. A fixed
// clock here would have this suite arranging the one thing the schema is
// checking.
type wallClock struct{}

// Now reports the current time, UTC and truncated to microseconds.
func (wallClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// mintedIDs mints a fresh identifier on every call, which is what the service
// does.
type mintedIDs struct{}

func (mintedIDs) WalletID() wagering.WalletID           { return wagering.NewWalletID() }
func (mintedIDs) TransactionID() wagering.TransactionID { return wagering.NewTransactionID() }
func (mintedIDs) LedgerEntryID() wagering.LedgerEntryID { return wagering.NewLedgerEntryID() }
func (mintedIDs) EventID() app.EventID                  { return app.NewEventID() }

// loggedDefects is the hook for a parked operation that could not be carried
// forward.
//
// It logs rather than discards, unlike the end-to-end suite's, because the
// reference scenarios here are the ones this hook exists for: an operation that
// cannot be rebuilt stays parked and is then rejected when its budget runs out,
// which looks exactly like the TTL rejection one of those scenarios is
// asserting. Printing it is the difference between a red test that says why and
// one that says the status was wrong.
type loggedDefects struct{ t *testing.T }

func (d loggedDefects) CannotCarryForward(
	_ context.Context, id wagering.TransactionID, err error,
) {
	d.t.Logf("the service could not carry operation %s forward: %v", id, err)
}

// recorder collects what the workers logged, prints it when the test that
// produced it failed, and lets a scenario wait for a line.
//
// Waiting on a line is not the same as asserting on one, and only the waiting
// is done here: every scenario below states its outcome in rows, in queue
// contents or in what the decorators saw. What a log line is used for is
// knowing WHEN the thing being asserted on has happened, which is otherwise a
// sleep long enough to be slow and short enough to be flaky.
type recorder struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	ready  chan struct{}
	logger *slog.Logger
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()
	r := &recorder{ready: make(chan struct{}, 1)}
	r.logger = slog.New(slog.NewTextHandler(r, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		t.Logf("what the workers logged:\n%s", r.buffer.String())
	})
	return r
}

// Write implements [io.Writer] for the log handler, and wakes whoever is
// waiting for a line.
func (r *recorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	n, err := r.buffer.Write(p)
	r.mu.Unlock()
	select {
	case r.ready <- struct{}{}:
	default:
	}
	return n, err
}

// count reports how many logged lines contain phrase.
func (r *recorder) count(phrase string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Count(r.buffer.String(), phrase)
}

// await blocks until phrase has been logged at least want times, and fails the
// test when it is not within the budget.
func (r *recorder) await(t *testing.T, phrase string, want int, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		if r.count(phrase) >= want {
			return
		}
		select {
		case <-r.ready:
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			if r.count(phrase) >= want {
				return
			}
			t.Fatalf("waited %s for %q to be logged %d times, saw it %d times",
				within, phrase, want, r.count(phrase))
		}
	}
}

// The lines this suite waits on.
//
// They are the consumer's and the reference worker's own, and nothing here
// checks that a worker still says them: one that renamed a line would turn
// every wait below into a timeout, which is a slow red rather than a quiet
// green. They are constants so that the rename is one edit, and so that a
// timeout names the line that went missing.
const (
	logApplied  = "the operation was applied"
	logDeferred = "the operation could not be applied and will be delivered again"
	logCarried  = "a parked operation was carried forward"
)

// openQueue builds the real queue adapter on a fixture queue and resolves it.
//
// The wait time is a second rather than the adapter's twenty-second default.
// Long polling is still long polling at a second — a short poll is zero, and
// the adapter refuses to express one — but a loop that gives its context up
// once a second is a loop a scenario can stop without waiting out a poll it no
// longer cares about.
func openQueue(t *testing.T, name string) *sqs.Queue {
	t.Helper()
	return openQueueOn(t, name, sharedSDK)
}

// openQueueOn is [openQueue] on a client the caller built, which is how the
// publisher scenarios watch what reached the wire.
func openQueueOn(t *testing.T, name string, client *awssqs.Client) *sqs.Queue {
	t.Helper()
	requireQueues(t)
	queue, err := sqs.NewQueue(sqs.Config{
		Client:      client,
		Name:        name,
		WaitTime:    time.Second,
		MaxMessages: 10,
	})
	if err != nil {
		t.Fatalf("build a queue on %s: %v", name, err)
	}
	if err := queue.OnStart(t.Context()); err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	return queue
}

// delivery is one message as it was handed to the consumer, and when.
//
// The instant is here for one assertion and is load-bearing for it: a message
// that came back sooner than its queue's own visibility timeout came back
// because the consumer pushed it back, and there is no other way to tell that
// from a message the queue timed out on.
type delivery struct {
	messageID     string
	receiptHandle string
	body          string
	receiveCount  int
	at            time.Time
}

// hidden is one call the consumer made to push a message's next delivery out.
type hidden struct {
	receiptHandle string
	in            time.Duration
}

// watchedQueue is the real queue with a record of what passed through it.
//
// It is not a substitute for anything: every method below forwards to the
// embedded *sqs.Queue against real LocalStack and returns exactly what it
// returned. What is added is memory, and it is here because half of what these
// scenarios are about leaves no row — that a duplicate was genuinely delivered
// a second time, that a transient failure pushed a message's visibility out
// rather than deleting it, that a shutdown handed a message back. A suite
// asserting only on the final balance would pass with all three of those
// deleted, which is the failure this suite was written after.
type watchedQueue struct {
	*sqs.Queue

	mu         sync.Mutex
	deliveries []delivery
	deletes    []string
	hides      []hidden
	releases   []string
	// owners maps a receipt handle back to the message it was issued for, so
	// that an assertion can be stated about a message rather than about a
	// handle that changes on every delivery.
	owners map[string]string
}

func watch(queue *sqs.Queue) *watchedQueue {
	return &watchedQueue{Queue: queue, owners: map[string]string{}}
}

// Receive forwards the receive and records what came back.
func (w *watchedQueue) Receive(ctx context.Context) ([]sqs.Message, error) {
	messages, err := w.Queue.Receive(ctx)
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range messages {
		w.deliveries = append(w.deliveries, delivery{
			messageID:     m.MessageID,
			receiptHandle: m.ReceiptHandle,
			body:          string(m.Body),
			receiveCount:  m.ReceiveCount,
			at:            time.Now(),
		})
		w.owners[m.ReceiptHandle] = m.MessageID
	}
	return messages, err
}

// Delete forwards the delete and records it.
func (w *watchedQueue) Delete(ctx context.Context, receiptHandle string) error {
	err := w.Queue.Delete(ctx, receiptHandle)
	w.record(&w.deletes, receiptHandle)
	return err
}

// ChangeVisibility forwards the call and records what was asked for.
func (w *watchedQueue) ChangeVisibility(
	ctx context.Context, receiptHandle string, in time.Duration,
) error {
	err := w.Queue.ChangeVisibility(ctx, receiptHandle, in)
	w.mu.Lock()
	w.hides = append(w.hides, hidden{receiptHandle: receiptHandle, in: in})
	w.mu.Unlock()
	return err
}

// Release forwards the release and records it.
//
// It is recorded separately from [watchedQueue.ChangeVisibility] although the
// adapter implements one in terms of the other, because the two mean different
// things to a scenario: a hide is a backoff and a release is a shutdown giving
// work back, and a suite that could not tell them apart would let the shutdown
// scenario pass on a message that was merely deferred.
func (w *watchedQueue) Release(ctx context.Context, receiptHandle string) error {
	err := w.Queue.Release(ctx, receiptHandle)
	w.record(&w.releases, receiptHandle)
	return err
}

func (w *watchedQueue) record(into *[]string, receiptHandle string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	*into = append(*into, receiptHandle)
}

// delivered reports every delivery of one message, oldest first.
func (w *watchedQueue) delivered(messageID string) []delivery {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []delivery
	for _, d := range w.deliveries {
		if d.messageID == messageID {
			out = append(out, d)
		}
	}
	return out
}

// messagesFor names the messages one call list concerns, in the order the calls
// were made.
func (w *watchedQueue) messagesFor(handles []string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(handles))
	for _, handle := range handles {
		out = append(out, w.owners[handle])
	}
	return out
}

// deleted, hiddenCalls and released report what the consumer did with the
// messages it was given, in the order it did it.
func (w *watchedQueue) deleted() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.deletes...)
}

func (w *watchedQueue) hiddenCalls() []hidden {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]hidden(nil), w.hides...)
}

func (w *watchedQueue) released() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.releases...)
}

// submitted is one call the consumer made to the write path, and what came
// back.
type submitted struct {
	// messageID is the envelope's, empty for a submission that carried no
	// inbox identity — which is the shape the HTTP handler produces.
	messageID string
	result    app.OperationResult
	err       error
}

// watchedSubmitter is the real write path with a record of what was asked of
// it.
//
// Like [watchedQueue] it replaces nothing: every call reaches the real
// app.Wagering over the real transaction manager, and the result is returned
// untouched. It exists because "the duplicate was received and answered as a
// replay" and "the duplicate was never delivered at all" leave exactly the same
// rows behind, and only one of them is the behaviour being asserted.
type watchedSubmitter struct {
	inner workers.Submitter

	mu    sync.Mutex
	calls []submitted
}

func follow(inner workers.Submitter) *watchedSubmitter {
	return &watchedSubmitter{inner: inner}
}

// Submit forwards the submission and records what it came to.
func (w *watchedSubmitter) Submit(
	ctx context.Context, cmd app.SubmitOperation,
) (app.OperationResult, error) {
	result, err := w.inner.Submit(ctx, cmd)
	var messageID string
	if cmd.Inbox != nil {
		messageID = cmd.Inbox.MessageID
	}
	w.mu.Lock()
	w.calls = append(w.calls, submitted{messageID: messageID, result: result, err: err})
	w.mu.Unlock()
	return result, err
}

// submissions reports every call made, oldest first.
func (w *watchedSubmitter) submissions() []submitted {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]submitted(nil), w.calls...)
}

// consumerSettings is what a scenario varies about the consumer it starts.
type consumerSettings struct {
	// name is the inbox consumer name, and is identical across replicas by
	// design: it is what makes a redelivery to another replica a replay.
	name string
	// backoff is how long a message waits after a transient failure. The zero
	// value takes the worker's own default.
	backoff workers.Backoff
	// drain is how long Stop waits for work in hand. Zero takes the default.
	drain time.Duration
}

// startConsumer wires and starts a consumer against a queue and a write path.
//
// The consumer is stopped on the way out whether the scenario stopped it or
// not. A second Stop is a no-op, so a scenario that asserts on its own shutdown
// is not fighting this one.
func startConsumer(
	t *testing.T,
	s *stack,
	queue workers.InboundQueue,
	submitter workers.Submitter,
	settings consumerSettings,
) *workers.Consumer {
	t.Helper()
	if settings.name == "" {
		settings.name = consumerName
	}
	consumer, err := workers.NewConsumer(workers.ConsumerConfig{
		Queue:    queue,
		Wagering: submitter,
		Name:     settings.name,
		// One receiver rather than the worker's default of four. Every
		// scenario here counts deliveries of one message, and a second
		// receiver could not take that message — SQS holds its group — but
		// could take the redelivery a moment before or after the first
		// receiver's poll, which turns a count into a schedule.
		Concurrency:     1,
		Backoff:         settings.backoff,
		DrainTimeout:    settings.drain,
		MaxReceiveCount: maxReceives,
		Logger:          s.logs.logger,
	})
	if err != nil {
		t.Fatalf("wire the consumer: %v", err)
	}
	if err := consumer.Start(t.Context()); err != nil {
		t.Fatalf("start the consumer: %v", err)
	}
	t.Cleanup(func() { stopWorker(t, "consumer", consumer.Stop) })
	return consumer
}

// startPublisher wires and starts an outbox publisher.
//
// The name is the caller's and is deliberately not defaulted: it lands in
// claimed_by and must be distinct per process, or two publishers reschedule
// each other's claims. A scenario that runs two of them names both.
func startPublisher(
	t *testing.T, s *stack, queue workers.OutboundQueue, name string, hold time.Duration,
) *workers.Publisher {
	t.Helper()
	publisher, err := workers.NewPublisher(workers.PublisherConfig{
		Outbox:   s.claims,
		Queue:    queue,
		Clock:    wallClock{},
		Name:     name,
		Hold:     hold,
		Interval: 100 * time.Millisecond,
		Logger:   s.logs.logger,
	})
	if err != nil {
		t.Fatalf("wire the publisher %s: %v", name, err)
	}
	if err := publisher.Start(t.Context()); err != nil {
		t.Fatalf("start the publisher %s: %v", name, err)
	}
	t.Cleanup(func() { stopWorker(t, "publisher "+name, publisher.Stop) })
	return publisher
}

// startReferenceWorker wires and starts the worker that carries parked
// operations forward.
func startReferenceWorker(t *testing.T, s *stack) *workers.ReferenceWorker {
	t.Helper()
	worker, err := workers.NewReferenceWorker(workers.ReferenceConfig{
		Wagering: s.wagering,
		Name:     "reference-worker",
		Interval: 100 * time.Millisecond,
		Logger:   s.logs.logger,
	})
	if err != nil {
		t.Fatalf("wire the reference worker: %v", err)
	}
	if err := worker.Start(t.Context()); err != nil {
		t.Fatalf("start the reference worker: %v", err)
	}
	t.Cleanup(func() { stopWorker(t, "reference worker", worker.Stop) })
	return worker
}

// stopWorker shuts one worker down on the way out of a test.
//
// A drain that ran out of time is logged rather than failed. This is teardown
// and not an assertion: the scenario that is about shutdown stops its consumer
// itself and asserts on what came back, and a teardown that failed every test
// which happened to leave a message in flight would be reporting the fixture
// rather than the service.
func stopWorker(t *testing.T, what string, stop func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
	defer cancel()
	if err := stop(ctx); err != nil {
		t.Logf("stopping the %s: %v", what, err)
	}
}
