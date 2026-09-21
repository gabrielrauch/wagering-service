//go:build integration

package fxmod

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/fx/fxtest"

	httpapi "github.com/gabrielrauch/wagering-service/internal/adapters/http"
	"github.com/gabrielrauch/wagering-service/internal/config"
)

// loaded reads the environment this suite configures a binary from, through the
// real loader.
func loaded(t *testing.T, overrides map[string]string) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Static(environment(t, overrides)))
	if err != nil {
		t.Fatalf("load the configuration: %v", err)
	}
	return cfg
}

// hookRun is one lifecycle hook Fx ran: who registered it, and what it
// reported.
type hookRun struct {
	caller string
	err    error
}

// hooks records the lifecycle hooks Fx ran, in the order it ran them.
//
// It is an fxevent.Logger because that is Fx's own account of its own
// lifecycle, and CallerName on those events is the function that appended the
// hook — this package's constructors, by name. Asserting on that is asserting
// on which component's hook ran when, which is the property the shutdown
// ordering is: a log line would be a restatement of the code that wrote it,
// where this is the framework reporting what it did.
type hooks struct {
	mu      sync.Mutex
	started []hookRun
	stopped []hookRun
}

// logger is the constructor fx.WithLogger takes.
func (h *hooks) logger() fxevent.Logger { return h }

// LogEvent records the two events this suite is about and ignores the rest.
func (h *hooks) LogEvent(event fxevent.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch e := event.(type) {
	case *fxevent.OnStartExecuted:
		h.started = append(h.started, hookRun{caller: e.CallerName, err: e.Err})
	case *fxevent.OnStopExecuted:
		h.stopped = append(h.stopped, hookRun{caller: e.CallerName, err: e.Err})
	}
}

// ranStop reports where in the shutdown a hook registered by the named
// constructor ran, and fails if none did.
//
// The match is on a suffix of the fully qualified name, so a test names
// "newPool" rather than the package path Fx reports it under.
func (h *hooks) ranStop(t *testing.T, constructor string) int {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	for i, run := range h.stopped {
		if strings.HasSuffix(run.caller, "."+constructor) {
			return i
		}
	}
	t.Fatalf("no OnStop hook registered by %s ran; the shutdown was %v",
		constructor, callersOf(h.stopped))
	return -1
}

// ranStart reports where in the start-up a hook registered by the named
// constructor ran, and fails if none did.
func (h *hooks) ranStart(t *testing.T, constructor string) int {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	for i, run := range h.started {
		if strings.HasSuffix(run.caller, "."+constructor) {
			return i
		}
	}
	t.Fatalf("no OnStart hook registered by %s ran; the start-up was %v",
		constructor, callersOf(h.started))
	return -1
}

// startUp is the order the start hooks ran in.
func (h *hooks) startUp() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return callersOf(h.started)
}

// failures reports every hook that returned an error.
func (h *hooks) failures() []hookRun {
	h.mu.Lock()
	defer h.mu.Unlock()

	var failed []hookRun
	for _, run := range append(append([]hookRun{}, h.started...), h.stopped...) {
		if run.err != nil {
			failed = append(failed, run)
		}
	}
	return failed
}

// shutdown is the order the stop hooks ran in, for a failure message.
func (h *hooks) shutdown() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return callersOf(h.stopped)
}

func callersOf(runs []hookRun) []string {
	names := make([]string, 0, len(runs))
	for _, run := range runs {
		if dot := strings.LastIndex(run.caller, "."); dot >= 0 {
			names = append(names, run.caller[dot+1:])
			continue
		}
		names = append(names, run.caller)
	}
	return names
}

// TestTheWorkerStartsAndStopsAgainstTheRealStack is the start-and-stop proof
// for cmd/worker.
//
// It asserts four things the graph is only ever wrong about at run time: that
// every start-up check passes against real infrastructure, that every loop
// stops within its drain deadline and says so, that the pool is closed
// afterwards, and that it was closed AFTER the loops rather than before.
func TestTheWorkerStartsAndStopsAgainstTheRealStack(t *testing.T) {
	requireDatabase(t)
	requireQueues(t)

	cfg := loaded(t, nil)
	recorded := &hooks{}

	var pool *pgxpool.Pool
	app := fxtest.New(t,
		Worker(cfg),
		fx.Populate(&pool),
		fx.WithLogger(recorded.logger),
	)
	app.RequireStart()

	// The pool reaches the cluster. This is the state the start-up check
	// claimed, read back rather than taken on the hook's word.
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("the worker started and its pool cannot reach the database: %v", err)
	}
	// Both queues resolved, which is what makes a queue nobody provisioned a
	// start-up failure rather than a consumer that receives nothing.
	for _, queue := range []string{"newInboundQueue", "newOutboundQueue"} {
		recorded.ranStart(t, queue)
	}
	recorded.ranStart(t, "newDatabaseHealth")

	app.RequireStop()

	// Every loop stopped, and stopped cleanly. stopWorker turns a drain that
	// ran out of time into an error on the hook, so a hook that ran and
	// reported nothing is a loop that gave its work back within its deadline.
	for _, loop := range []string{"runConsumer", "runPublisher", "runReferenceWorker"} {
		recorded.ranStop(t, loop)
	}
	if failed := recorded.failures(); len(failed) != 0 {
		t.Fatalf("hooks reported failures: %v", failed)
	}

	// The pool is closed. pgxpool refuses every call afterwards, so this is the
	// resource itself answering rather than a flag this suite set.
	if err := pool.Ping(context.Background()); err == nil {
		t.Fatal("the worker stopped and its pool is still open")
	}

	// And it closed after the loops. Fx runs OnStop in the reverse of the order
	// the hooks were appended, and the appending order is the dependency graph:
	// every loop is built from the pool, so every loop's hook is appended after
	// the pool's and runs before it. The failure this prevents is concrete —
	// Publisher.Stop hands its outbox claims back through this pool, and a pool
	// closed first leaves each claimed row holding up its wallet's whole event
	// stream until the hold expires.
	closed := recorded.ranStop(t, "newPool")
	for _, loop := range []string{"runConsumer", "runPublisher", "runReferenceWorker"} {
		if stopped := recorded.ranStop(t, loop); stopped > closed {
			t.Errorf("the pool closed before %s stopped; the shutdown was %v",
				loop, recorded.shutdown())
		}
	}
	// Telemetry is flushed last, after everything that could still emit into it
	// has stopped. That position is the whole of the seam step 5 plugs the
	// OpenTelemetry SDK into.
	if flushed := recorded.ranStop(t, "newLogger"); flushed < closed {
		t.Errorf("telemetry was flushed before the pool closed; the shutdown was %v",
			recorded.shutdown())
	}
}

// TestTheAPIStartsAndStopsAgainstTheRealStack is the start-and-stop proof for
// cmd/api.
//
// The server is asked a question over a socket rather than asserted about,
// because "the listener is bound" and "the handler is wired to a database and a
// queue that answer" are two different claims and /health/ready is the one
// request that makes both.
func TestTheAPIStartsAndStopsAgainstTheRealStack(t *testing.T) {
	requireDatabase(t)
	requireQueues(t)
	requireIdentityProvider(t)

	cfg := loaded(t, nil)
	recorded := &hooks{}

	var (
		pool   *pgxpool.Pool
		server *httpapi.Server
	)
	app := fxtest.New(t,
		API(cfg),
		fx.Populate(&pool, &server),
		fx.WithLogger(recorded.logger),
	)
	app.RequireStart()

	// The key set was fetched from the real realm. Without this hook a realm
	// that does not exist is a wall of 401s at the first request instead of a
	// process that refused to start.
	recorded.ranStart(t, "newAuthenticator")

	base := "http://" + server.Addr()
	if status, body := get(t, base+"/health/live"); status != http.StatusOK {
		t.Fatalf("GET /health/live answered %d: %s", status, body)
	}

	status, body := get(t, base+"/health/ready")
	if status != http.StatusOK {
		t.Fatalf("GET /health/ready answered %d: %s", status, body)
	}
	var ready struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(body, &ready); err != nil {
		t.Fatalf("read the readiness body %q: %v", body, err)
	}
	if ready.Status != "ready" {
		t.Errorf("readiness reported %q: %s", ready.Status, body)
	}
	// Both dependencies, by the names the composition root gave them. A
	// readiness set with one check missing reports ready while half the
	// process cannot work.
	for _, name := range []string{"postgres", "sqs"} {
		if got := ready.Checks[name]; got != "ok" {
			t.Errorf("readiness reports %s as %q, want ok: %s", name, got, body)
		}
	}

	app.RequireStop()

	if failed := recorded.failures(); len(failed) != 0 {
		t.Fatalf("hooks reported failures: %v", failed)
	}
	if err := pool.Ping(context.Background()); err == nil {
		t.Fatal("the API stopped and its pool is still open")
	}
	// The listener is gone. A server that stopped serving but kept its socket
	// would go on accepting connections nothing answers, which reads as a hung
	// service rather than a stopped one.
	if err := reach(t, base+"/health/live"); err == nil {
		t.Fatal("the API stopped and is still accepting connections")
	}

	// Drained first, before the pool the requests in flight are using closed.
	drained := recorded.ranStop(t, "serve")
	if closed := recorded.ranStop(t, "newPool"); drained > closed {
		t.Errorf("the pool closed before the server drained; the shutdown was %v",
			recorded.shutdown())
	}
}

// TestAQueueIsResolvedOnlyForALoopThatUsesOne proves the conditional wiring is
// the graph rather than a flag read at run time.
//
// A worker running only the reference loop talks to no queue, so it must not
// resolve one: a start-up check against a queue this process would never touch
// turns an unrelated outage into an outage of its own. The two halves are one
// test on purpose — the same unreachable endpoint that this configuration
// starts against is the one the consumer refuses to start against, so neither
// half can pass by accident.
func TestAQueueIsResolvedOnlyForALoopThatUsesOne(t *testing.T) {
	requireDatabase(t)

	// Nothing listens on port 1. Reaching it is a refused connection, and the
	// resolve timeout bounds how long that takes to establish.
	const nowhere = "http://127.0.0.1:1"

	t.Run("the reference worker alone starts", func(t *testing.T) {
		cfg := loaded(t, map[string]string{
			"AWS_ENDPOINT_URL":         nowhere,
			"CONSUMER_ENABLED":         "false",
			"PUBLISHER_ENABLED":        "false",
			"REFERENCE_WORKER_ENABLED": "true",
		})
		recorded := &hooks{}
		app := fxtest.New(t, Worker(cfg), fx.WithLogger(recorded.logger))
		app.RequireStart()
		app.RequireStop()

		started := recorded.startUp()
		for _, queue := range []string{"newInboundQueue", "newOutboundQueue"} {
			if slices.Contains(started, queue) {
				t.Errorf("a worker with no queue-driven loop resolved %s; its start-up "+
					"was %v", queue, started)
			}
		}
	})

	t.Run("the consumer does not", func(t *testing.T) {
		cfg := loaded(t, map[string]string{
			"AWS_ENDPOINT_URL":         nowhere,
			"CONSUMER_ENABLED":         "true",
			"PUBLISHER_ENABLED":        "false",
			"REFERENCE_WORKER_ENABLED": "false",
		})
		app := fx.New(Worker(cfg), fx.NopLogger)
		if err := app.Err(); err != nil {
			t.Fatalf("the graph would not build: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), app.StartTimeout())
		defer cancel()
		if err := app.Start(ctx); err == nil {
			_ = app.Stop(context.Background())
			t.Fatal("a consumer started against a queue it cannot reach")
		}
	})
}

// TestAQueueNobodyProvisionedStopsStartUp is the start-up check doing its job.
//
// The queues exist and one name is wrong, so this is not "SQS is down" but the
// case that actually happens: a deployment pointed at a queue that was never
// created. Without the check the consumer would receive nothing, report
// nothing, and look healthy.
func TestAQueueNobodyProvisionedStopsStartUp(t *testing.T) {
	requireDatabase(t)
	requireQueues(t)

	cfg := loaded(t, map[string]string{
		"SQS_INBOUND_QUEUE":        "nobody-provisioned-this.fifo",
		"PUBLISHER_ENABLED":        "false",
		"REFERENCE_WORKER_ENABLED": "false",
	})
	app := fx.New(Worker(cfg), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatalf("the graph would not build: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), app.StartTimeout())
	defer cancel()

	err := app.Start(ctx)
	if err == nil {
		_ = app.Stop(context.Background())
		t.Fatal("the worker started against a queue that does not exist")
	}
	if !strings.Contains(err.Error(), "nobody-provisioned-this.fifo") {
		t.Errorf("the refusal did not name the queue: %v", err)
	}
}

// TestARealmThatDoesNotExistStopsStartUp is the other start-up check doing its
// job.
//
// Keycloak is running and the realm in the issuer is not one it has, which is
// the misconfiguration that produces 401s no operator can explain. The fetch
// bounds itself, so this fails within OIDC_FETCH_TIMEOUT rather than within the
// lifecycle's budget.
func TestARealmThatDoesNotExistStopsStartUp(t *testing.T) {
	requireDatabase(t)
	requireQueues(t)
	requireIdentityProvider(t)

	cfg := loaded(t, map[string]string{
		"OIDC_ISSUER": identityBase + "/realms/no-such-realm",
	})
	app := fx.New(API(cfg), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatalf("the graph would not build: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), app.StartTimeout())
	defer cancel()

	began := time.Now()
	if err := app.Start(ctx); err == nil {
		_ = app.Stop(context.Background())
		t.Fatal("the API started against a realm that does not exist")
	}
	// Bounded by the fetch timeout and not only by the lifecycle's. Generous
	// against a loaded machine, and far below the thirty seconds START_TIMEOUT
	// allows, which is the distinction being asserted.
	if took := time.Since(began); took > 15*time.Second {
		t.Errorf("the key-set fetch took %s, so it is bounded by the lifecycle "+
			"rather than by OIDC_FETCH_TIMEOUT", took)
	}
}

// TestADatabaseNobodyCanReachStopsTheGraph covers the dependency that fails
// before the lifecycle rather than during it.
//
// The pool verifies a connection when it is opened, so a DSN naming a database
// nobody can reach is refused while the graph is being built. It is the same
// outcome for the process — it does not start — and the message says so.
func TestADatabaseNobodyCanReachStopsTheGraph(t *testing.T) {
	requireQueues(t)

	cfg := loaded(t, map[string]string{
		"DATABASE_URL":             "postgres://nobody:nothing@127.0.0.1:1/absent?sslmode=disable",
		"DATABASE_CONNECT_TIMEOUT": "2s",
	})
	app := fx.New(Worker(cfg), fx.NopLogger)
	err := app.Err()
	if err == nil {
		t.Fatal("the worker built a graph on a database nobody can reach")
	}
	if strings.Contains(err.Error(), "nothing") {
		t.Errorf("the refusal carried the password: %v", err)
	}
}

// get makes one request and reads the whole answer.
func get(t *testing.T, url string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build a request for %s: %v", url, err)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read the answer from %s: %v", url, err)
	}
	return response.StatusCode, body
}

// reach reports whether anything is still answering at url.
func reach(t *testing.T, url string) error {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build a request for %s: %v", url, err)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = response.Body.Close()
	return nil
}

// TestRunCarriesTheWorkerFromTheEnvironmentToACleanExit is the only test that
// drives a binary the way its main does.
//
// Everything else here builds a graph from a config.Config; this reads the
// environment, builds the graph, starts it against the containers, waits for
// the signal a deployment sends, drains and reports an exit code. What it pins
// is that those six steps join up — the exit code a supervisor sees for a clean
// shutdown is zero, and it is zero because every drain finished rather than
// because nothing checked.
func TestRunCarriesTheWorkerFromTheEnvironmentToACleanExit(t *testing.T) {
	requireDatabase(t)
	requireQueues(t)

	// The API graph rather than the worker's, because it is the one with a door
	// to knock on: this test has to cancel AFTER the application is up, and
	// polling a socket is the only signal a binary gives from outside itself.
	env := environment(t, map[string]string{"HTTP_ADDR": freePort(t)})

	signalled, signal := context.WithCancel(context.Background())
	defer signal()

	exit := make(chan int, 1)
	reported := &safeWriter{}
	go func() { exit <- Run(signalled, reported, "api", config.Static(env), API) }()

	base := "http://" + env["HTTP_ADDR"]
	awaitServing(t, base+"/health/live")

	// What a deployment does.
	signal()

	select {
	case code := <-exit:
		if code != ExitOK {
			t.Fatalf("a clean shutdown exited %d, want %d: %s", code, ExitOK, reported)
		}
	case <-time.After(time.Minute):
		t.Fatal("the process did not exit within a minute of the signal")
	}
	if err := reach(t, base+"/health/live"); err == nil {
		t.Error("the process exited and is still accepting connections")
	}
}

// freePort is an address nothing is listening on yet.
//
// A fixed one rather than :0, because this test has to reach the server and the
// binary reports its address to nobody. The window between closing this
// listener and the server binding is a race with anything else on the machine
// asking for a port, and a test that occasionally loses it is better than a
// test that reads the address out of a log line.
func freePort(t *testing.T) string {
	t.Helper()

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}
	return address
}

// awaitServing waits for the address to answer, or fails.
func awaitServing(t *testing.T, url string) {
	t.Helper()

	deadline := time.Now().Add(time.Minute)
	for {
		if err := reach(t, url); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited a minute for %s to answer", url)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// safeWriter collects what the process reported, across the goroutine it ran
// in and the test that reads it.
type safeWriter struct {
	mu      sync.Mutex
	written strings.Builder
}

func (w *safeWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written.Write(p)
}

func (w *safeWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written.String()
}
