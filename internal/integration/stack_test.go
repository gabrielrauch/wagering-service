//go:build integration

// The service, assembled the way a composition root will assemble it, onto one
// test's database.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	httpapi "github.com/gabrielrauch/wagering-service/internal/adapters/http"
	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The bounds this suite runs the service under.
//
// They are the same sort of numbers a deployment would use rather than test
// values: what is being exercised is the real server, and a timeout small
// enough to be a test fixture would make the suite's own contention look like
// the service failing.
const (
	stackLockTimeout      = 5 * time.Second
	stackStatementTimeout = 15 * time.Second
	stackConnectTimeout   = 10 * time.Second
	stackMaxConns         = 8
	stackMaxBody          = 1 << 20
	stackReadiness        = 3 * time.Second
	stackReferenceBudget  = time.Hour
	stackReferenceTries   = 5
)

// stack is one running service: the HTTP server a caller reaches it through,
// and an owner connection this suite audits the database with.
type stack struct {
	// base is the address the server bound, http://host:port.
	base string
	// owner reads the tables directly, as the role that can see all of them.
	// Nothing in this suite writes through it: a fixture that reached past the
	// API would be fixing up state the API is supposed to have produced.
	owner *pgxpool.Pool
}

// newStack wires the whole service onto a freshly migrated database.
//
// Nothing in it stands in for anything. The pool is opened by the adapter's own
// constructor as wagering_app, the transaction manager, the repositories and
// the two use cases are the real ones, the authenticator is the real one
// pointed at the Keycloak container, and the API is carried by the real server
// on a port the kernel chose.
func newStack(t *testing.T) *stack {
	t.Helper()
	requireCluster(t)
	requireIdentityProvider(t)

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
		LockTimeout:      stackLockTimeout,
		StatementTimeout: stackStatementTimeout,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}

	policy, err := wagering.NewReferencePolicy(stackReferenceTries, stackReferenceBudget)
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
		Backoff:   app.BackoffPolicy{Initial: time.Second, Factor: 2, Max: time.Minute},
		Defects:   discardDefects{},
	})
	if err != nil {
		t.Fatalf("wire the wagering service: %v", err)
	}
	wallets, err := app.NewWallets(manager, wallClock{}, mintedIDs{}, nil)
	if err != nil {
		t.Fatalf("wire the wallets service: %v", err)
	}

	health, err := postgres.NewHealth(pool, stackReadiness)
	if err != nil {
		t.Fatalf("wire the readiness check: %v", err)
	}
	verifier, err := authenticator()
	if err != nil {
		t.Fatalf("wire the authenticator: %v", err)
	}

	logs := newRecorder(t)
	api, err := httpapi.New(httpapi.Config{
		Wagering:         wagers,
		Wallets:          wallets,
		Authenticator:    verifier,
		Readiness:        map[string]httpapi.ReadinessCheck{"postgres": health},
		ReadinessTimeout: stackReadiness,
		MaxBodyBytes:     stackMaxBody,
		Logger:           logs.logger,
	})
	if err != nil {
		t.Fatalf("wire the API: %v", err)
	}

	server, err := httpapi.NewServer(httpapi.ServerConfig{
		Addr:              "127.0.0.1:0",
		Handler:           api,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
		ShutdownTimeout:   5 * time.Second,
		MaxHeaderBytes:    1 << 16,
		Logger:            logs.logger,
	})
	if err != nil {
		t.Fatalf("wire the server: %v", err)
	}
	if err := server.Start(t.Context()); err != nil {
		t.Fatalf("start the server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Stop(context.Background()); err != nil {
			t.Errorf("stop the server: %v", err)
		}
	})

	owner, err := newAdminPool(t.Context(), dsn, 2)
	if err != nil {
		t.Fatalf("open an owner pool: %v", err)
	}
	t.Cleanup(owner.Close)

	return &stack{base: "http://" + server.Addr(), owner: owner}
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

// discardDefects is the hook for a parked operation that could not be carried
// forward. Nothing here resumes, so there is nothing for it to see; it discards
// deliberately rather than by omission, which is what app.NewWagering asks of
// an adapter that wants no hook.
type discardDefects struct{}

func (discardDefects) CannotCarryForward(context.Context, wagering.TransactionID, error) {}

// recorder collects what the service logged and prints it only when the test
// that produced it failed.
//
// The distinctions this service deliberately keeps out of a response — which
// credential refusal happened, a provider walking somebody else's identifiers —
// exist to be visible on the inside, and a suite that discarded them would be
// throwing away the first thing to look at when one of these tests goes red.
type recorder struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	logger *slog.Logger
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()
	r := &recorder{}
	r.logger = slog.New(slog.NewTextHandler(r, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		t.Logf("what the service logged:\n%s", r.buffer.String())
	})
	return r
}

// Write implements [io.Writer] for the log handler.
func (r *recorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buffer.Write(p)
}

// call is one request this suite makes of the service.
//
// Every field is a string the caller writes out, because the point of this
// suite is to be a caller: nothing here builds a request through the types the
// handlers decode into.
type call struct {
	method string
	path   string
	body   string
	// token is put in Authorization as a bearer credential. Empty sends no
	// Authorization header at all, which is the missing-credential case.
	token string
	// rawAuthorization replaces the whole header, for the spellings a bearer
	// token does not have.
	rawAuthorization string
	idempotencyKey   string
	correlation      string
}

// answer is what came back, kept whole so that a test can compare bodies and
// headers rather than statuses.
type answer struct {
	status int
	header http.Header
	body   []byte
}

// String renders an answer for a failure message: the status and the body,
// which between them are what a red test needs in its first line.
func (a answer) String() string {
	return fmt.Sprintf("%d %s", a.status, strings.TrimSpace(string(a.body)))
}

// requests is the transport this suite reaches the service with. Redirects are
// not followed, because the router composes one deliberately and a client that
// chased it would hide which response arrived.
var requests = &http.Client{
	Timeout: 30 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// do makes one request and reads the whole answer.
func (s *stack) do(t *testing.T, c call) answer {
	t.Helper()

	var body io.Reader
	if c.body != "" {
		body = strings.NewReader(c.body)
	}
	req, err := http.NewRequestWithContext(t.Context(), c.method, s.base+c.path, body)
	if err != nil {
		t.Fatalf("build a %s %s: %v", c.method, c.path, err)
	}
	if c.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	switch {
	case c.rawAuthorization != "":
		req.Header.Set("Authorization", c.rawAuthorization)
	case c.token != "":
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", c.idempotencyKey)
	}
	if c.correlation != "" {
		req.Header.Set("X-Correlation-Id", c.correlation)
	}

	resp, err := requests.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", c.method, c.path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	read, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		t.Fatalf("read the answer to %s %s: %v", c.method, c.path, err)
	}
	return answer{status: resp.StatusCode, header: resp.Header.Clone(), body: read}
}
