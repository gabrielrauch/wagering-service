package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The fakes below record what reached them and answer what the test told them
// to. They count calls as well, because several of the properties under test
// here are about a call that must NOT happen: an unauthenticated request has to
// be refused with nothing touched, and "nothing touched" is only assertable if
// the thing that was not touched is counting.
//
// They are safe for concurrent use. One test drives a real server with a
// request in flight while the server is being shut down, and the race detector
// is part of the gate.

// fakeWagering stands in for the application's write path.
type fakeWagering struct {
	mu sync.Mutex

	calls          int
	lastSubmit     app.SubmitOperation
	lastPrincipal  app.Principal
	lastByID       wagering.TransactionID
	lastProvider   wagering.Provider
	lastByExternal wagering.ExternalTransactionID
	// lastContext is kept so a test can assert the context a handler passed
	// down was the request's own and not one of its making.
	lastContext context.Context

	result app.OperationResult
	err    error
}

func (f *fakeWagering) Submit(
	ctx context.Context, cmd app.SubmitOperation,
) (app.OperationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastSubmit = cmd
	f.lastPrincipal = cmd.Principal
	f.lastContext = ctx
	return f.result, f.err
}

func (f *fakeWagering) TransactionByID(
	ctx context.Context, principal app.Principal, id wagering.TransactionID,
) (app.OperationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastPrincipal = principal
	f.lastByID = id
	f.lastContext = ctx
	return f.result, f.err
}

func (f *fakeWagering) TransactionByExternalID(
	ctx context.Context, principal app.Principal,
	provider wagering.Provider, id wagering.ExternalTransactionID,
) (app.OperationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastPrincipal = principal
	f.lastProvider = provider
	f.lastByExternal = id
	f.lastContext = ctx
	return f.result, f.err
}

func (f *fakeWagering) called() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeWagering) submitted() app.SubmitOperation {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSubmit
}

// fakeWallets stands in for the application's wallet path.
type fakeWallets struct {
	mu sync.Mutex

	calls         int
	lastOpen      app.OpenWalletCommand
	lastPrincipal app.Principal
	lastWalletID  wagering.WalletID
	lastQuery     app.LedgerQuery

	view    app.WalletView
	opening *app.OperationResult
	page    app.LedgerPage
	report  app.Reconciliation
	err     error
}

func (f *fakeWallets) Open(
	_ context.Context, cmd app.OpenWalletCommand,
) (app.WalletView, *app.OperationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastOpen = cmd
	f.lastPrincipal = cmd.Principal
	return f.view, f.opening, f.err
}

func (f *fakeWallets) ByID(
	_ context.Context, principal app.Principal, id wagering.WalletID,
) (app.WalletView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastPrincipal = principal
	f.lastWalletID = id
	return f.view, f.err
}

func (f *fakeWallets) Ledger(
	_ context.Context, principal app.Principal, q app.LedgerQuery,
) (app.LedgerPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastPrincipal = principal
	f.lastQuery = q
	return f.page, f.err
}

func (f *fakeWallets) Reconcile(
	_ context.Context, principal app.Principal, id wagering.WalletID,
) (app.Reconciliation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastPrincipal = principal
	f.lastWalletID = id
	return f.report, f.err
}

func (f *fakeWallets) called() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeWallets) opened() app.OpenWalletCommand {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastOpen
}

// fakeAuthenticator stands in for the OIDC adapter. It needs no issuer, no
// JWKS and no signing key, which is the whole reason the API depends on an
// interface here.
type fakeAuthenticator struct {
	mu sync.Mutex

	calls          int
	lastCredential string

	principal app.Principal
	err       error
}

func (f *fakeAuthenticator) called() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeAuthenticator) Authenticate(
	_ context.Context, bearer string,
) (app.Principal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastCredential = bearer
	return f.principal, f.err
}

// fakeCheck stands in for a readiness probe.
//
// Counted under a lock like every other fake here, because readiness runs its
// checks at the same time: without it the count is a data race the moment there
// is more than one check to run.
type fakeCheck struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (f *fakeCheck) Ready(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.err
}

func (f *fakeCheck) called() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// delayedCheck answers after a delay, or gives up when the budget runs out. It
// is how a dependency that is slow rather than absent behaves.
type delayedCheck struct {
	delay time.Duration
	mu    sync.Mutex
	calls int
}

func (c *delayedCheck) Ready(ctx context.Context) error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.delay):
		return nil
	}
}

func (c *delayedCheck) called() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// classifiedError is an error carrying its class, and sometimes a sentinel, the
// way internal/app builds one: both ride in the chain, where errors.Is finds
// them, and neither appears in the message the error renders.
//
// It exists so a test can produce two errors that are indistinguishable to a
// caller and distinguishable to an operator — which is the property
// app.ErrForeignOperation is for, and the one a handler is most likely to break
// by rendering a cause chain.
type classifiedError struct {
	message string
	causes  []error
}

func (e *classifiedError) Error() string   { return e.message }
func (e *classifiedError) Unwrap() []error { return e.causes }

// classified builds an error of the given class carrying message, plus whatever
// sentinels ride alongside.
func classified(class app.Class, code failure.Code, message string, alongside ...error) error {
	causes := append([]error{&app.Error{Class: class, Code: code}}, alongside...)
	return &classifiedError{message: message, causes: causes}
}

// providerPrincipal builds the identity of a provider acting as itself.
func providerPrincipal(t *testing.T, provider wagering.Provider) app.Principal {
	t.Helper()
	principal, err := app.NewProviderPrincipal(provider, "service-account-"+provider.String())
	if err != nil {
		t.Fatalf("building a provider principal: %v", err)
	}
	return principal
}

// servicePrincipal builds the identity of this system acting for itself.
func servicePrincipal(t *testing.T) app.Principal {
	t.Helper()
	principal, err := app.NewServicePrincipal("service-account-internal")
	if err != nil {
		t.Fatalf("building a service principal: %v", err)
	}
	return principal
}

// forbidden is the refusal a provider gets from the application layer for
// asking after a wallet: a real app.Error, with a message and no code, which is
// the shape the 403 path has to render.
func forbidden(t *testing.T) error {
	t.Helper()
	err := providerPrincipal(t, "acme").MayAdministerWallets()
	if err == nil {
		t.Fatal("a provider principal was allowed to administer wallets")
	}
	return err
}

// discard is a logger that records nothing, for the tests that are not about
// what was logged.
func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// recorder keeps what was logged, so that the distinctions this package refuses
// to put on the wire can be asserted where it does put them.
//
// That is not a nicety. A foreign read and an ordinary miss are one answer to a
// caller by design, and the log line is the only place they are ever told
// apart; a credential refusal says one sentence to the caller whichever of five
// things went wrong, and the log line is the only place those are told apart.
// Both are therefore behaviour that a suite logging into a discard cannot see
// removed.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *recorder) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *recorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recorder) WithGroup(string) slog.Handler      { return h }

// find returns the first record carrying message, or nil.
func (h *recorder) find(message string) *slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.records {
		if h.records[i].Message == message {
			return &h.records[i]
		}
	}
	return nil
}

// attr reads one attribute off a record.
func attr(record *slog.Record, key string) string {
	var found string
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = a.Value.String()
			return false
		}
		return true
	})
	return found
}

// attrsOf reads every attribute off a record, rendered, for asserting that a
// line carries exactly a set of keys and nothing beside them.
func attrsOf(record *slog.Record) map[string]string {
	found := make(map[string]string, record.NumAttrs())
	record.Attrs(func(a slog.Attr) bool {
		found[a.Key] = a.Value.String()
		return true
	})
	return found
}

// rendered is everything that was logged, as one string, for asserting that
// something never appears in any of it.
func (h *recorder) rendered() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out strings.Builder
	for i := range h.records {
		out.WriteString(h.records[i].Message)
		h.records[i].Attrs(func(a slog.Attr) bool {
			out.WriteString(" " + a.Key + "=" + a.Value.String())
			return true
		})
		out.WriteString("\n")
	}
	return out.String()
}

// errBoom is an infrastructure failure nobody classified.
var errBoom = errors.New("dial tcp 10.0.0.5:5432: connect: connection refused")
