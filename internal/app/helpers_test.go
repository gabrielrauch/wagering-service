package app_test

import (
	"cmp"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

const (
	acme  = wagering.Provider("acme-games")
	rival = wagering.Provider("rival-games")
)

// fixture is everything one test needs, wired together.
type fixture struct {
	db       *fakeDB
	clock    *fakeClock
	ids      *fakeIDs
	observer *fakeObserver
	wagers   *app.Wagering
	wallets  *app.Wallets
}

// The reference policy every plain fixture runs under, named once so that a
// test building a service of its own cannot drift from the fixture's.
const (
	defaultMaxAttempts  = 3
	defaultReferenceTTL = time.Hour
)

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixtureWith(t, defaultMaxAttempts, defaultReferenceTTL,
		app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: time.Hour})
}

// newFixtureWith is for the tests that are about the wait budget itself, where
// the numbers are the thing under test rather than a backdrop.
func newFixtureWith(t *testing.T, maxAttempts int, ttl time.Duration, backoff app.BackoffPolicy) *fixture {
	t.Helper()
	db := newFakeDB()
	clock := newFakeClock()
	ids := &fakeIDs{}
	observer := &fakeObserver{}

	processor := newProcessor(t, maxAttempts, ttl)
	wagers, err := app.NewWagering(app.WageringDeps{
		Tx:        db,
		Processor: processor,
		Clock:     clock,
		IDs:       ids,
		Backoff:   backoff,
		Defects:   observer,
	})
	if err != nil {
		t.Fatalf("new wagering: %v", err)
	}
	wallets, err := app.NewWallets(db, clock, ids, observer)
	if err != nil {
		t.Fatalf("new wallets: %v", err)
	}
	return &fixture{db: db, clock: clock, ids: ids, observer: observer, wagers: wagers, wallets: wallets}
}

// newProcessor builds the domain processor a fixture runs on, and is shared
// with the tests that wire a service of their own so both get one policy.
func newProcessor(t *testing.T, maxAttempts int, ttl time.Duration) *wagering.Processor {
	t.Helper()
	policy, err := wagering.NewReferencePolicy(maxAttempts, ttl)
	if err != nil {
		t.Fatalf("reference policy: %v", err)
	}
	processor, err := wagering.NewProcessor(policy)
	if err != nil {
		t.Fatalf("processor: %v", err)
	}
	return processor
}

// What a test does with a fixture.

// assertUntouched is what "no financial effect and no data" means through a
// seam: nothing written, and no transaction even opened.
func (f *fixture) assertUntouched(t *testing.T) {
	t.Helper()
	if got := f.db.Begins(); got != 0 {
		t.Errorf("opened %d transactions, want none before authorization", got)
	}
	if !f.db.empty() {
		t.Errorf("store is not empty: %d wallets, %d transactions, %d entries, %d events",
			len(f.db.committed.wallets), len(f.db.committed.txns),
			len(f.db.committed.entries), len(f.db.committed.outbox))
	}
}

// submit runs one submission as a provider, and fails the test if it errors.
func (f *fixture) submit(t *testing.T, of app.OperationFields) app.OperationResult {
	t.Helper()
	result, err := f.trySubmit(t, of)
	if err != nil {
		t.Fatalf("submit %s %s: %v", of.Kind, of.ExternalTransactionID, err)
	}
	return result
}

func (f *fixture) trySubmit(t *testing.T, of app.OperationFields) (app.OperationResult, error) {
	t.Helper()
	provider, err := wagering.NewProvider(of.Provider)
	if err != nil {
		t.Fatalf("provider %q: %v", of.Provider, err)
	}
	return f.wagers.Submit(t.Context(), app.SubmitOperation{
		Principal:   providerPrincipal(t, provider),
		Correlation: "corr-" + of.ExternalTransactionID,
		Fields:      of,
	})
}

func providerPrincipal(t *testing.T, p wagering.Provider) app.Principal {
	t.Helper()
	principal, err := app.NewProviderPrincipal(p, "subject-"+p.String())
	if err != nil {
		t.Fatalf("provider principal: %v", err)
	}
	return principal
}

func servicePrincipal(t *testing.T) app.Principal {
	t.Helper()
	principal, err := app.NewServicePrincipal("wagering-service")
	if err != nil {
		t.Fatalf("service principal: %v", err)
	}
	return principal
}

func assertClass(t *testing.T, err error, want app.Class) {
	t.Helper()
	if err == nil {
		t.Fatalf("want a %s error, got success", want)
	}
	if got := app.ClassOf(err); got != want {
		t.Fatalf("classified %s, want %s (%v)", got, want, err)
	}
}

// submission is what a test submission says, beyond who is submitting it.
//
// A struct rather than four bare string parameters: kind, external, key and
// amount are all strings, so any two of them could be swapped at a call site
// and still compile.
type submission struct {
	Kind     string
	External string
	Key      string
	Amount   string

	// Reference names the operation this one acts on, and is empty when it
	// names none. Round overrides the default round.
	//
	// Both are fields rather than the wrappers they replaced: a submission that
	// had to be wrapped to say what it references read inside-out, and stacking
	// two wrappers put three call layers between a test and what it submitted.
	Reference string
	Round     string
}

// fields builds a submission. The defaults put every operation on one player,
// round and game, so a test only names what it is actually about.
func fields(p wagering.Provider, s submission) app.OperationFields {
	return app.OperationFields{
		Provider:              p.String(),
		ExternalTransactionID: s.External,
		IdempotencyKey:        s.Key,
		PlayerID:              "player-1",
		RoundID:               cmp.Or(s.Round, "round-1"),
		GameID:                "game-1",
		Kind:                  s.Kind,
		Amount:                s.Amount,
		Currency:              "BRL",

		ReferenceExternalTransactionID: s.Reference,
	}
}

func assertCode(t *testing.T, err error, want failure.Code) {
	t.Helper()
	got, ok := app.CodeOf(err)
	if !ok {
		t.Fatalf("error carries no failure code: %v", err)
	}
	if got != want {
		t.Fatalf("code %s, want %s (%v)", got, want, err)
	}
}

func assertStatus(t *testing.T, result app.OperationResult, status wagering.Status, code failure.Code) {
	t.Helper()
	if result.Status != status {
		t.Errorf("status %s, want %s", result.Status, status)
	}
	if result.FailureCode != code {
		t.Errorf("failure code %q, want %q", result.FailureCode, code)
	}
}

// row reads one committed transaction, failing the test when there is none.
// fakeDB.transaction is the only reader of the committed rows, so its
// not-found answer is turned into a fatal in one place rather than seven.
func (f *fixture) row(t *testing.T, id wagering.TransactionID) txnRow {
	t.Helper()
	r, ok := f.db.transaction(id)
	if !ok {
		t.Fatalf("no transaction %s", id)
	}
	return r
}

// assertField names the input an Invalid error blamed.
func assertField(t *testing.T, err error, want string) {
	t.Helper()
	e, ok := errors.AsType[*app.Error](err)
	if !ok {
		t.Fatalf("not an app.Error: %v", err)
	}
	if e.Field != want {
		t.Errorf("field %q, want %q", e.Field, want)
	}
}

// assertEvents names the event types published, in order. The order is part of
// the contract — a processed operation is announced before the balance change
// it caused — so it is compared as a sequence, never as a set.
func assertEvents(t *testing.T, db *fakeDB, want ...string) {
	t.Helper()
	if got := db.eventTypes(); !slices.Equal(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
}
