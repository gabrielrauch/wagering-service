package app_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The rendered form of an error is a contract too.
//
// These tests exist because the package had none, and the absence was not
// harmless: a classified error printed its code and its field twice, and the
// inbox defect printed its class twice, for as long as nothing looked.

// headAndBody splits a classified error's rendered form at the first colon
// following the head, which is where Error stops describing the classification
// and starts quoting the cause.
func headAndBody(t *testing.T, err error) (head, body string) {
	t.Helper()
	rendered := err.Error()
	head, body, found := strings.Cut(rendered, ": ")
	if !found {
		t.Fatalf("error %q has no message after its head", rendered)
	}
	return head, body
}

func TestAClassifiedErrorStatesItsCodeOnceInTheHeadAndNotAgainInTheBody(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	// "25.0" is a well-formed decimal carrying the wrong number of fraction
	// digits, which the domain refuses with a code and a message of its own.
	_, err := f.trySubmit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.0",
	}))

	assertClass(t, err, app.Invalid)
	assertCode(t, err, failure.InvalidAmountScale)

	code := string(failure.InvalidAmountScale)
	rendered := err.Error()
	if got := strings.Count(rendered, code); got != 1 {
		t.Errorf("the code appears %d times in %q, want exactly once: the head states it, "+
			"so the body must not restate it", got, rendered)
	}
	head, body := headAndBody(t, err)
	if head != string(app.Invalid)+" "+code {
		t.Errorf("head %q, want %q", head, string(app.Invalid)+" "+code)
	}
	if strings.Contains(body, code) {
		t.Errorf("body %q restates the code the head already carries", body)
	}
}

func TestAClassifiedErrorStatesItsFieldOnceInTheHeadAndNotAgainInTheBody(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")

	_, err := f.wallets.Ledger(t.Context(), servicePrincipal(t), app.LedgerQuery{
		WalletID: wallet,
		Cursor:   "!!!not-base64!!!",
	})

	assertClass(t, err, app.Invalid)
	assertField(t, err, "cursor")
	head, _ := headAndBody(t, err)
	if !strings.HasSuffix(head, "[cursor]") {
		t.Errorf("head %q does not name the field", head)
	}
}

// The inbox defect was reached through AsUnretryable(defect(...)), which
// classified an already-classified error and printed the class twice.
func TestTheInboxBodyMismatchStatesItsClassOnce(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	of := fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"})

	if _, err := f.fromQueue(t, of, "msg-1", "body-hash-1"); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	_, err := f.fromQueue(t, of, "msg-1", "body-hash-DIFFERENT")

	assertClass(t, err, app.Unretryable)
	rendered := err.Error()
	if got := strings.Count(rendered, string(app.Unretryable)); got != 1 {
		t.Errorf("class appears %d times in %q, want exactly once", got, rendered)
	}
}

// AsRetryable and AsUnretryable are what an adapter reaches for. Neither may
// overwrite a classification this layer already made: Class would tell the
// caller to send it again while CodeOf still told them to repair the payload.
func TestClassifyingAnAlreadyClassifiedErrorLeavesItAlone(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	_, invalid := f.trySubmit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.0",
	}))
	assertClass(t, invalid, app.Invalid)

	for _, tc := range []struct {
		name string
		mark func(error) error
	}{
		{"AsRetryable", app.AsRetryable},
		{"AsUnretryable", app.AsUnretryable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marked := tc.mark(invalid)
			if got := app.ClassOf(marked); got != app.Invalid {
				t.Errorf("class %s, want %s: %s overwrote a classification this layer made",
					got, app.Invalid, tc.name)
			}
			assertCode(t, marked, failure.InvalidAmountScale)
		})
	}
}

func TestClassifyingAnUnclassifiedErrorStillClassifiesIt(t *testing.T) {
	retryable := app.AsRetryable(errors.New("connection reset by peer"))
	if got := app.ClassOf(retryable); got != app.Retryable {
		t.Errorf("class %s, want %s", got, app.Retryable)
	}
	unretryable := app.AsUnretryable(errors.New("permission denied"))
	if got := app.ClassOf(unretryable); got != app.Unretryable {
		t.Errorf("class %s, want %s", got, app.Unretryable)
	}
}

// An infrastructure failure that happens to wrap a classified error is still an
// infrastructure failure, and the adapter holding it is entitled to say so.
func TestAnAdapterMayClassifyAFailureThatMerelyWrapsAClassifiedOne(t *testing.T) {
	inner := app.AsUnretryable(errors.New("statement timeout"))
	wrapped := fmt.Errorf("acquiring a connection: %w", inner)

	marked := app.AsRetryable(wrapped)

	if got := app.ClassOf(marked); got != app.Retryable {
		t.Errorf("class %s, want %s: the adapter's own assertion was discarded", got, app.Retryable)
	}
}

// app.Error is exported with exported fields, so an adapter can hand this
// package a typed nil boxed in a non-nil error. Classifying one must not crash
// the path whose whole job is to report failures calmly.
func TestATypedNilClassifiedErrorDoesNotPanic(t *testing.T) {
	var typed *app.Error
	var boxed error = typed

	if got := app.ClassOf(boxed); got != app.Unretryable {
		t.Errorf("class %s, want %s", got, app.Unretryable)
	}
	if _, ok := app.CodeOf(boxed); ok {
		t.Error("a nil classified error carries no code")
	}
	if boxed.Error() == "" {
		t.Error("a nil classified error must still render")
	}
	if errors.Unwrap(boxed) != nil {
		t.Error("a nil classified error unwraps to nothing")
	}
}

// Scoping a read must not become an existence oracle, so a provider asking
// about another provider's operation is told exactly what it is told for one
// that does not exist. The operator still needs to tell the two apart.
func TestAForeignOperationIsIndistinguishableToTheProviderAndVisibleToTheOperator(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	owned := f.submit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
	}))

	_, foreign := f.wagers.TransactionByID(t.Context(), providerPrincipal(t, rival), owned.TransactionID)
	_, missing := f.wagers.TransactionByID(t.Context(), providerPrincipal(t, rival), f.ids.TransactionID())

	assertClass(t, foreign, app.NotFound)
	assertClass(t, missing, app.NotFound)
	if !errors.Is(foreign, app.ErrForeignOperation) {
		t.Error("the operator cannot tell a cross-provider read from an ordinary miss")
	}
	if errors.Is(missing, app.ErrForeignOperation) {
		t.Error("an ordinary miss was reported as a cross-provider read")
	}
	// What the provider is told must be byte-identical to an ordinary miss,
	// including the identifier. Comparing it against the same read issued twice
	// compared a deterministic call with itself and could not fail, so it is
	// compared against the rendering an ordinary miss for that id produces.
	wantSame := fmt.Sprintf("NOT_FOUND: no operation %q", owned.TransactionID)
	if foreign.Error() != wantSame {
		t.Errorf("a scoped miss renders as %q, want %q", foreign.Error(), wantSame)
	}
	if _, ok := app.CodeOf(foreign); ok {
		t.Error("a scoped miss carries no code")
	}
}

// Every dependency is refused at construction, because a service built without
// one fails at the first command instead, by which time a provider is waiting.
func TestNewWageringRefusesEveryMissingDependency(t *testing.T) {
	backoff := app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: time.Hour}
	complete := func(t *testing.T) app.WageringDeps {
		t.Helper()
		return app.WageringDeps{
			Tx:        newFakeDB(),
			Processor: newProcessor(t, defaultMaxAttempts, defaultReferenceTTL),
			Clock:     newFakeClock(),
			IDs:       &fakeIDs{},
			Backoff:   backoff,
			Defects:   &fakeObserver{},
		}
	}
	for _, tc := range []struct {
		missing string
		remove  func(*app.WageringDeps)
	}{
		{"Tx", func(d *app.WageringDeps) { d.Tx = nil }},
		{"Processor", func(d *app.WageringDeps) { d.Processor = nil }},
		{"Clock", func(d *app.WageringDeps) { d.Clock = nil }},
		{"IDs", func(d *app.WageringDeps) { d.IDs = nil }},
		{"Defects", func(d *app.WageringDeps) { d.Defects = nil }},
	} {
		t.Run(tc.missing, func(t *testing.T) {
			deps := complete(t)
			tc.remove(&deps)
			service, err := app.NewWagering(deps)
			if err == nil {
				t.Fatalf("built a service with no %s", tc.missing)
			}
			if service != nil {
				t.Error("a refused construction returns no service")
			}
			assertClass(t, err, app.Unretryable)
		})
	}
	if _, err := app.NewWagering(complete(t)); err != nil {
		t.Fatalf("a complete set of dependencies was refused: %v", err)
	}
}

// A principal that cannot be built is Unauthorized and names no field: both
// values come from the token, so pointing the caller at a request field would
// name one that does not exist.
func TestAPrincipalThatCannotBeBuiltIsUnauthorizedAndNamesNoField(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func() (app.Principal, error)
	}{
		{"provider with no provider", func() (app.Principal, error) {
			return app.NewProviderPrincipal("", "subject-1")
		}},
		{"provider with no subject", func() (app.Principal, error) {
			return app.NewProviderPrincipal(acme, "  ")
		}},
		{"service with no subject", func() (app.Principal, error) {
			return app.NewServicePrincipal("")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			principal, err := tc.build()
			assertClass(t, err, app.Unauthorized)
			if _, ok := app.CodeOf(err); ok {
				t.Error("a principal that could not be built names no catalogued code")
			}
			if classified, ok := errors.AsType[*app.Error](err); ok && classified.Field != "" {
				t.Errorf("names field %q, which is a token claim and not a request field",
					classified.Field)
			}
			if principal.Kind() != "" {
				t.Error("a refused principal authorises nothing")
			}
		})
	}
}

// A submission with no correlation id names the field it is about.
func TestASubmissionWithoutACorrelationIdNamesTheField(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	_, err := f.wagers.Submit(t.Context(), app.SubmitOperation{
		Principal: providerPrincipal(t, acme),
		Fields: fields(acme, submission{
			Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
		}),
	})

	assertClass(t, err, app.Invalid)
	assertCode(t, err, failure.MissingRequiredField)
	assertField(t, err, "correlationId")
}

// commitProbe records how many transactions the store had committed at the
// moment it was told about a defect, which is what makes "the observer is
// called after the transaction closes" an assertion rather than a comment.
type commitProbe struct {
	db            *fakeDB
	commitsAtCall []int
	ids           []wagering.TransactionID
	causes        []error
}

func (p *commitProbe) CannotCarryForward(_ context.Context, id wagering.TransactionID, err error) {
	p.commitsAtCall = append(p.commitsAtCall, p.db.Commits())
	p.ids = append(p.ids, id)
	p.causes = append(p.causes, err)
}

// The defect hook runs an adapter's code. Running it inside the movement
// transaction would hold the wallet lock for as long as the adapter took, and
// would announce a write that had not committed.
func TestTheCarryForwardDefectIsReportedAfterTheTransactionCommits(t *testing.T) {
	f := newFixture(t)
	probe := &commitProbe{db: f.db}
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	parked := f.park(t, "ext-refund", "key-1")
	f.db.corruptPayloadHash(t, parked.TransactionID)
	f.clock.advance(5 * time.Minute)

	wagers, err := app.NewWagering(app.WageringDeps{
		Tx: f.db, Processor: newProcessor(t, defaultMaxAttempts, defaultReferenceTTL), Clock: f.clock, IDs: f.ids,
		Backoff: app.BackoffPolicy{Initial: time.Minute, Factor: 2, Max: time.Hour},
		Defects: probe,
	})
	if err != nil {
		t.Fatalf("new wagering: %v", err)
	}

	if _, err := wagers.Resume(t.Context(), servicePrincipal(t)); err != nil {
		t.Fatalf("resume: %v", err)
	}

	if len(probe.commitsAtCall) != 1 {
		t.Fatalf("told about %d defects, want exactly 1", len(probe.commitsAtCall))
	}
	if got, after := probe.commitsAtCall[0], f.db.Commits(); got != after {
		t.Errorf("the observer saw %d commits and the turn ended at %d: it was called inside "+
			"the transaction, holding the wallet lock", got, after)
	}
	if probe.ids[0] != parked.TransactionID {
		t.Errorf("reported %s, want %s", probe.ids[0], parked.TransactionID)
	}
	if probe.causes[0] == nil {
		t.Error("reported a defect with no cause")
	}
}

// The report fires for a defect and for nothing else. A hook called on every
// turn would be a hook nobody could act on.
func TestAResumeTurnWithNoDefectTellsTheObserverNothing(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	// Nothing is parked, so the turn claims nothing and fails at nothing.
	out, err := f.wagers.Resume(t.Context(), servicePrincipal(t))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out.Claimed {
		t.Error("claimed something with nothing due")
	}
	if len(f.observer.defects) != 0 {
		t.Errorf("reported %d defects on a turn that found none", len(f.observer.defects))
	}
}

// The unique index on active_reversal is the backstop behind a rule the domain
// enforces under the wallet lock. Reaching it means the reference view was built
// wrongly, so the rule now lives only in the schema — and the answer is still a
// conflict the provider can read, reported by being returned and not also
// through a hook whose subject is parked work.
func TestAReferenceHeldBehindTheDomainsBackIsAConflictAndNotADefectReport(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{
		Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00",
	}))
	f.submit(t, fields(acme, submission{
		Kind: "REFUND", External: "ext-refund-1", Key: "key-2", Amount: "25.00", Reference: "ext-bet",
	}))

	// From here the reference query no longer sees the hold it already granted,
	// so the domain is asked to judge a reference it believes is free.
	f.db.blindToActiveReversals = true

	_, err := f.trySubmit(t, fields(acme, submission{
		Kind: "REFUND", External: "ext-refund-2", Key: "key-3", Amount: "25.00", Reference: "ext-bet",
	}))

	assertClass(t, err, app.Conflict)
	assertCode(t, err, failure.ReferenceAlreadyReversed)
	if len(f.observer.defects) != 0 {
		t.Errorf("reported %d defects: a returned error is handled by the caller, and "+
			"CannotCarryForward names parked work this submission never was",
			len(f.observer.defects))
	}
}

// Reconciliation finds more than a balance that does not add up, and what it
// finds must keep its own name: an operator paged for LEDGER_BALANCE_MISMATCH
// goes looking for missing money, which is the wrong search for a duplicated
// entry.
func TestAStructuralReconciliationFindingKeepsItsOwnCodeAndField(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
	}))
	f.db.duplicateLedgerEntry(t, wallet)

	_, err := f.wallets.Reconcile(t.Context(), servicePrincipal(t), wallet)

	assertClass(t, err, app.Audit)
	assertCode(t, err, failure.InvalidFieldFormat)
	assertField(t, err, "transactionId")
	if code, _ := app.CodeOf(err); code == failure.LedgerBalanceMismatch {
		t.Error("a duplicated ledger entry was reported as a balance mismatch")
	}
}
