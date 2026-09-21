//go:build integration

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The timeouts every test's manager runs with.
//
// The lock timeout is the margin the concurrency tests live inside. Several of
// them hold a lock, sleep a fixed 250ms to show that a waiter really is
// waiting, and then release — so the timeout has to be comfortably longer than
// that sleep or a loaded machine turns a passing test into a lock timeout. Two
// seconds is an 8x margin, where 750ms was 3x and was the first thing expected
// to flake under -race on a shared runner.
//
// Only one test pays for the size of it: the one that deliberately waits the
// whole timeout out to prove the wait is bounded. That is a second and a
// quarter of extra runtime, once, for a suite that is otherwise the flakiest
// part of the change.
const (
	testLockTimeout      = 2 * time.Second
	testStatementTimeout = 10 * time.Second
	// contentionPause is how long a test holds something another goroutine
	// wants before checking that the other goroutine has not got it. Long
	// enough that an unserialized waiter would certainly have finished, and
	// far enough inside testLockTimeout that a slow machine does not turn the
	// wait into a failure.
	contentionPause = 250 * time.Millisecond
)

// base is the instant the fixtures count from. Truncated to the microsecond
// PostgreSQL keeps, so a value written and read back compares equal to itself.
var base = time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

// at returns a distinct instant, n seconds after base.
//
// Distinct matters more than it looks. wallet_clock_moves_forward refuses a
// balance change that does not move updated_at strictly forward, so two
// movements on one wallet stamped with the same instant are refused by the
// schema — correctly, and confusingly if a fixture did it by accident.
func at(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }

// world is one test's database: the manager under test, the pool it runs on as
// the application role, and an owner pool for the fixtures that have to write
// what the application deliberately cannot.
type world struct {
	tm    *TxManager
	app   *pgxpool.Pool
	owner *pgxpool.Pool
	// dsn names this test's database, so that a scenario needing a second pool
	// onto it can open one rather than share the sixteen connections above.
	dsn string
}

// newWorld gives a test a freshly migrated database and a manager on it.
//
// maxConns is generous because the concurrency tests hold several transactions
// open at once and a pool that ran out would look exactly like the lock
// contention they are trying to observe.
func newWorld(t *testing.T) *world {
	t.Helper()
	dsn := migrated(t)
	pool := newAppPool(t, dsn, 16)
	owner, err := newAdminPool(t.Context(), dsn, 4)
	if err != nil {
		t.Fatalf("open an owner pool: %v", err)
	}
	t.Cleanup(owner.Close)

	tm, err := NewTxManager(TxConfig{
		Pool:             pool,
		LockTimeout:      testLockTimeout,
		StatementTimeout: testStatementTimeout,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}
	return &world{tm: tm, app: pool, owner: owner, dsn: dsn}
}

// processor is the domain processor the fixtures apply operations with. The
// budget is generous: no fixture here is testing the wait budget.
func processor(t *testing.T) *wagering.Processor {
	t.Helper()
	policy, err := wagering.NewReferencePolicy(5, time.Hour)
	if err != nil {
		t.Fatalf("reference policy: %v", err)
	}
	p, err := wagering.NewProcessor(policy)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	return p
}

// openWallet creates a wallet through the real door, with its opening
// transaction and the ledger entry that records it when it is funded.
//
// Through the door rather than by INSERT, so that every test which later reads
// or reconciles this wallet is reading something the adapter itself produced.
func (w *world) openWallet(t *testing.T, player, amount, currency string) *wagering.Wallet {
	t.Helper()
	balance := mustMoney(t, amount, currency)
	in := wagering.OpenWalletInput{
		WalletID:       wagering.NewWalletID(),
		PlayerID:       mustPlayer(t, player),
		InitialBalance: balance,
	}
	if balance.IsPositive() {
		in.TransactionID = wagering.NewTransactionID()
		in.LedgerEntryID = wagering.NewLedgerEntryID()
	}
	wallet, outcome, err := wagering.OpenWallet(in, nil, at(0))
	if err != nil {
		t.Fatalf("open a wallet: %v", err)
	}
	err = w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		if err := r.Open(ctx, wallet, outcome, "fixture"); err != nil {
			return err
		}
		return w.append(ctx, r.Outbox, outcome.Events, at(0))
	})
	if err != nil {
		t.Fatalf("open a wallet: %v", err)
	}
	return wallet
}

// apply records one operation and settles whatever it came to, in one
// transaction, in the lock order the application uses.
//
// It is the smallest thing that produces realistic stored state — a settled
// transaction, its ledger entry, the wallet it moved and the events it emitted
// — without going through the use cases, which belong to another task. What it
// deliberately does not reproduce is the idempotency decision: a fixture that
// replayed would be testing the use case rather than setting up for a test.
func (w *world) apply(t *testing.T, cmd wagering.Command, now time.Time) wagering.Outcome {
	t.Helper()
	var outcome wagering.Outcome
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		wallet, err := r.Wallets.LockForMovement(ctx, wagering.WalletKey{
			PlayerID: cmd.PlayerID,
			Currency: cmd.Money.Currency(),
		})
		if err != nil {
			return err
		}
		if wallet == nil {
			t.Fatalf("player %q holds no %s wallet", cmd.PlayerID, cmd.Money.Currency())
		}
		tx, err := wagering.NewExternalTransaction(cmd, wallet.ID(), now)
		if err != nil {
			return err
		}
		if err := r.Transactions.Record(ctx, tx, "fixture"); err != nil {
			return err
		}
		var reference *wagering.ReferenceView
		if cmd.ReferenceExternalTransactionID != "" {
			reference, err = r.Transactions.ReferenceFor(ctx, cmd.Provider,
				cmd.ReferenceExternalTransactionID)
			if err != nil {
				return err
			}
		}
		outcome, err = processor(t).Continue(tx, cmd, wallet, reference, now)
		if err != nil {
			return err
		}
		var nextAttemptAt time.Time
		if outcome.Transaction.Status() == wagering.PendingReference {
			nextAttemptAt = now.Add(time.Minute)
		}
		if err := r.Settle(ctx, outcome, nextAttemptAt); err != nil {
			return err
		}
		return w.append(ctx, r.Outbox, outcome.Events, now)
	})
	if err != nil {
		t.Fatalf("apply a %s: %v", cmd.Kind, err)
	}
	return outcome
}

// append wraps an outcome's events and writes them, which is the last statement
// of every callback for the reason stated on app.Repos.
func (w *world) append(
	ctx context.Context,
	out app.OutboxWriter,
	events []wagering.Event,
	now time.Time,
) error {
	if len(events) == 0 {
		return nil
	}
	envelopes := make([]app.Envelope, 0, len(events))
	for _, event := range events {
		envelope, err := app.NewEnvelope(event, app.NewEventID(),
			app.Trace{Correlation: "fixture"}, now)
		if err != nil {
			return err
		}
		envelopes = append(envelopes, envelope)
	}
	return out.Append(ctx, envelopes)
}

// command builds a provider submission with the fields every kind needs.
func command(
	t *testing.T,
	kind wagering.Kind,
	player, external, amount, currency string,
) wagering.Command {
	t.Helper()
	cmd := wagering.Command{
		TransactionID:         wagering.NewTransactionID(),
		Provider:              "acme",
		ExternalTransactionID: wagering.ExternalTransactionID(external),
		IdempotencyKey:        wagering.IdempotencyKey("key-" + external),
		PlayerID:              mustPlayer(t, player),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Money:                 mustMoney(t, amount, currency),
	}
	if kind.MovesMoney() {
		cmd.LedgerEntryID = wagering.NewLedgerEntryID()
	}
	return cmd
}

// reversingCommand returns cmd pointed at the operation it undoes.
//
// Twin of [reversingSubmission], which does the same to a submission that has
// not been parsed yet. Two of them because the two fixtures work at different
// levels — this one builds the domain value a port-level test applies directly,
// that one the strings a provider sends — and naming them apart by level rather
// than by nothing is what keeps a reader from assuming there is only one.
func reversingCommand(cmd wagering.Command, reference string) wagering.Command {
	cmd.ReferenceExternalTransactionID = wagering.ExternalTransactionID(reference)
	return cmd
}

// count runs a scalar count against the owner pool, which sees everything.
func (w *world) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := w.owner.QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v\n\t%s", err, query)
	}
	return n
}

// exec runs a statement as the owner, for the fixtures the application cannot
// write.
func (w *world) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := w.owner.Exec(t.Context(), query, args...); err != nil {
		t.Fatalf("owner statement failed: %v\n\t%s", err, query)
	}
}

// corrupt runs a statement with the table's triggers disabled, which is the
// only way to store state this system cannot produce.
//
// Reconciliation exists to find exactly that state, so a test of it has to be
// able to create it: every path the application has is guarded by a trigger,
// and the guards work. This stands in for a bad migration, a manual UPDATE or a
// bug in another service.
func (w *world) corrupt(t *testing.T, table, query string, args ...any) {
	t.Helper()
	w.exec(t, `ALTER TABLE `+table+` DISABLE TRIGGER USER`)
	defer w.exec(t, `ALTER TABLE `+table+` ENABLE TRIGGER USER`)
	w.exec(t, query, args...)
}

// refusedBy asserts that err is a refusal carrying the named rule.
//
// The rule is asserted rather than the SQLSTATE, for the reason the storage
// suite gives: the SQLSTATE says only which mechanism refused, and P0001 —
// every trigger in this schema — does not say even that. The name is the
// contract; which mechanism enforces it today is not.
func refusedBy(t *testing.T, err error, rule string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the statement was allowed but should have been refused by %s", rule)
	}
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		t.Fatalf("wanted %s to refuse the statement, got an error with no SQLSTATE: %v", rule, err)
	}
	if pgErr.ConstraintName != rule {
		t.Fatalf("refused by %q, wanted %q: %v", pgErr.ConstraintName, rule, err)
	}
}

// refusedWith asserts that err carries the given SQLSTATE.
//
// For the refusals that have no rule to name: a privilege the role does not
// hold, or an access mode that forbids the statement outright. Where a named
// rule did the refusing, [refusedBy] pins the rule instead.
func refusedWith(t *testing.T, err error, state string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the statement was allowed but should have been refused with %s", state)
	}
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		t.Fatalf("wanted %s, got an error with no SQLSTATE: %v", state, err)
	}
	if pgErr.Code != state {
		t.Fatalf("refused with %s, wanted %s: %v", pgErr.Code, state, err)
	}
}

// classifies asserts how a caller should answer err.
func classifies(t *testing.T, err error, want app.Class) {
	t.Helper()
	if err == nil {
		t.Fatalf("wanted a %s error, got nil", want)
	}
	if got := app.ClassOf(err); got != want {
		t.Fatalf("classified %s, wanted %s: %v", got, want, err)
	}
}

// mustMoney is for fixtures, where an unparseable amount is a broken test
// rather than a case under test.
func mustMoney(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("parse %s %s: %v", amount, currency, err)
	}
	return m
}

func mustPlayer(t *testing.T, id string) wagering.PlayerID {
	t.Helper()
	player, err := wagering.NewPlayerID(id)
	if err != nil {
		t.Fatalf("player %q: %v", id, err)
	}
	return player
}

// park produces a PENDING_REFERENCE operation: a refund naming a reference that
// has not arrived, which is the one way an operation comes to be waiting.
//
// It returns the operation's identifier, which is what every claim query takes.
func (w *world) park(
	t *testing.T,
	player, external, reference string,
	now time.Time,
) wagering.TransactionID {
	t.Helper()
	cmd := reversingCommand(command(t, wagering.Refund, player, external, "10.00", "BRL"), reference)
	outcome := w.apply(t, cmd, now)
	if got := outcome.Transaction.Status(); got != wagering.PendingReference {
		t.Fatalf("the operation is %s, wanted PENDING_REFERENCE", got)
	}
	return outcome.Transaction.ID()
}

// referenceFor reads a reference view through the adapter.
func (w *world) referenceFor(
	t *testing.T,
	provider wagering.Provider,
	external wagering.ExternalTransactionID,
) *wagering.ReferenceView {
	t.Helper()
	var view *wagering.ReferenceView
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		var err error
		view, err = r.Transactions.ReferenceFor(ctx, provider, external)
		return err
	})
	if err != nil {
		t.Fatalf("read the reference for %q: %v", external, err)
	}
	return view
}

// rescheduleOperation moves a parked operation's next attempt through the
// adapter.
func (w *world) rescheduleOperation(t *testing.T, id wagering.TransactionID, to time.Time) {
	t.Helper()
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		return r.Transactions.Reschedule(ctx, id, to)
	})
	if err != nil {
		t.Fatalf("reschedule %s: %v", id, err)
	}
}

// nextDue asks the adapter what is claimable at an instant.
func (w *world) nextDue(t *testing.T, now time.Time) *app.DueCandidate {
	t.Helper()
	var candidate *app.DueCandidate
	err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
		store, ok := r.Transactions.(transactions)
		if !ok {
			t.Fatalf("the reader is a %T, not this package's", r.Transactions)
		}
		var err error
		candidate, err = store.NextDue(ctx, now)
		return err
	})
	if err != nil {
		t.Fatalf("find what is due: %v", err)
	}
	return candidate
}

// storedRow is the part of a wager transaction row the schedule tests assert
// on, read straight from SQL because the domain deliberately does not carry the
// worker's schedule.
type storedRow struct {
	status    string
	attempts  int32
	deadline  pgtype.Timestamptz
	scheduled pgtype.Timestamptz
	updatedAt time.Time
}

func (w *world) rowOf(t *testing.T, id wagering.TransactionID) storedRow {
	t.Helper()
	var row storedRow
	err := w.owner.QueryRow(t.Context(),
		`SELECT status, reference_attempts, reference_deadline, `+
			`reference_next_attempt_at, updated_at `+
			`FROM wagering.wager_transaction WHERE id = $1`, uuidOf(id)).
		Scan(&row.status, &row.attempts, &row.deadline, &row.scheduled, &row.updatedAt)
	if err != nil {
		t.Fatalf("read row %s: %v", id, err)
	}
	return row
}

// scheduleOf reads when an operation is next due to be looked at.
func (w *world) scheduleOf(t *testing.T, id wagering.TransactionID) time.Time {
	t.Helper()
	return timeFrom(w.rowOf(t, id).scheduled)
}

// wideManager opens a second application pool on this world's database, and a
// manager over it.
//
// Two things about it are deliberately not the world's own. The pool is as wide
// as the scenario's concurrency, so that fifty submissions are fifty
// connections rather than fifty goroutines taking turns on sixteen — which
// would make this suite's pool the serialisation point and leave the database's
// nothing to do. And the lock timeout bounds a queue rather than a hand-off:
// the last of fifty waiters on one wallet row waits for the forty-nine in front
// of it, so the two seconds that are generous for one waiter are not a bound on
// the same thing at all.
func (w *world) wideManager(t *testing.T, conns int32, lock time.Duration) *TxManager {
	t.Helper()
	tm, err := NewTxManager(TxConfig{
		Pool:             newAppPool(t, w.dsn, conns),
		LockTimeout:      lock,
		StatementTimeout: testStatementTimeout,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}
	return tm
}

// The ports this package does not implement, in the smallest form that proves
// the wiring. Their real adapters are somebody else's task; what matters here
// is that nothing about them is this package's problem.
//
// mintedIDs mints a fresh identifier on every call rather than answering a
// fixed one, and that is load-bearing for the scenarios that submit the same
// operation from fifty goroutines: a source handing them all one transaction id
// would make "fifty callers, one identifier" true of the fixture instead of
// true of the database.
type (
	mintedIDs      struct{}
	discardDefects struct{}
)

func (mintedIDs) WalletID() wagering.WalletID           { return wagering.NewWalletID() }
func (mintedIDs) TransactionID() wagering.TransactionID { return wagering.NewTransactionID() }
func (mintedIDs) LedgerEntryID() wagering.LedgerEntryID { return wagering.NewLedgerEntryID() }
func (mintedIDs) EventID() app.EventID                  { return app.NewEventID() }

func (discardDefects) CannotCarryForward(context.Context, wagering.TransactionID, error) {}

// tickingClock is the clock the use cases read, moved by hand.
//
// Settable rather than fixed, because a sequence of operations on one wallet
// has to be stamped with a sequence of instants: wallet_clock_moves_forward
// refuses a balance change that does not move updated_at strictly forward, so a
// clock answering the same instant for a bet and the refund that returns it
// would have the schema refuse the second for a reason that has nothing to do
// with what is under test.
//
// The mutex is not decoration. Submit samples the clock inside the movement
// transaction, and the scenarios here call it from fifty goroutines at once.
type tickingClock struct {
	mu sync.Mutex
	at time.Time
}

// newClock starts a clock at an instant.
func newClock(at time.Time) *tickingClock { return &tickingClock{at: at} }

// Now answers whatever the clock has been set to.
func (c *tickingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

// moveTo sets the instant every operation from here on is stamped with.
func (c *tickingClock) moveTo(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

// useCases is the real application layer, wired onto one test's database.
//
// Nothing in it stands in for anything: app.NewWagering and app.NewWallets over
// a real TxManager, the domain's own processor and a real reference policy.
// That is the whole point of the scenarios that use it — every other test in
// this package drives a port directly, which proves each port behaves and
// proves nothing about what a command does end to end.
type useCases struct {
	wagers  *app.Wagering
	wallets *app.Wallets
	clock   *tickingClock
}

// wire builds the application layer on this world's manager, clocked at now.
func (w *world) wire(t *testing.T, now time.Time) useCases {
	t.Helper()
	return wireOnto(t, w.tm, mintedIDs{}, now)
}

// wireOnto builds the same application layer over a manager and an identifier
// source the caller supplies: a pool wide enough for fifty transactions at
// once, or a source that cancels the request as it hands over an event id.
func wireOnto(t *testing.T, tm app.TxManager, ids app.IDs, now time.Time) useCases {
	t.Helper()
	clock := newClock(now)
	wagers, err := app.NewWagering(app.WageringDeps{
		Tx:        tm,
		Processor: processor(t),
		Clock:     clock,
		IDs:       ids,
		Backoff:   app.BackoffPolicy{Initial: time.Second, Factor: 2, Max: time.Minute},
		Defects:   discardDefects{},
	})
	if err != nil {
		t.Fatalf("wire the wagering service: %v", err)
	}
	wallets, err := app.NewWallets(tm, clock, ids, nil)
	if err != nil {
		t.Fatalf("wire the wallets service: %v", err)
	}
	return useCases{wagers: wagers, wallets: wallets, clock: clock}
}

// servicePrincipal is this system acting for itself: the only identity that may
// open a wallet, read one or resume parked work.
func servicePrincipal(t *testing.T) app.Principal {
	t.Helper()
	principal, err := app.NewServicePrincipal("service-subject")
	if err != nil {
		t.Fatalf("service principal: %v", err)
	}
	return principal
}

// providerPrincipal is the provider every submission here is sent as. It may
// not open a wallet, which is why the fixtures above take the other one.
func providerPrincipal(t *testing.T) app.Principal {
	t.Helper()
	principal, err := app.NewProviderPrincipal("acme", "provider-subject")
	if err != nil {
		t.Fatalf("provider principal: %v", err)
	}
	return principal
}

// open creates a wallet through the real use case.
//
// As the service rather than as the provider whose operations follow: a
// provider may not open a wallet, and a fixture that quietly used an identity
// with more authority than the scenario has would be proving the door open with
// a key the test never has to hold.
func (u useCases) open(t *testing.T, player, amount string) app.WalletView {
	t.Helper()
	view, _, err := u.wallets.Open(t.Context(), app.OpenWalletCommand{
		Principal:     servicePrincipal(t),
		Correlation:   "correlation-open-" + player,
		PlayerID:      player,
		InitialAmount: amount,
		Currency:      "BRL",
	})
	if err != nil {
		t.Fatalf("open a %s wallet for %q: %v", amount, player, err)
	}
	return view
}

// balanceOf reads a wallet's balance through the use case, as the decimal
// string a caller would be given.
func (u useCases) balanceOf(t *testing.T, id wagering.WalletID) string {
	t.Helper()
	view, err := u.wallets.ByID(t.Context(), servicePrincipal(t), id)
	if err != nil {
		t.Fatalf("read wallet %s: %v", id, err)
	}
	return view.Balance.Amount()
}

// submission is one operation as a provider sends it: still strings, still
// unparsed, exactly what a transport hands over.
//
// Separate from [command], which builds the domain value the port-level
// fixtures apply directly. A scenario goes in the front door, so the use case
// does the parsing and the identifier minting — which is half of what these
// tests exist to exercise. The currency is BRL because every fixture in this
// package uses it, and a fourth string parameter saying so at every call site
// would say nothing.
func submission(
	t *testing.T,
	kind wagering.Kind,
	player, external, key, amount string,
) app.SubmitOperation {
	t.Helper()
	return app.SubmitOperation{
		Principal:   providerPrincipal(t),
		Correlation: "correlation-" + external,
		Fields: app.OperationFields{
			Provider:              "acme",
			ExternalTransactionID: external,
			IdempotencyKey:        key,
			PlayerID:              player,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  kind.String(),
			Amount:                amount,
			Currency:              "BRL",
		},
	}
}

// reversingSubmission returns a submission pointed at the operation it undoes.
// Twin of [reversingCommand]; see the note there.
func reversingSubmission(s app.SubmitOperation, reference string) app.SubmitOperation {
	s.Fields.ReferenceExternalTransactionID = reference
	return s
}

// carrying attaches the queue message a submission arrived on, which is what
// puts an inbox row in the command's write set.
//
// The body hash is a real SHA-256 of the message id rather than a made-up
// string, because wagering.sha256_hex refuses anything that is not sixty-four
// lowercase hex characters — the schema holds the fixtures to the same shape it
// holds the service to.
func carrying(s app.SubmitOperation, messageID string) app.SubmitOperation {
	digest := sha256.Sum256([]byte(messageID))
	s.Inbox = &app.InboxMessage{
		Consumer:  "wagering-consumer",
		MessageID: messageID,
		BodyHash:  hex.EncodeToString(digest[:]),
	}
	return s
}

// renderedBalance is an operation result's balance as a caller would read it,
// and "none" when it carries none — which ports.go says is exactly when the
// operation is anything but PROCESSED.
func renderedBalance(r app.OperationResult) string {
	if r.Balance == nil {
		return "none"
	}
	return r.Balance.Amount()
}

// footprint is everything one wallet has left behind in the database: every
// table a command writes into, counted, plus what the wallet row itself says.
//
// It is compared whole rather than field by field, so that a test meaning "the
// database is exactly where it was" says so in one assertion instead of seven
// that could each be forgotten. Holds is the one that is easy to leave out and
// the one worth most: a reversal's write set includes a row the active_reversal
// trigger takes on its behalf, which no caller here writes and which a rollback
// therefore has to undo without anything in the application having asked for
// it. Inbox is not wallet-scoped and counts every row in the database, which is
// this test's alone.
type footprint struct {
	Balance      int64
	Version      int64
	Transactions int
	Entries      int
	Events       int
	Holds        int
	Inbox        int
}

// footprintOf reads that footprint, as the owner, so that nothing about what
// the application may see can hide a row from it.
func (w *world) footprintOf(t *testing.T, id wagering.WalletID) footprint {
	t.Helper()
	var f footprint
	err := w.owner.QueryRow(t.Context(),
		`SELECT w.balance_minor, w.version, `+
			`(SELECT count(*) FROM wagering.wager_transaction WHERE wallet_id = w.id), `+
			`(SELECT count(*) FROM wagering.wallet_ledger_entry WHERE wallet_id = w.id), `+
			`(SELECT count(*) FROM wagering.outbox WHERE aggregate_id = w.id), `+
			`(SELECT count(*) FROM wagering.active_reversal a `+
			`   JOIN wagering.wager_transaction r ON r.id = a.reversal_id `+
			`  WHERE r.wallet_id = w.id), `+
			`(SELECT count(*) FROM wagering.inbox) `+
			`FROM wagering.wallet w WHERE w.id = $1`, uuidOf(id)).
		Scan(&f.Balance, &f.Version, &f.Transactions, &f.Entries, &f.Events,
			&f.Holds, &f.Inbox)
	if err != nil {
		t.Fatalf("read what wallet %s has left behind: %v", id, err)
	}
	return f
}

// debitsOf counts the ledger entries that took money out of a wallet. The
// opening of a funded wallet is a credit, so a scenario's own debits are
// countable without discounting the fixture that set it up.
func (w *world) debitsOf(t *testing.T, id wagering.WalletID) int {
	t.Helper()
	return w.count(t,
		`SELECT count(*) FROM wagering.wallet_ledger_entry `+
			`WHERE wallet_id = $1 AND direction = 'DEBIT'`, uuidOf(id))
}

// queuedOnARowLock counts the backends waiting for a row lock in this test's
// database.
//
// transactionid and tuple are the two waits a row lock produces: a contender
// queues on the tuple to take its turn, and the one whose turn it is waits on
// the transaction that holds the row. Nothing else in this database locks a row
// while a hold is in place, so a backend in either wait is a backend waiting
// for the held wallet.
//
// The holder itself is never counted. It is blocked on a Go channel with its
// transaction open, which the server reports as waiting on the client.
const queuedOnARowLock = `SELECT count(*) FROM pg_stat_activity ` +
	`WHERE datname = current_database() AND wait_event_type = 'Lock' ` +
	`AND wait_event IN ('transactionid', 'tuple')`

// hold is one wallet's movement lock, taken by a transaction of the test's own
// and kept until [hold.release].
//
// It is the barrier the concurrency scenarios are released from. Starting
// goroutines together is not enough on its own: the first to arrive would take
// the lock, finish and commit while the rest were still being scheduled, and
// the contention the test is written for would never happen. With the lock
// already held, every submission queues on it instead.
type hold struct {
	world  *world
	player string
	let    chan struct{}
	letGo  sync.Once
	done   chan error
}

// holdWallet takes a wallet's movement lock and keeps it.
func (w *world) holdWallet(t *testing.T, player string) *hold {
	t.Helper()
	h := &hold{
		world:  w,
		player: player,
		let:    make(chan struct{}),
		done:   make(chan error, 1),
	}
	// Released whatever happens, and before the pools are closed: cleanups run
	// last in, first out and this world's pools registered theirs first. A test
	// that fails before it gets to release — awaitWaiters timing out is exactly
	// that — would otherwise leave this goroutine parked on a channel nothing
	// will ever close, holding a connection the pool's Close then waits for,
	// and a test that should have failed in fifteen seconds instead hangs until
	// the whole binary is killed. Observed, before this line existed.
	t.Cleanup(h.free)
	taken := make(chan struct{})
	go func() {
		h.done <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			wallet, err := r.Wallets.LockForMovement(ctx, wagering.WalletKey{
				PlayerID: mustPlayer(t, player),
				Currency: mustMoney(t, "0.00", "BRL").Currency(),
			})
			if err != nil {
				return err
			}
			if wallet == nil {
				return errors.New("there is no wallet to hold")
			}
			close(taken)
			<-h.let
			return nil
		})
	}()
	<-taken
	return h
}

// awaitWaiters blocks until exactly n backends are queued on the held wallet,
// and fails the test if they never are.
//
// This is what tells "queued on the wallet" apart from "queued on something
// else", and it is what the concurrency scenarios actually rest on. A
// submission still waiting for a connection from its pool has not begun a
// transaction and holds nothing, so it is invisible here — which means a pool
// too narrow to carry the whole race fails this outright instead of quietly
// reducing the contention to whatever the pool happened to allow. The pool's
// width stops being a constant a reader has to trust and becomes one this
// assertion enforces.
//
// It also replaces a sleep. That the queue has formed is a thing the database
// can be asked; that 250 milliseconds is long enough for it to have formed is a
// guess, and one that gets worse on a slower machine.
func (h *hold) awaitWaiters(t *testing.T, n int) {
	t.Helper()
	const (
		budget = 15 * time.Second
		poll   = 2 * time.Millisecond
	)
	deadline := time.Now().Add(budget)
	most := 0
	for time.Now().Before(deadline) {
		queued := h.world.count(t, queuedOnARowLock)
		if queued == n {
			return
		}
		// The highest seen rather than the last seen. A queue that formed and
		// then drained — every contender timing out on the lock, one after
		// another — reads as nought at the deadline, which says the opposite
		// of what happened.
		most = max(most, queued)
		time.Sleep(poll)
	}
	t.Fatalf("at most %d backends were ever queued on %q's wallet within %s, wanted %d: "+
		"the submissions are not all contending for the lock",
		most, h.player, budget, n)
}

// free lets the hold go without waiting for it, and may be called any number of
// times. It is what the cleanup above uses.
func (h *hold) free() { h.letGo.Do(func() { close(h.let) }) }

// release lets the hold go, waits for its transaction to commit, and reports
// the instant just before it let go.
func (h *hold) release(t *testing.T) time.Time {
	t.Helper()
	// Read before the lock goes, so that "answered after this" cannot be
	// satisfied by a submission that was never blocked.
	at := time.Now()
	h.free()
	if err := <-h.done; err != nil {
		t.Fatalf("hold %q's wallet: %v", h.player, err)
	}
	return at
}

// attempt is one submission's result and the window it ran in.
type attempt struct {
	result     app.OperationResult
	err        error
	startedAt  time.Time
	finishedAt time.Time
}

// racing submits every one of these at once, a goroutine each, released
// together, and returns the function that collects what they came to.
//
// The barrier is the point of it. Every goroutine waits at one channel and none
// of them is running while the others are still being started, so a submission
// cannot finish before another has begun — which a loop that started them one
// at a time permits, and which is the shape of concurrency test that passes
// whether or not anything contended.
//
// Collecting is a second call rather than part of this one so that the caller
// can do something while they are all in flight: hold the wallet lock they are
// queued on, and let go of it on its own terms.
func (u useCases) racing(t *testing.T, submissions []app.SubmitOperation) func() []attempt {
	t.Helper()
	var ready, done sync.WaitGroup
	ready.Add(len(submissions))
	done.Add(len(submissions))
	start := make(chan struct{})
	attempts := make([]attempt, len(submissions))
	for i, s := range submissions {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			a := attempt{startedAt: time.Now()}
			a.result, a.err = u.wagers.Submit(t.Context(), s)
			a.finishedAt = time.Now()
			attempts[i] = a
		}()
	}
	// Released only once every goroutine is at the line.
	ready.Wait()
	close(start)
	return func() []attempt {
		done.Wait()
		return attempts
	}
}

// submit sends one operation and asserts what it came to.
//
// It is the fixture for the operations a scenario needs settled before the one
// it is actually about — a bet that a refund can return, a refund that a second
// reversal can be refused against.
func (u useCases) submit(
	t *testing.T,
	s app.SubmitOperation,
	want wagering.Status,
) app.OperationResult {
	t.Helper()
	result, err := u.wagers.Submit(t.Context(), s)
	if err != nil {
		t.Fatalf("submit %s %q: %v", s.Fields.Kind, s.Fields.ExternalTransactionID, err)
	}
	if result.Status != want {
		t.Fatalf("%s %q came to %s under %q, wanted %s", s.Fields.Kind,
			s.Fields.ExternalTransactionID, result.Status, result.FailureCode, want)
	}
	return result
}
