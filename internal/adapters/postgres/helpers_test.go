//go:build integration

package postgres

import (
	"context"
	"errors"
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
	return &world{tm: tm, app: pool, owner: owner}
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

// reversing returns cmd pointed at the operation it undoes.
func reversing(cmd wagering.Command, reference string) wagering.Command {
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
	cmd := reversing(command(t, wagering.Refund, player, external, "10.00", "BRL"), reference)
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
