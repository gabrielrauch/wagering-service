//go:build integration

package postgres

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/faults"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// TestTheTransactionManagerCommitsAndRollsBack pins the whole of the callback
// contract: nil commits, an error rolls back and is returned unchanged, and a
// panic rolls back and carries on unwinding.
func TestTheTransactionManagerCommitsAndRollsBack(t *testing.T) {
	t.Parallel()

	t.Run("a nil return commits", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		w.openWallet(t, "player-commit", "100.00", "BRL")

		if got := w.count(t, `SELECT count(*) FROM wagering.wallet`); got != 1 {
			t.Fatalf("committed %d wallets, wanted 1", got)
		}
	})

	t.Run("an error rolls back and is returned as it left the callback", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		mine := errors.New("the callback gave up")

		err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			wallet, outcome := newWallet(t, "player-rollback", "100.00", "BRL")
			if err := r.Open(ctx, wallet, outcome, "rollback"); err != nil {
				return err
			}
			return mine
		})
		if !errors.Is(err, mine) {
			t.Fatalf("returned %v, wanted the callback's own error", err)
		}
		if got := w.count(t, `SELECT count(*) FROM wagering.wallet`); got != 0 {
			t.Fatalf("%d wallets survived a rollback, wanted 0", got)
		}
	})

	t.Run("a panic rolls back and keeps unwinding", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)

		func() {
			defer func() {
				recovered := recover()
				if recovered != "the callback exploded" {
					t.Fatalf("recovered %v, wanted the callback's own panic", recovered)
				}
			}()
			_ = w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
				wallet, outcome := newWallet(t, "player-panic", "100.00", "BRL")
				if err := r.Open(ctx, wallet, outcome, "panic"); err != nil {
					return err
				}
				panic("the callback exploded")
			})
		}()

		if got := w.count(t, `SELECT count(*) FROM wagering.wallet`); got != 0 {
			t.Fatalf("%d wallets survived a panic, wanted 0", got)
		}
		// The connection went back to the pool rather than being abandoned,
		// which is the half of panic handling a row count cannot show.
		w.openWallet(t, "player-after-panic", "5.00", "BRL")
	})
}

// TestTheTransactionManagerSetsWhatItPromises reads back the settings the
// manager applies, in the transaction it applied them to.
//
// Reading current_setting rather than inferring the isolation level from
// behaviour: behaviour proves READ COMMITTED is not REPEATABLE READ and vice
// versa, which TestAMovementSeesWhatCommittedWhileItRan and
// TestAReconciliationReadsOneInstant already do between them, but it cannot
// tell REPEATABLE READ from SERIALIZABLE, and the timeouts have no behaviour at
// all until something blocks.
func TestTheTransactionManagerSetsWhatItPromises(t *testing.T) {
	t.Parallel()
	w := newWorld(t)

	// The timeouts are read from pg_settings rather than current_setting,
	// because current_setting renders them the way a human would write them —
	// "750ms", but "2s" — and an assertion against that spelling is an
	// assertion about PostgreSQL's formatting that breaks when the configured
	// value crosses a unit. pg_settings.setting is the raw count in the
	// setting's base unit, which is milliseconds, which is exactly what
	// [milliseconds] renders. Comparing the two also checks that function
	// against what the server actually stored.
	const settings = `SELECT current_setting('transaction_isolation'), ` +
		`current_setting('transaction_read_only'), ` +
		`(SELECT setting FROM pg_settings WHERE name = 'lock_timeout'), ` +
		`(SELECT setting FROM pg_settings WHERE name = 'statement_timeout')`

	read := func(
		t *testing.T, span, kind string, options pgx.TxOptions,
	) (isolation, readOnly, lock, statement string) {
		t.Helper()
		err := w.tm.within(t.Context(), span, kind, options,
			func(ctx context.Context, tx pgx.Tx) error {
				return tx.QueryRow(ctx, settings).Scan(&isolation, &readOnly, &lock, &statement)
			})
		if err != nil {
			t.Fatalf("read the transaction's settings: %v", err)
		}
		return isolation, readOnly, lock, statement
	}

	t.Run("a movement", func(t *testing.T) {
		isolation, readOnly, lock, statement := read(t, telemetry.SpanMovement, telemetry.Movement, movementOptions)
		if isolation != "read committed" {
			t.Errorf("isolation %q, wanted read committed", isolation)
		}
		if readOnly != "off" {
			t.Errorf("read only %q, wanted off", readOnly)
		}
		if want := milliseconds(testLockTimeout); lock != want {
			t.Errorf("lock_timeout %q ms, wanted %q", lock, want)
		}
		if want := milliseconds(testStatementTimeout); statement != want {
			t.Errorf("statement_timeout %q ms, wanted %q", statement, want)
		}
	})

	t.Run("a snapshot", func(t *testing.T) {
		isolation, readOnly, lock, statement := read(t, telemetry.SpanSnapshot, telemetry.Snapshot, snapshotOptions)
		if isolation != "repeatable read" {
			t.Errorf("isolation %q, wanted repeatable read", isolation)
		}
		if readOnly != "on" {
			t.Errorf("read only %q, wanted on", readOnly)
		}
		if want := milliseconds(testLockTimeout); lock != want {
			t.Errorf("lock_timeout %q ms, wanted %q", lock, want)
		}
		if want := milliseconds(testStatementTimeout); statement != want {
			t.Errorf("statement_timeout %q ms, wanted %q", statement, want)
		}
	})

	t.Run("the timeouts do not outlive the transaction", func(t *testing.T) {
		// set_config with is_local true, not a session SET. A pooled connection
		// is reused, so a setting that leaked would apply to whatever command
		// happened to be handed that connection next.
		var lock string
		if err := w.app.QueryRow(t.Context(),
			`SELECT setting FROM pg_settings WHERE name = 'lock_timeout'`).Scan(&lock); err != nil {
			t.Fatalf("read the session's lock_timeout: %v", err)
		}
		if lock != "0" {
			t.Errorf("lock_timeout leaked out of the transaction as %q, wanted 0", lock)
		}
	})
}

// TestAMovementSeesWhatCommittedWhileItRan is READ COMMITTED, demonstrated.
//
// It is the other half of TestAReconciliationReadsOneInstant, and it is not a
// curiosity about isolation levels: ClaimForUpdate rests on it. The resume
// worker finds its work with an unlocked SELECT and then re-reads the row under
// its own lock, and that re-read is only worth making because a change another
// worker committed in between is visible to it. At REPEATABLE READ the re-read
// would see exactly what the unlocked SELECT saw, and the second worker would
// carry forward an operation the first had already settled.
func TestAMovementSeesWhatCommittedWhileItRan(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-read-committed", "100.00", "BRL")

	var before, after *wagering.Wallet
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		var err error
		// Read without the lock, so that the movement below is not simply
		// queued behind this transaction — which would prove nothing about what
		// this one can see.
		if before, err = r.Wallets.ByID(ctx, wallet.ID()); err != nil {
			return err
		}
		w.apply(t, command(t, wagering.Bet, "player-read-committed", "ext-rc-1", "30.00", "BRL"),
			at(1))
		after, err = r.Wallets.ByID(ctx, wallet.ID())
		return err
	})
	if err != nil {
		t.Fatalf("read across a commit: %v", err)
	}

	if got := before.Balance().Amount(); got != "100.00" {
		t.Fatalf("the first read saw %s, wanted 100.00", got)
	}
	if got := after.Balance().Amount(); got != "70.00" {
		t.Fatalf("the second read saw %s, wanted the 70.00 that committed meanwhile", got)
	}
}

// TestASnapshotRefusesAWrite proves the second half of READ ONLY.
//
// The first half is the type system: a snapshot callback is handed ReadRepos,
// which carries no writer, so a use case that tried to write would not compile.
// This is the half that holds when somebody reaches past the types — the server
// refuses the statement whatever the caller believed it was doing.
func TestASnapshotRefusesAWrite(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-readonly", "100.00", "BRL")

	err := w.tm.within(t.Context(), telemetry.SpanSnapshot, telemetry.Snapshot, snapshotOptions,
		func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE wagering.wallet SET balance_minor = 1 WHERE id = $1`,
				uuidOf(wallet.ID()))
			return err
		})
	// By SQLSTATE rather than by rule name, because no rule refused it: the
	// transaction's access mode did, before any constraint was consulted.
	refusedWith(t, err, pgerrcode.ReadOnlySQLTransaction)
}

// TestAMovementWaitsForAWalletAndThenGivesUp proves the lock wait is bounded.
//
// A command that arrives second on a busy wallet should wait its turn — which
// is why the lock is taken without NOWAIT — but it must not wait forever, or a
// transaction that hangs takes every later command on that wallet with it.
// lock_timeout is what draws the line, and the failure is Retryable because
// nothing was recorded and the wallet will be free shortly.
func TestAMovementWaitsForAWalletAndThenGivesUp(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-busy", "100.00", "BRL")

	held := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held

	started := time.Now()
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		_, err := r.Wallets.LockByID(ctx, wallet.ID())
		return err
	})
	waited := time.Since(started)
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("the holder failed: %v", err)
	}

	classifies(t, err, app.Retryable)
	if waited < testLockTimeout {
		t.Errorf("gave up after %s, before the %s lock timeout", waited, testLockTimeout)
	}
}

// newWallet builds a wallet and its opening without writing anything, for the
// tests that want to control when — and whether — the write lands.
func newWallet(t *testing.T, player, amount, currency string) (*wagering.Wallet, wagering.Outcome) {
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
		t.Fatalf("build a wallet: %v", err)
	}
	return wallet, outcome
}

// TestTheTwoWaysATransactionLosesARaceAreCountedApart pins the contention
// numbers, and the distinction between them.
//
// A lock timeout is the wallet lock WORKING: two commands met on one wallet and
// the second waited longer than DATABASE_LOCK_TIMEOUT allows. It is ordinary,
// it is tuned with that variable, and an operator watches the rate.
//
// A version conflict is the lock NOT having been held. The application layer
// takes it before it computes anything, so a write refused for a stale version
// means something reached a movement another way — and that number should be
// zero. Averaged into one "contention" counter the second is invisible, which
// is the whole reason they are two instruments.
func TestTheTwoWaysATransactionLosesARaceAreCountedApart(t *testing.T) {
	t.Parallel()

	t.Run("a lock somebody else is holding", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		reporting, reader := countedTelemetry(t)
		manager := tracedManager(t, w, reporting)
		wallet := w.openWallet(t, "player-counted-busy", "100.00", "BRL")

		held := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- manager.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
				if _, err := r.Wallets.LockByID(ctx, wallet.ID()); err != nil {
					return err
				}
				close(held)
				<-release
				return nil
			})
		}()
		<-held

		err := manager.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			_, err := r.Wallets.LockByID(ctx, wallet.ID())
			return err
		})
		close(release)
		if holderErr := <-done; holderErr != nil {
			t.Fatalf("the holder failed: %v", holderErr)
		}
		classifies(t, err, app.Retryable)

		if got := sumOf(t, reader, telemetry.MetricLockTimeouts,
			map[string]string{"transaction": telemetry.Movement}); got != 1 {
			t.Errorf("%s counted %d, wanted the timeout", telemetry.MetricLockTimeouts, got)
		}
		if got := sumOf(t, reader, telemetry.MetricVersionConflicts,
			map[string]string{"transaction": telemetry.Movement}); got != 0 {
			t.Errorf("%s counted %d for a lock timeout", telemetry.MetricVersionConflicts, got)
		}
	})

	t.Run("a wallet that moved under the movement", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		reporting, reader := countedTelemetry(t)
		manager := tracedManager(t, w, reporting)
		w.openWallet(t, "player-counted-stale", "100.00", "BRL")

		// Read outside any lock, which is the whole of the mistake being made.
		key := wagering.WalletKey{
			PlayerID: mustPlayer(t, "player-counted-stale"),
			Currency: mustMoney(t, "0.00", "BRL").Currency(),
		}
		var stale *wagering.Wallet
		if err := manager.WithinSnapshot(t.Context(),
			func(ctx context.Context, r *app.ReadRepos) error {
				var err error
				stale, err = r.Wallets.ByKey(ctx, key)
				return err
			}); err != nil || stale == nil {
			t.Fatalf("read the wallet: %v", err)
		}

		// Somebody else moves it properly, through the lock.
		w.apply(t, command(t, wagering.Bet, "player-counted-stale", "ext-w", "10.00", "BRL"), at(1))

		cmd := command(t, wagering.Bet, "player-counted-stale", "ext-l", "20.00", "BRL")
		err := manager.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			tx, err := wagering.NewExternalTransaction(cmd, stale.ID(), at(2))
			if err != nil {
				return err
			}
			if err := r.Transactions.Record(ctx, tx, "stale"); err != nil {
				return err
			}
			outcome, err := processor(t).Continue(tx, cmd, stale, nil, at(2))
			if err != nil {
				return err
			}
			return r.Settle(ctx, outcome, time.Time{})
		})
		if !errors.Is(err, ErrLostUpdate) {
			t.Fatalf("settled a stale movement with %v, wanted a lost update", err)
		}

		if got := sumOf(t, reader, telemetry.MetricVersionConflicts,
			map[string]string{"transaction": telemetry.Movement}); got != 1 {
			t.Errorf("%s counted %d, wanted the conflict", telemetry.MetricVersionConflicts, got)
		}
		if got := sumOf(t, reader, telemetry.MetricLockTimeouts,
			map[string]string{"transaction": telemetry.Movement}); got != 0 {
			t.Errorf("%s counted %d for a version conflict", telemetry.MetricLockTimeouts, got)
		}
	})
}

// TestAnOrdinaryFailureIsNotContention pins the other half of the two counters.
//
// A transaction fails for many reasons and almost none of them are contention:
// a constraint refused the write, the callback gave up, the connection went
// away. Counting any of those as a lock timeout would make the number an
// operator tunes DATABASE_LOCK_TIMEOUT from into a general failure rate, and
// the tuning would be nonsense.
func TestAnOrdinaryFailureIsNotContention(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	reporting, reader := countedTelemetry(t)
	manager := tracedManager(t, w, reporting)

	refused := errors.New("the callback gave up")
	err := manager.WithinMovement(t.Context(), func(context.Context, *app.Repos) error {
		return refused
	})
	if !errors.Is(err, refused) {
		t.Fatalf("the callback's error came back as %v", err)
	}

	for _, instrument := range []string{
		telemetry.MetricLockTimeouts, telemetry.MetricVersionConflicts,
	} {
		if got := sumOf(t, reader, instrument,
			map[string]string{"transaction": telemetry.Movement}); got != 0 {
			t.Errorf("%s counted %d for a failure that was not contention", instrument, got)
		}
	}
}

// countedTelemetry is a telemetry whose measurements are kept in memory.
func countedTelemetry(t *testing.T) (*telemetry.Telemetry, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	reporting, err := telemetry.New(telemetry.Config{
		MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	})
	if err != nil {
		t.Fatalf("build the telemetry: %v", err)
	}
	return reporting, reader
}

// tracedManager is this world's manager, reporting through the telemetry given.
func tracedManager(t *testing.T, w *world, reporting *telemetry.Telemetry) *TxManager {
	t.Helper()
	manager, err := NewTxManager(TxConfig{
		Pool:             w.app,
		LockTimeout:      testLockTimeout,
		StatementTimeout: testStatementTimeout,
		Telemetry:        reporting,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}
	return manager
}

// sumOf is what one counter holds under exactly these attributes, and nought
// when no point carries them.
func sumOf(
	t *testing.T, reader *sdkmetric.ManualReader, name string, want map[string]string,
) int64 {
	t.Helper()
	var into metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &into); err != nil {
		t.Fatalf("collect the measurements: %v", err)
	}
	for _, scope := range into.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is a %T, wanted an int64 sum", name, m.Data)
			}
			for _, point := range sum.DataPoints {
				attributes := point.Attributes.ToSlice()
				if len(attributes) != len(want) {
					continue
				}
				matched := true
				for _, attr := range attributes {
					if value, named := want[string(attr.Key)]; !named ||
						attr.Value.String() != value {
						matched = false
					}
				}
				if matched {
					return point.Value
				}
			}
		}
	}
	return 0
}

// childDatabase is how the child process of
// TestAMovementKilledBeforeTheCommitLeavesNothing is told which database to run
// its one movement against. Set, it means "you are the child".
const childDatabase = "POSTGRES_TEST_CHILD_DATABASE"

// TestAMovementKilledBeforeTheCommitLeavesNothing proves that the fault point
// in [TxManager.WithinMovement] is reached, and where.
//
// A fault point ends the process it fires in, so the firing path cannot be
// tested in the process running the assertions. The test binary re-executes
// itself with this one test selected, pointed at THIS test's database, on this
// test's cluster, and armed with [faults.BeforeCommit]. The child opens a
// funded wallet through the real manager — a wallet row, an opening
// transaction, a ledger entry and two outbox rows, which is every table a
// movement writes — and dies at the point. The parent then reads the exit
// status, which is the whole of what the recovery scenario in internal/multi
// reads, and the four tables, which should be empty: every statement ran and
// none of them committed.
//
// The second half runs the same child unarmed. It commits, and the same four
// tables hold what the movement wrote — which is what shows the emptiness
// above is the kill's doing and not the child's failing to reach the commit
// for some other reason.
func TestAMovementKilledBeforeTheCommitLeavesNothing(t *testing.T) {
	if dsn := os.Getenv(childDatabase); dsn != "" {
		openWalletInChild(t, dsn)
		return
	}
	t.Parallel()
	w := newWorld(t)

	tables := []string{"wallet", "wager_transaction", "wallet_ledger_entry", "outbox"}

	t.Run("armed, it dies having committed nothing", func(t *testing.T) {
		code, stderr := runMovementChild(t, w, faults.BeforeCommit)
		if code != faults.ExitCode {
			t.Fatalf("the child exited %d, want %d: the fault point was not reached\n%s",
				code, faults.ExitCode, stderr)
		}
		if want := "FAULT_POINT=" + faults.BeforeCommit + " fired"; !strings.Contains(stderr, want) {
			t.Fatalf("the child exited %d but never said %q, so it died somewhere else\n%s",
				code, want, stderr)
		}
		for _, table := range tables {
			if got := w.count(t, `SELECT count(*) FROM wagering.`+table); got != 0 {
				t.Errorf("wagering.%s holds %d rows after a death before the commit, want 0",
					table, got)
			}
		}
	})

	t.Run("unarmed, the same movement commits", func(t *testing.T) {
		code, stderr := runMovementChild(t, w, "")
		if code != 0 {
			t.Fatalf("the child exited %d, want 0\n%s", code, stderr)
		}
		for table, want := range map[string]int{
			"wallet": 1, "wager_transaction": 1, "wallet_ledger_entry": 1, "outbox": 2,
		} {
			if got := w.count(t, `SELECT count(*) FROM wagering.`+table); got != want {
				t.Errorf("wagering.%s holds %d rows after the commit, want %d", table, got, want)
			}
		}
	})
}

// runMovementChild re-executes the test binary against this world's database
// with one fault point armed, and reports how it ended.
//
// TEST_DATABASE_URL is handed down so that the child's TestMain joins this
// process's cluster rather than starting a container of its own; the child
// creates no database and drops none — it is handed one.
func runMovementChild(t *testing.T, w *world, armed string) (int, string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestAMovementKilledBeforeTheCommitLeavesNothing$")
	cmd.Env = append(os.Environ(),
		"TEST_DATABASE_URL="+sharedDSN,
		childDatabase+"="+w.dsn,
		faults.Variable+"="+armed)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()

	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, output.String()
	case errors.As(err, &exit):
		return exit.ExitCode(), output.String()
	default:
		t.Fatalf("run the child: %v", err)
		return 0, ""
	}
}

// openWalletInChild is the one movement the child runs: a funded wallet, with
// its opening and the two events it emits, through the real manager as the
// application role. Armed, this function never returns.
func openWalletInChild(t *testing.T, dsn string) {
	t.Helper()
	pool := newAppPool(t, dsn, 2)
	tm, err := NewTxManager(TxConfig{
		Pool:             pool,
		LockTimeout:      testLockTimeout,
		StatementTimeout: testStatementTimeout,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}
	wallet, outcome := newWallet(t, "player-child", "100.00", "BRL")
	// The events are appended through the same helper every fixture uses; it
	// reads nothing off the world it hangs off, so an empty one serves.
	var fixtures world
	err = tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		if err := r.Open(ctx, wallet, outcome, "child"); err != nil {
			return err
		}
		return fixtures.append(ctx, r.Outbox, outcome.Events, at(0))
	})
	if err != nil {
		t.Fatalf("open a wallet in the child: %v", err)
	}
}
