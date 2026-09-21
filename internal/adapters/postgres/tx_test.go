//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
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

	read := func(t *testing.T, options pgx.TxOptions) (isolation, readOnly, lock, statement string) {
		t.Helper()
		err := w.tm.within(t.Context(), options, func(ctx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(ctx, settings).Scan(&isolation, &readOnly, &lock, &statement)
		})
		if err != nil {
			t.Fatalf("read the transaction's settings: %v", err)
		}
		return isolation, readOnly, lock, statement
	}

	t.Run("a movement", func(t *testing.T) {
		isolation, readOnly, lock, statement := read(t, movementOptions)
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
		isolation, readOnly, lock, statement := read(t, snapshotOptions)
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

	err := w.tm.within(t.Context(), snapshotOptions, func(ctx context.Context, tx pgx.Tx) error {
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
