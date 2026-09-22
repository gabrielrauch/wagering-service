package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// asApp returns a connection acting as wagering_app.
//
// SET ROLE rather than a second login user: it drops the session to that role's
// privileges for real, so what these tests observe is what the service will
// observe, without putting a password anywhere.
func asApp(t *testing.T, db *sql.DB) appConn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("take a connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(context.Background(), `SET ROLE wagering_app`); err != nil {
		t.Fatalf("become wagering_app: %v", err)
	}
	return appConn{conn}
}

// appConn adapts a connection to the execer the assertions take.
type appConn struct{ conn *sql.Conn }

func (c appConn) Exec(query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(context.Background(), query, args...)
}

// inTx runs body in one transaction on the same connection, so that work the
// application does with a deferred constraint over it still commits as
// wagering_app rather than as whoever opened the pool.
func (c appConn) inTx(t *testing.T, body func(tx *sql.Tx) error) error {
	t.Helper()
	tx, err := c.conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin as the application: %v", err)
	}
	if err := body(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// TestApplicationCannotRewriteTheLedger is the privilege half of append-only.
// The trigger says nobody may; this says the application may not even attempt
// it, so the refusal arrives before any row is examined.
func TestApplicationCannotRewriteTheLedger(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)
	bet := processedBet(t, db, w)

	e := debit(w, bet, 2500, 10000, 2, base)
	e.settles(t, db)

	app := asApp(t, db)

	t.Run("reading", func(t *testing.T) {
		accepts(t, app, `SELECT 1 FROM wagering.wallet_ledger_entry WHERE id = $1`, e.id)
	})

	t.Run("appending", func(t *testing.T) {
		other := processedBet(t, db, w)
		next := debit(w, other, 1000, 7500, 3, base)
		// Appended with the balance change it explains, which is the only shape
		// the deferred pairing accepts and the only shape the service writes.
		err := app.inTx(t, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(t.Context(), insertLedgerEntry, next.args()...); err != nil {
				return err
			}
			_, err := tx.ExecContext(t.Context(), moveWallet, w.id, int64(6500), int64(3), settleAt(int64(3)))
			return err
		})
		if err != nil {
			t.Errorf("the application could not append to the ledger: %v", err)
		}
	})

	t.Run("updating", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`UPDATE wagering.wallet_ledger_entry SET amount_minor = 1 WHERE id = $1`, e.id)
	})

	t.Run("deleting", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`DELETE FROM wagering.wallet_ledger_entry WHERE id = $1`, e.id)
	})

	t.Run("truncating", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege, `TRUNCATE wagering.wallet_ledger_entry`)
	})
}

// TestApplicationCannotWriteDerivedState: active_reversal is maintained by its
// trigger and by nothing else. A derived table the application can write to is
// not derived.
func TestApplicationCannotWriteDerivedState(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)
	bet := processedBet(t, db, w)
	reversalOf(w, "REFUND", bet).accepts(t, db)

	app := asApp(t, db)

	t.Run("reading", func(t *testing.T) {
		accepts(t, app, `SELECT 1 FROM wagering.active_reversal WHERE reference_id = $1`, bet.id)
	})

	t.Run("releasing a reference by hand", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`DELETE FROM wagering.active_reversal WHERE reference_id = $1`, bet.id)
	})

	t.Run("claiming a reference by hand", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`INSERT INTO wagering.active_reversal (reference_id, reversal_id) VALUES ($1, $2)`,
			wagering.NewTransactionID().String(), wagering.NewTransactionID().String())
	})
}

// TestApplicationCannotRewriteAnEvent: the application holds UPDATE on the
// outbox because publishing is an update, and that privilege must not reach the
// event. It is a trigger and not a column privilege that says so, because a
// column-level grant would have to be restated every time a publisher column
// was added — and because the owner is held to it too.
func TestApplicationCannotRewriteAnEvent(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)
	app := asApp(t, db)

	id := wagering.NewTransactionID().String()
	accepts(t, app, insertEvent, id, w.id, "WagerTransactionProcessed", `{"walletId":"`+w.id+`"}`, base, base)

	t.Run("rewriting the payload", func(t *testing.T) {
		refusesRule(t, app, "outbox_payload_is_a_snapshot",
			`UPDATE wagering.outbox SET payload = '{}' WHERE event_id = $1`, id)
	})

	t.Run("rewriting the event type", func(t *testing.T) {
		refusesRule(t, app, "outbox_payload_is_a_snapshot",
			`UPDATE wagering.outbox SET event_type = 'WalletBalanceChanged' WHERE event_id = $1`, id)
	})

	t.Run("claiming it", func(t *testing.T) {
		at := base.Add(time.Second)
		accepts(t, app, claimDue, "publisher-1", at, at.Add(time.Minute), 10)
	})

	t.Run("marking it published", func(t *testing.T) {
		accepts(t, app, `
			UPDATE wagering.outbox SET published_at = $2,
				claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL
			WHERE event_id = $1`, id, base.Add(2*time.Second))
	})
}

// TestApplicationCannotRewriteTheCatalogues: the closed sets belong to the
// migrations, so a code or an event type appears by deployment and never at
// runtime.
func TestApplicationCannotRewriteTheCatalogues(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	app := asApp(t, db)

	for _, table := range []string{"failure_code", "settling_failure_code", "event_type"} {
		t.Run(table, func(t *testing.T) {
			accepts(t, app, `SELECT 1 FROM wagering.`+table+` LIMIT 1`)
			refuses(t, app, insufficientPrivilege,
				`DELETE FROM wagering.`+table)
		})
	}

	t.Run("adding a code at runtime", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`INSERT INTO wagering.failure_code (code, correctable, audit, description)
			 VALUES ('MADE_UP', false, false, 'nope')`)
	})
}

// TestApplicationHasNoDDL: the service can write rows and nothing else. Schema
// changes arrive through migrations, run by the other role.
func TestApplicationHasNoDDL(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	app := asApp(t, db)

	t.Run("creating a table", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege, `CREATE TABLE wagering.scratch (id int)`)
	})

	t.Run("dropping a constraint", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`ALTER TABLE wagering.wallet DROP CONSTRAINT wallet_balance_is_never_negative`)
	})
}

// TestApplicationCanDoItsWork is the other side of the same question: the
// privileges above must not have taken away anything the service needs.
func TestApplicationCanDoItsWork(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	app := asApp(t, db)

	w := wallet{
		id:       wagering.NewWalletID().String(),
		playerID: "player-" + wagering.NewWalletID().String(),
		currency: "BRL",
	}

	t.Run("opening a wallet", func(t *testing.T) {
		accepts(t, app, insertWallet, w.id, w.playerID, w.currency, 0, 1, base, base)
	})

	t.Run("recording an operation", func(t *testing.T) {
		loss := externalTx(w, "LOSS")
		loss.amount = int64(0)
		loss.accepts(t, app)
	})

	t.Run("emitting an event", func(t *testing.T) {
		accepts(t, app, insertEvent, wagering.NewTransactionID().String(), w.id,
			"WagerTransactionProcessed", `{"walletId":"`+w.id+`"}`, base, base)
	})

	t.Run("claiming and publishing it", func(t *testing.T) {
		accepts(t, app, `
			UPDATE wagering.outbox SET published_at = $2 WHERE aggregate_id = $1`,
			w.id, base.Add(time.Second))
	})

	t.Run("recording a handled message", func(t *testing.T) {
		message := "sqs-" + wagering.NewTransactionID().String()
		accepts(t, app, insertInboxMessage, "worker", message, hashOf(message), base, nil)
	})

	t.Run("pruning what it has published", func(t *testing.T) {
		accepts(t, app, `DELETE FROM wagering.outbox WHERE published_at IS NOT NULL`)
	})
}

// TestApplicationCannotDriveTheDefinerFunctions closes the back door into the
// derived tables.
//
// A SECURITY DEFINER function is created EXECUTE-to-PUBLIC, because that is what
// an empty proacl means, and both maintainers were left that way. The front door
// was locked — the application holds SELECT on active_reversal and nothing at
// all on the sequence counter — but any role may create a temporary table
// without a privilege, and attaching one of these functions to it as a trigger
// runs its DELETE and INSERT as the role that owns the derived table, against a
// NEW row of the caller's choosing. Holds could be written, and released.
func TestApplicationCannotDriveTheDefinerFunctions(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	app := asApp(t, db)

	accepts(t, app, `CREATE TEMP TABLE hijack (
		id uuid, kind text, resolved_reference_id uuid,
		aggregate_id uuid, aggregate_sequence bigint)`)

	// pgx sends one statement per Exec, so each attempt is asserted on its own.
	t.Run("the active reversal maintainer", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`CREATE TRIGGER stolen BEFORE INSERT ON hijack FOR EACH ROW `+
				`EXECUTE FUNCTION wagering.maintain_active_reversal()`)
	})

	t.Run("the outbox sequence assigner", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege,
			`CREATE TRIGGER stolen BEFORE INSERT ON hijack FOR EACH ROW `+
				`EXECUTE FUNCTION wagering.outbox_assign_sequence()`)
	})

	// Calling one directly is refused for the same reason, before PostgreSQL
	// gets as far as objecting that it is a trigger function.
	t.Run("calling the maintainer outright", func(t *testing.T) {
		refuses(t, app, insufficientPrivilege, `SELECT wagering.maintain_active_reversal()`)
	})

	// The one function the service is meant to run still runs.
	t.Run("reconciliation is still granted", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		accepts(t, app, `SELECT wagering.reconcile_wallet($1)`, w.id)
	})
}
