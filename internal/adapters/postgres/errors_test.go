//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestOneSQLSTATEIsTransientAtConnectTimeAndPermanentAfterwards is the whole
// argument for classifying a connect failure structurally rather than by code.
//
// 55000 is object_not_in_prerequisite_state. Raised by a statement it is
// permanent — the object is not in a state the statement needs and asking again
// will not change that. Raised by a CONNECTION ATTEMPT it is "this database is
// not currently accepting connections", which is a maintenance window and is
// exactly the kind of thing that ends.
//
// One code, opposite answers. No list of SQLSTATEs can tell them apart, and the
// version of this adapter that tried sent every in-flight wager to a dead-letter
// queue the first time a database was closed. What tells them apart is whether
// a connection was ever established, which is what a *pgconn.ConnectError in
// the chain says.
//
// Both halves are asserted together on purpose. Somebody "fixing" a future
// report by adding 55000 to transientCodes fails the first half; somebody
// removing the connect-error branch fails the second.
func TestOneSQLSTATEIsTransientAtConnectTimeAndPermanentAfterwards(t *testing.T) {
	t.Parallel()
	w := newWorld(t)

	t.Run("raised by a statement, it is permanent", func(t *testing.T) {
		// pg_wal_replay_resume on a server that is not recovering raises
		// 55000, and no amount of asking again will put the server into
		// recovery. It runs on the owner pool because recovery control is
		// restricted; which role provokes it is immaterial to the
		// classification being asserted.
		_, err := w.owner.Exec(t.Context(), `SELECT pg_wal_replay_resume()`)
		classified := fail("resume replay", err)

		classifies(t, classified, app.Unretryable)
		refusedWith(t, classified, pgerrcode.ObjectNotInPrerequisiteState)
	})

	t.Run("raised by a connection attempt, it is transient", func(t *testing.T) {
		// The command works before the window opens, so what fails below is
		// the closing and not the fixture.
		w.openWallet(t, "player-maintenance", "100.00", "BRL")

		w.setAllowConnections(t, false)
		t.Cleanup(func() { w.setAllowConnections(t, true) })

		// Existing connections keep working, which is why the delivery that
		// found this defect was deferred correctly on its first attempt and
		// misclassified only on the reconnect. Dropping the idle ones is what
		// forces the next command down the path that was wrong.
		w.app.Reset()

		err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			_, err := r.Wallets.ByID(ctx, wagering.NewWalletID())
			return err
		})

		classifies(t, err, app.Retryable)
		refusedConnection(t, err, pgerrcode.ObjectNotInPrerequisiteState)
	})
}

// TestEveryConnectionTheServerRefusesIsTransient pins the rule as a rule rather
// than as the one case that was reported.
//
// A connection the server refused carried no statement, so it recorded nothing,
// so it is Retryable — whatever the server refused with. The alternative is a
// list that has to be extended every time a server chooses a code nobody
// anticipated, which is how the defect this replaces arrived.
func TestEveryConnectionTheServerRefusesIsTransient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// alter turns a working DSN into one the server will refuse.
		alter func(*url.URL)
		state string
	}{
		{
			name:  "a database that does not exist",
			alter: func(u *url.URL) { u.Path = "/wagering_no_such_database" },
			state: pgerrcode.InvalidCatalogName,
		},
		{
			// The consequence of the rule, named rather than hidden. A wrong
			// password will never start being right, and this still classifies
			// Retryable — because what Unretryable means to the caller on the
			// queue path is "leave it for the redrive policy", and a
			// misconfigured secret in THIS service is not a defect in a
			// provider's wager. The redrive policy bounds the retrying either
			// way; this way an operator sees the same connection failure repeat
			// instead of a wager disappearing into a dead-letter queue.
			name: "a password the server rejects",
			alter: func(u *url.URL) {
				u.User = url.UserPassword(u.User.Username(), "not-the-password")
			},
			state: pgerrcode.InvalidPassword,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := url.Parse(sharedDSN)
			if err != nil {
				t.Fatalf("parse the shared dsn: %v", err)
			}
			c.alter(parsed)

			err = managerOn(t, parsed.String()).WithinMovement(t.Context(),
				func(ctx context.Context, r *app.Repos) error {
					_, err := r.Wallets.ByID(ctx, wagering.NewWalletID())
					return err
				})

			classifies(t, err, app.Retryable)
			refusedConnection(t, err, c.state)
		})
	}
}

// managerOn builds a transaction manager on a DSN without connecting to it.
//
// Not through NewPool, which verifies a connection and would report the refusal
// itself: the path under test is the one where a pool that looked fine opens a
// connection because a command needs one, which is where a maintenance window
// is met in production.
func managerOn(t *testing.T, dsn string) *TxManager {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", redact(dsn), err)
	}
	cfg.MaxConns = 2
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("build a pool on %s: %v", redact(dsn), err)
	}
	t.Cleanup(pool.Close)

	tm, err := NewTxManager(TxConfig{
		Pool:             pool,
		LockTimeout:      testLockTimeout,
		StatementTimeout: testStatementTimeout,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}
	return tm
}

// refusedConnection asserts that err is a refusal the server gave to a
// CONNECTION ATTEMPT, carrying the given SQLSTATE.
//
// The PgError is looked for inside the ConnectError rather than anywhere in the
// chain, because the shape is the point: the defect this guards was a SQLSTATE
// test that ran first, found exactly this inner error, answered from its code
// and returned, so the connect error was never reached. An assertion that only
// checked the class would pass on any retryable error at all, including one
// that never took this path.
func refusedConnection(t *testing.T, err error, state string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the connection was allowed but should have been refused with %s", state)
	}
	var connErr *pgconn.ConnectError
	if !errors.As(err, &connErr) {
		t.Fatalf("no connect error in the chain, so this was not a refused connection: %v", err)
	}
	pgErr, ok := errors.AsType[*pgconn.PgError](connErr)
	if !ok {
		t.Fatalf("the connect error carries no SQLSTATE, so it is not the wrapping case: %v", err)
	}
	if pgErr.Code != state {
		t.Fatalf("the server refused the connection with %s, wanted %s: %v",
			pgErr.Code, state, err)
	}
}
