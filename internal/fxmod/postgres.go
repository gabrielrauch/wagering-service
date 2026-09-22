package fxmod

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// Postgres is the database this service's state lives in: the pool, the
// transaction manager every command runs inside, the publisher's claim surface,
// the readiness probe, and the check that the connection cannot rewrite the
// ledger.
//
// The invoke is what makes the probe and the check exist in a binary that has
// no HTTP server to serve either from. Fx builds only what something asks for,
// so in cmd/worker nothing would reach [postgres.Health] and the start-up
// checks this task requires would quietly not happen — a process that starts
// against a database it cannot read, and finds out at the first message.
func Postgres() fx.Option {
	return fx.Module("postgres",
		fx.Provide(
			newPool,
			newTxManager,
			newOutboxClaims,
			newDatabaseHealth,
			newLedgerGuard,
		),
		fx.Invoke(checkDatabase),
	)
}

// newPool opens the connection pool and arranges for it to be closed last.
//
// The context is this constructor's own rather than the lifecycle's, because
// Fx runs constructors while the app is being built and there is no start
// context yet. It is bounded by the configured connect timeout, which is the
// bound that matters: a DSN naming a host that accepts the connection and then
// says nothing must not hold a deployment open.
//
// The close hook is appended here, where the pool is built, and that placement
// is the shutdown ordering. Everything that uses the pool is constructed from
// it, so every one of their hooks is appended after this one — and Fx runs
// OnStop in reverse, so this one runs after all of theirs. The failure it
// prevents is concrete: a publisher hands back its outbox claims from inside
// its own Stop, and a pool closed first turns that into a wallet's whole event
// stream waiting out a claim hold.
func newPool(
	lc fx.Lifecycle, cfg config.Postgres, reporting *telemetry.Telemetry, logger *slog.Logger,
) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	pool, err := postgres.NewPool(ctx, postgres.PoolConfig{
		DSN:            cfg.DSN,
		MaxConns:       cfg.MaxConns,
		ConnectTimeout: cfg.ConnectTimeout,
		Telemetry:      reporting,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: (&pools{pool: pool, logger: logger}).close})
	return pool, nil
}

// pools is the pool together with the logger that reports it closing.
type pools struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// close returns the connections.
//
// It cannot fail and does not pretend it might: pgxpool.Close waits for every
// connection to be returned and has nothing to report. The line is written
// because the ordering it proves — that this ran after every component that
// holds a transaction — is the thing worth seeing in a shutdown.
func (p *pools) close(ctx context.Context) error {
	p.pool.Close()
	p.logger.InfoContext(ctx, "closed the database pool")
	return nil
}

// newTxManager wires the transaction manager the application layer opens every
// command inside.
func newTxManager(
	pool *pgxpool.Pool, cfg config.Postgres, reporting *telemetry.Telemetry,
) (*postgres.TxManager, error) {
	return postgres.NewTxManager(postgres.TxConfig{
		Pool:             pool,
		LockTimeout:      cfg.LockTimeout,
		StatementTimeout: cfg.StatementTimeout,
		Telemetry:        reporting,
	})
}

// newOutboxClaims wires the publisher's side of the outbox.
//
// It takes the pool rather than the transaction manager, and that is the
// adapter's design rather than an oversight here: each claim call is one
// self-standing statement, and the application layer explicitly does not
// publish, so this is not one of its ports.
func newOutboxClaims(pool *pgxpool.Pool) (*postgres.OutboxClaims, error) {
	return postgres.NewOutboxClaims(pool)
}

// newDatabaseHealth wires the readiness probe and makes it the start-up check.
//
// The same probe /health/ready runs, deliberately: a process that has reported
// itself started has already answered the question the orchestrator is about to
// ask, rather than answering an easier one. Ready applies the configured
// timeout itself, so the hook is bounded by its own budget and not only by the
// lifecycle's.
func newDatabaseHealth(
	lc fx.Lifecycle, pool *pgxpool.Pool, cfg config.Postgres,
) (*postgres.Health, error) {
	health, err := postgres.NewHealth(pool, cfg.HealthTimeout)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStart: health.Ready})
	return health, nil
}

// ledgerPrivilegeProbe asks whether the role this process connected as could
// rewrite the ledger.
//
// It asks about the CURRENT role, which is what the DSN's
// `options=-c role=wagering_app` sets — so a DSN that lost the option answers
// for the owner, and the owner can do anything. It reads the catalogue and not
// the table, so it takes no lock a migration would contend with; and it runs
// once, at start-up, rather than on every readiness probe, because the answer
// cannot change while the connection is the same one.
//
// The first column says whether the ledger exists at all. has_table_privilege
// raises on a relation that does not, and a process started before the
// migration job would then be told about a privilege query when the fault is
// an unmigrated database; the CASE keeps the privilege call from running in
// that case, and the caller names the right fault.
const ledgerPrivilegeProbe = `SELECT
	to_regclass('wagering.wallet_ledger_entry') IS NOT NULL,
	CASE WHEN to_regclass('wagering.wallet_ledger_entry') IS NULL THEN false
	     ELSE has_table_privilege('wagering.wallet_ledger_entry', 'UPDATE')
	       OR has_table_privilege('wagering.wallet_ledger_entry', 'DELETE') END`

// ledgerGuard is the start-up check that this process cannot rewrite the
// ledger.
//
// The schema makes the ledger append-only twice over: a trigger refuses every
// UPDATE and DELETE, and wagering_app is granted neither. The service holds the
// second half only by connecting as a member of that role, and the only thing
// that arranges it is one option in a DSN — exactly the kind of thing a
// deployment loses in a copy. A process that connected as the owner would
// start, serve and work identically, with half of what makes the ledger
// append-only quietly gone and nothing to say so. This is the composition root
// asking, once, before anything else runs on the connection.
type ledgerGuard struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// newLedgerGuard wires the check, after the readiness ping.
//
// It takes the probe as a dependency for the ordering and for nothing else. Fx
// appends hooks in construction order, so this hook runs after
// [newDatabaseHealth]'s, and a database that is not answering is reported as
// that rather than as a privilege query that timed out. The bound is the same
// health timeout: this is one catalogue read on a connection the ping has just
// proven, and it should take no longer than the ping did.
func newLedgerGuard(
	lc fx.Lifecycle, pool *pgxpool.Pool, cfg config.Postgres, _ *postgres.Health,
) *ledgerGuard {
	guard := &ledgerGuard{pool: pool, timeout: cfg.HealthTimeout}
	lc.Append(fx.Hook{OnStart: guard.check})
	return guard
}

// check refuses to start a process whose connection could rewrite the ledger.
func (g *ledgerGuard) check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	var migrated, canRewrite bool
	if err := g.pool.QueryRow(ctx, ledgerPrivilegeProbe).Scan(&migrated, &canRewrite); err != nil {
		return fmt.Errorf("fxmod: check whether the connection can rewrite the ledger: %w", err)
	}
	if !migrated {
		return errors.New("fxmod: the schema is not migrated: wagering.wallet_ledger_entry does not exist; " +
			"run cmd/migrate before starting the service")
	}
	if canRewrite {
		return errors.New("fxmod: the database connection can rewrite the ledger; " +
			"connect as a member of wagering_app — DATABASE_URL should carry " +
			"options=-c role=wagering_app")
	}
	return nil
}

// checkDatabase exists to make the probe and the guard above exist.
//
// Every binary here needs the database, and only one of them serves a readiness
// endpoint; without this the worker would build no probe, no guard, and run no
// start-up check.
func checkDatabase(*postgres.Health, *ledgerGuard) {}
