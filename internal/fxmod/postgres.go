package fxmod

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// Postgres is the database this service's state lives in: the pool, the
// transaction manager every command runs inside, the publisher's claim surface,
// and the readiness probe.
//
// The invoke is what makes the probe exist in a binary that has no HTTP server
// to serve it from. Fx builds only what something asks for, so in cmd/worker
// nothing would reach [postgres.Health] and the start-up check this task
// requires would quietly not happen — a process that starts against a database
// it cannot read, and finds out at the first message.
func Postgres() fx.Option {
	return fx.Module("postgres",
		fx.Provide(
			newPool,
			newTxManager,
			newOutboxClaims,
			newDatabaseHealth,
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

// checkDatabase exists to make the probe above exist.
//
// Every binary here needs the database, and only one of them serves a readiness
// endpoint; without this the worker would build no probe and run no start-up
// check.
func checkDatabase(*postgres.Health) {}
