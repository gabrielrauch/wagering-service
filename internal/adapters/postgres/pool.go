package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig is what a connection pool is built from.
//
// It is deliberately three fields. Everything else pgx can be told is either a
// property of the server, which belongs in the DSN where an operator can change
// it without a deployment, or a default that has never needed changing here.
type PoolConfig struct {
	// DSN names the database and the role the service connects as. That role is
	// a member of wagering_app, which holds no DDL and no UPDATE or DELETE on
	// the ledger — the privileges are half of what makes the ledger
	// append-only, and connecting as anything else quietly removes them.
	DSN string
	// MaxConns bounds the pool. It is required rather than defaulted because
	// the right number is a property of the deployment — how many replicas
	// share one cluster — and a library default would be a guess that holds
	// until the second replica starts.
	MaxConns int32
	// ConnectTimeout bounds opening a connection, so a server that accepts the
	// TCP connection and then says nothing cannot hold a command indefinitely.
	ConnectTimeout time.Duration
}

// NewPool opens the connection pool every other type here is built from.
//
// It verifies a connection before returning, so a DSN that names a database
// nobody can reach fails at start-up rather than at the first command — by
// which time a provider is waiting and the failure looks like a wagering
// problem.
func NewPool(ctx context.Context, cfg PoolConfig) (*pgxpool.Pool, error) {
	switch {
	case cfg.DSN == "":
		return nil, errors.New("postgres: a pool needs a DSN")
	case cfg.MaxConns <= 0:
		return nil, fmt.Errorf("postgres: a pool needs a positive connection limit, got %d",
			cfg.MaxConns)
	case cfg.ConnectTimeout <= 0:
		return nil, fmt.Errorf("postgres: a pool needs a positive connect timeout, got %s",
			cfg.ConnectTimeout)
	}
	parsed, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse the DSN: %w", err)
	}
	parsed.MaxConns = cfg.MaxConns
	parsed.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		return nil, fmt.Errorf("postgres: open the pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: reach the database: %w", err)
	}
	return pool, nil
}
