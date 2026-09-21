package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// readinessProbe is the cheapest statement that proves a connection from the
// pool reaches a server that is answering.
//
// Deliberately not a query against the schema. A readiness probe that read a
// table would report the service unready whenever that table were locked by a
// migration, which is the opposite of what readiness is for: the question is
// whether this process can serve, and a probe that can be made to fail by
// somebody else's DDL answers a different one.
const readinessProbe = `SELECT 1`

// Health answers whether this process can reach the database.
type Health struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// NewHealth wires the readiness check.
//
// The timeout is required and bounds the probe itself. Without it a readiness
// endpoint inherits whatever deadline its caller happened to set, and an
// orchestrator that sets none would wait on a database that is not answering
// for as long as the connection stayed open — reporting neither ready nor
// unready, which is the one answer nothing can act on.
func NewHealth(pool *pgxpool.Pool, timeout time.Duration) (*Health, error) {
	switch {
	case pool == nil:
		return nil, errors.New("postgres: a readiness check needs a pool")
	case timeout <= 0:
		return nil, fmt.Errorf("postgres: a readiness check needs a positive timeout, got %s",
			timeout)
	}
	return &Health{pool: pool, timeout: timeout}, nil
}

// Ready reports whether the database is reachable, within the configured
// timeout.
//
// The bound is applied on top of the caller's context rather than instead of
// it, so a caller that is already giving up is not made to wait for this.
func (h *Health) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	var answer int
	if err := h.pool.QueryRow(ctx, readinessProbe).Scan(&answer); err != nil {
		return fail("check readiness", err)
	}
	return nil
}
