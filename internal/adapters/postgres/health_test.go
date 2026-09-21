//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// deadAddress names a port nothing listens on, so a connection attempt is
// refused rather than left hanging.
const deadAddress = "postgres://postgres:postgres@127.0.0.1:1/wagering?sslmode=disable"

// TestReadinessAnswersWhileTheDatabaseIsReachable is the ordinary case: a
// connection from the pool reaches a server that answers.
func TestReadinessAnswersWhileTheDatabaseIsReachable(t *testing.T) {
	t.Parallel()
	w := newWorld(t)

	health, err := NewHealth(w.app, time.Second)
	if err != nil {
		t.Fatalf("new readiness check: %v", err)
	}
	if err := health.Ready(t.Context()); err != nil {
		t.Fatalf("a reachable database reported %v, wanted ready", err)
	}
}

// TestReadinessFailsWhenTheDatabaseIsNotThere pins the answer an orchestrator
// acts on, and its class.
//
// Retryable rather than Unretryable: a database that is not answering now is
// the archetypal condition that passes, and reporting it as permanent would
// tell a caller to stop trying the one thing that will start working again.
func TestReadinessFailsWhenTheDatabaseIsNotThere(t *testing.T) {
	t.Parallel()
	// A port nothing listens on, built past NewPool because NewPool verifies a
	// connection and would refuse this at construction — which is its job, and
	// not the case under test.
	cfg, err := pgxpool.ParseConfig(deadAddress)
	if err != nil {
		t.Fatalf("parse the dead DSN: %v", err)
	}
	cfg.ConnConfig.ConnectTimeout = time.Second
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("open a pool on a dead address: %v", err)
	}
	t.Cleanup(pool.Close)

	health, err := NewHealth(pool, 5*time.Second)
	if err != nil {
		t.Fatalf("new readiness check: %v", err)
	}
	classifies(t, health.Ready(t.Context()), app.Retryable)
}

// TestReadinessGivesUpWithinItsTimeout is why the check carries a bound of its
// own rather than inheriting its caller's.
//
// The pool here holds one connection and that connection is busy, so the probe
// cannot even start. An unbounded check would wait for the pool, and a
// readiness endpoint that waits answers neither ready nor unready — which is the
// one answer nothing can act on.
func TestReadinessGivesUpWithinItsTimeout(t *testing.T) {
	t.Parallel()
	dsn := migrated(t)
	pool := newAppPool(t, dsn, 1)

	const timeout = 200 * time.Millisecond
	health, err := NewHealth(pool, timeout)
	if err != nil {
		t.Fatalf("new readiness check: %v", err)
	}

	busy := make(chan struct{})
	held := make(chan struct{})
	go func() {
		defer close(busy)
		conn, err := pool.Acquire(context.Background())
		if err != nil {
			return
		}
		defer conn.Release()
		close(held)
		_, _ = conn.Exec(context.Background(), `SELECT pg_sleep(3)`)
	}()
	<-held

	started := time.Now()
	err = health.Ready(t.Context())
	waited := time.Since(started)

	classifies(t, err, app.Retryable)
	// Bounded on both sides. The ceiling is what catches a check that stopped
	// applying its own timeout — a tenfold regression lands at two seconds, so a
	// two-second ceiling would have missed it. The floor is what catches the
	// opposite mistake: an answer that arrives instantly did not wait for the
	// pool and is reporting something other than the condition under test.
	if waited < timeout {
		t.Fatalf("gave up after %s, before its %s timeout: it did not wait for the pool",
			waited, timeout)
	}
	if waited > 5*timeout {
		t.Fatalf("gave up after %s, well past its %s timeout", waited, timeout)
	}
	<-busy
}
