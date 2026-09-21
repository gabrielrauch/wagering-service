//go:build integration

package postgres

import (
	"testing"
	"time"
)

// TestNewPoolFailsAtStartUpRatherThanAtTheFirstCommand is why the constructor
// verifies a connection instead of returning a pool that will find out later.
//
// pgxpool connects lazily, so a DSN naming a database nobody can reach builds a
// perfectly good pool and fails at the first command — by which time a provider
// is waiting and the failure looks like a wagering problem rather than a
// deployment one.
func TestNewPoolFailsAtStartUpRatherThanAtTheFirstCommand(t *testing.T) {
	t.Parallel()

	good := PoolConfig{DSN: migrated(t), MaxConns: 2, ConnectTimeout: 5 * time.Second}

	t.Run("a reachable database", func(t *testing.T) {
		t.Parallel()
		pool, err := NewPool(t.Context(), good)
		if err != nil {
			t.Fatalf("open a pool on a reachable database: %v", err)
		}
		t.Cleanup(pool.Close)
		if err := pool.Ping(t.Context()); err != nil {
			t.Fatalf("the pool does not work: %v", err)
		}
	})

	t.Run("an address nothing listens on", func(t *testing.T) {
		t.Parallel()
		dead := good
		dead.DSN = deadAddress
		dead.ConnectTimeout = time.Second
		if _, err := NewPool(t.Context(), dead); err == nil {
			t.Fatal("a pool was opened on an address nothing listens on")
		}
	})

	// The refusals below are at construction rather than at the first call, for
	// the same reason: a pool with no bound on its connections is a deployment
	// mistake, and the moment to report one is before anything depends on it.
	t.Run("a configuration that cannot work", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name   string
			change func(*PoolConfig)
		}{
			{name: "no DSN", change: func(c *PoolConfig) { c.DSN = "" }},
			{name: "no connection limit", change: func(c *PoolConfig) { c.MaxConns = 0 }},
			{name: "no connect timeout", change: func(c *PoolConfig) { c.ConnectTimeout = 0 }},
			{name: "an unparseable DSN", change: func(c *PoolConfig) { c.DSN = "not a dsn" }},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				cfg := good
				c.change(&cfg)
				pool, err := NewPool(t.Context(), cfg)
				if err == nil {
					pool.Close()
					t.Fatal("the pool was built from a configuration that cannot work")
				}
			})
		}
	})
}
