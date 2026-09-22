// The constructors' refusals, which need no database and so carry no build tag.
//
// They run in the fast suite deliberately. House style makes "a constructor
// refuses a dependency it cannot work without, at construction rather than at
// the first call" a rule of the tree, and a rule nothing checks is a rule that
// drifts — the more so here, where every one of these branches exists to turn a
// deployment mistake into a start-up failure instead of a wallet lock held for
// as long as the network allows.
//
// Nothing below connects. pgxpool builds a pool lazily, so a configuration
// pointing at nothing is still a usable non-nil pool for the purpose of asking
// whether a constructor accepts one.

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// unusedPool is a real pool that is never connected through, for the happy-path
// halves of the tables below.
func unusedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	// A port nothing listens on. The pool is never connected through, so the
	// address only has to parse.
	cfg, err := pgxpool.ParseConfig(
		"postgres://postgres:postgres@127.0.0.1:1/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("parse a DSN: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("build a pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestNewTxManagerRefusesWhatItCannotWorkWith(t *testing.T) {
	t.Parallel()
	pool := unusedPool(t)
	good := TxConfig{Pool: pool, LockTimeout: time.Second, StatementTimeout: 10 * time.Second}

	cases := []struct {
		name   string
		change func(*TxConfig)
		wantOK bool
	}{
		{name: "everything it needs", change: func(*TxConfig) {}, wantOK: true},
		{name: "no pool", change: func(c *TxConfig) { c.Pool = nil }},
		{name: "no lock timeout", change: func(c *TxConfig) { c.LockTimeout = 0 }},
		{name: "a negative lock timeout", change: func(c *TxConfig) { c.LockTimeout = -time.Second }},
		{name: "no statement timeout", change: func(c *TxConfig) { c.StatementTimeout = 0 }},
		{
			name:   "a negative statement timeout",
			change: func(c *TxConfig) { c.StatementTimeout = -time.Second },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg := good
			c.change(&cfg)
			tm, err := NewTxManager(cfg)
			switch {
			case c.wantOK && err != nil:
				t.Fatalf("refused a usable configuration: %v", err)
			case c.wantOK && tm == nil:
				t.Fatal("accepted a configuration and returned no manager")
			case !c.wantOK && err == nil:
				t.Fatal("accepted a configuration it cannot work with")
			}
		})
	}
}

// TestATimeoutIsNeverRenderedAsDisabled is the reason milliseconds rounds up.
//
// Zero is not "immediately" to PostgreSQL, it is DISABLED. A sub-millisecond
// timeout is a strange thing to configure and a perfectly acceptable one to
// accept, but rendering it as nothing at all would silently produce the exact
// condition NewTxManager refuses a missing timeout to prevent.
func TestATimeoutIsNeverRenderedAsDisabled(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		of   time.Duration
		want string
	}{
		{name: "a whole millisecond", of: time.Millisecond, want: "1"},
		{name: "a round duration", of: 750 * time.Millisecond, want: "750"},
		{name: "seconds", of: 2 * time.Second, want: "2000"},
		{
			name: "half a millisecond rounds up rather than to nothing",
			of:   500 * time.Microsecond, want: "1",
		},
		{name: "a single nanosecond still bounds something", of: time.Nanosecond, want: "1"},
		{name: "a fraction is never truncated downwards", of: 1500 * time.Microsecond, want: "2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := milliseconds(c.of); got != c.want {
				t.Fatalf("%s rendered as %q, wanted %q", c.of, got, c.want)
			}
			if got := milliseconds(c.of); got == "0" {
				t.Fatalf("%s rendered as a disabled timeout", c.of)
			}
		})
	}
}

func TestNewHealthRefusesWhatItCannotWorkWith(t *testing.T) {
	t.Parallel()
	pool := unusedPool(t)

	cases := []struct {
		name    string
		pool    *pgxpool.Pool
		timeout time.Duration
		wantOK  bool
	}{
		{name: "everything it needs", pool: pool, timeout: time.Second, wantOK: true},
		{name: "no pool", timeout: time.Second},
		{name: "no timeout", pool: pool},
		{name: "a negative timeout", pool: pool, timeout: -time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			health, err := NewHealth(c.pool, c.timeout)
			switch {
			case c.wantOK && err != nil:
				t.Fatalf("refused a usable configuration: %v", err)
			case c.wantOK && health == nil:
				t.Fatal("accepted a configuration and returned no check")
			case !c.wantOK && err == nil:
				t.Fatal("accepted a configuration it cannot work with")
			}
		})
	}
}

func TestNewOutboxClaimsRefusesNoPool(t *testing.T) {
	t.Parallel()

	if _, err := NewOutboxClaims(nil); err == nil {
		t.Fatal("built outbox claims with no pool")
	}
	claims, err := NewOutboxClaims(unusedPool(t))
	if err != nil {
		t.Fatalf("refused a usable pool: %v", err)
	}
	if claims == nil {
		t.Fatal("accepted a pool and returned nothing")
	}
}

// TestAClaimRequestIsRefusedBeforeItReachesTheDatabase covers the branches that
// keep a publisher from asking for something the outbox cannot mean: a claim
// nobody holds, an unbounded batch, or a hold that expires before it is taken.
func TestAClaimRequestIsRefusedBeforeItReachesTheDatabase(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	good := ClaimRequest{By: "publisher-a", Limit: 10, At: now, Hold: time.Minute}

	cases := []struct {
		name   string
		change func(*ClaimRequest)
		wantOK bool
	}{
		{name: "everything it needs", change: func(*ClaimRequest) {}, wantOK: true},
		{name: "no publisher", change: func(r *ClaimRequest) { r.By = "" }},
		{name: "no batch", change: func(r *ClaimRequest) { r.Limit = 0 }},
		{name: "a negative batch", change: func(r *ClaimRequest) { r.Limit = -1 }},
		{name: "no instant", change: func(r *ClaimRequest) { r.At = time.Time{} }},
		{name: "no hold", change: func(r *ClaimRequest) { r.Hold = 0 }},
		{name: "a hold that runs backwards", change: func(r *ClaimRequest) { r.Hold = -time.Minute }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			request := good
			c.change(&request)
			err := request.validate()
			if c.wantOK != (err == nil) {
				t.Fatalf("validate reported %v, wanted ok=%t", err, c.wantOK)
			}
		})
	}
}
