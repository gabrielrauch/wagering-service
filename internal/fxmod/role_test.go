//go:build integration

package fxmod

import (
	"maps"
	"net/url"
	"strings"
	"testing"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

// The two halves of the ledger check, against the real schema.
//
// The schema makes the ledger append-only twice over — a trigger refuses every
// UPDATE and DELETE, and wagering_app is granted neither — and the service
// holds the second half only by connecting as a member of that role, which one
// option in a DSN is all that arranges. A process that connected as the owner
// would start, serve and work identically. These two tests are the composition
// root noticing.

// TestTheServiceStartsAsTheApplicationRole is the check passing: the DSN this
// suite hands the composition root carries the role option, so the connection
// cannot rewrite the ledger and the process starts.
func TestTheServiceStartsAsTheApplicationRole(t *testing.T) {
	requireDatabase(t)

	// The reference loop alone: the lightest graph with a pool in it, and one
	// that resolves no queue, so what starts here is the database's checks and
	// nothing that could fail for a reason of its own.
	cfg := loaded(t, referenceOnly(nil))
	recorded := &hooks{}

	app := fxtest.New(t, Worker(cfg), fx.WithLogger(recorded.logger))
	app.RequireStart()
	app.RequireStop()

	// It ran, and it ran after the readiness ping — so a database that is not
	// answering is reported as that, rather than as a privilege query that
	// timed out.
	pinged := recorded.ranStart(t, "newDatabaseHealth")
	if guarded := recorded.ranStart(t, "newLedgerGuard"); guarded < pinged {
		t.Errorf("the ledger check ran before the readiness ping; the start-up was %v",
			recorded.startUp())
	}
	if failed := recorded.failures(); len(failed) != 0 {
		t.Fatalf("hooks reported failures: %v", failed)
	}
}

// TestTheServiceRefusesToStartOnAConnectionThatCanRewriteTheLedger is the
// check refusing: the same cluster and the same database, connected as the
// role that owns it — which is the DSN a deployment ends up with when the role
// option is lost in a copy.
func TestTheServiceRefusesToStartOnAConnectionThatCanRewriteTheLedger(t *testing.T) {
	requireDatabase(t)

	cfg := loaded(t, referenceOnly(map[string]string{
		"DATABASE_URL": asOwner(t, serviceDSN),
	}))
	err := startAndFail(t, Worker(cfg))
	for _, want := range []string{"can rewrite the ledger", "wagering_app"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal did not say %q: %v", want, err)
		}
	}
}

// referenceOnly is the environment for a worker running the reference loop
// and nothing else, overridden.
func referenceOnly(overrides map[string]string) map[string]string {
	env := map[string]string{
		"CONSUMER_ENABLED":         "false",
		"PUBLISHER_ENABLED":        "false",
		"REFERENCE_WORKER_ENABLED": "true",
	}
	maps.Copy(env, overrides)
	return env
}

// asOwner is the suite's DSN with the role option taken out, which is
// [asApplication] undone: the connection is then the owner's, and the owner
// can do anything.
func asOwner(t *testing.T, dsn string) string {
	t.Helper()

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse the suite's DSN: %v", err)
	}
	query := parsed.Query()
	if !query.Has("options") {
		t.Fatal("the suite's DSN carries no role option, so taking it out proves nothing")
	}
	query.Del("options")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
