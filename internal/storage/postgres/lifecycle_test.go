package postgres_test

import (
	"database/sql"
	"slices"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/storage/postgres"
)

// inventory is everything a migration can leave behind in the schema.
type inventory struct {
	relations []string
	domains   []string
	functions []string
	triggers  []string
	roles     []string
}

// TestMigrationsApplyRevertAndApplyAgain is the lifecycle the brief asks for.
//
// The second apply is the part that matters. A down migration that merely
// mostly reverses its up will still let the first apply succeed; only applying
// again, from whatever the revert actually left behind, shows whether the pair
// is a real inverse.
func TestMigrationsApplyRevertAndApplyAgain(t *testing.T) {
	t.Parallel()
	db, migrator := newDatabase(t)

	assertVersion(t, migrator, 0, "before anything is applied")

	if err := migrator.Up(t.Context()); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	applied := take(t, db)
	assertVersion(t, migrator, latestVersion, "after the first apply")
	assertSchemaIsComplete(t, applied)

	if err := migrator.Down(t.Context()); err != nil {
		t.Fatalf("revert: %v", err)
	}
	assertVersion(t, migrator, 0, "after reverting")
	assertSchemaIsClean(t, take(t, db))

	if err := migrator.Up(t.Context()); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	reapplied := take(t, db)
	assertVersion(t, migrator, latestVersion, "after the second apply")
	assertSchemaIsComplete(t, reapplied)

	// The two applies must produce the same schema, not merely a working one.
	for _, c := range []struct {
		what        string
		first, next []string
	}{
		{"relations", applied.relations, reapplied.relations},
		{"domains", applied.domains, reapplied.domains},
		{"functions", applied.functions, reapplied.functions},
		{"triggers", applied.triggers, reapplied.triggers},
		{"roles", applied.roles, reapplied.roles},
	} {
		if !slices.Equal(c.first, c.next) {
			t.Errorf("applying again produced different %s:\n first: %v\n again: %v", c.what, c.first, c.next)
		}
	}
}

// latestVersion is the highest migration. It is written down so that adding a
// migration without noticing is a failing test rather than a quiet change.
const latestVersion = 9

// assertSchemaIsComplete checks that a fully applied schema holds what the
// design says it holds.
func assertSchemaIsComplete(t *testing.T, got inventory) {
	t.Helper()

	want := []string{
		"active_reversal",
		"event_type",
		"failure_code",
		"inbox",
		"outbox",
		"outbox_aggregate_sequence",
		"outbox_sequence_seq",
		"schema_migrations",
		"settling_failure_code",
		"wager_transaction",
		"wallet",
		"wallet_ledger_entry",
	}
	if !slices.Equal(got.relations, want) {
		t.Errorf("the applied schema holds\n %v\nwanted\n %v", got.relations, want)
	}

	wantDomains := []string{"currency_code", "minor_amount", "opaque_id", "sha256_hex"}
	if !slices.Equal(got.domains, wantDomains) {
		t.Errorf("the applied schema defines domains %v, wanted %v", got.domains, wantDomains)
	}

	wantRoles := []string{"wagering_app", "wagering_migrator"}
	if !slices.Equal(got.roles, wantRoles) {
		t.Errorf("the applied schema has roles %v, wanted %v", got.roles, wantRoles)
	}
}

// assertSchemaIsClean checks that reverting left nothing behind.
//
// Two things survive, both deliberately. golang-migrate's bookkeeping table has
// to: a later apply reads it to know it is starting from nothing. The two roles
// do because they are cluster-wide, and a per-database revert has no business
// deciding that another database on the same cluster has finished with them —
// see the note in 000001_roles_and_domains.down.sql. Everything the migrations
// actually own — tables, sequences, domains, functions and triggers — is gone.
func assertSchemaIsClean(t *testing.T, got inventory) {
	t.Helper()

	if !slices.Equal(got.relations, []string{"schema_migrations"}) {
		t.Errorf("reverting left %v behind, wanted only schema_migrations", got.relations)
	}
	if !slices.Equal(got.roles, []string{"wagering_app", "wagering_migrator"}) {
		t.Errorf("reverting left roles %v, wanted both still standing", got.roles)
	}
	if len(got.domains) > 0 || len(got.functions) > 0 || len(got.triggers) > 0 {
		t.Errorf("reverting left domains %v, functions %v and triggers %v behind",
			got.domains, got.functions, got.triggers)
	}
}

func assertVersion(t *testing.T, migrator *postgres.Migrator, want uint, when string) {
	t.Helper()
	version, dirty, err := migrator.Version(t.Context())
	if err != nil {
		t.Fatalf("read the version %s: %v", when, err)
	}
	if dirty {
		t.Fatalf("the database is dirty %s", when)
	}
	if version != want {
		t.Errorf("the version is %d %s, wanted %d", version, when, want)
	}
}

// take reads everything the wagering schema currently holds.
func take(t *testing.T, db *sql.DB) inventory {
	t.Helper()
	return inventory{
		relations: names(t, db, `
			SELECT c.relname FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'wagering' AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
			ORDER BY 1`),
		domains: names(t, db, `
			SELECT t.typname FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE n.nspname = 'wagering' AND t.typtype = 'd'
			ORDER BY 1`),
		functions: names(t, db, `
			SELECT p.proname FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = 'wagering'
			ORDER BY 1`),
		triggers: names(t, db, `
			SELECT tg.tgname FROM pg_trigger tg
			JOIN pg_class c ON c.oid = tg.tgrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'wagering' AND NOT tg.tgisinternal
			ORDER BY 1`),
		roles: names(t, db, `
			SELECT rolname FROM pg_roles
			WHERE rolname IN ('wagering_app', 'wagering_migrator')
			ORDER BY 1`),
	}
}

// TestRevertingOneDatabaseLeavesTheClusterAlone is why the down migrations no
// longer drop the roles.
//
// Roles are cluster-wide and a migration is per-database, so a revert cannot
// know whether a sibling database is still using them — and PostgreSQL will not
// let it find out. DROP OWNED BY reaches only the current database, so DROP ROLE
// raised 2BP01 the moment another database still held a grant. That failure
// landed mid-migration and left the database being reverted recorded at version
// -1 and dirty, with its objects already gone: unrecoverable without a manual
// force, and caused entirely by a database it had nothing to do with.
//
// The lifecycle test used to be given a cluster of its own to keep this hidden.
func TestRevertingOneDatabaseLeavesTheClusterAlone(t *testing.T) {
	t.Parallel()
	if sharedErr != nil {
		t.Skipf("no PostgreSQL available: %v", sharedErr)
	}

	neighbour, neighbourMigrator := newDatabase(t)
	_, revertedMigrator := newDatabase(t)

	for _, m := range []*postgres.Migrator{neighbourMigrator, revertedMigrator} {
		if err := m.Up(t.Context()); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	if err := revertedMigrator.Down(t.Context()); err != nil {
		t.Fatalf("reverting one database of a shared cluster failed: %v", err)
	}
	assertVersion(t, revertedMigrator, 0, "after reverting one database of a shared cluster")

	// The neighbour is untouched: still at the latest version, still holding the
	// roles its grants are written against.
	assertVersion(t, neighbourMigrator, latestVersion, "in the database that was not reverted")
	assertSchemaIsComplete(t, take(t, neighbour))

	var holds int
	if err := neighbour.QueryRowContext(t.Context(), `
		SELECT count(*) FROM information_schema.table_privileges
		WHERE table_schema = 'wagering' AND grantee = 'wagering_app'`).Scan(&holds); err != nil {
		t.Fatalf("read the neighbour's grants: %v", err)
	}
	if holds == 0 {
		t.Error("the neighbour's grants went with the reverted database's roles")
	}

	// And the reverted one can be applied again, which a dirty -1 would refuse.
	if err := revertedMigrator.Up(t.Context()); err != nil {
		t.Fatalf("re-applying the reverted database failed: %v", err)
	}
	assertVersion(t, revertedMigrator, latestVersion, "after re-applying the reverted database")
}
