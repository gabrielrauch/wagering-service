package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestUpCreatesTheSchema is the first thing the migrator has to do: put the
// namespace every other object lives in onto a database that has none.
//
// The schema is created by Up rather than by the first migration, because
// golang-migrate writes its own bookkeeping table into that schema before it
// runs anything. It is the namespace the migrations live in, in the same way
// the database itself is, and neither is something a migration creates.
func TestUpCreatesTheSchema(t *testing.T) {
	t.Parallel()
	db, migrator := newDatabase(t)

	if schemaExists(t, db) {
		t.Fatal("a fresh database already carries the wagering schema")
	}
	if err := migrator.Up(t.Context()); err != nil {
		t.Fatalf("up: %v", err)
	}
	if !schemaExists(t, db) {
		t.Error("the wagering schema is absent after Up")
	}
}

// schemaExists reports whether the wagering schema is present.
func schemaExists(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(t.Context(), `SELECT to_regnamespace('wagering') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("look up the wagering schema: %v", err)
	}
	return exists
}

// TestVersionReportsDirtyWithoutAVersion is the state an operator is most likely
// to meet and was least likely to be told about.
//
// golang-migrate reports "no version" by returning (0, false, ErrNilVersion) —
// discarding the dirty flag it has just read. A revert that stopped part way
// lands exactly there, at version -1 and dirty, so taking that false at face
// value reported the most broken state the database can be in as a clean, empty
// one: `migrate version` printed 0 and exited successfully, while `migrate up`
// refused to run at all.
func TestVersionReportsDirtyWithoutAVersion(t *testing.T) {
	t.Parallel()
	db, migrator := newDatabase(t)

	if err := migrator.Up(t.Context()); err != nil {
		t.Fatalf("up: %v", err)
	}

	// Exactly what a down migration that failed part way leaves behind.
	if _, err := db.ExecContext(t.Context(), `UPDATE wagering.schema_migrations SET version = -1, dirty = true`); err != nil {
		t.Fatalf("record a half-reverted database: %v", err)
	}

	version, dirty, err := migrator.Version(t.Context())
	if err != nil {
		t.Fatalf("read the version: %v", err)
	}
	if !dirty {
		t.Error("a database left dirty at version -1 was reported clean")
	}
	if version != 0 {
		t.Errorf("the version is %d, wanted 0 alongside the dirty flag", version)
	}
}

// TestVersionReportsAnEmptyDatabaseAsClean is the case the dirty read must not
// turn into a false alarm: nothing applied and nothing recorded is version 0,
// not a half-finished migration.
func TestVersionReportsAnEmptyDatabaseAsClean(t *testing.T) {
	t.Parallel()
	_, migrator := newDatabase(t)

	version, dirty, err := migrator.Version(t.Context())
	if err != nil {
		t.Fatalf("read the version: %v", err)
	}
	if dirty || version != 0 {
		t.Errorf("an untouched database reports version %d dirty=%v, wanted 0 and clean", version, dirty)
	}
}

// TestMigratorReturnsItsConnections is the leak.
//
// golang-migrate's driver leases a connection for its whole lifetime and its
// Close returns that lease by closing the pool underneath it. Handed a pool the
// caller was still using, the migrator held one of its connections for the life
// of the process — enough, on a tightly sized pool, for a service to migrate at
// startup and then block on its first query. The migrator opens its own
// connection now, and gives it back.
func TestMigratorReturnsItsConnections(t *testing.T) {
	db, migrator := newDatabase(t)

	before := backends(t, db)
	if err := migrator.Up(t.Context()); err != nil {
		t.Fatalf("up: %v", err)
	}
	if during := backends(t, db); during <= before {
		t.Fatalf("the migrator ran on %d backends, no more than the %d already open — this proves nothing", during, before)
	}
	if err := migrator.Close(); err != nil {
		t.Fatalf("close the migrator: %v", err)
	}

	// Closing a pool disconnects in the background, so this is given a moment
	// to settle rather than read once and hope.
	deadline := time.Now().Add(5 * time.Second)
	for {
		after := backends(t, db)
		if after <= before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends are still open, wanted no more than the %d this test opened", after, before)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := migrator.Up(t.Context()); err == nil {
		t.Error("a closed migrator still applied migrations")
	}
}

// backends counts the connections open against this test's own database.
func backends(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), `
		SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()`).Scan(&n); err != nil {
		t.Fatalf("count backends: %v", err)
	}
	return n
}

// TestUpRespectsACancelledContext: the runner took no context at all, so a
// migration could not be called off and an interrupt killed the process
// mid-statement, which is how a database ends up dirty. Cancellation now reaches
// the driver, and a run that never started leaves nothing behind.
func TestUpRespectsACancelledContext(t *testing.T) {
	t.Parallel()
	_, migrator := newDatabase(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := migrator.Up(ctx)
	if err == nil {
		t.Fatal("a cancelled Up applied the schema anyway")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled Up failed with %v, wanted a context cancellation", err)
	}

	version, dirty, err := migrator.Version(t.Context())
	if err != nil {
		t.Fatalf("read the version after cancelling: %v", err)
	}
	if dirty || version != 0 {
		t.Errorf("a cancelled Up left version %d dirty=%v, wanted an untouched database", version, dirty)
	}
}
