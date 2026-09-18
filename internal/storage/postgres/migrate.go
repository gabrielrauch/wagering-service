// Package postgres applies the wagering schema to a PostgreSQL database.
//
// It owns nothing but the migration lifecycle: apply, revert, and report where
// a database stands. There is no repository here and no knowledge of the
// domain — the schema those migrations install is what carries the domain's
// invariants, and it carries them as constraints rather than as code.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	pgxdriver "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	// Registers the "pgx" driver this package opens its connection with.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/gabrielrauch/wagering-service/migrations"
)

const (
	// Schema is the namespace every wagering object lives in. Keeping the
	// objects out of public means the migration role owns one namespace
	// outright, and that reverting every migration leaves one identifiable
	// place to inspect.
	Schema = "wagering"

	// VersionTable records which migrations have been applied. It lives inside
	// [Schema] alongside what it describes, and is the one relation that
	// survives a full revert.
	VersionTable = "schema_migrations"

	// sourceName and driverName label the source and database instances in
	// golang-migrate's error messages. Neither selects an implementation.
	sourceName = "iofs"
	driverName = "pgx5"
)

// createSchema is built from constants at compile time, so nothing about it is
// assembled from input. An identifier cannot be a placeholder in any case,
// which is why it is written this way rather than parameterised.
const createSchema = `CREATE SCHEMA IF NOT EXISTS ` + Schema

// Migrator applies and reverts the versioned schema.
//
// Its zero value is unusable; [NewMigrator] is the only way to build one. It
// does no work until a method is called, so constructing one touches no
// database.
//
// A Migrator owns its connection and must be closed. It is not safe to call
// [Migrator.Close] while another method is running, and a migrator whose run was
// cancelled is spent: golang-migrate latches the stop signal, so every later
// call would do nothing. Close it and build another.
type Migrator struct {
	db *sql.DB

	mu sync.Mutex
	m  *migrate.Migrate
	// Kept because it reports the version and the dirty flag together, which
	// the migrate instance above does not. See [Migrator.Version].
	driver database.Driver
}

// NewMigrator builds a migrator against the database that dsn names.
//
// The connection is opened here rather than borrowed, because golang-migrate's
// driver leases a connection for its whole lifetime and its Close returns that
// lease by closing the pool underneath it. Handed a pool the caller was still
// using, the migrator would either leak a connection out of it for the life of
// the process or close a pool it does not own; owning one settles both.
func NewMigrator(dsn string) (*Migrator, error) {
	if dsn == "" {
		return nil, errors.New("postgres: a migrator needs a database URL")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open database: %w", err)
	}
	// One is enough: the migration driver leases a connection for its whole
	// lifetime, and nothing else in this package queries the pool.
	db.SetMaxOpenConns(1)
	return &Migrator{db: db}, nil
}

// Close releases the connection the migrator opened.
func (mg *Migrator) Close() error {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	if mg.m == nil {
		return mg.db.Close()
	}
	// This closes the pool as well as the leased connection; both are ours.
	sourceErr, dbErr := mg.m.Close()
	if err := errors.Join(sourceErr, dbErr); err != nil {
		return fmt.Errorf("postgres: close migrator: %w", err)
	}
	return nil
}

// Up applies every migration the database has not yet seen.
//
// A database already at the latest version is not an error: applying migrations
// is something a deployment does unconditionally, and "nothing to do" is a
// successful outcome rather than a condition every caller has to recognise.
func (mg *Migrator) Up(ctx context.Context) error {
	return mg.run(ctx, "apply migrations", (*migrate.Migrate).Up)
}

// Down reverts every applied migration, newest first.
//
// It leaves [Schema] in place holding [VersionTable], because that table is
// what a later [Migrator.Up] reads to know it is starting from nothing.
func (mg *Migrator) Down(ctx context.Context) error {
	return mg.run(ctx, "revert migrations", (*migrate.Migrate).Down)
}

// Steps applies n migrations forward, or reverts -n of them.
func (mg *Migrator) Steps(ctx context.Context, n int) error {
	return mg.run(ctx, fmt.Sprintf("step %d migrations", n),
		func(m *migrate.Migrate) error { return m.Steps(n) })
}

// run carries out one migration operation, stopping it if ctx is cancelled.
//
// golang-migrate takes no context, so cancellation goes through its stop
// signal, which it checks between versions. That is the useful place to stop:
// the version in flight finishes and commits, and the database is left clean at
// a boundary rather than half applied and dirty.
func (mg *Migrator) run(ctx context.Context, what string, op func(*migrate.Migrate) error) error {
	m, _, err := mg.instance(ctx)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- op(m) }()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("postgres: %s: %w", what, err)
		}
		return nil
	case <-ctx.Done():
		m.GracefulStop <- true
		<-done
		return fmt.Errorf("postgres: %s: %w", what, ctx.Err())
	}
}

// Version reports the applied version and whether the database is dirty.
//
// A database with nothing applied reports version 0 rather than an error: "no
// migrations yet" is a position on the same scale as every other version, not a
// failure to read one.
//
// Dirty means a migration failed part way and the database is in neither the
// version before nor the version after. Nothing here fixes that; an operator
// must decide what the half-applied version left behind.
func (mg *Migrator) Version(ctx context.Context) (version uint, dirty bool, err error) {
	// The driver is read rather than the migrate instance wrapping it, because
	// [migrate.Migrate.Version] answers the no-version case with
	// (0, false, ErrNilVersion) — discarding the dirty flag it has just read. A
	// revert that stopped part way lands exactly there, at version -1 and dirty,
	// so that false would report the most broken state a database can be in as a
	// clean, empty one. The driver underneath returns both, and is the seam this
	// package already owns.
	_, driver, err := mg.instance(ctx)
	if err != nil {
		return 0, false, err
	}
	applied, dirty, err := driver.Version()
	if err != nil {
		return 0, false, fmt.Errorf("postgres: read schema version: %w", err)
	}
	if applied == database.NilVersion {
		return 0, dirty, nil
	}
	return uint(applied), dirty, nil
}

// instance builds the underlying migrate instance once, creating the schema
// first.
//
// The schema has to exist before golang-migrate is handed the database, because
// the first thing it does is write [VersionTable] into it. IF NOT EXISTS rather
// than a check, so that two deployments racing to migrate the same database
// both succeed.
//
// A failure is not remembered: it is almost always the database being
// unreachable, and a migrator that had refused once would go on refusing after
// the database came back.
func (mg *Migrator) instance(ctx context.Context) (*migrate.Migrate, database.Driver, error) {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	if mg.m != nil {
		return mg.m, mg.driver, nil
	}
	if _, err := mg.db.ExecContext(ctx, createSchema); err != nil {
		return nil, nil, fmt.Errorf("postgres: create schema %s: %w", Schema, err)
	}
	driver, err := pgxdriver.WithInstance(mg.db, &pgxdriver.Config{
		SchemaName:      Schema,
		MigrationsTable: VersionTable,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: open migration driver: %w", err)
	}
	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: read embedded migrations: %w", err)
	}
	m, err := migrate.NewWithInstance(sourceName, source, driverName, driver)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: build migrator: %w", err)
	}
	mg.m, mg.driver = m, driver
	return mg.m, mg.driver, nil
}
