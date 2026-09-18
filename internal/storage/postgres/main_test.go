package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Registers the "pgx" driver used by every test connection.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/gabrielrauch/wagering-service/internal/storage/postgres"
)

// image is the PostgreSQL the schema is written against. The brief names 16,
// and the schema uses nothing newer, so pinning it here means a test passing
// locally says something about the version that will run in production.
const image = "postgres:16-alpine"

// The tests share one cluster and take a database each. A database is cheap,
// gives each test its own tables, and lets them run in parallel; a container is
// neither. Reverting is safe on the shared cluster because the down migrations
// drop only what lives in one database: the two roles are cluster-wide and are
// deliberately left standing, which is what lets the lifecycle test run here
// alongside everything else.
var (
	sharedDSN string
	sharedErr error
	databases atomic.Uint64

	// admin issues the CREATE DATABASE statements, as one pool for the whole
	// package. Creating a database is a single statement, and opening a pool to
	// issue it costs an order of magnitude more than issuing it.
	admin *sql.DB
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns the shared cluster's lifetime. It is separate from TestMain because
// os.Exit skips deferred functions, and a leaked container outlives the test
// binary.
func run(m *testing.M) int {
	ctx := context.Background()
	container, dsn, err := startCluster(ctx)
	if err != nil {
		sharedErr = err
		return m.Run()
	}
	defer func() { _ = testcontainers.TerminateContainer(container) }()

	pool, err := sql.Open("pgx", dsn)
	if err != nil {
		sharedErr = fmt.Errorf("open the admin connection: %w", err)
		return m.Run()
	}
	defer func() { _ = pool.Close() }()
	// Capped because every parallel test asks this pool for a database at once,
	// and an uncapped pool would answer by opening a connection each — enough,
	// on a default cluster, to run the whole suite into max_connections.
	pool.SetMaxOpenConns(8)

	sharedDSN, admin = dsn, pool
	return m.Run()
}

// startCluster brings up a PostgreSQL 16 container, or returns the cluster
// named by TEST_DATABASE_URL when one is already provided.
func startCluster(ctx context.Context) (*tcpostgres.PostgresContainer, string, error) {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return nil, dsn, nil
	}
	container, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase("wagering"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, "", err
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", err
	}
	return container, dsn, nil
}

// newDatabase creates an empty database on the shared cluster and returns a
// connection to it, along with a migrator pointed at the same database.
//
// This is the unmigrated one, for the tests that are about migrating. Anything
// that only wants the finished schema should call [migrated], which does not
// pay to apply it.
func newDatabase(t *testing.T) (*sql.DB, *postgres.Migrator) {
	t.Helper()
	target := freshDatabase(t, "")
	db := connect(t, target)
	migrator, err := postgres.NewMigrator(target)
	if err != nil {
		t.Fatalf("new migrator: %v", err)
	}
	// The migrator opens a connection of its own, so it closes one of its own.
	t.Cleanup(func() { _ = migrator.Close() })
	return db, migrator
}

// migrated returns a database with every migration applied.
//
// The schema is built once and copied for each test rather than replayed by
// each test: the eight migrations are the same eight every time, and copying
// the result of them costs about a third of running them. Every test still gets
// a database of its own, so nothing about isolation changes.
func migrated(t *testing.T) *sql.DB {
	t.Helper()
	requireCluster(t)
	name, err := schemaTemplate()
	if err != nil {
		t.Fatalf("build the schema template: %v", err)
	}
	return connect(t, freshDatabase(t, name))
}

// schemaTemplate names a database holding the applied schema, building it the
// first time it is asked for.
var schemaTemplate = sync.OnceValues(func() (string, error) {
	name, err := create("")
	if err != nil {
		return "", err
	}
	target, err := rename(sharedDSN, name)
	if err != nil {
		return "", err
	}
	migrator, err := postgres.NewMigrator(target)
	if err != nil {
		return "", fmt.Errorf("new migrator: %w", err)
	}
	// Closed before this returns, and so before any clone is taken:
	// PostgreSQL refuses to copy a database that has a session connected.
	defer func() { _ = migrator.Close() }()
	if err := migrator.Up(context.Background()); err != nil {
		return "", fmt.Errorf("migrate the template: %w", err)
	}
	return name, nil
})

// freshDatabase creates a database, copying template when one is named, and
// returns a DSN pointing at it.
func freshDatabase(t *testing.T, template string) string {
	t.Helper()
	requireCluster(t)
	name, err := create(template)
	if err != nil {
		t.Fatal(err)
	}
	target, err := rename(sharedDSN, name)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

// create makes a database and returns its name.
//
// The name is generated here rather than supplied, so there is nothing to quote
// against; a database name cannot be a placeholder in any case.
func create(template string) (string, error) {
	name := fmt.Sprintf("wagering_test_%d", databases.Add(1))
	statement := "CREATE DATABASE " + name
	if template != "" {
		statement += " TEMPLATE " + template
	}
	if _, err := admin.ExecContext(context.Background(), statement); err != nil {
		return "", fmt.Errorf("%s: %w", statement, err)
	}
	return name, nil
}

// requireCluster skips rather than fails when no cluster is reachable. A
// migration test that passes because there was no database to migrate is worse
// than one that says out loud it did not run.
func requireCluster(t *testing.T) {
	t.Helper()
	if sharedErr != nil {
		t.Skipf("no PostgreSQL available: %v", sharedErr)
	}
}

// connect opens a pool against dsn and closes it when the test ends.
func connect(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", redact(dsn), err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("ping %s: %v", redact(dsn), err)
	}
	return db
}

// rename returns dsn pointing at a different database on the same cluster.
func rename(dsn, name string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	parsed.Path = "/" + name
	return parsed.String(), nil
}

// redact removes the password from a DSN so a failing test does not print it.
func redact(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "<unparseable dsn>"
	}
	if _, ok := parsed.User.Password(); ok {
		parsed.User = url.UserPassword(parsed.User.Username(), "xxxxx")
	}
	return strings.TrimSuffix(parsed.String(), "?")
}
