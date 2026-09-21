//go:build integration

// The integration suite for the PostgreSQL adapter.
//
// It is an internal test package rather than the external one its neighbour in
// internal/storage/postgres uses, and that is a deliberate difference of one.
// Half of what this adapter promises is a property of the transaction the
// manager opens — its isolation level, its access mode, the timeouts it applies
// — and the only handle on that transaction is inside the package, because the
// whole point of the design is that a use case cannot reach it. Testing the
// promise from outside would mean widening the door it exists to keep shut.
//
// Everything else follows the shape of the storage suite on purpose: one
// container for the package, a freshly migrated database per test, a template
// so the eight migrations run once rather than once per test, and
// TEST_DATABASE_URL to point the lot at a cluster that already exists.
//
// One thing does differ. Where that suite skips when no cluster is reachable,
// this one fails. The build tag already makes running it a deliberate act, so a
// run that was asked for and quietly checked nothing is the worse of the two
// outcomes — and it is the outcome a green pipeline would report as success.
package postgres

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	storage "github.com/gabrielrauch/wagering-service/internal/storage/postgres"
)

// image is the PostgreSQL the schema is written against, pinned to the same
// version the storage suite pins so that a test passing here says something
// about the version that will run in production.
const image = "postgres:16-alpine"

var (
	sharedDSN string
	sharedErr error
	databases atomic.Uint64

	// admin issues the CREATE DATABASE statements, as one pool for the whole
	// package: creating a database is a single statement, and opening a pool to
	// issue it costs an order of magnitude more than issuing it.
	admin *pgxpool.Pool
)

// runID distinguishes this run's databases from those of every other run.
//
// A counter alone was not enough, and the way it failed is worth stating: it
// starts at 1 in every process, and nothing here drops the databases it makes
// — deliberately, because a database left standing is the one a failing test
// can be investigated against. Two runs against one persistent cluster
// therefore asked for the same name twice, which is exactly what
// `go test ./...` followed by `go test -tags integration ./...` does under
// TEST_DATABASE_URL. Base 36 keeps the name short enough to stay well inside
// PostgreSQL's 63-byte identifier limit.
var runID = strconv.FormatUint(rand.Uint64(), 36)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns the shared cluster's lifetime, separately from TestMain because
// os.Exit skips deferred functions and a leaked container outlives the binary.
func run(m *testing.M) int {
	ctx := context.Background()
	container, dsn, err := startCluster(ctx)
	if err != nil {
		sharedErr = err
		return m.Run()
	}
	defer func() { _ = testcontainers.TerminateContainer(container) }()

	pool, err := newAdminPool(ctx, dsn, 8)
	if err != nil {
		sharedErr = err
		return m.Run()
	}
	defer pool.Close()

	sharedDSN, admin = dsn, pool
	code := m.Run()
	// After every test has finished, so nothing is still copying from it.
	dropTemplate()
	return code
}

// builtTemplate names the schema template once it has been built, so that
// [run] can remove it.
//
// The template is the one database no test owns: it is created on the first
// test that asks for a migrated database and then copied by every later one, so
// there is no *testing.T whose cleanup it belongs to. Left behind it is the
// residue a suite run leaves on a persistent cluster even when every test
// passed and dropped its own.
var builtTemplate string

func dropTemplate() {
	if builtTemplate == "" {
		return
	}
	if _, err := admin.Exec(context.Background(),
		"DROP DATABASE IF EXISTS "+builtTemplate+" WITH (FORCE)"); err != nil {
		fmt.Fprintf(os.Stderr, "could not drop the schema template %s: %v\n", builtTemplate, err)
	}
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

// newAdminPool opens a pool as the cluster's owning role, which the fixtures
// need for the writes the application deliberately cannot make.
func newAdminPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", redact(dsn), err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", redact(dsn), err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s: %w", redact(dsn), err)
	}
	return pool, nil
}

// newAppPool opens a pool that acts as wagering_app.
//
// SET ROLE on every new connection rather than a second login user: it drops
// the session to that role's privileges for real, so what these tests observe
// is what the service will observe, without putting a password anywhere. pgx
// does not reset a session between acquisitions, so the role holds for the life
// of the connection.
func newAppPool(t *testing.T, dsn string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", redact(dsn), err)
	}
	cfg.MaxConns = maxConns
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET ROLE wagering_app`)
		return err
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open %s: %v", redact(dsn), err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("ping %s: %v", redact(dsn), err)
	}
	return pool
}

// migrated returns a DSN for a database with every migration applied.
//
// The schema is built once and copied for each test rather than replayed by
// each test: the eight migrations are the same eight every time, and copying
// the result costs about a third of running them. Every test still gets a
// database of its own.
func migrated(t *testing.T) string {
	t.Helper()
	requireCluster(t)
	name, err := schemaTemplate()
	if err != nil {
		t.Fatalf("build the schema template: %v", err)
	}
	return freshDatabase(t, name)
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
	migrator, err := storage.NewMigrator(target)
	if err != nil {
		return "", fmt.Errorf("new migrator: %w", err)
	}
	// Closed before this returns, and so before any clone is taken: PostgreSQL
	// refuses to copy a database that has a session connected.
	defer func() { _ = migrator.Close() }()
	if err := migrator.Up(context.Background()); err != nil {
		return "", fmt.Errorf("migrate the template: %w", err)
	}
	builtTemplate = name
	return name, nil
})

// freshDatabase creates a database from a template and returns a DSN for it.
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
	// Registered before the pools that will connect to it, so that it runs
	// after them: cleanups run last in, first out.
	t.Cleanup(func() { drop(t, name) })
	return target
}

// drop removes a database once the test that took it has passed.
//
// Kept on failure, deliberately: the comment this replaces defended keeping
// every database on the grounds that "a database left standing is the one a
// failing test can be investigated against", which argues for keeping the
// databases of FAILING tests and says nothing for the rest. A passing test's
// database is ~8MB nobody will ever look at, and a suite run repeatedly against
// one cluster accumulated them until the cluster ran out of disk.
//
// WITH (FORCE) because a pool that has not finished closing would otherwise
// hold the drop off; the cleanup that closes it is registered later and so runs
// first, but a connection the server has not yet reaped is not this test's to
// wait for. A drop that fails is logged and not fatal — the database simply
// stays, which is what used to happen to all of them.
func drop(t *testing.T, name string) {
	t.Helper()
	if t.Failed() {
		return
	}
	if _, err := admin.Exec(context.Background(),
		"DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
		t.Logf("could not drop %s, leaving it behind: %v", name, err)
	}
}

// create makes a database and returns its name. The name is generated here
// rather than supplied, so there is nothing to quote against; a database name
// cannot be a placeholder in any case.
func create(template string) (string, error) {
	name := fmt.Sprintf("wagering_adapter_%s_%d", runID, databases.Add(1))
	statement := "CREATE DATABASE " + name
	if template != "" {
		statement += " TEMPLATE " + template
	}
	if _, err := admin.Exec(context.Background(), statement); err != nil {
		return "", fmt.Errorf("%s: %w", statement, err)
	}
	return name, nil
}

// requireCluster fails rather than skips. See the note at the top of the file.
func requireCluster(t *testing.T) {
	t.Helper()
	if sharedErr != nil {
		t.Fatalf("the integration suite needs a PostgreSQL and found none "+
			"(start Docker, or set TEST_DATABASE_URL): %v", sharedErr)
	}
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
