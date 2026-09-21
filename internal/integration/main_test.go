//go:build integration

// The containers this suite runs against, and the databases it hands out.
//
// The shape is the PostgreSQL adapter's suite, deliberately: one cluster for
// the package, a schema template built once and copied per test, a database per
// test dropped on success and kept on failure, and TEST_DATABASE_URL to point
// the lot at a cluster that already exists. What is added is a second container
// — Keycloak, importing the realm this service is deployed with — and it is
// shared the same way, because bringing an identity provider up costs about
// twelve seconds and doing that per test would make the suite unusable.
//
// Nothing here skips. The build tag already makes running this a deliberate
// act, so a run that was asked for and quietly checked nothing is the worse of
// the two outcomes, and it is the one a green pipeline reports as success.
package integration

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	storage "github.com/gabrielrauch/wagering-service/internal/storage/postgres"
)

// postgresImage is the version the schema is written against, pinned to the one
// the other two suites pin so that a test passing here says something about the
// version that will run in production.
const postgresImage = "postgres:16-alpine"

var (
	sharedDSN string
	sharedErr error
	databases atomic.Uint64

	// admin issues the CREATE DATABASE statements, as one pool for the whole
	// package: creating a database is a single statement, and opening a pool to
	// issue it costs an order of magnitude more than issuing it.
	admin *pgxpool.Pool
)

// runID distinguishes this run's databases from those of every other run, which
// matters because nothing drops a database a test failed on and two runs
// against one persistent cluster would otherwise ask for the same name twice.
var runID = strconv.FormatUint(rand.Uint64(), 36)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns both containers' lifetimes, separately from TestMain because
// os.Exit skips deferred functions and a leaked container outlives the binary.
func run(m *testing.M) int {
	ctx := context.Background()

	// Both at once. They have nothing to say to each other and Keycloak takes
	// four times as long to answer, so starting them in sequence would add the
	// cluster's start-up to every run for no reason.
	var (
		cluster *tcpostgres.PostgresContainer
		dsn     string
		realm   testcontainers.Container
		wg      sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cluster, dsn, sharedErr = startCluster(ctx)
	}()
	go func() {
		defer wg.Done()
		realm, identityErr = startKeycloak(ctx)
	}()
	wg.Wait()
	defer func() { _ = testcontainers.TerminateContainer(cluster) }()
	defer func() { _ = testcontainers.TerminateContainer(realm) }()

	if sharedErr == nil {
		pool, err := newAdminPool(ctx, dsn, 8)
		if err != nil {
			sharedErr = err
		} else {
			defer pool.Close()
			sharedDSN, admin = dsn, pool
		}
	}

	code := m.Run()
	// After every test has finished, so nothing is still copying from it.
	dropTemplate()
	return code
}

// startCluster brings up a PostgreSQL 16 container, or returns the cluster
// named by TEST_DATABASE_URL when one is already provided.
func startCluster(ctx context.Context) (*tcpostgres.PostgresContainer, string, error) {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return nil, dsn, nil
	}
	container, err := tcpostgres.Run(ctx, postgresImage,
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
// need for the reads and writes the application deliberately cannot make.
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

// migrated returns a DSN for a database with every migration applied.
//
// The schema is built once and copied for each test rather than replayed by
// each test: the eight migrations are the same eight every time, and copying
// the result costs about a third of running them.
func migrated(t *testing.T) string {
	t.Helper()
	requireCluster(t)
	name, err := schemaTemplate()
	if err != nil {
		t.Fatalf("build the schema template: %v", err)
	}
	return freshDatabase(t, name)
}

// builtTemplate names the schema template once it has been built, so that [run]
// can remove it. It is the one database no test owns.
var builtTemplate string

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

func dropTemplate() {
	if builtTemplate == "" {
		return
	}
	if _, err := admin.Exec(context.Background(),
		"DROP DATABASE IF EXISTS "+builtTemplate+" WITH (FORCE)"); err != nil {
		fmt.Fprintf(os.Stderr, "could not drop the schema template %s: %v\n", builtTemplate, err)
	}
}

// freshDatabase creates a database from a template and returns a DSN for it.
func freshDatabase(t *testing.T, template string) string {
	t.Helper()
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

// drop removes a database once the test that took it has passed, and keeps it
// when the test failed, because that is the one a failure can be investigated
// against.
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
	name := fmt.Sprintf("wagering_e2e_%s_%d", runID, databases.Add(1))
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
		t.Fatalf("this suite needs a PostgreSQL and found none "+
			"(start Docker, or set TEST_DATABASE_URL): %v", sharedErr)
	}
}

// rename returns dsn pointing at a different database on the same cluster, as
// the role that owns it.
//
// That role is what the migrations need — they create a schema — and what the
// table snapshots need, because the application's role deliberately cannot see
// all of it. The service's own connections go through [asApplication].
func rename(dsn, name string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	parsed.Path = "/" + name
	return parsed.String(), nil
}

// asApplication drops a DSN to the role the service runs under.
//
// The role is carried in the connection options rather than applied with a
// SET ROLE afterwards, so that postgres.NewPool — the constructor the
// composition root will call — is the one that opens every connection here,
// unwrapped and unadorned. wagering_app has no DDL and no UPDATE or DELETE on
// the ledger, and those privileges are half of what makes the ledger
// append-only: a suite that connected as the owner would be proving the
// application's guarantees with an authority the application does not have.
//
// The space is written as %20 rather than left to url.Values.Encode, which
// spells a space "+" — a spelling libpq does not decode, so the server reads
// the parameter as "+role" and refuses the connection.
func asApplication(dsn string) string {
	const option = "options=-c%20role%3Dwagering_app"
	if strings.Contains(dsn, "?") {
		return dsn + "&" + option
	}
	return dsn + "?" + option
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

// repositoryFile resolves a path relative to the repository root.
//
// The suite's working directory is this package, two levels down, and the realm
// export is a deployment artefact rather than a test fixture — it is the file
// the Keycloak container in deploy/ imports, and importing a copy would let the
// two drift.
func repositoryFile(rest ...string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	return filepath.Abs(filepath.Join(append([]string{cwd, "..", ".."}, rest...)...))
}
