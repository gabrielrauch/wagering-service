//go:build integration

// The containers this suite runs against, and the databases it hands out.
//
// Two of them, brought up together because they have nothing to say to each
// other: a PostgreSQL 16 the schema is applied to, and a LocalStack provisioned
// by deploy/localstack/01-queues.sh — the file an operator actually runs,
// mounted rather than reimplemented. The script is what makes the redrive
// policy this suite tests against the deployed one: [maxReceives] is read back
// off the provisioned queue rather than written down here, so a change to the
// number is a change to what these tests exercise instead of a number in two
// places that quietly disagree.
//
// Nothing here skips. The build tag already makes running this a deliberate
// act, so a run that was asked for and quietly checked nothing is the worse of
// the two outcomes, and it is the one a green pipeline reports as success.
//
// # This file duplicates internal/adapters/postgres/main_test.go for the third
// time, deliberately
//
// internal/integration/main_test.go carries the same note and the same
// reasoning, and both still hold: a helper shared between two packages' _test.go
// files cannot itself be a test file, so factoring it out means a non-test
// package existing only for tests and compiled into every build of the tree,
// and collapsing the copies means editing suites this task was told not to
// touch. What is new is only that there are now three, which raises the price of
// leaving it alone rather than changing the argument — so the note in that file
// stands, this is the third caller of it, and whoever is next allowed to change
// all three should collapse them into one internal package and delete all three
// notes.
//
// There is a cost neither of the other notes records, and it is the strongest
// argument the status quo has. testcontainers-go is reached from _test.go files
// only, in all six suites that use it; a shared helper would have to be a
// non-test package, which would make it a non-test import of this module. What
// `go mod graph` reports, what an SBOM lists and what a vulnerability scanner
// considers shipped would all then include a container runtime client that
// nothing in the built service touches. Whoever collapses these three has to
// answer that first, and a build tag on the shared package is probably the
// answer.
//
// The deliberate divergences from the two existing copies: the database name
// prefix is wagering_msg_, so a leftover says which suite left it; the admin
// pool is larger, because these tests run in parallel and each of them opens an
// owner pool of its own; and the LocalStack below has no counterpart there.
package messaging

import (
	"context"
	"encoding/json"
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

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	storage "github.com/gabrielrauch/wagering-service/internal/storage/postgres"
)

// The images the two dependencies are pinned to, both the versions the rest of
// the tree pins, so that a test passing here says something about the versions
// that will run in production.
const (
	postgresImage   = "postgres:16-alpine"
	localstackImage = "localstack/localstack:4"
)

// region is what the queues are provisioned in. LocalStack answers for any
// region; this one is the deploy script's own.
const region = "us-east-1"

// The names deploy/localstack/01-queues.sh provisions. Only the inbound one is
// read here, for its redrive policy; the tests take queues of their own.
const provisionedInbound = "wager-transactions.fifo"

// The budgets the two containers have to come up in.
const (
	postgresStartupBudget   = 60 * time.Second
	localstackStartupBudget = 120 * time.Second
)

var (
	sharedDSN string
	sharedErr error
	databases atomic.Uint64

	// admin issues the CREATE DATABASE statements, as one pool for the whole
	// package: creating a database is a single statement, and opening a pool to
	// issue it costs an order of magnitude more than issuing it.
	admin *pgxpool.Pool

	// queuesErr is LocalStack's half of the same story. It is separate from
	// sharedErr so that a suite with a database and no queues says which of the
	// two it is missing.
	queuesErr error
	// sharedSDK is the raw client the fixtures create, fill and drain queues
	// with. The adapter has no such call and should not: provisioning is the
	// deploy script's job, and an adapter that could create a queue would let a
	// typo in a queue name quietly succeed.
	sharedSDK *awssqs.Client
	// queues numbers this run's fixture queues.
	queues atomic.Uint64
	// maxReceives is the deployed redrive policy, read off the provisioned
	// queue in [readRedrivePolicy].
	maxReceives int
)

// runID distinguishes this run's databases and queues from those of every other
// run, which matters because nothing drops a database a test failed on and
// because TEST_DATABASE_URL and TEST_SQS_ENDPOINT may point several runs at one
// cluster and one LocalStack.
var runID = strconv.FormatUint(rand.Uint64(), 36)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns both containers' lifetimes, separately from TestMain because os.Exit
// skips deferred functions and a leaked container outlives the binary.
func run(m *testing.M) int {
	ctx := context.Background()

	if err := setEnvironment(); err != nil {
		queuesErr = err
	}

	// Both at once. LocalStack takes several times as long to answer as the
	// cluster does, so starting them in sequence would add the cluster's
	// start-up to every run for no reason.
	var (
		cluster *tcpostgres.PostgresContainer
		dsn     string
		stack   testcontainers.Container
		wg      sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cluster, dsn, sharedErr = startCluster(ctx)
	}()
	go func() {
		defer wg.Done()
		if queuesErr != nil {
			return
		}
		stack, queuesErr = startLocalStack(ctx)
	}()
	wg.Wait()
	defer func() { _ = testcontainers.TerminateContainer(cluster) }()
	defer func() { _ = testcontainers.TerminateContainer(stack) }()

	if sharedErr == nil {
		pool, err := newAdminPool(ctx, dsn, 24)
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

// setEnvironment puts the placeholder credentials the SDK insists on signing
// with into this process.
//
// LocalStack ignores them and they are not a credential in any sense — nothing
// in this tree ever holds a real one. Set here rather than with t.Setenv
// because the client is built once for the package, before any test exists to
// own it.
//
// AWS_EC2_METADATA_DISABLED is the fourth because without it the default
// credential chain probes the EC2 metadata service on a machine that has none,
// which costs a second on every client built.
func setEnvironment() error {
	for name, value := range map[string]string{
		"AWS_ACCESS_KEY_ID":         "test",
		"AWS_SECRET_ACCESS_KEY":     "test",
		"AWS_REGION":                region,
		"AWS_EC2_METADATA_DISABLED": "true",
	} {
		if err := os.Setenv(name, value); err != nil {
			return fmt.Errorf("set %s: %w", name, err)
		}
	}
	return nil
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
		// AndDeadline because WithWaitStrategy wraps a sixty-second context
		// timeout around whatever inner timeout it is given, so the inner one
		// can only ever be the smaller of the two. Sixty is right for
		// PostgreSQL, which is up in about three seconds; writing it once
		// rather than twice is what keeps it from being right by accident.
		testcontainers.WithWaitStrategyAndDeadline(postgresStartupBudget,
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(postgresStartupBudget)),
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

// startLocalStack brings up a LocalStack with this repository's own queue
// script, builds the client every fixture here uses, and reads the deployed
// redrive policy off the queue the script provisioned.
//
// TEST_SQS_ENDPOINT points it at a LocalStack that already exists, which is
// what a developer running the suite repeatedly wants; the queues that endpoint
// serves are then expected to have been provisioned by the same script.
func startLocalStack(ctx context.Context) (testcontainers.Container, error) {
	container, endpoint, err := localStackEndpoint(ctx)
	if err != nil {
		return container, err
	}
	client, err := sqs.NewClient(ctx, sqs.ClientConfig{Region: region, Endpoint: endpoint})
	if err != nil {
		return container, fmt.Errorf("build the SQS client: %w", err)
	}
	sharedSDK = client
	if maxReceives, err = readRedrivePolicy(ctx, client); err != nil {
		return container, err
	}
	return container, nil
}

// localStackEndpoint brings the container up and says where it answers.
func localStackEndpoint(ctx context.Context) (testcontainers.Container, string, error) {
	if endpoint := os.Getenv("TEST_SQS_ENDPOINT"); endpoint != "" {
		return nil, endpoint, nil
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "deploy", "localstack",
		"01-queues.sh"))
	if err != nil {
		return nil, "", fmt.Errorf("locate the queue script: %w", err)
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started:      true,
		Image:        localstackImage,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			// Only the service these workers speak to. LocalStack loads
			// everything it is told to and nothing it is not, and the rest of
			// AWS is start-up time this suite pays for and never uses.
			"SERVICES": "sqs",
			// Two pieces of start-up work that reach the internet: a CA bundle
			// download and the usage ping. Both are irrelevant here.
			"SKIP_SSL_CERT_DOWNLOAD": "1",
			"DISABLE_EVENTS":         "1",
			// The init script's own line still reaches the log at this level;
			// what is dropped is a request log per call, and this suite makes
			// thousands of them.
			"LS_LOG": "warning",
		},
		Files: []testcontainers.ContainerFile{{
			HostFilePath:      script,
			ContainerFilePath: "/etc/localstack/init/ready.d/01-queues.sh",
			FileMode:          0o755,
		}},
		// The script's own last words, not the container's "Ready.".
		// LocalStack runs the init stage AFTER it reports ready, so a suite
		// that waited for the container would race the queues into existence.
		WaitingFor: wait.ForLog("provisioned the wagering queues").
			WithStartupTimeout(localstackStartupBudget),
	})
	if err != nil {
		return container, "", err
	}
	host, err := container.Host(ctx)
	if err != nil {
		return container, "", err
	}
	port, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		return container, "", err
	}
	return container, fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

// redrivePolicy is the shape SQS stores a redrive policy in.
//
// maxReceiveCount is a json.Number because the attribute is a string carrying a
// JSON document, and whether the count inside it is spelled as a number or as a
// string is the backend's choice rather than this suite's.
type redrivePolicy struct {
	DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
	MaxReceiveCount     json.Number `json:"maxReceiveCount"`
}

// readRedrivePolicy reads the deployed maxReceiveCount off the provisioned
// inbound queue.
//
// It is read rather than restated so that the two scenarios which turn on the
// retry limit — an unreadable message and a message whose body changed under
// its id — exercise the number an operator deployed. Restating it here would
// let the deploy script move to three and leave two tests passing against five.
func readRedrivePolicy(ctx context.Context, client *awssqs.Client) (int, error) {
	found, err := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{
		QueueName: aws.String(provisionedInbound),
	})
	if err != nil {
		return 0, fmt.Errorf("resolve %s: %w", provisionedInbound, err)
	}
	attributes, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       found.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		return 0, fmt.Errorf("read %s's redrive policy: %w", provisionedInbound, err)
	}
	raw, ok := attributes.Attributes[string(types.QueueAttributeNameRedrivePolicy)]
	if !ok {
		return 0, fmt.Errorf("%s carries no redrive policy", provisionedInbound)
	}
	var policy redrivePolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return 0, fmt.Errorf("read %s's redrive policy %q: %w", provisionedInbound, raw, err)
	}
	count, err := policy.MaxReceiveCount.Int64()
	if err != nil || count < 1 {
		return 0, fmt.Errorf("%s allows %q receives, which is not a retry limit",
			provisionedInbound, policy.MaxReceiveCount)
	}
	return int(count), nil
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
	name := fmt.Sprintf("wagering_msg_%s_%d", runID, databases.Add(1))
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

// requireQueues fails rather than skips, for the same reason.
func requireQueues(t *testing.T) {
	t.Helper()
	if queuesErr != nil {
		t.Fatalf("this suite needs a LocalStack with the wagering queues and found none "+
			"(start Docker, or set TEST_SQS_ENDPOINT): %v", queuesErr)
	}
}

// rename returns dsn pointing at a different database on the same cluster, as
// the role that owns it.
//
// That role is what the migrations need — they create a schema — and what the
// table reads need, because the application's role deliberately cannot see all
// of it. The service's own connections go through [asApplication].
func rename(dsn, name string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	parsed.Path = "/" + name
	return parsed.String(), nil
}

// databaseOf names the database a DSN points at, for the one scenario that has
// to take it away and give it back.
func databaseOf(t *testing.T, dsn string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	return strings.TrimPrefix(parsed.Path, "/")
}

// asApplication drops a DSN to the role the service runs under.
//
// The role is carried in the connection options rather than applied with a
// SET ROLE afterwards, so that postgres.NewPool — the constructor the
// composition root calls — is the one that opens every connection here,
// unwrapped and unadorned. wagering_app has no DDL and no UPDATE or DELETE on
// the ledger, and those privileges are half of what makes the ledger
// append-only: a suite that connected as the owner would be proving the
// application's guarantees with an authority the application does not have.
//
// The space is written as %20 rather than left to url.Values.Encode, which
// spells a space "+". pgx's own URL parser passes that "+" through as a literal
// rather than decoding it back to a space, so what reaches the server is
// "-c+role=wagering_app" and PostgreSQL refuses the connection with
// `unrecognized configuration parameter "+role"`.
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
