//go:build integration

// The containers this suite runs against, and the environment it configures
// both binaries from.
//
// Three of them, because the composition root's start-up check is the thing
// under test and there is one per dependency: a PostgreSQL 16 with the schema
// applied, a LocalStack provisioned by deploy/localstack/01-queues.sh, and a
// Keycloak importing deploy/keycloak/realm-export.json. Substituting any of
// them would delete the assertion — a fake queue resolves every name and a fake
// issuer publishes every key, so a start-up check against one proves that the
// check compiles.
//
// Nothing here skips. The build tag already makes running this a deliberate
// act, so a run that was asked for and quietly checked nothing is the worse of
// the two outcomes, and it is the one a green pipeline reports as success.
//
// # This is NOT the fourth copy of the PostgreSQL harness
//
// internal/adapters/postgres, internal/integration and internal/messaging carry
// the same two hundred lines three times, and the note in the third says
// plainly what should happen: whoever is next allowed to change all three
// should collapse them into one internal package. This task is not allowed to
// change any of them, so that note still stands and is not discharged here.
//
// What is here instead is deliberately not that code. Those three hand out a
// database per test, which needs a schema template, a clone per test, an admin
// pool and a drop that keeps the database a failing test ran against. This
// suite starts and stops an application; it writes no row it then reads, so it
// takes one database for the package and applies the migrations to it once.
// That is a different fixture with a different lifetime, and copying the
// per-test machinery in order to use a tenth of it would have made the
// duplication worse in exchange for looking familiar. The two escapes the other
// suites offer — TEST_DATABASE_URL and TEST_SQS_ENDPOINT — are honoured, with
// TEST_OIDC_BASE added for the identity provider, so all four suites can be
// pointed at one already-running stack.
package fxmod

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// The images, pinned to the ones the other suites pin so that a test passing
// here says something about the versions that will run in production.
const (
	postgresImage   = "postgres:16-alpine"
	localstackImage = "localstack/localstack:4"
	keycloakImage   = "quay.io/keycloak/keycloak:26.4"
)

// The start-up budgets. Keycloak's is the one that is not generous by
// preference: it boots in about twelve seconds unloaded and was measured at
// seventy-three under `go test -race -tags integration ./...`, where it
// competes with the other container suites for the same cores.
const (
	postgresStartupBudget   = 60 * time.Second
	localstackStartupBudget = 2 * time.Minute
	keycloakStartupBudget   = 3 * time.Minute
)

// The realm and the queues, as deploy/ declares them.
const (
	realmName     = "wagering"
	apiAudience   = "wagering-api"
	region        = "us-east-1"
	inboundName   = "wager-transactions.fifo"
	outboundName  = "wallet-events.fifo"
	discoveryPath = "/realms/" + realmName + "/.well-known/openid-configuration"
)

// What the containers came to, read by every test through the require
// functions below.
var (
	serviceDSN  string
	databaseErr error

	queueEndpoint string
	queueClient   *awssqs.Client
	queueErr      error

	identityBase string
	identityErr  error
)

// runID names this run's database apart from every other run's, which matters
// because TEST_DATABASE_URL may point at a cluster that outlives the process.
var runID = strconv.FormatUint(rand.Uint64(), 36)

func TestMain(m *testing.M) { os.Exit(run(m)) }

// run owns the containers' lifetimes, separately from TestMain because os.Exit
// skips deferred functions and a leaked container outlives the binary.
func run(m *testing.M) int {
	ctx := context.Background()

	if err := setEnvironment(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// All three at once. They have nothing to say to each other and Keycloak
	// takes several times as long as the other two, so starting them in
	// sequence would add the rest to every run for no reason.
	var (
		cluster  *tcpostgres.PostgresContainer
		queues   testcontainers.Container
		realm    testcontainers.Container
		ownerDSN string
		wg       sync.WaitGroup
	)
	wg.Add(3)
	go func() {
		defer wg.Done()
		cluster, ownerDSN, databaseErr = startCluster(ctx)
	}()
	go func() {
		defer wg.Done()
		queues, queueEndpoint, queueErr = startLocalStack(ctx)
	}()
	go func() {
		defer wg.Done()
		realm, identityBase, identityErr = startKeycloak(ctx)
	}()
	wg.Wait()
	defer func() { _ = testcontainers.TerminateContainer(cluster) }()
	defer func() { _ = testcontainers.TerminateContainer(queues) }()
	defer func() { _ = testcontainers.TerminateContainer(realm) }()

	var dropDatabase func()
	if databaseErr == nil {
		serviceDSN, dropDatabase, databaseErr = migratedDatabase(ctx, ownerDSN)
	}
	if queueErr == nil {
		// This suite's own client, quite separate from the one the composition
		// root builds: it is how a test watches what the service's consumer did
		// to a queue without standing between the two.
		queueClient, queueErr = sqs.NewClient(ctx, sqs.ClientConfig{
			Region: region, Endpoint: queueEndpoint,
		})
	}

	code := m.Run()
	if dropDatabase != nil {
		// After every test, so nothing is still connected to it.
		dropDatabase()
	}
	return code
}

// setEnvironment puts the placeholder credentials the SDK insists on signing
// with into this process.
//
// It has to be the process's own environment and not the map below: the
// composition root builds its client through config.LoadDefaultConfig, which
// reads the environment the SDK's chain is documented to read, and that is the
// whole point — this service never holds a credential of its own. LocalStack
// ignores them and nothing here is a credential in any sense.
//
// os.Setenv rather than t.Setenv because the containers come up before any test
// exists to own them.
//
// AWS_EC2_METADATA_DISABLED is the fourth, and it is not cosmetic. Without it
// the default chain finds no credentials in the environment of a laptop and
// falls through to the EC2 metadata service, which is black-holed rather than
// refused — five seconds of retries per client, which is exactly the queue
// resolve timeout, so the start-up check this suite is about fails for a reason
// that has nothing to do with the queue.
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

// startCluster brings up a PostgreSQL 16, or uses the one TEST_DATABASE_URL
// names.
func startCluster(ctx context.Context) (*tcpostgres.PostgresContainer, string, error) {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return nil, dsn, nil
	}
	container, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("wagering"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		// AndDeadline, because WithWaitStrategy wraps a sixty-second context
		// timeout around whatever inner timeout it is given, so an inner one
		// larger than that can never take effect.
		testcontainers.WithWaitStrategyAndDeadline(postgresStartupBudget,
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(postgresStartupBudget)),
	)
	if err != nil {
		return container, "", err
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return container, "", err
	}
	return container, dsn, nil
}

// migratedDatabase creates one database for this package, applies the schema to
// it, and returns a DSN for it as the role the service runs under.
//
// One for the package rather than one per test. Nothing here writes a row it
// then reads — these tests start an application and stop it — so the isolation
// the other suites buy with a template and a clone per test would be paid for
// and not used.
func migratedDatabase(ctx context.Context, ownerDSN string) (string, func(), error) {
	admin, err := pgxpool.New(ctx, ownerDSN)
	if err != nil {
		return "", nil, fmt.Errorf("open the owner pool: %w", err)
	}
	defer admin.Close()
	if err := admin.Ping(ctx); err != nil {
		return "", nil, fmt.Errorf("reach the cluster: %w", err)
	}

	// The name is generated here rather than supplied, so there is nothing to
	// quote against; a database name cannot be a placeholder in any case.
	name := "wagering_fx_" + runID
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		return "", nil, fmt.Errorf("create %s: %w", name, err)
	}

	target, err := rename(ownerDSN, name)
	if err != nil {
		return "", nil, err
	}
	migrator, err := storage.NewMigrator(target)
	if err != nil {
		return "", nil, fmt.Errorf("new migrator: %w", err)
	}
	if err := migrator.Up(ctx); err != nil {
		_ = migrator.Close()
		return "", nil, fmt.Errorf("migrate %s: %w", name, err)
	}
	if err := migrator.Close(); err != nil {
		return "", nil, fmt.Errorf("close the migrator: %w", err)
	}

	drop := func() {
		pool, err := pgxpool.New(context.Background(), ownerDSN)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not drop %s: %v\n", name, err)
			return
		}
		defer pool.Close()
		if _, err := pool.Exec(context.Background(),
			"DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			fmt.Fprintf(os.Stderr, "could not drop %s: %v\n", name, err)
		}
	}
	return asApplication(target), drop, nil
}

// startLocalStack brings up the queues, or uses the endpoint TEST_SQS_ENDPOINT
// names.
func startLocalStack(ctx context.Context) (testcontainers.Container, string, error) {
	if endpoint := os.Getenv("TEST_SQS_ENDPOINT"); endpoint != "" {
		return nil, endpoint, nil
	}
	script, err := repositoryFile("deploy", "localstack", "01-queues.sh")
	if err != nil {
		return nil, "", err
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started:      true,
		Image:        localstackImage,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES":               "sqs",
			"SKIP_SSL_CERT_DOWNLOAD": "1",
			"DISABLE_EVENTS":         "1",
			"LS_LOG":                 "warning",
		},
		Files: []testcontainers.ContainerFile{{
			HostFilePath:      script,
			ContainerFilePath: "/etc/localstack/init/ready.d/01-queues.sh",
			FileMode:          0o755,
		}},
		// The script's own last words, not the container's "Ready.".
		// LocalStack runs the init stage AFTER it reports ready, so a suite
		// that waited for the container would race the queues into existence —
		// and this suite's whole subject is a start-up check that resolves a
		// queue by name.
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

// startKeycloak brings up the identity provider with the realm imported, or
// uses the base TEST_OIDC_BASE names.
//
// The export is the repository's own deploy/keycloak/realm-export.json rather
// than a fixture of this suite's, so that the realm whose keys are fetched here
// is the artefact that ships.
func startKeycloak(ctx context.Context) (testcontainers.Container, string, error) {
	if base := os.Getenv("TEST_OIDC_BASE"); base != "" {
		return nil, strings.TrimSuffix(base, "/"), nil
	}
	export, err := repositoryFile("deploy", "keycloak", "realm-export.json")
	if err != nil {
		return nil, "", err
	}
	container, err := testcontainers.Run(ctx, keycloakImage,
		testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithCmd("start-dev", "--import-realm"),
		testcontainers.WithEnv(map[string]string{
			"KC_BOOTSTRAP_ADMIN_USERNAME": "admin",
			"KC_BOOTSTRAP_ADMIN_PASSWORD": "admin",
		}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      export,
			ContainerFilePath: "/opt/keycloak/data/import/realm-export.json",
			FileMode:          0o644,
		}),
		// The realm's own discovery document, not the container's health
		// endpoint: Keycloak accepts connections well before the import has
		// finished, and a JWKS fetch against a realm that does not exist yet is
		// the exact failure this suite asserts does not happen.
		testcontainers.WithWaitStrategyAndDeadline(keycloakStartupBudget,
			wait.ForHTTP(discoveryPath).
				WithPort("8080/tcp").
				WithStartupTimeout(keycloakStartupBudget)),
	)
	if err != nil {
		return container, "", fmt.Errorf("start keycloak: %w", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		return container, "", fmt.Errorf("keycloak host: %w", err)
	}
	port, err := container.MappedPort(ctx, "8080/tcp")
	if err != nil {
		return container, "", fmt.Errorf("keycloak port: %w", err)
	}
	return container, "http://" + host + ":" + port.Port(), nil
}

// The three require functions fail rather than skip. See the note at the top.

func requireDatabase(t *testing.T) {
	t.Helper()
	if databaseErr != nil {
		t.Fatalf("this suite needs a PostgreSQL and found none "+
			"(start Docker, or set TEST_DATABASE_URL): %v", databaseErr)
	}
}

func requireQueues(t *testing.T) {
	t.Helper()
	if queueErr != nil {
		t.Fatalf("this suite needs the provisioned queues and found none "+
			"(start Docker, or set TEST_SQS_ENDPOINT): %v", queueErr)
	}
}

func requireIdentityProvider(t *testing.T) {
	t.Helper()
	if identityErr != nil {
		t.Fatalf("this suite needs a Keycloak with the realm imported and found none "+
			"(start Docker, or set TEST_OIDC_BASE): %v", identityErr)
	}
}

// environment is the configuration both binaries are built from here, as the
// variables an operator would set.
//
// Deliberately the real variables through the real loader rather than a
// config.Config literal: a field this suite filled in by hand would be a field
// the loader could stop filling in without anything going red.
func environment(t *testing.T, overrides map[string]string) map[string]string {
	t.Helper()
	requireDatabase(t)
	requireQueues(t)

	env := map[string]string{
		"DATABASE_URL":     serviceDSN,
		"AWS_REGION":       region,
		"AWS_ENDPOINT_URL": queueEndpoint,

		"OIDC_ISSUER":   identityBase + "/realms/" + realmName,
		"OIDC_AUDIENCE": apiAudience,

		"SQS_INBOUND_QUEUE":  inboundName,
		"SQS_OUTBOUND_QUEUE": outboundName,
		// One second rather than the twenty a deployment polls with, so that a
		// consumer being stopped is not waiting out a long poll it started
		// moments earlier and the suite is not paying twenty seconds a test for
		// it. It is the receive timeout and nothing else; the drain path is
		// what these tests are about and it is left alone.
		"SQS_WAIT_TIME": "1s",

		// An arbitrary free port on the loopback: the suite reads back what was
		// bound, and binding a fixed one would make two runs on one machine
		// fight over it.
		"HTTP_ADDR": "127.0.0.1:0",

		// Distinct per process is the rule; distinct per test run is what makes
		// two runs against one cluster not claim each other's outbox rows.
		"PUBLISHER_NAME": "publisher-" + runID,
	}
	maps.Copy(env, overrides)
	return env
}

// rename returns dsn pointing at a different database on the same cluster, as
// the role that owns it.
func rename(dsn, name string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse the dsn: %w", err)
	}
	parsed.Path = "/" + name
	return parsed.String(), nil
}

// asApplication drops a DSN to the role the service runs under.
//
// wagering_app has no DDL and no UPDATE or DELETE on the ledger, and those
// privileges are half of what makes the ledger append-only — so a suite that
// let the composition root connect as the owner would be starting a process
// with an authority the deployed one does not have.
//
// The space is written as %20 rather than left to url.Values.Encode, which
// spells a space "+": pgx passes that "+" through as a literal, and PostgreSQL
// refuses the connection with `unrecognized configuration parameter "+role"`.
func asApplication(dsn string) string {
	const option = "options=-c%20role%3Dwagering_app"
	if strings.Contains(dsn, "?") {
		return dsn + "&" + option
	}
	return dsn + "?" + option
}

// repositoryFile resolves a path relative to the repository root, so that the
// deployment artefacts under deploy/ are used rather than copies of them.
func repositoryFile(rest ...string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	return filepath.Abs(filepath.Join(append([]string{cwd, "..", ".."}, rest...)...))
}

// The two attributes a test reads to see what the service's consumer is doing.
//
// Watching the queue rather than the service is deliberate: a decorator around
// the queue the composition root built would be a decorator this suite had put
// there, and what these tests are about is whether the loops the ROOT wired are
// running. SQS counts a message as not visible from the moment somebody
// receives it, so the count is the queue reporting on the consumer.
const (
	visibleAttribute  = types.QueueAttributeNameApproximateNumberOfMessages
	inFlightAttribute = types.QueueAttributeNameApproximateNumberOfMessagesNotVisible
)

// queueURLOf resolves a provisioned queue's URL through this suite's own
// client.
func queueURLOf(t *testing.T, name string) string {
	t.Helper()
	requireQueues(t)

	found, err := queueClient.GetQueueUrl(t.Context(), &awssqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("resolve the queue %s: %v", name, err)
	}
	return *found.QueueUrl
}

// put sends one message. The body is deliberately not a valid envelope: what
// these tests want to know is whether anything RECEIVED it, and a consumer
// refusing to parse a message has received it just as surely as one that
// applies it.
//
// Both FIFO ids are the caller's to supply — the queues have no content-based
// deduplication — and a distinct group per call is what keeps one test's
// message from being stuck behind another's.
func put(t *testing.T, url, group, body string) {
	t.Helper()

	_, err := queueClient.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl:               aws.String(url),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(group + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)),
	})
	if err != nil {
		t.Fatalf("send to %s: %v", url, err)
	}
}

// inFlight is how many messages the queue is holding invisible, which is how
// many somebody has received and not yet finished with.
func inFlight(t *testing.T, url string) int {
	t.Helper()

	answer, err := queueClient.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{inFlightAttribute, visibleAttribute},
	})
	if err != nil {
		t.Fatalf("read the attributes of %s: %v", url, err)
	}
	held, err := strconv.Atoi(answer.Attributes[string(inFlightAttribute)])
	if err != nil {
		t.Fatalf("read %s of %s: %v", inFlightAttribute, url, err)
	}
	return held
}

// awaitInFlight waits for the queue to be holding more than it was, and reports
// how long that took.
func awaitInFlight(t *testing.T, url string, above int, within time.Duration) time.Duration {
	t.Helper()

	began := time.Now()
	for {
		if held := inFlight(t, url); held > above {
			return time.Since(began)
		}
		if time.Since(began) > within {
			t.Fatalf("waited %s for something to receive from %s; it is still holding %d",
				within, url, above)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// deliveries takes a message off the queue itself and reports how many times it
// had been delivered before this one.
//
// Per message, rather than the queue's aggregate counters, because the aggregate
// cannot answer the question a stopped worker raises: "did anything receive THIS
// one". SQS carries the count on the message, so one is a message nobody else
// has seen and two is a message something took first.
//
// It deletes what it takes, so a test that sends a message leaves the queue as
// it found it.
func deliveries(t *testing.T, url, marker string, within time.Duration) int {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		received, err := queueClient.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			t.Fatalf("receive from %s: %v", url, err)
		}
		found := -1
		for _, message := range received.Messages {
			// Everything it sees is deleted, the match and the rest: a probe
			// message left behind locks its group, and the next test to send
			// one would be waiting behind it.
			if strings.Contains(aws.ToString(message.Body), marker) {
				count, err := strconv.Atoi(message.Attributes[string(
					types.MessageSystemAttributeNameApproximateReceiveCount)])
				if err != nil {
					t.Fatalf("read the delivery count of %s: %v", marker, err)
				}
				found = count
			}
			if _, err := queueClient.DeleteMessage(t.Context(), &awssqs.DeleteMessageInput{
				QueueUrl: aws.String(url), ReceiptHandle: message.ReceiptHandle,
			}); err != nil {
				t.Logf("could not delete a message from %s: %v", url, err)
			}
		}
		if found >= 0 {
			return found
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s to come back to %s and it never did, so something "+
				"else is holding it", within, marker, url)
		}
	}
}

// drain takes everything visible off the queue, for a moment, and deletes it.
//
// It is how a test starts from a clean queue without PurgeQueue, which SQS
// allows only once a minute and which this suite would exceed.
func drain(t *testing.T, url string, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		received, err := queueClient.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Fatalf("receive from %s: %v", url, err)
		}
		for _, message := range received.Messages {
			if _, err := queueClient.DeleteMessage(t.Context(), &awssqs.DeleteMessageInput{
				QueueUrl: aws.String(url), ReceiptHandle: message.ReceiptHandle,
			}); err != nil {
				t.Logf("could not delete a message from %s: %v", url, err)
			}
		}
	}
}
