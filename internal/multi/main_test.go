//go:build multi

// What this suite runs against: the compose deployment, brought up by the
// suite itself, and the two binaries, built by it.
//
// # Why TestMain starts the stack rather than insisting one is running
//
// Because a suite whose first failure is "connection refused" has told whoever
// ran it nothing, and because `docker compose up --wait` against a stack that
// is already healthy costs about two seconds. The five services named are the
// ones every scenario needs; they pull in the migration job and the collector
// through their own dependencies. Grafana and Prometheus are deliberately not
// named — nothing here reads a dashboard, and Grafana alone is forty seconds.
//
// # Nothing here skips
//
// The build tag already makes running this a deliberate act, so a run that was
// asked for and quietly checked nothing is the worse of the two outcomes, and
// it is the one a green pipeline reports as success. A stack that will not
// start is reported by every test, with the command to run by hand.
package multi

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
)

// Where the compose stack publishes what this suite talks to.
//
// These are docker-compose.yml's published ports, written out rather than
// parsed back from it: the file is the statement of what the deployment is, and
// a suite that read its own expectations out of it could not notice the two
// disagreeing.
const (
	// composeDSN is the compose database as the owner, which is what the
	// fixtures need — the service's own role deliberately cannot read all of
	// it, and cannot create a database at all.
	composeDSN = "postgres://postgres:postgres@localhost:5432/wagering?sslmode=disable"
	// queueEndpoint is LocalStack. The AWS SDK carries a queue URL in the
	// request body rather than in the address, so this alone is enough.
	queueEndpoint = "http://localhost:4566"
	// region is the region every queue in this stack lives in.
	region = "us-east-1"
	// keycloakBase is the identity provider as the host reaches it. The tokens
	// it issues say `http://keycloak:8080/...` whichever interface they were
	// asked for through, because KC_HOSTNAME pins the issuer — see
	// docker-compose.yml. That is why [issuer] below is the container spelling
	// and this one is not.
	keycloakBase = "http://localhost:8080"
	// issuer is the value a token's "iss" claim carries, byte for byte.
	issuer = "http://keycloak:8080/realms/wagering"
	// jwksURI is where a process running on the host fetches the realm's
	// signing keys. Given explicitly because discovery would follow [issuer],
	// and a host process cannot resolve `keycloak`.
	jwksURI = keycloakBase + "/realms/wagering/protocol/openid-connect/certs"
	// audience is the value that must appear in a token's "aud" claim.
	audience = "wagering-api"
)

// instances are the three API replicas, by published port.
//
// Addressable by position because a scenario that submits to one replica and
// reads back from another has to be able to say which is which — which is the
// whole reason docker-compose.yml writes three services out rather than using
// deploy.replicas. See its comment above api-1.
var instances = [3]string{
	"http://localhost:8081",
	"http://localhost:8082",
	"http://localhost:8083",
}

// The services `docker compose up --wait` is asked for. The migration job, the
// database, the identity provider, LocalStack and the collector all come up
// underneath them through depends_on.
var composeServices = []string{"api-1", "api-2", "api-3", "worker"}

// stackBudget bounds bringing the deployment up from nothing, including the
// image build. Fifty seconds is what it takes on a warm Docker; this is three
// times that, because the one thing worse than a slow start here is a test
// suite that calls a slow start a broken one.
const stackBudget = 5 * time.Minute

// buildBudget bounds compiling the two binaries. The race detector is what
// makes this minutes rather than seconds on a cold build cache.
const buildBudget = 5 * time.Minute

var (
	// stackErr is why this suite cannot run, and nil when it can. Every test
	// asks [requireStack] rather than skipping.
	stackErr error
	// owner is the pool the fixtures read and write the compose database
	// through, and the one that creates each world's database.
	owner *pgxpool.Pool
	// queues is the SQS client this suite administers queues with. It is the
	// adapter's own constructor, so the client a test sends through is built
	// exactly as the service's is.
	queues *awssqs.Client
	// binaries names the two executables this suite starts.
	binaries struct{ api, worker string }
	// worlds counts the databases and queue sets handed out, so that two
	// worlds in one run cannot ask for the same name.
	worlds atomic.Uint64
)

// runID distinguishes this run's databases, queues and identifiers from every
// other run's. The compose database is shared and outlives a run — nothing
// drops it — so every row a scenario asserts on is found by a value carrying
// this rather than by counting what is in the table.
var runID = strconv.FormatUint(rand.Uint64(), 36)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns everything with a lifetime, separately from TestMain because
// os.Exit skips deferred functions.
func run(m *testing.M) int {
	ctx := context.Background()

	// The placeholder credentials the SDK insists on signing with. LocalStack
	// accepts any pair; without them the credential chain falls through to the
	// EC2 metadata service, which on a laptop is black-holed rather than
	// refused — five seconds of retries before every client works.
	for name, value := range map[string]string{
		"AWS_ACCESS_KEY_ID":         "test",
		"AWS_SECRET_ACCESS_KEY":     "test",
		"AWS_EC2_METADATA_DISABLED": "true",
		"AWS_REGION":                region,
	} {
		if err := os.Setenv(name, value); err != nil {
			fmt.Fprintf(os.Stderr, "set %s: %v\n", name, err)
			return 1
		}
	}

	// Both at once. The stack is mostly waiting on containers and the build is
	// mostly compiling, so doing them in sequence would add one to the other
	// for no reason.
	var (
		buildErr error
		wg       sync.WaitGroup
	)
	wg.Add(2)
	go func() { defer wg.Done(); stackErr = ensureStack(ctx) }()
	go func() { defer wg.Done(); buildErr = build(ctx) }()
	wg.Wait()
	if stackErr == nil {
		stackErr = buildErr
	}

	if stackErr == nil {
		pool, err := newPool(ctx, composeDSN, 16)
		if err != nil {
			stackErr = err
		} else {
			defer pool.Close()
			owner = pool
		}
	}
	if stackErr == nil {
		client, err := sqs.NewClient(ctx, sqs.ClientConfig{
			Region:   region,
			Endpoint: queueEndpoint,
		})
		if err != nil {
			stackErr = fmt.Errorf("build the SQS client: %w", err)
		} else {
			queues = client
		}
	}
	return m.Run()
}

// ensureStack brings the deployment up and waits for it to be healthy.
//
// `up --build --wait` rather than a check that it is already running: the
// binaries this suite builds come from the working tree, and a container
// serving an image built before the last edit would be a suite testing the
// wrong code. Compose rebuilds only what changed, so this is a few seconds on a
// stack that is already correct.
func ensureStack(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, stackBudget)
	defer cancel()

	root, err := repositoryRoot()
	if err != nil {
		return err
	}
	args := append([]string{"compose", "up", "--build", "--detach", "--wait"}, composeServices...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker %v: %w\n%s", args, err, out)
	}
	return nil
}

// build compiles the two binaries this suite starts as processes.
//
// Into a directory of this suite's own rather than onto the PATH, so that a run
// cannot pick up a stale binary somebody installed, and with the race detector
// when the suite itself was built with it — see raceFlags. The binaries are the
// thing under test here; a data race inside one of them should fail this run.
func build(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, buildBudget)
	defer cancel()

	root, err := repositoryRoot()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "wagering-multi-")
	if err != nil {
		return fmt.Errorf("make a directory for the binaries: %w", err)
	}
	binaries.api = filepath.Join(dir, "api")
	binaries.worker = filepath.Join(dir, "worker")

	for target, out := range map[string]string{
		"./cmd/api":    binaries.api,
		"./cmd/worker": binaries.worker,
	} {
		args := append([]string{"build"}, raceFlags...)
		args = append(args, "-o", out, target)
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = root
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("go %v: %w\n%s", args, err, output)
		}
	}
	return nil
}

// newPool opens a pool and proves it answers, so that a misconfiguration is
// reported here rather than at the first query of the first test.
func newPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse a DSN: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open a pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// requireStack fails rather than skips. See the note at the top of the file.
func requireStack(t *testing.T) {
	t.Helper()
	if stackErr != nil {
		t.Fatalf("this suite needs the compose stack and the two binaries, and could not "+
			"have them (start Docker, then `make up`): %v", stackErr)
	}
}

// repositoryRoot is two levels up from this package, which is where the compose
// file and the two commands are.
func repositoryRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	return filepath.Abs(filepath.Join(cwd, "..", ".."))
}
