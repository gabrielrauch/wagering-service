//go:build integration

// The integration suite for the SQS adapter.
//
// It runs against a real LocalStack, provisioned by the very script this
// repository deploys — deploy/localstack/01-queues.sh, mounted into the
// container's init directory rather than reimplemented here. That is the point
// of doing it this way: the queue parameters the consumer and the publisher
// depend on are asserted against the file an operator will actually run, so a
// change to the redrive policy or the visibility timeout that nobody meant to
// make fails a test instead of a production morning.
//
// It is an internal test package, like the PostgreSQL adapter's suite and for
// the same reason: half of what this adapter promises — that a queue refuses
// every call before OnStart, how an AWS failure is classified, where a batch is
// split — is only reachable from inside, and testing it from outside would mean
// widening the door it exists to keep shut.
//
// One container for the package. Bringing LocalStack up costs several seconds
// and the tests have nothing to say to each other; each takes a FIFO queue of
// its own where it needs isolation.
//
// Nothing here skips. The build tag already makes running this a deliberate
// act, so a run that was asked for and quietly checked nothing is the worse of
// the two outcomes — and it is the one a green pipeline reports as success.
package sqs

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// image is the LocalStack the init script was validated against.
const image = "localstack/localstack:4"

// region is what the queues are provisioned in. LocalStack answers for any
// region; this one is the script's own, so a queue ARN read back matches what
// the script wrote.
const region = "us-east-1"

// The names deploy/localstack/01-queues.sh provisions.
const (
	inboundQueue  = "wager-transactions.fifo"
	deadLetter    = "wager-transactions-dlq.fifo"
	outboundQueue = "wallet-events.fifo"
)

var (
	sharedErr error
	// sharedSDK is the raw client the fixtures create and delete queues with.
	// The adapter has no such call and should not: provisioning is the deploy
	// script's job, and an adapter that could create a queue would let a typo
	// in a queue name quietly succeed.
	sharedSDK *awssqs.Client
	// queues numbers the fixture queues. Atomic because the tests that take a
	// queue each may run at the same time.
	queues atomic.Uint64
)

// runID distinguishes this run's fixture queues from those of every other run,
// which matters when TEST_SQS_ENDPOINT points several runs at one LocalStack.
var runID = strconv.FormatUint(rand.Uint64(), 36)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run owns the container's lifetime, separately from TestMain because os.Exit
// skips deferred functions and a leaked container outlives the binary.
func run(m *testing.M) int {
	ctx := context.Background()

	if err := setEnvironment(); err != nil {
		sharedErr = err
		return m.Run()
	}

	container, endpoint, err := startLocalStack(ctx)
	if err != nil {
		sharedErr = err
		return m.Run()
	}
	defer func() { _ = testcontainers.TerminateContainer(container) }()

	client, err := NewClient(ctx, ClientConfig{Region: region, Endpoint: endpoint})
	if err != nil {
		sharedErr = err
		return m.Run()
	}
	sharedSDK = client
	return m.Run()
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

// startLocalStack brings up a LocalStack with this repository's own queue
// script, or returns the endpoint named by TEST_SQS_ENDPOINT when one is
// already provided.
func startLocalStack(ctx context.Context) (testcontainers.Container, string, error) {
	if endpoint := os.Getenv("TEST_SQS_ENDPOINT"); endpoint != "" {
		return nil, endpoint, nil
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "..", "deploy", "localstack",
		"01-queues.sh"))
	if err != nil {
		return nil, "", fmt.Errorf("locate the queue script: %w", err)
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started:      true,
		Image:        image,
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			// Only the service this adapter speaks to. LocalStack loads
			// everything it is told to and nothing it is not, and the rest of
			// AWS is start-up time this suite pays for and never uses.
			"SERVICES": "sqs",
			// Two pieces of start-up work that reach the internet: a CA bundle
			// download and the usage ping. Both are irrelevant here, and a
			// suite whose containers come up faster is a suite that leaves
			// more of the machine to whatever `go test ./...` is running
			// beside it.
			"SKIP_SSL_CERT_DOWNLOAD": "1",
			"DISABLE_EVENTS":         "1",
			// The init script's own line still reaches the log at this level;
			// what is dropped is a request log per call.
			"LS_LOG": "warning",
		},
		Files: []testcontainers.ContainerFile{{
			HostFilePath:      script,
			ContainerFilePath: "/etc/localstack/init/ready.d/01-queues.sh",
			FileMode:          0o755,
		}},
		// The script's own last words, not the container's "Ready.".
		// LocalStack runs the init stage AFTER it reports ready, so a test
		// that waited for the container would race the queues into
		// existence and fail on whichever ran first.
		WaitingFor: wait.ForLog("provisioned the wagering queues").
			WithStartupTimeout(90 * time.Second),
	})
	if err != nil {
		return nil, "", err
	}
	host, err := container.Host(ctx)
	if err != nil {
		return nil, "", err
	}
	port, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		return nil, "", err
	}
	return container, fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

// requireLocalStack fails rather than skips. See the note at the top of the
// file.
func requireLocalStack(t *testing.T) {
	t.Helper()
	if sharedErr != nil {
		t.Fatalf("the integration suite needs a LocalStack and found none "+
			"(start Docker, or set TEST_SQS_ENDPOINT): %v", sharedErr)
	}
}

// fixtureQueue creates a FIFO queue this test alone uses, and removes it
// afterwards.
//
// A queue each rather than the provisioned ones, because several of these tests
// turn on a queue being empty or on a message being the only one in its group,
// and a shared queue would make them depend on the order they ran in.
func fixtureQueue(t *testing.T, attributes map[string]string) string {
	t.Helper()
	requireLocalStack(t)
	name := fmt.Sprintf("test-%s-%d.fifo", runID, queues.Add(1))

	settings := map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "false",
		"VisibilityTimeout":         "30",
	}
	maps.Copy(settings, attributes)
	made, err := sharedSDK.CreateQueue(t.Context(), &awssqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: settings,
	})
	if err != nil {
		t.Fatalf("create the fixture queue %s: %v", name, err)
	}
	t.Cleanup(func() {
		// A context of this test's own: t.Context is already cancelled by the
		// time cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := sharedSDK.DeleteQueue(ctx, &awssqs.DeleteQueueInput{
			QueueUrl: made.QueueUrl}); err != nil {
			t.Logf("could not delete %s, leaving it behind: %v", name, err)
		}
	})
	return name
}

// openQueue builds an adapter handle on a queue and resolves it.
func openQueue(t *testing.T, cfg Config) *Queue {
	t.Helper()
	requireLocalStack(t)
	cfg.Client = sharedSDK
	queue, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("build a queue for %s: %v", cfg.Name, err)
	}
	if err := queue.OnStart(t.Context()); err != nil {
		t.Fatalf("resolve %s: %v", cfg.Name, err)
	}
	return queue
}

// send puts one message on a queue through the raw client, for the tests whose
// subject is receiving rather than sending.
func send(t *testing.T, name, body, group, dedupe string,
	attributes map[string]types.MessageAttributeValue,
) {
	t.Helper()
	url, err := sharedSDK.GetQueueUrl(t.Context(), &awssqs.GetQueueUrlInput{
		QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	if _, err := sharedSDK.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl:               url.QueueUrl,
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(dedupe),
		MessageAttributes:      attributes,
	}); err != nil {
		t.Fatalf("send to %s: %v", name, err)
	}
}
