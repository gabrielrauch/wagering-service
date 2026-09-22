//go:build multi

// A world: a database and a pair of queues of its own, and the processes that
// run against them.
//
// # Why five scenarios build one instead of using the deployment
//
// Three of them kill a process at a named instant and one shortens the wait
// budget from five minutes to four seconds. Neither is possible against the
// deployment without changing what the deployment is: the compose worker
// replicas would race an armed process for the message it has to be the one to
// receive, their publishers would claim the outbox rows a scenario needs
// exactly two publishers to compete over, and their wait budget is the one a
// deployment actually runs with.
//
// So those scenarios get their own everything. The database is created and
// migrated for the scenario; the three queues are created for the scenario;
// the processes are this suite's to start, arm and kill. Nothing in a world
// can be reached by the deployment and nothing in the deployment can reach it,
// which is what makes "exactly two publishers" a statement rather than a hope.
//
// What a world is not is a second copy of the PostgreSQL harness. There is no
// cluster to start — Compose started one — no schema template to clone and
// nothing shared between tests, so what would be two hundred lines elsewhere is
// a CREATE DATABASE, a migration and a DROP.
package multi

import (
	"context"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"

	storage "github.com/gabrielrauch/wagering-service/internal/storage/postgres"
)

// consumerName is the name every consumer in every world runs under.
//
// Identical across processes, deliberately, exactly as docker-compose.yml sets
// it: the inbox holds one row per consumer per message and that row is what
// turns a redelivery into a replay. A per-process name would give each process
// its own rows and let one message be applied once by each of them — which is
// the property two of these scenarios exist to check.
const consumerName = "wager-consumer"

// The bounds a world's queues and processes run under.
//
// Short, and every one of them for a reason a deployment would not have. The
// visibility timeout is how long a message abandoned by a process that died
// stays invisible, so it is what a recovery scenario spends waiting; the drain
// timeout has to be under it, or a message handed back at shutdown is handed
// back with a receipt handle that has already expired.
const (
	worldVisibility = 5 * time.Second
	worldDrain      = 3 * time.Second
	// worldHold is how long a publisher's claim stands. A publisher killed
	// holding one must not hold the event hostage, and this is how long "not
	// hostage" takes.
	worldHold = 4 * time.Second
	// worldPoll is how often the two claiming loops look. A deployment's
	// default is a second; these scenarios are waiting on exactly these loops.
	worldPoll = 200 * time.Millisecond
)

// world is one scenario's private deployment.
type world struct {
	// name is what failures call it, and what its database and queues are
	// named after.
	name string
	// database is the database created for this world, on the cluster Compose
	// started.
	database string
	// owner is the pool the fixtures read through. The service's own role
	// deliberately cannot see all of this.
	owner *pgxpool.Pool
	// appDSN is the connection the processes make, as the role the service
	// runs under: no DDL, and no UPDATE or DELETE on the ledger.
	appDSN string
	// The three queues, by name and by URL.
	inbound, outbound, deadLetter          string
	inboundURL, outboundURL, deadLetterURL string
	// api is the instance every world runs, because a scenario needs a door to
	// open a wallet through and an endpoint to reconcile against.
	api *process
	// base is where that instance answers.
	base string
}

// newWorld creates a database, three queues and an API instance, and hands back
// the lot.
func newWorld(t *testing.T, name string) *world {
	t.Helper()
	requireStack(t)
	ctx := context.Background()

	w := &world{name: name}
	w.database = fmt.Sprintf("wagering_multi_%s_%d", runID, worlds.Add(1))
	prefix := fmt.Sprintf("multi-%s-%s", runID, name)
	w.inbound = prefix + "-in.fifo"
	w.outbound = prefix + "-out.fifo"
	w.deadLetter = prefix + "-dlq.fifo"

	if _, err := owner.Exec(ctx, "CREATE DATABASE "+w.database); err != nil {
		t.Fatalf("create the database for %s: %v", name, err)
	}
	t.Cleanup(func() { w.dropDatabase(t) })

	ownerDSN := rename(t, composeDSN, w.database)
	w.appDSN = asApplication(ownerDSN)
	migrate(t, ownerDSN)

	pool, err := newPool(ctx, ownerDSN, 8)
	if err != nil {
		t.Fatalf("open the fixtures' pool for %s: %v", name, err)
	}
	t.Cleanup(pool.Close)
	w.owner = pool

	w.deadLetterURL = createQueue(t, w.deadLetter, nil)
	w.inboundURL = createQueue(t, w.inbound, map[string]string{
		"VisibilityTimeout": strconv.Itoa(int(worldVisibility.Seconds())),
		"RedrivePolicy": fmt.Sprintf(
			`{"deadLetterTargetArn":%q,"maxReceiveCount":5}`, queueARN(t, w.deadLetterURL)),
	})
	w.outboundURL = createQueue(t, w.outbound, nil)

	w.startAPI(t)
	return w
}

// startAPI starts this world's API instance on a port nothing else holds.
func (w *world) startAPI(t *testing.T) {
	t.Helper()
	port := freePort(t)
	w.base = "http://127.0.0.1:" + strconv.Itoa(port)
	env := w.serviceEnv()
	env["HTTP_ADDR"] = "127.0.0.1:" + strconv.Itoa(port)
	// Given rather than discovered. Discovery would follow the issuer, and the
	// issuer is the address Keycloak answers to on the compose network — which
	// a process on the host cannot resolve. The tokens are the same tokens
	// either way; only the fetch differs.
	env["OIDC_JWKS_URI"] = jwksURI
	// The API runs no publisher and never reads this. It is required anyway,
	// because one config.Config serves both binaries and PUBLISHER_NAME is the
	// one value with no default — so an API started with neither it nor
	// HOSTNAME set refuses to start over a loop it does not have. In the
	// deployment the container runtime sets HOSTNAME and the question never
	// arises; a process started from a shell has to be told.
	env["PUBLISHER_NAME"] = w.name + "-api"

	w.api = launch(t, w.name+"-api", binaries.api, env)
	w.awaitReady(t)
}

// awaitReady waits for this world's instance to report that it can serve.
//
// /health/ready and not a connection succeeding: the endpoint reports the
// database and the queue, so this is "it can do the work" rather than "the
// listener is open", and a scenario that raced past a pool still opening would
// see its first submission fail for a reason that is not about the scenario.
func (w *world) awaitReady(t *testing.T) {
	t.Helper()
	eventually(t, readyBudget, w.name+"'s instance to be ready", func() error {
		// A process that has already exited will never answer, and the reason
		// it exited is in what it wrote. Waiting the whole budget out to report
		// "connection refused" would hide it.
		if w.api.exited() {
			return fmt.Errorf("it exited %d before it could serve\n%s",
				statusOf(w.api.err), w.api.log)
		}
		got, err := attempt(call{base: w.base, method: http.MethodGet, path: "/health/ready"})
		if err != nil {
			return err
		}
		if got.status != http.StatusOK {
			return fmt.Errorf("/health/ready answered %s\n%s", got, w.api.log)
		}
		return nil
	})
}

// workerSettings is which loops a worker runs and how it is armed.
//
// Every loop is named rather than defaulted, because a worker running a loop
// the scenario did not ask for is the failure mode these scenarios are most
// exposed to: a publisher nobody wanted claiming the rows two publishers were
// supposed to compete over, or a reference worker nobody wanted carrying an
// operation forward before the process that was supposed to die had died.
type workerSettings struct {
	consumes   bool
	publishes  bool
	resumes    bool
	publisher  string
	faultPoint string
	// extra overrides anything above it, for the scenarios that need a budget
	// or a batch size of their own.
	extra map[string]string
}

// startWorker starts one worker process and waits for the loops it runs to say
// they have started.
func (w *world) startWorker(t *testing.T, name string, settings workerSettings) *process {
	t.Helper()
	env := w.serviceEnv()
	env["CONSUMER_ENABLED"] = strconv.FormatBool(settings.consumes)
	env["PUBLISHER_ENABLED"] = strconv.FormatBool(settings.publishes)
	env["REFERENCE_WORKER_ENABLED"] = strconv.FormatBool(settings.resumes)
	env["CONSUMER_NAME"] = consumerName
	env["CONSUMER_DRAIN_TIMEOUT"] = worldDrain.String()
	env["PUBLISHER_DRAIN_TIMEOUT"] = worldDrain.String()
	env["REFERENCE_WORKER_DRAIN_TIMEOUT"] = worldDrain.String()
	env["PUBLISHER_INTERVAL"] = worldPoll.String()
	env["PUBLISHER_HOLD"] = worldHold.String()
	env["REFERENCE_WORKER_INTERVAL"] = worldPoll.String()
	// Distinct per process, and there is no default: it lands in
	// outbox.claimed_by and scopes a reschedule to the publisher holding the
	// row, so two publishers sharing a name put back each other's claims.
	env["PUBLISHER_NAME"] = settings.publisher
	if settings.publisher == "" {
		env["PUBLISHER_NAME"] = w.name + "-" + name
	}
	env["REFERENCE_WORKER_NAME"] = w.name + "-" + name
	if settings.faultPoint != "" {
		env["FAULT_POINT"] = settings.faultPoint
	}
	maps.Copy(env, settings.extra)

	p := launch(t, w.name+"-"+name, binaries.worker, env)
	// Each loop says so when it starts. Waiting for the lines rather than for
	// a duration is what makes a scenario that arms a fault deterministic: the
	// message is not sent until the process that must receive it is receiving.
	if settings.consumes {
		p.awaitLine(t, readyBudget, "starting to consume", isLine("consuming wager operations"))
	}
	if settings.publishes {
		p.awaitLine(t, readyBudget, "starting to publish", isLine("publishing wallet events"))
	}
	if settings.resumes {
		p.awaitLine(t, readyBudget, "starting to resume",
			isLine("carrying parked operations forward"))
	}
	return p
}

// isLine matches a log entry by its message.
func isLine(message string) func(entry) bool {
	return func(e entry) bool { return e.message() == message }
}

// serviceEnv is everything both binaries need to reach this world and nothing
// else.
//
// OTEL_EXPORTER_OTLP_ENDPOINT is deliberately absent, which the service reads
// as "there is nowhere to send telemetry" and reports once. These processes
// live for seconds and nothing reads their traces; exporting would add a
// connection and a flush to every start and stop this suite performs.
func (w *world) serviceEnv() map[string]string {
	return map[string]string{
		"SERVICE_NAME":              "wagering",
		"LOG_FORMAT":                "json",
		"LOG_LEVEL":                 "info",
		"DATABASE_URL":              w.appDSN,
		"AWS_REGION":                region,
		"AWS_ENDPOINT_URL":          queueEndpoint,
		"AWS_ACCESS_KEY_ID":         "test",
		"AWS_SECRET_ACCESS_KEY":     "test",
		"AWS_EC2_METADATA_DISABLED": "true",
		"SQS_INBOUND_QUEUE":         w.inbound,
		"SQS_OUTBOUND_QUEUE":        w.outbound,
		"SQS_VISIBILITY_TIMEOUT":    worldVisibility.String(),
		// The worker authenticates nothing — a message on the inbound queue was
		// authorised by whatever put it there — and it is required to name an
		// identity provider anyway, because one config.Config serves both
		// binaries and these two have no default. docker-compose.yml sets them
		// for both processes out of one anchor for the same reason.
		"OIDC_ISSUER":   issuer,
		"OIDC_AUDIENCE": audience,
	}
}

// put sends one message to this world's inbound queue.
//
// The group is the wallet and the deduplication id is the envelope's message
// id, which is what deploy/localstack/01-queues.sh says the producer must do
// and what makes the queue's five-minute window and the inbox's permanent
// record agree about what "the same message" is.
func (w *world) put(t *testing.T, e envelope, wallet string) string {
	t.Helper()
	sent, err := queues.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl:               aws.String(w.inboundURL),
		MessageBody:            aws.String(encode(t, e)),
		MessageGroupId:         aws.String(wallet),
		MessageDeduplicationId: aws.String(e.MessageID),
	})
	if err != nil {
		t.Fatalf("put %s on %s: %v", e.MessageID, w.inbound, err)
	}
	return aws.ToString(sent.MessageId)
}

// awaitEmpty waits for a queue to hold nothing, visible or in flight.
//
// Both counts, because a message that has been received and not deleted is not
// on the queue by the first count and is very much still there.
func (w *world) awaitEmpty(t *testing.T, url, what string) {
	t.Helper()
	eventually(t, settleBudget, what+" to be empty", func() error {
		visible, hidden := depth(t, url)
		if visible != 0 || hidden != 0 {
			return fmt.Errorf("%d visible and %d in flight", visible, hidden)
		}
		return nil
	})
}

// noneDeadLettered asserts that the inbound queue emptied because its messages
// were DELETED rather than because the redrive policy gave up on them.
//
// It is not decoration. A consumer that never deleted anything also empties the
// inbound queue — after five deliveries each message is redriven, and the
// balance is still right because every one of those deliveries is a replay. So
// "the queue is empty" on its own is satisfied by the failure it was written to
// catch, and this is the half that tells the two apart.
func (w *world) noneDeadLettered(t *testing.T) {
	t.Helper()
	visible, hidden := depth(t, w.deadLetterURL)
	if visible != 0 || hidden != 0 {
		t.Errorf("the dead-letter queue holds %d messages and %d in flight: the inbound "+
			"queue emptied because the redrive policy gave up, not because anything was "+
			"deleted", visible, hidden)
	}
}

// drain takes everything off one of this world's queues and answers the
// bodies, oldest first.
func (w *world) drain(t *testing.T, url string) []string {
	t.Helper()
	return drainQueue(t, url)
}

// drainQueue takes everything off a queue and answers the bodies, oldest
// first. It is a function rather than only a method because the deployment's
// own outbound queue is read the same way, and the deployment is not a world.
//
// Deleting as it goes, because a FIFO group delivers one message at a time and
// a drain that left them in flight would read the first message of each group
// and call the queue empty.
func drainQueue(t *testing.T, url string) []string {
	t.Helper()
	var bodies []string
	// Three empty receives in a row rather than one: LocalStack answers a
	// short-polled receive from one internal shard, so a single empty answer
	// says nothing about a queue that still holds messages.
	empty := 0
	for empty < 3 {
		received, err := queues.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
			VisibilityTimeout:   30,
		})
		if err != nil {
			t.Fatalf("drain %s: %v", url, err)
		}
		if len(received.Messages) == 0 {
			empty++
			continue
		}
		empty = 0
		for _, m := range received.Messages {
			bodies = append(bodies, aws.ToString(m.Body))
			if _, err := queues.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{
				QueueUrl:      aws.String(url),
				ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				t.Fatalf("delete from %s: %v", url, err)
			}
		}
	}
	return bodies
}

// depth is how many messages a queue holds, visible and in flight.
func depth(t *testing.T, url string) (visible, hidden int) {
	t.Helper()
	got, err := queues.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatalf("read the depth of %s: %v", url, err)
	}
	return atoi(t, got.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]),
		atoi(t, got.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
}

func atoi(t *testing.T, value string) int {
	t.Helper()
	if value == "" {
		return 0
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("read %q as a count: %v", value, err)
	}
	return n
}

// createQueue makes one FIFO queue with the attributes this repository's own
// provisioning script gives it, plus whatever the caller adds.
func createQueue(t *testing.T, name string, extra map[string]string) string {
	t.Helper()
	attributes := map[string]string{
		"FifoQueue": "true",
		// Off, exactly as the deployment has it: two bodies differing only in
		// whitespace are one message here, and a content hash would make them
		// two.
		"ContentBasedDeduplication":     "false",
		"MessageRetentionPeriod":        "3600",
		"ReceiveMessageWaitTimeSeconds": "1",
	}
	maps.Copy(attributes, extra)
	created, err := queues.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attributes,
	})
	if err != nil {
		t.Fatalf("create the queue %s: %v", name, err)
	}
	url := aws.ToString(created.QueueUrl)
	t.Cleanup(func() {
		if _, err := queues.DeleteQueue(context.Background(),
			&awssqs.DeleteQueueInput{QueueUrl: aws.String(url)}); err != nil {
			t.Logf("could not delete the queue %s: %v", name, err)
		}
	})
	return url
}

// queueARN reads a queue's ARN, which a redrive policy names it by.
func queueARN(t *testing.T, url string) string {
	t.Helper()
	got, err := queues.GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("read the ARN of %s: %v", url, err)
	}
	return got.Attributes[string(types.QueueAttributeNameQueueArn)]
}

// migrate applies every migration to a world's database.
func migrate(t *testing.T, dsn string) {
	t.Helper()
	migrator, err := storage.NewMigrator(dsn)
	if err != nil {
		t.Fatalf("new migrator: %v", err)
	}
	defer func() { _ = migrator.Close() }()
	if err := migrator.Up(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

// dropDatabase removes a world's database once the scenario has passed, and
// keeps it when the scenario failed — because that is the one a failure can be
// investigated against.
func (w *world) dropDatabase(t *testing.T) {
	t.Helper()
	if t.Failed() {
		t.Logf("keeping %s for investigation", w.database)
		return
	}
	if _, err := owner.Exec(context.Background(),
		"DROP DATABASE IF EXISTS "+w.database+" WITH (FORCE)"); err != nil {
		t.Logf("could not drop %s, leaving it behind: %v", w.database, err)
	}
}

// rename returns dsn pointing at a different database on the same cluster.
func rename(t *testing.T, dsn, name string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse a DSN: %v", err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

// asApplication drops a DSN to the role the service runs under.
//
// The role is carried in the connection options rather than applied with a SET
// ROLE afterwards, so that the pool the service opens is opened as that role
// from the first connection. wagering_app has no DDL and no UPDATE or DELETE on
// the ledger, and those privileges are half of what makes the ledger
// append-only — a world whose processes connected as the owner would be proving
// the service's guarantees with an authority the service does not have.
//
// The space is written as %20 rather than left to url.Values.Encode, which
// spells a space "+": pgx passes that "+" through as a literal and PostgreSQL
// refuses the connection with `unrecognized configuration parameter "+role"`.
func asApplication(dsn string) string {
	const option = "options=-c%20role%3Dwagering_app"
	if strings.Contains(dsn, "?") {
		return dsn + "&" + option
	}
	return dsn + "?" + option
}

// freePort asks the kernel for a port nothing holds.
//
// Taken and released rather than held, because the process that is about to
// bind it is a different process and cannot inherit the listener. The window
// between the release and the bind is a race in principle; in practice the
// kernel does not hand the same ephemeral port out twice in that window, and
// the alternative — a fixed port per world — collides with whatever else the
// developer is running.
func freePort(t *testing.T) int {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ask for a free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}
	return port
}
