//go:build multi

// The two dependencies, each unavailable for a while.
//
// # Against the deployment, and paused rather than stopped
//
// Both scenarios run against the compose stack rather than a world, because
// what they are about is what the deployment's own processes do — the API
// replicas' pools and readiness probes, the worker replicas' publishers —
// when a container they depend on stops answering. `docker compose pause`
// freezes the container's processes and nothing else: connections stay open,
// packets are accepted by the kernel and answered by nobody, which is the
// shape of a database or a queue that has hung rather than one that has gone.
// It is the harder of the two to survive, because nothing fails fast.
//
// # Sequential, and restored whatever happens
//
// Neither takes t.Parallel. A paused database is paused for every process on
// the stack, so a scenario running beside one of these would be measuring the
// outage; and every scenario that builds a world of its own is a parallel test
// that starts only once the sequential ones are done. Each pause is undone in
// a cleanup, whether the test passed or not, and the cleanup waits for every
// instance to report ready before the next test is allowed to start.
package multi

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// The deployment's own outbound queue, as deploy/localstack/01-queues.sh
// creates it. Nothing in the stack consumes it, so what the publisher put there
// stays there until this suite reads it.
const deployedOutbound = "wallet-events.fifo"

// composeMaxConns is DATABASE_MAX_CONNS as the deployment runs with it, which
// is .env.example's default: docker-compose.yml does not set it. See
// [drainPool] for what it is used for.
const composeMaxConns = 10

// The budgets these two scenarios wait under.
const (
	// composeBudget bounds one `docker compose` command.
	composeBudget = 60 * time.Second
	// outageBudget bounds the deployment noticing a dependency is gone, and
	// noticing it is back.
	outageBudget = 30 * time.Second
	// answeredWithin bounds a submission being ANSWERED during the outage. It
	// is DATABASE_CONNECT_TIMEOUT with room, and well under the client's own
	// thirty seconds: an answer that arrived at the client's timeout would not
	// have been an answer.
	answeredWithin = 10 * time.Second
)

// healthAnswer is what /health/ready says.
type healthAnswer struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// refusal is the body a refused request carries.
type refusal struct {
	Code string `json:"code"`
}

// TestAPostgresOutageIsAnsweredAsTransientAndNothingIsLost pauses the
// database under a running API instance.
//
// While it is paused: readiness says which dependency failed, and a
// submission is ANSWERED — 503, Retry-After, RETRYABLE — inside the connect
// timeout rather than held until somebody gives up. Once it is back: readiness
// recovers on its own, nothing of the refused submission exists, and the same
// submission under the same key is applied once, as a first application and
// not a replay. Retryable is a promise that sending it again is safe, and the
// second half is that promise kept.
//
// The answer during the outage is deterministic only once the instance's pool
// is empty — see [drainPool] for why, and for how it is made so.
func TestAPostgresOutageIsAnsweredAsTransientAndNothingIsLost(t *testing.T) {
	requireStack(t)
	base := instances[0]

	player := scoped("player-postgres-outage")
	external := scoped("postgres-outage-bet")
	key := scoped("postgres-outage-key")
	wallet := openWallet(t, base, player, "100.00")
	operation := bet(providerA, external, wallet, "25.00")
	// Fetched before the pause: the realm is not what is being taken away.
	credential := token(t, providerA)

	pauseService(t, "postgres")

	unready := readiness(t, base)
	if unready.status != http.StatusServiceUnavailable || unready.checks["postgres"] != "failed" {
		t.Fatalf("/health/ready answered %d %v while the database was paused, want 503 with "+
			"postgres failed", unready.status, unready.checks)
	}
	if unready.checks["sqs"] != "ok" {
		t.Errorf("/health/ready reports sqs %q while only the database is paused, want ok",
			unready.checks["sqs"])
	}

	drainPool(t, base)

	started := time.Now()
	refused := send(t, call{
		base:           base,
		method:         http.MethodPost,
		path:           "/wagering/transactions",
		body:           encode(t, operation),
		token:          credential,
		idempotencyKey: key,
	})
	took := time.Since(started)
	if refused.status != http.StatusServiceUnavailable {
		t.Fatalf("a submission during the outage answered %s, want 503", refused)
	}
	if got := refused.header.Get("Retry-After"); got == "" {
		t.Errorf("the 503 carries no Retry-After")
	}
	var why refusal
	decode(t, refused, &why)
	if why.Code != "RETRYABLE" {
		t.Errorf("the 503 is coded %q, want RETRYABLE", why.Code)
	}
	if took > answeredWithin {
		t.Errorf("the submission was answered after %s, want inside %s: it was held rather "+
			"than refused", took, answeredWithin)
	}

	unpauseService(t, "postgres")
	awaitReadiness(t, base)

	// Nothing of the refused submission exists.
	if _, recorded := operationFor(t, owner, providerA, external); recorded {
		t.Fatalf("the bet %s is recorded, and the submission that carried it was refused",
			external)
	}
	if got, want := balanceOf(t, owner, wallet.WalletID), minor(t, "100.00"); got != want {
		t.Fatalf("the wallet holds %d minor units after the outage, want %d untouched", got, want)
	}

	// The same submission, the same key: applied once, for the first time.
	applied := operationOf(t, submit(t, base, providerA, operation, key))
	if applied.Status != processed {
		t.Fatalf("the retried submission is %s (%s), want %s",
			applied.Status, applied.FailureCode, processed)
	}
	if applied.IdempotentReplay {
		t.Errorf("the retried submission was answered as a replay, and the refused one " +
			"recorded nothing to replay")
	}
	if operations := countRows(t, owner,
		`SELECT count(*) FROM wagering.wager_transaction `+
			`WHERE provider = $1 AND external_transaction_id = $2`, providerA, external); operations != 1 {
		t.Errorf("the table holds %d wager transactions for %s, want 1", operations, external)
	}
	var moved int
	for _, entry := range ledgerOf(t, owner, wallet.WalletID) {
		if entry.transactionID == applied.TransactionID {
			moved++
		}
	}
	if moved != 1 {
		t.Errorf("transaction %s produced %d ledger entries, want 1", applied.TransactionID, moved)
	}
	if got, want := balanceOf(t, owner, wallet.WalletID), minor(t, "75.00"); got != want {
		t.Errorf("the wallet holds %d minor units, want %d", got, want)
	}
	reconciled(t, instances[1], wallet.WalletID)
}

// TestAnSQSOutageDoesNotLoseACommittedEvent pauses LocalStack under the
// deployment and submits an operation while it is paused.
//
// The submission is applied — the API does not touch the queue on that path,
// and a wallet service that refused to move money because a queue was slow
// would be the wrong trade. What it produced sits in the outbox: two rows the
// worker replicas' publishers claim and cannot deliver, and which therefore
// stay unpublished, claimed and counted, for as long as the queue is away.
// Once it is back, both rows are published without anybody asking, and the
// queue holds each event exactly once — by event id, which is what the
// deduplication id is, so a send that was retried across the outage did not
// become a second message.
func TestAnSQSOutageDoesNotLoseACommittedEvent(t *testing.T) {
	requireStack(t)
	base := instances[0]

	player := scoped("player-sqs-outage")
	external := scoped("sqs-outage-bet")
	key := scoped("sqs-outage-key")
	wallet := openWallet(t, base, player, "100.00")
	operation := bet(providerA, external, wallet, "25.00")
	credential := token(t, providerA)

	// Nothing in flight when the queue goes away. A publisher caught mid-send
	// by the pause spends the SDK's whole retry budget on that send before it
	// claims anything else, and this scenario is about the rows it claims
	// AFTER the queue is gone — so the outbox is drained first, and the pause
	// lands on two idle publishers.
	awaitOutboxDrained(t)
	pauseService(t, "localstack")

	got := send(t, call{
		base:           base,
		method:         http.MethodPost,
		path:           "/wagering/transactions",
		body:           encode(t, operation),
		token:          credential,
		idempotencyKey: key,
	})
	if got.status != http.StatusOK {
		t.Fatalf("a submission while the queue was paused answered %s, want 200: the API "+
			"does not need the queue to apply an operation", got)
	}
	applied := operationOf(t, got)
	if applied.Status != processed {
		t.Fatalf("the bet is %s (%s), want %s", applied.Status, applied.FailureCode, processed)
	}

	unready := readiness(t, base)
	if unready.status != http.StatusServiceUnavailable || unready.checks["sqs"] != "failed" {
		t.Errorf("/health/ready answered %d %v while the queue was paused, want 503 with sqs "+
			"failed", unready.status, unready.checks)
	}
	if unready.checks["postgres"] != "ok" {
		t.Errorf("/health/ready reports postgres %q while only the queue is paused, want ok",
			unready.checks["postgres"])
	}

	// The two rows exist, a publisher takes them, and they stay unpublished.
	rows := outboxFor(t, applied.TransactionID)
	if len(rows) != 2 {
		t.Fatalf("the outbox holds %d rows for %s, want 2: the operation and the balance change",
			len(rows), applied.TransactionID)
	}
	eventually(t, outageBudget, "a publisher to claim the operation's event", func() error {
		for _, row := range outboxFor(t, applied.TransactionID) {
			if row.published {
				t.Fatalf("event %s was published while the queue was paused", row.eventID)
			}
			if row.attempts >= 1 {
				return nil
			}
		}
		return fmt.Errorf("no publisher has claimed either event yet")
	})
	for _, row := range outboxFor(t, applied.TransactionID) {
		if row.published {
			t.Errorf("event %s is marked published, and the queue could not have taken it",
				row.eventID)
		}
	}

	unpauseService(t, "localstack")

	eventually(t, outageBudget, "both events to be published once the queue is back", func() error {
		for _, row := range outboxFor(t, applied.TransactionID) {
			if !row.published {
				return fmt.Errorf("event %s (%s) is still unpublished after %d attempts",
					row.eventID, row.eventType, row.attempts)
			}
		}
		return nil
	})
	awaitReadiness(t, base)

	// On the wire exactly once each. The deployment's queue holds everything
	// every scenario in this run published; only these two are counted.
	onTheWire := map[string]int{}
	url, err := queueURLOf(deployedOutbound)
	if err != nil {
		t.Fatalf("%v", err)
	}
	for _, body := range drainQueue(t, url) {
		var carried struct {
			EventID string `json:"eventId"`
		}
		if err := json.Unmarshal([]byte(body), &carried); err != nil {
			t.Fatalf("read an event off %s: %v\n%s", deployedOutbound, err, body)
		}
		onTheWire[carried.EventID]++
	}
	for _, row := range rows {
		switch onTheWire[row.eventID] {
		case 1:
		case 0:
			t.Errorf("event %s (%s) is marked published and is not on %s: it was lost",
				row.eventID, row.eventType, deployedOutbound)
		default:
			t.Errorf("event %s (%s) is on %s %d times, want once",
				row.eventID, row.eventType, deployedOutbound, onTheWire[row.eventID])
		}
	}
	reconciled(t, instances[2], wallet.WalletID)
}

// deployedOutboxRow is one of the deployment's outbox rows, as this scenario
// reads it.
type deployedOutboxRow struct {
	eventID   string
	eventType string
	attempts  int
	published bool
}

// outboxFor reads the outbox rows one transaction produced, in the order they
// were written.
//
// By the transaction id inside the payload, which is the stored envelope: the
// operation's event and the balance change it caused both carry it under
// data, and nothing else in the compose database's shared outbox does.
func outboxFor(t *testing.T, transaction string) []deployedOutboxRow {
	t.Helper()
	rows, err := owner.Query(context.Background(),
		`SELECT event_id::text, event_type, attempts, published_at IS NOT NULL `+
			`FROM wagering.outbox WHERE payload->'data'->>'transactionId' = $1 ORDER BY sequence`,
		transaction)
	if err != nil {
		t.Fatalf("read the outbox for %s: %v", transaction, err)
	}
	defer rows.Close()
	var found []deployedOutboxRow
	for rows.Next() {
		var row deployedOutboxRow
		if err := rows.Scan(&row.eventID, &row.eventType, &row.attempts, &row.published); err != nil {
			t.Fatalf("scan an outbox row for %s: %v", transaction, err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the outbox for %s: %v", transaction, err)
	}
	return found
}

// awaitOutboxDrained waits for the deployment's outbox to hold nothing
// unpublished, which is the state in which both worker replicas' publishers
// are idle.
func awaitOutboxDrained(t *testing.T) {
	t.Helper()
	eventually(t, outageBudget, "the deployment's outbox to drain", func() error {
		if left := countRows(t, owner,
			`SELECT count(*) FROM wagering.outbox WHERE published_at IS NULL`); left != 0 {
			return fmt.Errorf("%d events are still unpublished", left)
		}
		return nil
	})
}

// readinessAnswer is one answer from /health/ready, status and checks.
type readinessAnswer struct {
	status int
	checks map[string]string
}

// readiness asks one instance whether it can serve, with the client's own
// thirty seconds — the endpoint answers inside HTTP_READINESS_TIMEOUT whatever
// its dependencies do, and that is part of what is being checked.
func readiness(t *testing.T, base string) readinessAnswer {
	t.Helper()
	got := send(t, call{base: base, method: http.MethodGet, path: "/health/ready"})
	var body healthAnswer
	decode(t, got, &body)
	return readinessAnswer{status: got.status, checks: body.Checks}
}

// awaitReadiness waits for one instance to report that it can serve again.
func awaitReadiness(t *testing.T, base string) {
	t.Helper()
	eventually(t, outageBudget, base+" to be ready again", func() error {
		got, err := attempt(call{base: base, method: http.MethodGet, path: "/health/ready"})
		if err != nil {
			return err
		}
		if got.status != http.StatusOK {
			return fmt.Errorf("/health/ready answered %s", got)
		}
		return nil
	})
}

// drainPool empties an API instance's connection pool while its database is
// paused, so that the submission which follows is answered the same way every
// time.
//
// Without this the answer depends on the pool's history. pgxpool pings a
// connection that has sat idle for over a second before handing it out, and
// it pings it with the CALLER's context — which for a submission has no
// deadline of its own, so a pinged connection to a paused server holds the
// request until the client gives up, and the only answer is a client timeout.
// A connection the pool has to OPEN is different: the paused server never
// completes the handshake and DATABASE_CONNECT_TIMEOUT ends the attempt, which
// the adapter classifies Retryable and the API answers 503.
//
// The readiness probe is what empties the pool. It pings under
// DATABASE_HEALTH_TIMEOUT, and a ping that fails destroys the connection it
// was made on — one per probe. Ten probes at once take ten distinct
// connections, which is every connection the pool can hold, and the second
// round is for the ones the first found busy. Afterwards the pool holds
// nothing, every acquisition has to open a connection, and every one of those
// ends where the connect timeout says.
func drainPool(t *testing.T, base string) {
	t.Helper()
	for range 2 {
		var wg sync.WaitGroup
		for range composeMaxConns {
			wg.Go(func() {
				_, _ = attempt(call{base: base, method: http.MethodGet, path: "/health/ready"})
			})
		}
		wg.Wait()
	}
}

// pauseService freezes one compose service, and arranges for it to be thawed
// and the deployment to be ready again when the test ends — whether or not the
// test thawed it itself, and whether or not the test passed.
func pauseService(t *testing.T, service string) {
	t.Helper()
	t.Cleanup(func() {
		// Unpausing a service that is not paused is an error to Compose and
		// nothing to this suite: the test did it already.
		if out, err := compose("unpause", service); err != nil &&
			!strings.Contains(out, "is not paused") {
			t.Logf("unpausing %s at cleanup: %v\n%s", service, err, out)
		}
		for _, base := range instances {
			awaitReadiness(t, base)
		}
	})
	if out, err := compose("pause", service); err != nil {
		t.Fatalf("pause %s: %v\n%s", service, err, out)
	}
}

// unpauseService thaws one compose service, in the body of a test that wants
// to assert on what happens next.
func unpauseService(t *testing.T, service string) {
	t.Helper()
	if out, err := compose("unpause", service); err != nil {
		t.Fatalf("unpause %s: %v\n%s", service, err, out)
	}
}

// queueURLOf resolves one of the deployment's queues by name.
func queueURLOf(name string) (string, error) {
	got, err := queues.GetQueueUrl(context.Background(),
		&awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", name, err)
	}
	return aws.ToString(got.QueueUrl), nil
}

// compose runs one `docker compose` command from the repository root, the way
// TestMain brings the stack up.
func compose(args ...string) (string, error) {
	root, err := repositoryRoot()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), composeBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose"}, args...)...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker compose %v: %w", args, err)
	}
	return string(out), nil
}
