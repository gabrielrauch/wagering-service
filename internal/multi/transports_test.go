//go:build multi

// One operation, two transports, both orders.
//
// # The gap this closes
//
// internal/messaging says, in its own package documentation, that nothing there
// drives the HTTP adapter: its cross-transport scenario reaches
// app.Wagering.Submit directly, so the handler's half — the Idempotency-Key
// HEADER, the strict decode of a body that deliberately has no such member —
// is not exercised, and the bug class that leaves open is the HTTP door and the
// queue envelope disagreeing about where the idempotency key lives. Reaching
// the real door from there would have meant something standing in at the
// credential gate, which this tree's brief refuses.
//
// Here there is nothing to stand in for. The submission goes over a real
// listener with a token a real Keycloak issued, the message goes on the real
// FIFO queue and is taken off it by the deployment's own worker replicas, and
// the two are written out from two independent types in helpers_test.go — one
// that carries the key in a header and one that carries it as a member. A
// rename on either side makes one of the two orders below produce two wager
// transactions instead of one.
package multi

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// The deployment's own queues, as deploy/localstack/01-queues.sh creates them.
const deployedInbound = "wager-transactions.fifo"

// deliveryBudget bounds a worker replica applying a message this suite put on
// the deployment's queue.
//
// Long, and deliberately not [settleBudget]. The deployment's visibility
// timeout is thirty seconds, and a delivery that is lost — to a receive that was
// open on a container Compose had just replaced, which is exactly what `up
// --build` does at the start of every run — costs the whole of it before the
// message comes back. Two of those still fit inside this.
const deliveryBudget = 150 * time.Second

// logInterval is how often the deployment's logs are read while waiting.
//
// A second rather than [pollInterval], because each look is a `docker compose
// logs` process: at fifty milliseconds this would start three thousand of them
// waiting out the budget above, which is a load the scenario would then be
// measuring.
const logInterval = time.Second

// TestOneOperationOverHTTPAndOverTheQueueSettlesOnceInEitherOrder submits one
// operation twice: once through an API instance and once as a message the
// worker replicas consume, in both orders.
//
// Both halves assert the same three things and each proves a different one of
// them. The wallet moved once and the ledger has one entry, which is the
// outcome. The inbox holds a completed row for the message, which is the queue
// side genuinely receiving and finishing a delivery rather than the message
// being dropped. And the SECOND of the two arrivals — whichever transport it
// came over — is reported as a replay: over HTTP by `idempotentReplay` in the
// answer, over the queue by `replay` in the consumer's own log line. A
// duplicate that was never received cannot be reported as one.
func TestOneOperationOverHTTPAndOverTheQueueSettlesOnceInEitherOrder(t *testing.T) {
	requireStack(t)

	t.Run("HTTP first, then the queue", func(t *testing.T) {
		player := scoped("player-http-first")
		external := scoped("http-first")
		key := scoped("http-first-key")
		message := scoped("http-first-message")
		wallet := openWallet(t, instances[0], player, "100.00")
		operation := bet(providerA, external, player, "25.00")

		applied := operationOf(t, submit(t, instances[0], providerA, operation, key))
		if applied.Status != processed || applied.IdempotentReplay {
			t.Fatalf("the submission over HTTP is %s and replay=%v, wanted %s and false",
				applied.Status, applied.IdempotentReplay, processed)
		}

		since := time.Now()
		putOnDeployedQueue(t, onTheQueue(operation, message, key), wallet.WalletID)

		line := awaitConsumerLine(t, since, message)
		if replay, stated := line.flag("replay"); !stated || !replay {
			t.Errorf("the worker applied message %s with replay=%v, and it had already been "+
				"applied over HTTP: the queue side did not recognise the operation",
				message, line.fields["replay"])
		}
		if got := line.text("transactionId"); got != applied.TransactionID {
			t.Errorf("the worker answered transaction %s and the HTTP door answered %s: "+
				"one operation became two", got, applied.TransactionID)
		}
		settledOnce(t, wallet.WalletID, external, applied.TransactionID, message, "75.00")
	})

	t.Run("the queue first, then HTTP", func(t *testing.T) {
		player := scoped("player-queue-first")
		external := scoped("queue-first")
		key := scoped("queue-first-key")
		message := scoped("queue-first-message")
		wallet := openWallet(t, instances[0], player, "100.00")
		operation := bet(providerA, external, player, "25.00")

		since := time.Now()
		putOnDeployedQueue(t, onTheQueue(operation, message, key), wallet.WalletID)
		line := awaitConsumerLine(t, since, message)
		if replay, stated := line.flag("replay"); !stated || replay {
			t.Fatalf("the worker applied message %s with replay=%v, and nothing had applied "+
				"it before", message, line.fields["replay"])
		}
		if got := line.text("status"); got != processed {
			t.Fatalf("the worker settled message %s as %s, wanted %s", message, got, processed)
		}
		first := line.text("transactionId")

		again := operationOf(t, submit(t, instances[2], providerA, operation, key))
		if !again.IdempotentReplay {
			t.Errorf("the submission over HTTP was not answered as a replay, and the same " +
				"operation had already arrived over the queue: the HTTP door is reading " +
				"the idempotency key from somewhere the queue envelope does not write it")
		}
		if again.TransactionID != first {
			t.Errorf("the HTTP door answered transaction %s and the queue produced %s: "+
				"one operation became two", again.TransactionID, first)
		}
		if again.Status != processed {
			t.Errorf("the replay is %s, wanted %s", again.Status, processed)
		}
		if again.Balance == nil || again.Balance.Amount != "75.00" {
			t.Errorf("the replay answered balance %v, wanted the 75.00 the first arrival "+
				"recorded", again.Balance)
		}
		settledOnce(t, wallet.WalletID, external, first, message, "75.00")
	})
}

// settledOnce asserts that one operation reached the database once, whichever
// transport brought it, and that the queue's delivery of it was recorded.
func settledOnce(t *testing.T, wallet, external, transaction, message, balance string) {
	t.Helper()
	operations := countRows(t, owner,
		`SELECT count(*) FROM wagering.wager_transaction `+
			`WHERE provider = $1 AND external_transaction_id = $2`, providerA, external)
	if operations != 1 {
		t.Errorf("the table holds %d wager transactions for %s, wanted 1", operations, external)
	}
	entries := ledgerOf(t, owner, wallet)
	var moved int
	for _, entry := range entries {
		if entry.transactionID == transaction {
			moved++
		}
	}
	if moved != 1 {
		t.Errorf("transaction %s produced %d ledger entries, wanted 1", transaction, moved)
	}
	if got, want := balanceOf(t, owner, wallet), minor(t, balance); got != want {
		t.Errorf("the wallet holds %d minor units, want %d", got, want)
	}
	// The inbox is the record that the message arrived and was finished with —
	// the thing the balance cannot tell you. A completed row is a delivery that
	// reached an answer rather than one that was dropped on the way.
	row, found := inboxFor(t, owner, "wager-consumer", message)
	if !found {
		t.Fatalf("the inbox holds no row for message %s, so the queue's delivery of this "+
			"operation left no trace", message)
	}
	if !row.completed {
		t.Errorf("the inbox row for message %s is not completed", message)
	}
	reconciled(t, instances[1], wallet)
}

// deployedInboundURL resolves the deployment's inbound queue once.
var deployedInboundURL = sync.OnceValues(func() (string, error) {
	got, err := queues.GetQueueUrl(context.Background(),
		&awssqs.GetQueueUrlInput{QueueName: aws.String(deployedInbound)})
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", deployedInbound, err)
	}
	return aws.ToString(got.QueueUrl), nil
})

// putOnDeployedQueue sends one message to the queue the worker replicas read.
func putOnDeployedQueue(t *testing.T, e envelope, wallet string) {
	t.Helper()
	url, err := deployedInboundURL()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := queues.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl:               aws.String(url),
		MessageBody:            aws.String(encode(t, e)),
		MessageGroupId:         aws.String(wallet),
		MessageDeduplicationId: aws.String(e.MessageID),
	}); err != nil {
		t.Fatalf("put %s on %s: %v", e.MessageID, deployedInbound, err)
	}
}

// awaitConsumerLine waits for one of the two worker replicas to report that it
// applied the message named, and answers the line it wrote.
//
// Read out of Compose rather than out of a pipe, because these two processes
// are containers this suite did not start. The line is the consumer's own —
// "the operation was applied" — and it carries `replay` and `receiveCount`,
// which is exactly what a scenario about a second arrival needs and what no
// amount of reading the balance would tell it.
func awaitConsumerLine(t *testing.T, since time.Time, message string) entry {
	t.Helper()
	deadline := time.Now().Add(deliveryBudget)
	for {
		for _, line := range composeLogs(t, "worker", since) {
			if line.message() == "the operation was applied" &&
				line.text("messageId") == message {
				return line
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for a worker replica to apply message %s and none did",
				deliveryBudget, message)
			return entry{}
		}
		time.Sleep(logInterval)
	}
}

// composeLogs reads one compose service's log since an instant, as entries.
//
// Compose prefixes every line with the container's name and a pipe. The prefix
// is dropped by taking the line from its first brace, which is safe because
// everything these two processes write is JSON — and a line that is not is not
// a log entry this suite has anything to say about.
func composeLogs(t *testing.T, service string, since time.Time) []entry {
	t.Helper()
	root, err := repositoryRoot()
	if err != nil {
		t.Fatalf("%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "logs", "--no-color",
		"--since", since.UTC().Format(time.RFC3339), service)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("read the logs of %s: %v", service, err)
	}
	var found []entry
	for line := range strings.SplitSeq(string(out), "\n") {
		at := strings.Index(line, "{")
		if at < 0 {
			continue
		}
		parsed := entry{raw: line[at:]}
		var fields map[string]any
		if err := json.Unmarshal([]byte(parsed.raw), &fields); err != nil {
			continue
		}
		parsed.fields = fields
		found = append(found, parsed)
	}
	return found
}
