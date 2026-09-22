//go:build integration

package sqs

import (
	"encoding/json"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// TestTheDeployedScriptProvisionsWhatTheAdapterAssumes reads back the queues
// this repository's own init script created.
//
// It is the only place the queue parameters are checked against anything, and
// they are not decoration: the consumer's unit of work has to fit inside the
// visibility timeout, the redrive policy is what stops a message that cannot be
// handled from holding the head of its wallet's group forever, and
// deduplication being explicit rather than content-based is what lets the
// envelope's own message id be the identity both the queue and the inbox use.
func TestTheDeployedScriptProvisionsWhatTheAdapterAssumes(t *testing.T) {
	requireLocalStack(t)

	for _, name := range []string{inboundQueue, deadLetter, outboundQueue} {
		t.Run(name, func(t *testing.T) {
			queue := openQueue(t, Config{Name: name})
			if queue.Name() != name {
				t.Errorf("name = %q, want %q", queue.Name(), name)
			}
			attributes := queueAttributes(t, name)
			if attributes["FifoQueue"] != "true" {
				t.Errorf("FifoQueue = %q, want true: ordering per wallet depends on it",
					attributes["FifoQueue"])
			}
			if attributes["ContentBasedDeduplication"] != "false" {
				t.Errorf("ContentBasedDeduplication = %q, want false: two bodies differing "+
					"only in whitespace must be one message",
					attributes["ContentBasedDeduplication"])
			}
			// Fourteen days, the SQS maximum, on all three. On the dead-letter
			// queue it is the window an operator has to notice; on the other
			// two it is what survives an outage over a long weekend.
			if got := attributes["MessageRetentionPeriod"]; got != "1209600" {
				t.Errorf("MessageRetentionPeriod = %q, want 1209600", got)
			}
		})
	}

	t.Run("the outbound queue also long-polls by default", func(t *testing.T) {
		attributes := queueAttributes(t, outboundQueue)
		if got := attributes["ReceiveMessageWaitTimeSeconds"]; got != "20" {
			t.Errorf("ReceiveMessageWaitTimeSeconds = %q, want 20", got)
		}
	})

	t.Run("the inbound queue's visibility and redrive", func(t *testing.T) {
		attributes := queueAttributes(t, inboundQueue)
		if got := attributes["VisibilityTimeout"]; got != "30" {
			t.Errorf("VisibilityTimeout = %q, want 30", got)
		}
		if got := attributes["ReceiveMessageWaitTimeSeconds"]; got != "20" {
			t.Errorf("ReceiveMessageWaitTimeSeconds = %q, want 20: a consumer that forgets "+
				"to ask for long polling must not spin", got)
		}
		var redrive struct {
			DeadLetterTargetArn string `json:"deadLetterTargetArn"`
			MaxReceiveCount     int    `json:"maxReceiveCount"`
		}
		raw := attributes["RedrivePolicy"]
		if raw == "" {
			t.Fatalf("no RedrivePolicy: a message that cannot be handled would stay at the "+
				"head of its group forever (attributes: %v)", attributes)
		}
		if err := json.Unmarshal([]byte(raw), &redrive); err != nil {
			t.Fatalf("read the redrive policy %q: %v", raw, err)
		}
		if redrive.MaxReceiveCount != 5 {
			t.Errorf("maxReceiveCount = %d, want 5", redrive.MaxReceiveCount)
		}
		if !strings.HasSuffix(redrive.DeadLetterTargetArn, ":"+deadLetter) {
			t.Errorf("deadLetterTargetArn = %q, want it to name %s", redrive.DeadLetterTargetArn,
				deadLetter)
		}
	})

	// The broker's own access control, by IAM role. LocalStack Community does
	// not enforce it, so what can be asserted here is that the policy the
	// script provisions says what it is meant to — a denied call cannot be
	// demonstrated against this backend, and on AWS it would be.
	t.Run("the resource policies", func(t *testing.T) {
		const (
			producer = "arn:aws:iam::000000000000:role/wagering-producer"
			worker   = "arn:aws:iam::000000000000:role/wagering-worker"
			api      = "arn:aws:iam::000000000000:role/wagering-api"
		)

		inbound := queuePolicy(t, inboundQueue)
		if !inbound.allows(producer, "sqs:SendMessage") {
			t.Errorf("%s does not let the producer role send, and the providers' "+
				"integration is what sends", inboundQueue)
		}
		for _, action := range []string{
			"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility",
		} {
			if !inbound.allows(worker, action) {
				t.Errorf("%s does not grant %s to the worker role, and the consumer "+
					"cannot run without it", inboundQueue, action)
			}
		}
		if inbound.allows(api, "sqs:SendMessage") {
			t.Errorf("%s lets the API role send, and the API never puts anything on a "+
				"queue itself: a submission is an outbox row the worker publishes",
				inboundQueue)
		}
		if got := inbound.granted("sqs:SendMessage"); !slices.Equal(got, []string{producer}) {
			t.Errorf("%s may be sent to by %v, want the producer role only", inboundQueue, got)
		}

		outbound := queuePolicy(t, outboundQueue)
		if got := outbound.granted("sqs:SendMessage"); !slices.Equal(got, []string{worker}) {
			t.Errorf("%s may be sent to by %v, want the worker role only: it is where "+
				"the outbox publisher writes, and nothing else may put an event there",
				outboundQueue, got)
		}

		dlq := queuePolicy(t, deadLetter)
		if got := dlq.granted("sqs:ReceiveMessage"); !slices.Equal(got, []string{worker}) {
			t.Errorf("%s may be received from by %v, want the worker role only",
				deadLetter, got)
		}
	})
}

// queueAttributes reads a queue's whole attribute set through the raw client.
func queueAttributes(t *testing.T, name string) map[string]string {
	t.Helper()
	url, err := sharedSDK.GetQueueUrl(t.Context(), &awssqs.GetQueueUrlInput{
		QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	attributes, err := sharedSDK.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl:       url.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		t.Fatalf("read the attributes of %s: %v", name, err)
	}
	return attributes.Attributes
}

// queuePolicy reads a queue's resource policy back and parses it.
func queuePolicy(t *testing.T, name string) policy {
	t.Helper()
	raw := queueAttributes(t, name)["Policy"]
	if raw == "" {
		t.Fatalf("%s has no Policy attribute: any credential in the account could send to "+
			"it or read it", name)
	}
	var parsed policy
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("read the policy of %s %q: %v", name, raw, err)
	}
	if parsed.Version != "2012-10-17" {
		t.Errorf("the policy of %s is version %q, want 2012-10-17", name, parsed.Version)
	}
	return parsed
}

// policy is the part of an SQS resource policy these assertions read.
type policy struct {
	Version   string      `json:"Version"`
	Statement []statement `json:"Statement"`
}

type statement struct {
	Effect    string    `json:"Effect"`
	Principal principal `json:"Principal"`
	Action    oneOrMany `json:"Action"`
}

// principal is `{"AWS": ...}`, or the string "*", which IAM also admits.
type principal struct {
	AWS oneOrMany `json:"AWS"`
}

func (p *principal) UnmarshalJSON(raw []byte) error {
	if string(raw) == `"*"` {
		p.AWS = oneOrMany{"*"}
		return nil
	}
	type plain principal
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*p = principal(decoded)
	return nil
}

// oneOrMany is a field IAM lets be written as one string or as a list of them.
type oneOrMany []string

func (o *oneOrMany) UnmarshalJSON(raw []byte) error {
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		*o = oneOrMany{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return err
	}
	*o = many
	return nil
}

// allows reports whether an Allow statement names both the principal and the
// action. Exact names: the script writes no wildcards, and a wildcard that
// crept in should read as a change rather than as a grant.
func (p policy) allows(who, action string) bool {
	return slices.Contains(p.granted(action), who)
}

// granted lists every principal some Allow statement grants the action to,
// sorted and without repeats.
func (p policy) granted(action string) []string {
	var principals []string
	for _, s := range p.Statement {
		if s.Effect != "Allow" || !slices.Contains(s.Action, action) {
			continue
		}
		for _, who := range s.Principal.AWS {
			if !slices.Contains(principals, who) {
				principals = append(principals, who)
			}
		}
	}
	slices.Sort(principals)
	return principals
}

// TestOnStartRefusesAQueueNobodyProvisioned is the reason resolution happens at
// start-up at all: a name that is not there has to fail while somebody is
// watching the deployment, not on the first receive at three in the morning.
func TestOnStartRefusesAQueueNobodyProvisioned(t *testing.T) {
	requireLocalStack(t)

	queue, err := NewQueue(Config{Client: sharedSDK, Name: "nobody-provisioned-this.fifo"})
	if err != nil {
		t.Fatalf("build a queue: %v", err)
	}
	err = queue.OnStart(t.Context())
	if err == nil {
		t.Fatalf("OnStart on a queue that does not exist = nil, want a refusal")
	}
	if got := app.ClassOf(err); got != app.Unretryable {
		t.Errorf("class = %s, want %s: the queue is not going to start existing", got,
			app.Unretryable)
	}
	if !strings.Contains(err.Error(), "nobody-provisioned-this.fifo") {
		t.Errorf("error = %q, want it to name the queue", err)
	}
	// And the handle is still unusable, rather than half-built.
	if _, err := queue.Receive(t.Context()); err == nil {
		t.Errorf("a queue whose OnStart failed still received, want a refusal")
	}
}

// TestOnStartIsBoundedByItsOwnTimeout points a client at something that accepts
// connections and then says nothing, which is the failure a connect timeout
// does not catch and the one that hangs a deployment.
func TestOnStartIsBoundedByItsOwnTimeout(t *testing.T) {
	client, err := NewClient(t.Context(), ClientConfig{
		Region: region, Endpoint: silentEndpoint(t)})
	if err != nil {
		t.Fatalf("build a client: %v", err)
	}
	queue, err := NewQueue(Config{
		Client: client, Name: inboundQueue, ResolveTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("build a queue: %v", err)
	}

	// No deadline on the caller's context: the bound under test is the queue's
	// own, and a context that also expired would prove nothing about it.
	started := time.Now()
	err = queue.OnStart(t.Context())
	took := time.Since(started)

	if err == nil {
		t.Fatalf("OnStart against an endpoint that never answers = nil, want a refusal")
	}
	if took > 5*time.Second {
		t.Errorf("OnStart took %s, want it bounded near its 500ms timeout", took)
	}
	if got := app.ClassOf(err); got != app.Retryable {
		t.Errorf("class = %s, want %s: nothing was refused, nothing answered", got, app.Retryable)
	}
}

// silentEndpoint is an address that completes a TCP handshake and then never
// writes a byte.
//
// A closed port would not do: it is refused immediately, which is the failure
// mode a timeout is not needed for. What a timeout exists for is the peer that
// is there and is not answering.
func silentEndpoint(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// One cleanup, registered from the test's own goroutine and closing
	// everything the accept loop took. Registering a cleanup per connection
	// from inside that loop would be a cleanup registered after cleanups had
	// begun for any connection accepted late, and that one would never run.
	var (
		mu   sync.Mutex
		held []net.Conn
		shut bool
	)
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		shut = true
		for _, conn := range held {
			_ = conn.Close()
		}
	})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Held open and never answered. A connection accepted after the
			// cleanup has run is closed on the spot rather than kept.
			mu.Lock()
			if shut {
				mu.Unlock()
				_ = conn.Close()
				return
			}
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	return "http://" + listener.Addr().String()
}
