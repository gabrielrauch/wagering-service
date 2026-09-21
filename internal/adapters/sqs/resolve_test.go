//go:build integration

package sqs

import (
	"encoding/json"
	"net"
	"strings"
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
		})
	}

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
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Held open, never answered, and closed when the test ends.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	return "http://" + listener.Addr().String()
}
