//go:build integration

// The one trace that has to span a command and the event it caused, proved
// against a real PostgreSQL and a real LocalStack.
package messaging

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// attributed is one message read off a queue, with its message attributes.
//
// The suite's own peek does not ask for them — nothing else here is about trace
// context — and asking for them everywhere would be every other scenario paying
// for this one.
type attributed struct {
	body       string
	attributes map[string]string
}

// awaitAttributed drains a queue until it has seen want messages and returns
// them with their attributes.
//
// It DELETES what it reads, unlike the suite's own peek, and on a FIFO queue it
// has to: a received message locks its group until it is deleted or its
// visibility expires, and every event of one wallet is one group — so a reader
// that did not delete would be handed the same head message for ever and never
// see the three behind it. Nothing else reads this queue: outbound(t) provisions
// one per test.
func awaitAttributed(t *testing.T, queue string, want int, within time.Duration) []attributed {
	t.Helper()
	deadline := time.Now().Add(within)
	byID := map[string]attributed{}
	for {
		received, err := sharedSDK.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
			QueueUrl:              queueURL(t, queue),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       pollSeconds(time.Second),
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatalf("read %s: %v", queue, err)
		}
		for _, m := range received.Messages {
			carried := make(map[string]string, len(m.MessageAttributes))
			for name, value := range m.MessageAttributes {
				carried[name] = aws.ToString(value.StringValue)
			}
			byID[aws.ToString(m.MessageId)] = attributed{
				body:       aws.ToString(m.Body),
				attributes: carried,
			}
			if _, err := sharedSDK.DeleteMessage(t.Context(), &awssqs.DeleteMessageInput{
				QueueUrl:      queueURL(t, queue),
				ReceiptHandle: m.ReceiptHandle,
			}); err != nil {
				t.Fatalf("delete a message from %s: %v", queue, err)
			}
		}
		if len(byID) >= want {
			out := make([]attributed, 0, len(byID))
			for _, m := range byID {
				out = append(out, m)
			}
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s held %d messages after %s, wanted %d", queue, len(byID), within, want)
		}
	}
}

// TestOneTraceSpansTheCommandAndTheEventItCaused is the scenario the outbox
// carries trace context for.
//
// The requirement is ONE trace from the request a provider made to the event
// this service published for it — not two traces sharing a correlation. Between
// the two there is a durable, asynchronous handover: the command commits, the
// request is answered, and a publisher in another process sends the event later
// with no memory of any of it. The only thing that crosses is the outbox row.
//
// Everything below is real. The command runs through the real transaction
// manager against PostgreSQL, the row is written by the real adapter, the real
// publisher claims it, and the message is read back off LocalStack. What is
// asserted is the join: the traceparent on the wire names the trace the command
// ran in.
//
// It also asserts the cost that must NOT have been paid. The body on the queue
// is the envelope and nothing else — the member the adapter wrote into the row
// is stripped by the claim — so a downstream consumer reading the body strictly
// sees exactly what it saw before any of this existed.
func TestOneTraceSpansTheCommandAndTheEventItCaused(t *testing.T) {
	t.Parallel()

	spans := tracetest.NewSpanRecorder()
	reporting, err := telemetry.New(telemetry.Config{
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		t.Fatalf("build the telemetry: %v", err)
	}

	s := newStack(t, withTelemetry(reporting))
	const player = "player-traced"
	wallet := s.openWallet(t, player, "100.00")

	// The span an HTTP request would have opened, and the command inside it.
	ctx, request := reporting.Start(t.Context(), "POST /wagering/transactions",
		oteltrace.WithSpanKind(oteltrace.SpanKindServer))
	acting, err := wagering.NewProvider(provider)
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	principal, err := app.NewProviderPrincipal(acting, "api-client")
	if err != nil {
		t.Fatalf("principal: %v", err)
	}
	op := operationOf("BET", "external-traced", player, wallet, "25.00")
	result, err := s.wagering.Submit(ctx, app.SubmitOperation{
		Principal:   principal,
		Correlation: "thread-traced",
		Fields:      op.fields(),
	})
	if err != nil {
		t.Fatalf("submit the operation: %v", err)
	}
	request.End()

	// The trace the request ran in. Everything below has to be in it.
	wanted := request.SpanContext().TraceID()
	if !wanted.IsValid() {
		t.Fatal("the request span has no trace, so this test cannot mean anything")
	}

	name := outbound(t)
	startPublisher(t, s, openQueueOn(t, name, sharedSDK), publisherSettings{
		name:      "publisher-traced",
		hold:      30 * time.Second,
		telemetry: reporting,
	})

	// Four events: the wallet's opening pair and the bet's pair.
	published := awaitAttributed(t, name, 4, settleBudget)
	message, found := eventFor(t, published, result.TransactionID.String())
	if !found {
		t.Fatalf("no published event names transaction %s; the queue held %d messages",
			result.TransactionID, len(published))
	}

	carried := message.attributes["traceparent"]
	if carried == "" {
		t.Fatalf("the published event carries no traceparent: %v", message.attributes)
	}
	continued := oteltrace.SpanContextFromContext(
		reporting.Extract(t.Context(), message.attributes))
	if got := continued.TraceID(); got != wanted {
		t.Errorf("the published event is in trace %s and the request was in %s; "+
			"they must be one trace", got, wanted)
	}
	if got := message.attributes["correlationId"]; got != "thread-traced" {
		t.Errorf("the published event carries correlation %q, wanted thread-traced", got)
	}

	// And the body is the envelope, with nothing of this in it.
	var members map[string]json.RawMessage
	if err := json.Unmarshal([]byte(message.body), &members); err != nil {
		t.Fatalf("the published body is not JSON: %v\n%s", err, message.body)
	}
	for _, name := range []string{"$trace", "traceparent", "tracestate"} {
		if _, present := members[name]; present {
			t.Errorf("the published body carries %q, which is not part of the envelope:\n%s",
				name, message.body)
		}
	}
	for _, name := range []string{"eventId", "eventType", "aggregateId", "correlationId", "data"} {
		if _, present := members[name]; !present {
			t.Errorf("the published body is missing %q:\n%s", name, message.body)
		}
	}

	// The publishing span itself is in the trace too, which is what makes the
	// flow visible as one thread rather than as a request and a message that
	// happen to share an identifier.
	var publishing []sdktrace.ReadOnlySpan
	for _, span := range spans.Ended() {
		if span.Name() == telemetry.SpanPublishEvent &&
			span.SpanContext().TraceID() == wanted {
			publishing = append(publishing, span)
		}
	}
	if len(publishing) == 0 {
		t.Errorf("no %s span is in the request's trace", telemetry.SpanPublishEvent)
	}
}

// eventFor finds the published event that names one transaction.
func eventFor(t *testing.T, messages []attributed, transactionID string) (attributed, bool) {
	t.Helper()
	for _, m := range messages {
		var envelope struct {
			EventType string `json:"eventType"`
			Data      struct {
				TransactionID string `json:"transactionId"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(m.body), &envelope); err != nil {
			t.Fatalf("read a published envelope: %v\n%s", err, m.body)
		}
		if envelope.EventType == "WagerTransactionProcessed" &&
			envelope.Data.TransactionID == transactionID {
			return m, true
		}
	}
	return attributed{}, false
}
