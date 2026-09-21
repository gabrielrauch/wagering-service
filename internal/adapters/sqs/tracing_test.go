package sqs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/smithy-go/middleware"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// handling is a stub for the rest of the SDK's middleware stack.
type handling struct{ err error }

func (h handling) HandleInitialize(
	context.Context, middleware.InitializeInput,
) (middleware.InitializeOutput, middleware.Metadata, error) {
	return middleware.InitializeOutput{}, middleware.Metadata{}, h.err
}

// TestACallAgainstTheQueueIsOneSpanNamingTheOperation pins what this package's
// own middleware records, and what it must not.
//
// It is written by hand rather than taken from the contrib module, because that
// module links the S3, DynamoDB and SNS SDKs into both of this service's
// binaries — its attribute setters for those services live in the same package
// as the middleware. The whole of what was given up is asserted here.
//
// The span carries the OPERATION and nothing about the message. A queue URL
// names an account, a body is a financial payload, and the message attributes
// are the trace context this service has just injected — none of them belongs
// on a span that exists to say how long a call took.
func TestACallAgainstTheQueueIsOneSpanNamingTheOperation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// failed is what the rest of the stack reports.
		failed error
		// class is the app.Class the span must end under, or "" for a call that
		// succeeded.
		class string
	}{
		{name: "a call the service answered", failed: nil},
		{
			name:   "a call the service refused",
			failed: errors.New("the queue is not answering"),
			class:  string(app.Unretryable),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			spans := tracetest.NewSpanRecorder()
			reporting, err := telemetry.New(telemetry.Config{
				TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
			})
			if err != nil {
				t.Fatalf("build the telemetry: %v", err)
			}

			ctx := middleware.WithOperationName(t.Context(), "SendMessageBatch")
			_, _, got := span(reporting)(ctx, middleware.InitializeInput{}, handling{err: c.failed})
			if !errors.Is(got, c.failed) {
				t.Fatalf("the middleware changed the call's answer to %v", got)
			}

			ended := spans.Ended()
			if len(ended) != 1 {
				t.Fatalf("%d spans finished, wanted one", len(ended))
			}
			if got := ended[0].Name(); got != "SQS.SendMessageBatch" {
				t.Errorf("the span is named %q, wanted SQS.SendMessageBatch", got)
			}
			for key, want := range map[string]string{
				telemetry.KeyRPCSystem:  telemetry.RPCSystemAWS,
				telemetry.KeyRPCService: "SQS",
				telemetry.KeyRPCMethod:  "SendMessageBatch",
			} {
				if got := attributeOf(ended[0], key); got != want {
					t.Errorf("%s is %q, wanted %q", key, got, want)
				}
			}
			if got := ended[0].Status().Description; got != c.class {
				t.Errorf("the span ended under %q, wanted %q", got, c.class)
			}
			// The failure's own words are never on the span, for the reason
			// telemetry.Telemetry.Failed gives: a class is a published
			// vocabulary, a driver's message is whatever the service put in it.
			if c.failed != nil && strings.Contains(renderedSpan(ended[0]), c.failed.Error()) {
				t.Errorf("the span carried the failure's message:\n%s", renderedSpan(ended[0]))
			}
		})
	}
}

// attributeOf reads one string attribute off a span.
func attributeOf(span sdktrace.ReadOnlySpan, key string) string {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.String()
		}
	}
	return ""
}

// renderedSpan is everything a span says, for a failure message.
func renderedSpan(span sdktrace.ReadOnlySpan) string {
	parts := []string{span.Name(), span.Status().Description}
	for _, attr := range span.Attributes() {
		parts = append(parts, string(attr.Key)+"="+attr.Value.String())
	}
	for _, event := range span.Events() {
		parts = append(parts, event.Name)
		for _, attr := range event.Attributes {
			parts = append(parts, string(attr.Key)+"="+attr.Value.String())
		}
	}
	return strings.Join(parts, "\n")
}
