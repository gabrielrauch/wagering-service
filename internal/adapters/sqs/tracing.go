package sqs

import (
	"context"

	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// The service name every call this package makes is against.
const rpcService = "SQS"

// traced puts one client span around each call against SQS.
//
// # Why this is written here and not taken from the contrib module
//
// go.opentelemetry.io/contrib/…/otelaws does exactly this, and importing it
// links the S3, DynamoDB and SNS SDKs into both binaries: its attribute setters
// for those services live in the same package as the middleware, so the import
// graph reaches all three whether a service uses them or not. That is a large
// dependency for a service that talks to one queue, and it is a large
// dependency in the two containers this tree ships. The middleware itself is
// the twenty lines below.
//
// It is registered at Initialize rather than at Finalize or Deserialize, so the
// span covers the whole call including the SDK's own retries. A span per
// attempt would be more detailed and less useful: what a reader wants to know
// is how long SendMessageBatch took, and the retries are why.
func traced(t *telemetry.Telemetry) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(
			middleware.InitializeMiddlewareFunc("Telemetry", span(t)),
			middleware.After,
		)
	}
}

// span is the middleware itself.
//
// The span carries the operation and nothing about the message. A queue URL
// names an account, a body is a financial payload and a message attribute set
// is the trace context this service just injected — none of them belongs on a
// span that exists to say how long a call took.
func span(t *telemetry.Telemetry) func(
	context.Context, middleware.InitializeInput, middleware.InitializeHandler,
) (middleware.InitializeOutput, middleware.Metadata, error) {
	return func(
		ctx context.Context,
		in middleware.InitializeInput,
		next middleware.InitializeHandler,
	) (middleware.InitializeOutput, middleware.Metadata, error) {
		operation := middleware.GetOperationName(ctx)
		ctx, call := t.Start(ctx, rpcService+"."+operation,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.String(telemetry.KeyRPCSystem, telemetry.RPCSystemAWS),
				attribute.String(telemetry.KeyRPCService, rpcService),
				attribute.String(telemetry.KeyRPCMethod, operation),
			))
		defer call.End()

		out, metadata, err := next.HandleInitialize(ctx, in)
		if err != nil {
			// The class and not the message, for the reason
			// [telemetry.Telemetry.Failed] gives. This package already turns an
			// AWS error into a class that says whether the call may be made
			// again, which is the one thing a reader of a trace can act on.
			t.Failed(call, string(app.ClassOf(fail(operation, err))))
		}
		return out, metadata, err
	}
}
