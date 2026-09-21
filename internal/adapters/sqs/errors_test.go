package sqs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// operation wraps err the way the SDK's middleware does, so that these cases
// exercise the chain a caller will actually be handed rather than the bare
// error underneath it.
func operation(err error) error {
	return &smithy.OperationError{ServiceID: "SQS", OperationName: "ReceiveMessage", Err: err}
}

// response wraps an API error in the HTTP response it arrived on.
func response(status int, err error) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      err,
	}
}

// timeoutError is a network timeout, which is what a request that was sent and
// never answered looks like below the SDK.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.Error = timeoutError{}

func TestClassification(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class app.Class
	}{
		{
			name:  "a queue that does not exist is not going to start existing",
			err:   operation(response(400, &types.QueueDoesNotExist{})),
			class: app.Unretryable,
		},
		{
			name:  "a receipt handle from an earlier delivery is spent",
			err:   operation(response(400, &types.ReceiptHandleIsInvalid{})),
			class: app.Unretryable,
		},
		{
			name:  "a body SQS cannot carry will not be carried next time either",
			err:   operation(response(400, &types.InvalidMessageContents{})),
			class: app.Unretryable,
		},
		{
			name: "a throttle is a client fault that must still be retried",
			err: operation(response(400, &smithy.GenericAPIError{
				Code: "ThrottlingException", Fault: smithy.FaultClient})),
			class: app.Retryable,
		},
		{
			name:  "the in-flight limit is a queue that is busy, not a request that is wrong",
			err:   operation(response(403, &types.OverLimit{})),
			class: app.Retryable,
		},
		{
			name: "a server fault is the service's problem",
			err: operation(response(500, &smithy.GenericAPIError{
				Code: "InternalError", Fault: smithy.FaultServer})),
			class: app.Retryable,
		},
		{
			name: "a 503 counts even when the body said nothing useful",
			err: operation(response(503, &smithy.GenericAPIError{
				Code: "Unknown", Fault: smithy.FaultUnknown})),
			class: app.Retryable,
		},
		{
			name: "too many requests counts even without a code",
			err: operation(response(429, &smithy.GenericAPIError{
				Code: "Unknown", Fault: smithy.FaultUnknown})),
			class: app.Retryable,
		},
		{
			name: "a client fault with an ordinary status is refused for good",
			err: operation(response(400, &smithy.GenericAPIError{
				Code: "InvalidParameterValue", Fault: smithy.FaultClient})),
			class: app.Unretryable,
		},
		{
			name:  "a request that never reached a server recorded nothing",
			err:   operation(&smithyhttp.RequestSendError{Err: errors.New("connection refused")}),
			class: app.Retryable,
		},
		{
			name:  "a timeout on the wire recorded nothing",
			err:   operation(timeoutError{}),
			class: app.Retryable,
		},
		{
			name:  "a caller that gave up was not refused",
			err:   operation(context.Canceled),
			class: app.Retryable,
		},
		{
			name:  "a deadline that ran out was not a refusal either",
			err:   operation(fmt.Errorf("send: %w", context.DeadlineExceeded)),
			class: app.Retryable,
		},
		{
			name:  "a request this process could not even build is a defect",
			err:   operation(&smithy.SerializationError{Err: errors.New("bad shape")}),
			class: app.Unretryable,
		},
		{
			name:  "a failure nobody recognised is not an invitation to retry",
			err:   errors.New("something nobody classified"),
			class: app.Unretryable,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			classified := fail("receive from wager-transactions.fifo", c.err)
			if got := app.ClassOf(classified); got != c.class {
				t.Errorf("class = %s, want %s", got, c.class)
			}
			if !errors.Is(classified, c.err) {
				t.Errorf("the chain lost the cause: %v", classified)
			}
			if !strings.Contains(classified.Error(), "receive from wager-transactions.fifo") {
				t.Errorf("error = %q, want it to name the work in progress", classified)
			}
		})
	}
}

func TestFailPassesNilThrough(t *testing.T) {
	if err := fail("delete a message", nil); err != nil {
		t.Errorf("fail(nil) = %v, want nil", err)
	}
}

func TestMissingRecognisesTheQueuesAbsence(t *testing.T) {
	absent := operation(response(400, &types.QueueDoesNotExist{}))
	if !missing(absent) {
		t.Errorf("missing(%v) = false, want true", absent)
	}
	present := operation(response(400, &types.ReceiptHandleIsInvalid{}))
	if missing(present) {
		t.Errorf("missing(%v) = true, want false", present)
	}
}

// TestEntryFailureClassification is the per-entry half. It matters separately
// because SendMessageBatch reports refusals inside a 200, so nothing above ever
// sees them and the fault flag is the only thing the service offers.
func TestEntryFailureClassification(t *testing.T) {
	cases := []struct {
		name        string
		code        string
		senderFault bool
		class       app.Class
	}{
		{
			name:        "the sender's fault is the sender's fault",
			code:        "InvalidParameterValue",
			senderFault: true,
			class:       app.Unretryable,
		},
		{
			name:        "a throttle is retried whatever the flag says",
			code:        "RequestThrottled",
			senderFault: true,
			class:       app.Retryable,
		},
		{
			name: "a body SQS cannot carry is settled by its code, not by the flag",
			// LocalStack reports exactly this: SenderFault false on a refusal
			// that is entirely the message's own. Believing the flag would have
			// a publisher reschedule it forever.
			code:        "InvalidMessageContents",
			senderFault: false,
			class:       app.Unretryable,
		},
		{
			name:        "a service fault is worth another attempt",
			code:        "InternalError",
			senderFault: false,
			class:       app.Retryable,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := entryFailure(c.code, "the service said so", c.senderFault)
			if got := app.ClassOf(err); got != c.class {
				t.Errorf("class = %s, want %s", got, c.class)
			}
			if !strings.Contains(err.Error(), c.code) {
				t.Errorf("error = %q, want it to name the code", err)
			}
		})
	}
}
