package sqs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// ErrNotResolved reports a queue used before [Queue.OnStart] resolved its URL.
//
// It is this package's own because nothing outside it has a name for the
// condition: it is a wiring mistake, not an outcome, and it is [app.Unretryable]
// for the reason every defect is — the next call will be made against the same
// unresolved queue.
var ErrNotResolved = errors.New("sqs: the queue's URL is not resolved yet")

// ErrSendAborted reports a message [Queue.SendBatch] never attempted, because
// an earlier chunk's call failed and the send stopped there.
//
// It exists so that a publisher reading the results can tell a message SQS
// refused from one that was never offered to it. Both are rescheduled, so the
// distinction changes no behaviour — it changes what the log line says at three
// in the morning, which is the only audience it has. It is [app.Retryable]: not
// being attempted is not evidence of anything.
var ErrSendAborted = errors.New("sqs: the send stopped before this message")

// throttlingCodes are the API error codes that mean "not now", said in a
// response SQS returns with a 4xx status.
//
// They need their own table for exactly that reason. Every other judgement
// below can lean on the fault the service declares, and a throttle declares a
// client fault — which is true, in the sense that the client asked for too
// much, and is the opposite of what a caller should do about it.
var throttlingCodes = map[string]bool{
	"ThrottlingException":                    true,
	"Throttling":                             true,
	"RequestThrottled":                       true,
	"RequestThrottledException":              true,
	"TooManyRequestsException":               true,
	"RequestLimitExceeded":                   true,
	"ProvisionedThroughputExceededException": true,
	"SlowDown":                               true,
	"KMS.ThrottlingException":                true,
	// OverLimit is the in-flight message limit, which is a queue that is full
	// of work rather than a request that is wrong. Receiving again once
	// something has been deleted succeeds.
	"OverLimit": true,
}

// retryableStatuses are the HTTP statuses that mean the request may be made
// again, where the status is more reliable than anything in the body.
//
// 5xx is handled as a class below; these are the two 4xx that are not the
// request's fault. 429 is a throttle with no code worth trusting, and 408 is
// the server giving up on a request it never read — neither did anything.
var retryableStatuses = map[int]bool{
	http.StatusRequestTimeout:  true,
	http.StatusTooManyRequests: true,
}

// entryFaultCodes are the per-entry batch failures that are the message's own
// fault, whatever the service says about whose fault it was.
//
// SendMessageBatch reports a SenderFault flag per failed entry, and believing
// it is the right default — but it is not always right. LocalStack reports
// InvalidMessageContents with SenderFault false, and a publisher that took that
// at face value would reschedule a message that can never be sent, forever,
// holding the head of its wallet's group while it did. These codes describe the
// entry rather than the moment, so they settle it.
var entryFaultCodes = map[string]bool{
	"InvalidMessageContents":                      true,
	"InvalidParameterValue":                       true,
	"InvalidParameterValueException":              true,
	"MissingRequiredParameterException":           true,
	"InvalidAttributeName":                        true,
	"InvalidAttributeValue":                       true,
	"AWS.SimpleQueueService.InvalidBatchEntryId":  true,
	"AWS.SimpleQueueService.BatchRequestTooLong":  true,
	"AWS.SimpleQueueService.UnsupportedOperation": true,
}

// fail turns an SDK error into the error a worker is entitled to see.
//
// what names the work in progress, so a failure says which call it came from
// without the caller having to recognise an operation name out of the SDK's own
// message.
//
// Note what has already happened by the time this is reached: the SDK retried
// on its own schedule and gave up. Classifying Retryable here is therefore not
// "try again immediately" — it is "this recorded nothing, so the worker's own
// backoff may carry it", which is a different and slower loop.
func fail(what string, err error) error {
	if err == nil {
		return nil
	}
	if retryable(err) {
		return app.AsRetryable(fmt.Errorf("%s: %w", what, err))
	}
	return app.AsUnretryable(fmt.Errorf("%s: %w", what, err))
}

// retryable reports whether err is the service or the network declining this
// attempt rather than refusing this work.
//
// The order is the contract, and each step is there because the step after it
// would answer wrongly:
//
//  1. The caller's own context ending. It is not a refusal at all, and nothing
//     was recorded.
//  2. A throttle, by code, before the fault is consulted — see [throttlingCodes].
//  3. The HTTP status, where SQS returned one. A 5xx is the service failing to
//     answer for reasons that have nothing to do with the request.
//  4. The fault the service declared. A server fault is the service's problem;
//     a client fault is a request that will be refused again identically.
//  5. A request that never reached a server: a connection that failed, a
//     timeout on the wire. Nothing on the other side saw it.
//
// Anything left is Unretryable. A failure nobody recognised is not one anybody
// has established is safe to repeat — the rule the PostgreSQL adapter states
// and the reason a serialisation defect in this process does not become an
// infinite loop.
func retryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	apiErr, isAPI := errors.AsType[smithy.APIError](err)
	if isAPI && throttlingCodes[apiErr.ErrorCode()] {
		return true
	}
	// Outside the branch above rather than inside it, because a response this
	// process could not parse into an API error still carries a status, and a
	// 503 with an unrecognisable body is still a 503.
	if status, hasStatus := errors.AsType[httpStatus](err); hasStatus {
		code := status.HTTPStatusCode()
		if code >= http.StatusInternalServerError || retryableStatuses[code] {
			return true
		}
	}
	if isAPI {
		return apiErr.ErrorFault() == smithy.FaultServer
	}
	if conn, ok := errors.AsType[connectionFailure](err); ok && conn.ConnectionError() {
		return true
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return true
	}
	return false
}

// httpStatus is the part of a transport error that names the status SQS
// answered with.
//
// Matched as an interface rather than as a concrete type because two of them
// implement it — the smithy transport's response error and the AWS SDK's own
// wrapper around it — and which one arrives depends on where in the middleware
// stack the failure was noticed.
type httpStatus interface {
	error
	HTTPStatusCode() int
}

// connectionFailure is the part of a transport error that says the request
// never reached a server. Matched as an interface for the same reason.
type connectionFailure interface {
	error
	ConnectionError() bool
}

// entryFailure turns one refused batch entry into a classified error.
//
// The codes settle it first, then the flag; see [entryFaultCodes] for why the
// flag alone is not enough and [throttlingCodes] for why a throttle has to be
// read before anything decides whose fault it was.
func entryFailure(code, message string, senderFault bool) error {
	err := fmt.Errorf("sqs: the queue refused the message: %s: %s", code, message)
	switch {
	case throttlingCodes[code]:
		return app.AsRetryable(err)
	case entryFaultCodes[code], senderFault:
		return app.AsUnretryable(err)
	default:
		return app.AsRetryable(err)
	}
}

// missing reports whether err is the queue this handle names not existing.
//
// Exported behaviour rather than an exported sentinel: a start-up failure and a
// readiness failure both want to say "that queue is not there" in a way an
// operator can act on, and both already carry the SDK's own typed exception.
func missing(err error) bool {
	_, ok := errors.AsType[*types.QueueDoesNotExist](err)
	return ok
}
