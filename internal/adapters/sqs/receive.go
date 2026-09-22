package sqs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// Message is one message taken off the queue.
//
// Body is the bytes as they arrived, not a decoded envelope. Decoding belongs
// to whoever knows what the queue carries, and a body this package could not
// parse would otherwise become a failure of the receive rather than of the one
// message it concerns.
type Message struct {
	// MessageID is the queue's own identifier for this message. It is not the
	// envelope's message id and must not be used as one: SQS mints it, the
	// sender does not, and it is not what the inbox keys on.
	MessageID string
	// ReceiptHandle identifies this delivery, not this message. It changes
	// every time the message is received, which is why deleting takes one and
	// why a handle from an earlier delivery no longer works.
	ReceiptHandle string
	// Body is the message as it was sent.
	Body []byte
	// Attributes are the message attributes, decoded. Nil when there were
	// none.
	Attributes map[string]string
	// ReceiveCount is how many times this message has now been delivered,
	// counting this delivery — so a first delivery reports one.
	//
	// It is here because it is the only attempt counter a redelivered message
	// carries: a consumer choosing how far to back off has no state of its own
	// to count with, and the queue's redrive policy is judged on this same
	// number. It is approximate, as SQS names it, and must not be used to
	// decide anything that has to be exact.
	ReceiveCount int
}

// Receive takes up to the configured number of messages, waiting the configured
// long-poll time for the first of them.
//
// An empty slice is not an error. Having nothing to do is a consumer's normal
// state, and reporting it as a failure would alert on an idle system.
//
// The call blocks for up to the wait time, so a caller that means to stop
// should cancel the context rather than wait for the poll to end — a cancelled
// long poll is [app.Retryable] and records nothing.
func (q *Queue) Receive(ctx context.Context) ([]Message, error) {
	url, err := q.resolved()
	if err != nil {
		return nil, err
	}
	input := &awssqs.ReceiveMessageInput{
		QueueUrl:            url,
		MaxNumberOfMessages: q.settings.maxMessages,
		WaitTimeSeconds:     q.settings.waitTime,
		// An open visibility timeout is modelled as a nil *int32 and flattened
		// to a plain zero here, and that zero means "leave the queue's own
		// timeout alone" only because the SDK omits a zero-valued
		// VisibilityTimeout from the request — the generated serialiser guards
		// it with `if v.VisibilityTimeout != 0`.
		//
		// This is worth stating because the SDK is not consistent about it.
		// ChangeMessageVisibility's serialiser has no such guard and always
		// writes the field, which is exactly why [Queue.Release] can hand a
		// message back by asking for zero. Two opposite conventions, one SDK,
		// and this package depends on both of them.
		//
		// The consequence of the omission going away is not subtle: every
		// received message would be handed to the next consumer immediately,
		// so a receive would no longer reserve anything. Both halves are
		// pinned by tests against a real queue —
		// TestAnOpenVisibilityTimeoutLeavesTheQueuesOwnInPlace for this one and
		// TestReleaseHandsTheMessageBackAtOnce for the other.
		VisibilityTimeout: aws.ToInt32(q.settings.visibilityTimeout),
		// Without this, message attributes are simply absent from the response
		// — not empty, absent — and the trace context a sender went to the
		// trouble of attaching disappears silently.
		MessageAttributeNames: []string{"All"},
		// Named one by one rather than "All". The set SQS can attach grows,
		// and a consumer that asked for all of them would start paying for
		// whatever is added next without anybody deciding to.
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	}
	received, err := q.settings.client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, fail("receive from "+q.settings.name, err)
	}
	if len(received.Messages) == 0 {
		return nil, nil
	}
	messages := make([]Message, 0, len(received.Messages))
	for _, message := range received.Messages {
		messages = append(messages, Message{
			MessageID:     aws.ToString(message.MessageId),
			ReceiptHandle: aws.ToString(message.ReceiptHandle),
			Body:          []byte(aws.ToString(message.Body)),
			Attributes:    decodeAttributes(message.MessageAttributes),
			ReceiveCount:  receiveCount(message.Attributes),
		})
	}
	return messages, nil
}

// receiveCount reads ApproximateReceiveCount, defaulting to one delivery.
//
// A count that is absent or unreadable becomes 1 rather than 0, because every
// message in hand has been delivered at least once and a zero would tell a
// backoff policy it was looking at a delivery that has not happened.
func receiveCount(attributes map[string]string) int {
	raw, ok := attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]
	if !ok {
		return 1
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		return 1
	}
	return count
}

// Delete removes one message from the queue.
//
// A consumer calls this only after the transaction that handled the message has
// committed. Deleting first would turn every crash in between into a lost
// operation; deleting after means a crash in between is a redelivery, which the
// inbox turns into a replay.
func (q *Queue) Delete(ctx context.Context, receiptHandle string) error {
	url, err := q.resolved()
	if err != nil {
		return err
	}
	if receiptHandle == "" {
		return app.AsUnretryable(errors.New("sqs: deleting a message needs its receipt handle"))
	}
	_, err = q.settings.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      url,
		ReceiptHandle: aws.String(receiptHandle),
	})
	return fail("delete a message from "+q.settings.name, err)
}

// ChangeVisibility sets how much longer a received message stays hidden,
// counted from now rather than from when it was received.
//
// This is how a transient failure is handed back: the message is not deleted,
// its visibility is pushed out by whatever the caller's backoff says, and the
// redelivery that follows finds the inbox empty and does the work again. It is
// not an extension of the existing timeout — the value replaces it, so passing
// a shorter duration than the time already spent brings the message back
// sooner.
func (q *Queue) ChangeVisibility(ctx context.Context, receiptHandle string, in time.Duration) error {
	url, err := q.resolved()
	if err != nil {
		return err
	}
	switch {
	case receiptHandle == "":
		return app.AsUnretryable(
			errors.New("sqs: changing a message's visibility needs its receipt handle"))
	case in < 0 || in > maxVisibilityTimeout:
		return app.AsUnretryable(fmt.Errorf("sqs: a visibility timeout runs from 0 to %s, got %s",
			maxVisibilityTimeout, in))
	case in%time.Second != 0:
		return app.AsUnretryable(
			fmt.Errorf("sqs: a visibility timeout is whole seconds, got %s", in))
	}
	_, err = q.settings.client.ChangeMessageVisibility(ctx,
		&awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          url,
			ReceiptHandle:     aws.String(receiptHandle),
			VisibilityTimeout: seconds(in),
		})
	return fail("change a message's visibility on "+q.settings.name, err)
}

// Release hands a message straight back, so that it is redelivered at once
// rather than after its visibility timeout has run out.
//
// It is [Queue.ChangeVisibility] with a zero duration, named because zero is
// not self-explanatory at a call site and because this is the shutdown path: on
// SIGTERM a consumer stops receiving, finishes what it can inside the deadline,
// and releases whatever is still in flight. Without it a clean shutdown looks
// exactly like a crash to the queue, and the work waits out a timeout that
// exists for the case where nobody released it.
func (q *Queue) Release(ctx context.Context, receiptHandle string) error {
	return q.ChangeVisibility(ctx, receiptHandle, 0)
}
