//go:build integration

// The queues a test takes, and the raw client it looks at them with.
//
// A queue pair each rather than the provisioned ones, because every scenario
// here turns on a queue being empty, or on a message being the only one in its
// group, or on a delivery count nothing else has spent — and a shared queue
// would make each of them depend on the order they ran in.
//
// What is borrowed from the provisioned queues instead is the only parameter
// this suite cannot invent: [maxReceives], the deployed retry limit. The
// visibility timeout is deliberately NOT borrowed. The deployed thirty seconds
// is asserted by internal/adapters/sqs against the script itself, and a suite
// that had to watch five deliveries at that timeout would spend two and a half
// minutes doing it; the scenarios below say in each case what their own timeout
// has to be for the assertion to mean anything.
package messaging

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// cleanupBudget bounds the calls a t.Cleanup makes. t.Context is already
// cancelled by the time cleanups run, so each of them needs a context of its
// own or every one of them fails.
const cleanupBudget = 15 * time.Second

// The bounds SQS puts on one receive's long poll, restated here because a probe
// states its budget as a duration and a receive takes whole seconds.
//
// The floor is the one that matters. A budget under a second truncates to zero,
// and zero is a SHORT poll — which samples a subset of the queue's hosts and
// answers empty while messages are waiting, the one thing a probe asserting
// "nothing came back" must never do. The ceiling is simply what SQS refuses
// past; a caller with a longer budget is served by several receives rather than
// by an InvalidParameterValue.
const (
	minPoll = time.Second
	maxPoll = 20 * time.Second
)

// pollSeconds is a probe's budget in the unit a receive takes, clamped to what
// SQS allows.
func pollSeconds(within time.Duration) int32 {
	return int32(min(max(within, minPoll), maxPoll) / time.Second)
}

// fifoQueue creates a FIFO queue this test alone uses, and removes it
// afterwards.
func fifoQueue(t *testing.T, kind string, attributes map[string]string) string {
	t.Helper()
	requireQueues(t)
	name := fmt.Sprintf("test-%s-%s-%d.fifo", runID, kind, queues.Add(1))

	settings := map[string]string{
		"FifoQueue": "true",
		// Off, as the deployed queues have it: two bodies differing only in
		// whitespace are one message here, and a content hash would make them
		// two. Everything this suite sends names its own deduplication id.
		"ContentBasedDeduplication": "false",
	}
	maps.Copy(settings, attributes)
	made, err := sharedSDK.CreateQueue(t.Context(), &awssqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: settings,
	})
	if err != nil {
		t.Fatalf("create the fixture queue %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
		defer cancel()
		if _, err := sharedSDK.DeleteQueue(ctx, &awssqs.DeleteQueueInput{
			QueueUrl: made.QueueUrl}); err != nil {
			t.Logf("could not delete %s, leaving it behind: %v", name, err)
		}
	})
	return name
}

// inboundPair creates a source queue and the dead-letter queue it redrives to,
// under the deployed retry limit.
//
// visibility is this test's own, for the reason at the top of the file, and it
// is the number every timing assertion in the scenario that takes it is stated
// against.
func inboundPair(t *testing.T, visibility time.Duration) (source, dead string) {
	t.Helper()
	dead = fifoQueue(t, "dlq", nil)
	policy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":%d}`,
		queueARN(t, dead), maxReceives)
	source = fifoQueue(t, "in", map[string]string{
		"VisibilityTimeout": strconv.Itoa(int(visibility / time.Second)),
		"RedrivePolicy":     policy,
	})
	return source, dead
}

// inbound creates a source queue with no dead-letter queue behind it, for the
// scenarios where a message being redriven mid-test would end the thing they
// are watching.
func inbound(t *testing.T, visibility time.Duration) string {
	t.Helper()
	return fifoQueue(t, "in", map[string]string{
		"VisibilityTimeout": strconv.Itoa(int(visibility / time.Second)),
	})
}

// outbound creates the queue the publisher sends to.
func outbound(t *testing.T) string {
	t.Helper()
	return fifoQueue(t, "out", nil)
}

// queueURL resolves a queue's name the way everything but the adapter has to.
func queueURL(t *testing.T, name string) *string {
	t.Helper()
	found, err := sharedSDK.GetQueueUrl(t.Context(), &awssqs.GetQueueUrlInput{
		QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	return found.QueueUrl
}

// queueARN reads a queue's ARN, which is how a redrive policy names it.
func queueARN(t *testing.T, name string) string {
	t.Helper()
	attributes, err := sharedSDK.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl:       queueURL(t, name),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("read %s's ARN: %v", name, err)
	}
	arn := attributes.Attributes[string(types.QueueAttributeNameQueueArn)]
	if arn == "" {
		t.Fatalf("%s has no ARN, so nothing can redrive to it", name)
	}
	return arn
}

// put sends one message through the raw client, as a provider's own producer
// would.
//
// The deduplication id is the caller's rather than derived from the body,
// because two of these scenarios send one envelope message id twice and the
// queue must treat them as two deliveries rather than as one message — which is
// exactly the condition the inbox exists to answer.
//
// Nothing rides beside the body. The consumer reads a correlation out of the
// message attributes when one is there and falls back to the envelope's message
// id when it is not, and the fallback is what every scenario here exercises;
// which of the two it took is a property of one log line and not of any outcome
// below.
func put(t *testing.T, queue, body, group, dedupe string) string {
	t.Helper()
	sent, err := sharedSDK.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl:               queueURL(t, queue),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(dedupe),
	})
	if err != nil {
		t.Fatalf("send to %s: %v", queue, err)
	}
	return aws.ToString(sent.MessageId)
}

// seen is one message as the raw client reads it, with the two FIFO system
// attributes the queue adapter deliberately does not ask for.
//
// The adapter names the system attributes it wants one by one and takes only
// ApproximateReceiveCount, because a consumer that asked for all of them would
// start paying for whatever SQS adds next. This suite asks for the other two
// because the group and the deduplication id ARE the outbound contract — the
// aggregate id orders a wallet's events and the event id is what makes a
// republication the same event — and there is no other way to see what was
// actually written on the wire.
type seen struct {
	messageID       string
	receiptHandle   string
	body            string
	receiveCount    int
	groupID         string
	deduplicationID string
}

// peek takes up to ten messages off a queue without deleting them, waiting up
// to within — clamped by [pollSeconds] — for the first of them.
//
// Nothing is deleted because every caller here is asking what is on the queue
// rather than consuming it, and a probe that deleted would make the next
// assertion in the same test depend on the order the assertions were written
// in.
func peek(t *testing.T, queue string, within time.Duration) []seen {
	t.Helper()
	received, err := sharedSDK.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL(t, queue),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     pollSeconds(within),
		// This zero is the visibility timeout and means "leave the queue's own
		// alone", which is not the zero [pollSeconds] exists to prevent.
		VisibilityTimeout: 0,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameMessageGroupId,
			types.MessageSystemAttributeNameMessageDeduplicationId,
		},
	})
	if err != nil {
		t.Fatalf("peek at %s: %v", queue, err)
	}
	out := make([]seen, 0, len(received.Messages))
	for _, m := range received.Messages {
		out = append(out, seen{
			messageID:     aws.ToString(m.MessageId),
			receiptHandle: aws.ToString(m.ReceiptHandle),
			body:          aws.ToString(m.Body),
			receiveCount: countOf(m.Attributes,
				types.MessageSystemAttributeNameApproximateReceiveCount),
			groupID: m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
			deduplicationID: m.Attributes[string(
				types.MessageSystemAttributeNameMessageDeduplicationId)],
		})
	}
	return out
}

// countOf reads a numeric system attribute, reporting zero when it is absent —
// which for a receive count means "the queue did not say", and is never a count
// a caller should assert on.
func countOf(attributes map[string]string, name types.MessageSystemAttributeName) int {
	count, err := strconv.Atoi(attributes[string(name)])
	if err != nil {
		return 0
	}
	return count
}

// unhide gives peeked messages straight back, so that a later read of the same
// queue does not have to wait out the visibility timeout this test's own probe
// started.
//
// Only a scenario that reads one queue twice needs it. Everything else reads
// once and lets the messages time out with the fixture.
func unhide(t *testing.T, queue string, messages []seen) {
	t.Helper()
	url := queueURL(t, queue)
	for _, m := range messages {
		if _, err := sharedSDK.ChangeMessageVisibility(t.Context(),
			&awssqs.ChangeMessageVisibilityInput{
				QueueUrl:          url,
				ReceiptHandle:     aws.String(m.receiptHandle),
				VisibilityTimeout: 0,
			}); err != nil {
			t.Fatalf("hand %s back to %s: %v", m.messageID, queue, err)
		}
	}
}

// empty asserts that a queue holds nothing, having waited long enough for
// anything hidden to have come back.
//
// within has to be longer than the queue's own visibility timeout wherever the
// question is whether a message was deleted: a queue asked for messages inside
// that window answers empty whether the delete happened or not, so a probe
// inside it would pass against a Delete that did nothing. That is not a
// hypothetical — it is the shape a delete assertion in this tree once had, and
// it is why this one is given a budget rather than asked straight away.
// It spends its budget across as many receives as it takes rather than in one,
// because SQS refuses a wait past twenty seconds and some of these queues are
// provisioned with a visibility timeout longer than that.
func empty(t *testing.T, queue string, within time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if left := peek(t, queue, time.Until(deadline)); len(left) != 0 {
			for _, m := range left {
				t.Errorf("%s: %s still holds %s (delivery %d)", what, queue, m.body,
					m.receiveCount)
			}
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
	}
}

// awaitMessages waits for a queue to hold at least want messages and returns
// them, failing when it does not reach that within the budget.
//
// It polls rather than taking one long receive because a receive returns at
// most ten messages and hides what it took, so a queue holding more than one
// call's worth is only readable across several — and because a batch that
// arrived in pieces would otherwise be read as a queue that never filled.
func awaitMessages(t *testing.T, queue string, want int, within time.Duration, what string) []seen {
	t.Helper()
	deadline := time.Now().Add(within)
	byID := map[string]seen{}
	for {
		for _, m := range peek(t, queue, time.Second) {
			byID[m.messageID] = m
		}
		if len(byID) >= want {
			return sortedByID(byID)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s held %d messages after %s, wanted %d", what, queue, len(byID),
				within, want)
		}
	}
}

// sortedByID returns the collected messages in a stable order, so that a
// failure prints the same list twice and an assertion over them does not depend
// on the order a map happened to yield.
func sortedByID(byID map[string]seen) []seen {
	out := make([]seen, 0, len(byID))
	for _, m := range byID {
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b seen) int { return strings.Compare(a.messageID, b.messageID) })
	return out
}
