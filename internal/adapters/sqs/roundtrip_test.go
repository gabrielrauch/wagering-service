//go:build integration

package sqs

import (
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// TestRoundTrip is the consumer's whole contract in one test: what goes on the
// queue comes back with its body, its identity and its trace context intact,
// and a delete after the work is done removes it for good.
func TestRoundTrip(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 3 * time.Second})

	body := `{"messageId":"018f2b9c-0000-7000-8000-00000000000a","kind":"BET"}`
	trace := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "congo=t61rcWkgMzE",
	}
	results, err := queue.SendBatch(t.Context(), []Outbound{{
		Body:            []byte(body),
		GroupID:         "018f2b9c-0000-7000-8000-000000000001",
		DeduplicationID: "018f2b9c-0000-7000-8000-00000000000a",
		Attributes:      trace,
	}})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(results) != 1 || !results[0].Sent() {
		t.Fatalf("results = %+v, want one message on the queue", results)
	}
	if results[0].MessageID == "" {
		t.Errorf("the queue accepted the message and named no id for it")
	}

	messages := receiveOne(t, queue)
	got := messages[0]
	if string(got.Body) != body {
		t.Errorf("body = %q, want %q", got.Body, body)
	}
	if got.MessageID != results[0].MessageID {
		t.Errorf("message id = %q, want the one the send reported, %q", got.MessageID,
			results[0].MessageID)
	}
	if got.ReceiptHandle == "" {
		t.Errorf("no receipt handle: there would be no way to delete this message")
	}
	if !maps.Equal(got.Attributes, trace) {
		t.Errorf("attributes = %v, want %v", got.Attributes, trace)
	}
	if got.ReceiveCount != 1 {
		t.Errorf("receive count = %d, want 1 on a first delivery", got.ReceiveCount)
	}

	if err := queue.Delete(t.Context(), got.ReceiptHandle); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Released first so that the queue is not merely holding it invisible.
	if left := drain(t, queue); len(left) != 0 {
		t.Errorf("%d messages left after the delete, want none", len(left))
	}
}

// TestAttributesSurviveWhoeverElseWroteThem receives a message this package did
// not send, because the consumer's side of the queue is fed by somebody else's
// producer and has to read what that producer writes.
func TestAttributesSurviveWhoeverElseWroteThem(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 3 * time.Second})

	send(t, name, `{"messageId":"a"}`, "wallet-1", "dedupe-1",
		map[string]types.MessageAttributeValue{
			"traceparent": {DataType: aws.String("String"),
				StringValue: aws.String("00-abc-def-01")},
			"binary": {DataType: aws.String("Binary"), BinaryValue: []byte{0x01, 0x02}},
		})

	got := receiveOne(t, queue)[0]
	if want := "00-abc-def-01"; got.Attributes["traceparent"] != want {
		t.Errorf("traceparent = %q, want %q", got.Attributes["traceparent"], want)
	}
	if _, carried := got.Attributes["binary"]; carried {
		t.Errorf("attributes = %v, want the binary one dropped rather than mangled",
			got.Attributes)
	}
}

// TestLongPollingWaits is the behaviour that keeps an idle consumer from
// billing a request a millisecond.
func TestLongPollingWaits(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 3 * time.Second})

	started := time.Now()
	messages, err := queue.Receive(t.Context())
	waited := time.Since(started)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("received %d messages from an empty queue", len(messages))
	}
	// A short poll answers in milliseconds. Two and a half seconds of a
	// three-second wait is a margin a loaded machine keeps.
	if waited < 2500*time.Millisecond {
		t.Errorf("an empty receive answered after %s, want it to have waited near 3s", waited)
	}
}

// TestLongPollingReturnsAsSoonAsThereIsWork is the other half: the wait is a
// ceiling, not a delay, so a consumer configured for twenty seconds still picks
// work up at once.
func TestLongPollingReturnsAsSoonAsThereIsWork(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 20 * time.Second})
	send(t, name, `{"messageId":"b"}`, "wallet-1", "dedupe-1", nil)

	started := time.Now()
	messages, err := queue.Receive(t.Context())
	waited := time.Since(started)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("received %d messages, want 1", len(messages))
	}
	if waited > 10*time.Second {
		t.Errorf("a receive with work waiting took %s, want it to answer at once", waited)
	}
}

// TestChangeVisibilityDefersRedelivery is the transient-failure path: the
// message is not deleted, it is pushed out, and it comes back when the backoff
// has run.
func TestChangeVisibilityDefersRedelivery(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{
		Name: name, WaitTime: 1 * time.Second, VisibilityTimeout: 60 * time.Second})
	send(t, name, `{"messageId":"c"}`, "wallet-1", "dedupe-1", nil)

	first := receiveOne(t, queue)[0]
	if err := queue.ChangeVisibility(t.Context(), first.ReceiptHandle, 2*time.Second); err != nil {
		t.Fatalf("change visibility: %v", err)
	}

	// Still hidden a moment later: the new timeout replaced the 60 seconds,
	// it did not expire it.
	early, err := queue.Receive(t.Context())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(early) != 0 {
		t.Fatalf("received %d messages before the backoff had run", len(early))
	}

	again := waitForRedelivery(t, queue, 10*time.Second)
	if again.MessageID != first.MessageID {
		t.Errorf("redelivered %q, want the message that was deferred, %q", again.MessageID,
			first.MessageID)
	}
	if again.ReceiveCount != 2 {
		t.Errorf("receive count = %d, want 2 on a redelivery", again.ReceiveCount)
	}
}

// TestReleaseHandsTheMessageBackAtOnce is the shutdown path. Without it a
// replica that stops cleanly looks exactly like one that crashed, and its
// in-flight work waits out a timeout that exists for the case where nobody
// handed it back.
func TestReleaseHandsTheMessageBackAtOnce(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{
		Name: name, WaitTime: 1 * time.Second, VisibilityTimeout: 12 * time.Hour})
	send(t, name, `{"messageId":"d"}`, "wallet-1", "dedupe-1", nil)

	first := receiveOne(t, queue)[0]
	hidden, err := queue.Receive(t.Context())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(hidden) != 0 {
		t.Fatalf("received %d messages while one was in flight", len(hidden))
	}

	if err := queue.Release(t.Context(), first.ReceiptHandle); err != nil {
		t.Fatalf("release: %v", err)
	}
	back := waitForRedelivery(t, queue, 10*time.Second)
	if back.MessageID != first.MessageID {
		t.Errorf("redelivered %q, want the released message %q", back.MessageID, first.MessageID)
	}
	if back.ReceiveCount != 2 {
		t.Errorf("receive count = %d, want 2", back.ReceiveCount)
	}
	// A visibility timeout of twelve hours means nothing but the release could
	// have brought it back.
}

// TestDeleteRefusesAHandleThatIsNotOne holds the classification on the path a
// consumer reaches when it deletes with a handle from an earlier delivery.
func TestDeleteRefusesAHandleThatIsNotOne(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: time.Second})

	err := queue.Delete(t.Context(), "this-is-not-a-receipt-handle")
	if err == nil {
		t.Fatalf("delete with a nonsense handle = nil, want a refusal")
	}
	if got := app.ClassOf(err); got != app.Unretryable {
		t.Errorf("class = %s, want %s: the handle will not become valid", got, app.Unretryable)
	}
}

// receiveOne takes exactly one message, failing if the queue produced anything
// else.
func receiveOne(t *testing.T, queue *Queue) []Message {
	t.Helper()
	messages, err := queue.Receive(t.Context())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("received %d messages, want 1", len(messages))
	}
	return messages
}

// waitForRedelivery polls until a message comes back, or gives up.
func waitForRedelivery(t *testing.T, queue *Queue, within time.Duration) Message {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		messages, err := queue.Receive(t.Context())
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if len(messages) > 0 {
			return messages[0]
		}
	}
	t.Fatalf("nothing was redelivered within %s", within)
	return Message{}
}

// drain takes everything the queue will give up, deleting as it goes.
func drain(t *testing.T, queue *Queue) []Message {
	t.Helper()
	var all []Message
	for {
		messages, err := queue.Receive(t.Context())
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if len(messages) == 0 {
			return all
		}
		all = append(all, messages...)
		for _, message := range messages {
			if err := queue.Delete(t.Context(), message.ReceiptHandle); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
	}
}
