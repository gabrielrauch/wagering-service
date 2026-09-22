//go:build integration

package sqs

import (
	"fmt"
	"maps"
	"sync"
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
	// A one-second visibility timeout, deliberately. The delete is only
	// observable once the message would otherwise have come back: a queue asked
	// for messages inside the visibility window answers empty whether the
	// delete happened or not, so a round trip that drained inside it would pass
	// with no delete at all. Three seconds of long polling is comfortably past
	// the second, and the poll returns the moment anything appears — so this
	// costs three seconds when the delete worked and fails at once when it did
	// not.
	queue := openQueue(t, Config{
		Name: name, WaitTime: 3 * time.Second, VisibilityTimeout: time.Second})

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
	// One receive rather than a drain, and the difference is the whole
	// assertion. A drain deletes what it takes and loops until the queue is
	// empty, so against a Delete that does not delete it receives the same
	// message forever — hanging where it should fail. A single poll past the
	// visibility window answers the only question being asked: does the message
	// come back.
	left, err := queue.Receive(t.Context())
	if err != nil {
		t.Fatalf("receive after the delete: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("%d messages came back after the delete and the visibility timeout that "+
			"followed it, want none", len(left))
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

// TestAnOpenVisibilityTimeoutLeavesTheQueuesOwnInPlace pins a correctness
// property that rests on an SDK detail rather than on anything this package
// controls.
//
// A [Config] that names no visibility timeout is modelled as a nil *int32 and
// reaches the wire as a plain zero, and that zero means "the queue's own
// timeout" only because the SDK omits a zero-valued VisibilityTimeout from a
// ReceiveMessage request. If that omission ever went away, a zero would be
// sent and read as "make it visible again immediately" — every received message
// handed to every other consumer at once, and a receive that reserves nothing.
//
// The fixture queue holds messages for six seconds and the receive waits two,
// so a second receive straight after the first must find nothing and the
// message must come back once those six seconds have run. The first half says
// the zero did not reach the wire; the second says the queue's own timeout is
// what hid it. The gap between the two seconds and the six is what makes this
// bite: a package that sent any timeout of its own — a zero, or a default it
// invented — would hand the message back inside the poll.
func TestAnOpenVisibilityTimeoutLeavesTheQueuesOwnInPlace(t *testing.T) {
	name := fixtureQueue(t, map[string]string{"VisibilityTimeout": "6"})
	queue := openQueue(t, Config{Name: name, WaitTime: 2 * time.Second})
	send(t, name, `{"messageId":"e"}`, "wallet-1", "dedupe-1", nil)

	first := receiveOne(t, queue)[0]
	hidden, err := queue.Receive(t.Context())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(hidden) != 0 {
		t.Fatalf("the message was visible again at once: a zero visibility "+
			"timeout reached the wire (received %d)", len(hidden))
	}

	back := waitForRedelivery(t, queue, 20*time.Second)
	if back.MessageID != first.MessageID {
		t.Errorf("came back as %q, want the message the queue was hiding, %q", back.MessageID,
			first.MessageID)
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
//
// Bounded, and the bound is not caution for its own sake. This loop's exit
// condition is an empty receive, which a Delete that did not delete would never
// produce: the same message would be handed back on every pass and the helper
// whose job is to prove a queue is empty would hang until the test binary's own
// timeout. A suite that fails takes seconds to read; one that hangs for ten
// minutes and then reports a timeout tells whoever is reading it nothing about
// which promise broke.
//
// The cap is far above what any test here needs — the largest sends 25 messages
// across three groups — so reaching it means something is wrong rather than
// something is busy.
func drain(t *testing.T, queue *Queue) []Message {
	t.Helper()
	const rounds = 40
	var all []Message
	for range rounds {
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
	t.Fatalf("the queue was still handing messages back after %d rounds of receive and "+
		"delete, having produced %d: something is not deleting", rounds, len(all))
	return nil
}

// TestOneQueueServesSeveralGoroutinesAtOnce pins the promise [Queue]'s own doc
// comment makes, because the worker task is about to build on it.
//
// One handle, four senders, two receivers and a readiness probe, all at once
// and all under -race. That is not a synthetic arrangement: it is exactly how
// this runs in production, where the HTTP server answers /health/ready on the
// same queue the consumer is polling and the publisher is sending to.
//
// Every message is accounted for at the end — either a receiver took it and
// deleted it, or it is still on the queue — so a lost send shows up as a
// missing body rather than as a count that happened to come out right.
func TestOneQueueServesSeveralGoroutinesAtOnce(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 2 * time.Second})
	health, err := NewHealth(queue, 5*time.Second)
	if err != nil {
		t.Fatalf("build a readiness check: %v", err)
	}

	const senders, each = 4, 5
	const total = senders * each

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		failures []error
		seen     = map[string]bool{}
	)
	note := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		failures = append(failures, err)
	}
	record := func(messages []Message) {
		mu.Lock()
		defer mu.Unlock()
		for _, message := range messages {
			seen[string(message.Body)] = true
		}
	}

	for s := range senders {
		wg.Go(func() {
			messages := make([]Outbound, each)
			for i := range messages {
				messages[i] = Outbound{
					Body:            fmt.Appendf(nil, `{"eventId":"s%d-%d"}`, s, i),
					GroupID:         fmt.Sprintf("wallet-%d", s),
					DeduplicationID: fmt.Sprintf("s%d-%d", s, i),
				}
			}
			results, err := queue.SendBatch(t.Context(), messages)
			if err != nil {
				note(err)
				return
			}
			for _, result := range results {
				if !result.Sent() {
					note(result.Err)
				}
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for range 3 {
				messages, err := queue.Receive(t.Context())
				if err != nil {
					note(err)
					return
				}
				record(messages)
				for _, message := range messages {
					if err := queue.Delete(t.Context(), message.ReceiptHandle); err != nil {
						note(err)
						return
					}
				}
			}
		})
	}
	wg.Go(func() {
		for range 10 {
			if err := health.Ready(t.Context()); err != nil {
				note(err)
				return
			}
		}
	})
	wg.Wait()

	for _, err := range failures {
		t.Errorf("a goroutine failed: %v", err)
	}
	record(drain(t, queue))
	if len(seen) != total {
		t.Errorf("%d distinct messages accounted for, want %d", len(seen), total)
	}
}
