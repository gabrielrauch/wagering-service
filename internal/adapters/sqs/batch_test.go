//go:build integration

package sqs

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// TestSendBatchSplitsAtTheEntryLimit sends more than one call's worth and
// proves every message reached the queue.
//
// Twenty-five is three calls: ten, ten and five. What is being tested is that
// the publisher does not have to know that.
func TestSendBatchSplitsAtTheEntryLimit(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 2 * time.Second})

	const count = 25
	messages := make([]Outbound, count)
	for i := range messages {
		messages[i] = Outbound{
			Body:            fmt.Appendf(nil, `{"eventId":"event-%02d"}`, i),
			GroupID:         fmt.Sprintf("wallet-%d", i%3),
			DeduplicationID: fmt.Sprintf("event-%02d", i),
		}
	}
	results, err := queue.SendBatch(t.Context(), messages)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(results) != count {
		t.Fatalf("%d results for %d messages", len(results), count)
	}
	ids := make(map[string]bool, count)
	for i, result := range results {
		if !result.Sent() {
			t.Fatalf("results[%d] = %v, want it sent", i, result.Err)
		}
		if ids[result.MessageID] {
			t.Errorf("results[%d] repeats message id %q", i, result.MessageID)
		}
		ids[result.MessageID] = true
	}

	arrived := drain(t, queue)
	if len(arrived) != count {
		t.Fatalf("%d messages arrived, want %d", len(arrived), count)
	}
}

// TestSendBatchSplitsAtTheSizeLimit is the other bound. Three 100 KiB messages
// do not fit in one 256 KiB call although three entries fit in one batch, and a
// publisher that let SQS discover that would have lost all three.
func TestSendBatchSplitsAtTheSizeLimit(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 2 * time.Second})

	const each = 100 * 1024
	messages := make([]Outbound, 3)
	for i := range messages {
		messages[i] = Outbound{
			Body:            []byte(strings.Repeat(string(rune('a'+i)), each)),
			GroupID:         "one-wallet",
			DeduplicationID: fmt.Sprintf("big-%d", i),
		}
	}
	results, err := queue.SendBatch(t.Context(), messages)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	for i, result := range results {
		if !result.Sent() {
			t.Fatalf("results[%d] = %v, want it sent", i, result.Err)
		}
	}
	arrived := drain(t, queue)
	if len(arrived) != 3 {
		t.Fatalf("%d messages arrived, want 3", len(arrived))
	}
	for _, message := range arrived {
		if len(message.Body) != each {
			t.Errorf("a message arrived at %d bytes, want %d", len(message.Body), each)
		}
	}
}

// TestSendBatchReportsAPartialFailure is the property the outbox publisher is
// built on: SQS refuses entries inside a 200, and the publisher must mark
// exactly the ones that were accepted.
//
// The refusal is provoked with a control character in the body, which SQS
// refuses as InvalidMessageContents. It is a refusal this package deliberately
// does not pre-empt — validating the body's characters here would mean this
// adapter deciding what an envelope may contain — so it is a genuine per-entry
// failure from the service.
func TestSendBatchReportsAPartialFailure(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 2 * time.Second})

	messages := []Outbound{
		{Body: []byte(`{"eventId":"first"}`), GroupID: "wallet-1", DeduplicationID: "first"},
		{Body: []byte("bad\x07character"), GroupID: "wallet-1", DeduplicationID: "second"},
		{Body: []byte(`{"eventId":"third"}`), GroupID: "wallet-1", DeduplicationID: "third"},
	}
	results, err := queue.SendBatch(t.Context(), messages)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("%d results for 3 messages", len(results))
	}
	if !results[0].Sent() || !results[2].Sent() {
		t.Fatalf("the sound messages did not go out: %v, %v", results[0].Err, results[2].Err)
	}
	if results[1].Sent() {
		t.Fatalf("results[1] = %+v, want the refusal", results[1])
	}
	if got := app.ClassOf(results[1].Err); got != app.Unretryable {
		t.Errorf("class = %s, want %s: a body SQS cannot carry will not become carriable",
			got, app.Unretryable)
	}
	if !strings.Contains(results[1].Err.Error(), "InvalidMessageContents") {
		t.Errorf("error = %q, want it to name the code SQS gave", results[1].Err)
	}

	arrived := drain(t, queue)
	if len(arrived) != 2 {
		t.Fatalf("%d messages arrived, want the 2 SQS accepted", len(arrived))
	}
}

// TestSendBatchRefusesOneMessageAndSendsTheRest is the same property one step
// earlier: a message this package refuses before sending must not take the
// batch with it, because an event nobody can shrink would otherwise stop the
// publisher for good.
func TestSendBatchRefusesOneMessageAndSendsTheRest(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 2 * time.Second})

	messages := []Outbound{
		{Body: []byte(`{"eventId":"first"}`), GroupID: "wallet-1", DeduplicationID: "first"},
		{Body: []byte(`{"eventId":"second"}`), DeduplicationID: "second"},
		{Body: make([]byte, maxBatchBytes+1), GroupID: "wallet-1", DeduplicationID: "third"},
		{Body: []byte(`{"eventId":"fourth"}`), GroupID: "wallet-1", DeduplicationID: "fourth"},
	}
	results, err := queue.SendBatch(t.Context(), messages)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	for _, index := range []int{0, 3} {
		if !results[index].Sent() {
			t.Errorf("results[%d] = %v, want it sent", index, results[index].Err)
		}
	}
	for _, index := range []int{1, 2} {
		if results[index].Sent() {
			t.Errorf("results[%d] = %+v, want the refusal", index, results[index])
		}
		if got := app.ClassOf(results[index].Err); got != app.Unretryable {
			t.Errorf("results[%d] class = %s, want %s", index, got, app.Unretryable)
		}
	}

	arrived := drain(t, queue)
	if len(arrived) != 2 {
		t.Fatalf("%d messages arrived, want the 2 that were sound", len(arrived))
	}
}

// TestDeduplicationIsTheSendersToState proves what the queues are configured
// for: two sends carrying one deduplication id are one message, whatever the
// bodies say.
//
// It is the property a publisher killed between sending and marking the outbox
// row depends on — it republishes, and the event does not appear twice.
func TestDeduplicationIsTheSendersToState(t *testing.T) {
	name := fixtureQueue(t, nil)
	queue := openQueue(t, Config{Name: name, WaitTime: 2 * time.Second})

	message := Outbound{
		Body:            []byte(`{"eventId":"once"}`),
		GroupID:         "wallet-1",
		DeduplicationID: "the-event-id",
	}
	for attempt := range 2 {
		results, err := queue.SendBatch(t.Context(), []Outbound{message})
		if err != nil {
			t.Fatalf("send %d: %v", attempt, err)
		}
		if !results[0].Sent() {
			t.Fatalf("send %d was refused: %v", attempt, results[0].Err)
		}
	}
	arrived := drain(t, queue)
	if len(arrived) != 1 {
		t.Fatalf("%d messages arrived, want 1: the deduplication id did not hold", len(arrived))
	}
}
