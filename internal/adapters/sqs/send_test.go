package sqs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// plansOf builds n plans of the given size, positioned the way SendBatch would
// have positioned them.
func plansOf(sizes ...int) []plan {
	plans := make([]plan, len(sizes))
	for i, size := range sizes {
		plans[i] = plan{
			index: i,
			size:  size,
			entry: types.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i))},
		}
	}
	return plans
}

// shape reports how many entries each batch holds, which is what the chunking
// rules are actually about.
func shape(batches [][]plan) []int {
	sizes := make([]int, len(batches))
	for i, batch := range batches {
		sizes[i] = len(batch)
	}
	return sizes
}

func repeat(n int, size int) []int {
	sizes := make([]int, n)
	for i := range sizes {
		sizes[i] = size
	}
	return sizes
}

func TestChunking(t *testing.T) {
	cases := []struct {
		name  string
		sizes []int
		want  []int
	}{
		{
			name: "nothing makes no calls",
		},
		{
			name:  "a full batch is one call",
			sizes: repeat(maxBatchEntries, 10),
			want:  []int{10},
		},
		{
			name:  "one past a full batch is two calls",
			sizes: repeat(maxBatchEntries+1, 10),
			want:  []int{10, 1},
		},
		{
			name:  "twenty-five is ten, ten and five",
			sizes: repeat(25, 10),
			want:  []int{10, 10, 5},
		},
		{
			name: "the weight limit splits a batch the entry count would not",
			// Three at 100 KiB: the first two fit in 256 KiB, the third does
			// not, so it starts a call of its own.
			sizes: repeat(3, 100*1024),
			want:  []int{2, 1},
		},
		{
			name:  "a batch that lands exactly on the limit is still one call",
			sizes: []int{maxBatchBytes / 2, maxBatchBytes / 2},
			want:  []int{2},
		},
		{
			name:  "one byte past the limit is two",
			sizes: []int{maxBatchBytes/2 + 1, maxBatchBytes / 2},
			want:  []int{1, 1},
		},
		{
			name:  "a message that fills a whole batch travels alone",
			sizes: []int{10, maxBatchBytes, 10},
			want:  []int{1, 1, 1},
		},
		{
			// It cannot reach chunking — entry() refuses it — and if it ever
			// did, the answer must not be an empty first batch: SQS refuses an
			// empty batch outright, taking every message behind it down.
			name:  "an oversized first message does not produce an empty call",
			sizes: []int{maxBatchBytes + 1, 10},
			want:  []int{1, 1},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			batches := chunk(plansOf(c.sizes...))
			if got := shape(batches); !equal(got, c.want) {
				t.Fatalf("chunk sizes = %v, want %v", got, c.want)
			}
			assertOrderKept(t, batches, len(c.sizes))
			assertWithinBounds(t, batches)
		})
	}
}

// assertOrderKept is the FIFO property: a wallet's events must reach the queue
// in the order the publisher handed them over, so chunking may split the run
// but may never reorder it.
func assertOrderKept(t *testing.T, batches [][]plan, total int) {
	t.Helper()
	next := 0
	for _, batch := range batches {
		for _, p := range batch {
			if p.index != next {
				t.Fatalf("entry at position %d came out as %d: chunking reordered the run",
					next, p.index)
			}
			next++
		}
	}
	if next != total {
		t.Fatalf("chunking carried %d of %d entries", next, total)
	}
}

func assertWithinBounds(t *testing.T, batches [][]plan) {
	t.Helper()
	for i, batch := range batches {
		if len(batch) > maxBatchEntries {
			t.Errorf("batch %d holds %d entries, past the %d SQS accepts", i, len(batch),
				maxBatchEntries)
		}
		weight := 0
		for _, p := range batch {
			weight += p.size
		}
		if weight > maxBatchBytes && len(batch) > 1 {
			t.Errorf("batch %d weighs %d bytes, past the %d SQS accepts", i, weight,
				maxBatchBytes)
		}
	}
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestOutboundRefusals(t *testing.T) {
	sound := Outbound{Body: []byte(`{"eventId":"x"}`), GroupID: "wallet", DeduplicationID: "event"}
	cases := []struct {
		name    string
		message Outbound
		because string
	}{
		{
			name:    "no body",
			message: Outbound{GroupID: "wallet", DeduplicationID: "event"},
			because: "needs a body",
		},
		{
			name:    "no group",
			message: Outbound{Body: sound.Body, DeduplicationID: "event"},
			because: "needs a group id",
		},
		{
			name: "a group id past what FIFO allows",
			message: Outbound{Body: sound.Body, GroupID: strings.Repeat("g", 129),
				DeduplicationID: "event"},
			because: "at most 128 bytes",
		},
		{
			name:    "no deduplication id",
			message: Outbound{Body: sound.Body, GroupID: "wallet"},
			because: "needs a deduplication id",
		},
		{
			name: "a deduplication id past what FIFO allows",
			message: Outbound{Body: sound.Body, GroupID: "wallet",
				DeduplicationID: strings.Repeat("d", 129)},
			because: "at most 128 bytes",
		},
		{
			name: "a message SQS could never carry",
			message: Outbound{Body: make([]byte, maxBatchBytes+1), GroupID: "wallet",
				DeduplicationID: "event"},
			because: "past the 262144 SQS allows",
		},
		{
			name: "a body at the limit once its attributes are counted",
			message: Outbound{Body: make([]byte, maxBatchBytes), GroupID: "wallet",
				DeduplicationID: "event", Attributes: map[string]string{"traceparent": "x"}},
			because: "past the 262144 SQS allows",
		},
		{
			name: "an attribute name SQS would refuse the whole call for",
			message: Outbound{Body: sound.Body, GroupID: "wallet", DeduplicationID: "event",
				Attributes: map[string]string{"AWS.TraceHeader": "x"}},
			because: "prefix SQS reserves",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := c.message.entry(0)
			if err == nil {
				t.Fatalf("entry() = nil, want a refusal")
			}
			if got := app.ClassOf(err); got != app.Unretryable {
				t.Errorf("class = %s, want %s: sending it again changes nothing", got,
					app.Unretryable)
			}
			if !strings.Contains(err.Error(), c.because) {
				t.Errorf("error = %q, want it to say %q", err, c.because)
			}
		})
	}
}

func TestOutboundEntryCarriesTheFifoFields(t *testing.T) {
	message := Outbound{
		Body:            []byte(`{"eventId":"7a"}`),
		GroupID:         "018f2b9c-0000-7000-8000-000000000001",
		DeduplicationID: "018f2b9c-0000-7000-8000-0000000000ff",
		Attributes:      map[string]string{"traceparent": "00-abc-def-01"},
	}
	entry, size, err := message.entry(4)
	if err != nil {
		t.Fatalf("entry(): %v", err)
	}
	switch {
	case aws.ToString(entry.Id) != "4":
		t.Errorf("id = %q, want the position in the caller's slice", aws.ToString(entry.Id))
	case aws.ToString(entry.MessageBody) != string(message.Body):
		t.Errorf("body = %q, want it unaltered", aws.ToString(entry.MessageBody))
	case aws.ToString(entry.MessageGroupId) != message.GroupID:
		t.Errorf("group = %q, want %q", aws.ToString(entry.MessageGroupId), message.GroupID)
	case aws.ToString(entry.MessageDeduplicationId) != message.DeduplicationID:
		t.Errorf("deduplication id = %q, want %q",
			aws.ToString(entry.MessageDeduplicationId), message.DeduplicationID)
	case len(entry.MessageAttributes) != 1:
		t.Errorf("attributes = %v, want the one that was given", entry.MessageAttributes)
	}
	if want := len(message.Body) + attributesSize(message.Attributes); size != want {
		t.Errorf("size = %d, want %d", size, want)
	}
}

func TestRecordPutsOutcomesBackWhereTheyBelong(t *testing.T) {
	// A call carrying the messages at positions 1, 2 and 3 of a longer slice,
	// of which SQS accepted two and refused one — reported in neither the order
	// they were sent nor the order they were asked about.
	results := make([]SendResult, 5)
	batch := []plan{{index: 1}, {index: 2}, {index: 3}}
	record(results, batch, &awssqs.SendMessageBatchOutput{
		Successful: []types.SendMessageBatchResultEntry{
			{Id: aws.String("3"), MessageId: aws.String("queue-id-3")},
			{Id: aws.String("1"), MessageId: aws.String("queue-id-1")},
		},
		Failed: []types.BatchResultErrorEntry{
			{Id: aws.String("2"), Code: aws.String("InvalidMessageContents"),
				Message: aws.String("Invalid characters found."), SenderFault: false},
		},
	})

	if !results[1].Sent() || results[1].MessageID != "queue-id-1" {
		t.Errorf("results[1] = %+v, want the message SQS accepted", results[1])
	}
	if !results[3].Sent() || results[3].MessageID != "queue-id-3" {
		t.Errorf("results[3] = %+v, want the message SQS accepted", results[3])
	}
	if results[2].Sent() {
		t.Fatalf("results[2] = %+v, want the refusal", results[2])
	}
	if got := app.ClassOf(results[2].Err); got != app.Unretryable {
		t.Errorf("class = %s, want %s for a body SQS cannot carry", got, app.Unretryable)
	}
	// Nothing was said about the positions this call did not carry.
	for _, untouched := range []int{0, 4} {
		if results[untouched] != (SendResult{}) {
			t.Errorf("results[%d] = %+v, want it left alone", untouched, results[untouched])
		}
	}
}

// TestRecordRefusesToAssumeSuccess is the one that keeps a publisher honest: a
// message the service said nothing about must not be marked published because
// nothing said otherwise.
func TestRecordRefusesToAssumeSuccess(t *testing.T) {
	results := make([]SendResult, 2)
	record(results, []plan{{index: 0}, {index: 1}}, &awssqs.SendMessageBatchOutput{
		Successful: []types.SendMessageBatchResultEntry{
			{Id: aws.String("0"), MessageId: aws.String("queue-id-0")},
		},
	})
	if !results[0].Sent() {
		t.Errorf("results[0] = %+v, want the message SQS accepted", results[0])
	}
	if results[1].Sent() {
		t.Fatalf("results[1] = %+v, want a failure for the entry nobody reported on", results[1])
	}
	if got := app.ClassOf(results[1].Err); got != app.Retryable {
		t.Errorf("class = %s, want %s", got, app.Retryable)
	}
}

// TestRecordIgnoresOutcomesForOtherCalls guards the mapping itself: an id that
// does not belong to this batch must not overwrite anything, because a
// validation refusal recorded before any call was made lives in that same
// slice.
func TestRecordIgnoresOutcomesForOtherCalls(t *testing.T) {
	refused := app.AsUnretryable(errors.New("sqs: a message needs a body"))
	results := make([]SendResult, 3)
	results[0].Err = refused
	record(results, []plan{{index: 1}}, &awssqs.SendMessageBatchOutput{
		Successful: []types.SendMessageBatchResultEntry{
			{Id: aws.String("1"), MessageId: aws.String("queue-id-1")},
			{Id: aws.String("0"), MessageId: aws.String("not-this-call")},
			{Id: aws.String("nonsense"), MessageId: aws.String("not-a-position")},
			{Id: aws.String("99"), MessageId: aws.String("past-the-end")},
		},
	})
	if !errors.Is(results[0].Err, refused) {
		t.Errorf("results[0] = %+v, want the refusal it already carried", results[0])
	}
	if results[1].MessageID != "queue-id-1" {
		t.Errorf("results[1] = %+v, want this call's own outcome", results[1])
	}
	if results[2] != (SendResult{}) {
		t.Errorf("results[2] = %+v, want it left alone", results[2])
	}
}

func TestAbortMarksWhatWasNeverOffered(t *testing.T) {
	cause := app.AsRetryable(errors.New("sqs: connection refused"))
	results := make([]SendResult, 4)
	abort(results, [][]plan{{{index: 2}}, {{index: 3}}}, cause)

	for _, index := range []int{2, 3} {
		if !errors.Is(results[index].Err, ErrSendAborted) {
			t.Errorf("results[%d] = %v, want it to carry ErrSendAborted", index,
				results[index].Err)
		}
		if !errors.Is(results[index].Err, cause) {
			t.Errorf("results[%d] = %v, want it to keep the cause", index, results[index].Err)
		}
		if got := app.ClassOf(results[index].Err); got != app.Retryable {
			t.Errorf("class = %s, want %s: not being attempted is evidence of nothing", got,
				app.Retryable)
		}
	}
	for _, index := range []int{0, 1} {
		if results[index] != (SendResult{}) {
			t.Errorf("results[%d] = %+v, want it left alone", index, results[index])
		}
	}
}

// TestSendBatchRefusesEntriesWithoutRefusingTheCall is the property a publisher
// depends on most: one message it can never send must not stop the others.
func TestSendBatchRefusesEntriesWithoutRefusingTheCall(t *testing.T) {
	messages := []Outbound{
		{Body: []byte("one"), GroupID: "w", DeduplicationID: "e1"},
		{Body: nil, GroupID: "w", DeduplicationID: "e2"},
		{Body: []byte("three"), GroupID: "w", DeduplicationID: "e3"},
	}
	var plans []plan
	results := make([]SendResult, len(messages))
	for i, message := range messages {
		entry, size, err := message.entry(i)
		if err != nil {
			results[i].Err = err
			continue
		}
		plans = append(plans, plan{index: i, size: size, entry: entry})
	}
	if len(plans) != 2 {
		t.Fatalf("%d messages survived validation, want 2", len(plans))
	}
	if results[1].Sent() {
		t.Errorf("results[1] = %+v, want the refusal", results[1])
	}
	batches := chunk(plans)
	if len(batches) != 1 {
		t.Fatalf("%d calls, want 1", len(batches))
	}
	if got := fmt.Sprint(shape(batches)); got != "[2]" {
		t.Errorf("chunk sizes = %s, want [2]", got)
	}
}
