package sqs

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// The bounds SendMessageBatch applies to one call, applied here instead so that
// a caller with more than a call's worth of work gets several calls rather than
// a refusal.
const (
	// maxBatchEntries is the most entries one SendMessageBatch may carry.
	maxBatchEntries = 10
	// maxBatchBytes is the most a batch may weigh in total, and also the most
	// any single message may weigh. 256 KiB, as SQS counts it — the body plus
	// the message attributes with their length prefixes, see attributesSize.
	maxBatchBytes = 256 * 1024
	// maxFifoIDBytes bounds a group id and a deduplication id. Both are well
	// inside it here — a wallet id and an event id are UUIDs — which is exactly
	// why the check is cheap and worth making: it catches the day somebody
	// composes an id out of something unbounded.
	maxFifoIDBytes = 128
)

// Outbound is one message to put on the queue.
//
// Every field but Attributes is required, and that is a property of the queues
// this service uses rather than of SQS: both are FIFO with content-based
// deduplication off, so a message that names no group has no order and a
// message that names no deduplication id has no identity.
type Outbound struct {
	// Body is the message as it goes on the wire — for the publisher, the
	// stored envelope's own bytes, unaltered.
	Body []byte
	// GroupID is the FIFO group this message is ordered within: the aggregate
	// id outbound, the wallet id inbound. Messages in one group are delivered
	// in order and one at a time; messages in different groups are not ordered
	// against each other at all.
	GroupID string
	// DeduplicationID is what makes two sends of this message one message
	// within SQS's deduplication window: the event id outbound, the envelope's
	// message id inbound. It must be stable across republication, or a
	// publisher that died between sending and recording the send would put a
	// second copy on the queue when it came back.
	DeduplicationID string
	// Attributes are carried beside the body, for trace context. Optional.
	Attributes map[string]string
}

// SendResult is what became of one outbound message.
//
// Results are positional: the result at index i is about the message at index i
// of the slice given to [Queue.SendBatch]. That is the correlation, and there
// is deliberately no id field duplicating it — a publisher marking exactly the
// events that were sent has to be certain which result belongs to which event,
// and a position cannot be mismatched the way a copied identifier can.
type SendResult struct {
	// MessageID is the queue's identifier for the message, set only when the
	// queue accepted it.
	MessageID string
	// Err is why this message is not on the queue, and nil when it is.
	//
	// Three things end up here and they are told apart by their chains: a
	// message this package refused before sending, a message SQS refused, and
	// a message never offered to SQS at all because an earlier chunk's call
	// failed — the last wrapping [ErrSendAborted].
	Err error
}

// Sent reports whether the queue accepted this message. It is the one question
// a publisher has to answer per event before it marks anything.
func (r SendResult) Sent() bool { return r.Err == nil }

// plan is one message that passed validation, with the entry it will be sent
// as and the size SQS will charge it at.
type plan struct {
	// index is the position in the caller's slice, carried through chunking so
	// that a result can be put back where it belongs.
	index int
	size  int
	entry types.SendMessageBatchRequestEntry
}

// SendBatch puts every message on the queue and reports what became of each
// one.
//
// It sends in as many calls as the bounds require: ten entries and 256 KiB per
// call, both enforced here rather than discovered from a refusal, because SQS
// refuses an oversized batch as a unit and a publisher that learned the limit
// that way would lose nine good messages to one that was too big.
//
// The returned slice is the same length as the input and positionally aligned
// with it. The error is non-nil only when nothing could be attempted at all —
// the queue is not resolved — because a call that sent some of its messages
// must report that fact, and an error for the call would force the caller to
// choose between marking events that were refused and republishing events that
// were not.
//
// Sending nothing is not an error. A publisher whose claim came back empty has
// nothing to do, and reporting that as a failure would alert on an idle system.
//
// A call that fails outright stops the send: its own entries carry that
// failure, and everything after them carries [ErrSendAborted]. Continuing would
// mean making the same doomed call for every remaining chunk, and the entries
// that were not attempted are rescheduled on the same terms as the ones that
// were refused.
func (q *Queue) SendBatch(ctx context.Context, messages []Outbound) ([]SendResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	url, err := q.resolved()
	if err != nil {
		return nil, err
	}

	results := make([]SendResult, len(messages))
	plans := make([]plan, 0, len(messages))
	for i, message := range messages {
		entry, size, err := message.entry(i)
		if err != nil {
			results[i].Err = err
			continue
		}
		plans = append(plans, plan{index: i, size: size, entry: entry})
	}

	batches := chunk(plans)
	for i, batch := range batches {
		sent, err := q.settings.client.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{
			QueueUrl: url,
			Entries:  entriesOf(batch),
		})
		if err != nil {
			failure := fail(fmt.Sprintf("send a batch of %d to %s", len(batch), q.settings.name),
				err)
			for _, p := range batch {
				results[p.index].Err = failure
			}
			abort(results, batches[i+1:], failure)
			return results, nil
		}
		record(results, batch, sent)
	}
	return results, nil
}

// entry turns one outbound message into what SQS is sent, refusing what SQS
// would refuse.
//
// The id is the message's position in the caller's slice, rendered. SQS only
// requires that ids are distinct within a batch and hands them back on both the
// successes and the failures, so using the position makes the mapping back
// exact — there is no lookup table to get wrong and no identifier of the
// caller's to collide.
func (m Outbound) entry(index int) (types.SendMessageBatchRequestEntry, int, error) {
	size := len(m.Body) + attributesSize(m.Attributes)
	switch {
	case len(m.Body) == 0:
		return types.SendMessageBatchRequestEntry{}, 0,
			app.AsUnretryable(errors.New("sqs: a message needs a body"))
	case m.GroupID == "":
		return types.SendMessageBatchRequestEntry{}, 0,
			app.AsUnretryable(errors.New("sqs: a message on a FIFO queue needs a group id"))
	case len(m.GroupID) > maxFifoIDBytes:
		return types.SendMessageBatchRequestEntry{}, 0, app.AsUnretryable(fmt.Errorf(
			"sqs: a group id is at most %d bytes, got %d", maxFifoIDBytes, len(m.GroupID)))
	case m.DeduplicationID == "":
		return types.SendMessageBatchRequestEntry{}, 0, app.AsUnretryable(errors.New(
			"sqs: a message on a FIFO queue without content-based deduplication needs a " +
				"deduplication id"))
	case len(m.DeduplicationID) > maxFifoIDBytes:
		return types.SendMessageBatchRequestEntry{}, 0, app.AsUnretryable(fmt.Errorf(
			"sqs: a deduplication id is at most %d bytes, got %d", maxFifoIDBytes,
			len(m.DeduplicationID)))
	case size > maxBatchBytes:
		// Refused per message rather than per call, and this is the difference
		// that matters: one event too large for the queue is one event that can
		// never be published, and taking the whole batch down with it would
		// stop the publisher entirely on a body nobody can shrink.
		return types.SendMessageBatchRequestEntry{}, 0, app.AsUnretryable(fmt.Errorf(
			"sqs: the message weighs %d bytes, past the %d SQS allows", size, maxBatchBytes))
	}
	if err := checkAttributes(m.Attributes); err != nil {
		return types.SendMessageBatchRequestEntry{}, 0, app.AsUnretryable(err)
	}
	return types.SendMessageBatchRequestEntry{
		Id:                     aws.String(strconv.Itoa(index)),
		MessageBody:            aws.String(string(m.Body)),
		MessageGroupId:         aws.String(m.GroupID),
		MessageDeduplicationId: aws.String(m.DeduplicationID),
		MessageAttributes:      encodeAttributes(m.Attributes),
	}, size, nil
}

// chunk packs messages into the largest batches SQS will accept.
//
// Greedy and order-preserving, which is not an accident: FIFO orders within a
// group, and a publisher hands this a wallet's events in the order their
// aggregate sequence was assigned. Packing that reordered them — filling by
// size, say — would put a wallet's second event in an earlier call than its
// first, and the queue would then hold them in that order for good.
func chunk(plans []plan) [][]plan {
	var (
		batches [][]plan
		current []plan
		weight  int
	)
	for _, p := range plans {
		full := len(current) == maxBatchEntries || weight+p.size > maxBatchBytes
		// The second half of that guard is about the first message only. One
		// that fills a batch on its own is refused before it reaches here, so
		// this cannot happen — and if it ever did, flushing would put an empty
		// batch in the list and SQS answers an empty batch with
		// EmptyBatchRequest, which is a refusal of every message behind it.
		if full && len(current) > 0 {
			batches = append(batches, current)
			current, weight = nil, 0
		}
		current = append(current, p)
		weight += p.size
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

// entriesOf is the SQS form of one batch.
func entriesOf(batch []plan) []types.SendMessageBatchRequestEntry {
	entries := make([]types.SendMessageBatchRequestEntry, len(batch))
	for i, p := range batch {
		entries[i] = p.entry
	}
	return entries
}

// record puts one call's per-entry outcomes back where they belong.
//
// An entry the service reported on neither list is recorded as a failure. It
// should not happen; if it does, the alternative is a result that says the
// message was sent because nothing said otherwise, and a publisher would mark
// an event nobody has seen.
func record(results []SendResult, batch []plan, sent *awssqs.SendMessageBatchOutput) {
	// Only this batch's own positions are accepted. An id naming anything else
	// is a service saying something about a message this call did not carry,
	// and acting on it would let one call overwrite another's outcome — or a
	// validation refusal recorded before any call was made.
	answered := make(map[int]bool, len(batch))
	for _, p := range batch {
		answered[p.index] = false
	}
	mine := func(id *string) (int, bool) {
		index, err := strconv.Atoi(aws.ToString(id))
		if err != nil {
			return 0, false
		}
		_, inBatch := answered[index]
		return index, inBatch
	}
	for _, ok := range sent.Successful {
		if index, inBatch := mine(ok.Id); inBatch {
			answered[index] = true
			results[index] = SendResult{MessageID: aws.ToString(ok.MessageId)}
		}
	}
	for _, refused := range sent.Failed {
		if index, inBatch := mine(refused.Id); inBatch {
			answered[index] = true
			results[index] = SendResult{Err: entryFailure(aws.ToString(refused.Code),
				aws.ToString(refused.Message), refused.SenderFault)}
		}
	}
	for index, reported := range answered {
		if !reported {
			results[index].Err = app.AsRetryable(errors.New(
				"sqs: the queue reported neither success nor failure for this message"))
		}
	}
}

// abort records the messages a stopped send never offered to SQS.
func abort(results []SendResult, remaining [][]plan, cause error) {
	for _, batch := range remaining {
		for _, p := range batch {
			results[p.index].Err = app.AsRetryable(fmt.Errorf("%w: %w", ErrSendAborted, cause))
		}
	}
}
