package workers

import (
	"context"
	"encoding/json/v2"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The fakes below record what reached them and answer what the test told them
// to. Several of the properties under test here are about a call that must NOT
// happen — a message that is not deleted, an event that is not marked — so every
// one of them counts, and "nothing touched" is only assertable if the thing that
// was not touched is counting.
//
// They are safe for concurrent use. A consumer drives several receivers at once
// and the race detector is part of the gate.

// settled is how long a test waits for a worker to do something before deciding
// it never will. Generous, because the wait is on a goroutine and not on a
// clock: every one of these completes in microseconds when the behaviour is
// there, and the value only bounds how long a broken build takes to fail.
const settledWithin = 5 * time.Second

// discard is a logger for a test that is not asserting on log lines.
func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// recorder keeps the records a test wants to assert on.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *recorder) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *recorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recorder) WithGroup(string) slog.Handler      { return h }

func (h *recorder) logger() *slog.Logger { return slog.New(h) }

// await waits for a record with this message and returns it.
//
// The waiting is the point. Several of the barriers a test synchronises on —
// an event marked, a batch decided — are a call reaching a fake, and the line
// reporting what became of it is written after that call returns. Asserting on
// the log the instant the barrier opens is a race the test would lose about one
// run in a hundred.
func (h *recorder) await(t *testing.T, message string) *slog.Record {
	t.Helper()
	deadline := time.Now().Add(settledWithin)
	for {
		if record := h.find(message); record != nil {
			return record
		}
		if time.Now().After(deadline) {
			t.Fatalf("no line saying %q", message)
			return nil
		}
		time.Sleep(time.Millisecond)
	}
}

// find returns the first record with this message, or nil. It is for asserting
// that something was NOT said; [recorder.await] is for the other direction.
func (h *recorder) find(message string) *slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.records {
		if h.records[i].Message == message {
			return &h.records[i]
		}
	}
	return nil
}

// attr reads one attribute off a record, rendered.
func attr(record *slog.Record, key string) (string, bool) {
	var (
		value string
		found bool
	)
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value, found = a.Value.String(), true
			return false
		}
		return true
	})
	return value, found
}

// hidden is one call to ChangeVisibility.
type hidden struct {
	handle string
	in     time.Duration
}

// fakeQueue stands in for the inbound queue.
//
// Receive serves the scripted batches in order. Once the script is exhausted it
// signals and then blocks on the context, which gives a test an exact barrier:
// a receiver only asks for another batch once it has finished handling the last
// one, so "the script ran out" means "everything scripted has been decided".
type fakeQueue struct {
	mu sync.Mutex

	batches [][]sqs.Message
	errs    []error
	// stream, when set, replaces the script with an endless supply, for a test
	// about how much a consumer takes on at once rather than about what it
	// does with any one message.
	stream func(i int) []sqs.Message

	receives int
	deleted  []string
	changed  []hidden
	released []string

	deleteErr  error
	changeErr  error
	releaseErr error

	once      sync.Once
	exhausted chan struct{}
}

func newFakeQueue(batches ...[]sqs.Message) *fakeQueue {
	return &fakeQueue{batches: batches, exhausted: make(chan struct{})}
}

func (q *fakeQueue) Receive(ctx context.Context) ([]sqs.Message, error) {
	q.mu.Lock()
	i := q.receives
	q.receives++
	stream := q.stream
	var (
		batch []sqs.Message
		err   error
	)
	if i < len(q.errs) {
		err = q.errs[i]
	}
	if i < len(q.batches) {
		batch = q.batches[i]
	}
	past := i >= len(q.batches) && i >= len(q.errs)
	q.mu.Unlock()

	if stream != nil {
		return stream(i), nil
	}
	if !past {
		return batch, err
	}
	q.once.Do(func() { close(q.exhausted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// handled waits until every scripted batch has been decided.
func (q *fakeQueue) handled(t *testing.T) {
	t.Helper()
	select {
	case <-q.exhausted:
	case <-time.After(settledWithin):
		t.Fatal("the consumer never came back for another batch")
	}
}

func (q *fakeQueue) Delete(_ context.Context, receiptHandle string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.deleteErr != nil {
		return q.deleteErr
	}
	q.deleted = append(q.deleted, receiptHandle)
	return nil
}

func (q *fakeQueue) ChangeVisibility(_ context.Context, receiptHandle string, in time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.changeErr != nil {
		return q.changeErr
	}
	q.changed = append(q.changed, hidden{handle: receiptHandle, in: in})
	return nil
}

func (q *fakeQueue) Release(_ context.Context, receiptHandle string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.releaseErr != nil {
		return q.releaseErr
	}
	q.released = append(q.released, receiptHandle)
	return nil
}

// receiveCount is how many times the consumer has asked for a batch.
func (q *fakeQueue) receiveCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.receives
}

func (q *fakeQueue) deletedHandles() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.deleted...)
}

func (q *fakeQueue) changes() []hidden {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]hidden(nil), q.changed...)
}

func (q *fakeQueue) releasedHandles() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.released...)
}

// fakeSubmitter stands in for the application's write path.
type fakeSubmitter struct {
	mu sync.Mutex

	calls []app.SubmitOperation
	// answer decides each call from its ordinal. Nil means every submission is
	// processed.
	//
	// It is given the context the consumer passed down, because a fake that
	// blocked without watching it would be a fake the application layer is not:
	// a shutdown deadline ends a submission in flight, and a test about that
	// deadline needs the submission to end when it does.
	answer func(ctx context.Context, n int, cmd app.SubmitOperation) (app.OperationResult, error)

	running int
	peak    int
}

func (s *fakeSubmitter) Submit(
	ctx context.Context, cmd app.SubmitOperation,
) (app.OperationResult, error) {
	s.mu.Lock()
	n := len(s.calls)
	s.calls = append(s.calls, cmd)
	s.running++
	s.peak = max(s.peak, s.running)
	answer := s.answer
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running--
		s.mu.Unlock()
	}()

	if answer == nil {
		return processed(), nil
	}
	return answer(ctx, n, cmd)
}

func (s *fakeSubmitter) submissions() []app.SubmitOperation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]app.SubmitOperation(nil), s.calls...)
}

func (s *fakeSubmitter) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *fakeSubmitter) peakConcurrency() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// processed is the ordinary answer: an operation that moved money.
func processed() app.OperationResult {
	return app.OperationResult{
		TransactionID: wagering.NewTransactionID(),
		Kind:          wagering.Bet,
		Status:        wagering.Processed,
	}
}

// fakeResumer stands in for the application's worker door.
type fakeResumer struct {
	mu sync.Mutex

	calls     int
	principal app.Principal
	answer    func(n int) (app.ResumeOutcome, error)

	turns chan struct{}
}

func newFakeResumer(answer func(n int) (app.ResumeOutcome, error)) *fakeResumer {
	return &fakeResumer{answer: answer, turns: make(chan struct{}, 64)}
}

func (r *fakeResumer) Resume(
	_ context.Context, principal app.Principal,
) (app.ResumeOutcome, error) {
	r.mu.Lock()
	n := r.calls
	r.calls++
	r.principal = principal
	answer := r.answer
	r.mu.Unlock()

	select {
	case r.turns <- struct{}{}:
	default:
	}
	if answer == nil {
		return app.ResumeOutcome{}, nil
	}
	return answer(n)
}

func (r *fakeResumer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// actedAs is the identity the worker resumed under.
func (r *fakeResumer) actedAs() app.Principal {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.principal
}

// awaitTurns waits for n turns to have been taken.
func (r *fakeResumer) awaitTurns(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(settledWithin)
	for range n {
		select {
		case <-r.turns:
		case <-deadline:
			t.Fatalf("the worker took fewer than %d turns", n)
		}
	}
}

// claimCall, markCall and rescheduleCall are what reached the outbox.
type claimCall struct{ req postgres.ClaimRequest }

type markCall struct {
	id app.EventID
	at time.Time
}

type rescheduleCall struct {
	by string
	id app.EventID
	at time.Time
}

// fakeOutbox stands in for the publisher's side of the outbox.
type fakeOutbox struct {
	mu sync.Mutex

	claims      []claimCall
	marks       []markCall
	reschedules []rescheduleCall
	releases    int

	claimAnswer      func(n int) ([]postgres.ClaimedEvent, error)
	markAnswer       func(id app.EventID) (bool, error)
	rescheduleAnswer func(id app.EventID) (bool, error)
	releaseErr       error

	once      sync.Once
	exhausted chan struct{}
	// decisions carries one token per event marked or rescheduled, so a test
	// can wait for a batch to have been settled without waiting for the
	// publisher's poll interval to come round again.
	decisions chan struct{}
}

func newFakeOutbox(batches ...[]postgres.ClaimedEvent) *fakeOutbox {
	o := &fakeOutbox{exhausted: make(chan struct{}), decisions: make(chan struct{}, 64)}
	o.claimAnswer = func(n int) ([]postgres.ClaimedEvent, error) {
		if n < len(batches) {
			return batches[n], nil
		}
		return nil, nil
	}
	return o
}

func (o *fakeOutbox) Claim(
	_ context.Context, req postgres.ClaimRequest,
) ([]postgres.ClaimedEvent, error) {
	o.mu.Lock()
	n := len(o.claims)
	o.claims = append(o.claims, claimCall{req: req})
	answer := o.claimAnswer
	o.mu.Unlock()

	claimed, err := answer(n)
	if len(claimed) == 0 && err == nil {
		o.once.Do(func() { close(o.exhausted) })
	}
	return claimed, err
}

// drained waits until the publisher has found the outbox empty, which is the
// point at which everything scripted has been published or put back.
func (o *fakeOutbox) drained(t *testing.T) {
	t.Helper()
	select {
	case <-o.exhausted:
	case <-time.After(settledWithin):
		t.Fatal("the publisher never claimed an empty batch")
	}
}

func (o *fakeOutbox) MarkPublished(
	_ context.Context, id app.EventID, at time.Time,
) (bool, error) {
	o.mu.Lock()
	o.marks = append(o.marks, markCall{id: id, at: at})
	answer := o.markAnswer
	o.mu.Unlock()
	o.decided()
	if answer == nil {
		return true, nil
	}
	return answer(id)
}

func (o *fakeOutbox) Reschedule(
	_ context.Context, by string, id app.EventID, at time.Time,
) (bool, error) {
	o.mu.Lock()
	o.reschedules = append(o.reschedules, rescheduleCall{by: by, id: id, at: at})
	answer := o.rescheduleAnswer
	o.mu.Unlock()
	o.decided()
	if answer == nil {
		return true, nil
	}
	return answer(id)
}

func (o *fakeOutbox) decided() {
	select {
	case o.decisions <- struct{}{}:
	default:
	}
}

// settled waits until n events have been marked published or rescheduled, which
// is every way an event leaves a publisher's hands.
func (o *fakeOutbox) settled(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(settledWithin)
	for range n {
		select {
		case <-o.decisions:
		case <-deadline:
			t.Fatalf("fewer than %d events were published or rescheduled", n)
		}
	}
}

func (o *fakeOutbox) ReleaseClaims(_ context.Context, _ string) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.releases++
	if o.releaseErr != nil {
		return 0, o.releaseErr
	}
	return 1, nil
}

func (o *fakeOutbox) marked() []markCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]markCall(nil), o.marks...)
}

func (o *fakeOutbox) put() []rescheduleCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]rescheduleCall(nil), o.reschedules...)
}

func (o *fakeOutbox) claimed() []claimCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]claimCall(nil), o.claims...)
}

func (o *fakeOutbox) releaseCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.releases
}

// fakeSender stands in for the outbound queue.
type fakeSender struct {
	mu sync.Mutex

	sent   [][]sqs.Outbound
	answer func(n int, messages []sqs.Outbound) ([]sqs.SendResult, error)
}

func (s *fakeSender) SendBatch(
	_ context.Context, messages []sqs.Outbound,
) ([]sqs.SendResult, error) {
	s.mu.Lock()
	n := len(s.sent)
	s.sent = append(s.sent, messages)
	answer := s.answer
	s.mu.Unlock()

	if answer == nil {
		results := make([]sqs.SendResult, len(messages))
		for i := range results {
			results[i] = sqs.SendResult{MessageID: "queue-message"}
		}
		return results, nil
	}
	return answer(n, messages)
}

func (s *fakeSender) batches() [][]sqs.Outbound {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]sqs.Outbound(nil), s.sent...)
}

// fixedClock reads one instant, so that a schedule a worker writes is a value a
// test can state rather than one it has to bound.
type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

// testTime is the instant every test that needs one reads.
func testTime() time.Time { return time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC) }

// event builds one claimed outbox row.
func event(attempts int, payload []byte) postgres.ClaimedEvent {
	return postgres.ClaimedEvent{
		EventID:           app.NewEventID(),
		AggregateID:       wagering.NewWalletID(),
		AggregateSequence: 1,
		EventType:         "WagerTransactionProcessed",
		EventVersion:      1,
		Payload:           payload,
		Attempts:          attempts,
	}
}

// message builds one received message.
func message(handle string, body []byte, receiveCount int) sqs.Message {
	return sqs.Message{
		MessageID:     "queue-" + handle,
		ReceiptHandle: handle,
		Body:          body,
		ReceiveCount:  receiveCount,
	}
}

// validBody renders a well-formed envelope with the named top-level members
// replaced, so that a test about one broken member says only that.
func validBody(t *testing.T, overrides map[string]any) []byte {
	t.Helper()
	body := map[string]any{
		"messageId":  "message-1",
		"type":       MessageType,
		"occurredAt": "2026-09-21T09:59:00Z",
		"data":       validData(nil),
	}
	maps.Copy(body, overrides)
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("render an envelope: %v", err)
	}
	return raw
}

// validData renders the business fields, with the named members replaced.
func validData(overrides map[string]any) map[string]any {
	data := map[string]any{
		"provider":              "acme",
		"externalTransactionId": "external-1",
		"idempotencyKey":        "key-1",
		"playerId":              "player-1",
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money":                 map[string]any{"amount": "25.00", "currency": "BRL"},
	}
	maps.Copy(data, overrides)
	return data
}
