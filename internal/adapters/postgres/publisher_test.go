//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// The hold every claim in these tests takes. Long enough that nothing expires
// by accident, and named so the expiry test can reason from it.
const testHold = time.Minute

// TestEventsAreAppendedInTheOrderTheyHappened pins what the outbox promises a
// consumer: contiguous numbering per wallet, in the order the domain emitted
// the events.
//
// The order is part of the contract — a processed operation is announced before
// the balance change it caused — and nothing in this package may sort it,
// because the numbering is assigned per row as the rows are inserted.
func TestEventsAreAppendedInTheOrderTheyHappened(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-events", "100.00", "BRL")
	w.apply(t, command(t, wagering.Bet, "player-events", "ext-e-1", "10.00", "BRL"), at(1))

	rows := w.outboxRows(t, wallet.ID())
	want := []string{
		"WagerTransactionProcessed", "WalletBalanceChanged",
		"WagerTransactionProcessed", "WalletBalanceChanged",
	}
	got := make([]string, 0, len(rows))
	for i, row := range rows {
		got = append(got, row.eventType)
		if row.aggregateSequence != int64(i+1) {
			t.Errorf("event %d carries aggregate sequence %d, wanted %d",
				i, row.aggregateSequence, i+1)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("published %v, wanted %v", got, want)
	}

	// The payload is the whole envelope, and it is content rather than bytes:
	// jsonb normalises key order and spacing, so what comes back is the
	// envelope's meaning and not the rendering that was written.
	if got := w.jsonField(t, rows[0].eventID, "eventType"); got != "WagerTransactionProcessed" {
		t.Fatalf("the stored payload names %q, wanted WagerTransactionProcessed", got)
	}
	if got := w.jsonField(t, rows[0].eventID, "correlationId"); got != "fixture" {
		t.Fatalf("the stored payload's correlation is %q, wanted fixture", got)
	}
}

// TestAClaimIsBoundedAndHeadOfLine pins the two properties a publisher's loop
// depends on.
//
// Bounded, because every row claimed is a row no other publisher will touch
// until the hold expires. Head-of-line per aggregate, because a wallet's second
// event is not claimable until its first is published — which is what makes
// per-wallet ordering a property of the claim rather than of how the publisher
// happens to be scheduled.
func TestAClaimIsBoundedAndHeadOfLine(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	one := w.openWallet(t, "player-claim-one", "100.00", "BRL")
	two := w.openWallet(t, "player-claim-two", "100.00", "BRL")
	claims := w.claims(t)

	// Two wallets, two events each. A claim asking for four may take only the
	// head of each aggregate.
	batch := w.claim(t, claims, "publisher-a", 4, at(10))
	if len(batch) != 2 {
		t.Fatalf("claimed %d events, wanted the head of each of the two wallets", len(batch))
	}
	for _, event := range batch {
		if event.AggregateSequence != 1 {
			t.Errorf("claimed sequence %d, wanted the head of the aggregate",
				event.AggregateSequence)
		}
		if event.Attempts != 1 {
			t.Errorf("a first claim reports %d attempts, wanted 1", event.Attempts)
		}
	}
	aggregates := []wagering.WalletID{batch[0].AggregateID, batch[1].AggregateID}
	if !slices.Contains(aggregates, one.ID()) || !slices.Contains(aggregates, two.ID()) {
		t.Fatalf("claimed %v, wanted one event from each wallet", aggregates)
	}

	// Both heads published, so both wallets' second events are now free of the
	// head-of-line rule and two rows are claimable at once. Only now does a
	// limit of one mean anything: until this point the batch was bounded by
	// what was claimable rather than by what was asked for, and a test that
	// asserted on the limit here would have passed with any limit at all.
	w.markPublished(t, claims, batch[0].EventID, at(11))
	w.markPublished(t, claims, batch[1].EventID, at(11))

	first := w.claim(t, claims, "publisher-a", 1, at(12))
	if len(first) != 1 {
		t.Fatalf("claimed %d events under a limit of 1, with two claimable", len(first))
	}
	second := w.claim(t, claims, "publisher-a", 1, at(13))
	if len(second) != 1 {
		t.Fatalf("claimed %d events on the second turn, wanted the other one", len(second))
	}
	if first[0].EventID == second[0].EventID {
		t.Fatal("the second turn claimed the event the first had already taken")
	}
	if got := w.claim(t, claims, "publisher-a", 1, at(14)); len(got) != 0 {
		t.Fatalf("claimed %d events with nothing left claimable", len(got))
	}
}

// TestASecondPublisherDoesNotTakeAClaimedEvent is the no-double-claim rule.
//
// The claim is a stored, visible hold rather than a row lock, which is what
// makes it work across publishers that never meet: the second one sees the
// claim in the row and passes over it, and an operator can see who holds what
// and until when.
func TestASecondPublisherDoesNotTakeAClaimedEvent(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-two-publishers", "100.00", "BRL")
	claims := w.claims(t)

	first := w.claim(t, claims, "publisher-a", 10, at(10))
	if len(first) != 1 {
		t.Fatalf("the first publisher claimed %d events, wanted 1", len(first))
	}
	second := w.claim(t, claims, "publisher-b", 10, at(11))
	if len(second) != 0 {
		t.Fatalf("the second publisher claimed %d events another publisher holds", len(second))
	}

	var holder string
	if err := w.owner.QueryRow(t.Context(),
		`SELECT claimed_by FROM wagering.outbox WHERE event_id = $1`,
		uuidOf(first[0].EventID)).Scan(&holder); err != nil {
		t.Fatalf("read the claim: %v", err)
	}
	if holder != "publisher-a" {
		t.Fatalf("the claim is held by %q, wanted publisher-a", holder)
	}
}

// TestAnExpiredClaimComesBack is why the claim expires by wall clock.
//
// A publisher that crashed holds nothing the database can notice: its
// connection may stay open for as long as a keepalive allows, and a claim that
// waited on that connection closing would strand the wallet's whole stream. An
// expiry the claim carries in its own row needs nobody to be alive to observe
// it.
func TestAnExpiredClaimComesBack(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-expiry", "100.00", "BRL")
	claims := w.claims(t)

	first := w.claim(t, claims, "publisher-gone", 10, at(10))
	if len(first) != 1 {
		t.Fatalf("claimed %d events, wanted 1", len(first))
	}
	if got := w.claim(t, claims, "publisher-live", 10, at(10).Add(testHold/2)); len(got) != 0 {
		t.Fatalf("a claim was taken %s into a %s hold", testHold/2, testHold)
	}

	recovered := w.claim(t, claims, "publisher-live", 10, at(10).Add(testHold+time.Second))
	if len(recovered) != 1 {
		t.Fatalf("recovered %d events after the hold expired, wanted 1", len(recovered))
	}
	if recovered[0].EventID != first[0].EventID {
		t.Fatal("the recovered event is not the abandoned one")
	}
	if recovered[0].Attempts != 2 {
		t.Fatalf("the recovered event reports %d attempts, wanted 2", recovered[0].Attempts)
	}
}

// TestPublishingAnEventReleasesTheNextOne is head-of-line advancing, which is
// the other half of the ordering guarantee: a wallet's stream moves exactly one
// event at a time and only forwards.
func TestPublishingAnEventReleasesTheNextOne(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-advance", "100.00", "BRL")
	claims := w.claims(t)

	first := w.claim(t, claims, "publisher-a", 10, at(10))
	if len(first) != 1 || first[0].AggregateSequence != 1 {
		t.Fatalf("claimed %v, wanted the head of the wallet", first)
	}
	w.markPublished(t, claims, first[0].EventID, at(11))

	second := w.claim(t, claims, "publisher-a", 10, at(12))
	if len(second) != 1 || second[0].AggregateSequence != 2 {
		t.Fatalf("claimed %v, wanted the wallet's second event", second)
	}
	w.markPublished(t, claims, second[0].EventID, at(13))

	if got := w.claim(t, claims, "publisher-a", 10, at(14)); len(got) != 0 {
		t.Fatalf("claimed %d events after the wallet's stream was published", len(got))
	}
	if got := w.count(t,
		`SELECT count(*) FROM wagering.outbox WHERE aggregate_id = $1 AND published_at IS NULL`,
		uuidOf(wallet.ID())); got != 0 {
		t.Fatalf("%d events are still unpublished", got)
	}

	// Publishing something already published is not a failure, and now says so:
	// false rather than an error. A claim expires by wall clock, so a slow
	// publisher can find its work has been done by another, and that is the
	// recovery path working rather than something to alert on.
	published, err := claims.MarkPublished(t.Context(), first[0].EventID, at(15))
	if err != nil {
		t.Fatalf("publish an already-published event: %v", err)
	}
	if published {
		t.Fatal("publishing an already-published event reported that it did it")
	}
}

// TestReschedulingDelaysAnEventAndDropsItsClaim is one of the two ways a
// publisher gives up: on a single event it could not send.
func TestReschedulingDelaysAnEventAndDropsItsClaim(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-reschedule", "100.00", "BRL")
	claims := w.claims(t)

	batch := w.claim(t, claims, "publisher-a", 10, at(10))
	if len(batch) != 1 {
		t.Fatalf("claimed %d events, wanted 1", len(batch))
	}
	if moved := w.putEventBack(t, claims, "publisher-a", batch[0].EventID, at(60)); !moved {
		t.Fatal("the holder could not reschedule its own event")
	}

	// The claim is gone, so nothing waits out a hold on work this publisher has
	// already given up on — but the event is not due yet either.
	if got := w.claim(t, claims, "publisher-b", 10, at(20)); len(got) != 0 {
		t.Fatalf("claimed %d events before the new schedule", len(got))
	}
	if got := w.claim(t, claims, "publisher-b", 10, at(61)); len(got) != 1 {
		t.Fatalf("claimed %d events once the new schedule came round, wanted 1", len(got))
	}
}

// TestOnlyTheHolderReschedulesAnEvent is what scoping the write to the claim
// buys.
//
// A claim expires by wall clock, so a publisher can be slow enough to lose one
// and not know it. Unscoped, its eventual failure to send would clear the new
// holder's claim and push next_attempt_at wherever the stale publisher decided
// — and the outbox is head-of-line per aggregate, so that stalls the whole
// wallet's stream for as long as it picked.
func TestOnlyTheHolderReschedulesAnEvent(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-stale-publisher", "100.00", "BRL")
	claims := w.claims(t)

	held := w.claim(t, claims, "publisher-holder", 10, at(10))
	if len(held) != 1 {
		t.Fatalf("claimed %d events, wanted 1", len(held))
	}

	if moved := w.putEventBack(t, claims, "publisher-stale", held[0].EventID, at(9000)); moved {
		t.Fatal("a publisher that holds no claim rescheduled somebody else's event")
	}
	// Untouched: still due when it was, and still claimed by the holder.
	var (
		holder string
		due    time.Time
	)
	if err := w.owner.QueryRow(t.Context(),
		`SELECT claimed_by, next_attempt_at FROM wagering.outbox WHERE event_id = $1`,
		uuidOf(held[0].EventID)).Scan(&holder, &due); err != nil {
		t.Fatalf("read the event back: %v", err)
	}
	if holder != "publisher-holder" {
		t.Fatalf("the claim is held by %q, wanted publisher-holder", holder)
	}
	if !due.Equal(at(0)) {
		t.Fatalf("the event is due at %s, wanted the %s it was written with", due, at(0))
	}

	// And an event nobody holds is still reschedulable, which is what an event
	// released at shutdown and not yet reclaimed looks like.
	if _, err := claims.ReleaseClaims(t.Context(), "publisher-holder"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if moved := w.putEventBack(t, claims, "publisher-any", held[0].EventID, at(70)); !moved {
		t.Fatal("an unclaimed event could not be rescheduled")
	}
}

// TestReleasingHandsBackEverythingAPublisherHolds is the other way a publisher
// gives up: on everything at once, because it is shutting down.
//
// Without it a replica that stops cleanly leaves its claimed rows waiting out a
// hold that exists for the case where it did not.
func TestReleasingHandsBackEverythingAPublisherHolds(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-shutdown-one", "100.00", "BRL")
	w.openWallet(t, "player-shutdown-two", "100.00", "BRL")
	claims := w.claims(t)

	held := w.claim(t, claims, "publisher-stopping", 10, at(10))
	if len(held) != 2 {
		t.Fatalf("claimed %d events, wanted the head of each of the two wallets", len(held))
	}

	released, err := claims.ReleaseClaims(t.Context(), "publisher-stopping")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released != 2 {
		t.Fatalf("released %d claims, wanted 2", released)
	}
	// Immediately, rather than when the hold would have expired.
	if got := w.claim(t, claims, "publisher-next", 10, at(11)); len(got) != 2 {
		t.Fatalf("claimed %d events after a clean shutdown, wanted 2", len(got))
	}
	// And a publisher holding nothing releases nothing.
	released, err = claims.ReleaseClaims(t.Context(), "publisher-stopping")
	if err != nil {
		t.Fatalf("release again: %v", err)
	}
	if released != 0 {
		t.Fatalf("released %d claims for a publisher that holds none", released)
	}
}

// claims builds the publisher's side of the outbox on the application's pool.
func (w *world) claims(t *testing.T) *OutboxClaims {
	t.Helper()
	c, err := NewOutboxClaims(w.app)
	if err != nil {
		t.Fatalf("new outbox claims: %v", err)
	}
	return c
}

func (w *world) claim(
	t *testing.T,
	c *OutboxClaims,
	by string,
	limit int,
	now time.Time,
) []ClaimedEvent {
	t.Helper()
	batch, err := c.Claim(t.Context(), ClaimRequest{By: by, Limit: limit, At: now, Hold: testHold})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return batch
}

// markPublished publishes an event and asserts that this call was the one that
// did it, which every caller here has arranged to be true.
func (w *world) markPublished(t *testing.T, c *OutboxClaims, id app.EventID, now time.Time) {
	t.Helper()
	published, err := c.MarkPublished(t.Context(), id, now)
	if err != nil {
		t.Fatalf("mark %s published: %v", id, err)
	}
	if !published {
		t.Fatalf("event %s was already published by somebody else", id)
	}
}

// putEventBack reschedules an event and reports whether it was still there to
// move.
func (w *world) putEventBack(
	t *testing.T,
	c *OutboxClaims,
	by string,
	id app.EventID,
	to time.Time,
) bool {
	t.Helper()
	moved, err := c.Reschedule(t.Context(), by, id, to)
	if err != nil {
		t.Fatalf("reschedule %s: %v", id, err)
	}
	return moved
}

// outboxRow is the projection the ordering assertions read.
type outboxRow struct {
	eventID           app.EventID
	aggregateSequence int64
	eventType         string
}

// outboxRows reads a wallet's stream in the order a publisher takes it.
func (w *world) outboxRows(t *testing.T, id wagering.WalletID) []outboxRow {
	t.Helper()
	rows, err := w.owner.Query(t.Context(),
		`SELECT event_id, aggregate_sequence, event_type FROM wagering.outbox `+
			`WHERE aggregate_id = $1 ORDER BY aggregate_sequence`, uuidOf(id))
	if err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	defer rows.Close()

	var found []outboxRow
	for rows.Next() {
		var (
			row outboxRow
			raw pgtype.UUID
		)
		if err := rows.Scan(&raw, &row.aggregateSequence, &row.eventType); err != nil {
			t.Fatalf("scan an outbox row: %v", err)
		}
		row.eventID = idFrom[app.EventID](raw)
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the outbox: %v", err)
	}
	return found
}

// jsonField reads one top-level string out of a stored payload.
func (w *world) jsonField(t *testing.T, id app.EventID, field string) string {
	t.Helper()
	var value string
	if err := w.owner.QueryRow(t.Context(),
		`SELECT payload ->> $2 FROM wagering.outbox WHERE event_id = $1`,
		uuidOf(id), field).Scan(&value); err != nil {
		t.Fatalf("read %s of event %s: %v", field, id, err)
	}
	return value
}

// TestPublishersClaimingAtOnceNeverTakeTheSameEvent is the same rule under
// contention.
//
// Two publishers that never meet are kept apart by the stored claim; two
// claiming in the same instant are kept apart by SKIP LOCKED, which passes over
// a row another publisher is taking rather than waiting for it. Both are needed:
// the stored claim cannot see an UPDATE that has not committed, and a row lock
// cannot outlive the statement that took it.
func TestPublishersClaimingAtOnceNeverTakeTheSameEvent(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	const wallets = 8
	for i := range wallets {
		w.openWallet(t, "player-contended-"+string(rune('a'+i)), "100.00", "BRL")
	}
	claims := w.claims(t)

	const publishers = 4
	batches := make(chan []ClaimedEvent, publishers)
	for i := range publishers {
		go func() {
			batch, err := claims.Claim(t.Context(), ClaimRequest{
				By:    "publisher-" + string(rune('a'+i)),
				Limit: 4,
				At:    at(10),
				Hold:  testHold,
			})
			if err != nil {
				t.Errorf("claim: %v", err)
			}
			batches <- batch
		}()
	}

	taken := map[app.EventID]string{}
	for range publishers {
		for _, event := range <-batches {
			var holder string
			if err := w.owner.QueryRow(t.Context(),
				`SELECT claimed_by FROM wagering.outbox WHERE event_id = $1`,
				uuidOf(event.EventID)).Scan(&holder); err != nil {
				t.Fatalf("read the claim: %v", err)
			}
			if first, twice := taken[event.EventID]; twice {
				t.Fatalf("event %s was claimed by %s and again by %s",
					event.EventID, first, holder)
			}
			taken[event.EventID] = holder
		}
	}
	if len(taken) != wallets {
		t.Fatalf("%d events were claimed between them, wanted the head of each of %d wallets",
			len(taken), wallets)
	}
}

// TestTheClaimReturnsTheTraceBesideTheEnvelopeAndNotInsideIt is the SQL half of
// the outbox's trace carriage.
//
// The adapter writes the trace into the payload because there is no column for
// it and no migration here to add one, and the claim takes it back out —
// `payload - '$trace'` as the body, `payload -> '$trace'` beside it. That
// projection is what keeps the published contract exactly what it was: the
// bytes a publisher sends are the envelope, so a downstream consumer reading
// the body strictly is not broken by a member it has never heard of.
//
// Both halves have to hold together, and only one of them is visible in Go.
// An operator dropping the `- '$trace'` would publish a telemetry field to
// every consumer; dropping the `-> '$trace'` would publish nothing and silently
// end every trace at the outbox. Neither shows up anywhere else.
func TestTheClaimReturnsTheTraceBesideTheEnvelopeAndNotInsideIt(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	// Opened at nought, so it emits no events of its own: the outbox is
	// head-of-line per wallet, and an opening pair ahead of the event under
	// test would be all the claim ever returned.
	wallet := w.openWallet(t, "player-traced-claim", "0.00", "BRL")

	// The row is written through the real adapter, inside a transaction the
	// manager opened under a span — which is exactly how a command writes one.
	spans := tracetest.NewSpanRecorder()
	reporting, err := telemetry.New(telemetry.Config{
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		t.Fatalf("build the telemetry: %v", err)
	}
	manager, err := NewTxManager(TxConfig{
		Pool:             w.app,
		LockTimeout:      testLockTimeout,
		StatementTimeout: testStatementTimeout,
		Telemetry:        reporting,
	})
	if err != nil {
		t.Fatalf("new transaction manager: %v", err)
	}

	// The span an HTTP request would have opened, so that what is asserted is
	// the trace a command inherited rather than one the manager rooted itself.
	requestCtx, request := reporting.Start(t.Context(), "POST /wagering/transactions")
	defer request.End()

	envelope := anOutboxEnvelope(t, wallet.ID())
	var traced oteltrace.TraceID
	err = manager.WithinMovement(requestCtx, func(ctx context.Context, r *app.Repos) error {
		traced = oteltrace.SpanContextFromContext(ctx).TraceID()
		return r.Outbox.Append(ctx, []app.Envelope{envelope})
	})
	if err != nil {
		t.Fatalf("append an event: %v", err)
	}
	if !traced.IsValid() {
		t.Fatal("the transaction ran in no trace, so this test cannot mean anything")
	}

	// The row itself carries the member, which is the only reason the publisher
	// can ever see it.
	if got := w.jsonField(t, envelope.EventID, traceMember); got == "" {
		t.Fatalf("the stored payload carries no %s member", traceMember)
	}

	claimed := w.claim(t, w.claims(t), "publisher-traced", 10, at(20))
	var found *ClaimedEvent
	for i := range claimed {
		if claimed[i].EventID == envelope.EventID {
			found = &claimed[i]
		}
	}
	if found == nil {
		t.Fatalf("the claim did not return event %s", envelope.EventID)
	}

	// Beside the envelope.
	if found.Trace["traceparent"] == "" {
		t.Errorf("the claim returned no carried trace: %v", found.Trace)
	}
	continued := oteltrace.SpanContextFromContext(reporting.Extract(t.Context(), found.Trace))
	if got := continued.TraceID(); got != traced {
		t.Errorf("the claim carries trace %s, wanted the transaction's %s", got, traced)
	}
	if got := request.SpanContext().TraceID(); got != traced {
		t.Errorf("the transaction ran in trace %s and the request in %s; a command's "+
			"transaction is a level of the request's trace", traced, got)
	}

	// The transaction is a span of its own, under the request's. Without it the
	// trace still reaches the outbox — the request's context does that on its
	// own — and the level that says how long the wallet lock was held is simply
	// missing, which is the child span the task asks for and the one a reader
	// looking at a slow submission wants.
	movement := endedSpan(t, spans, telemetry.SpanMovement)
	if movement == nil {
		t.Fatalf("no %s span finished; the transaction opened none", telemetry.SpanMovement)
	}
	if got := movement.Parent().SpanID(); got != request.SpanContext().SpanID() {
		t.Errorf("the transaction's span hangs off %s, wanted the request's %s",
			got, request.SpanContext().SpanID())
	}

	// And not inside it.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(found.Payload, &members); err != nil {
		t.Fatalf("the claimed payload is not JSON: %v\n%s", err, found.Payload)
	}
	if _, present := members[traceMember]; present {
		t.Errorf("the claimed body still carries %s, so it would be published:\n%s",
			traceMember, found.Payload)
	}
	if _, present := members["eventId"]; !present {
		t.Errorf("the claimed body is not an envelope:\n%s", found.Payload)
	}

	// occurred_at comes back too, because the outbox lag is measured from it.
	if !found.OccurredAt.Equal(envelope.OccurredAt) {
		t.Errorf("the claim reports the event happened at %s, wanted %s",
			found.OccurredAt, envelope.OccurredAt)
	}
}

// TestTheOutboxLagIsTheAgeOfTheOldestUnpublishedEvent pins the gauge's query.
//
// It is the one number that says whether the outbox is draining, and because
// the outbox is head-of-line per wallet it also bounds how far behind any one
// wallet's event stream has fallen. An empty outbox is told apart from a query
// that failed, because a gauge reporting zero for both would say the backlog is
// clear at the exact moment nothing is answering.
func TestTheOutboxLagIsTheAgeOfTheOldestUnpublishedEvent(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	claims := w.claims(t)

	if _, waiting, err := claims.OldestUnpublished(t.Context()); err != nil || waiting {
		t.Fatalf("an empty outbox reports waiting=%v, err=%v", waiting, err)
	}

	wallet := w.openWallet(t, "player-lag", "100.00", "BRL")
	rows := w.outboxRows(t, wallet.ID())
	if len(rows) == 0 {
		t.Fatal("opening a funded wallet wrote no outbox rows")
	}

	oldest, waiting, err := claims.OldestUnpublished(t.Context())
	if err != nil {
		t.Fatalf("read the outbox lag: %v", err)
	}
	if !waiting {
		t.Fatal("the outbox holds unpublished rows and reports nothing waiting")
	}

	// Everything published: the backlog is empty again, and that is reported as
	// nothing waiting rather than as an age of nought.
	for _, row := range rows {
		w.markPublished(t, claims, row.eventID, at(30))
	}
	if _, waiting, err := claims.OldestUnpublished(t.Context()); err != nil || waiting {
		t.Fatalf("a drained outbox reports waiting=%v, err=%v (oldest was %s)",
			waiting, err, oldest)
	}
}

// anOutboxEnvelope is one event to append, of the shape the application layer
// produces.
func anOutboxEnvelope(t *testing.T, wallet wagering.WalletID) app.Envelope {
	t.Helper()
	return app.Envelope{
		EventID:       app.NewEventID(),
		EventType:     "WalletBalanceChanged",
		EventVersion:  1,
		AggregateType: "WALLET",
		AggregateID:   wallet,
		CorrelationID: "thread-traced",
		OccurredAt:    at(19).UTC(),
		Data:          map[string]string{"walletId": wallet.String()},
	}
}

// endedSpan is the first finished span with that name, or nil.
func endedSpan(t *testing.T, spans *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans.Ended() {
		if span.Name() == name {
			return span
		}
	}
	return nil
}
