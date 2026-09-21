//go:build integration

package postgres

import (
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
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

	// The limit is honoured even when more is claimable.
	w.markPublished(t, claims, batch[0].EventID, at(11))
	if got := w.claim(t, claims, "publisher-a", 1, at(12)); len(got) != 1 {
		t.Fatalf("claimed %d events under a limit of 1", len(got))
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

	// Publishing something already published is not a failure. A claim expires
	// by wall clock, so a slow publisher can find its work has been done by
	// another, and that is the recovery path working.
	w.markPublished(t, claims, first[0].EventID, at(15))
}

// TestReschedulingAndReleasingHandWorkBack covers the two ways a publisher
// gives up: on one event it could not send, and on everything it holds when it
// is shutting down.
func TestReschedulingAndReleasingHandWorkBack(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-handback", "100.00", "BRL")
	claims := w.claims(t)

	t.Run("rescheduling delays the event and drops the claim", func(t *testing.T) {
		batch := w.claim(t, claims, "publisher-a", 10, at(10))
		if len(batch) != 1 {
			t.Fatalf("claimed %d events, wanted 1", len(batch))
		}
		if err := claims.Reschedule(t.Context(), batch[0].EventID, at(60)); err != nil {
			t.Fatalf("reschedule: %v", err)
		}
		// The claim is gone, so nothing waits out a hold on work this publisher
		// has already given up on — but the event is not due yet either.
		if got := w.claim(t, claims, "publisher-b", 10, at(20)); len(got) != 0 {
			t.Fatalf("claimed %d events before the new schedule", len(got))
		}
		if got := w.claim(t, claims, "publisher-b", 10, at(61)); len(got) != 1 {
			t.Fatalf("claimed %d events once the new schedule came round, wanted 1", len(got))
		}
	})

	t.Run("releasing hands back everything this publisher holds", func(t *testing.T) {
		released, err := claims.ReleaseClaims(t.Context(), "publisher-b")
		if err != nil {
			t.Fatalf("release: %v", err)
		}
		if released != 1 {
			t.Fatalf("released %d claims, wanted 1", released)
		}
		if got := w.claim(t, claims, "publisher-c", 10, at(62)); len(got) != 1 {
			t.Fatalf("claimed %d events after a clean shutdown, wanted 1", len(got))
		}
	})
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

func (w *world) markPublished(t *testing.T, c *OutboxClaims, id app.EventID, now time.Time) {
	t.Helper()
	if err := c.MarkPublished(t.Context(), id, now); err != nil {
		t.Fatalf("mark %s published: %v", id, err)
	}
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
