package postgres_test

import (
	"database/sql"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

const insertEvent = `
	INSERT INTO wagering.outbox (
		event_id, aggregate_type, aggregate_id, event_type, event_version,
		payload, occurred_at, attempts, next_attempt_at
	) VALUES ($1, 'WALLET', $2, $3, 1, $4, $5, 0, $6)`

// claimDue is the query a publisher runs. It is part of what the schema is for,
// so it is written here once and documented in docs/schema.md as the supported
// way to take work.
//
// The NOT EXISTS is the head-of-line rule: only the oldest unpublished event of
// an aggregate is claimable, so a wallet's events are published in the order
// they happened. Different wallets are unaffected, and SKIP LOCKED keeps
// publishers off each other.
const claimDue = `
	UPDATE wagering.outbox SET
		claimed_by = $1, claimed_at = $2, claim_expires_at = $3, attempts = attempts + 1
	WHERE event_id IN (
		SELECT o.event_id
		FROM wagering.outbox o
		WHERE o.published_at IS NULL
		  AND o.next_attempt_at <= $2
		  AND (o.claim_expires_at IS NULL OR o.claim_expires_at <= $2)
		  AND NOT EXISTS (
			  SELECT 1 FROM wagering.outbox p
			  WHERE p.aggregate_id = o.aggregate_id
				AND p.published_at IS NULL
				AND p.aggregate_sequence < o.aggregate_sequence
		  )
		ORDER BY o.next_attempt_at, o.sequence
		FOR UPDATE SKIP LOCKED
		LIMIT $4
	)
	RETURNING aggregate_id, aggregate_sequence`

// emit writes one event for a wallet and returns its identity.
func emit(t *testing.T, db execer, w wallet, eventType string, at time.Time) string {
	t.Helper()
	id := wagering.NewTransactionID().String()
	accepts(t, db, insertEvent, id, w.id, eventType, `{"walletId":"`+w.id+`"}`, at, at)
	return id
}

// claim takes up to n due events and returns them as "<aggregate>/<sequence>".
func claim(t *testing.T, db *sql.DB, at time.Time, n int) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), claimDue, "publisher-1", at, at.Add(time.Minute), n)
	if err != nil {
		t.Fatalf("claim due events: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var claimed []string
	for rows.Next() {
		var (
			aggregate string
			sequence  int64
		)
		if err := rows.Scan(&aggregate, &sequence); err != nil {
			t.Fatalf("scan claim: %v", err)
		}
		claimed = append(claimed, aggregate+"/"+strconv.FormatInt(sequence, 10))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read claims: %v", err)
	}
	return claimed
}

// TestOutboxNumbersEventsPerAggregate: the sequence is assigned by the database
// and is contiguous per wallet, which is what lets a consumer notice a gap
// instead of trusting the publisher to have sent things in order.
func TestOutboxNumbersEventsPerAggregate(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	first := newWallet(t, db, 0)
	second := newWallet(t, db, 0)

	emit(t, db, first, "WagerTransactionProcessed", base)
	emit(t, db, first, "WalletBalanceChanged", base)
	emit(t, db, second, "WagerTransactionProcessed", base)

	for _, c := range []struct {
		wallet wallet
		want   []int64
	}{
		{first, []int64{1, 2}},
		{second, []int64{1}},
	} {
		got := column[int64](t, db, `
			SELECT aggregate_sequence FROM wagering.outbox
			WHERE aggregate_id = $1 ORDER BY sequence`, c.wallet.id)
		if !slices.Equal(got, c.want) {
			t.Errorf("the wallet's events are numbered %v, wanted %v", got, c.want)
		}
	}
}

// TestOutboxPreservesTheOrderEventsWereWritten: the domain emits
// WagerTransactionProcessed before WalletBalanceChanged for every operation, and
// the sequence assigned inside that one transaction has to keep them that way.
func TestOutboxPreservesTheOrderEventsWereWritten(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	if err := inTx(t, db, func(tx *sql.Tx) error {
		for _, name := range []string{"WagerTransactionProcessed", "WalletBalanceChanged"} {
			id := wagering.NewTransactionID().String()
			if _, err := tx.ExecContext(t.Context(), insertEvent, id, w.id, name, `{"walletId":"`+w.id+`"}`, base, base); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("emit an operation's events: %v", err)
	}

	order := names(t, db, `
		SELECT event_type FROM wagering.outbox
		WHERE aggregate_id = $1 ORDER BY aggregate_sequence`, w.id)
	want := []string{"WagerTransactionProcessed", "WalletBalanceChanged"}
	if !slices.Equal(order, want) {
		t.Errorf("events are numbered %v, wanted %v", order, want)
	}
}

// TestOutboxSerialisesWritersForOneAggregate: two writers for one wallet queue
// behind each other and take consecutive numbers, rather than racing for the
// same one. Writers for different wallets never meet.
func TestOutboxSerialisesWritersForOneAggregate(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	ahead, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	behind, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Both transactions are rolled back however the test ends. Cleanups run after
	// t.Context() is cancelled, which aborts whatever statement is still in flight,
	// so neither rollback waits on a lock the other holds and the order they are
	// registered in does not matter. That depends on the pair being opened against
	// t.Context(): on a background context, the blocked writer would deadlock here.
	t.Cleanup(func() { _ = ahead.Rollback() })
	t.Cleanup(func() { _ = behind.Rollback() })

	first := wagering.NewTransactionID().String()
	if _, err := ahead.ExecContext(t.Context(), insertEvent, first, w.id, "WagerTransactionProcessed",
		`{"walletId":"`+w.id+`"}`, base, base); err != nil {
		t.Fatalf("the first writer was refused: %v", err)
	}

	second := wagering.NewTransactionID().String()
	done := make(chan error, 1)
	go func() {
		_, err := behind.ExecContext(t.Context(), insertEvent, second, w.id, "WalletBalanceChanged",
			`{"walletId":"`+w.id+`"}`, base, base)
		done <- err
	}()

	waitForBlockedWriter(t, db)

	if err := ahead.Commit(); err != nil {
		t.Fatalf("the first writer could not commit: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("the second writer was refused instead of queueing: %v", err)
	}
	if err := behind.Commit(); err != nil {
		t.Fatalf("the second writer could not commit: %v", err)
	}

	for _, c := range []struct {
		event string
		want  int64
	}{{first, 1}, {second, 2}} {
		var got int64
		if err := db.QueryRowContext(t.Context(), `
			SELECT aggregate_sequence FROM wagering.outbox WHERE event_id = $1`, c.event).Scan(&got); err != nil {
			t.Fatalf("read sequence: %v", err)
		}
		if got != c.want {
			t.Errorf("event took sequence %d, wanted %d", got, c.want)
		}
	}
}

// TestOutboxPublishesAnAggregateInOrder is the end-to-end FIFO rule: a wallet's
// second event is not claimable until its first is published, so a publisher
// cannot send them out of order however it is scheduled.
func TestOutboxPublishesAnAggregateInOrder(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	first := newWallet(t, db, 0)
	second := newWallet(t, db, 0)

	emit(t, db, first, "WagerTransactionProcessed", base)
	emit(t, db, first, "WalletBalanceChanged", base)
	emit(t, db, second, "WagerTransactionProcessed", base)

	at := base.Add(time.Second)
	claimed := claim(t, db, at, 10)

	want := map[string]bool{first.id + "/1": true, second.id + "/1": true}
	if len(claimed) != 2 {
		t.Fatalf("claimed %v, wanted only the head of each wallet", claimed)
	}
	for _, c := range claimed {
		if !want[c] {
			t.Errorf("claimed %s, which is not the head of its wallet", c)
		}
	}

	t.Run("the next event becomes claimable once the first is published", func(t *testing.T) {
		accepts(t, db, `
			UPDATE wagering.outbox SET published_at = $2, claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL
			WHERE aggregate_id = $1 AND aggregate_sequence = 1`, first.id, at)

		// The claim above is still held on the second wallet's head, so only
		// the first wallet's next event comes back.
		got := claim(t, db, at.Add(time.Second), 10)
		if len(got) != 1 || got[0] != first.id+"/2" {
			t.Errorf("claimed %v, wanted only %s/2", got, first.id)
		}
	})
}

// TestOutboxClaimsExpire: work abandoned by a crashed publisher returns to the
// pool by wall clock, rather than waiting on a connection that may never close.
func TestOutboxClaimsExpire(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	emit(t, db, w, "WagerTransactionProcessed", base)

	at := base.Add(time.Second)
	if got := claim(t, db, at, 10); len(got) != 1 {
		t.Fatalf("claimed %v, wanted one event", got)
	}

	t.Run("while the claim stands", func(t *testing.T) {
		if got := claim(t, db, at.Add(time.Second), 10); len(got) != 0 {
			t.Errorf("claimed %v, but another publisher already holds it", got)
		}
	})

	t.Run("once the claim has expired", func(t *testing.T) {
		if got := claim(t, db, at.Add(2*time.Minute), 10); len(got) != 1 {
			t.Errorf("claimed %v, wanted the abandoned event back", got)
		}
	})
}

// TestOutboxKeepsEventIdentityAcrossRepublication: republishing is an update to
// the same row, never a new one, so a consumer deduplicating on event identity
// still sees one event.
func TestOutboxKeepsEventIdentityAcrossRepublication(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	id := emit(t, db, w, "WagerTransactionProcessed", base)

	accepts(t, db, `UPDATE wagering.outbox SET published_at = $2 WHERE event_id = $1`, id, base.Add(time.Second))
	accepts(t, db, `UPDATE wagering.outbox SET published_at = NULL, attempts = attempts + 1 WHERE event_id = $1`, id)
	accepts(t, db, `UPDATE wagering.outbox SET published_at = $2 WHERE event_id = $1`, id, base.Add(time.Minute))

	var count int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM wagering.outbox WHERE aggregate_id = $1`, w.id).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 1 {
		t.Errorf("republishing left %d rows, wanted 1", count)
	}
}

// TestOutboxPayloadIsASnapshot: an event is what happened, written once. The
// application holds UPDATE on the whole table because publishing IS an update —
// a claim, an attempt count, a schedule, a publication time — and before this
// guard nothing kept that privilege off the event itself. `UPDATE outbox SET
// payload = '{}'` succeeded. The trigger refuses any change to the event's
// identity, ordering, type, payload and time, for every role including the
// owner; the publisher's columns stay writable, and DELETE stays allowed
// because retention needs it.
func TestOutboxPayloadIsASnapshot(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)
	other := newWallet(t, db, 0)

	const rule = "outbox_payload_is_a_snapshot"

	for _, c := range []struct {
		name   string
		column string
		value  any
	}{
		{"the payload", "payload", `{}`},
		{"the event type", "event_type", "WalletBalanceChanged"},
		{"the event version", "event_version", int32(2)},
		{"when it occurred", "occurred_at", base.Add(time.Hour)},
		{"the event's identity", "event_id", wagering.NewTransactionID().String()},
		{"the aggregate", "aggregate_id", other.id},
		{"the aggregate's numbering", "aggregate_sequence", int64(99)},
		{"the aggregate type", "aggregate_type", "PLAYER"},
	} {
		t.Run(c.name, func(t *testing.T) {
			id := emit(t, db, w, "WagerTransactionProcessed", base)
			refusesRule(t, db, rule,
				`UPDATE wagering.outbox SET `+c.column+` = $2 WHERE event_id = $1`, id, c.value)
		})
	}

	// Restating a column to the value it already holds changes nothing, and is
	// not refused: the publisher's statements do not touch these columns, but
	// an operator's UPDATE ... SET payload = payload should not fail either.
	t.Run("restating the payload unchanged", func(t *testing.T) {
		id := emit(t, db, w, "WagerTransactionProcessed", base)
		accepts(t, db, `UPDATE wagering.outbox SET payload = payload WHERE event_id = $1`, id)
	})

	t.Run("the publisher's own columns are still writable", func(t *testing.T) {
		id := emit(t, db, w, "WagerTransactionProcessed", base)
		at := base.Add(time.Second)
		accepts(t, db, `
			UPDATE wagering.outbox SET
				claimed_by = 'publisher-1', claimed_at = $2, claim_expires_at = $3, attempts = attempts + 1
			WHERE event_id = $1`, id, at, at.Add(time.Minute))
		accepts(t, db, `
			UPDATE wagering.outbox SET next_attempt_at = $2,
				claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL
			WHERE event_id = $1`, id, at.Add(time.Minute))
		accepts(t, db, `
			UPDATE wagering.outbox SET published_at = $2,
				claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL
			WHERE event_id = $1`, id, at.Add(2*time.Second))
	})

	t.Run("retention still deletes", func(t *testing.T) {
		id := emit(t, db, w, "WagerTransactionProcessed", base)
		accepts(t, db, `UPDATE wagering.outbox SET published_at = $2 WHERE event_id = $1`, id, base)
		accepts(t, db, `DELETE FROM wagering.outbox WHERE event_id = $1 AND published_at IS NOT NULL`, id)
	})
}

func TestOutboxRefusesWhatItCannotPublish(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	t.Run("an event type nobody declared", func(t *testing.T) {
		refuses(t, db, foreignKeyViolation, insertEvent,
			wagering.NewTransactionID().String(), w.id, "SomethingHappened",
			`{"walletId":"`+w.id+`"}`, base, base)
	})

	t.Run("an aggregate that does not exist", func(t *testing.T) {
		refuses(t, db, foreignKeyViolation, insertEvent,
			wagering.NewTransactionID().String(), wagering.NewWalletID().String(),
			"WagerTransactionProcessed", `{}`, base, base)
	})

	t.Run("a payload that is not an object", func(t *testing.T) {
		refuses(t, db, checkViolation, insertEvent,
			wagering.NewTransactionID().String(), w.id, "WagerTransactionProcessed",
			`["not an object"]`, base, base)
	})

	t.Run("half a claim", func(t *testing.T) {
		id := emit(t, db, w, "WagerTransactionProcessed", base)
		refuses(t, db, checkViolation,
			`UPDATE wagering.outbox SET claimed_by = 'publisher-1' WHERE event_id = $1`, id)
	})
}

// TestAggregateNumberingSurvivesRetention is the reason the counter is a table
// and not max() over the outbox.
//
// docs/schema.md prescribes pruning published rows, and the application holds
// the DELETE to do it. A max() derivation has no memory of the numbers it has
// already issued, so once the last row for a quiet wallet is pruned the next
// event starts at 1 again — and outbox_aggregate_sequence_key cannot object,
// because the rows it would collide with are the ones that were deleted. A
// consumer tracking last-seen-per-aggregate reads the replay as a gap, or as
// something it has already handled.
func TestAggregateNumberingSurvivesRetention(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	for range 3 {
		emit(t, db, w, "WalletBalanceChanged", base)
	}
	accepts(t, db, `UPDATE wagering.outbox SET published_at = $2 WHERE aggregate_id = $1`, w.id, base)

	// The retention statement docs/schema.md prints, run in full.
	accepts(t, db, `DELETE FROM wagering.outbox WHERE published_at IS NOT NULL AND aggregate_id = $1`, w.id)

	var surviving int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM wagering.outbox WHERE aggregate_id = $1`, w.id).
		Scan(&surviving); err != nil {
		t.Fatalf("count what retention left: %v", err)
	}
	if surviving != 0 {
		t.Fatalf("retention left %d rows behind, so this proves nothing", surviving)
	}

	emit(t, db, w, "WalletBalanceChanged", base.Add(time.Hour))

	var sequence int64
	if err := db.QueryRowContext(t.Context(), `
		SELECT aggregate_sequence FROM wagering.outbox WHERE aggregate_id = $1`, w.id).
		Scan(&sequence); err != nil {
		t.Fatalf("read the sequence after retention: %v", err)
	}
	if sequence != 4 {
		t.Errorf("numbering restarted at %d after pruning, wanted it to carry on at 4", sequence)
	}
}

// TestNumberingDoesNotWaitOnAMoneyMovement is the deadlock that used to be.
//
// Assigning the sequence took FOR UPDATE on the wallet row, which is a lock
// upgrade: a balance change holds FOR NO KEY UPDATE and every foreign key into
// wallet holds FOR KEY SHARE, and FOR UPDATE may join neither. Two ordinary
// writers for one wallet could therefore each end up waiting on the other and be
// broken apart by the deadlock detector, which is the opposite of the queueing
// the lock was taken for.
//
// FOR NO KEY UPDATE is the lock that tells the two apart. The old FOR UPDATE
// conflicted with it and blocked; the outbox foreign key's own FOR KEY SHARE
// does not, so with the wallet untouched by the trigger the insert goes through
// while a balance change is still in flight.
func TestNumberingDoesNotWaitOnAMoneyMovement(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	mover, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Rolled back rather than committed: the point is to hold the lock for the
	// whole test, and the deferred pairing would refuse a balance change with no
	// ledger entry behind it anyway.
	t.Cleanup(func() { _ = mover.Rollback() })

	if _, err := mover.ExecContext(t.Context(), moveWallet, w.id, int64(7500), int64(2), settleAt(int64(2))); err != nil {
		t.Fatalf("start a balance change: %v", err)
	}

	emitted := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(t.Context(), insertEvent,
			wagering.NewTransactionID().String(), w.id, "WalletBalanceChanged",
			`{"walletId":"`+w.id+`"}`, base, base)
		emitted <- err
	}()

	select {
	case err := <-emitted:
		if err != nil {
			t.Errorf("emitting an event during a balance change was refused: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("emitting an event waited on the balance change's row lock")
	}
}
