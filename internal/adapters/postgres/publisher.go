package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"encoding/json"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The publisher's side of the outbox.
//
// It is not one of the internal/app ports and could not be: that package
// explicitly does not publish, so it declares no interface for this and never
// learns that a publisher exists. What it declares is OutboxWriter.Append,
// which is the other half — the write that happens inside the command's
// transaction. Everything below runs afterwards, outside it, on the pool.

const (
	// The supported way to take work, as docs/schema.md states it.
	//
	// Three predicates, each doing one job. published_at IS NULL and
	// next_attempt_at are which rows are outstanding and due. The claim test
	// takes a row whose claim has expired by WALL CLOCK, which is what lets
	// work abandoned by a publisher that crashed return to the pool visibly,
	// rather than waiting on a connection that may never close. And the NOT
	// EXISTS is the head-of-line rule: a wallet's second event is not claimable
	// until its first is published, so the publisher cannot send a wallet's
	// events out of order however it is scheduled.
	//
	// FOR UPDATE SKIP LOCKED keeps publishers off each other: a row another
	// publisher is claiming in this instant is passed over rather than waited
	// for. The order matches outbox_due_idx.
	//
	// The payload comes back WITHOUT the trace member the adapter wrote into
	// it, and the member comes back beside it. That split is what keeps the
	// published contract exactly what it was: the body a publisher sends is the
	// envelope and nothing else, so a downstream consumer reading it strictly
	// is not broken by a member it has never heard of, while the trace the
	// operation was submitted under still reaches the publisher — which is the
	// whole reason it was stored. See traceMember in outbox.go.
	//
	// The cast on the operand is not decoration: jsonb has both `- text` and
	// `- integer`, and an untyped literal makes the operator ambiguous.
	claimOutbox = `UPDATE wagering.outbox SET ` +
		`claimed_by = $1, claimed_at = $2, claim_expires_at = $3, attempts = attempts + 1 ` +
		`WHERE event_id IN (` +
		`SELECT o.event_id FROM wagering.outbox o ` +
		`WHERE o.published_at IS NULL AND o.next_attempt_at <= $2 ` +
		`AND (o.claim_expires_at IS NULL OR o.claim_expires_at <= $2) ` +
		`AND NOT EXISTS (SELECT 1 FROM wagering.outbox p ` +
		`WHERE p.aggregate_id = o.aggregate_id AND p.published_at IS NULL ` +
		`AND p.aggregate_sequence < o.aggregate_sequence) ` +
		`ORDER BY o.next_attempt_at, o.sequence FOR UPDATE SKIP LOCKED LIMIT $4) ` +
		`RETURNING event_id, aggregate_id, aggregate_sequence, event_type, event_version, ` +
		`payload - '` + traceMember + `'::text, payload -> '` + traceMember + `', ` +
		`occurred_at, attempts`

	// How far behind the outbox is: when the oldest thing nobody has published
	// yet happened.
	//
	// min() over a partial index rather than a column of its own. The only
	// index that covers `published_at IS NULL` is outbox_due_idx, so this scans
	// the UNPUBLISHED rows and nothing else — which is the backlog, and is
	// normally near zero. It is asked once per metric collection interval
	// rather than once per turn, because it exists to be a gauge and not a
	// decision.
	//
	// NULL when there is nothing waiting, which the caller reports as no lag
	// rather than as zero seconds of it; the two are the same number and
	// different facts, and only one of them should be distinguishable from a
	// query that failed.
	oldestUnpublished = `SELECT min(occurred_at) FROM wagering.outbox WHERE published_at IS NULL`

	// The claim is released along with the publication, so a published row
	// carries no stale holder for an operator to wonder about.
	markPublished = `UPDATE wagering.outbox SET published_at = $2, ` +
		`claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL ` +
		`WHERE event_id = $1 AND published_at IS NULL`

	// Putting the event back with a later schedule. The claim goes with it:
	// holding a claim on work this publisher has given up on would keep the row
	// out of the pool until the claim expired, for no reason.
	//
	// Scoped to the publisher that holds the claim, like releaseClaims below and
	// unlike markPublished above, and the asymmetry is the point. A claim
	// expires by wall clock, so a publisher can be slow enough to lose one to
	// another and not know it. If it could then reschedule anyway it would clear
	// the new holder's claim AND set next_attempt_at to an instant of its own
	// choosing — and because the outbox is head-of-line per aggregate, that
	// stalls the whole wallet's stream for as long as the stale publisher
	// happened to pick. Marking published has no equivalent: its worst case is
	// a second send, which at-least-once already permits.
	//
	// A row nobody holds is still reschedulable, which is what claimed_by IS
	// NULL admits: an event released at shutdown and not yet reclaimed has no
	// holder to take it from.
	rescheduleEvent = `UPDATE wagering.outbox SET next_attempt_at = $3, ` +
		`claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL ` +
		`WHERE event_id = $2 AND published_at IS NULL ` +
		`AND (claimed_by = $1 OR claimed_by IS NULL)`

	// Handing everything back at shutdown, so a replica that stops cleanly does
	// not leave its work waiting for a claim to expire.
	releaseClaims = `UPDATE wagering.outbox SET ` +
		`claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL ` +
		`WHERE claimed_by = $1 AND published_at IS NULL`
)

// ClaimedEvent is one outbox row a publisher has taken responsibility for.
//
// Payload is the stored envelope, as bytes read back from jsonb. It is what the
// publisher sends, unaltered: jsonb normalises key order and spacing, so these
// bytes are not the bytes that were written and nothing may hash them and
// expect to recognise the write.
type ClaimedEvent struct {
	EventID           app.EventID
	AggregateID       wagering.WalletID
	AggregateSequence int64
	EventType         string
	EventVersion      int
	Payload           []byte
	// Trace is the trace context the command that produced this event ran
	// under, as the propagator wrote it — a traceparent, and whatever else was
	// being carried. Empty for an event written by a process with telemetry
	// switched off, and for every event written before this existed.
	//
	// It is not in Payload and never was on the wire: see traceMember.
	Trace map[string]string
	// OccurredAt is when the event happened, which is what the outbox lag is
	// measured from.
	OccurredAt time.Time
	// Attempts counts this claim, so the first delivery reports one. A
	// publisher deciding how long to back off reads it as "how many times has
	// this been tried", which is the number it wants.
	Attempts int
}

// ClaimRequest is one turn of a publisher's loop.
type ClaimRequest struct {
	// By names the publisher, and is stored so that an operator can see who
	// holds what and until when.
	By string
	// Limit bounds the batch. Claiming is not free to be unbounded: every row
	// taken is a row no other publisher will touch until the hold expires.
	Limit int
	// At is the instant the claim is taken and due-ness is judged at. It comes
	// from the caller rather than from now() so that a publisher and its tests
	// read one clock.
	At time.Time
	// Hold is how long the claim stands before the row returns to the pool.
	// It must outlast a publication attempt and must not outlast a deployment,
	// which is the whole of the tuning.
	Hold time.Duration
}

func (r ClaimRequest) validate() error {
	switch {
	case r.By == "":
		return errors.New("postgres: a claim names the publisher taking it")
	case r.Limit <= 0:
		return fmt.Errorf("postgres: a claim takes a positive batch, got %d", r.Limit)
	case r.At.IsZero():
		return errors.New("postgres: a claim needs the instant it is taken at")
	case r.Hold <= 0:
		return fmt.Errorf("postgres: a claim needs a positive hold, got %s", r.Hold)
	}
	return nil
}

// OutboxClaims is how a publisher takes, completes and gives back outbox work.
//
// It holds the pool rather than a transaction, unlike everything a use case
// reaches: each call below is one statement that stands on its own, and wrapping
// them in a transaction would only widen the window in which a claim is held.
type OutboxClaims struct{ pool *pgxpool.Pool }

// NewOutboxClaims wires the publisher's side of the outbox.
func NewOutboxClaims(pool *pgxpool.Pool) (*OutboxClaims, error) {
	if pool == nil {
		return nil, errors.New("postgres: outbox claims need a pool")
	}
	return &OutboxClaims{pool: pool}, nil
}

// Claim takes a bounded batch of due, unpublished, head-of-line events.
//
// An empty batch is an empty slice and not an error: having nothing to publish
// is a publisher's normal state, and reporting it as a failure would alert on
// an idle system.
func (c *OutboxClaims) Claim(ctx context.Context, req ClaimRequest) ([]ClaimedEvent, error) {
	const what = "claim outbox events"
	if err := req.validate(); err != nil {
		return nil, app.AsUnretryable(err)
	}
	rows, err := c.pool.Query(ctx, claimOutbox, req.By, req.At, req.At.Add(req.Hold), req.Limit)
	if err != nil {
		return nil, fail(what, err)
	}
	defer rows.Close()

	var claimed []ClaimedEvent
	for rows.Next() {
		var (
			event        ClaimedEvent
			id, walletID pgtype.UUID
			carried      []byte
		)
		if err := rows.Scan(&id, &walletID, &event.AggregateSequence, &event.EventType,
			&event.EventVersion, &event.Payload, &carried, &event.OccurredAt,
			&event.Attempts); err != nil {
			return nil, fail(what, err)
		}
		if len(carried) > 0 {
			// A member this process cannot read is dropped rather than failing
			// the claim. The consequence is an event published outside the
			// trace it belonged to, which is a worse morning for somebody; a
			// refusal here would be a wallet's whole event stream stopped over
			// a telemetry field.
			_ = json.Unmarshal(carried, &event.Trace)
		}
		event.EventID = idFrom[app.EventID](id)
		event.AggregateID = idFrom[wagering.WalletID](walletID)
		claimed = append(claimed, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fail(what, err)
	}
	return claimed, nil
}

// OldestUnpublished reports when the oldest event nobody has published yet
// happened, and whether there was one.
//
// It is the outbox lag, before it is turned into a duration. The instant rather
// than the age, because the age depends on a clock and this package has no
// opinion about which one — the caller reads the same clock everything else in
// its process reads, and a query that returned an age would have used the
// database's.
//
// A false is an empty outbox and not an error. It is reported apart from a
// failure deliberately: a gauge that recorded zero for both would say the
// backlog is clear at the exact moment the database has stopped answering.
func (c *OutboxClaims) OldestUnpublished(ctx context.Context) (time.Time, bool, error) {
	var oldest pgtype.Timestamptz
	if err := c.pool.QueryRow(ctx, oldestUnpublished).Scan(&oldest); err != nil {
		return time.Time{}, false, fail("read the outbox lag", err)
	}
	if !oldest.Valid {
		return time.Time{}, false, nil
	}
	return oldest.Time, true, nil
}

// MarkPublished records that one event has been sent and releases its claim,
// reporting whether this call was the one that did it.
//
// An event that is already published is not an error, and the false is not a
// failure either. A claim expires by wall clock, so a publisher that was slow
// can find its work has been done by another — that is the recovery path
// working, and the only thing worth doing with the report is counting it.
//
// Both of the writes below report the row count rather than discarding it, and
// both do it by value where [transactions.Reschedule] does it by returning an
// error. That difference is deliberate and turns on who holds what. There, the
// caller is inside a transaction holding the row's lock and has just asserted
// the operation is parked, so changing nothing means the caller's model of the
// row is wrong — a defect, and the error channel is where a defect belongs.
// Here, nothing is held across the call and a claim may legitimately have
// expired between taking it and finishing with it, so changing nothing is an
// ordinary outcome that a publisher may want to observe and must not alert on.
func (c *OutboxClaims) MarkPublished(
	ctx context.Context,
	id app.EventID,
	at time.Time,
) (bool, error) {
	tag, err := c.pool.Exec(ctx, markPublished, uuidOf(id), at)
	if err != nil {
		return false, fail("mark an event published", err)
	}
	return tag.RowsAffected() > 0, nil
}

// Reschedule puts an event this publisher holds back with a later next attempt,
// releasing the claim, and reports whether it was still there to move.
//
// The instant is the caller's, because the backoff policy is the publisher's:
// how long to wait after a send that failed has no business meaning and nothing
// in the database has an opinion about it.
//
// A false means the event was published or claimed by somebody else while this
// publisher held it — see rescheduleEvent for why taking it back anyway would
// be worse than not rescheduling at all.
func (c *OutboxClaims) Reschedule(
	ctx context.Context,
	by string,
	id app.EventID,
	at time.Time,
) (bool, error) {
	if by == "" {
		return false, app.AsUnretryable(
			errors.New("postgres: rescheduling an event names the publisher holding it"))
	}
	tag, err := c.pool.Exec(ctx, rescheduleEvent, by, uuidOf(id), at)
	if err != nil {
		return false, fail("reschedule an event", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ReleaseClaims hands back everything this publisher holds, and reports how
// many rows it released.
//
// For shutdown. Without it a replica that stops cleanly leaves its claimed rows
// waiting out a hold that exists for the case where it did not.
func (c *OutboxClaims) ReleaseClaims(ctx context.Context, by string) (int, error) {
	if by == "" {
		return 0, app.AsUnretryable(errors.New("postgres: releasing claims names the publisher"))
	}
	tag, err := c.pool.Exec(ctx, releaseClaims, by)
	if err != nil {
		return 0, fail("release outbox claims", err)
	}
	return int(tag.RowsAffected()), nil
}
