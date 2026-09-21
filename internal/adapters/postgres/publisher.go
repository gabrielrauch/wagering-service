package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

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
		`payload, attempts`

	// The claim is released along with the publication, so a published row
	// carries no stale holder for an operator to wonder about.
	markPublished = `UPDATE wagering.outbox SET published_at = $2, ` +
		`claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL ` +
		`WHERE event_id = $1 AND published_at IS NULL`

	// Putting the event back with a later schedule. The claim goes with it:
	// holding a claim on work this publisher has given up on would keep the row
	// out of the pool until the claim expired, for no reason.
	rescheduleEvent = `UPDATE wagering.outbox SET next_attempt_at = $2, ` +
		`claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL ` +
		`WHERE event_id = $1 AND published_at IS NULL`

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
		)
		if err := rows.Scan(&id, &walletID, &event.AggregateSequence, &event.EventType,
			&event.EventVersion, &event.Payload, &event.Attempts); err != nil {
			return nil, fail(what, err)
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

// MarkPublished records that one event has been sent and releases its claim.
//
// An event that is already published is not an error. A claim expires by wall
// clock, so a publisher that was slow can find its work has been done by
// another — and that is the recovery path working, not a failure to report.
func (c *OutboxClaims) MarkPublished(ctx context.Context, id app.EventID, at time.Time) error {
	_, err := c.pool.Exec(ctx, markPublished, uuidOf(id), at)
	return fail("mark an event published", err)
}

// Reschedule puts an event back with a later next attempt, releasing the claim.
//
// The instant is the caller's, because the backoff policy is the publisher's:
// how long to wait after a send that failed has no business meaning and nothing
// in the database has an opinion about it.
func (c *OutboxClaims) Reschedule(ctx context.Context, id app.EventID, at time.Time) error {
	_, err := c.pool.Exec(ctx, rescheduleEvent, uuidOf(id), at)
	return fail("reschedule an event", err)
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
