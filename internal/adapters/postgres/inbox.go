package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

const (
	selectInbox = `SELECT consumer_name, message_id, payload_hash, received_at, completed_at ` +
		`FROM wagering.inbox WHERE consumer_name = $1 AND message_id = $2`

	// received_at and completed_at are the same instant, on purpose. There is no
	// second write marking the message complete: the row and the domain changes
	// it describes commit together or not at all, so a row that exists is a
	// message that was handled.
	insertInbox = `INSERT INTO wagering.inbox ` +
		`(consumer_name, message_id, payload_hash, received_at, completed_at) ` +
		`VALUES ($1, $2, $3, $4, $4)`
)

// inbox reads and records handled messages.
type inbox struct{ tx pgx.Tx }

// Find reads the record of a message already handled, or (nil, nil).
func (i inbox) Find(ctx context.Context, k app.InboxKey) (*app.InboxRecord, error) {
	const what = "read the inbox"
	var (
		record    app.InboxRecord
		completed pgtype.Timestamptz
	)
	err := i.tx.QueryRow(ctx, selectInbox, k.Consumer, k.MessageID).Scan(
		&record.Consumer, &record.MessageID, &record.BodyHash, &record.ReceivedAt, &completed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fail(what, err)
	}
	record.CompletedAt = timeFrom(completed)
	return &record, nil
}

// Record writes the message as handled, in the same transaction as the work it
// describes.
//
// No ON CONFLICT. The caller has already read the row and found none, so a
// collision here is a second consumer that claimed it in between — and the
// primary key is the serialisation point that decides which of them handles the
// message. The loser is told the attempt failed and may send it again, which is
// what errors.go maps inbox_pkey to: by the time the message is redelivered the
// winner has committed, Find sees the row, and the redelivery replays the
// settled result instead of reapplying it.
func (i inbox) Record(ctx context.Context, m app.InboxMessage, at time.Time) error {
	_, err := i.tx.Exec(ctx, insertInbox, m.Consumer, m.MessageID, m.BodyHash, at)
	return fail("record an inbox message", err)
}
