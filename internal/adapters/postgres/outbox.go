package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// appendOutbox writes one event for the publisher to pick up.
//
// aggregate_sequence is absent from the column list because the trigger assigns
// it and refuses one that was supplied. attempts starts at nought and the first
// attempt is due the moment the event occurred: there is nothing to wait for,
// and a schedule in the future would be a delay nobody asked for.
//
// The payload is the whole envelope. The row's other columns are its indexable
// projection rather than its content, so nothing here has to keep the two in
// step beyond writing them from one value — and because payload is jsonb, what
// is stored is the envelope's CONTENT and not its bytes.
const appendOutbox = `INSERT INTO wagering.outbox ` +
	`(event_id, aggregate_type, aggregate_id, event_type, event_version, payload, ` +
	`occurred_at, attempts, next_attempt_at) ` +
	`VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $7)`

// outbox appends events for a separate publisher to pick up. Nothing in this
// package publishes.
type outbox struct{ tx pgx.Tx }

// Append writes the envelopes in the order they were produced.
//
// One statement per envelope rather than one multi-row insert, because the
// order is what the per-aggregate sequence is assigned in and a loop makes that
// literal. The cost is bounded and small: an outcome emits at most two events,
// so the round trips this saves would buy less than the ordering argument a
// batch would then have to carry in a comment.
//
// It is the last statement of every callback, which is the lock order stated on
// app.Repos: the outbox keeps a per-aggregate counter row, which is a second
// per-wallet serialisation point, and taking it before the wallet is how two
// commands deadlock.
func (o outbox) Append(ctx context.Context, envelopes []app.Envelope) error {
	const what = "append to the outbox"
	for _, envelope := range envelopes {
		payload, err := json.Marshal(envelope)
		if err != nil {
			return app.AsUnretryable(fmt.Errorf("%s: render event %s: %w",
				what, envelope.EventID, err))
		}
		_, err = o.tx.Exec(ctx, appendOutbox,
			uuidOf(envelope.EventID),
			envelope.AggregateType,
			uuidOf(envelope.AggregateID),
			envelope.EventType,
			envelope.EventVersion,
			payload,
			envelope.OccurredAt,
		)
		if err != nil {
			return fail(what, err)
		}
	}
	return nil
}
