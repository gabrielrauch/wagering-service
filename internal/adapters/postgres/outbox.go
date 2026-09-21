package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// appendOutbox writes one event for the publisher to pick up.
//
// aggregate_sequence is absent from the column list because the trigger assigns
// it and refuses one that was supplied. attempts starts at nought and the first
// attempt is due the moment the event occurred: there is nothing to wait for,
// and a schedule in the future would be a delay nobody asked for.
//
// The payload is the whole envelope, plus the one member described on
// [traceMember]. The row's other columns are its indexable projection rather
// than its content, so nothing here has to keep the two in step beyond writing
// them from one value — and because payload is jsonb, what is stored is the
// envelope's CONTENT and not its bytes.
const appendOutbox = `INSERT INTO wagering.outbox ` +
	`(event_id, aggregate_type, aggregate_id, event_type, event_version, payload, ` +
	`occurred_at, attempts, next_attempt_at) ` +
	`VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $7)`

// traceMember is the one member of an outbox payload that is not part of the
// event.
//
// # Why the trace is here at all
//
// An operation submitted over HTTP and the event it causes have to be ONE
// trace, and the outbox is the link that breaks: it is durable and
// asynchronous, so the publisher that eventually sends the event runs minutes
// later, in another process, with no memory of the request. Something written
// inside the command's transaction has to carry the trace across, and the outbox
// row is the only thing written there that the publisher reads.
//
// # Why it is in the payload rather than in a column
//
// Because there is no column, and there is no migration here to add one.
// wagering.outbox has fifteen columns and every one of them is load-bearing;
// payload is jsonb and is the only place a value can be added without changing
// the schema. The alternative considered and rejected was adding the trace to
// app.Envelope, which would have made an internal/app change out of an
// operational concern that layer explicitly has no opinion about — see the
// documentation on that type, which says what the envelope is FOR.
//
// # Why the publisher never sends it
//
// The claim strips it: see claimOutbox, which returns `payload - '$trace'` as
// the body and the member itself as a separate value. So the bytes that reach
// the queue are exactly the envelope, byte for byte, as they were before this
// existed — the published contract is unchanged and a downstream consumer
// reading the body strictly is not broken by a member it has never heard of.
// What reaches the consumer instead is the message ATTRIBUTES, which is where
// the task this implements says trace context belongs and where
// internal/adapters/sqs was already built to carry it.
//
// # Why a dollar sign
//
// Every member of the envelope is a camelCase identifier — eventId, occurredAt,
// correlationId — so a name beginning with a character none of them may contain
// cannot collide with one now or after the envelope grows. It also reads, to
// whoever opens a row in psql, as "this is not part of the document".
const traceMember = "$trace"

// outbox appends events for a separate publisher to pick up. Nothing in this
// package publishes.
type outbox struct {
	tx pgx.Tx
	// telemetry is where the trace this event belongs to is read from. Never
	// nil: the transaction manager passes [telemetry.Or]'s answer.
	telemetry *telemetry.Telemetry
}

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
//
// The trace is read once for the whole call rather than per envelope. Every
// envelope in one call was produced by one command inside one transaction, so
// they belong to one trace by construction, and reading it twice could only
// ever produce two answers that were the same or a bug.
func (o outbox) Append(ctx context.Context, envelopes []app.Envelope) error {
	const what = "append to the outbox"
	carried := o.telemetry.Inject(ctx)
	for _, envelope := range envelopes {
		payload, err := json.Marshal(envelope)
		if err != nil {
			return app.AsUnretryable(fmt.Errorf("%s: render event %s: %w",
				what, envelope.EventID, err))
		}
		payload, err = withTrace(payload, carried)
		if err != nil {
			return app.AsUnretryable(fmt.Errorf("%s: carry the trace of event %s: %w",
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

// withTrace puts the trace context into a rendered envelope, under
// [traceMember].
//
// It splices rather than decoding and re-encoding, and that is the whole reason
// it is written by hand. Round-tripping the document through a map would
// re-render every value in it — including the amounts, which cross this
// boundary as strings precisely so that nothing re-renders them — and would
// turn "the payload is the marshalled envelope" into "the payload is something
// this function produced from it".
//
// Nothing to carry writes nothing, so a process with telemetry switched off
// stores exactly what it stored before this existed.
func withTrace(payload []byte, carried map[string]string) ([]byte, error) {
	if len(carried) == 0 {
		return payload, nil
	}
	if len(payload) < 2 || payload[0] != '{' {
		// Unreachable: Envelope.MarshalJSON writes an object of eight members.
		// Asserted anyway, because the alternative to noticing is a payload
		// column that fails outbox_payload_is_an_object at the last statement
		// of a command that had otherwise succeeded.
		return nil, fmt.Errorf("a rendered envelope is not a JSON object (%d bytes)", len(payload))
	}
	trace, err := json.Marshal(carried)
	if err != nil {
		return nil, err
	}
	// The member goes first so that the rest of the document is copied
	// untouched. jsonb normalises the order at rest anyway; what matters here
	// is that no byte of the envelope is rewritten.
	spliced := make([]byte, 0, len(payload)+len(trace)+len(traceMember)+4)
	spliced = append(spliced, '{', '"')
	spliced = append(spliced, traceMember...)
	spliced = append(spliced, '"', ':')
	spliced = append(spliced, trace...)
	if payload[1] != '}' {
		// The separator is conditional because an EMPTY object has nothing to
		// separate from, and "{"$trace":{…},}" is not JSON — it is a payload
		// column that fails at the last statement of a command that had
		// otherwise succeeded. An envelope is never empty, which is exactly
		// why this would have been found by nothing.
		spliced = append(spliced, ',')
	}
	return append(spliced, payload[1:]...), nil
}
