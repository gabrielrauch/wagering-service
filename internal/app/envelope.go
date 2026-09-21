package app

import (
	"encoding/json"
	"time"
	"uuid"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// EventID identifies one published event, stably across every republication.
type EventID uuid.UUID

// NewEventID mints a time-ordered event identifier.
func NewEventID() EventID { return EventID(uuid.NewV7()) }

// ParseEventID reads an event identifier from its canonical UUID form.
func ParseEventID(s string) (EventID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return EventID{}, invalidField("eventId", failure.InvalidFieldFormat, "event id %q is not a UUID", s)
	}
	return EventID(id), nil
}

// IsZero reports whether the identifier names nothing.
func (id EventID) IsZero() bool { return uuid.UUID(id) == uuid.Nil() }

// String returns the canonical UUID form.
func (id EventID) String() string { return uuid.UUID(id).String() }

// aggregateWallet is the only aggregate type this system publishes under. All
// four events name a wallet, the wallet is the root of the financial aggregate,
// and it is the FIFO group a consumer orders within.
const aggregateWallet = "WALLET"

// Envelope is the published form of a domain event: what it is, what it is
// about, what request it belongs to, and when it happened.
//
// The envelope lives here rather than in the domain because it is entirely about
// publication. The domain decides what happened; who is listening, what they
// deduplicate on and how they trace it back to a request are questions it has no
// rule for.
//
// The stored outbox row has no correlation or causation column, so the whole
// envelope is the payload and the row's columns are its indexable projection.
// The snapshot is therefore of CONTENT, not of bytes: payload is jsonb and
// normalises key order and spacing, so nothing may ever hash the stored bytes and
// expect to get back what was written.
type Envelope struct {
	EventID       EventID
	EventType     string
	EventVersion  int
	AggregateType string
	AggregateID   wagering.WalletID
	CorrelationID string
	// CausationID is the single thing that caused this one, and is empty when
	// there is none to name.
	CausationID string
	OccurredAt  time.Time
	Data        any
}

// Trace is what links an envelope back to the request that caused it.
//
// The two travel together from the command that started the work down to every
// envelope it emits, and they are grouped rather than passed side by side
// because two adjacent strings are transposable at a call site and the compiler
// would not notice. Causation is empty when there is no causing event to name.
type Trace struct {
	Correlation string
	Causation   string
}

// envelopeJSON is the wire form. It exists because the identifier types are
// arrays of bytes underneath, and marshalling those directly would publish a
// UUID as sixteen numbers.
type envelopeJSON struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateType string    `json:"aggregateType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          any       `json:"data"`
}

// MarshalJSON renders the envelope as it is published. OccurredAt is UTC and
// marshals as RFC 3339.
func (e Envelope) MarshalJSON() ([]byte, error) {
	return json.Marshal(envelopeJSON{
		EventID:       e.EventID.String(),
		EventType:     e.EventType,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID.String(),
		CorrelationID: e.CorrelationID,
		CausationID:   e.CausationID,
		OccurredAt:    e.OccurredAt.UTC(),
		Version:       e.EventVersion,
		Data:          e.Data,
	})
}

// NewEnvelope wraps a domain event for publication.
//
// It refuses an event it has no payload for. That refusal is the point of the
// switch: an event added to the domain without a payload here fails loudly at
// the moment it is first emitted, rather than publishing an empty object that a
// consumer would have no way to tell from an event that genuinely carried
// nothing.
func NewEnvelope(e wagering.Event, id EventID, trace Trace, at time.Time) (Envelope, error) {
	if e == nil {
		return Envelope{}, defect("an envelope needs an event")
	}
	if id.IsZero() {
		return Envelope{}, defect("an envelope needs an event id")
	}
	if trace.Correlation == "" {
		return Envelope{}, defect("an envelope needs a correlation id")
	}
	data, aggregate, err := payloadFor(e)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		EventID:       id,
		EventType:     e.EventType(),
		EventVersion:  e.EventVersion(),
		AggregateType: aggregateWallet,
		AggregateID:   aggregate,
		CorrelationID: trace.Correlation,
		CausationID:   trace.Causation,
		OccurredAt:    at.UTC(),
		Data:          data,
	}, nil
}

// payloadFor turns a domain event into its published payload and names the
// aggregate it belongs to.
func payloadFor(e wagering.Event) (any, wagering.WalletID, error) {
	switch ev := e.(type) {
	case wagering.WagerTransactionProcessed:
		external, _ := ev.ExternalTransactionID()
		return wagerTransactionProcessed{
			TransactionID:         ev.TransactionID().String(),
			WalletID:              ev.WalletID().String(),
			PlayerID:              ev.PlayerID().String(),
			Kind:                  ev.Kind().String(),
			Money:                 ev.Money(),
			BalanceAfter:          ev.BalanceAfter(),
			ExternalTransactionID: external.String(),
		}, ev.WalletID(), nil

	case wagering.WagerTransactionRejected:
		external, _ := ev.ExternalTransactionID()
		return wagerTransactionRejected{
			TransactionID:         ev.TransactionID().String(),
			WalletID:              ev.WalletID().String(),
			PlayerID:              ev.PlayerID().String(),
			Kind:                  ev.Kind().String(),
			Money:                 ev.Money(),
			FailureCode:           ev.FailureCode().String(),
			ExternalTransactionID: external.String(),
		}, ev.WalletID(), nil

	case wagering.WalletBalanceChanged:
		return walletBalanceChanged{
			WalletID:      ev.WalletID().String(),
			TransactionID: ev.TransactionID().String(),
			Direction:     ev.Direction().String(),
			Money:         ev.Money(),
			BalanceBefore: ev.BalanceBefore(),
			BalanceAfter:  ev.BalanceAfter(),
			WalletVersion: ev.WalletVersion(),
		}, ev.WalletID(), nil

	case wagering.WagerTransactionPendingReference:
		return wagerTransactionPendingReference{
			TransactionID:                  ev.TransactionID().String(),
			WalletID:                       ev.WalletID().String(),
			PlayerID:                       ev.PlayerID().String(),
			Kind:                           ev.Kind().String(),
			ReferenceExternalTransactionID: ev.ReferenceExternalTransactionID().String(),
			Attempts:                       ev.Attempts(),
			ExternalTransactionID:          ev.ExternalTransactionID().String(),
		}, ev.WalletID(), nil

	default:
		return nil, wagering.WalletID{}, defect(
			"%T has no published payload: add one to payloadFor before emitting it", e)
	}
}
