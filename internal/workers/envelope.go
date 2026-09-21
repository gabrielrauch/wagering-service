package workers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// MessageType is the one type this consumer accepts on the inbound queue.
//
// The original challenge specification is not in this tree, so the spelling is
// this implementation's and is recorded in ARCHITECTURE.md as such. It is
// PascalCase to match the event types the outbound envelope already carries —
// WagerTransactionProcessed and its siblings — and it names what the message
// is, which is a provider asking for an operation to be applied, rather than
// which operation: the kind lives in data.kind exactly as it does on the HTTP
// path, so that one payload is not described in two places.
//
// It is exported because a producer has to be able to name it and because the
// integration suite sends it. Any other value is a permanent error.
const MessageType = "WagerTransactionSubmitted"

// envelope is the inbound message: what it is, which message it is, when the
// provider says it happened, and the operation it carries.
//
// It is not [app.Envelope]. That one is the published form of an event this
// service emitted; this is a command arriving from outside, and the two share
// neither their members nor their direction. Naming them alike in one package
// would be an invitation to publish one where the other was meant.
type envelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       operation `json:"data"`
}

// operation is the business fields of one submission.
//
// The member names are the HTTP body's, because they are the names
// wagering.CanonicalPayload hashes: the bytes a provider sends over either
// transport, the bytes that are hashed, and the bytes that come back are one
// vocabulary, and a submission that arrived by queue must hash to what the same
// submission would have hashed to over HTTP.
//
// The idempotency key is the one addition, and it is a member here where HTTP
// takes it in a header. There is no header on a queue; the key is still about
// this delivery of the operation rather than about the operation, which is why
// the canonical payload excludes it in both directions.
type operation struct {
	Provider                       string     `json:"provider"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	IdempotencyKey                 string     `json:"idempotencyKey"`
	PlayerID                       string     `json:"playerId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          moneyInput `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitzero"`
}

// moneyInput is an amount and its currency, both still strings.
//
// Neither is parsed here and neither is normalised. money.Parse is the only
// thing in this system that turns them into a value, it is reached once per
// submission inside the application layer, and what a provider sent is what is
// hashed — so a transport that repaired a value on the way in would be hashing
// something nobody submitted.
type moneyInput struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// parseEnvelope reads and checks one message body.
//
// Every failure it reports is permanent by construction. Nothing here depends
// on anything outside the bytes in hand, so a body this refuses once it refuses
// every time, and the consumer hands it to the redrive policy rather than
// retrying it — see [Consumer] for what that costs and why it is what the queue
// is configured for.
//
// The decode is strict, exactly as the HTTP path's is and for the same reasons:
// a member this envelope does not have is refused rather than ignored, a member
// named twice is refused rather than resolved to the last one, and names match
// case for case. {"kind":"BET","kind":"LOSS"} read leniently is a LOSS with
// nothing left to say that a BET was also asked for, and a producer that added
// a field this service silently does not apply would find out from a balance.
// The cost is that a producer extending the envelope reaches the dead-letter
// queue rather than being quietly misunderstood, which is the trade this tree
// makes everywhere else it reads input.
func parseEnvelope(body []byte) (envelope, error) {
	var e envelope
	if err := json.Unmarshal(body, &e, json.RejectUnknownMembers(true)); err != nil {
		return envelope{}, describeDecode(err)
	}
	if err := e.validate(); err != nil {
		return envelope{}, err
	}
	return e, nil
}

// describeDecode says why a body could not be read, in this contract's terms
// rather than the decoder's.
//
// The decoder's own message names the Go type it could not fill, which is an
// internal name that says nothing to whoever is reading a dead-letter queue.
func describeDecode(err error) error {
	switch {
	case errors.Is(err, json.ErrUnknownName):
		return errors.New("workers: the message names a field this envelope does not have")
	case errors.Is(err, jsontext.ErrDuplicateName):
		return errors.New("workers: the message names the same field twice")
	default:
		return errors.New("workers: the message is not a JSON envelope this consumer can read")
	}
}

// validate checks what this consumer must know before the application layer is
// reached.
//
// It deliberately stops there. The business fields are parsed exactly once, by
// the application layer, so that HTTP and SQS cannot disagree about what a
// submission said — a second opinion here would be a second place for the two
// to drift apart. What is checked is what has to be right before a transaction
// is opened: which message this is, that it is a message this consumer handles
// at all, and that the provider it claims is a provider.
func (e envelope) validate() error {
	switch {
	case e.MessageID == "":
		return errors.New("workers: the envelope carries no messageId")
	case !opaqueID(e.MessageID):
		return fmt.Errorf("workers: the envelope's messageId is not storable: at most %d bytes, "+
			"no control characters, no surrounding whitespace", maxOpaqueIDBytes)
	case e.Type == "":
		return errors.New("workers: the envelope carries no type")
	case e.Type != MessageType:
		// Named, not printed back verbatim beyond what the envelope already
		// said: this line is what an operator reads off the dead-letter queue,
		// and it has to be actionable without them opening the body.
		return fmt.Errorf("workers: %q is not a message type this consumer handles, expected %q",
			e.Type, MessageType)
	case e.OccurredAt.IsZero():
		return errors.New("workers: the envelope carries no occurredAt")
	}
	return nil
}

// maxOpaqueIDBytes is what wagering.opaque_id allows, restated because the
// inbox's message id is stored in one and a value the database would refuse has
// to be refused before a transaction has begun.
const maxOpaqueIDBytes = 128

// opaqueID reports whether s is a value wagering.opaque_id accepts: between one
// and 128 octets, valid UTF-8, no control characters, and refused rather than
// trimmed when it carries surrounding whitespace.
func opaqueID(s string) bool {
	if s == "" || len(s) > maxOpaqueIDBytes ||
		!utf8.ValidString(s) || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if r < asciiSpace || r == asciiDelete {
			return false
		}
	}
	return true
}

// fields is the submission as the application layer takes it, still in strings.
func (e envelope) fields() app.OperationFields {
	return app.OperationFields{
		Provider:                       e.Data.Provider,
		ExternalTransactionID:          e.Data.ExternalTransactionID,
		IdempotencyKey:                 e.Data.IdempotencyKey,
		PlayerID:                       e.Data.PlayerID,
		RoundID:                        e.Data.RoundID,
		GameID:                         e.Data.GameID,
		Kind:                           e.Data.Kind,
		Amount:                         e.Data.Money.Amount,
		Currency:                       e.Data.Money.Currency,
		ReferenceExternalTransactionID: e.Data.ReferenceExternalTransactionID,
	}
}

// bodyHash fingerprints a message body for the inbox.
//
// Lowercase hex SHA-256, which is what wagering.sha256_hex holds and what the
// domain's own payload hash produces. It is taken over the bytes as they
// arrived rather than over the parsed fields, deliberately: the inbox answers
// "has THIS message been handled", and the idempotency key already answers "has
// this operation been recorded". Hashing the parsed fields would make the two
// the same question asked twice and would leave one message id carrying two
// different bodies undetectable — which is the single condition this hash
// exists for, and the one the application layer refuses permanently when it
// finds it.
func bodyHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
