package workers

import (
	"strings"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

func TestParsingAnEnvelope(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		// why names what the refusal has to be about, so a test that passes
		// because something else went wrong is a test that fails.
		why string
	}{
		{
			name: "a well-formed envelope is accepted",
			body: validBody(t, nil),
		},
		{
			name: "a body that is not JSON is refused",
			body: []byte("not json at all"),
			why:  "not a JSON envelope",
		},
		{
			name: "a truncated body is refused",
			body: []byte(`{"messageId":"m-1","type":`),
			why:  "not a JSON envelope",
		},
		{
			name: "a JSON array is refused",
			body: []byte(`[{"messageId":"m-1"}]`),
			why:  "not a JSON envelope",
		},
		{
			// A second document after the first is refused rather than
			// discarded. Nothing in the loop would apply it, so a decoder that
			// accepted it would be accepting an operation nobody runs.
			name: "a second envelope after the first is refused",
			body: append(validBody(t, nil), validBody(t, nil)...),
			why:  "not a JSON envelope",
		},
		{
			name: "trailing rubbish is refused",
			body: append(validBody(t, nil), []byte(" nonsense")...),
			why:  "not a JSON envelope",
		},
		{
			name: "a trailing array is refused",
			body: append(validBody(t, nil), []byte(`[1,2]`)...),
			why:  "not a JSON envelope",
		},
		{
			name: "an unknown type is refused by name",
			body: validBody(t, map[string]any{"type": "WagerTransactionCancelled"}),
			why:  "is not a message type this consumer handles",
		},
		{
			// The spelling this consumer accepted before the specification was
			// in the tree. A producer still sending it reaches the dead-letter
			// queue with the expected type in the reason.
			name: "the superseded type spelling is refused by name",
			body: validBody(t, map[string]any{"type": "WagerTransactionSubmitted"}),
			why:  `expected "WagerTransactionRequested"`,
		},
		{
			// Likewise the superseded member name for the provider: strict
			// decoding refuses it as a member the envelope does not have,
			// rather than quietly reading no provider at all.
			name: "the superseded provider member is refused",
			body: validBody(t, map[string]any{"data": validData(map[string]any{"provider": "acme"})}),
			why:  "a field this envelope does not have",
		},
		{
			name: "an absent walletId is refused",
			body: validBody(t, map[string]any{"data": validData(map[string]any{"walletId": ""})}),
			why:  "carries no walletId",
		},
		{
			name: "an absent type is refused",
			body: validBody(t, map[string]any{"type": ""}),
			why:  "carries no type",
		},
		{
			name: "a member this envelope does not have is refused",
			body: validBody(t, map[string]any{"priority": "high"}),
			why:  "a field this envelope does not have",
		},
		{
			name: "an unknown member inside data is refused",
			body: validBody(t, map[string]any{"data": validData(map[string]any{"tip": "1.00"})}),
			why:  "a field this envelope does not have",
		},
		{
			name: "a member named twice is refused",
			body: []byte(`{"messageId":"m-1","messageId":"m-2","type":"` + MessageType +
				`","occurredAt":"2026-09-21T09:59:00Z","data":{}}`),
			why: "names the same field twice",
		},
		{
			name: "an absent messageId is refused",
			body: validBody(t, map[string]any{"messageId": ""}),
			why:  "carries no messageId",
		},
		{
			name: "a messageId the inbox could not store is refused",
			body: validBody(t, map[string]any{"messageId": strings.Repeat("m", 129)}),
			why:  "not storable",
		},
		{
			name: "a messageId carrying a control character is refused",
			body: validBody(t, map[string]any{"messageId": "m-1\nmessage"}),
			why:  "not storable",
		},
		{
			name: "a messageId with surrounding whitespace is refused",
			body: validBody(t, map[string]any{"messageId": " m-1 "}),
			why:  "not storable",
		},
		{
			name: "an absent occurredAt is refused",
			body: validBody(t, map[string]any{"occurredAt": nil}),
			why:  "carries no occurredAt",
		},
		{
			name: "an occurredAt that is not a timestamp is refused",
			body: validBody(t, map[string]any{"occurredAt": "yesterday"}),
			why:  "not a JSON envelope",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e, err := parseEnvelope(c.body)
			if c.why == "" {
				if err != nil {
					t.Fatalf("parse a well-formed envelope: %v", err)
				}
				if e.MessageID != "message-1" {
					t.Errorf("messageId = %q, want %q", e.MessageID, "message-1")
				}
				if e.Type != MessageType {
					t.Errorf("type = %q, want %q", e.Type, MessageType)
				}
				if e.OccurredAt.IsZero() {
					t.Error("occurredAt was not read")
				}
				return
			}
			if err == nil {
				t.Fatalf("parsed a body that should have been refused for %q", c.why)
			}
			if !strings.Contains(err.Error(), c.why) {
				t.Errorf("refusal = %q, want it to say %q", err, c.why)
			}
		})
	}
}

// The business fields reach the application layer exactly as they arrived. A
// transport that repaired one would be handing the idempotency hash something
// no provider ever sent.
func TestTheEnvelopeCarriesTheBusinessFieldsThrough(t *testing.T) {
	body := validBody(t, map[string]any{
		"data": validData(map[string]any{
			"kind":                           "ROLLBACK",
			"referenceExternalTransactionId": "external-0",
			"money":                          map[string]any{"amount": "10.50", "currency": "BRL"},
		}),
	})
	e, err := parseEnvelope(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	fields := e.fields()
	for _, want := range []struct {
		name, got, want string
	}{
		{"providerId", fields.Provider, "acme"},
		{"externalTransactionId", fields.ExternalTransactionID, "external-1"},
		{"idempotencyKey", fields.IdempotencyKey, "key-1"},
		{"playerId", fields.PlayerID, "player-1"},
		{"walletId", fields.WalletID, "0192f291-27dd-7d3f-8071-5f8685deef37"},
		{"roundId", fields.RoundID, "round-1"},
		{"gameId", fields.GameID, "game-1"},
		{"kind", fields.Kind, "ROLLBACK"},
		{"amount", fields.Amount, "10.50"},
		{"currency", fields.Currency, "BRL"},
		{"reference", fields.ReferenceExternalTransactionID, "external-0"},
	} {
		if want.got != want.want {
			t.Errorf("%s = %q, want %q", want.name, want.got, want.want)
		}
	}
}

// The specification's own example message, verbatim: its type, its member
// names, and an occurredAt carrying milliseconds. It parses, and every business
// field reaches the application layer as it was written.
func TestTheSpecificationsEnvelopeIsAccepted(t *testing.T) {
	const body = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`
	e, err := parseEnvelope([]byte(body))
	if err != nil {
		t.Fatalf("the specification's envelope was refused: %v", err)
	}
	if e.MessageID != "msg-123" || e.Type != MessageType {
		t.Errorf("read %q of type %q, want msg-123 of type %s", e.MessageID, e.Type, MessageType)
	}
	if want := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC); !e.OccurredAt.Equal(want) {
		t.Errorf("occurredAt = %s, want %s", e.OccurredAt, want)
	}
	want := app.OperationFields{
		Provider:              "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Amount:                "25.00",
		Currency:              "BRL",
	}
	if got := e.fields(); got != want {
		t.Errorf("the application layer would be given\n%+v\nwant\n%+v", got, want)
	}
}

// The inbox hash is of the bytes as they arrived, which is what makes one
// message id carrying two different bodies detectable at all.
func TestTheBodyHashIsOfTheBytesThatArrived(t *testing.T) {
	spaced := []byte(`{"a": 1}`)
	tight := []byte(`{"a":1}`)

	if bodyHash(spaced) == bodyHash(tight) {
		t.Error("two different bodies hashed the same")
	}
	if bodyHash(spaced) != bodyHash([]byte(`{"a": 1}`)) {
		t.Error("the same bytes hashed differently")
	}
	if got := len(bodyHash(spaced)); got != 64 {
		t.Errorf("hash length = %d, want 64 so that wagering.sha256_hex will hold it", got)
	}
	for _, r := range bodyHash(spaced) {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("hash %q is not lowercase hex", bodyHash(spaced))
		}
	}
}
