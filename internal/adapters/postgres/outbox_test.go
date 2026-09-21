// The outbox's trace carriage, which needs no database and so carries no build
// tag.
//
// It runs in the fast suite deliberately. What it pins is that a rendered
// envelope is not rewritten on its way into the row — the one property that
// makes "the published body is exactly the envelope" true — and that is a
// property of bytes rather than of PostgreSQL.

package postgres

import (
	"bytes"
	"encoding/json"
	"maps"
	"testing"
)

// anEnvelope is a rendered envelope of the shape app.Envelope.MarshalJSON
// produces: one object, camelCase members, money as a string.
const anEnvelope = `{"eventId":"0199c0de-0000-7000-8000-000000000001",` +
	`"eventType":"WagerTransactionProcessed","aggregateType":"WALLET",` +
	`"aggregateId":"0199c0de-0000-7000-8000-000000000002",` +
	`"correlationId":"thread-1","causationId":"message-1",` +
	`"occurredAt":"2026-09-21T09:00:00Z","version":1,` +
	`"data":{"money":{"amount":"25.00","currency":"BRL"}}}`

// TestTheTraceRidesInThePayloadAndTheEnvelopeIsNotRewritten pins the mechanism
// the whole cross-process trace rests on, and the promise it must not break.
//
// The outbox row is the only durable thing the publisher reads, so it is the
// only place the trace can cross from the command to the publication. What it
// must not cost is the published contract: the claim strips the member again,
// so removing it has to give back the envelope EXACTLY — not an equivalent
// document, the same bytes — or the body on the queue is something this service
// rendered twice.
func TestTheTraceRidesInThePayloadAndTheEnvelopeIsNotRewritten(t *testing.T) {
	t.Parallel()

	carried := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "wagering=1",
	}
	stored, err := withTrace([]byte(anEnvelope), carried)
	if err != nil {
		t.Fatalf("carry the trace: %v", err)
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(stored, &members); err != nil {
		t.Fatalf("the stored payload is not a JSON object: %v\n%s", err, stored)
	}

	// The member is there and says what it was given.
	raw, present := members[traceMember]
	if !present {
		t.Fatalf("the stored payload carries no %s member: %s", traceMember, stored)
	}
	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("the %s member is not an object of strings: %v", traceMember, err)
	}
	if !maps.Equal(got, carried) {
		t.Errorf("the stored trace is %v, wanted %v", got, carried)
	}

	// And removing it again — which is what the claim's `payload - '$trace'`
	// does — gives back the envelope, member for member and value for value.
	delete(members, traceMember)
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(anEnvelope), &envelope); err != nil {
		t.Fatalf("the fixture is not a JSON object: %v", err)
	}
	if len(members) != len(envelope) {
		t.Fatalf("the stored payload has %d members without the trace, the envelope has %d",
			len(members), len(envelope))
	}
	for name, want := range envelope {
		if string(members[name]) != string(want) {
			t.Errorf("%s is %s in the row and %s in the envelope", name, members[name], want)
		}
	}
}

// TestAPayloadWithNothingToCarryIsStoredExactlyAsItWasRendered pins the
// behaviour of a process with telemetry switched off.
//
// Byte for byte, not merely equivalent. A service that is not tracing must
// store what it stored before any of this existed, so that switching telemetry
// on and off cannot change what is in the database.
func TestAPayloadWithNothingToCarryIsStoredExactlyAsItWasRendered(t *testing.T) {
	t.Parallel()

	for _, carried := range []map[string]string{nil, {}} {
		stored, err := withTrace([]byte(anEnvelope), carried)
		if err != nil {
			t.Fatalf("carry nothing: %v", err)
		}
		if string(stored) != anEnvelope {
			t.Errorf("a payload with nothing to carry was rewritten:\n got %s\nwant %s",
				stored, anEnvelope)
		}
	}
}

// TestTheTraceIsWrittenLastSoTheLiveValueWins pins the collision policy.
//
// jsonb resolves a duplicate key to the LAST one — verified against PostgreSQL
// 16: `'{"$trace":"A","$trace":"B"}'::jsonb ->> '$trace'` is "B". So the member
// written last is the member that survives, and the one that should survive is
// the one this process just injected rather than something that arrived inside
// a document. That is the same rule besideTheBody applies to the message
// attributes, and two collision policies that contradicted each other would be
// the kind of thing that stops being unreachable.
//
// It is unreachable today — app.Envelope.MarshalJSON marshals a fixed struct of
// camelCase tags — which is exactly why the position needs a test rather than a
// comment: nothing else in this tree would notice it moving.
func TestTheTraceIsWrittenLastSoTheLiveValueWins(t *testing.T) {
	t.Parallel()

	const live = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	carried := map[string]string{"traceparent": live}

	// A document that already carries the member, which today's envelope never
	// does. What matters is which of the two a jsonb column would keep.
	envelope := `{"eventId":"e-1","` + traceMember + `":{"traceparent":"STALE"},"version":1}`
	stored, err := withTrace([]byte(envelope), carried)
	if err != nil {
		t.Fatalf("carry the trace: %v", err)
	}

	first := bytes.Index(stored, []byte(`"`+traceMember+`"`))
	last := bytes.LastIndex(stored, []byte(`"`+traceMember+`"`))
	if first == last {
		t.Fatalf("the fixture did not produce two members to choose between: %s", stored)
	}
	if !bytes.Contains(stored[last:], []byte(live)) {
		t.Errorf("the LAST %s member is not the live one, so jsonb would keep the stale "+
			"one:\n%s", traceMember, stored)
	}
	if bytes.Contains(stored[last:], []byte("STALE")) {
		t.Errorf("the live trace was written before the stale one:\n%s", stored)
	}
}

// TestAnEmptyObjectStillBecomesOne pins the separator that is not always there.
//
// An envelope is never empty, so this case is unreachable — which is precisely
// why the missing guard would have been found by nothing: the splice would have
// produced `{"$trace":{...},}`, which is not JSON, and the only symptom would
// be outbox_payload_is_an_object refusing the LAST statement of a command that
// had otherwise moved money.
func TestAnEmptyObjectStillBecomesOne(t *testing.T) {
	t.Parallel()

	carried := map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	stored, err := withTrace([]byte("{}"), carried)
	if err != nil {
		t.Fatalf("carry the trace: %v", err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(stored, &members); err != nil {
		t.Fatalf("the stored payload is not JSON: %v\n%s", err, stored)
	}
	if len(members) != 1 {
		t.Errorf("the stored payload has %d members, wanted only the trace: %s",
			len(members), stored)
	}
}

// TestSomethingThatIsNotAnObjectIsRefusedBeforeItReachesTheRow pins the
// assertion rather than the condition.
//
// It is unreachable — app.Envelope.MarshalJSON writes an object of eight
// members — and the alternative to asserting it is a payload column that fails
// outbox_payload_is_an_object at the LAST statement of a command that had
// otherwise succeeded, which rolls back a wallet movement for a telemetry
// field.
func TestSomethingThatIsNotAnObjectIsRefusedBeforeItReachesTheRow(t *testing.T) {
	t.Parallel()

	carried := map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	for _, payload := range []string{"", "[]", `"an envelope"`, "null"} {
		if _, err := withTrace([]byte(payload), carried); err == nil {
			t.Errorf("%q was accepted as a rendered envelope", payload)
		}
	}
}
