package app_test

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

const consumer = "wager-consumer"

func (f *fixture) fromQueue(
	t *testing.T,
	of app.OperationFields,
	messageID, bodyHash string,
) (app.OperationResult, error) {
	t.Helper()
	return f.wagers.Submit(t.Context(), app.SubmitOperation{
		Principal:   providerPrincipal(t, acme),
		Correlation: "corr-" + messageID,
		Inbox:       &app.InboxMessage{Consumer: consumer, MessageID: messageID, BodyHash: bodyHash},
		Fields:      of,
	})
}

// The inbox row and the domain changes commit together, so there is no second
// write marking the message complete and no window in which it is handled but
// not recorded. A row that exists is a message that was handled.
func TestAQueueMessageIsRecordedInTheSameCommitAsItsWork(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	result, err := f.fromQueue(t, fields(acme, submission{
		Kind:     "BET",
		External: "ext-1",
		Key:      "key-1",
		Amount:   "25.00",
	}), "msg-1", "body-hash-1")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if result.Status != wagering.Processed {
		t.Fatalf("status %s, want %s", result.Status, wagering.Processed)
	}

	records := f.db.inboxRecords()
	if len(records) != 1 {
		t.Fatalf("%d inbox records, want 1", len(records))
	}
	if records[0].MessageID != "msg-1" || records[0].BodyHash != "body-hash-1" {
		t.Errorf("recorded %+v, want msg-1 / body-hash-1", records[0])
	}
	if records[0].CompletedAt.IsZero() {
		t.Error("the row is written already completed: there is no second write to make it so")
	}
	if f.db.Commits() != 1 {
		t.Errorf("%d commits, want 1", f.db.Commits())
	}
}

// A redelivery returns what the first delivery produced, and does not do the work
// again.
func TestARedeliveredMessageReturnsTheStoredResult(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	first, err := f.fromQueue(t, fields(acme, submission{
		Kind:     "BET",
		External: "ext-1",
		Key:      "key-1",
		Amount:   "25.00",
	}), "msg-1", "body-hash-1")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	again, err := f.fromQueue(t, fields(acme, submission{
		Kind:     "BET",
		External: "ext-1",
		Key:      "key-1",
		Amount:   "25.00",
	}), "msg-1", "body-hash-1")
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	if !again.IdempotentReplay {
		t.Error("a redelivery is a replay")
	}
	if again.TransactionID != first.TransactionID {
		t.Error("a redelivery names the original transaction")
	}
	if got := f.db.balanceOf(t, wallet).Amount(); got != "75.00" {
		t.Errorf("wallet holds %s, want 75.00 — the bet was applied twice", got)
	}
	if got := len(f.db.inboxRecords()); got != 1 {
		t.Errorf("%d inbox records, want 1", got)
	}
}

// One message id, two bodies. Nothing can decide which is real, and sending it
// again will not change that — so it is permanent, and the queue must not keep
// redelivering it.
func TestAMessageRedeliveredWithADifferentBodyIsPermanent(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	if _, err := f.fromQueue(t, fields(acme, submission{
		Kind:     "BET",
		External: "ext-1",
		Key:      "key-1",
		Amount:   "25.00",
	}), "msg-1", "body-hash-1"); err != nil {
		t.Fatalf("submit: %v", err)
	}

	_, err := f.fromQueue(t, fields(acme, submission{
		Kind:     "BET",
		External: "ext-2",
		Key:      "key-2",
		Amount:   "25.00",
	}), "msg-1", "body-hash-DIFFERENT")

	assertClass(t, err, app.Unretryable)
}

// Parking is a complete handling of the message: the worker owns the
// continuation, and the queue has nothing left to do.
func TestAParkedOperationStillCompletesItsMessage(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	result, err := f.fromQueue(t,
		fields(acme, submission{
			Kind:     "REFUND",
			External: "ext-1",
			Key:      "key-1",
			Amount:   "25.00", Reference: "never-arrived",
		}),
		"msg-1", "body-hash-1")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if result.Status != wagering.PendingReference {
		t.Fatalf("status %s, want %s", result.Status, wagering.PendingReference)
	}
	records := f.db.inboxRecords()
	if len(records) != 1 || records[0].CompletedAt.IsZero() {
		t.Errorf("inbox records %+v, want one, completed", records)
	}
}

// On the queue path the message is what caused the operation, and it has an
// identity worth recording.
func TestQueueEnvelopesNameTheMessageThatCausedThem(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	if _, err := f.fromQueue(t, fields(acme, submission{
		Kind:     "BET",
		External: "ext-1",
		Key:      "key-1",
		Amount:   "25.00",
	}), "msg-1", "body-hash-1"); err != nil {
		t.Fatalf("submit: %v", err)
	}

	for _, e := range f.db.envelopes() {
		if e.CausationID != "msg-1" {
			t.Errorf("causation %q, want msg-1", e.CausationID)
		}
	}
}
