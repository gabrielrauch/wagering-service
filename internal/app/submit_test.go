package app_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

func TestBetOnAFundedWalletProcesses(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")

	result := f.submit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
	}))

	if result.Status != wagering.Processed {
		t.Errorf("status %s, want %s", result.Status, wagering.Processed)
	}
	if result.IdempotentReplay {
		t.Error("first submission reported as a replay")
	}
	if result.Balance == nil {
		t.Fatal("a processed operation reports the balance it produced")
	}
	if got := result.Balance.Amount(); got != "75.00" {
		t.Errorf("reported balance %s, want 75.00", got)
	}

	if got := f.db.balanceOf(t, wallet).Amount(); got != "75.00" {
		t.Errorf("wallet holds %s, want 75.00", got)
	}
	// The opening took version 1, so the bet takes 2.
	if got := f.db.walletVersion(t, wallet); got != 2 {
		t.Errorf("wallet version %d, want 2", got)
	}

	// One entry beyond the opening, recording the debit.
	entries := f.db.ledgerEntries()
	if len(entries) != 2 {
		t.Fatalf("%d ledger entries, want 2", len(entries))
	}
	debit := entries[1]
	if debit.Direction != wagering.Debit {
		t.Errorf("entry direction %s, want %s", debit.Direction, wagering.Debit)
	}
	if got := debit.Amount.Amount(); got != "25.00" {
		t.Errorf("entry amount %s, want 25.00", got)
	}
	if debit.BalanceBefore.Amount() != "100.00" || debit.BalanceAfter.Amount() != "75.00" {
		t.Errorf("entry moved %s -> %s, want 100.00 -> 75.00",
			debit.BalanceBefore.Amount(), debit.BalanceAfter.Amount())
	}
	if debit.WalletVersion != 2 {
		t.Errorf("entry records version %d, want 2", debit.WalletVersion)
	}
}

// The processed operation is announced before the balance change it caused. The
// outbox keeps insertion order within a transaction, so this order is what a
// consumer sees.
func TestAProcessedMovementPublishesProcessedThenBalanceChanged(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	f.submit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
	}))

	assertEvents(t, f.db, "WagerTransactionProcessed", "WalletBalanceChanged")
}

func TestEveryEnvelopeIsAddressedAndTraceable(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")

	if _, err := f.wagers.Submit(t.Context(), app.SubmitOperation{
		Principal:   providerPrincipal(t, acme),
		Correlation: "corr-abc",
		Fields:      fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	seen := map[string]bool{}
	for _, e := range f.db.envelopes() {
		if e.EventID.IsZero() {
			t.Error("envelope has no event id")
		}
		if seen[e.EventID.String()] {
			t.Errorf("event id %s used twice", e.EventID)
		}
		seen[e.EventID.String()] = true

		if e.CorrelationID != "corr-abc" {
			t.Errorf("correlation %q, want corr-abc", e.CorrelationID)
		}
		// HTTP has no causing event to name.
		if e.CausationID != "" {
			t.Errorf("causation %q, want none on the HTTP path", e.CausationID)
		}
		if e.EventVersion != 1 {
			t.Errorf("event version %d, want 1", e.EventVersion)
		}
		if e.AggregateType != "WALLET" || e.AggregateID != wallet {
			t.Errorf("addressed to %s %s, want WALLET %s", e.AggregateType, e.AggregateID, wallet)
		}
		if e.OccurredAt.Location() != time.UTC {
			t.Errorf("occurredAt is %s, want UTC", e.OccurredAt.Location())
		}
	}
}

// Money crosses this boundary as a decimal string, never as a number: a
// consumer parsing 25.00 as a float has already lost the guarantee the minor
// units exist to give.
func TestMoneyIsPublishedAsADecimalString(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	if _, err := f.wagers.Submit(t.Context(), app.SubmitOperation{
		Principal:   providerPrincipal(t, acme),
		Correlation: "corr-1",
		Fields:      fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	raw, err := json.Marshal(f.db.envelopes()[0])
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	body := string(raw)

	for _, want := range []string{
		`"money":{"amount":"25.00","currency":"BRL"}`,
		`"balanceAfter":{"amount":"75.00","currency":"BRL"}`,
		`"eventType":"WagerTransactionProcessed"`,
		`"correlationId":"corr-1"`,
		`"version":1`,
		`"externalTransactionId":"ext-1"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("published payload is missing %s\ngot %s", want, body)
		}
	}
	// RFC 3339, UTC.
	if !strings.Contains(body, `"occurredAt":"2026-09-19T12:00:00Z"`) {
		t.Errorf("occurredAt is not RFC 3339 UTC: %s", body)
	}
}

// A rejection is a business fact, not an error: it is persisted, it emits an
// event, and it binds the idempotency key to its payload for good.
func TestBetWithoutBalanceIsRejectedAndPersisted(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "10.00", "BRL")

	result := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	assertStatus(t, result, wagering.Rejected, failure.InsufficientFunds)
	if result.Balance != nil {
		t.Error("a rejection reports no balance: nothing was produced")
	}

	rows := f.db.wagerTransactions()
	if len(rows) != 2 {
		t.Fatalf("%d transactions, want the opening and the rejected bet", len(rows))
	}
	if got := rows[1].Status; got != wagering.Rejected {
		t.Errorf("stored status %s, want %s", got, wagering.Rejected)
	}

	if got := f.db.balanceOf(t, wallet).Amount(); got != "10.00" {
		t.Errorf("wallet holds %s, want 10.00 — a rejection moves nothing", got)
	}
	if got := f.db.walletVersion(t, wallet); got != 1 {
		t.Errorf("wallet version %d, want 1", got)
	}
	if got := len(f.db.ledgerEntries()); got != 1 {
		t.Errorf("%d ledger entries, want only the opening", got)
	}

	assertEvents(t, f.db, "WagerTransactionRejected")
}

// A malformed submission never becomes a transaction, so there is nothing to
// persist and nothing to publish — and no transaction is even opened.
func TestMalformedSubmissionsAreInvalidAndWriteNothing(t *testing.T) {
	cases := map[string]app.OperationFields{
		"a bet of nothing": fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "0.00"}),
		"a loss that moves money": fields(acme, submission{
			Kind:     "LOSS",
			External: "ext-1",
			Key:      "key-1",
			Amount:   "25.00",
		}),
		"an unknown kind": fields(acme, submission{
			Kind:     "SIDEBET",
			External: "ext-1",
			Key:      "key-1",
			Amount:   "25.00",
		}),
		"an opening from a provider": fields(acme, submission{
			Kind:     "OPENING",
			External: "ext-1",
			Key:      "key-1",
			Amount:   "25.00",
		}),
		"an amount with one decimal": fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.0"}),
		"a refund naming nothing": fields(acme, submission{
			Kind:     "REFUND",
			External: "ext-1",
			Key:      "key-1",
			Amount:   "25.00",
		}),
	}

	for name, of := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			// A funded wallet, so that nothing but the payload can be the
			// reason the submission is refused.
			f.db.seedWallet(t, "player-1", "100.00", "BRL")
			before := f.db.transactionCount()

			_, err := f.trySubmit(t, of)

			assertClass(t, err, app.Invalid)
			if got := f.db.Begins(); got != 0 {
				t.Errorf("opened %d transactions, want none: the submission never had to reach one", got)
			}
			if got := f.db.transactionCount(); got != before {
				t.Errorf("%d transactions recorded, want the %d already there", got, before)
			}
		})
	}
}

// A loss completes without moving money: no entry, no balance change, no version
// change — and still an event, because the round ending with no payout is
// exactly what a subscriber is waiting to hear.
func TestLossProcessesWithoutMovingAnything(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	before := f.db.walletVersion(t, wallet)

	result := f.submit(t, fields(acme, submission{Kind: "LOSS", External: "ext-1", Key: "key-1", Amount: "0.00"}))

	assertStatus(t, result, wagering.Processed, "")
	if result.Balance == nil || result.Balance.Amount() != "100.00" {
		t.Errorf("reported balance %v, want 100.00", result.Balance)
	}
	if got := f.db.walletVersion(t, wallet); got != before {
		t.Errorf("wallet version moved to %d, want %d", got, before)
	}
	if got := len(f.db.ledgerEntries()); got != 1 {
		t.Errorf("%d ledger entries, want only the opening", got)
	}
	assertEvents(t, f.db, "WagerTransactionProcessed")
}
