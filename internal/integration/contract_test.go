//go:build integration

package integration

import (
	"net/http"
	"strings"
	"testing"
)

// The specification's own body shape — providerId and walletId among the
// members — is what the door accepts, and the wallet it names is the wallet
// the operation lands in.
func TestTheSpecificationsSubmissionBodyIsAcceptedAndProcessed(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	wallet := s.openWallet(t, "player-spec-body", "100.00")

	got := s.do(t, call{
		method: http.MethodPost,
		path:   "/wagering/transactions",
		body: encode(t, map[string]any{
			"providerId":            providerA,
			"externalTransactionId": "transaction-spec-body",
			"playerId":              wallet.PlayerID,
			"walletId":              wallet.WalletID,
			"roundId":               "round-spec-body",
			"gameId":                "fortune-chimp",
			"kind":                  "BET",
			"money":                 amount{Amount: "25.00", Currency: currency},
		}),
		token:          tokenFor(t, providerA),
		idempotencyKey: providerA + ":transaction-spec-body",
	})
	applied := operationOf(t, got)
	if applied.Status != "PROCESSED" || applied.IdempotentReplay {
		t.Fatalf("the specification's body came to %s (replay=%v), wanted PROCESSED", applied.Status, applied.IdempotentReplay)
	}
	if applied.Balance == nil || applied.Balance.Amount != "75.00" {
		t.Errorf("the balance reported is %v, wanted 75.00", applied.Balance)
	}

	read := s.do(t, call{
		method: http.MethodGet,
		path:   "/wallets/" + wallet.WalletID,
		token:  tokenFor(t, walletService),
	})
	var view walletView
	decode(t, read, &view)
	if view.Balance.Amount != "75.00" || view.Version != 2 {
		t.Errorf("the wallet named in the body holds %s at version %d, wanted 75.00 at version 2",
			view.Balance.Amount, view.Version)
	}
}

// A body naming a wallet that belongs to another player is answered exactly as
// one naming a wallet that does not exist: 404, the same sentence, the owner
// never named — and not one row anywhere. The wallet was read to learn whose
// it is, and nothing else happened.
func TestABodyNamingAnotherPlayersWalletIsRefusedAndWritesNothing(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	mine := s.openWallet(t, "player-addressing-mine", "100.00")
	theirs := s.openWallet(t, "player-addressing-theirs", "100.00")
	before := s.snapshot(t)

	misaddressed := bet(providerA, "ext-misaddressed", mine, "10.00")
	misaddressed.WalletID = theirs.WalletID
	got := s.submit(t, providerA, misaddressed, "key-misaddressed")

	body := refused(t, got, http.StatusNotFound, "NOT_FOUND")
	if contains(body.Message, "player-addressing-theirs") {
		t.Errorf("the refusal names the wallet's owner: %q", body.Message)
	}

	// The same sentence a wallet nobody holds gets, so that neither answer
	// says whether an identifier is in use.
	absent := bet(providerA, "ext-absent", mine, "10.00")
	absent.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
	absentBody := refused(t, s.submit(t, providerA, absent, "key-absent"), http.StatusNotFound, "NOT_FOUND")
	if strings.ReplaceAll(body.Message, theirs.WalletID, "<id>") !=
		strings.ReplaceAll(absentBody.Message, absent.WalletID, "<id>") {
		t.Errorf("another player's wallet and an absent one are told apart:\n%q\n%q",
			body.Message, absentBody.Message)
	}
	unchanged(t, before, s.snapshot(t), "a submission naming another player's wallet")

	// The key is still free: the corrected body is processed under it.
	corrected := operationOf(t, s.submit(t, providerA,
		bet(providerA, "ext-misaddressed", mine, "10.00"), "key-misaddressed"))
	if corrected.Status != "PROCESSED" || corrected.IdempotentReplay {
		t.Errorf("the corrected submission came to %s (replay=%v), wanted PROCESSED and not a replay",
			corrected.Status, corrected.IdempotentReplay)
	}
}

// A body in a currency the named wallet does not hold is a business outcome:
// 422 with CURRENCY_MISMATCH, one transaction recorded, one event, and a ledger
// that did not move.
func TestABodyInTheWrongCurrencyIsRejectedWithOneRow(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	wallet := s.openWallet(t, "player-currency", "100.00")
	before := s.snapshot(t)

	inDollars := bet(providerA, "ext-in-dollars", wallet, "10.00")
	inDollars.Money.Currency = "USD"
	got := s.submit(t, providerA, inDollars, "key-in-dollars")

	if got.status != http.StatusUnprocessableEntity {
		t.Fatalf("wanted 422 and a rejected operation, got %s", got)
	}
	var op operation
	decode(t, got, &op)
	if op.Status != "REJECTED" || op.FailureCode != "CURRENCY_MISMATCH" {
		t.Fatalf("the operation is %s (%s), wanted REJECTED with CURRENCY_MISMATCH", op.Status, op.FailureCode)
	}
	if op.Balance != nil {
		t.Errorf("a rejected operation reported a balance of %+v", op.Balance)
	}

	after := s.snapshot(t)
	if after["wager_transaction"].rows != before["wager_transaction"].rows+1 {
		t.Errorf("wager_transaction went from %d to %d rows, wanted exactly one more",
			before["wager_transaction"].rows, after["wager_transaction"].rows)
	}
	if after["outbox"].rows != before["outbox"].rows+1 {
		t.Errorf("outbox went from %d to %d rows, wanted exactly the rejection's event",
			before["outbox"].rows, after["outbox"].rows)
	}
	for _, table := range []string{"wallet", "wallet_ledger_entry"} {
		if before[table] != after[table] {
			t.Errorf("a rejection changed wagering.%s: %d rows %s became %d rows %s", table,
				before[table].rows, before[table].digest, after[table].rows, after[table].digest)
		}
	}

	// The key is bound to that payload: the same submission is the same
	// rejection, replayed, and writes nothing more.
	again := s.submit(t, providerA, inDollars, "key-in-dollars")
	var replay operation
	decode(t, again, &replay)
	if again.status != http.StatusUnprocessableEntity || !replay.IdempotentReplay ||
		replay.FailureCode != "CURRENCY_MISMATCH" || replay.TransactionID != op.TransactionID {
		t.Errorf("resending was answered %s, wanted the same CURRENCY_MISMATCH rejection as a replay of %s",
			again, op.TransactionID)
	}
	unchanged(t, after, s.snapshot(t), "replaying the rejection")
}

// contains reports whether s mentions sub, for a message assertion that
// should not depend on the whole sentence.
func contains(s, sub string) bool { return strings.Contains(s, sub) }
