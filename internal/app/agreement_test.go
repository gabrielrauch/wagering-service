package app_test

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestALossInAnotherCurrencyIsSettledAsACurrencyMismatch is the currency rule
// on the one kind that moves no money.
//
// A loss carries 0.00 and touches no balance, so a currency check that lived
// in the movement — where a bet's would naturally be — would let a loss in
// dollars complete against a wallet in reais. The rule runs before anything is
// applied, for every kind, and the outcome is the same as a bet's: a rejection
// with a row and an event, the key bound to that payload, and nothing else
// written.
func TestALossInAnotherCurrencyIsSettledAsACurrencyMismatch(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	txnsBefore := f.db.transactionCount()
	entriesBefore := len(f.db.ledgerEntries())
	versionBefore := f.db.walletVersion(t, wallet)

	// The wallet is named rather than looked up: the fixture resolves a wallet
	// by player and currency, and there is no USD wallet to resolve to. What
	// is submitted is a loss in dollars against the player's wallet in reais.
	loss := fields(acme, submission{Kind: "LOSS", External: "ext-loss", Key: "key-loss", Amount: "0.00"})
	loss.WalletID = wallet.String()
	loss.Currency = "USD"

	result := f.submit(t, loss)

	assertStatus(t, result, wagering.Rejected, failure.CurrencyMismatch)
	if result.IdempotentReplay {
		t.Error("the first submission was answered as a replay")
	}
	if result.Balance != nil {
		t.Errorf("a rejected loss reported a balance of %s", result.Balance.Amount())
	}
	if result.WalletID != wallet {
		t.Errorf("the result names wallet %s, want %s", result.WalletID, wallet)
	}

	// One row, and it is the loss as it was submitted: rejected, in dollars.
	rows := f.db.wagerTransactions()
	if got := len(rows); got != txnsBefore+1 {
		t.Fatalf("%d transactions recorded, want %d", got, txnsBefore+1)
	}
	var stored *wagering.TransactionSnapshot
	for i := range rows {
		if rows[i].ID == result.TransactionID {
			stored = &rows[i]
		}
	}
	if stored == nil {
		t.Fatalf("no row for the rejected loss %s", result.TransactionID)
	}
	if stored.Kind != wagering.Loss || stored.Status != wagering.Rejected ||
		stored.FailureCode != failure.CurrencyMismatch {
		t.Errorf("stored %s %s (%s), want a %s %s under %s", stored.Kind, stored.Status,
			stored.FailureCode, wagering.Loss, wagering.Rejected, failure.CurrencyMismatch)
	}
	if got := stored.Money.Currency().String(); got != "USD" {
		t.Errorf("the row is in %s, want the USD that was submitted", got)
	}

	// One event, no ledger entry, and a wallet that did not move.
	assertEvents(t, f.db, "WagerTransactionRejected")
	if got := len(f.db.ledgerEntries()); got != entriesBefore {
		t.Errorf("%d ledger entries, want the %d already there", got, entriesBefore)
	}
	if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
		t.Errorf("wallet holds %s, want 100.00 untouched", got)
	}
	if got := f.db.walletVersion(t, wallet); got != versionBefore {
		t.Errorf("wallet version %d, want %d untouched", got, versionBefore)
	}
}

// TestAReversalOnAnotherWalletOfTheSamePlayerIsAMismatch pins which wallet a
// reversal must name.
//
// A player may hold one wallet per currency, and a reversal names the wallet it
// addresses exactly as the bet did. A refund that names the player's OTHER
// wallet — the right player, a wallet that exists and is theirs, money in that
// wallet's own currency — passes every check that runs before the reference
// is consulted, and is then a REFERENCE_MISMATCH: the bet it names was not
// made on that wallet. It is settled as one, on the wallet the provider named,
// and the bet stays refundable on its own.
func TestAReversalOnAnotherWalletOfTheSamePlayerIsAMismatch(t *testing.T) {
	f := newFixture(t)
	reais := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	dollars := f.db.seedWallet(t, "player-1", "100.00", "USD")

	staked := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))
	assertStatus(t, staked, wagering.Processed, "")
	if staked.WalletID != reais {
		t.Fatalf("the bet landed on wallet %s, want the BRL wallet %s", staked.WalletID, reais)
	}

	refund := fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-2",
		Amount:   "25.00", Reference: "ext-bet",
	})
	refund.WalletID = dollars.String()
	refund.Currency = "USD"

	result := f.submit(t, refund)

	assertStatus(t, result, wagering.Rejected, failure.ReferenceMismatch)
	if result.WalletID != dollars {
		t.Errorf("the rejection is recorded on wallet %s, want the USD wallet %s the provider "+
			"named", result.WalletID, dollars)
	}
	if result.Balance != nil {
		t.Errorf("a rejected refund reported a balance of %s", result.Balance.Amount())
	}
	for _, row := range f.db.wagerTransactions() {
		if row.ID == result.TransactionID && !row.External.ResolvedReferenceID.IsZero() {
			t.Errorf("the mismatched refund resolved to %s", row.External.ResolvedReferenceID)
		}
	}

	// Neither wallet moved for it: the stake is still out of the BRL wallet and
	// the USD wallet is where it was opened.
	if got := f.db.balanceOf(t, reais).Amount(); got != "75.00" {
		t.Errorf("the BRL wallet holds %s, want 75.00", got)
	}
	if got := f.db.balanceOf(t, dollars).Amount(); got != "100.00" {
		t.Errorf("the USD wallet holds %s, want 100.00 untouched", got)
	}
	if got := f.db.walletVersion(t, dollars); got != 1 {
		t.Errorf("the USD wallet is at version %d, want 1 untouched", got)
	}
	assertEvents(t, f.db, "WagerTransactionProcessed", "WalletBalanceChanged", "WagerTransactionRejected")

	// The mismatch held nothing: the same bet is refunded on its own wallet.
	returned := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund-2",
		Key:      "key-3",
		Amount:   "25.00", Reference: "ext-bet",
	}))
	assertStatus(t, returned, wagering.Processed, "")
	if returned.WalletID != reais {
		t.Errorf("the refund landed on wallet %s, want the BRL wallet %s", returned.WalletID, reais)
	}
	if got := f.db.balanceOf(t, reais).Amount(); got != "100.00" {
		t.Errorf("the BRL wallet holds %s, want 100.00", got)
	}
}
