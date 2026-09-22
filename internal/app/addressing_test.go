package app_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// A submission names the wallet it addresses, and the wallet must belong to
// the player it names. One that does not is answered exactly as a wallet that
// does not exist — a provider cannot read wallets, so this must not become the
// place it learns whether an identifier is in use, or whose. Nothing is
// persisted and the key stays free.
func TestSubmittingAgainstAnotherPlayersWalletIsNotFoundAndPersistsNothing(t *testing.T) {
	f := newFixture(t)
	mine := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	theirs := f.db.seedWallet(t, "player-2", "100.00", "BRL")
	// The openings are transactions and entries too, so the baseline is what
	// the seeds wrote rather than nothing.
	txnsBefore := f.db.transactionCount()
	entriesBefore := len(f.db.ledgerEntries())

	misaddressed := fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"})
	misaddressed.WalletID = theirs.String()
	_, err := f.trySubmit(t, misaddressed)

	assertClass(t, err, app.NotFound)
	if strings.Contains(err.Error(), "player-2") {
		t.Errorf("the refusal names the wallet's owner: %v", err)
	}
	// The distinction is kept for a log line, in the chain and nowhere else —
	// and as a code-less sentinel, so that no failure code can be rendered.
	if !errors.Is(err, app.ErrForeignWallet) {
		t.Errorf("the chain does not carry ErrForeignWallet: %v", err)
	}
	if _, coded := failure.CodeOf(err); coded {
		t.Errorf("the refusal carries a failure code a renderer would show: %v", err)
	}

	unknown := misaddressed
	unknown.WalletID = wagering.NewWalletID().String()
	_, unknownErr := f.trySubmit(t, unknown)
	assertClass(t, unknownErr, app.NotFound)
	if sameSentence := strings.ReplaceAll(err.Error(), theirs.String(), "<id>") ==
		strings.ReplaceAll(unknownErr.Error(), unknown.WalletID, "<id>"); !sameSentence {
		t.Errorf("another player's wallet and an unknown one are told apart:\n%v\n%v", err, unknownErr)
	}

	// The wallet had to be read to learn whose it is, so a transaction was
	// opened; nothing in it was committed.
	if got := f.db.Commits(); got != 0 {
		t.Errorf("%d commits, want none", got)
	}
	if got := f.db.transactionCount(); got != txnsBefore {
		t.Errorf("%d transactions recorded, want the %d the openings wrote", got, txnsBefore)
	}
	if got := len(f.db.ledgerEntries()); got != entriesBefore {
		t.Errorf("%d ledger entries, want the %d already there", got, entriesBefore)
	}
	// The seeds publish nothing, so nothing at all is in the outbox.
	assertEvents(t, f.db)
	for _, wallet := range []wagering.WalletID{mine, theirs} {
		if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
			t.Errorf("wallet %s holds %s, want 100.00 untouched", wallet, got)
		}
	}

	// The key is still free: the corrected payload is processed under it, and
	// is not a replay of anything.
	corrected := misaddressed
	corrected.WalletID = mine.String()
	result := f.submit(t, corrected)
	assertStatus(t, result, wagering.Processed, "")
	if result.IdempotentReplay {
		t.Error("the corrected submission was answered as a replay")
	}
}

// A wallet of the right player in another currency is a real business outcome:
// the provider addressed a wallet that exists and is the player's, and the
// money is the wrong kind. It settles as a rejection, with a row, an event, and
// the key bound to that payload for good.
func TestACurrencyMismatchIsSettledAsARejection(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	txnsBefore := f.db.transactionCount()
	entriesBefore := len(f.db.ledgerEntries())

	inDollars := fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"})
	inDollars.WalletID = wallet.String()
	inDollars.Currency = "USD"

	result := f.submit(t, inDollars)

	assertStatus(t, result, wagering.Rejected, failure.CurrencyMismatch)
	if result.IdempotentReplay {
		t.Error("the first submission was answered as a replay")
	}
	if result.Balance != nil {
		t.Errorf("a rejected operation reported a balance of %s", result.Balance.Amount())
	}
	if result.WalletID != wallet {
		t.Errorf("the result names wallet %s, want %s", result.WalletID, wallet)
	}

	// One row, one event, and a wallet that did not move.
	if got := f.db.transactionCount(); got != txnsBefore+1 {
		t.Errorf("%d transactions recorded, want %d", got, txnsBefore+1)
	}
	assertEvents(t, f.db, "WagerTransactionRejected")
	if got := len(f.db.ledgerEntries()); got != entriesBefore {
		t.Errorf("%d ledger entries, want the %d already there", got, entriesBefore)
	}
	if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
		t.Errorf("wallet holds %s, want 100.00 untouched", got)
	}
	if got := f.db.walletVersion(t, wallet); got != 1 {
		t.Errorf("wallet version %d, want 1 untouched", got)
	}

	// The key is spent: the same submission again is the same rejection,
	// replayed, and nothing more is written.
	again := f.submit(t, inDollars)
	assertStatus(t, again, wagering.Rejected, failure.CurrencyMismatch)
	if !again.IdempotentReplay {
		t.Error("the resubmission was not answered as a replay")
	}
	if again.TransactionID != result.TransactionID {
		t.Errorf("the replay names transaction %s, want %s", again.TransactionID, result.TransactionID)
	}
	if got := f.db.transactionCount(); got != txnsBefore+1 {
		t.Errorf("%d transactions after the replay, want still %d", got, txnsBefore+1)
	}
	assertEvents(t, f.db, "WagerTransactionRejected")
}

// The wallet is a required member of the contract, and one that is not a UUID
// is malformed. Both are refused before anything is read — no wallet is seeded,
// because none is needed for the refusal and the store staying empty is the
// proof that nothing was reached.
func TestAWalletThatIsNotAnIdentifierIsInvalid(t *testing.T) {
	for _, tc := range []struct {
		name   string
		wallet string
		want   failure.Code
	}{
		{"absent", "", failure.MissingRequiredField},
		{"not a UUID", "wallet-1", failure.InvalidFieldFormat},
		{"the nil UUID", "00000000-0000-0000-0000-000000000000", failure.MissingRequiredField},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)

			of := fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"})
			of.WalletID = tc.wallet
			_, err := f.wagers.Submit(t.Context(), app.SubmitOperation{
				Principal:   providerPrincipal(t, acme),
				Correlation: "corr-ext-1",
				Fields:      of,
			})

			assertClass(t, err, app.Invalid)
			assertCode(t, err, tc.want)
			assertField(t, err, "walletId")
			f.assertUntouched(t)
		})
	}
}

// A result names the wallet it was applied to and the provider that submitted
// it, so a transport can report both without reading the row again. An opening
// has no provider, and says so with the zero value.
func TestAResultNamesItsWalletAndProvider(t *testing.T) {
	f := newFixture(t)
	view, opening, err := f.open(t, "player-1", "100.00", "BRL")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if opening == nil {
		t.Fatal("a wallet opened with money in it records an opening")
	}
	if opening.WalletID != view.ID || opening.ProviderID != "" {
		t.Errorf("the opening names wallet %s and provider %q, want %s and none",
			opening.WalletID, opening.ProviderID, view.ID)
	}

	result := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
	if result.WalletID != view.ID || result.ProviderID != acme {
		t.Errorf("the bet names wallet %s and provider %q, want %s and %q",
			result.WalletID, result.ProviderID, view.ID, acme)
	}

	replay := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
	if replay.WalletID != view.ID || replay.ProviderID != acme {
		t.Errorf("the replay names wallet %s and provider %q, want %s and %q",
			replay.WalletID, replay.ProviderID, view.ID, acme)
	}
}

// A reconciliation says how many ledger entries it summed — the opening and
// every movement since — so a reader can tell a wallet found consistent over
// its whole ledger from one found consistent over an empty one.
func TestReconciliationCountsTheEntriesItSummed(t *testing.T) {
	t.Run("the opening and every movement", func(t *testing.T) {
		f := newFixture(t)
		wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
		f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
		f.submit(t, fields(acme, submission{Kind: "WIN", External: "ext-2", Key: "key-2", Amount: "10.00"}))
		// A loss moves nothing and writes no entry, so it is not counted.
		f.submit(t, fields(acme, submission{Kind: "LOSS", External: "ext-3", Key: "key-3", Amount: "0.00"}))

		report, err := f.wallets.Reconcile(t.Context(), servicePrincipal(t), wallet)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if !report.Consistent {
			t.Errorf("reported a divergence of %s, want consistent", report.Difference.Amount())
		}
		if report.CheckedEntries != 3 {
			t.Errorf("checked %d entries, want 3: the opening, the bet and the win", report.CheckedEntries)
		}
	})

	t.Run("a wallet opened at zero has nothing to sum", func(t *testing.T) {
		f := newFixture(t)
		wallet := f.db.seedWallet(t, "player-1", "0.00", "BRL")

		report, err := f.wallets.Reconcile(t.Context(), servicePrincipal(t), wallet)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if !report.Consistent || report.CheckedEntries != 0 {
			t.Errorf("report = consistent %v over %d entries, want consistent over 0",
				report.Consistent, report.CheckedEntries)
		}
	})

	t.Run("a divergence still says what it summed", func(t *testing.T) {
		f := newFixture(t)
		wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
		f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
		f.db.corruptBalance(t, wallet, "70.00", "BRL")

		report, err := f.wallets.Reconcile(t.Context(), servicePrincipal(t), wallet)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if report.Consistent {
			t.Fatal("want a divergence")
		}
		if report.CheckedEntries != 2 {
			t.Errorf("checked %d entries, want 2", report.CheckedEntries)
		}
	})
}
