package app_test

import (
	"slices"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

func (f *fixture) open(t *testing.T, player, amount, currency string) (app.WalletView, *app.OperationResult, error) {
	t.Helper()
	return f.wallets.Open(t.Context(), app.OpenWalletCommand{
		Principal:     servicePrincipal(t),
		Correlation:   "corr-open",
		PlayerID:      player,
		InitialAmount: amount,
		Currency:      currency,
	})
}

// The opening is part of creating the wallet, not a change to one that already
// existed — so the wallet is at version 1 with its opening entry already in the
// ledger, and all of it lands in one commit.
func TestOpeningAWalletWithMoneyRecordsTheOpening(t *testing.T) {
	f := newFixture(t)

	view, opening, err := f.open(t, "player-1", "100.00", "BRL")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if view.Balance.Amount() != "100.00" {
		t.Errorf("wallet holds %s, want 100.00", view.Balance.Amount())
	}
	if view.Version != 1 {
		t.Errorf("version %d, want 1", view.Version)
	}
	if opening == nil {
		t.Fatal("a wallet opened with money reports its opening")
	}
	if opening.Kind != wagering.Opening || opening.Status != wagering.Processed {
		t.Errorf("opening is %s/%s, want OPENING/PROCESSED", opening.Kind, opening.Status)
	}
	if opening.ExternalTransactionID != "" {
		t.Errorf("opening carries external id %q, want none: it came from us", opening.ExternalTransactionID)
	}

	entries := f.db.ledgerEntries()
	if len(entries) != 1 || entries[0].Direction != wagering.Credit {
		t.Fatalf("ledger %+v, want one credit", entries)
	}
	if entries[0].BalanceBefore.Amount() != "0.00" || entries[0].BalanceAfter.Amount() != "100.00" {
		t.Errorf("opening entry moved %s -> %s, want 0.00 -> 100.00",
			entries[0].BalanceBefore.Amount(), entries[0].BalanceAfter.Amount())
	}

	assertEvents(t, f.db, "WagerTransactionProcessed", "WalletBalanceChanged")
	if f.db.Commits() != 1 {
		t.Errorf("%d commits, want 1 — the wallet, the opening, its entry and its events are one fact", f.db.Commits())
	}
}

// An opening records a starting balance, and a wallet opened at zero has none.
// The absence is reported by returning no result rather than an empty one a
// caller would have to know to ignore.
func TestOpeningAWalletAtZeroRecordsNothingElse(t *testing.T) {
	f := newFixture(t)

	view, opening, err := f.open(t, "player-1", "0.00", "BRL")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if opening != nil {
		t.Errorf("reported an opening %+v, want none", opening)
	}
	if view.Version != 1 {
		t.Errorf("version %d, want 1 — a wallet opened at zero still has a version", view.Version)
	}
	if view.Balance.Amount() != "0.00" {
		t.Errorf("wallet holds %s, want 0.00", view.Balance.Amount())
	}
	if got := f.db.transactionCount(); got != 0 {
		t.Errorf("%d transactions, want none", got)
	}
	if got := len(f.db.ledgerEntries()); got != 0 {
		t.Errorf("%d ledger entries, want none", got)
	}
	if got := len(f.db.envelopes()); got != 0 {
		t.Errorf("published %d events, want none", got)
	}
}

func TestASecondWalletForOnePlayerAndCurrencyIsAConflict(t *testing.T) {
	f := newFixture(t)
	if _, _, err := f.open(t, "player-1", "100.00", "BRL"); err != nil {
		t.Fatalf("open: %v", err)
	}

	_, _, err := f.open(t, "player-1", "50.00", "BRL")

	assertClass(t, err, app.Conflict)
	assertCode(t, err, failure.WalletAlreadyExists)
}

// A player may hold wallets in several currencies: it is the pair that is unique.
func TestAPlayerMayHoldOneWalletPerCurrency(t *testing.T) {
	f := newFixture(t)
	if _, _, err := f.open(t, "player-1", "100.00", "BRL"); err != nil {
		t.Fatalf("open BRL: %v", err)
	}

	if _, _, err := f.open(t, "player-1", "100.00", "USD"); err != nil {
		t.Errorf("open USD: %v", err)
	}
}

// A bet for a player with no wallet in that currency is NotFound, and nothing is
// persisted — so the same submission succeeds under the same key once the wallet
// exists. This layer never opens a wallet on a provider's behalf.
func TestSubmittingAgainstAnUnknownWalletIsNotFoundAndPersistsNothing(t *testing.T) {
	f := newFixture(t)

	_, err := f.trySubmit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	assertClass(t, err, app.NotFound)
	if got := f.db.transactionCount(); got != 0 {
		t.Fatalf("%d transactions, want none", got)
	}

	// The key is still free.
	if _, _, err := f.open(t, "player-1", "100.00", "BRL"); err != nil {
		t.Fatalf("open: %v", err)
	}
	result := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
	if result.Status != wagering.Processed {
		t.Errorf("status %s, want %s", result.Status, wagering.Processed)
	}
}

func TestLedgerPagesInOrderAndItsCursorRoundTrips(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "10.00"}))
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-2", Key: "key-2", Amount: "20.00"}))
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-3", Key: "key-3", Amount: "30.00"}))

	var seen []uint64
	query := app.LedgerQuery{WalletID: wallet, Limit: 2}
	for {
		page, err := f.wallets.Ledger(t.Context(), servicePrincipal(t), query)
		if err != nil {
			t.Fatalf("ledger: %v", err)
		}
		for _, e := range page.Entries {
			seen = append(seen, e.WalletVersion())
		}
		if page.NextCursor == "" {
			break
		}
		query.Cursor = page.NextCursor
	}

	// The opening plus three bets, in version order, each exactly once.
	want := []uint64{1, 2, 3, 4}
	if !slices.Equal(seen, want) {
		t.Fatalf("paged versions %v, want %v", seen, want)
	}
}

// A cursor names its wallet, so one from another wallet is refused rather than
// silently returning a page of somebody else's ledger.
func TestACursorFromAnotherWalletIsRefused(t *testing.T) {
	f := newFixture(t)
	mine := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	theirs := f.db.seedWallet(t, "player-2", "100.00", "BRL")

	page, err := f.wallets.Ledger(t.Context(), servicePrincipal(t), app.LedgerQuery{WalletID: theirs, Limit: 1})
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}

	_, err = f.wallets.Ledger(t.Context(), servicePrincipal(t), app.LedgerQuery{
		WalletID: mine, Cursor: page.NextCursor, Limit: 1,
	})

	assertClass(t, err, app.Invalid)
	assertField(t, err, "cursor")
}

// Unauthorized would confirm the operation exists. A provider asking about
// somebody else's transaction is told there is none.
func TestAProviderReadingAnotherProvidersOperationIsToldThereIsNone(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	theirs := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	_, err := f.wagers.TransactionByID(t.Context(), providerPrincipal(t, rival), theirs.TransactionID)

	assertClass(t, err, app.NotFound)
}

func TestAProviderReadsItsOwnOperations(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	mine := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	byID, err := f.wagers.TransactionByID(t.Context(), providerPrincipal(t, acme), mine.TransactionID)
	if err != nil {
		t.Fatalf("by id: %v", err)
	}
	if byID.TransactionID != mine.TransactionID {
		t.Errorf("read %s, want %s", byID.TransactionID, mine.TransactionID)
	}

	byExternal, err := f.wagers.TransactionByExternalID(t.Context(), providerPrincipal(t, acme), "ext-1")
	if err != nil {
		t.Fatalf("by external id: %v", err)
	}
	if byExternal.TransactionID != mine.TransactionID {
		t.Errorf("read %s, want %s", byExternal.TransactionID, mine.TransactionID)
	}
	if byExternal.Balance == nil || byExternal.Balance.Amount() != "75.00" {
		t.Errorf("read balance %v, want 75.00", byExternal.Balance)
	}
}

func TestReconciliationOfAConsistentWalletReportsNoDivergence(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	report, err := f.wallets.Reconcile(t.Context(), servicePrincipal(t), wallet)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if !report.Consistent {
		t.Errorf("reported a divergence of %s, want consistent", report.Difference.Amount())
	}
	if !report.Difference.IsZero() {
		t.Errorf("difference %s, want zero", report.Difference.Amount())
	}
	if len(f.observer.divergences) != 0 {
		t.Errorf("called the hook %d times, want none", len(f.observer.divergences))
	}
}

// Stored less reconstructed, and signed: which way a wallet is out is the first
// thing an operator asks. This is the one amount in the system that may be
// negative.
func TestReconciliationReportsTheSignedDifferenceAndCallsTheHook(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	entriesBefore := len(f.db.ledgerEntries())
	f.db.corruptBalance(t, wallet, "75.00", "BRL")

	report, err := f.wallets.Reconcile(t.Context(), servicePrincipal(t), wallet)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if report.Consistent {
		t.Fatal("want a divergence")
	}
	if got := report.Stored.Amount(); got != "75.00" {
		t.Errorf("stored %s, want 75.00", got)
	}
	if got := report.Reconstructed.Amount(); got != "100.00" {
		t.Errorf("reconstructed %s, want 100.00", got)
	}
	if got := report.Difference.Amount(); got != "-25.00" {
		t.Errorf("difference %s, want -25.00", got)
	}
	if !report.Difference.IsNegative() {
		t.Error("the wallet holds less than its ledger says: the difference is negative")
	}

	if len(f.observer.divergences) != 1 || f.observer.divergences[0].WalletID != wallet {
		t.Errorf("hook calls %+v, want exactly one for %s", f.observer.divergences, wallet)
	}

	// Reconciliation reports and returns. It never corrects.
	if got := f.db.balanceOf(t, wallet).Amount(); got != "75.00" {
		t.Errorf("wallet holds %s, want the 75.00 it was found holding", got)
	}
	if got := len(f.db.ledgerEntries()); got != entriesBefore {
		t.Errorf("%d ledger entries, want the %d already there", got, entriesBefore)
	}
	if f.db.Commits() != 0 {
		t.Errorf("%d commits, want none — reconciliation reads", f.db.Commits())
	}
}

func TestReconcilingAnUnknownWalletIsNotFound(t *testing.T) {
	f := newFixture(t)

	_, err := f.wallets.Reconcile(t.Context(), servicePrincipal(t), wagering.NewWalletID())

	assertClass(t, err, app.NotFound)
}
