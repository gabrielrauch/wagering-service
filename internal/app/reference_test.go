package app_test

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

func TestWinCreditsTheWallet(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")

	result := f.submit(t, fields(acme, submission{Kind: "WIN", External: "ext-win", Key: "key-1", Amount: "50.00"}))

	assertStatus(t, result, wagering.Processed, "")
	if got := f.db.balanceOf(t, wallet).Amount(); got != "150.00" {
		t.Errorf("wallet holds %s, want 150.00", got)
	}
}

// A win may name the bet it pays out on. Naming one that has not arrived is not
// an error — the queue does not promise order, so the operation waits.
func TestWinNamingAnUnknownBetWaits(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")

	result := f.submit(t, fields(acme, submission{
		Kind:     "WIN",
		External: "ext-win",
		Key:      "key-1",
		Amount:   "50.00", Reference: "ext-bet",
	}))

	if result.Status != wagering.PendingReference {
		t.Fatalf("status %s, want %s", result.Status, wagering.PendingReference)
	}
	if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
		t.Errorf("wallet holds %s, want 100.00 — a parked operation moves nothing", got)
	}
	assertEvents(t, f.db, "WagerTransactionPendingReference")
}

func TestRefundReturnsTheFullStake(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))

	result := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-2",
		Amount:   "25.00", Reference: "ext-bet",
	}))

	assertStatus(t, result, wagering.Processed, "")
	if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
		t.Errorf("wallet holds %s, want 100.00", got)
	}
}

// A rollback has no direction of its own: it takes the opposite of whatever it
// undoes.
func TestRollbackMovesTheOppositeWayToWhatItUndoes(t *testing.T) {
	t.Run("of a bet, credits", func(t *testing.T) {
		f := newFixture(t)
		wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
		f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))

		f.submit(t, fields(acme, submission{
			Kind:     "ROLLBACK",
			External: "ext-rb",
			Key:      "key-2",
			Amount:   "25.00", Reference: "ext-bet",
		}))

		if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
			t.Errorf("wallet holds %s, want 100.00", got)
		}
	})

	t.Run("of a win, debits", func(t *testing.T) {
		f := newFixture(t)
		wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
		f.submit(t, fields(acme, submission{Kind: "WIN", External: "ext-win", Key: "key-1", Amount: "50.00"}))

		f.submit(t, fields(acme, submission{
			Kind:     "ROLLBACK",
			External: "ext-rb",
			Key:      "key-2",
			Amount:   "50.00", Reference: "ext-win",
		}))

		if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
			t.Errorf("wallet holds %s, want 100.00", got)
		}
	})

	t.Run("of a refund, debits", func(t *testing.T) {
		f := newFixture(t)
		wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
		f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))
		f.submit(t, fields(acme, submission{
			Kind:     "REFUND",
			External: "ext-refund",
			Key:      "key-2",
			Amount:   "25.00", Reference: "ext-bet",
		}))

		f.submit(t, fields(acme, submission{
			Kind:     "ROLLBACK",
			External: "ext-rb",
			Key:      "key-3",
			Amount:   "25.00", Reference: "ext-refund",
		}))

		if got := f.db.balanceOf(t, wallet).Amount(); got != "75.00" {
			t.Errorf("wallet holds %s, want 75.00", got)
		}
	})
}

// A player betting beyond their balance is routine. A reversal that can no
// longer be applied means money has already left the wallet, which is an
// operator's problem — so the two carry different codes.
func TestAReversalThatWouldOverdrawHasItsOwnCode(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "WIN", External: "ext-win", Key: "key-1", Amount: "50.00"}))
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-2", Amount: "150.00"}))

	result := f.submit(t, fields(acme, submission{
		Kind:     "ROLLBACK",
		External: "ext-rb",
		Key:      "key-3",
		Amount:   "50.00", Reference: "ext-win",
	}))

	assertStatus(t, result, wagering.Rejected, failure.ReversalInsufficientFunds)
	if result.FailureCode == failure.InsufficientFunds {
		t.Error("a reversal that cannot be applied is not a player betting too much")
	}
}

func TestReferenceThatIsStillWaitingKeepsTheReversalWaiting(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	// A refund whose own reference never arrived: parked.
	f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-1",
		Amount:   "25.00", Reference: "never-arrived",
	}))

	result := f.submit(t, fields(acme, submission{
		Kind:     "ROLLBACK",
		External: "ext-rb",
		Key:      "key-2",
		Amount:   "25.00", Reference: "ext-refund",
	}))

	if result.Status != wagering.PendingReference {
		t.Errorf("status %s, want %s — a reference that has not settled is one to wait for",
			result.Status, wagering.PendingReference)
	}
}

// A reference that ended unsuccessfully is settled, so there is nothing to wait
// for and nothing to reverse. Waiting would burn the whole budget on an answer
// that is already known.
func TestReferenceThatWasRejectedIsRefusedImmediately(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "10.00", "BRL")
	rejected := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))
	if rejected.Status != wagering.Rejected {
		t.Fatalf("fixture: bet is %s, want %s", rejected.Status, wagering.Rejected)
	}

	result := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-2",
		Amount:   "25.00", Reference: "ext-bet",
	}))

	assertStatus(t, result, wagering.Rejected, failure.ReferenceNotProcessed)
}

func TestReferenceInAnotherRoundIsAMismatch(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))

	result := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-2",
		Amount:   "25.00", Round: "another-round", Reference: "ext-bet",
	}))

	assertStatus(t, result, wagering.Rejected, failure.ReferenceMismatch)
}

// Partial reversals do not exist.
func TestAReversalOfADifferentAmountIsRefused(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))

	result := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-2",
		Amount:   "30.00", Reference: "ext-bet",
	}))

	assertStatus(t, result, wagering.Rejected, failure.ReversalAmountMismatch)
}

// Decided by the domain under the wallet lock, and persisted as a rejection with
// its event — not raised by the primary key, which is only the backstop.
func TestASecondReversalOfOneReferenceIsRejectedAndPersisted(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))
	f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-2",
		Amount:   "25.00", Reference: "ext-bet",
	}))

	result := f.submit(t, fields(acme, submission{
		Kind:     "ROLLBACK",
		External: "ext-rb",
		Key:      "key-3",
		Amount:   "25.00", Reference: "ext-bet",
	}))

	assertStatus(t, result, wagering.Rejected, failure.ReferenceAlreadyReversed)
	if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
		t.Errorf("wallet holds %s, want 100.00 — the money was not returned twice", got)
	}
	if got := f.db.eventTypes(); got[len(got)-1] != "WagerTransactionRejected" {
		t.Errorf("last event %s, want WagerTransactionRejected", got[len(got)-1])
	}
}

// ADR-0003, end to end: at most one ACTIVE reversal, so undoing a refund gives
// the bet back and the bet can be reversed again.
func TestRollingBackARefundReleasesTheBetItReturned(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")

	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))
	f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-2",
		Amount:   "25.00", Reference: "ext-bet",
	}))
	f.submit(t, fields(acme, submission{
		Kind:     "ROLLBACK",
		External: "ext-rb",
		Key:      "key-3",
		Amount:   "25.00", Reference: "ext-refund",
	}))

	// The refund is undone, so the bet stands again — and is reversible again.
	if got := f.db.balanceOf(t, wallet).Amount(); got != "75.00" {
		t.Fatalf("wallet holds %s, want 75.00", got)
	}

	fourth := f.submit(t, fields(acme, submission{
		Kind:     "ROLLBACK",
		External: "ext-rb2",
		Key:      "key-4",
		Amount:   "25.00", Reference: "ext-bet",
	}))

	assertStatus(t, fourth, wagering.Processed, "")
	if got := f.db.balanceOf(t, wallet).Amount(); got != "100.00" {
		t.Errorf("wallet holds %s, want 100.00", got)
	}
}

// The asymmetry is deliberate: a rollback is a provider saying an operation
// never happened, and nothing undoes that.
func TestARollbackStraightToABetHoldsItPermanently(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-bet", Key: "key-1", Amount: "25.00"}))
	f.submit(t, fields(acme, submission{
		Kind:     "ROLLBACK",
		External: "ext-rb",
		Key:      "key-2",
		Amount:   "25.00", Reference: "ext-bet",
	}))

	result := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-refund",
		Key:      "key-3",
		Amount:   "25.00", Reference: "ext-bet",
	}))

	assertStatus(t, result, wagering.Rejected, failure.ReferenceAlreadyReversed)
}
