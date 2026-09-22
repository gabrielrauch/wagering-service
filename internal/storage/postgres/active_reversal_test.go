package postgres_test

import (
	"database/sql"
	"testing"
)

// The brief asks two things of a reference, and the schema keeps them apart.
//
// At most one *active* reversal: ADR-0003 settles on that, so rolling back a
// refund releases the bet it returned. active_reversal is that rule as a
// primary key (ADR-0007). And never two *successful* reversals of one kind:
// wager_transaction_one_successful_reversal_per_kind is that rule as a partial
// unique index on (resolved_reference_id, kind), which does not care whether the
// first was later undone. A released bet is therefore reversible again by a
// ROLLBACK and not by a second REFUND. Taken alone, a unique index on the
// reference would have forbidden the rollback too, which is the literal reading
// ADR-0007 rejects; taken alone, the active rule allowed the second refund.

// reversalOf is a processed reversal pointing at target.
// heldBy asserts which reversal currently holds a reference.
//
// This is the invariant ADR-0007 rests on, so it is worth having exactly one
// spelling of: reference_id is the primary key of active_reversal, and the row
// under it names the one reversal that took it.
func heldBy(t *testing.T, db *sql.DB, reference, reversal txn) {
	t.Helper()
	var holder string
	if err := db.QueryRowContext(t.Context(), `
		SELECT reversal_id FROM wagering.active_reversal WHERE reference_id = $1`,
		reference.id).Scan(&holder); err != nil {
		t.Fatalf("the %v %s is not held at all: %v", reference.kind, reference.id, err)
	}
	if holder != reversal.id {
		t.Errorf("the %v is held by %s, wanted the %v %s",
			reference.kind, holder, reversal.kind, reversal.id)
	}
}

func reversalOf(w wallet, kind string, target txn) txn {
	x := externalTx(w, kind)
	x.referenceExternalID = target.externalID
	x.resolvedReferenceID = target.id
	x.status, x.result = "PROCESSED", int64(10000)
	return x
}

// processedBet is a bet that has completed, and so can be reversed.
func processedBet(t *testing.T, db execer, w wallet) txn {
	t.Helper()
	bet := externalTx(w, "BET")
	bet.status, bet.result = "PROCESSED", int64(7500)
	bet.accepts(t, db)
	return bet
}

func TestAReferenceHasAtMostOneActiveReversal(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	bet := processedBet(t, db, w)
	reversalOf(w, "REFUND", bet).accepts(t, db)

	t.Run("a second processed reversal of the same bet", func(t *testing.T) {
		reversalOf(w, "ROLLBACK", bet).refuses(t, db, uniqueViolation)
	})

	t.Run("a second refund of the same bet", func(t *testing.T) {
		reversalOf(w, "REFUND", bet).refuses(t, db, uniqueViolation)
	})
}

// TestOnlyAProcessedReversalHoldsTheReference: a reversal that did not take
// effect must not block a legitimate retry, and a win is not a reversal at all.
func TestOnlyAProcessedReversalHoldsTheReference(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	t.Run("a rejected reversal leaves the bet reversible", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)

		rejected := reversalOf(w, "REFUND", bet)
		rejected.status, rejected.result = "REJECTED", nil
		rejected.failureCode = "REVERSAL_INSUFFICIENT_FUNDS"
		rejected.accepts(t, db)

		reversalOf(w, "REFUND", bet).accepts(t, db)
	})

	t.Run("a pending reversal leaves the bet reversible", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)

		pending := reversalOf(w, "ROLLBACK", bet)
		pending.status, pending.result = "PENDING", nil
		pending.accepts(t, db)

		reversalOf(w, "REFUND", bet).accepts(t, db)
	})

	t.Run("a win pointing at a bet does not consume its reversal slot", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)

		win := externalTx(w, "WIN")
		win.referenceExternalID, win.resolvedReferenceID = bet.externalID, bet.id
		win.status, win.result = "PROCESSED", int64(12500)
		win.accepts(t, db)

		reversalOf(w, "REFUND", bet).accepts(t, db)
	})
}

// TestReversingAReversalReleasesTheReference walks the sequence ADR-0003
// settles on, and says exactly what is released: the active slot, for a
// reversal of a different kind. A rolled-back refund frees the bet to be rolled
// back, which is the whole reason active_reversal is a derived table rather
// than a unique index on the resolved reference. It does not free the bet to
// be refunded again — that is the per-kind rule, tested below.
func TestReversingAReversalReleasesTheReference(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	bet := processedBet(t, db, w)

	refund := reversalOf(w, "REFUND", bet)
	refund.accepts(t, db)

	// Rolling back the refund: the refund stops holding the bet, and starts
	// being held itself.
	rollbackOfRefund := reversalOf(w, "ROLLBACK", refund)
	rollbackOfRefund.accepts(t, db)

	t.Run("the bet is no longer held", func(t *testing.T) {
		if got := names(t, db, `
			SELECT reversal_id FROM wagering.active_reversal WHERE reference_id = $1`, bet.id); len(got) != 0 {
			t.Errorf("the bet is still held by %v after its refund was rolled back", got)
		}
		heldBy(t, db, refund, rollbackOfRefund)
	})

	t.Run("the bet is reversible again, by a rollback", func(t *testing.T) {
		reversalOf(w, "ROLLBACK", bet).accepts(t, db)
	})

	t.Run("the refund is not reversible twice", func(t *testing.T) {
		reversalOf(w, "ROLLBACK", refund).refuses(t, db, uniqueViolation)
	})
}

// TestAReferenceReceivesAtMostOneSuccessfulReversalPerKind is the brief's
// sentence as an index: "guarantee that a reference does not receive two
// successful reversals of the same kind".
//
// The active rule alone let BET → REFUND → ROLLBACK of the refund → REFUND
// through, because the rollback released the bet and the second refund found
// the slot free — two PROCESSED refunds of one bet, each returning the stake.
// The index counts successful reversals by (reference, kind) and never releases,
// so the second refund is a duplicate key while a rollback of the same bet,
// being another kind, is not.
func TestAReferenceReceivesAtMostOneSuccessfulReversalPerKind(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	bet := processedBet(t, db, w)
	refund := reversalOf(w, "REFUND", bet)
	refund.accepts(t, db)
	reversalOf(w, "ROLLBACK", refund).accepts(t, db)

	t.Run("a second processed refund of the bet", func(t *testing.T) {
		second := reversalOf(w, "REFUND", bet)
		second.refuses(t, db, uniqueViolation)
		second.refusedBy(t, db, "wager_transaction_one_successful_reversal_per_kind")
	})

	t.Run("a processed rollback of the bet", func(t *testing.T) {
		reversalOf(w, "ROLLBACK", bet).accepts(t, db)
	})

	// The index is partial on success, so it does not count what returned
	// nothing: a refund that was refused, or one still waiting, leaves the
	// pair free for the one that eventually takes effect.
	t.Run("a rejected refund does not count", func(t *testing.T) {
		other := processedBet(t, db, w)
		rejected := reversalOf(w, "REFUND", other)
		rejected.status, rejected.result = "REJECTED", nil
		rejected.failureCode = "REVERSAL_INSUFFICIENT_FUNDS"
		rejected.accepts(t, db)

		reversalOf(w, "REFUND", other).accepts(t, db)
	})

	// And the other way a reversal reaches PROCESSED: parked, then settled by
	// an update. The index is on the row, so it sees the update too.
	t.Run("settling a parked second refund is refused the same way", func(t *testing.T) {
		other := processedBet(t, db, w)
		reversalOf(w, "REFUND", other).accepts(t, db)

		parked := reversalOf(w, "REFUND", other)
		parked.status, parked.result = "PENDING", nil
		parked.accepts(t, db)

		refusesRule(t, db, "wager_transaction_one_successful_reversal_per_kind", `
			UPDATE wagering.wager_transaction
			SET status = 'PROCESSED', result_balance_minor = $2, updated_at = $3
			WHERE id = $1`, parked.id, int64(10000), base.Add(1))
	})
}

// TestAReversalTakesTheReferenceWhenItIsProcessed covers the other way a
// reversal reaches PROCESSED: parked first, settled later by an update.
func TestAReversalTakesTheReferenceWhenItIsProcessed(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	bet := processedBet(t, db, w)

	parked := reversalOf(w, "REFUND", bet)
	parked.status, parked.result = "PENDING", nil
	parked.accepts(t, db)

	accepts(t, db, `
		UPDATE wagering.wager_transaction
		SET status = 'PROCESSED', result_balance_minor = $2, updated_at = $3
		WHERE id = $1`, parked.id, int64(10000), base.Add(1))

	reversalOf(w, "ROLLBACK", bet).refuses(t, db, uniqueViolation)
}

// TestTheActiveReversalIsQueryable checks that the derived table answers the
// question it exists for: what currently holds this transaction?
func TestTheActiveReversalIsQueryable(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	bet := processedBet(t, db, w)
	refund := reversalOf(w, "REFUND", bet)
	refund.accepts(t, db)

	heldBy(t, db, bet, refund)
}

// TestTwoReversalsCannotRaceForOneReference is the guarantee ADR-0007 rests on,
// and it has to be written as a race to show anything.
//
// ADR-0003 reasoned that two concurrent reversals of one bet are serialised by
// the wallet's version, because every reversal moves money and so both must
// write the same wallet row. That is true, and it is the weaker of the two
// guarantees. Neither reversal here writes a ledger entry or touches the
// balance, so the version never comes into it — and the reference is still held
// exactly once.
func TestTwoReversalsCannotRaceForOneReference(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	bet := processedBet(t, db, w)

	ahead, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	behind, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Both transactions are rolled back however the test ends. Cleanups run after
	// t.Context() is cancelled, which aborts whatever statement is still in flight,
	// so neither rollback waits on a lock the other holds and the order they are
	// registered in does not matter. That depends on the pair being opened against
	// t.Context(): on a background context, the blocked writer would deadlock here.
	t.Cleanup(func() { _ = ahead.Rollback() })
	t.Cleanup(func() { _ = behind.Rollback() })

	refund := reversalOf(w, "REFUND", bet)
	if _, err := ahead.ExecContext(t.Context(), insertTransaction, refund.args()...); err != nil {
		t.Fatalf("the refund was refused: %v", err)
	}

	// The rollback's trigger finds no committed hold on the bet, so it tries to
	// take one and blocks on the key the refund is already holding.
	rollback := reversalOf(w, "ROLLBACK", bet)
	refused := make(chan error, 1)
	go func() {
		_, err := behind.ExecContext(t.Context(), insertTransaction, rollback.args()...)
		refused <- err
	}()

	waitForBlockedWriter(t, db)

	if err := ahead.Commit(); err != nil {
		t.Fatalf("the refund could not commit: %v", err)
	}

	assertState(t, <-refused, uniqueViolation, "the second reversal of one bet", "")

	// The refund holds the bet, and it is the only thing that does.
	heldBy(t, db, bet, refund)
}

// TestAProcessedReversalSaysWhatItResolved is what makes active_reversal load
// bearing at all.
//
// Both maintainer triggers are gated on resolved_reference_id, and the only
// thing a reversal was forced to carry was the provider's external name.
// A REFUND could therefore reach PROCESSED with the internal reference still
// null: no trigger fired, no hold was taken, and the same bet could be returned
// again and again with REFERENCE_ALREADY_REVERSED never raised once.
func TestAProcessedReversalSaysWhatItResolved(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	t.Run("a processed refund that resolved nothing", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)

		unresolved := reversalOf(w, "REFUND", bet)
		unresolved.resolvedReferenceID = nil
		unresolved.refuses(t, db, checkViolation)
	})

	t.Run("a processed rollback that resolved nothing", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)

		unresolved := reversalOf(w, "ROLLBACK", bet)
		unresolved.resolvedReferenceID = nil
		unresolved.refuses(t, db, checkViolation)
	})

	// The bet can only be returned once, which is the whole point, and the
	// refusal has to be the one the domain reports as REFERENCE_ALREADY_REVERSED
	// rather than a row quietly slipping past the trigger.
	t.Run("a second refund of a bet is still a unique violation", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)
		reversalOf(w, "REFUND", bet).accepts(t, db)

		second := reversalOf(w, "REFUND", bet)
		second.resolvedReferenceID = nil
		second.refuses(t, db, checkViolation)
		reversalOf(w, "REFUND", bet).refuses(t, db, uniqueViolation)
	})

	// Only a settled reversal is held to it. One still waiting has not looked
	// the reference up yet, and ADR-0005 has the resolution arrive as a value.
	t.Run("a pending reversal has not resolved anything yet", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)

		waiting := reversalOf(w, "REFUND", bet)
		waiting.status, waiting.result, waiting.resolvedReferenceID = "PENDING", nil, nil
		waiting.accepts(t, db)
	})
}

// TestAReversalResolvesTheReferenceItNames ties the two halves of a reference
// together.
//
// Naming a reference and resolving one were separate facts: the CHECK asserted
// only that some external name had been given, and the foreign key only that the
// resolved id was some transaction. A refund could name one bet and take the
// hold on another — a different provider's, a different player's, a different
// wallet's — and every constraint agreed.
func TestAReversalResolvesTheReferenceItNames(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	named := processedBet(t, db, w)
	other := processedBet(t, db, w)

	crossed := reversalOf(w, "REFUND", named)
	crossed.resolvedReferenceID = other.id
	crossed.refuses(t, db, foreignKeyViolation)

	t.Run("naming and resolving the same bet is accepted", func(t *testing.T) {
		reversalOf(w, "REFUND", named).accepts(t, db)
	})
}

// TestARollbackOfABetIsPermanent is ADR-0003's asymmetry, which the maintainer
// deleted on the way past.
//
// The release was keyed on reversal_id alone, so it let go of whatever hold it
// found — including a ROLLBACK's. A rollback applied straight to a bet is
// permanent, because nothing may undo a rollback; releasing it handed back a bet
// the domain holds forever, and the bet became reversible all over again.
func TestARollbackOfABetIsPermanent(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	bet := processedBet(t, db, w)
	rolled := reversalOf(w, "ROLLBACK", bet)
	rolled.accepts(t, db)

	t.Run("rolling back the rollback is refused", func(t *testing.T) {
		undo := reversalOf(w, "ROLLBACK", rolled)
		undo.refusedBy(t, db, "active_reversal_reference_is_reversible")
	})

	t.Run("a refund cannot reverse a rollback either", func(t *testing.T) {
		undo := reversalOf(w, "REFUND", rolled)
		undo.refusedBy(t, db, "active_reversal_reference_is_reversible")
	})

	t.Run("the bet is still held afterwards", func(t *testing.T) {
		heldBy(t, db, bet, rolled)
		reversalOf(w, "REFUND", bet).refuses(t, db, uniqueViolation)
	})
}
