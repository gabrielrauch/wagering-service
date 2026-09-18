package wagering

import (
	"strings"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// processedOf applies a command that is expected to succeed and returns the
// transaction, ready to be used as a reference.
func processedOf(t *testing.T, p *Processor, w *Wallet, cmd Command, ref *ReferenceView) *WagerTransaction {
	t.Helper()
	return mustSubmit(t, p, w, cmd, ref, baseTime).Transaction
}

func TestBet(t *testing.T) {
	t.Parallel()

	t.Run("debits the wallet", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		out := mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime)

		assertBalance(t, w, "75.00")
		assertVersion(t, w, 2)
		if out.LedgerEntry == nil {
			t.Fatal("a bet produced no ledger entry")
		}
		if out.LedgerEntry.Direction() != Debit {
			t.Errorf("direction = %s, want %s", out.LedgerEntry.Direction(), Debit)
		}
		assertEventTypes(t, out, typeWagerTransactionProcessed, typeWalletBalanceChanged)

		result, ok := out.Transaction.Result()
		if !ok || result.Amount() != "75.00" {
			t.Errorf("reported balance = %v, %v, want 75.00", result, ok)
		}
	})

	t.Run("is rejected without sufficient balance", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "10.00")
		out := submitOK(t, p, w, command(t, Bet, "25.00"), nil, baseTime)

		assertRejected(t, out, failure.InsufficientFunds)
		assertBalance(t, w, "10.00")
		assertVersion(t, w, 1)
	})

	t.Run("may spend the balance exactly", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "25.00")
		mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime)
		assertBalance(t, w, "0.00")
	})
}

func TestWin(t *testing.T) {
	t.Parallel()

	t.Run("credits the wallet", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		out := mustSubmit(t, p, w, command(t, Win, "40.00"), nil, baseTime)

		assertBalance(t, w, "140.00")
		assertVersion(t, w, 2)
		if out.LedgerEntry.Direction() != Credit {
			t.Errorf("direction = %s, want %s", out.LedgerEntry.Direction(), Credit)
		}
		assertEventTypes(t, out, typeWagerTransactionProcessed, typeWalletBalanceChanged)
	})

	t.Run("may name the bet it pays out on", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)

		win := command(t, Win, "40.00", withReference(externalID(t, bet)))
		mustSubmit(t, p, w, win, referenceTo(bet), baseTime)
		assertBalance(t, w, "115.00")
	})

	t.Run("need not match the stake it pays out on", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)

		// A win is not a reversal, so the amounts are unrelated.
		win := command(t, Win, "500.00", withReference(externalID(t, bet)))
		mustSubmit(t, p, w, win, referenceTo(bet), baseTime)
		assertBalance(t, w, "575.00")
	})

	// A win does not hold the bet it names, so the bet stays reversible and
	// several wins may point at the same one.
	t.Run("does not consume the bet's reversal slot", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
		betID := externalID(t, bet)

		firstWin := processedOf(t, p, w, command(t, Win, "10.00", withReference(betID)), referenceTo(bet))
		secondWin := command(t, Win, "10.00", withReference(betID))
		mustSubmit(t, p, w, secondWin, referenceTo(bet), baseTime)

		// The bet can still be refunded, with the two wins in view.
		refund := command(t, Refund, "25.00", withReference(betID))
		out := submitOK(t, p, w, refund, referenceWith(bet,
			ReversalView{Transaction: firstWin}), baseTime)
		assertStatus(t, out, Processed)
	})

	t.Run("must name a bet, not another kind", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		otherWin := processedOf(t, p, w, command(t, Win, "10.00"), nil)

		win := command(t, Win, "40.00", withReference(externalID(t, otherWin)))
		out := submitOK(t, p, w, win, referenceTo(otherWin), baseTime)
		assertRejected(t, out, failure.ReferenceMismatch)
	})
}

// TestLoss covers the one kind that completes without moving money.
func TestLoss(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "100.00")
	out := mustSubmit(t, p, w, command(t, Loss, "0.00"), nil, baseTime)

	assertBalance(t, w, "100.00")
	// No money moved, so the version does not advance.
	assertVersion(t, w, 1)
	if out.LedgerEntry != nil {
		t.Error("a loss produced a ledger entry")
	}
	// A loss is still a completed operation, so it is reported as processed —
	// but nothing changed, so no balance event is emitted.
	assertEventTypes(t, out, typeWagerTransactionProcessed)

	result, ok := out.Transaction.Result()
	if !ok || result.Amount() != "100.00" {
		t.Errorf("reported balance = %v, %v, want the unchanged 100.00", result, ok)
	}
}

func TestRefund(t *testing.T) {
	t.Parallel()

	t.Run("returns a processed bet in full", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
		assertBalance(t, w, "75.00")

		refund := command(t, Refund, "25.00", withReference(externalID(t, bet)))
		out := mustSubmit(t, p, w, refund, referenceTo(bet), baseTime)

		assertBalance(t, w, "100.00")
		if out.LedgerEntry.Direction() != Credit {
			t.Errorf("direction = %s, want %s", out.LedgerEntry.Direction(), Credit)
		}
		if got, ok := out.Transaction.ResolvedReferenceID(); !ok || got != bet.ID() {
			t.Errorf("resolved reference = %v, %v, want %s", got, ok, bet.ID())
		}
	})

	t.Run("refuses a partial return", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)

		refund := command(t, Refund, "10.00", withReference(externalID(t, bet)))
		out := submitOK(t, p, w, refund, referenceTo(bet), baseTime)
		assertRejected(t, out, failure.ReversalAmountMismatch)
		assertBalance(t, w, "75.00")
	})

	t.Run("may only return a bet", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		win := processedOf(t, p, w, command(t, Win, "40.00"), nil)

		refund := command(t, Refund, "40.00", withReference(externalID(t, win)))
		out := submitOK(t, p, w, refund, referenceTo(win), baseTime)
		assertRejected(t, out, failure.ReferenceNotReversible)
	})
}

func TestRollback(t *testing.T) {
	t.Parallel()

	t.Run("undoes a bet by crediting", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)

		rollback := command(t, Rollback, "25.00", withReference(externalID(t, bet)))
		out := mustSubmit(t, p, w, rollback, referenceTo(bet), baseTime)

		assertBalance(t, w, "100.00")
		if out.LedgerEntry.Direction() != Credit {
			t.Errorf("direction = %s, want %s", out.LedgerEntry.Direction(), Credit)
		}
	})

	t.Run("undoes a win by debiting", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		win := processedOf(t, p, w, command(t, Win, "40.00"), nil)
		assertBalance(t, w, "140.00")

		rollback := command(t, Rollback, "40.00", withReference(externalID(t, win)))
		out := mustSubmit(t, p, w, rollback, referenceTo(win), baseTime)

		assertBalance(t, w, "100.00")
		if out.LedgerEntry.Direction() != Debit {
			t.Errorf("direction = %s, want %s", out.LedgerEntry.Direction(), Debit)
		}
	})

	t.Run("undoes a refund by debiting", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
		refund := processedOf(t, p, w,
			command(t, Refund, "25.00", withReference(externalID(t, bet))), referenceTo(bet))
		assertBalance(t, w, "100.00")

		rollback := command(t, Rollback, "25.00", withReference(externalID(t, refund)))
		out := mustSubmit(t, p, w, rollback, referenceTo(refund), baseTime)

		// The refund is undone, so the bet stands debited again.
		assertBalance(t, w, "75.00")
		if out.LedgerEntry.Direction() != Debit {
			t.Errorf("direction = %s, want %s", out.LedgerEntry.Direction(), Debit)
		}
	})

	t.Run("cannot undo another rollback", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
		first := processedOf(t, p, w,
			command(t, Rollback, "25.00", withReference(externalID(t, bet))), referenceTo(bet))

		second := command(t, Rollback, "25.00", withReference(externalID(t, first)))
		out := submitOK(t, p, w, second, referenceTo(first), baseTime)
		assertRejected(t, out, failure.ReferenceNotReversible)
	})

	t.Run("cannot undo a loss", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		loss := processedOf(t, p, w, command(t, Loss, "0.00"), nil)

		rollback := command(t, Rollback, "25.00", withReference(externalID(t, loss)))
		out := submitOK(t, p, w, rollback, referenceTo(loss), baseTime)
		assertRejected(t, out, failure.ReferenceNotReversible)
	})

	// A reversal that cannot be applied means money has already left the wallet.
	// It gets its own code so it is not mistaken for a player betting too much.
	t.Run("is rejected under its own code when the balance cannot cover it", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "0.00")
		win := processedOf(t, p, w, command(t, Win, "40.00"), nil)
		mustSubmit(t, p, w, command(t, Bet, "40.00"), nil, baseTime)
		assertBalance(t, w, "0.00")

		rollback := command(t, Rollback, "40.00", withReference(externalID(t, win)))
		out := submitOK(t, p, w, rollback, referenceTo(win), baseTime)

		assertRejected(t, out, failure.ReversalInsufficientFunds)
		if failure.ReversalInsufficientFunds == failure.InsufficientFunds {
			t.Error("a reversal that cannot be applied shares a code with an ordinary overdraw")
		}
		assertBalance(t, w, "0.00")
	})
}

// TestReversalExclusivity covers the rule that stops the same debit being
// returned twice, and the chain that deliberately releases it again.
func TestReversalExclusivity(t *testing.T) {
	t.Parallel()

	t.Run("a bet holding an active refund cannot also be rolled back", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
		refund := processedOf(t, p, w,
			command(t, Refund, "25.00", withReference(externalID(t, bet))), referenceTo(bet))
		assertBalance(t, w, "100.00")

		rollback := command(t, Rollback, "25.00", withReference(externalID(t, bet)))
		out := submitOK(t, p, w, rollback,
			referenceWith(bet, ReversalView{Transaction: refund}), baseTime)

		assertRejected(t, out, failure.ReferenceAlreadyReversed)
		// The stake was returned once and stays returned once.
		assertBalance(t, w, "100.00")
	})

	t.Run("undoing the refund releases the bet again", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
		refund := processedOf(t, p, w,
			command(t, Refund, "25.00", withReference(externalID(t, bet))), referenceTo(bet))
		processedOf(t, p, w,
			command(t, Rollback, "25.00", withReference(externalID(t, refund))), referenceTo(refund))
		assertBalance(t, w, "75.00")

		// The refund has itself been reversed, so it no longer holds the bet.
		freed := referenceWith(bet, ReversalView{Transaction: refund, Reversed: true})
		if _, held := freed.ActiveReversal(); held {
			t.Fatal("a reversed refund still holds the bet")
		}

		second := command(t, Refund, "25.00", withReference(externalID(t, bet)))
		out := mustSubmit(t, p, w, second, freed, baseTime)
		assertStatus(t, out, Processed)
		assertBalance(t, w, "100.00")
	})

	// A rollback can never be undone, so one applied straight to a bet holds it
	// for good. That asymmetry is the point: a rollback says the operation never
	// happened, while a refund is a decision that may be revisited.
	t.Run("a rollback holds the bet permanently", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
		rollback := processedOf(t, p, w,
			command(t, Rollback, "25.00", withReference(externalID(t, bet))), referenceTo(bet))

		refund := command(t, Refund, "25.00", withReference(externalID(t, bet)))
		out := submitOK(t, p, w, refund,
			referenceWith(bet, ReversalView{Transaction: rollback}), baseTime)
		assertRejected(t, out, failure.ReferenceAlreadyReversed)
	})

	t.Run("a rejected reversal does not hold the reference", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)

		// A refund that was refused for the wrong amount leaves the bet free.
		failed := submitOK(t, p, w,
			command(t, Refund, "10.00", withReference(externalID(t, bet))), referenceTo(bet), baseTime)
		assertRejected(t, failed, failure.ReversalAmountMismatch)

		good := command(t, Refund, "25.00", withReference(externalID(t, bet)))
		out := mustSubmit(t, p, w, good,
			referenceWith(bet, ReversalView{Transaction: failed.Transaction}), baseTime)
		assertStatus(t, out, Processed)
	})
}

// TestReferenceOutcomes is the documented matrix of what a reference says about
// an operation.
func TestReferenceOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reference  func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID)
		wantStatus Status
		wantCode   failure.Code
	}{
		{
			name: "not found",
			reference: func(*testing.T, *Processor, *Wallet) (*ReferenceView, ExternalTransactionID) {
				return &ReferenceView{}, "ext-missing"
			},
			wantStatus: PendingReference,
		},
		{
			name: "found but still pending",
			reference: func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID) {
				tx, err := NewExternalTransaction(command(t, Bet, "25.00"), w.ID(), baseTime)
				if err != nil {
					t.Fatalf("NewExternalTransaction: %v", err)
				}
				return referenceTo(tx), externalID(t, tx)
			},
			wantStatus: PendingReference,
		},
		{
			name: "found but rejected",
			reference: func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID) {
				out := submitOK(t, p, w, command(t, Bet, "10000.00"), nil, baseTime)
				assertRejected(t, out, failure.InsufficientFunds)
				return referenceTo(out.Transaction), externalID(t, out.Transaction)
			},
			wantStatus: Rejected,
			wantCode:   failure.ReferenceNotProcessed,
		},
		{
			name: "found but failed",
			reference: func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID) {
				tx, err := NewExternalTransaction(command(t, Bet, "25.00"), w.ID(), baseTime)
				if err != nil {
					t.Fatalf("NewExternalTransaction: %v", err)
				}
				if err := tx.Fail(baseTime); err != nil {
					t.Fatalf("Fail: %v", err)
				}
				return referenceTo(tx), externalID(t, tx)
			},
			wantStatus: Rejected,
			wantCode:   failure.ReferenceNotProcessed,
		},
		{
			name: "disagrees on provider",
			reference: func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID) {
				tx := processedOf(t, p, w, command(t, Bet, "25.00", withProvider("other-games")), nil)
				return referenceTo(tx), externalID(t, tx)
			},
			wantStatus: Rejected,
			wantCode:   failure.ReferenceMismatch,
		},
		{
			name: "disagrees on round",
			reference: func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID) {
				tx := processedOf(t, p, w, command(t, Bet, "25.00", withRound("round-99")), nil)
				return referenceTo(tx), externalID(t, tx)
			},
			wantStatus: Rejected,
			wantCode:   failure.ReferenceMismatch,
		},
		{
			name: "wrong kind to reverse",
			reference: func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID) {
				tx := processedOf(t, p, w, command(t, Loss, "0.00"), nil)
				return referenceTo(tx), externalID(t, tx)
			},
			wantStatus: Rejected,
			wantCode:   failure.ReferenceNotReversible,
		},
		{
			name: "already reversed",
			reference: func(t *testing.T, p *Processor, w *Wallet) (*ReferenceView, ExternalTransactionID) {
				bet := processedOf(t, p, w, command(t, Bet, "25.00"), nil)
				other := processedOf(t, p, w,
					command(t, Refund, "25.00", withReference(externalID(t, bet))), referenceTo(bet))
				return referenceWith(bet, ReversalView{Transaction: other}), externalID(t, bet)
			},
			wantStatus: Rejected,
			wantCode:   failure.ReferenceAlreadyReversed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, w := newProcessor(t), newWallet(t, "1000.00")
			ref, referenceID := tc.reference(t, p, w)

			cmd := command(t, Rollback, "25.00", withReference(referenceID))
			out := submitOK(t, p, w, cmd, ref, baseTime)

			assertStatus(t, out, tc.wantStatus)
			if tc.wantCode != "" {
				got, _ := out.Transaction.FailureCode()
				if got != tc.wantCode {
					t.Errorf("failure code = %s, want %s", got, tc.wantCode)
				}
			}
			if tc.wantStatus == PendingReference {
				assertEventTypes(t, out, typeWagerTransactionPendingReference)
			}
		})
	}
}

// TestWaitBudgetEndsInRejection covers the brief's rule that a reference which
// never arrives settles the operation rather than leaving it waiting for ever.
func TestWaitBudgetEndsInRejection(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "100.00")
	cmd := command(t, Refund, "25.00", withReference("ext-never-arrives"))
	missing := &ReferenceView{}

	out := submitOK(t, p, w, cmd, missing, baseTime)
	assertStatus(t, out, PendingReference)
	tx := out.Transaction

	// Each retry carries on from the same transaction, so the attempts add up.
	for attempt := 2; attempt <= p.Policy().MaxAttempts; attempt++ {
		out, err := p.Continue(tx, cmd, w, missing, baseTime)
		if err != nil {
			t.Fatalf("Resume attempt %d: %v", attempt, err)
		}
		assertStatus(t, out, PendingReference)
		if tx.ReferenceAttempts() != attempt {
			t.Fatalf("attempts = %d, want %d", tx.ReferenceAttempts(), attempt)
		}
	}

	final, err := p.Continue(tx, cmd, w, missing, baseTime)
	if err != nil {
		t.Fatalf("Resume past the budget: %v", err)
	}
	assertRejected(t, final, failure.ReferenceNotFound)
	assertBalance(t, w, "100.00")
}

func TestContinueRefusesAMismatchedCommand(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "100.00")
	cmd := command(t, Refund, "25.00", withReference("ext-missing"))
	out := submitOK(t, p, w, cmd, &ReferenceView{}, baseTime)
	tx := out.Transaction

	t.Run("a different payload under the same transaction", func(t *testing.T) {
		t.Parallel()
		altered := cmd
		altered.Money = brl(t, "26.00")
		_, err := p.Continue(tx, altered, w, &ReferenceView{}, baseTime)
		if !failure.Is(err, failure.IdempotencyPayloadConflict) {
			t.Errorf("Continue with an altered payload = %v, want %v", err, failure.IdempotencyPayloadConflict)
		}
	})

	t.Run("a command for a different transaction", func(t *testing.T) {
		t.Parallel()
		other := command(t, Refund, "25.00", withReference("ext-missing"))
		_, err := p.Continue(tx, other, w, &ReferenceView{}, baseTime)
		if !failure.Is(err, failure.ReferenceMismatch) {
			t.Errorf("Continue with another transaction's command = %v, want %v",
				err, failure.ReferenceMismatch)
		}
	})

	t.Run("a wallet the transaction does not belong to", func(t *testing.T) {
		t.Parallel()
		stranger := newWallet(t, "100.00")
		_, err := p.Continue(tx, cmd, stranger, &ReferenceView{}, baseTime)
		if !failure.Is(err, failure.ReferenceMismatch) {
			t.Errorf("Continue against another wallet = %v, want %v", err, failure.ReferenceMismatch)
		}
	})

	t.Run("a settled transaction", func(t *testing.T) {
		t.Parallel()
		for _, status := range []Status{Processed, Rejected, Failed} {
			settled := txInStatus(t, status)
			_, err := p.Continue(settled, cmd, w, &ReferenceView{}, baseTime)
			if !failure.Is(err, failure.InvalidStateTransition) {
				t.Errorf("Continue of a %s transaction = %v, want %v",
					status, err, failure.InvalidStateTransition)
			}
			// The wording is asserted as well as the code. Continue refuses a
			// settled transaction and a transaction in an unrecognised status
			// under the same InvalidStateTransition, so the message is the only
			// thing that tells a reader which guard answered — and without this,
			// deleting either guard leaves the suite green.
			if err != nil && !strings.Contains(err.Error(), "is settled and cannot be carried forward") {
				t.Errorf("Continue of a %s transaction = %q, want the settled-transaction wording",
					status, err)
			}
		}
	})

	t.Run("a transaction in an unrecognised status", func(t *testing.T) {
		t.Parallel()
		// Built by hand rather than through a constructor, because no
		// constructor can produce this: NewExternalTransaction always starts at
		// Pending, RehydrateWagerTransaction refuses a status that is not
		// Known, and every transition lands on a declared one. The guard exists
		// for a status added to the enum without updating Continue, so reaching
		// it means stepping around the very constructors that make it
		// unreachable. Without this the guard can be deleted and the suite
		// stays green, because the settled-transaction cases are caught by the
		// terminal guard above it.
		stray := &WagerTransaction{status: Status("REVERSING")}
		_, err := p.Continue(stray, cmd, w, &ReferenceView{}, baseTime)
		if !failure.Is(err, failure.InvalidStateTransition) {
			t.Errorf("Continue of an unrecognised status = %v, want %v",
				err, failure.InvalidStateTransition)
		}
		if err != nil && !strings.Contains(err.Error(), "only an unsettled operation can be carried forward") {
			t.Errorf("Continue of an unrecognised status = %q, want the unsettled-operation wording", err)
		}
	})

	t.Run("a missing transaction", func(t *testing.T) {
		t.Parallel()
		_, err := p.Continue(nil, cmd, w, &ReferenceView{}, baseTime)
		if !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("Continue(nil) = %v, want %v", err, failure.UninitializedValue)
		}
	})
}

// TestContinueRecoversAStrandedSubmission covers the PENDING case: a submission
// recorded to claim its idempotency key and then orphaned before it was applied.
// Without a way forward it would be found by every later lookup, answer PENDING
// for ever and never settle.
func TestContinueRecoversAStrandedSubmission(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "100.00")
	cmd := command(t, Bet, "25.00")

	// The transaction exists but nothing was ever applied from it.
	stranded, err := NewExternalTransaction(cmd, w.ID(), baseTime)
	if err != nil {
		t.Fatalf("NewExternalTransaction: %v", err)
	}
	if stranded.Status() != Pending {
		t.Fatalf("status = %s, want %s", stranded.Status(), Pending)
	}
	assertBalance(t, w, "100.00")

	out, err := p.Continue(stranded, cmd, w, nil, baseTime.Add(time.Hour))
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}

	assertStatus(t, out, Processed)
	assertBalance(t, w, "75.00")
	assertEventTypes(t, out, typeWagerTransactionProcessed, typeWalletBalanceChanged)
	// It carried the same transaction forward rather than starting a new one.
	if out.Transaction.ID() != stranded.ID() {
		t.Error("Continue started a new transaction instead of carrying the stranded one forward")
	}
}

// TestContinueRecoversAStrandedRejection shows the recovery path settling rather
// than succeeding, since a stranded submission may still be refused.
func TestContinueRecoversAStrandedRejection(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "10.00")
	cmd := command(t, Bet, "25.00")
	stranded, err := NewExternalTransaction(cmd, w.ID(), baseTime)
	if err != nil {
		t.Fatalf("NewExternalTransaction: %v", err)
	}

	out, err := p.Continue(stranded, cmd, w, nil, baseTime)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	assertRejected(t, out, failure.InsufficientFunds)
}

// TestZeroAmountPolicy covers the amount each kind will and will not accept.
func TestZeroAmountPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind      Kind
		zeroIsOK  bool
		aboveIsOK bool
	}{
		{Bet, false, true},
		{Win, false, true},
		{Loss, true, false},
		{Refund, false, true},
		{Rollback, false, true},
	}
	for _, tc := range tests {
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()

			zero := command(t, tc.kind, "0.00", withReference("ext-ref"))
			if !tc.kind.AllowsReference() {
				zero.ReferenceExternalTransactionID = ""
			}
			if !tc.kind.MovesMoney() {
				zero.LedgerEntryID = LedgerEntryID{}
			}
			err := zero.Validate()
			if tc.zeroIsOK && err != nil {
				t.Errorf("a %s of 0.00 = %v, want no error", tc.kind, err)
			}
			if !tc.zeroIsOK && !failure.Is(err, failure.InvalidAmountForKind) {
				t.Errorf("a %s of 0.00 = %v, want %v", tc.kind, err, failure.InvalidAmountForKind)
			}

			above := command(t, tc.kind, "1.00", withReference("ext-ref"))
			if !tc.kind.AllowsReference() {
				above.ReferenceExternalTransactionID = ""
			}
			if !tc.kind.MovesMoney() {
				above.LedgerEntryID = LedgerEntryID{}
			}
			err = above.Validate()
			if tc.aboveIsOK && err != nil {
				t.Errorf("a %s of 1.00 = %v, want no error", tc.kind, err)
			}
			if !tc.aboveIsOK && !failure.Is(err, failure.InvalidAmountForKind) {
				t.Errorf("a %s of 1.00 = %v, want %v", tc.kind, err, failure.InvalidAmountForKind)
			}
		})
	}
}

// TestCurrencyMismatchIsSettled covers the brief's requirement for a currency
// mismatch test even though the main flows are BRL only.
func TestCurrencyMismatchIsSettled(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "100.00")
	cmd := command(t, Bet, "25.00", withMoney(usd(t, "25.00")))

	out := submitOK(t, p, w, cmd, nil, baseTime)
	assertRejected(t, out, failure.CurrencyMismatch)
	assertBalance(t, w, "100.00")
	assertVersion(t, w, 1)
}

func TestCommandValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Command)
		want   failure.Code
	}{
		{"unknown kind", func(c *Command) { c.Kind = "TRANSFER" }, failure.InvalidFieldFormat},
		{"opening from a provider", func(c *Command) { c.Kind = Opening }, failure.UnsupportedTransactionKind},
		{"no transaction id", func(c *Command) { c.TransactionID = TransactionID{} }, failure.MissingRequiredField},
		{"no ledger entry id", func(c *Command) { c.LedgerEntryID = LedgerEntryID{} }, failure.MissingRequiredField},
		{"no provider", func(c *Command) { c.Provider = "" }, failure.MissingRequiredField},
		{"no external id", func(c *Command) { c.ExternalTransactionID = "" }, failure.MissingRequiredField},
		{"no idempotency key", func(c *Command) { c.IdempotencyKey = "" }, failure.MissingRequiredField},
		{"no player", func(c *Command) { c.PlayerID = "" }, failure.MissingRequiredField},
		{"no round", func(c *Command) { c.RoundID = "" }, failure.MissingRequiredField},
		{"no game", func(c *Command) { c.GameID = "" }, failure.MissingRequiredField},
		{"padded identifier", func(c *Command) { c.RoundID = " round-1 " }, failure.InvalidFieldFormat},
		{"uninitialised money", func(c *Command) { c.Money = money.Money{} }, failure.UninitializedValue},
		{
			"reference on a bet",
			func(c *Command) { c.ReferenceExternalTransactionID = "ext-other" },
			failure.ReferenceNotApplicable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := command(t, Bet, "25.00")
			tc.mutate(&cmd)

			err := cmd.Validate()
			if !failure.Is(err, tc.want) {
				t.Fatalf("Validate = %v, want code %v", err, tc.want)
			}
			// Nothing was recorded, so the idempotency key is still free for a
			// corrected resubmission.
			if !failure.Correctable(err) {
				t.Errorf("%s is definitive, but no transaction was recorded", tc.want)
			}

			p, w := newProcessor(t), newWallet(t, "100.00")
			out, err := p.Submit(cmd, w, nil, baseTime)
			if err == nil {
				t.Fatalf("Process = %v, want an error", out)
			}
			if out.Transaction != nil {
				t.Error("a malformed submission produced a transaction")
			}
		})
	}
}

func TestReversalRequiresAReference(t *testing.T) {
	t.Parallel()

	for _, kind := range []Kind{Refund, Rollback} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			cmd := command(t, kind, "25.00")
			if err := cmd.Validate(); !failure.Is(err, failure.ReferenceRequired) {
				t.Errorf("a %s without a reference = %v, want %v", kind, err, failure.ReferenceRequired)
			}
		})
	}
}

func TestOperationCannotReferenceItself(t *testing.T) {
	t.Parallel()

	cmd := command(t, Refund, "25.00")
	cmd.ReferenceExternalTransactionID = cmd.ExternalTransactionID

	if err := cmd.Validate(); !failure.Is(err, failure.InvalidFieldFormat) {
		t.Errorf("a self-referencing operation = %v, want %v", err, failure.InvalidFieldFormat)
	}
}

// TestAWalletBelongingToAnotherPlayerIsRefusedNotSettled pins which side of the
// line this defect falls on.
//
// A provider names a player; loading the wallet that belongs to that name is
// this service's work. A wallet held by somebody else therefore cannot be
// anything the provider submitted, and settling it as a rejection persisted a
// transaction and bound their idempotency key to it for good. Once the lookup
// here was fixed, the same correct submission would keep meeting that stored
// rejection, and the provider had no way to release the key.
//
// This test previously asserted the opposite — assertRejected under
// REFERENCE_MISMATCH — because it was written from the implementation rather
// than from ADR-0002.
//
// Both entry points are covered: Submit creates the transaction, Continue is
// handed one that already exists, and neither may record anything here.
func TestAWalletBelongingToAnotherPlayerIsRefusedNotSettled(t *testing.T) {
	t.Parallel()

	t.Run("on submission", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		cmd := command(t, Bet, "25.00", withPlayer("someone-else"))

		out, err := p.Submit(cmd, w, nil, baseTime)
		if !failure.Is(err, failure.InvalidFieldFormat) {
			t.Fatalf("Submit against another player's wallet = %v, want %v", err, failure.InvalidFieldFormat)
		}
		// The code has to agree with the contract the refusal relies on:
		// nothing was persisted, so the key is still the provider's to use.
		if !failure.Correctable(err) {
			t.Error("the refusal reports as definitive, which says the idempotency key is spent")
		}
		// Nothing recorded means nothing to persist and no key bound.
		if out.Recorded() {
			t.Error("a refused submission still produced a transaction")
		}
		if len(out.Events) != 0 {
			t.Errorf("a refused submission emitted %v", eventTypes(out.Events))
		}
		if out.LedgerEntry != nil {
			t.Error("a refused submission produced a ledger entry")
		}
		assertBalance(t, w, "100.00")
		assertVersion(t, w, 1)
	})

	t.Run("on continuation", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")

		// A transaction parked against the right wallet, then carried forward
		// against a wallet whose player does not match.
		cmd := command(t, Refund, "25.00", withReference("ext-not-here"))
		parked := submitOK(t, p, w, cmd, &ReferenceView{}, baseTime)
		assertStatus(t, parked, PendingReference)

		impostor := newWallet(t, "100.00")
		impostor.playerID = "someone-else"
		impostor.id = w.ID() // past the wallet-identity check, onto the player one

		out, err := p.Continue(parked.Transaction, cmd, impostor, &ReferenceView{}, baseTime)
		if !failure.Is(err, failure.InvalidFieldFormat) {
			t.Fatalf("Continue against another player's wallet = %v, want %v", err, failure.InvalidFieldFormat)
		}
		if !strings.Contains(err.Error(), "player") {
			t.Errorf("err = %q, want the refusal to be about the player", err)
		}
		if out.Recorded() {
			t.Error("a refused continuation still produced an outcome")
		}
		// The parked transaction must be exactly where it was, still able to
		// finish once the caller hands over the right wallet.
		if got := parked.Transaction.Status(); got != PendingReference {
			t.Errorf("the parked transaction moved to %s", got)
		}
		if got := parked.Transaction.ReferenceAttempts(); got != 1 {
			t.Errorf("attempts = %d, want the refused continuation not to have counted", got)
		}
	})
}

// TestABalanceThatCannotGrowSettlesTheOperation pins the rule apply's own
// comment states: once a transaction exists, a refusal is a settled outcome and
// not an error.
//
// A credit that would overflow escaped as failure.AmountOutOfRange, which is
// correctable — and a correctable error means, by the contract Submit and
// Continue document, that nothing was recorded and the idempotency key is still
// free. On the Continue path that is plainly false: the transaction is already
// in storage with its key bound to its payload, so the provider would be told
// to resubmit something that can only come back as a conflict. The same apply
// runs for both entry points, so settling it here settles it for both.
//
// BalanceOutOfRange is definitive because it describes the balance rather than
// the payload. Repairing the amount does not help, and resending the same one
// overflows again.
func TestABalanceThatCannotGrowSettlesTheOperation(t *testing.T) {
	t.Parallel()

	// Exactly what int64 minor units can hold, so the next credit cannot land.
	const ceiling = "92233720368547758.07"
	p, w := newProcessor(t), newWallet(t, ceiling)

	out := submitOK(t, p, w, command(t, Win, "0.01"), nil, baseTime)

	assertRejected(t, out, failure.BalanceOutOfRange)
	// A refusal that settles must leave the wallet exactly as it found it.
	assertBalance(t, w, ceiling)
	assertVersion(t, w, 1)
}

func TestNewProcessorRefusesAMeaninglessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy ReferencePolicy
	}{
		{"no attempts", ReferencePolicy{MaxAttempts: 0, TTL: time.Minute}},
		{"negative attempts", ReferencePolicy{MaxAttempts: -1, TTL: time.Minute}},
		{"no time to live", ReferencePolicy{MaxAttempts: 3}},
		{"negative time to live", ReferencePolicy{MaxAttempts: 3, TTL: -time.Minute}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewProcessor(tc.policy); !failure.Is(err, failure.InvalidFieldFormat) {
				t.Errorf("NewProcessor(%v) = %v, want %v", tc.policy, err, failure.InvalidFieldFormat)
			}
		})
	}
}

// TestStrayReferenceViewIsIgnored pins that a ReferenceView supplied for an
// operation that names no reference cannot change what happens to it.
//
// A caller that resolves references uniformly — looking one up before every
// submission and passing whatever it found — must not thereby break a bet. The
// operation names no reference, so nothing the caller looked up bears on it.
//
// Regression: apply() resolved the reference whenever the view carried a
// transaction, without asking whether the command named one. A bet submitted
// with a stray view came back REFERENCE_NOT_APPLICABLE as an error after the
// transaction had already been created — so per ADR-0002 nothing was recorded
// and the idempotency key stayed free, while the code was correctable, inviting
// the provider to resubmit the identical payload and fail identically for ever.
func TestStrayReferenceViewIsIgnored(t *testing.T) {
	t.Parallel()

	// strayView is a perfectly good transaction that simply has no bearing on
	// the operation it is handed to. It belongs to a wallet of its own, so it
	// cannot perturb the balance under test — and being someone else's
	// transaction is exactly what makes it irrelevant.
	strayView := func(t *testing.T, p *Processor) *ReferenceView {
		t.Helper()
		elsewhere := newWallet(t, "100.00")
		unrelated := mustSubmit(t, p, elsewhere, command(t, Bet, "5.00"), nil, baseTime).Transaction
		return referenceTo(unrelated)
	}

	tests := []struct {
		name    string
		kind    Kind
		amount  string
		balance string
	}{
		{"bet names no reference", Bet, "25.00", "75.00"},
		{"win names no reference", Win, "25.00", "125.00"},
		{"loss names no reference", Loss, "0.00", "100.00"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newProcessor(t)

			// Establish the baseline on a wallet of its own, so the two runs
			// cannot interfere.
			clean := newWallet(t, "100.00")
			want := mustSubmit(t, p, clean, command(t, tc.kind, tc.amount), nil, baseTime)

			// Now the same operation, with an irrelevant view handed over.
			w := newWallet(t, "100.00")
			stray := strayView(t, p)
			out, err := p.Submit(command(t, tc.kind, tc.amount), w, stray, baseTime)
			if err != nil {
				t.Fatalf("a stray reference view failed the submission: %v", err)
			}
			if !out.Recorded() {
				t.Fatal("a stray reference view discarded the transaction")
			}
			assertStatus(t, out, Processed)

			// It must be indistinguishable from the baseline.
			if got := len(out.Events); got != len(want.Events) {
				t.Errorf("events = %v, want %v", eventTypes(out.Events), eventTypes(want.Events))
			}
			if (out.LedgerEntry != nil) != (want.LedgerEntry != nil) {
				t.Errorf("ledger entry present = %v, want %v", out.LedgerEntry != nil, want.LedgerEntry != nil)
			}
			if got, _ := out.Transaction.Result(); got.Amount() != tc.balance {
				t.Errorf("reported balance = %s, want %s", got.Amount(), tc.balance)
			}
			// Nothing was resolved, because nothing was named.
			if id, ok := out.Transaction.ResolvedReferenceID(); ok {
				t.Errorf("an operation naming no reference resolved one: %s", id)
			}
		})
	}

	t.Run("an operation that does name a reference still resolves it", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime).Transaction

		cmd := command(t, Refund, "25.00", withReference(externalID(t, bet)))
		out := mustSubmit(t, p, w, cmd, referenceTo(bet), baseTime)

		id, ok := out.Transaction.ResolvedReferenceID()
		if !ok {
			t.Fatal("a refund did not resolve the reference it names")
		}
		if id != bet.ID() {
			t.Errorf("resolved reference = %s, want %s", id, bet.ID())
		}
	})
}

// TestOutcomeRecordedContract pins what an Outcome promises.
//
// Every outcome the processor returns with a nil error carries a transaction —
// a rejection and a parked operation are recorded just as much as a processed
// one. Exactly one successful outcome in the domain carries none: a wallet
// opened at zero, which records no opening because there is no starting balance
// to record. Outcome.Recorded is how a caller tells them apart.
//
// Regression: Outcome's doc said "Transaction is always present", so a caller
// following it dereferenced the nil that OpenWallet returns at zero. The doc was
// wrong rather than the code — CONTEXT.md produces an opening "only when a
// wallet is created with money in it" — and this test keeps the corrected rule
// true of every path rather than only of the one that exposed it.
func TestOutcomeRecordedContract(t *testing.T) {
	t.Parallel()

	t.Run("a wallet opened at zero records nothing", func(t *testing.T) {
		t.Parallel()
		_, out, err := OpenWallet(openInput(t, "0.00"), nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet: %v", err)
		}
		if out.Recorded() {
			t.Error("a wallet opened at zero reported a recorded outcome")
		}
		if out.Transaction != nil {
			t.Error("Recorded and Transaction disagree")
		}
	})

	t.Run("a wallet opened with a balance records its opening", func(t *testing.T) {
		t.Parallel()
		_, out, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet: %v", err)
		}
		if !out.Recorded() {
			t.Fatal("a wallet opened with a balance reported no recorded outcome")
		}
		if out.Transaction.Kind() != Opening {
			t.Errorf("kind = %s, want %s", out.Transaction.Kind(), Opening)
		}
	})

	// Every kind a provider may submit, reaching every status the processor can
	// return. If a new path ever returns Outcome{} with a nil error, this fails.
	t.Run("every successful processor outcome is recorded", func(t *testing.T) {
		t.Parallel()

		type scenario struct {
			name string
			want Status
			run  func(t *testing.T, p *Processor, w *Wallet) Outcome
		}
		processedBet := func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction {
			t.Helper()
			return mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime).Transaction
		}

		scenarios := []scenario{
			{"processed bet", Processed, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				return submitOK(t, p, w, command(t, Bet, "25.00"), nil, baseTime)
			}},
			{"processed win", Processed, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				return submitOK(t, p, w, command(t, Win, "10.00"), nil, baseTime)
			}},
			{"processed loss", Processed, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				return submitOK(t, p, w, command(t, Loss, "0.00"), nil, baseTime)
			}},
			{"processed refund", Processed, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				bet := processedBet(t, p, w)
				cmd := command(t, Refund, "25.00", withReference(externalID(t, bet)))
				return submitOK(t, p, w, cmd, referenceTo(bet), baseTime)
			}},
			{"processed rollback", Processed, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				bet := processedBet(t, p, w)
				cmd := command(t, Rollback, "25.00", withReference(externalID(t, bet)))
				return submitOK(t, p, w, cmd, referenceTo(bet), baseTime)
			}},
			{"rejected on a business rule", Rejected, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				cmd := command(t, Bet, "25.00", withMoney(usd(t, "25.00")))
				return submitOK(t, p, w, cmd, nil, baseTime)
			}},
			{"rejected for insufficient funds", Rejected, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				return submitOK(t, p, w, command(t, Bet, "999.00"), nil, baseTime)
			}},
			{"parked waiting for a reference", PendingReference, func(t *testing.T, p *Processor, w *Wallet) Outcome {
				cmd := command(t, Refund, "25.00", withReference("ext-not-here-yet"))
				return submitOK(t, p, w, cmd, &ReferenceView{}, baseTime)
			}},
		}

		for _, tc := range scenarios {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				out := tc.run(t, newProcessor(t), newWallet(t, "100.00"))

				if !out.Recorded() {
					t.Fatal("a successful processor outcome carried no transaction")
				}
				if out.Transaction == nil {
					t.Fatal("Recorded and Transaction disagree")
				}
				assertStatus(t, out, tc.want)
				if len(out.Events) == 0 {
					t.Error("a recorded outcome produced no events")
				}
			})
		}
	})

	t.Run("a refused submission is not recorded", func(t *testing.T) {
		t.Parallel()
		// A malformed submission is the other side of the rule: nothing is
		// recorded, so there is no transaction and Recorded says so.
		p, w := newProcessor(t), newWallet(t, "100.00")
		out, err := p.Submit(command(t, Bet, "25.00", withProvider("")), w, nil, baseTime)
		if err == nil {
			t.Fatal("a malformed submission was accepted")
		}
		if out.Recorded() {
			t.Error("a refused submission reported a recorded outcome")
		}
	})
}

// TestZeroValueProcessorRefusesInsteadOfSettling pins the zero value of
// Processor as unusable.
//
// A Processor carries only an unexported policy, so `Processor{}` is a legal
// composite literal outside this package and compiles anywhere NewProcessor
// would. Its MaxAttempts is zero, which every transaction trivially meets, so a
// wait budget read from it reports "spent" on the very first park. That turns a
// routine PENDING_REFERENCE into a REFERENCE_NOT_FOUND rejection — a definitive
// code, so the transaction is persisted, WagerTransactionRejected is published,
// and the idempotency key is bound to that payload for good. The provider is
// told its reference does not exist when nobody ever waited for it, and the
// operation can never be retried under the same key.
//
// The requirement is therefore stronger than "do not settle wrongly": the
// operation must be refused before a transaction exists at all, so that the
// error means what ADR-0002 says it means — nothing was recorded and the key is
// still free.
//
// Regression: ReferenceBudgetExhausted answered from the unvalidated policy and
// Processor.wait believed it.
func TestZeroValueProcessorRefusesInsteadOfSettling(t *testing.T) {
	t.Parallel()

	// A refund whose reference has not been found is the operation that must be
	// parked; it is the one a spent budget would settle.
	waitingRefund := func(t *testing.T) (Command, *Wallet, *ReferenceView) {
		t.Helper()
		return command(t, Refund, "25.00", withReference("ext-bet-unknown")),
			newWallet(t, "100.00"),
			&ReferenceView{Transaction: nil}
	}

	t.Run("Submit refuses before recording anything", func(t *testing.T) {
		t.Parallel()
		cmd, w, ref := waitingRefund(t)
		var zero Processor

		out, err := zero.Submit(cmd, w, ref, baseTime)
		if err == nil {
			code, _ := out.Transaction.FailureCode()
			t.Fatalf("a zero-value Processor settled the operation as %s (%s) instead of refusing it",
				out.Transaction.Status(), code)
		}
		if !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("Submit = %v, want %v", err, failure.UninitializedValue)
		}
		if !failure.Correctable(err) {
			t.Error("the refusal is not correctable, so the idempotency key would stay bound")
		}
		if out.Transaction != nil {
			t.Error("a refused submission produced a transaction")
		}
		if len(out.Events) != 0 {
			t.Errorf("a refused submission produced %d events", len(out.Events))
		}
		assertBalance(t, w, "100.00")
		assertVersion(t, w, 1)
	})

	t.Run("Continue refuses before touching the transaction", func(t *testing.T) {
		t.Parallel()
		cmd, w, ref := waitingRefund(t)
		parked := submitOK(t, newProcessor(t), w, cmd, ref, baseTime)
		assertStatus(t, parked, PendingReference)
		attempts := parked.Transaction.ReferenceAttempts()

		var zero Processor
		if _, err := zero.Continue(parked.Transaction, cmd, w, ref, baseTime); !failure.Is(err, failure.UninitializedValue) {
			t.Fatalf("Continue = %v, want %v", err, failure.UninitializedValue)
		}
		if got := parked.Transaction.Status(); got != PendingReference {
			t.Errorf("a refused Continue moved the transaction to %s", got)
		}
		if got := parked.Transaction.ReferenceAttempts(); got != attempts {
			t.Errorf("a refused Continue spent an attempt: %d, want %d", got, attempts)
		}
	})

	t.Run("ReferenceBudgetExhausted refuses a meaningless policy", func(t *testing.T) {
		t.Parallel()
		tx := txInStatus(t, Pending)
		for _, policy := range []ReferencePolicy{
			{},
			{MaxAttempts: 0, TTL: time.Minute},
			{MaxAttempts: -1, TTL: time.Minute},
			{MaxAttempts: 3},
		} {
			spent, err := tx.ReferenceBudgetExhausted(policy, baseTime)
			if !failure.Is(err, failure.InvalidFieldFormat) {
				t.Errorf("ReferenceBudgetExhausted(%v) err = %v, want %v", policy, err, failure.InvalidFieldFormat)
			}
			if spent {
				t.Errorf("ReferenceBudgetExhausted(%v) reported a spent budget alongside an error", policy)
			}
		}
	})

	t.Run("a built processor still parks the same operation", func(t *testing.T) {
		t.Parallel()
		cmd, w, ref := waitingRefund(t)

		out := submitOK(t, newProcessor(t), w, cmd, ref, baseTime)
		assertStatus(t, out, PendingReference)
		assertEventTypes(t, out, typeWagerTransactionPendingReference)
		assertBalance(t, w, "100.00")
		assertVersion(t, w, 1)
	})
}
