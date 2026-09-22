package wagering

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// TestReferenceMustAgreeOnEveryField pins referenceAgrees one field at a time.
//
// A reversal and the operation it undoes describe one piece of business, and
// the domain says so by comparing five things: the wallet, the player, the
// currency, the provider and the round. TestReferenceOutcomes covers two of
// them. This table covers all five, for both reversal kinds, and each case
// differs from the control at the top by EXACTLY one field — so a check that
// was dropped from referenceAgrees fails the one case that is about it rather
// than being masked by another field that also happened to disagree.
//
// Three of the references cannot be produced through the domain's own doors,
// because the doors refuse them: a bet on this wallet under another player, and
// a bet on this wallet in another currency, are what storage could hand the
// domain and what the domain never writes. They are built the way
// TestAWalletBelongingToAnotherPlayerIsRefusedNotSettled builds its impostor —
// a wallet carrying the id under test and the one field that disagrees.
func TestReferenceMustAgreeOnEveryField(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// reference builds the PROCESSED bet the reversal will name, given the
		// processor and the wallet the reversal is submitted against.
		reference func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction
		want      Status
		code      failure.Code
	}{
		{
			name: "agrees on every field",
			reference: func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction {
				return processedOf(t, p, w, command(t, Bet, "25.00"), nil)
			},
			want: Processed,
		},
		{
			name: "disagrees on the player",
			reference: func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction {
				// This wallet's id, somebody else's name on it.
				impostor := newWallet(t, "1000.00")
				impostor.playerID = "someone-else"
				impostor.id = w.ID()
				return processedOf(t, p, impostor,
					command(t, Bet, "25.00", withPlayer("someone-else")), nil)
			},
			want: Rejected,
			code: failure.ReferenceMismatch,
		},
		{
			name: "disagrees on the wallet",
			reference: func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction {
				// The same player's other wallet, in the same currency.
				other := newWallet(t, "1000.00")
				return processedOf(t, p, other, command(t, Bet, "25.00"), nil)
			},
			want: Rejected,
			code: failure.ReferenceMismatch,
		},
		{
			name: "disagrees on the currency",
			reference: func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction {
				// This wallet's id, holding dollars: the bet it records is in USD
				// and the reversal below is in the BRL the wallet under test
				// holds, so the reversal's own currency check passes and only
				// the reference's currency disagrees.
				dollars := newWalletHolding(t, usd(t, "1000.00"))
				dollars.id = w.ID()
				return processedOf(t, p, dollars,
					command(t, Bet, "25.00", withMoney(usd(t, "25.00"))), nil)
			},
			want: Rejected,
			code: failure.ReferenceMismatch,
		},
		{
			name: "disagrees on the provider",
			reference: func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction {
				return processedOf(t, p, w, command(t, Bet, "25.00", withProvider("other-games")), nil)
			},
			want: Rejected,
			code: failure.ReferenceMismatch,
		},
		{
			name: "disagrees on the round",
			reference: func(t *testing.T, p *Processor, w *Wallet) *WagerTransaction {
				return processedOf(t, p, w, command(t, Bet, "25.00", withRound("round-99")), nil)
			},
			want: Rejected,
			code: failure.ReferenceMismatch,
		},
	}

	for _, kind := range []Kind{Refund, Rollback} {
		for _, tc := range cases {
			t.Run("a "+kind.String()+" that "+tc.name, func(t *testing.T) {
				t.Parallel()
				p, w := newProcessor(t), newWallet(t, "1000.00")
				bet := tc.reference(t, p, w)
				before := w.Balance().Amount()

				cmd := command(t, kind, "25.00", withReference(externalID(t, bet)))
				out := submitOK(t, p, w, cmd, referenceTo(bet), baseTime)

				if tc.want == Processed {
					assertStatus(t, out, Processed)
					if out.LedgerEntry == nil || out.LedgerEntry.Direction() != Credit {
						t.Fatalf("a %s of a bet produced %v, want a credit", kind, out.LedgerEntry)
					}
					assertBalance(t, w, "1000.00")
					return
				}
				assertRejected(t, out, tc.code)
				// A mismatch settles the reversal and touches nothing else: the
				// wallet is where the reference left it, and the reversal
				// resolved to nothing.
				assertBalance(t, w, before)
				if _, resolved := out.Transaction.ResolvedReferenceID(); resolved {
					t.Error("a mismatched reversal resolved its reference")
				}
			})
		}
	}
}

// TestAWinNamingABetInAnotherRoundIsAMismatch is the same rule on the one
// non-reversal that names a reference.
//
// A win may name the bet it pays out on, and the domain checks that naming
// with referenceAgrees before it checks anything about the kind — so a win
// pointing at a bet in another round is REFERENCE_MISMATCH, exactly as a
// refund would be, and not a win that quietly pays out on the wrong round.
// The control comes first: the same win, in the bet's round, is applied.
func TestAWinNamingABetInAnotherRoundIsAMismatch(t *testing.T) {
	t.Parallel()

	t.Run("in the bet's round it is applied", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00", withRound("round-99")), nil)

		win := command(t, Win, "40.00", withReference(externalID(t, bet)), withRound("round-99"))
		out := mustSubmit(t, p, w, win, referenceTo(bet), baseTime)
		assertBalance(t, w, "115.00")
		if got, ok := out.Transaction.ResolvedReferenceID(); !ok || got != bet.ID() {
			t.Errorf("resolved reference = %v, %v, want %s", got, ok, bet.ID())
		}
	})

	t.Run("in another round it is a mismatch", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		bet := processedOf(t, p, w, command(t, Bet, "25.00", withRound("round-99")), nil)
		assertBalance(t, w, "75.00")

		// The default round, which is not the bet's.
		win := command(t, Win, "40.00", withReference(externalID(t, bet)))
		out := submitOK(t, p, w, win, referenceTo(bet), baseTime)

		assertRejected(t, out, failure.ReferenceMismatch)
		assertBalance(t, w, "75.00")
		assertVersion(t, w, 2)
		if _, resolved := out.Transaction.ResolvedReferenceID(); resolved {
			t.Error("a mismatched win resolved its reference")
		}

		// The rejected win holds nothing: the bet is still refundable, with
		// that win in view.
		refund := command(t, Refund, "25.00", withReference(externalID(t, bet)), withRound("round-99"))
		returned := submitOK(t, p, w, refund,
			referenceWith(bet, ReversalView{Transaction: out.Transaction}), baseTime)
		assertStatus(t, returned, Processed)
		assertBalance(t, w, "100.00")
	})
}

// newWalletHolding opens a wallet for the test player holding the given money,
// in whatever currency it is in.
//
// Beside [newWallet], which is BRL only: the one scenario that needs a wallet
// in another currency is the reference disagreeing on it, and everything else
// in this package deals in one.
func newWalletHolding(t *testing.T, balance money.Money) *Wallet {
	t.Helper()
	in := OpenWalletInput{
		WalletID:       NewWalletID(),
		PlayerID:       testPlayer,
		InitialBalance: balance,
	}
	if balance.IsPositive() {
		in.TransactionID = NewTransactionID()
		in.LedgerEntryID = NewLedgerEntryID()
	}
	w, _, err := OpenWallet(in, nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet(%s): %v", balance.Amount(), err)
	}
	return w
}
