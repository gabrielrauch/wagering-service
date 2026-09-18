package wagering

import (
	"slices"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// Kind is what an operation does to a wallet.
type Kind string

const (
	// Opening records a wallet's starting balance. It is raised only by the
	// system, when a wallet is created with money already in it, and is refused
	// when a provider submits it.
	Opening Kind = "OPENING"
	// Bet is a player staking money on a round. It debits the wallet and needs
	// a positive amount and sufficient balance.
	Bet Kind = "BET"
	// Win is a payout for a round. It credits the wallet and needs a positive
	// amount. It may name the bet it pays out on, in the same round.
	Win Kind = "WIN"
	// Loss records that a round ended with no payout. It moves no money, needs
	// a zero amount, writes no ledger entry and leaves the wallet version
	// untouched.
	Loss Kind = "LOSS"
	// Refund returns a processed bet's full stake. It credits the wallet and
	// must name the bet it returns.
	Refund Kind = "REFUND"
	// Rollback undoes a processed bet, win or refund in full, moving money in
	// the opposite direction to the operation it undoes.
	Rollback Kind = "ROLLBACK"
)

// kinds lists every declared kind, in the order the brief presents them.
var kinds = []Kind{Opening, Bet, Win, Loss, Refund, Rollback}

// Kinds returns every declared kind.
func Kinds() []Kind { return slices.Clone(kinds) }

// ParseKind reads a kind from its wire form.
func ParseKind(s string) (Kind, error) {
	if k := Kind(s); k.Known() {
		return k, nil
	}
	return "", failure.New(failure.InvalidFieldFormat, "%q is not a known kind", s).WithField("kind")
}

// String returns the wire form of the kind.
func (k Kind) String() string { return string(k) }

// Known reports whether k is a declared kind.
func (k Kind) Known() bool { return slices.Contains(kinds, k) }

// IsInternal reports whether the kind may only be raised by this system.
func (k Kind) IsInternal() bool { return k == Opening }

// IsExternal reports whether the kind may be submitted by a provider.
func (k Kind) IsExternal() bool { return k.Known() && !k.IsInternal() }

// IsReversal reports whether the kind exists to undo another transaction.
func (k Kind) IsReversal() bool { return k == Refund || k == Rollback }

// MovesMoney reports whether a processed transaction of this kind changes the
// wallet balance, and so writes a ledger entry.
func (k Kind) MovesMoney() bool { return k.Known() && k != Loss }

// RequiresReference reports whether the kind cannot be submitted without naming
// the transaction it acts on.
func (k Kind) RequiresReference() bool { return k.IsReversal() }

// AllowsReference reports whether the kind may name another transaction. A
// reference on any other kind is refused rather than ignored.
func (k Kind) AllowsReference() bool { return k == Win || k.IsReversal() }

// RequiresPositiveAmount reports whether the kind refuses a zero amount.
func (k Kind) RequiresPositiveAmount() bool {
	return k == Bet || k == Win || k.IsReversal()
}

// RequiresZeroAmount reports whether the kind refuses a non-zero amount.
func (k Kind) RequiresZeroAmount() bool { return k == Loss }

// CanReverse reports whether a reversal of this kind may act on a transaction
// of kind target.
//
// A refund returns a bet and nothing else. A rollback undoes a bet, a win or a
// refund — but never another rollback, which is why a rollback applied directly
// to a bet is permanent while a refund can itself be undone.
func (k Kind) CanReverse(target Kind) bool {
	switch k {
	case Refund:
		return target == Bet
	case Rollback:
		return target == Bet || target == Win || target == Refund
	default:
		return false
	}
}

// Direction returns the way a processed transaction of this kind moves money.
//
// It is defined for every kind that moves money on its own terms. A rollback
// has no direction of its own — it takes the opposite of whatever it undoes —
// and a loss moves nothing, so both report false.
func (k Kind) Direction() (Direction, bool) {
	switch k {
	case Opening, Win, Refund:
		return Credit, true
	case Bet:
		return Debit, true
	default:
		return "", false
	}
}

// zeroAmount is how a zero amount is spelled in the contract, used in the
// message a provider sees when a loss carries money.
const zeroAmount = "0.00"

// checkAmount reports whether m is an amount this kind may carry.
//
// It is the one statement of the rule. A submission is held to it by
// [Command.Validate], a stored row by [TransactionSnapshot.validateAmount], and
// both ask here rather than restating it, so storage cannot become a way around
// what construction refuses.
//
// The caller has already established that m is a usable value: an uninitialised
// or negative amount is refused earlier, under a code that says so.
func (k Kind) checkAmount(m money.Money) error {
	if k.RequiresZeroAmount() {
		if !m.IsZero() {
			return failure.New(failure.InvalidAmountForKind,
				"a %s moves no money and requires an amount of %s, got %s",
				k, zeroAmount, m.Amount()).WithField("money")
		}
		return nil
	}
	if !m.IsPositive() {
		return failure.New(failure.InvalidAmountForKind,
			"a %s moves money and requires an amount greater than zero, got %s",
			k, m.Amount()).WithField("money")
	}
	return nil
}

// checkReference reports whether named is a reference this kind may carry,
// given that the operation's own identifier is own.
//
// Like [Kind.checkAmount] it is asked by both [Command.Validate] and
// [TransactionSnapshot.validateReference], so the two cannot drift into
// accepting different things.
func (k Kind) checkReference(own, named ExternalTransactionID) error {
	switch {
	case k.RequiresReference() && named == "":
		return failure.New(failure.ReferenceRequired,
			"a %s must name the transaction it reverses", k).WithField("referenceExternalTransactionId")
	case !k.AllowsReference() && named != "":
		return failure.New(failure.ReferenceNotApplicable,
			"a %s names no reference", k).WithField("referenceExternalTransactionId")
	case named == "":
		return nil
	}
	if _, err := NewExternalTransactionID(string(named)); err != nil {
		return err
	}
	if named == own {
		return failure.New(failure.InvalidFieldFormat,
			"an operation cannot reference itself").WithField("referenceExternalTransactionId")
	}
	return nil
}
