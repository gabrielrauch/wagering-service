package wagering

import (
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// ReferenceView is everything the domain needs to know about the transaction an
// operation points at.
//
// Transaction is nil when the reference has not been found — which is different
// from the operation naming none at all, in which case the whole view is nil.
//
// Passing a view anyway for an operation that names no reference is tolerated
// rather than fatal. It is still the wrong thing to hand over, and the domain
// ignores it completely: a bet names nothing, so nothing about a bet can depend
// on what a caller happened to look up. A caller that resolves references
// uniformly before submitting therefore cannot break an operation that has none.
//
// Reversals are the transactions that point back at Transaction. A caller may
// pass everything that references it — a query for "transactions referencing X"
// naturally returns wins too — and both [ReferenceView.ActiveReversal] and
// [ReferenceView.HasSuccessfulReversalOfKind] ignore anything that is not a
// reversal, so a bet does not become unreturnable the moment it pays out.
//
// What a caller must not leave out is a processed reversal that has since been
// undone. Two rules read this view, and they count different things: the
// active-reversal rule wants the one reversal still holding the reference, and
// the per-kind rule wants every reversal of a kind that ever took effect, undone
// or not. A view carrying only the current holder answers the first and is
// silent on the second, which puts REFERENCE_ALREADY_REVERSED for a repeated
// refund out of the domain's reach and leaves it to the schema alone.
//
// Each reversal carries whether it has itself been reversed, which is as deep
// as this ever needs to go: a rollback cannot be reversed, and a refund can
// only be reversed by a rollback, so the tree is never more than two levels
// deep and no caller has to walk an open-ended chain.
type ReferenceView struct {
	Transaction *WagerTransaction
	Reversals   []ReversalView
}

// ReversalView is one transaction pointing at a reference, and whether it still
// counts.
type ReversalView struct {
	// Transaction is the transaction pointing at the reference.
	Transaction *WagerTransaction
	// Reversed reports whether this transaction has been undone by a processed
	// rollback of its own.
	Reversed bool
}

// Found reports whether the referenced transaction has been located.
func (v *ReferenceView) Found() bool {
	return v != nil && v.Transaction != nil
}

// ActiveReversal returns the reversal currently holding the reference, if any.
//
// A reversal holds the reference while it is processed and has not itself been
// reversed. Undoing a refund therefore releases the bet it returned, and the
// bet can be rolled back — the sequence bet, refund, rollback of that refund
// nets to the bet standing, and leaves it reversible by a rollback. It does not
// leave it refundable again: the refund that was undone still counts as a
// successful refund of the bet, which is [ReferenceView.HasSuccessfulReversalOfKind]
// and the rule evaluateReference asks first.
//
// A rollback applied directly to a bet can never be undone, so it holds the bet
// permanently. That asymmetry is deliberate: a rollback is a provider saying an
// operation never happened, while a refund is a business decision that may be
// revisited.
func (v *ReferenceView) ActiveReversal() (*WagerTransaction, bool) {
	if v == nil {
		return nil, false
	}
	for _, r := range v.Reversals {
		if r.Transaction == nil || !r.Transaction.Kind().IsReversal() {
			// A win may point at a bet without undoing it, so it never holds
			// the slot.
			continue
		}
		if r.Transaction.Status() != Processed {
			// Only a reversal that actually took effect holds the reference. A
			// rejected or still-pending one must not block a legitimate retry.
			continue
		}
		if r.Reversed {
			continue
		}
		return r.Transaction, true
	}
	return nil, false
}

// HasSuccessfulReversalOfKind reports whether a processed reversal of the given
// kind already points at the reference — whether or not that reversal has
// since been reversed itself.
//
// This is the brief's rule that a reference never receives two successful
// reversals of one kind, and it is deliberately blind to release. The
// active-reversal rule lets a rollback of a refund free the bet, and without
// this one the bet could then be refunded a second time: BET → REFUND →
// ROLLBACK of the refund → REFUND, two processed refunds of one bet, each
// returning the stake. A different kind is still allowed through — the same
// sequence followed by a ROLLBACK of the bet is the case ADR-0003 exists to
// permit — so what is counted is the pair (reference, kind), never the
// reference alone.
//
// Only reversals count, and only ones that took effect. A win pointing at the
// bet is not a reversal of it, and a rejected or still-pending reversal
// returned nothing. Asking about a kind that is not a reversal answers false,
// because no such reversal can exist.
func (v *ReferenceView) HasSuccessfulReversalOfKind(kind Kind) bool {
	if v == nil || !kind.IsReversal() {
		return false
	}
	for _, r := range v.Reversals {
		if r.Transaction == nil || r.Transaction.Kind() != kind {
			continue
		}
		if r.Transaction.Status() == Processed {
			return true
		}
	}
	return false
}

// referenceOutcome is what the reference says should happen to an operation.
type referenceOutcome int

const (
	// referenceApply means the operation may proceed.
	referenceApply referenceOutcome = iota
	// referenceWait means the reference is not usable yet but may become so.
	referenceWait
	// referenceReject means the reference settles the operation against it.
	referenceReject
)

// evaluateReference decides what the reference says about an operation.
//
// The checks run in a fixed order, so the code a provider receives is
// deterministic: first whether the reference is available at all, then whether
// it describes the same piece of business, then whether it is the right kind to
// act on, then whether the amounts agree, and only then whether history already
// spent it. Everything about the request is settled before anything about what
// has happened since.
//
// History is asked two questions, under one code. Whether a reversal of this
// kind has already succeeded comes first, because it is the stronger claim: it
// holds even after the earlier reversal was undone. Whether some reversal is
// still holding the reference comes second. Both answer
// REFERENCE_ALREADY_REVERSED, so which one refused is not something a provider
// has to tell apart.
func evaluateReference(cmd Command, w *Wallet, ref *ReferenceView) (referenceOutcome, failure.Code) {
	if !cmd.namesReference() {
		return referenceApply, ""
	}
	if !ref.Found() {
		return referenceWait, ""
	}

	target := ref.Transaction
	switch target.Status() {
	case Processed:
		// The only status that can be acted on; the checks below decide whether
		// this particular command agrees with it.
	case Pending, PendingReference:
		// The reference exists but has not settled. Waiting is right: it may
		// still be processed, and acting now would guess at its outcome.
		return referenceWait, ""
	default:
		// Rejected, Failed, or a status not yet known: there is nothing to act on.
		return referenceReject, failure.ReferenceNotProcessed
	}

	if !referenceAgrees(cmd, w, target) {
		return referenceReject, failure.ReferenceMismatch
	}

	if cmd.Kind.IsReversal() {
		if !cmd.Kind.CanReverse(target.Kind()) {
			return referenceReject, failure.ReferenceNotReversible
		}
		// Partial reversals do not exist: a reversal returns exactly what the
		// operation it undoes moved.
		if !cmd.Money.Equal(target.Money()) {
			return referenceReject, failure.ReversalAmountMismatch
		}
		if ref.HasSuccessfulReversalOfKind(cmd.Kind) {
			return referenceReject, failure.ReferenceAlreadyReversed
		}
		if _, held := ref.ActiveReversal(); held {
			return referenceReject, failure.ReferenceAlreadyReversed
		}
		return referenceApply, ""
	}

	// A win may name the bet it pays out on. That is not a reversal: it does not
	// have to match the stake, and it does not hold the bet, so several wins may
	// point at one bet and the bet stays refundable.
	if target.Kind() != Bet {
		return referenceReject, failure.ReferenceMismatch
	}
	return referenceApply, ""
}

// referenceAgrees reports whether an operation and its reference describe the
// same piece of business.
func referenceAgrees(cmd Command, w *Wallet, target *WagerTransaction) bool {
	if target.WalletID() != w.ID() || target.PlayerID() != cmd.PlayerID {
		return false
	}
	if target.Currency() != cmd.Money.Currency() {
		return false
	}
	provider, ok := target.Provider()
	if !ok || provider != cmd.Provider {
		return false
	}
	round, ok := target.RoundID()
	if !ok || round != cmd.RoundID {
		return false
	}
	return true
}

// reversalDirection returns the way a reversal of target must move money: the
// opposite of however target moved it.
func reversalDirection(target Kind) (Direction, bool) {
	forward, ok := target.Direction()
	if !ok {
		return "", false
	}
	return forward.Opposite()
}
