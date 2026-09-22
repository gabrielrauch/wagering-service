package wagering

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// Outcome is everything one operation produced.
//
// Every outcome [Processor.Submit] or [Processor.Continue] returns with a nil
// error carries a Transaction: the operation was recorded, whatever became of
// it. LedgerEntry is present only when money actually moved — never for a loss,
// never for a rejection. Events are returned rather than buffered anywhere, so
// the caller holds the only copy and rehydration has nothing to emit.
//
// There is exactly one outcome with no transaction, and it does not come from
// the processor: [OpenWallet] returns the zero Outcome when a wallet is opened
// at zero, because an opening records a starting balance and a wallet opened at
// zero has none to record. That is the domain's rule rather than an omission —
// an opening exists only when a wallet is created with money in it — so the
// absence is reported through [Outcome.Recorded] rather than by a nil check the
// caller has to know to write.
type Outcome struct {
	Transaction *WagerTransaction
	LedgerEntry *WalletLedgerEntry
	Events      []Event
}

// Recorded reports whether the outcome carries a transaction.
//
// It is true for everything the processor returns successfully and false only
// for a wallet opened at zero, which records no opening. Callers that walk an
// outcome should branch on it rather than dereference Transaction, which is the
// one nil an otherwise successful result can hold.
func (o Outcome) Recorded() bool { return o.Transaction != nil }

// Processor applies operations to wallets.
//
// It is stateless apart from the policy it was built with: everything else
// arrives as an argument and leaves as a result. It performs no I/O, opens no
// transaction and knows of no repository — it decides, and the caller persists.
//
// It composes the three things that own the rules rather than owning them
// itself: [Wallet] keeps the balance invariants, [WagerTransaction] keeps its
// own state machine, and the reference rules live with [ReferenceView].
//
// Its zero value is unusable. [NewProcessor] is the only way to build one that
// works, and every entry point checks, so a processor that was never given a
// wait budget refuses the operation outright instead of settling it wrongly.
type Processor struct {
	policy ReferencePolicy
}

// NewProcessor builds a processor with the wait budget it should apply.
func NewProcessor(policy ReferencePolicy) (*Processor, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return &Processor{policy: policy}, nil
}

// Policy returns the wait budget the processor applies.
func (p *Processor) Policy() ReferencePolicy { return p.policy }

// usable reports whether the processor was built with a meaningful wait budget.
//
// It is checked before a transaction exists, so a processor that was never
// built reports a malformed submission — nothing is recorded and the
// idempotency key stays free — rather than reaching [Processor.wait] and
// settling the operation against a budget that was never set.
func (p *Processor) usable() error {
	if err := p.policy.validate(); err != nil {
		return failure.Wrap(err, failure.UninitializedValue,
			"processor has no wait budget; build it with NewProcessor")
	}
	return nil
}

// Submit records a newly arrived operation and applies it to a wallet.
//
// The reference view is nil when the command names no reference, and carries a
// nil transaction when the reference was named but not found.
//
// The error and the outcome mean different things, and the distinction is the
// whole error model:
//
//   - A non-nil error means the submission was malformed and no transaction was
//     recorded. The idempotency key is untouched, so a corrected payload may be
//     resubmitted under it.
//   - A nil error means an [Outcome] exists, carrying a transaction that reached
//     [Processed], [Rejected] or [PendingReference]. A rejection is a business
//     fact, not an error: it is persisted, it emits an event, and it binds the
//     idempotency key to its payload for good.
//
// An operation that did not settle is carried forward with [Processor.Continue],
// never by submitting it again.
func (p *Processor) Submit(cmd Command, w *Wallet, ref *ReferenceView, now time.Time) (Outcome, error) {
	if err := p.usable(); err != nil {
		return Outcome{}, err
	}
	if w == nil {
		return Outcome{}, failure.New(failure.UninitializedValue, "wallet must be present").WithField("wallet")
	}
	if err := WalletBelongsToPlayer(cmd, w); err != nil {
		return Outcome{}, err
	}
	tx, err := NewExternalTransaction(cmd, w.ID(), now)
	if err != nil {
		return Outcome{}, err
	}
	return p.apply(tx, cmd, w, ref, now)
}

// Continue carries an unsettled operation forward.
//
// Retrying is not resubmitting. The operation already exists, and its wait
// budget is recorded on it, so continuing must carry on from that transaction
// rather than start a new one — otherwise the attempt count would reset on every
// retry and the operation would wait for ever.
//
// Two statuses can be carried forward, and they arise differently:
//
//   - [PendingReference] is an operation parked because the transaction it
//     depends on had not arrived. Continuing is the retry the wait budget counts.
//   - [Pending] is an operation recorded but never applied — a submission whose
//     idempotency key was claimed before processing, then orphaned. Without a way
//     forward it would be found by every later lookup, answer [Pending] for ever
//     and never settle.
//
// The transaction and the command must describe the same operation, which is
// checked against the payload hash rather than taken on trust.
func (p *Processor) Continue(tx *WagerTransaction, cmd Command, w *Wallet, ref *ReferenceView, now time.Time) (Outcome, error) {
	if err := p.assertContinuable(tx, cmd, w); err != nil {
		return Outcome{}, err
	}
	return p.apply(tx, cmd, w, ref, now)
}

// assertContinuable reports whether this transaction, command and wallet
// describe one and the same unsettled operation.
//
// The checks run in this order deliberately: the processor's own wait budget
// first, then the arguments a caller supplied, then the status, and only last
// the payload hash — so the error a caller sees names the outermost thing that
// is wrong rather than an inner consequence of it.
func (p *Processor) assertContinuable(tx *WagerTransaction, cmd Command, w *Wallet) error {
	if err := p.usable(); err != nil {
		return err
	}
	if w == nil {
		return failure.New(failure.UninitializedValue, "wallet must be present").WithField("wallet")
	}
	if tx == nil {
		return failure.New(failure.UninitializedValue, "transaction must be present").WithField("transaction")
	}
	switch status := tx.Status(); {
	case status.IsTerminal():
		return failure.New(failure.InvalidStateTransition,
			"%s is settled and cannot be carried forward", status)
	case status != Pending && status != PendingReference:
		return failure.New(failure.InvalidStateTransition,
			"only an unsettled operation can be carried forward, this one is %s", status)
	}
	if tx.ID() != cmd.TransactionID {
		return failure.New(failure.ReferenceMismatch,
			"the command describes transaction %s, not %s", cmd.TransactionID, tx.ID()).WithField("transactionId")
	}
	if tx.WalletID() != w.ID() {
		return failure.New(failure.ReferenceMismatch,
			"the transaction belongs to wallet %s, not %s", tx.WalletID(), w.ID()).WithField("walletId")
	}
	if err := WalletBelongsToPlayer(cmd, w); err != nil {
		return err
	}
	hash, err := cmd.PayloadHash()
	if err != nil {
		return err
	}
	return tx.AssertSamePayload(hash)
}

// apply runs the rules against a transaction that already exists, whether it
// was created a moment ago or parked since an earlier attempt.
func (p *Processor) apply(tx *WagerTransaction, cmd Command, w *Wallet, ref *ReferenceView, now time.Time) (Outcome, error) {
	// From here a transaction exists, so every refusal is a settled outcome
	// rather than an error: it is recorded, reported and auditable. The checks
	// that belong to the caller rather than to the provider — a missing wallet,
	// a wallet held by another player — have already run, before there was
	// anything to settle.
	if cmd.Money.Currency() != w.Currency() {
		return reject(tx, failure.CurrencyMismatch, now)
	}

	switch outcome, code := evaluateReference(cmd, w, ref); outcome {
	case referenceReject:
		return reject(tx, code, now)
	case referenceWait:
		return p.wait(tx, now)
	}

	// Only an operation that names a reference has one to resolve. A view
	// supplied for an operation that names none says nothing about it and is
	// ignored, the same way evaluateReference ignored it a moment ago: a caller
	// that resolves references uniformly must not turn a bet into a failure by
	// handing over state the bet never asked for.
	if cmd.namesReference() && ref.Found() {
		if err := tx.ResolveReference(ref.Transaction.ID()); err != nil {
			return Outcome{}, err
		}
	}

	direction, moves, err := movement(cmd, ref)
	if err != nil {
		return Outcome{}, err
	}

	// A loss completes without touching the balance: no ledger entry, no
	// version change, and no WalletBalanceChanged. Everything else moves money
	// first and then settles the same way, so the entry is the only difference
	// between the two and the settling tail below is written once.
	var moved *WalletLedgerEntry
	if moves {
		entry, err := w.move(direction, Movement{
			EntryID:       cmd.LedgerEntryID,
			TransactionID: cmd.TransactionID,
			Amount:        cmd.Money,
			At:            now,
		})
		if err != nil {
			if code, settles := movementRejection(cmd, err); settles {
				return reject(tx, code, now)
			}
			return Outcome{}, err
		}
		moved = &entry
	}

	if err := tx.MarkProcessed(w.Balance(), now); err != nil {
		return Outcome{}, err
	}
	processed, err := NewWagerTransactionProcessed(tx)
	if err != nil {
		return Outcome{}, err
	}
	// An operation that completes emits the transaction event, and the balance
	// event as well when money moved. Two is the most there can ever be, so the
	// slice is sized for both at once rather than grown the moment the second
	// one arrives — which, for every kind but a loss, is always.
	events := make([]Event, 0, 2)
	events = append(events, processed)
	if moved != nil {
		changed, err := NewWalletBalanceChanged(*moved)
		if err != nil {
			return Outcome{}, err
		}
		events = append(events, changed)
	}
	return Outcome{Transaction: tx, LedgerEntry: moved, Events: events}, nil
}

// wait parks a transaction until its reference arrives, or settles it when the
// budget for waiting is spent.
func (p *Processor) wait(tx *WagerTransaction, now time.Time) (Outcome, error) {
	spent, err := tx.ReferenceBudgetExhausted(p.policy, now)
	if err != nil {
		return Outcome{}, err
	}
	if spent {
		return reject(tx, failure.ReferenceNotFound, now)
	}
	if err := tx.MarkPendingReference(p.policy, now); err != nil {
		return Outcome{}, err
	}
	pending, err := NewWagerTransactionPendingReference(tx)
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Transaction: tx, Events: []Event{pending}}, nil
}

// WalletBelongsToPlayer checks that the wallet handed in belongs to the player
// the command names.
//
// It is checked before a transaction exists, and it is exported so that the
// application layer can ask it before recording anything. The command names a
// wallet and a player, and the wallet is loaded by its id; a wallet held by
// another player is therefore a submission addressing somebody else's wallet,
// which is not an operation a provider can perform but a payload to repair.
//
// Settling it as a rejection instead would persist a transaction and bind the
// provider's idempotency key to it permanently, with no way for them to get
// that key back once the payload was corrected. Refusing outright records
// nothing and leaves the key free.
//
// The code is correctable, which is the half of this that is easy to get wrong.
// [failure.Correctable] means precisely "nothing was persisted and the key may
// be reused", and both are true here — so reporting a definitive code would
// tell a caller mapping codes to responses that the key is spent, which is the
// very outcome this check exists to prevent. It follows [NewProcessor], which
// reports a caller's bad argument the same way.
//
// Currency is deliberately not checked here. A wallet holds one currency, so a
// provider betting in another against it is a real business outcome — the
// wallet exists, it is the player's, and the money is the wrong kind — and
// [Processor.apply] settles it as one, under CurrencyMismatch.
func WalletBelongsToPlayer(cmd Command, w *Wallet) error {
	if w.PlayerID() != cmd.PlayerID {
		// The owner is deliberately not named: this message can reach the
		// provider that sent the wrong identifier, and whose wallet it is
		// belongs to nobody but the service.
		return failure.New(failure.InvalidFieldFormat,
			"wallet %s is not held by player %q",
			w.ID(), cmd.PlayerID).WithField("walletId")
	}
	return nil
}

// movementRejection maps a movement the wallet refused onto the code that
// settles the operation, and reports false when the refusal is not one that
// settles it.
//
// A limit on what the wallet can hold settles the operation. The transaction
// already exists — on the Continue path it is already in storage with its
// idempotency key bound — so letting one escape as an error would tell the
// caller nothing was recorded and invite a resubmission that can only conflict.
//
// The wallet is left untouched either way: it computes the new balance and
// builds the entry before it moves anything, so a movement it refuses did not
// happen.
func movementRejection(cmd Command, err error) (failure.Code, bool) {
	switch {
	case failure.Is(err, failure.InsufficientFunds):
		// A reversal that cannot be applied means money has already left the
		// wallet and will not come back on its own. It is reported under its own
		// code so it can be told apart from a player simply betting too much.
		if cmd.Kind.IsReversal() {
			return failure.ReversalInsufficientFunds, true
		}
		return failure.InsufficientFunds, true
	case failure.Is(err, failure.AmountOutOfRange):
		// The arithmetic overflowed, so the wallet cannot hold the result.
		// Reported under a definitive code of its own because AmountOutOfRange is
		// correctable, and this is not: the amount is well formed and the balance
		// is what it is.
		return failure.BalanceOutOfRange, true
	}
	return "", false
}

// movement returns the direction an operation moves money, and whether it moves
// any at all.
func movement(cmd Command, ref *ReferenceView) (Direction, bool, error) {
	if !cmd.Kind.MovesMoney() {
		return "", false, nil
	}
	if cmd.Kind == Rollback {
		// A rollback has no direction of its own; it takes the opposite of
		// whatever it undoes.
		if !ref.Found() {
			return "", false, failure.New(failure.ReferenceRequired,
				"a %s takes its direction from the transaction it undoes", cmd.Kind)
		}
		direction, ok := reversalDirection(ref.Transaction.Kind())
		if !ok {
			return "", false, failure.New(failure.ReferenceNotReversible,
				"a %s cannot be undone", ref.Transaction.Kind())
		}
		return direction, true, nil
	}
	direction, ok := cmd.Kind.Direction()
	if !ok {
		return "", false, failure.New(failure.InvalidFieldFormat,
			"a %s has no movement direction", cmd.Kind).WithField("kind")
	}
	return direction, true, nil
}

// reject settles a transaction against a business rule and reports it.
func reject(tx *WagerTransaction, code failure.Code, now time.Time) (Outcome, error) {
	if err := tx.Reject(code, now); err != nil {
		return Outcome{}, err
	}
	rejected, err := NewWagerTransactionRejected(tx)
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Transaction: tx, Events: []Event{rejected}}, nil
}
