package app

import (
	"context"
	"errors"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// Wagering is the write path for operations a provider submits, and the worker
// door that carries parked ones forward.
//
// One service rather than one type per use case: Submit and Resume are the same
// processing path reached from two directions, and splitting them would mean
// duplicating the part that decides what an operation came to.
type Wagering struct {
	tx        TxManager
	processor *wagering.Processor
	clock     Clock
	ids       IDs
	backoff   BackoffPolicy
	defects   DefectObserver
}

// WageringDeps is everything the write path is built from.
//
// A struct rather than six positional parameters, so that a dependency is named
// at the call site rather than counted.
type WageringDeps struct {
	Tx        TxManager
	Processor *wagering.Processor
	Clock     Clock
	IDs       IDs
	Backoff   BackoffPolicy
	Defects   DefectObserver
}

// NewWagering wires the write path.
//
// Every dependency is refused when nil, because a service that was built without
// one would fail at the first command instead of at start-up, and by then a
// provider is waiting.
//
// Defects is refused with the rest, and it did not used to be. It is the only
// channel a failure to carry a parked operation forward has: that path returns a
// nil error by design, because the row stays parked and the provider is not
// waiting on it, so an absent observer does not degrade the report — it deletes
// it. The condition then stays invisible until the wait budget runs out and the
// operation is rejected for a reason that was never true, which is the exact
// outcome DefectObserver is documented as existing to prevent. An adapter that
// genuinely wants no hook passes one that discards, and says so in its own code.
func NewWagering(d WageringDeps) (*Wagering, error) {
	switch {
	case d.Tx == nil:
		return nil, defect("wagering needs a transaction manager")
	case d.Processor == nil:
		return nil, defect("wagering needs a processor")
	case d.Clock == nil:
		return nil, defect("wagering needs a clock")
	case d.IDs == nil:
		return nil, defect("wagering needs an identifier source")
	case d.Defects == nil:
		return nil, defect("wagering needs a defect observer")
	}
	if err := d.Backoff.validate(); err != nil {
		return nil, err
	}
	return &Wagering{
		tx:        d.Tx,
		processor: d.Processor,
		clock:     d.Clock,
		ids:       d.IDs,
		backoff:   d.Backoff,
		defects:   d.Defects,
	}, nil
}

// Submit records one operation and applies it, idempotently.
func (w *Wagering) Submit(ctx context.Context, cmd SubmitOperation) (OperationResult, error) {
	provider, err := wagering.NewProvider(cmd.Fields.Provider)
	if err != nil {
		return OperationResult{}, classify(err)
	}
	// Before anything else, and in particular before any I/O: an unauthorized
	// submission must leave no trace, and the cheapest way to guarantee that is
	// for there to be nothing to undo.
	if err := cmd.Principal.MaySubmitAs(provider); err != nil {
		return OperationResult{}, err
	}
	if cmd.Correlation == "" {
		return OperationResult{}, invalidField("correlationId", failure.MissingRequiredField,
			"a submission needs a correlation id")
	}

	// Parsed once, here, so that HTTP and SQS cannot disagree about what the
	// submission said. The hash is taken over the parsed values, so two
	// transports parsing differently would decide differently about whether a
	// submission is a retry.
	command, err := w.command(cmd.Fields, provider)
	if err != nil {
		return OperationResult{}, err
	}
	hash, err := command.PayloadHash()
	if err != nil {
		return OperationResult{}, classify(err)
	}

	result, err := w.submitOnce(ctx, cmd, command, hash)
	switch {
	case errors.Is(err, ErrDuplicateSubmission):
		// Somebody else claimed one of the two keys between the read and the
		// insert. The transaction is dead, so the winner can only be read in a
		// new one — see resolveDuplicate for why this is not a retry.
		return w.resolveDuplicate(ctx, command, hash)
	case errors.Is(err, ErrReferenceAlreadyReversed):
		// Unreachable by design: the reference view is built under the wallet
		// lock and the domain rejects a held reference properly, with a row and
		// an event. Arriving here means the view was built wrongly, so the rule
		// now lives only in the schema.
		//
		// Reported by returning it and not also through DefectObserver. That hook
		// is for the resume path, where nothing is returned to anybody and the
		// report is the only trace; here the caller is handed a classified error
		// and is the one handling it. Calling both would log the same failure
		// twice, and would call CannotCarryForward for a live submission that was
		// never parked, with a transaction id naming a row the rollback removed.
		return OperationResult{}, conflict(failure.ReferenceAlreadyReversed, err,
			"reference %q already carries an active reversal", command.ReferenceExternalTransactionID)
	case err != nil:
		return OperationResult{}, classify(err)
	}
	return result, nil
}

// command turns the submitted strings into the domain's values, minting the
// identifiers this system owns.
//
// A ledger entry id is minted only for a kind that moves money. Minting one for
// a loss would hand the domain an identifier for an entry it is never going to
// write, and Command.Validate refuses exactly that.
func (w *Wagering) command(f OperationFields, provider wagering.Provider) (wagering.Command, error) {
	kind, err := wagering.ParseKind(f.Kind)
	if err != nil {
		return wagering.Command{}, classify(err)
	}
	external, err := wagering.NewExternalTransactionID(f.ExternalTransactionID)
	if err != nil {
		return wagering.Command{}, classify(err)
	}
	key, err := wagering.NewIdempotencyKey(f.IdempotencyKey)
	if err != nil {
		return wagering.Command{}, classify(err)
	}
	player, err := wagering.NewPlayerID(f.PlayerID)
	if err != nil {
		return wagering.Command{}, classify(err)
	}
	round, err := wagering.NewRoundID(f.RoundID)
	if err != nil {
		return wagering.Command{}, classify(err)
	}
	game, err := wagering.NewGameID(f.GameID)
	if err != nil {
		return wagering.Command{}, classify(err)
	}
	amount, err := money.Parse(f.Amount, f.Currency)
	if err != nil {
		return wagering.Command{}, classify(err)
	}

	command := wagering.Command{
		TransactionID:         w.ids.TransactionID(),
		Provider:              provider,
		ExternalTransactionID: external,
		IdempotencyKey:        key,
		PlayerID:              player,
		RoundID:               round,
		GameID:                game,
		Kind:                  kind,
		Money:                 amount,
	}
	if kind.MovesMoney() {
		command.LedgerEntryID = w.ids.LedgerEntryID()
	}
	if f.ReferenceExternalTransactionID != "" {
		reference, err := wagering.NewExternalTransactionID(f.ReferenceExternalTransactionID)
		if err != nil {
			return wagering.Command{}, classify(err)
		}
		command.ReferenceExternalTransactionID = reference
	}
	return command, nil
}

// submitOnce is the command, in one transaction. The order of the statements is
// the lock order documented on Repos, and is not free to change.
func (w *Wagering) submitOnce(
	ctx context.Context,
	cmd SubmitOperation,
	command wagering.Command,
	hash wagering.PayloadHash,
) (OperationResult, error) {
	var result OperationResult
	err := w.tx.WithinMovement(ctx, func(ctx context.Context, r *Repos) error {
		// The inbox row is claimed first, so that two consumers racing on one
		// message collide here rather than after one of them has taken a wallet
		// lock. Its timestamp is read before the wallet lock because the inbox
		// has no ordering to keep with the wallet; everything the wallet touches
		// is stamped with the single `now` taken under the lock below.
		if cmd.Inbox != nil {
			handled, err := r.Inbox.Find(ctx, cmd.Inbox.Key())
			if err != nil {
				return err
			}
			switch {
			case handled == nil:
				if err := r.Inbox.Record(ctx, *cmd.Inbox, w.clock.Now()); err != nil {
					return err
				}
			case handled.BodyHash != cmd.Inbox.BodyHash:
				// One message id, two bodies. Nothing can decide which is real,
				// and sending it again will not change that — which is what
				// defect already says, so there is no AsUnretryable around it
				// to say it twice. AsUnretryable would now return this
				// untouched anyway; the call was redundant rather than wrong.
				return defect("message %q was handled with a different body", cmd.Inbox.MessageID)
			}
			// Already handled with the same body falls through: the settled
			// result is read below, and reading it is the whole of the work.
		}

		settled, err := w.replay(ctx, r.Transactions, command, hash)
		if err != nil {
			return err
		}
		if settled != nil {
			result = *settled
			return nil
		}

		wallet, err := r.Wallets.LockForMovement(ctx, wagering.WalletKey{
			PlayerID: command.PlayerID,
			Currency: command.Money.Currency(),
		})
		if err != nil {
			return err
		}
		if wallet == nil {
			// This layer never opens a wallet on a provider's behalf. Nothing is
			// persisted, so the same submission succeeds under the same key once
			// the wallet exists.
			return notFound("player %q holds no %s wallet", command.PlayerID, command.Money.Currency())
		}

		// Sampled here, under the lock, rather than on the way in: a command that
		// queued behind another must not stamp the wallet with a time older than
		// the one that went first. One value for the row, the entry, the wallet
		// and the envelopes.
		now := w.clock.Now()

		tx, err := wagering.NewExternalTransaction(command, wallet.ID(), now)
		if err != nil {
			return classify(err)
		}
		// Recorded before any money work, which is what makes the key the thing
		// a duplicate loses on rather than the balance.
		if err := r.Transactions.Record(ctx, tx, cmd.Correlation); err != nil {
			return err
		}

		var reference *wagering.ReferenceView
		if command.ReferenceExternalTransactionID != "" {
			reference, err = r.Transactions.ReferenceFor(ctx, command.Provider, command.ReferenceExternalTransactionID)
			if err != nil {
				return err
			}
		}

		// Continue, not Submit: the row already exists, claimed a moment ago, and
		// this is the domain's door for an operation recorded but not yet applied.
		outcome, err := w.processor.Continue(tx, command, wallet, reference, now)
		if err != nil {
			return classify(err)
		}

		if _, _, err := w.commitOutcome(ctx, outcome, commitScope{
			Repos:  r,
			Wallet: wallet,
			Now:    now,
			Trace:  Trace{Correlation: cmd.Correlation, Causation: causationOf(cmd)},
		}); err != nil {
			return err
		}
		result = resultOf(outcome.Transaction, false)
		return nil
	})
	if err != nil {
		return OperationResult{}, err
	}
	return result, nil
}

// replay reports the result an operation already reached, when this submission
// is one that has been seen before.
//
// Both keys are checked, and in this order. The idempotency key answers "is this
// the same submission?", which has a real answer — replay or conflict — decided
// by the payload hash. The provider's external id answers "has this operation
// already been submitted under a different key?", which is only ever a conflict,
// and carries no failure code because nothing is persisted for it: the catalogue
// describes outcomes a provider can act on, and this is not one.
//
// # Two lookups, and on the write path two instants
//
// [TxManager.WithinMovement] is READ COMMITTED, so these two statements do not
// share a snapshot, and a submission racing its own twin can fall between them:
// the first lookup runs before the winner commits and finds nothing under the
// key, the second runs after it commits and finds the winner under the external
// id. Read without looking at what was found, that is "this operation is already
// recorded under another idempotency key" — which is false, because it is
// recorded under exactly this one, and is the wrong category besides. Conflict
// promises that the same submission sent again unchanged will be refused again,
// and this one sent again is a replay. The provider is told to stop retrying
// something that would have succeeded.
//
// The window is closed by recognising the row rather than by widening the
// transaction. A row found by external id carries the key it was recorded
// under, so a submission can tell its own row from somebody else's and take the
// branch the first lookup would have taken. Raising the isolation level would
// close it too, and would do it by changing the concurrency design that ADR-0011
// and the wallet lock are built around — a large answer to a window that costs
// nothing once the row is read for what it is. This layer should not need the
// isolation to be stronger than the port promises.
func (w *Wagering) replay(
	ctx context.Context,
	store TransactionReader,
	command wagering.Command,
	hash wagering.PayloadHash,
) (*OperationResult, error) {
	existing, err := store.ByIdempotencyKey(ctx, command.Provider, command.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return replayOf(existing, command, hash)
	}

	other, err := store.ByExternal(ctx, command.Provider, command.ExternalTransactionID)
	if err != nil {
		return nil, err
	}
	if other == nil {
		return nil, nil
	}
	// The key is read off the row rather than inferred from which lookup found
	// it. Equal means this is the submission's own row, seen across the window
	// above, and it is answered exactly as the first lookup would have answered
	// it. Only a different key is the conflict the message below describes.
	//
	// A row carrying no key at all is neither case and needs no branch of its
	// own: only an opening has none, an opening has no provider and no external
	// id, and this lookup is scoped by both, so it cannot be what was found.
	if key, ok := other.Transaction.IdempotencyKey(); ok && key == command.IdempotencyKey {
		return replayOf(other, command, hash)
	}
	return nil, conflict("", nil,
		"operation %q is already recorded under another idempotency key",
		command.ExternalTransactionID)
}

// replayOf decides what a row recorded under this submission's own idempotency
// key means for it: a replay when it carries the same payload, and a conflict
// when the key has been bound to a different one.
//
// It is reached from both lookups in [Wagering.replay] and gives one answer to
// both, which is the point: which index found the row is an accident of timing,
// and a submission must not learn a different outcome from it.
func replayOf(
	stored *StoredTransaction,
	command wagering.Command,
	hash wagering.PayloadHash,
) (*OperationResult, error) {
	if err := stored.Transaction.AssertSamePayload(hash); err != nil {
		// The code is read off the refusal rather than assumed, because
		// AssertSamePayload has a second one: a transaction carrying no
		// payload hash at all. Only an opening has none, an opening has no
		// provider, and both lookups are scoped by provider — so that answer
		// is the store returning a row it was not asked for, which is this
		// system's defect and not a conflict to report to a provider.
		if !failure.Is(err, failure.IdempotencyPayloadConflict) {
			// Carrying the refusal rather than only naming the condition:
			// this branch is reached when the assumption behind it is
			// already wrong, so the one thing worth keeping is what the
			// domain actually said.
			return nil, newError(Unretryable, "", err,
				"operation %s answered a provider-scoped lookup with no payload hash",
				stored.Transaction.ID())
		}
		return nil, conflict(failure.IdempotencyPayloadConflict, err,
			"idempotency key %q is already bound to another operation", command.IdempotencyKey)
	}
	// A still-parked original is replayed as it stands and deliberately not
	// carried forward here: continuing is the worker's job, and doing it on a
	// provider's retry would spend the wait budget twice as fast as the policy
	// says.
	settled := resultOf(stored.Transaction, true)
	return &settled, nil
}

// resolveDuplicate decides what a submission that lost a race actually was.
//
// It is the one place this layer runs a second transaction, and it is a
// re-classification rather than a retry: the work is not attempted again, the
// winner is read and reported. A second transaction is unavoidable — the first
// was aborted by the unique violation, and nothing can be read in an aborted
// transaction.
//
// It re-runs the whole of the idempotency decision rather than branching on
// which constraint was reported. A true replay collides on both keys at once and
// a database names only one of them, so the name says nothing about which case
// this is.
func (w *Wagering) resolveDuplicate(
	ctx context.Context,
	command wagering.Command,
	hash wagering.PayloadHash,
) (OperationResult, error) {
	var result OperationResult
	err := w.tx.WithinSnapshot(ctx, func(ctx context.Context, r *ReadRepos) error {
		settled, err := w.replay(ctx, r.Transactions, command, hash)
		if err != nil {
			return err
		}
		if settled == nil {
			return defect("submission %q lost a duplicate race but matched nothing on re-read",
				command.ExternalTransactionID)
		}
		result = *settled
		return nil
	})
	if err != nil {
		return OperationResult{}, classify(err)
	}
	return result, nil
}

// commitScope is the ambient state one commit writes within: the repositories it
// writes through, the wallet it is for, the instant everything it touches is
// stamped with, and the trace its envelopes carry.
//
// Deliberately not named for a movement: CONTEXT.md reserves that word for a
// balance change a wallet is asked to make, and this package already uses it for
// the read-write transaction, which is what Repos is.
type commitScope struct {
	Repos  *Repos
	Wallet *wagering.Wallet
	Now    time.Time
	Trace  Trace
}

// commitOutcome writes what an outcome produced, in the lock order: the
// transaction and its wallet, then the operations waiting on it, then the
// outbox. Appending to the outbox is always last, because the outbox keeps a
// per-wallet sequence counter and taking that before the wallet is how two
// commands deadlock.
func (w *Wagering) commitOutcome(
	ctx context.Context,
	outcome wagering.Outcome,
	c commitScope,
) (woke int, nextAttemptAt time.Time, err error) {
	tx := outcome.Transaction

	if tx.Status() == wagering.PendingReference {
		deadline, _ := tx.ReferenceDeadline()
		// The attempt count and the deadline are the domain's and are written
		// exactly as it left them: MarkPendingReference already counted this
		// attempt. Only the schedule is ours.
		nextAttemptAt = w.backoff.next(tx.ReferenceAttempts(), c.Now, deadline, tx.ID())
	}
	if err := c.Repos.Settle(ctx, outcome, nextAttemptAt); err != nil {
		return 0, time.Time{}, err
	}

	if tx.Status() == wagering.Processed {
		if external, ok := tx.ExternalTransactionID(); ok {
			provider, _ := tx.Provider()
			// In this commit, not in a later one: an operation that becomes
			// available and the waiters that become due on it must be one fact.
			woke, err = c.Repos.Transactions.MakeDue(ctx, SettledOperation{
				WalletID: c.Wallet.ID(),
				Provider: provider,
				External: external,
			}, c.Now)
			if err != nil {
				return 0, time.Time{}, err
			}
		}
	}

	if err := emit(ctx, c.Repos.Outbox, w.ids, outcome.Events, c.Trace, c.Now); err != nil {
		return 0, time.Time{}, err
	}
	return woke, nextAttemptAt, nil
}

// carriedForwardDefect is a defect found while carrying a parked operation
// forward, held until the transaction has closed.
//
// It is recorded rather than reported on the spot because the spot is inside the
// movement transaction, with the wallet row locked. Reporting from there would
// run an adapter's code — a metrics push, an error tracker, whatever it turns
// out to be — while this transaction holds a lock every other command on that
// wallet is queued behind, which makes the length of the lock somebody else's
// decision. It would also announce a write that has not committed and may yet
// roll back. Wallets.Reconcile already calls its observer after its transaction
// closes, for the first of those reasons; this is the same rule, on the path
// where a lock rather than a snapshot is at stake.
type carriedForwardDefect struct {
	id    wagering.TransactionID
	cause error
}

func (w *Wagering) reportDefect(ctx context.Context, d carriedForwardDefect) {
	if d.cause != nil {
		w.defects.CannotCarryForward(ctx, d.id, d.cause)
	}
}

// Resume carries one due parked operation forward.
func (w *Wagering) Resume(ctx context.Context, principal Principal) (ResumeOutcome, error) {
	if err := principal.MayResume(); err != nil {
		return ResumeOutcome{}, err
	}
	var (
		out ResumeOutcome
		// Filled by cannotCarryForward, reported once the transaction has
		// closed and the wallet lock is gone.
		found carriedForwardDefect
	)
	err := w.tx.WithinMovement(ctx, func(ctx context.Context, r *Repos) error {
		// Both accumulators are reset, not just declared outside. A manager that
		// ran the callback twice would otherwise report a defect from an attempt
		// that was rolled back, or carry a stale Claimed into a turn that
		// claimed nothing.
		out, found = ResumeOutcome{}, carriedForwardDefect{}
		now := w.clock.Now()

		// A plain SELECT, taking no lock. Claiming the row here and then wanting
		// its wallet closes a cycle with a submission holding that wallet and
		// wanting this row — and because a reversal must agree with its reference
		// on the wallet, the two are the same wallet by construction rather than
		// by coincidence.
		candidate, err := r.Transactions.NextDue(ctx, now)
		if err != nil {
			return err
		}
		if candidate == nil {
			return nil
		}

		// Wallet first, then the row. This is the same order every movement takes.
		wallet, err := r.Wallets.LockByID(ctx, candidate.WalletID)
		if err != nil {
			return err
		}
		if wallet == nil {
			return defect("operation %s names wallet %s, which does not exist",
				candidate.TransactionID, candidate.WalletID)
		}

		// Re-read under the lock and re-assert that it is still parked and still
		// due. Between the unlocked SELECT and here, another worker may have
		// taken it, or the operation it waits on may have arrived and settled it.
		stored, err := r.Transactions.ClaimForUpdate(ctx, candidate.TransactionID, now)
		if err != nil {
			return err
		}
		if stored == nil {
			// Somebody else got there first, or it settled. Neither is a failure,
			// and a worker that treated it as one would alert on its own normal
			// operation.
			return nil
		}
		out.Claimed = true
		tx := stored.Transaction

		command, err := w.rebuild(tx)
		if err != nil {
			return w.cannotCarryForward(ctx, resumeAttempt{Repos: r, Tx: tx, Now: now}, err, &out, &found)
		}

		var reference *wagering.ReferenceView
		if command.ReferenceExternalTransactionID != "" {
			reference, err = r.Transactions.ReferenceFor(ctx, command.Provider, command.ReferenceExternalTransactionID)
			if err != nil {
				return err
			}
		}

		outcome, err := w.processor.Continue(tx, command, wallet, reference, now)
		if err != nil {
			// Continue's contract — nothing was recorded, the key is free — is
			// written for a submission and is false here: the row exists and a
			// provider was told days ago that it was accepted. An error here is
			// this system failing to rebuild its own operation, so it must not be
			// reported to anybody as a business outcome.
			return w.cannotCarryForward(ctx, resumeAttempt{Repos: r, Tx: tx, Now: now}, err, &out, &found)
		}

		correlation := stored.Correlation
		if correlation == "" {
			// Every envelope needs a trace. A row with none is old or was written
			// by something that did not set it, and the operation's own id is the
			// one value guaranteed to identify it.
			correlation = tx.ID().String()
		}
		// Causation is empty: a resumed operation has no causing event with an
		// identity. The message that first carried it has its own inbox row.
		woke, nextAttemptAt, err := w.commitOutcome(ctx, outcome, commitScope{
			Repos:  r,
			Wallet: wallet,
			Now:    now,
			Trace:  Trace{Correlation: correlation},
		})
		if err != nil {
			return err
		}
		out.Result = resultOf(outcome.Transaction, false)
		out.Woke = woke
		if !nextAttemptAt.IsZero() {
			out.Rescheduled = true
			out.NextAttemptAt = nextAttemptAt
		}
		return nil
	})
	// Reported here, with the transaction closed and the wallet lock released,
	// and before the error check: a defect found on this turn happened whether
	// or not the write recording it committed, and it is the one report nobody
	// else is going to make.
	w.reportDefect(ctx, found)
	if err != nil {
		return ResumeOutcome{}, classify(err)
	}
	return out, nil
}

// rebuild reconstructs the command a parked operation was submitted with.
//
// Every field comes from the stored row. The idempotency key especially: it is
// excluded from the canonical payload and is never compared by the domain's
// continuation checks, so it is the one field a faulty reconstruction could get
// wrong without anything noticing. What catches it is the settle write, which
// restates the whole immutable tuple and is refused by the guard if any of it
// disagrees — the reconstruction is checked by being written back.
//
// The ledger entry id is fresh. The earlier attempt wrote no entry, so there is
// none to reuse, and reusing one would collide on the ledger's primary key.
func (w *Wagering) rebuild(tx *wagering.WagerTransaction) (wagering.Command, error) {
	if !tx.IsExternal() {
		return wagering.Command{}, defect("operation %s has no provider side to rebuild", tx.ID())
	}
	provider, _ := tx.Provider()
	external, _ := tx.ExternalTransactionID()
	key, _ := tx.IdempotencyKey()
	round, _ := tx.RoundID()
	game, _ := tx.GameID()
	reference, _ := tx.ReferenceExternalTransactionID()

	command := wagering.Command{
		TransactionID:                  tx.ID(),
		Provider:                       provider,
		ExternalTransactionID:          external,
		IdempotencyKey:                 key,
		PlayerID:                       tx.PlayerID(),
		RoundID:                        round,
		GameID:                         game,
		Kind:                           tx.Kind(),
		Money:                          tx.Money(),
		ReferenceExternalTransactionID: reference,
	}
	if tx.Kind().MovesMoney() {
		command.LedgerEntryID = w.ids.LedgerEntryID()
	}
	if err := command.Validate(); err != nil {
		return wagering.Command{}, err
	}
	return command, nil
}

// resumeAttempt is the parked operation one resume turn is carrying forward: the
// repositories it writes through, the row itself, and the instant the turn is
// stamped with.
type resumeAttempt struct {
	Repos *Repos
	Tx    *wagering.WagerTransaction
	Now   time.Time
}

// cannotCarryForward handles this system failing to move a parked operation.
//
// It fails the operation only once the deadline has passed. FAILED is terminal,
// carries no code and no balance, and emits no event, so a provider that asks
// later is told an operation exists and nothing about what became of it — that is
// a last resort, not the answer to a deployment that shipped a broken rebuild for
// ten minutes.
//
// Short of the deadline the row stays parked, the next attempt is pushed out, and
// NO ATTEMPT IS COUNTED: nothing was spent, and counting would let a bug consume
// a budget that exists to bound how long a reference may take to arrive.
//
// The deadline is checked here rather than left to the domain because
// ReferenceBudgetExhausted is reached inside the processor's waiting path, and an
// operation that could not be rebuilt never gets that far.
func (w *Wagering) cannotCarryForward(
	ctx context.Context,
	a resumeAttempt,
	cause error,
	out *ResumeOutcome,
	found *carriedForwardDefect,
) error {
	tx := a.Tx
	// Recorded, not reported: the caller reports it once this transaction has
	// closed and the wallet lock is gone. See carriedForwardDefect.
	*found = carriedForwardDefect{id: tx.ID(), cause: cause}

	deadline, waiting := tx.ReferenceDeadline()
	if waiting && !a.Now.Before(deadline) {
		if err := tx.Fail(a.Now); err != nil {
			return classify(err)
		}
		if err := a.Repos.Transactions.Fail(ctx, tx); err != nil {
			return err
		}
		out.Result = resultOf(tx, false)
		return nil
	}

	at := w.backoff.next(tx.ReferenceAttempts(), a.Now, deadline, tx.ID())
	if err := a.Repos.Transactions.Reschedule(ctx, tx.ID(), at); err != nil {
		return err
	}
	out.Result = resultOf(tx, false)
	out.Rescheduled = true
	out.NextAttemptAt = at
	return nil
}

// TransactionByID reads one operation by this system's identifier for it.
func (w *Wagering) TransactionByID(
	ctx context.Context,
	principal Principal,
	id wagering.TransactionID,
) (OperationResult, error) {
	var result OperationResult
	err := w.tx.WithinSnapshot(ctx, func(ctx context.Context, r *ReadRepos) error {
		stored, err := r.Transactions.ByID(ctx, id)
		if err != nil {
			return err
		}
		return scopedResult(principal, stored, id.String(), &result)
	})
	if err != nil {
		return OperationResult{}, classify(err)
	}
	return result, nil
}

// TransactionByExternalID reads one operation by the provider's identifier for
// it.
//
// The provider is named rather than derived from the principal, because an
// external id identifies an operation only within the provider that issued it
// and the service reads every provider's. [Principal.MayReadAs] is what keeps
// that from widening the scope a provider has: it is answered before any row is
// looked for, so a provider naming somebody else is refused without this door
// ever having gone to see whether the operation exists.
func (w *Wagering) TransactionByExternalID(
	ctx context.Context,
	principal Principal,
	provider wagering.Provider,
	id wagering.ExternalTransactionID,
) (OperationResult, error) {
	if err := principal.MayReadAs(provider); err != nil {
		return OperationResult{}, err
	}
	var result OperationResult
	err := w.tx.WithinSnapshot(ctx, func(ctx context.Context, r *ReadRepos) error {
		stored, err := r.Transactions.ByExternal(ctx, provider, id)
		if err != nil {
			return err
		}
		return scopedResult(principal, stored, id.String(), &result)
	})
	if err != nil {
		return OperationResult{}, classify(err)
	}
	return result, nil
}

// scopedResult answers a read within what the principal may see.
//
// A provider asking about somebody else's operation is told NotFound, not
// Unauthorized. Unauthorized would confirm that the operation exists, and an
// existence oracle is the thing scoping a read is meant to deny — a provider
// could otherwise enumerate a competitor's transaction ids and learn which
// landed.
func scopedResult(principal Principal, stored *StoredTransaction, named string, out *OperationResult) error {
	if stored == nil {
		return notFound("no operation %q", named)
	}
	if provider, isProvider := principal.Provider(); isProvider {
		owner, external := stored.Transaction.Provider()
		if !external || owner != provider {
			// Byte-identical to the answer above, which is the whole point, but
			// carrying ErrForeignOperation in its chain so that the two cases
			// are still tellable apart by the operator who has to notice a
			// provider walking somebody else's identifiers.
			return notFoundWrapping(ErrForeignOperation, "no operation %q", named)
		}
	}
	*out = resultOf(stored.Transaction, false)
	return nil
}

// envelopesFor wraps what an outcome emitted, in the order the domain returned it.
// That order is part of the contract — a processed operation is announced before
// the balance change it caused — and the outbox preserves insertion order within
// one transaction, so nothing here may sort it.
func envelopesFor(ids IDs, events []wagering.Event, trace Trace, at time.Time) ([]Envelope, error) {
	if len(events) == 0 {
		return nil, nil
	}
	out := make([]Envelope, 0, len(events))
	for _, event := range events {
		envelope, err := NewEnvelope(event, ids.EventID(), trace, at)
		if err != nil {
			return nil, err
		}
		out = append(out, envelope)
	}
	return out, nil
}

// emit wraps what an outcome produced and appends it. It is the last statement
// of every callback, which is the lock order stated on Repos: the outbox keeps a
// per-wallet sequence counter, so taking that before the wallet is how two
// commands deadlock.
//
// One function rather than a rule two call sites remember. An outcome that
// emitted nothing appends nothing, because an empty batch is not a write.
func emit(
	ctx context.Context,
	out OutboxWriter,
	ids IDs,
	events []wagering.Event,
	trace Trace,
	at time.Time,
) error {
	envelopes, err := envelopesFor(ids, events, trace, at)
	if err != nil || len(envelopes) == 0 {
		return err
	}
	return out.Append(ctx, envelopes)
}

// causationOf names the single thing that caused this operation. On the queue
// path that is the message; on the HTTP path a request is not an event and has
// no identity to name, so there is none.
func causationOf(cmd SubmitOperation) string {
	if cmd.Inbox == nil {
		return ""
	}
	return cmd.Inbox.MessageID
}

// resultOf reads an outcome off a transaction. Balance is present exactly when
// the transaction is processed, and is the balance recorded at that moment —
// never the wallet's balance now, so that a replay answers what the first
// submission answered however far the wallet has moved since.
func resultOf(tx *wagering.WagerTransaction, replay bool) OperationResult {
	result := OperationResult{
		TransactionID:    tx.ID(),
		Kind:             tx.Kind(),
		Status:           tx.Status(),
		Money:            tx.Money(),
		IdempotentReplay: replay,
	}
	// Both accessors return the zero value when they report false, so assigning
	// it says exactly what a guard would have. Balance is the one that genuinely
	// differs: absence is spelled nil there, so it keeps its guard.
	result.ExternalTransactionID, _ = tx.ExternalTransactionID()
	result.FailureCode, _ = tx.FailureCode()
	if balance, ok := tx.Result(); ok {
		result.Balance = &balance
	}
	return result
}
