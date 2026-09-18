package wagering

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// WagerTransaction is the durable record of one operation against a wallet.
//
// A transaction has one of two origins, and the model keeps them apart rather
// than leaving it to convention. An opening is raised internally when a wallet
// is created with money in it: it has no provider, no external identifier, no
// idempotency key, no round and no game, and it is born [Processed]. Everything
// else arrives from a provider, carries all of those, and is born [Pending].
//
// The provider-owned fields live behind a pointer that is nil for an opening,
// so an opening carrying a provider is not merely invalid but unrepresentable.
//
// The transaction owns its own status transitions and refuses any that the
// state machine in [Status] does not allow, including every move out of a
// terminal status.
type WagerTransaction struct {
	id        TransactionID
	walletID  WalletID
	playerID  PlayerID
	kind      Kind
	amount    money.Money
	status    Status
	createdAt time.Time
	updatedAt time.Time

	// external is nil for an opening and populated for every other kind.
	external *externalDetails

	// result is the balance reported back to the provider, set when the
	// transaction is processed.
	//
	// It is held by value. [money.Money] already carries its own uninitialised
	// state, so a pointer here would be a second way to spell an absence the
	// type spells perfectly well on its own — at the cost of an allocation per
	// processed transaction, and of a defensive copy at every write to stop the
	// balance being aliased from outside.
	result money.Money
	// failureCode is the documented reason, set when the transaction is
	// rejected.
	failureCode failure.Code

	// referenceAttempts counts how many times this transaction has been parked
	// waiting for its reference; referenceDeadline is when waiting stops being
	// worthwhile. Both are zero until the first wait.
	referenceAttempts int
	referenceDeadline time.Time
}

// externalDetails holds everything that applies only to a provider submission.
type externalDetails struct {
	provider              Provider
	externalTransactionID ExternalTransactionID
	idempotencyKey        IdempotencyKey
	payloadHash           PayloadHash
	roundID               RoundID
	gameID                GameID

	// referenceExternalID is the provider's identifier for the transaction this
	// one acts on, empty when the operation names none.
	referenceExternalID ExternalTransactionID
	// resolvedReferenceID is this system's identifier for that transaction,
	// zero until the reference has been found.
	resolvedReferenceID TransactionID
}

// NewExternalTransaction records a provider submission, in [Pending].
//
// The command is validated in full first, so a transaction never exists in a
// shape its kind forbids.
func NewExternalTransaction(cmd Command, walletID WalletID, now time.Time) (*WagerTransaction, error) {
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	if walletID.IsZero() {
		return nil, missing("walletId")
	}
	if now.IsZero() {
		return nil, missing("now")
	}
	// cmd.Validate has just run, so the already-validated hash is taken
	// directly rather than through the exported PayloadHash, which would
	// validate the whole command a second time.
	hash := cmd.payloadHash()
	return &WagerTransaction{
		id:        cmd.TransactionID,
		walletID:  walletID,
		playerID:  cmd.PlayerID,
		kind:      cmd.Kind,
		amount:    cmd.Money,
		status:    Pending,
		createdAt: now,
		updatedAt: now,
		external: &externalDetails{
			provider:              cmd.Provider,
			externalTransactionID: cmd.ExternalTransactionID,
			idempotencyKey:        cmd.IdempotencyKey,
			payloadHash:           hash,
			roundID:               cmd.RoundID,
			gameID:                cmd.GameID,
			referenceExternalID:   cmd.ReferenceExternalTransactionID,
		},
	}, nil
}

// newOpeningTransaction records a wallet's starting balance.
//
// It is unexported because an opening belongs to wallet creation and to nothing
// else, and it is born [Processed] because the balance is applied as part of
// creating the wallet: there is no moment at which an opening is pending.
func newOpeningTransaction(in OpenWalletInput, now time.Time) (*WagerTransaction, error) {
	amount := in.InitialBalance
	switch {
	case in.TransactionID.IsZero():
		return nil, missing("transactionId")
	case in.WalletID.IsZero():
		return nil, missing("walletId")
	case in.PlayerID == "":
		return nil, missing("playerId")
	case amount.IsUninitialized():
		return nil, uninitialized("money")
	case !amount.IsPositive():
		return nil, failure.New(failure.InvalidAmountForKind,
			"an opening records a positive starting balance, got %s", amount).WithField("money")
	case now.IsZero():
		return nil, missing("now")
	}
	return &WagerTransaction{
		id:        in.TransactionID,
		walletID:  in.WalletID,
		playerID:  in.PlayerID,
		kind:      Opening,
		amount:    amount,
		status:    Processed,
		createdAt: now,
		updatedAt: now,
		result:    amount,
	}, nil
}

// ID returns this system's identifier for the transaction.
func (t *WagerTransaction) ID() TransactionID { return t.id }

// WalletID returns the wallet the transaction acts on.
func (t *WagerTransaction) WalletID() WalletID { return t.walletID }

// PlayerID returns the player the transaction belongs to.
func (t *WagerTransaction) PlayerID() PlayerID { return t.playerID }

// Kind returns what the operation does.
func (t *WagerTransaction) Kind() Kind { return t.kind }

// Money returns the amount the operation moves.
func (t *WagerTransaction) Money() money.Money { return t.amount }

// Currency returns the currency the operation is denominated in.
func (t *WagerTransaction) Currency() money.Currency { return t.amount.Currency() }

// Status returns where the transaction stands.
func (t *WagerTransaction) Status() Status { return t.status }

// CreatedAt returns when the transaction was recorded.
func (t *WagerTransaction) CreatedAt() time.Time { return t.createdAt }

// UpdatedAt returns when the transaction last changed.
func (t *WagerTransaction) UpdatedAt() time.Time { return t.updatedAt }

// IsExternal reports whether the transaction came from a provider.
func (t *WagerTransaction) IsExternal() bool { return t.external != nil }

// IsInternal reports whether the transaction was raised by this system.
func (t *WagerTransaction) IsInternal() bool { return t.external == nil }

// Provider returns the submitting provider, and false for an opening.
func (t *WagerTransaction) Provider() (Provider, bool) {
	if t.external == nil {
		return "", false
	}
	return t.external.provider, true
}

// ExternalTransactionID returns the provider's identifier for the operation,
// and false for an opening.
func (t *WagerTransaction) ExternalTransactionID() (ExternalTransactionID, bool) {
	if t.external == nil {
		return "", false
	}
	return t.external.externalTransactionID, true
}

// IdempotencyKey returns the key the operation was submitted under, and false
// for an opening.
func (t *WagerTransaction) IdempotencyKey() (IdempotencyKey, bool) {
	if t.external == nil {
		return "", false
	}
	return t.external.idempotencyKey, true
}

// PayloadHash returns the fingerprint of the operation's business fields, and
// false for an opening.
func (t *WagerTransaction) PayloadHash() (PayloadHash, bool) {
	if t.external == nil {
		return "", false
	}
	return t.external.payloadHash, true
}

// RoundID returns the round, and false for an opening.
func (t *WagerTransaction) RoundID() (RoundID, bool) {
	if t.external == nil {
		return "", false
	}
	return t.external.roundID, true
}

// GameID returns the game, and false for an opening.
func (t *WagerTransaction) GameID() (GameID, bool) {
	if t.external == nil {
		return "", false
	}
	return t.external.gameID, true
}

// ReferenceExternalTransactionID returns the provider's identifier for the
// transaction this one acts on, and false when it names none.
func (t *WagerTransaction) ReferenceExternalTransactionID() (ExternalTransactionID, bool) {
	if t.external == nil || t.external.referenceExternalID == "" {
		return "", false
	}
	return t.external.referenceExternalID, true
}

// ResolvedReferenceID returns this system's identifier for the referenced
// transaction, and false until the reference has been found.
func (t *WagerTransaction) ResolvedReferenceID() (TransactionID, bool) {
	if t.external == nil || t.external.resolvedReferenceID.IsZero() {
		return TransactionID{}, false
	}
	return t.external.resolvedReferenceID, true
}

// Result returns the balance reported back to the provider, and false unless
// the transaction is processed.
func (t *WagerTransaction) Result() (money.Money, bool) {
	if t.result.IsUninitialized() {
		return money.Money{}, false
	}
	return t.result, true
}

// FailureCode returns why the transaction was rejected, and false unless it
// was.
func (t *WagerTransaction) FailureCode() (failure.Code, bool) {
	if t.failureCode == "" {
		return "", false
	}
	return t.failureCode, true
}

// ReferenceAttempts returns how many times the transaction has been parked
// waiting for its reference.
func (t *WagerTransaction) ReferenceAttempts() int { return t.referenceAttempts }

// ReferenceDeadline returns when waiting for the reference stops, and false
// before the first wait.
func (t *WagerTransaction) ReferenceDeadline() (time.Time, bool) {
	if t.referenceDeadline.IsZero() {
		return time.Time{}, false
	}
	return t.referenceDeadline, true
}

// MarkProcessed completes the transaction successfully, recording the balance
// reported back to the provider.
//
// The balance must be one a wallet could actually have held: in the
// transaction's own currency, and not negative.
//
// A transaction only ever acts on one wallet, and a wallet holds one currency,
// so a result in another currency is a balance no wallet this transaction could
// touch has ever held. A negative one is likewise unreachable — [Wallet.move]
// and [NewWalletLedgerEntry] both refuse to leave a wallet below zero.
//
// Both checks live here rather than in the caller because the amount moved and
// the balance reported are two values the transaction carries at once, and
// nothing outside it is in a position to keep them in step.
func (t *WagerTransaction) MarkProcessed(balanceAfter money.Money, now time.Time) error {
	if err := checkReportedBalance(balanceAfter, t.Currency(), "balanceAfter"); err != nil {
		return err
	}
	if err := t.transition(Processed, now); err != nil {
		return err
	}
	t.result = balanceAfter
	return nil
}

// MarkPendingReference parks the transaction until its reference arrives,
// counting the attempt and setting the deadline on the first wait.
//
// It refuses to park a transaction whose wait budget is already spent; the
// caller must reject it with [failure.ReferenceNotFound] instead.
func (t *WagerTransaction) MarkPendingReference(policy ReferencePolicy, now time.Time) error {
	if err := policy.validate(); err != nil {
		return err
	}
	if now.IsZero() {
		return missing("now")
	}
	spent, err := t.ReferenceBudgetExhausted(policy, now)
	if err != nil {
		return err
	}
	if spent {
		return failure.New(failure.InvalidStateTransition,
			"the wait budget for the reference is spent after %d attempts", t.referenceAttempts)
	}
	if err := t.transition(PendingReference, now); err != nil {
		return err
	}
	if t.referenceDeadline.IsZero() {
		t.referenceDeadline = now.Add(policy.TTL)
	}
	t.referenceAttempts++
	return nil
}

// ReferenceBudgetExhausted reports whether the transaction has waited as long
// or as often as the policy allows.
//
// The policy is validated rather than trusted, because the answer decides
// whether an operation is parked or settled for good. An unbuilt policy has a
// MaxAttempts of zero, which every transaction trivially meets, so a predicate
// that answered from it would report a spent budget on the very first wait and
// turn a routine park into a definitive [failure.ReferenceNotFound] rejection.
// A meaningless policy therefore produces an error, never a verdict.
func (t *WagerTransaction) ReferenceBudgetExhausted(policy ReferencePolicy, now time.Time) (bool, error) {
	if err := policy.validate(); err != nil {
		return false, err
	}
	if t.referenceAttempts >= policy.MaxAttempts {
		return true, nil
	}
	return !t.referenceDeadline.IsZero() && !now.Before(t.referenceDeadline), nil
}

// Reject settles the transaction against a business rule.
//
// Only a definitive code may settle a transaction. A correctable code says the
// submission was malformed and nothing was persisted, so recording one here
// would bind the idempotency key to a payload the provider is still entitled to
// repair and resubmit — the split [failure.Code.Correctable] exists to make.
// Refusing it is what keeps that split a property of the type rather than a
// convention the caller has to remember.
//
// An audit code is refused for the opposite reason. It is definitive, but it
// reports corruption found in stored state rather than the outcome of anything
// submitted, so there was no operation for it to settle; see [failure.Code.Audit].
func (t *WagerTransaction) Reject(code failure.Code, now time.Time) error {
	if err := checkSettlingCode(code); err != nil {
		return err
	}
	if err := t.transition(Rejected, now); err != nil {
		return err
	}
	t.failureCode = code
	return nil
}

// Fail records a permanent infrastructure failure for audit.
//
// This is not a business outcome. It exists so that an operation lost to
// something outside the domain still leaves a durable trace.
func (t *WagerTransaction) Fail(now time.Time) error {
	return t.transition(Failed, now)
}

// ResolveReference records this system's identifier for the referenced
// transaction, once it has been found.
//
// A settled transaction is never written to, so resolving against a terminal
// status is refused: a transaction that has reached [Processed], [Rejected] or
// [Failed] never changes again, and reversal history is additive rather than
// something that rewrites what came before.
//
// Resolving is also write-once. Re-resolving to the same identifier is a no-op,
// so an operation carried forward with [Processor.Continue] after an earlier
// attempt already found its reference succeeds unchanged; pointing an already
// resolved reference at a different transaction is refused.
func (t *WagerTransaction) ResolveReference(id TransactionID) error {
	if t.external == nil {
		return failure.New(failure.ReferenceNotApplicable, "an %s names no reference", t.kind)
	}
	if t.external.referenceExternalID == "" {
		return failure.New(failure.ReferenceNotApplicable, "this %s names no reference", t.kind)
	}
	if id.IsZero() {
		return missing("resolvedReferenceId")
	}
	if t.status.IsTerminal() {
		return failure.New(failure.InvalidStateTransition,
			"%s is terminal and its reference cannot be resolved", t.status)
	}
	if settled := t.external.resolvedReferenceID; !settled.IsZero() && settled != id {
		return failure.New(failure.InvalidStateTransition,
			"the reference is already resolved to %s and cannot be repointed to %s", settled, id).
			WithField("resolvedReferenceId")
	}
	t.external.resolvedReferenceID = id
	return nil
}

// AssertSamePayload reports whether an incoming submission carries the same
// business fields as this transaction.
//
// Two submissions under one idempotency key must describe the same operation.
// A different payload under the same key is a contract violation by the
// provider, not a retry, and is settled rather than corrected: the key is
// already bound to this transaction.
func (t *WagerTransaction) AssertSamePayload(hash PayloadHash) error {
	stored, ok := t.PayloadHash()
	if !ok {
		return failure.New(failure.ReferenceNotApplicable, "an %s carries no payload hash", t.kind)
	}
	if stored != hash {
		return failure.New(failure.IdempotencyPayloadConflict,
			"idempotency key is already bound to payload %s, got %s", stored, hash)
	}
	return nil
}

// transition moves the transaction, refusing anything the state machine does
// not allow. A refused transition is reported as an error and never as a panic.
func (t *WagerTransaction) transition(next Status, now time.Time) error {
	if now.IsZero() {
		return missing("now")
	}
	if t.status.IsTerminal() {
		return failure.New(failure.InvalidStateTransition,
			"%s is terminal and cannot move to %s", t.status, next)
	}
	if !t.status.CanTransitionTo(next) {
		return failure.New(failure.InvalidStateTransition,
			"cannot move from %s to %s", t.status, next)
	}
	t.status = next
	t.updatedAt = now
	return nil
}

// checkReportedBalance reports whether balance is one a wallet this transaction
// could touch might actually have held: initialised, in the transaction's own
// currency, and not below zero.
//
// [WagerTransaction.MarkProcessed] applies it to the balance it is about to
// record and [TransactionSnapshot.validateSettlement] to the balance that was
// stored, so writing a row directly and reading it back is not a way around the
// rule. The field is named by the caller, since the two know the value under
// different names.
func checkReportedBalance(balance money.Money, txCurrency money.Currency, field string) error {
	switch {
	case balance.IsUninitialized():
		return uninitialized(field)
	case balance.Currency() != txCurrency:
		return failure.New(failure.CurrencyMismatch,
			"the transaction is in %s but the balance it reported is in %s",
			txCurrency, balance.Currency()).WithField(field)
	case balance.IsNegative():
		return failure.New(failure.InsufficientFunds,
			"a wallet never holds less than nothing, got %s", balance).WithField(field)
	}
	return nil
}

// checkSettlingCode reports whether code is one that may settle a transaction.
//
// Only a definitive code may: a correctable one says the submission was
// malformed and nothing was persisted, and an audit code reports corruption
// found in stored state rather than the outcome of anything submitted. Both
// halves are asked here so that [WagerTransaction.Reject] and
// [TransactionSnapshot.validateSettlement] cannot answer them differently.
func checkSettlingCode(code failure.Code) error {
	switch {
	case !code.Known():
		return failure.New(failure.InvalidFieldFormat,
			"%q is not a known failure code", code).WithField("failureCode")
	case !code.Definitive():
		return failure.New(failure.InvalidFieldFormat,
			"%s is correctable and cannot settle a transaction", code).WithField("failureCode")
	case code.Audit():
		return failure.New(failure.InvalidFieldFormat,
			"%s reports on stored state and settles no transaction", code).WithField("failureCode")
	}
	return nil
}
