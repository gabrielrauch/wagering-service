package wagering

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// TransactionSnapshot is the stored shape of a wager transaction.
//
// Fields under External apply only to provider submissions and must be absent
// for an opening.
type TransactionSnapshot struct {
	ID        TransactionID
	WalletID  WalletID
	PlayerID  PlayerID
	Kind      Kind
	Money     money.Money
	Status    Status
	CreatedAt time.Time
	UpdatedAt time.Time

	External *ExternalSnapshot

	// Result is the balance reported to the provider, present exactly when the
	// transaction is processed.
	Result *money.Money
	// FailureCode is the reason, present exactly when the transaction is
	// rejected.
	FailureCode failure.Code

	ReferenceAttempts int
	ReferenceDeadline time.Time
}

// ExternalSnapshot is the stored shape of the provider-owned fields.
type ExternalSnapshot struct {
	Provider                       Provider
	ExternalTransactionID          ExternalTransactionID
	IdempotencyKey                 IdempotencyKey
	PayloadHash                    PayloadHash
	RoundID                        RoundID
	GameID                         GameID
	ReferenceExternalTransactionID ExternalTransactionID
	ResolvedReferenceID            TransactionID
}

// RehydrateWagerTransaction rebuilds a transaction from storage.
//
// It applies no movement, runs no transition and emits no event: a rehydrated
// transaction is exactly what was stored, and this function has no code path
// that could make it anything else.
//
// What it does do is refuse anything the constructors and the state machine
// could never have produced. Rehydration is not a weaker door into the model —
// a stored row describing a loss that moved money, an opening left pending, a
// processed transaction with no resulting balance or a rejection under a
// correctable code is corruption, and corruption is reported rather than
// loaded. Were it accepted here, every invariant the type enforces on the way
// in could be bypassed by writing the row directly and reading it back.
//
// The checks are therefore exactly the ones construction makes, in the same
// order: presence and shape first, then origin, amount, settlement, reference
// and wait budget. What it deliberately does not check is anything requiring
// knowledge this function does not have — whether the payload hash matches the
// fields, or whether the balance in Result agrees with the wallet's ledger.
func RehydrateWagerTransaction(s TransactionSnapshot) (*WagerTransaction, error) {
	switch {
	case s.ID.IsZero():
		return nil, missing("id")
	case s.WalletID.IsZero():
		return nil, missing("walletId")
	case !s.Kind.Known():
		return nil, failure.New(failure.InvalidFieldFormat, "%q is not a known kind", s.Kind).WithField("kind")
	case !s.Status.Known():
		return nil, failure.New(failure.InvalidFieldFormat, "%q is not a known status", s.Status).WithField("status")
	case s.Money.IsUninitialized():
		return nil, uninitialized("money")
	case s.CreatedAt.IsZero():
		return nil, missing("createdAt")
	case s.UpdatedAt.IsZero():
		return nil, missing("updatedAt")
	case s.UpdatedAt.Before(s.CreatedAt):
		return nil, failure.New(failure.InvalidFieldFormat, "must not precede createdAt").WithField("updatedAt")
	}
	if _, err := NewPlayerID(string(s.PlayerID)); err != nil {
		return nil, err
	}
	if err := s.validateOrigin(); err != nil {
		return nil, err
	}
	if err := s.validateAmount(); err != nil {
		return nil, err
	}
	if err := s.validateSettlement(); err != nil {
		return nil, err
	}
	if err := s.validateReference(); err != nil {
		return nil, err
	}
	if err := s.validateWaitBudget(); err != nil {
		return nil, err
	}

	tx := &WagerTransaction{
		id:                s.ID,
		walletID:          s.WalletID,
		playerID:          s.PlayerID,
		kind:              s.Kind,
		amount:            s.Money,
		status:            s.Status,
		createdAt:         s.CreatedAt,
		updatedAt:         s.UpdatedAt,
		failureCode:       s.FailureCode,
		referenceAttempts: s.ReferenceAttempts,
		referenceDeadline: s.ReferenceDeadline,
	}
	if s.Result != nil {
		// Copied rather than aliased, so a caller holding the snapshot cannot
		// reach into the rehydrated transaction and change the balance it
		// reported. validateSettlement has already refused an uninitialised one,
		// so the copy is never the value that means "no result".
		tx.result = *s.Result
	}
	if s.External != nil {
		tx.external = &externalDetails{
			provider:              s.External.Provider,
			externalTransactionID: s.External.ExternalTransactionID,
			idempotencyKey:        s.External.IdempotencyKey,
			payloadHash:           s.External.PayloadHash,
			roundID:               s.External.RoundID,
			gameID:                s.External.GameID,
			referenceExternalID:   s.External.ReferenceExternalTransactionID,
			resolvedReferenceID:   s.External.ResolvedReferenceID,
		}
	}
	return tx, nil
}

// validateOrigin checks that the kind and the provider-owned fields agree.
//
// An opening is raised by this system when a wallet is created, so it carries
// no provider side and is born [Processed] — there is no moment at which one is
// pending, so no other status can have been stored. Every other kind always
// carries a provider side, and its fields are validated for shape exactly as a
// submission's are, because a stored identifier that a submission would have
// been refused for is corruption.
func (s TransactionSnapshot) validateOrigin() error {
	if s.Kind.IsInternal() {
		if s.External != nil {
			return failure.New(failure.InvalidFieldFormat,
				"an %s carries no provider fields", s.Kind).WithField("external")
		}
		if s.Status != Processed {
			return failure.New(failure.InvalidFieldFormat,
				"an %s is born %s and can have reached no other status, got %s",
				s.Kind, Processed, s.Status).WithField("status")
		}
		return nil
	}
	if s.External == nil {
		return failure.New(failure.MissingRequiredField,
			"a %s carries provider fields", s.Kind).WithField("external")
	}
	if err := validateProviderFields(providerFields{
		Provider:              s.External.Provider,
		ExternalTransactionID: s.External.ExternalTransactionID,
		IdempotencyKey:        s.External.IdempotencyKey,
		PlayerID:              s.PlayerID,
		RoundID:               s.External.RoundID,
		GameID:                s.External.GameID,
	}); err != nil {
		return err
	}
	if s.External.PayloadHash == "" {
		// Without it the idempotency key is bound to nothing, and any resubmission
		// under that key would match a payload the transaction never carried.
		return missing("payloadHash")
	}
	return nil
}

// validateAmount checks the amount against what the kind allows, the same rule
// [Command.Validate] and [newOpeningTransaction] apply on the way in.
func (s TransactionSnapshot) validateAmount() error {
	return s.Kind.checkAmount(s.Money)
}

// validateSettlement checks that the status agrees with what settling produced.
//
// [WagerTransaction.MarkProcessed] records the resulting balance and
// [WagerTransaction.Reject] records the code that settled the operation, so each
// is present exactly when its status was reached and absent otherwise. The code
// must also be definitive: a correctable one means nothing was persisted, so a
// stored rejection under one describes a transaction that should not exist.
//
// The balance is held to the same two rules MarkProcessed applies — the
// transaction's own currency, never below zero — so that writing a row and
// reading it back is not a way around them.
func (s TransactionSnapshot) validateSettlement() error {
	if s.FailureCode != "" && !s.FailureCode.Known() {
		return failure.New(failure.InvalidFieldFormat,
			"%q is not a known failure code", s.FailureCode).WithField("failureCode")
	}
	switch s.Status {
	case Processed:
		if s.Result == nil {
			return failure.New(failure.MissingRequiredField,
				"a %s transaction carries the balance it reported", s.Status).WithField("result")
		}
	case Rejected:
		if s.FailureCode == "" {
			return failure.New(failure.MissingRequiredField,
				"a %s transaction carries the code that settled it", s.Status).WithField("failureCode")
		}
		if err := checkSettlingCode(s.FailureCode); err != nil {
			return err
		}
	}
	if s.Status != Processed && s.Result != nil {
		return failure.New(failure.InvalidFieldFormat,
			"only a %s transaction carries a resulting balance, this one is %s",
			Processed, s.Status).WithField("result")
	}
	if s.Status != Rejected && s.FailureCode != "" {
		return failure.New(failure.InvalidFieldFormat,
			"only a %s transaction carries a failure code, this one is %s",
			Rejected, s.Status).WithField("failureCode")
	}
	// The stored balance must be one [WagerTransaction.MarkProcessed] could have
	// recorded. Without these, storage would be the way around the guards there:
	// a row is written directly and read back, and the transaction reports a
	// balance in a currency it never dealt in, or one below zero that no wallet
	// was ever allowed to reach.
	//
	// validateAmount has already run, so s.Money carries a currency to compare
	// against.
	if s.Result != nil {
		if err := checkReportedBalance(*s.Result, s.Money.Currency(), "result"); err != nil {
			return err
		}
	}
	return nil
}

// validateReference checks the reference fields against what the kind allows,
// and that nothing was resolved that was never named.
func (s TransactionSnapshot) validateReference() error {
	if s.External == nil {
		// An opening names nothing, and validateOrigin has already refused a
		// stored opening that claims a provider side.
		return nil
	}
	named := s.External.ReferenceExternalTransactionID
	if err := s.Kind.checkReference(s.External.ExternalTransactionID, named); err != nil {
		return err
	}
	if named != "" {
		return nil
	}
	if !s.External.ResolvedReferenceID.IsZero() {
		return failure.New(failure.InvalidFieldFormat,
			"a transaction naming no reference has none to resolve").WithField("resolvedReferenceId")
	}
	return nil
}

// validateWaitBudget checks that the record of waiting is one waiting could have
// left behind.
//
// [WagerTransaction.MarkPendingReference] sets the deadline on the first wait
// and counts every wait, so the count and the deadline appear together or not at
// all, and a transaction stored as [PendingReference] has waited at least once.
func (s TransactionSnapshot) validateWaitBudget() error {
	switch {
	case s.ReferenceAttempts < 0:
		return failure.New(failure.InvalidFieldFormat,
			"must not be negative, got %d", s.ReferenceAttempts).WithField("referenceAttempts")
	case s.ReferenceAttempts > 0 && s.ReferenceDeadline.IsZero():
		return failure.New(failure.InvalidFieldFormat,
			"a transaction that has waited %d times carries the deadline its first wait set",
			s.ReferenceAttempts).WithField("referenceDeadline")
	case s.ReferenceAttempts == 0 && !s.ReferenceDeadline.IsZero():
		return failure.New(failure.InvalidFieldFormat,
			"a transaction that has never waited carries no deadline").WithField("referenceDeadline")
	case s.Status == PendingReference && s.ReferenceAttempts == 0:
		return failure.New(failure.InvalidFieldFormat,
			"a %s transaction has waited at least once", s.Status).WithField("referenceAttempts")
	}
	return nil
}
