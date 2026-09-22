package wagering

import (
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// Command is one operation a provider has submitted.
//
// It carries typed values only: whoever builds a command has already turned
// strings into identifiers and an amount into [money.Money], so a command that
// exists is at least well-formed. [Command.Validate] then enforces what each
// kind requires.
//
// TransactionID and LedgerEntryID are supplied rather than minted here. The
// domain can mint an identifier on request but never does so implicitly, which
// keeps every operation reproducible: the same command applied twice in a test
// produces the same identifiers.
type Command struct {
	// TransactionID is this system's identifier for the operation.
	TransactionID TransactionID
	// LedgerEntryID records the balance change, and is required only for kinds
	// that move money.
	LedgerEntryID LedgerEntryID

	// WalletID is the wallet the provider addresses. It is submitted rather than
	// derived: the provider learned it when the wallet was opened, names it on
	// every operation, and it is hashed with the rest of the business fields.
	// The wallet must belong to PlayerID, which [WalletBelongsToPlayer] checks
	// against the wallet actually loaded.
	WalletID WalletID

	Provider              Provider
	ExternalTransactionID ExternalTransactionID
	IdempotencyKey        IdempotencyKey
	PlayerID              PlayerID
	RoundID               RoundID
	GameID                GameID
	Kind                  Kind
	Money                 money.Money

	// ReferenceExternalTransactionID names the transaction this operation acts
	// on. It is required for a refund or a rollback, optional for a win, and
	// refused on anything else.
	ReferenceExternalTransactionID ExternalTransactionID
}

// Validate enforces what the command's kind requires.
//
// Every failure it reports is correctable: nothing has been recorded, so the
// provider may repair the payload and resubmit it under the same idempotency
// key. That is the practical difference between a malformed submission and a
// settled one — a settled one has bound the key to its payload for good.
func (c Command) Validate() error {
	if !c.Kind.Known() {
		return failure.New(failure.InvalidFieldFormat, "%q is not a known kind", c.Kind).WithField("kind")
	}
	// An opening belongs to wallet creation. Submitted by a provider it is not
	// a rejected operation but an impossible one: its origin is decided by its
	// kind, so there is no transaction to record and nothing to audit.
	if c.Kind.IsInternal() {
		return failure.New(failure.UnsupportedTransactionKind,
			"%s is raised only when a wallet is opened", c.Kind).WithField("kind")
	}

	if c.TransactionID.IsZero() {
		return missing("transactionId")
	}
	if c.WalletID.IsZero() {
		return missing("walletId")
	}
	if c.Kind.MovesMoney() && c.LedgerEntryID.IsZero() {
		return failure.New(failure.MissingRequiredField,
			"a %s moves money and must record a ledger entry", c.Kind).WithField("ledgerEntryId")
	}
	if !c.Kind.MovesMoney() && !c.LedgerEntryID.IsZero() {
		return failure.New(failure.ReferenceNotApplicable,
			"a %s moves no money and records no ledger entry", c.Kind).WithField("ledgerEntryId")
	}

	// Every external kind carries the same provider-owned fields.
	if err := validateProviderFields(providerFields{
		Provider:              c.Provider,
		ExternalTransactionID: c.ExternalTransactionID,
		IdempotencyKey:        c.IdempotencyKey,
		PlayerID:              c.PlayerID,
		RoundID:               c.RoundID,
		GameID:                c.GameID,
	}); err != nil {
		return err
	}

	if c.Money.IsUninitialized() {
		return uninitialized("money")
	}
	if c.Money.IsNegative() {
		return failure.New(failure.InvalidAmountFormat, "must not be negative").WithField("money")
	}
	if err := c.Kind.checkAmount(c.Money); err != nil {
		return err
	}
	return c.Kind.checkReference(c.ExternalTransactionID, c.ReferenceExternalTransactionID)
}

// namesReference reports whether the operation actually points at another
// transaction.
//
// A kind that may carry a reference does not necessarily carry one — a win is
// free to name the bet it pays out on or to name nothing — so the kind alone
// does not answer the question and neither does the field alone.
//
// Everything that acts on a reference asks this one predicate, so no two places
// can disagree about whether an operation has one. That mattered: the resolution
// step once keyed off the supplied [ReferenceView] instead, so a view handed to
// an operation that names no reference turned a perfectly valid submission into
// a failure.
func (c Command) namesReference() bool {
	return c.Kind.AllowsReference() && c.ReferenceExternalTransactionID != ""
}
