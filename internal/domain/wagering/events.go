package wagering

import (
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// Event is something that happened, ready to be published.
//
// Each event is a concrete type whose constructor fixes its type name and
// version; neither can be set by a caller. The envelope — event id, correlation
// id, causation id, occurred-at — belongs to the layer that publishes, not to
// the domain that decides.
//
// Events are returned from the operation that produced them and are never
// buffered on an aggregate. That is what makes "rehydration emits no events"
// structurally true rather than a rule someone has to remember: a rehydration
// function has no event to return and no buffer to fill.
//
// Every event is built from an aggregate that has already reached the state the
// event describes, so an event cannot contradict the transaction it reports.
type Event interface {
	// EventType names the event.
	EventType() string
	// EventVersion is the schema version of the payload.
	EventVersion() int
}

// Event type names and versions, fixed by the constructors below.
const (
	typeWagerTransactionProcessed        = "WagerTransactionProcessed"
	typeWagerTransactionRejected         = "WagerTransactionRejected"
	typeWalletBalanceChanged             = "WalletBalanceChanged"
	typeWagerTransactionPendingReference = "WagerTransactionPendingReference"

	eventVersion = 1
)

// WagerTransactionProcessed reports an operation that completed successfully.
//
// It is emitted for every processed kind, including a loss, which completes
// without moving money.
type WagerTransactionProcessed struct {
	transactionID         TransactionID
	walletID              WalletID
	playerID              PlayerID
	kind                  Kind
	amount                money.Money
	balanceAfter          money.Money
	externalTransactionID ExternalTransactionID
	// isExternal is the transaction's own answer to whether it came from a
	// provider, kept rather than inferred from externalTransactionID: origin is
	// decided by the transaction, and an event that re-derived it from an empty
	// string would be guessing at it.
	isExternal bool
}

// NewWagerTransactionProcessed reports tx as processed.
func NewWagerTransactionProcessed(tx *WagerTransaction) (WagerTransactionProcessed, error) {
	if tx == nil {
		return WagerTransactionProcessed{}, failure.New(failure.UninitializedValue, "transaction must be present")
	}
	if tx.Status() != Processed {
		return WagerTransactionProcessed{}, failure.New(failure.InvalidStateTransition,
			"cannot report %s as processed", tx.Status())
	}
	balanceAfter, ok := tx.Result()
	if !ok {
		return WagerTransactionProcessed{}, failure.New(failure.UninitializedValue,
			"a processed transaction carries the resulting balance")
	}
	externalID, isExternal := tx.ExternalTransactionID()
	return WagerTransactionProcessed{
		transactionID:         tx.ID(),
		walletID:              tx.WalletID(),
		playerID:              tx.PlayerID(),
		kind:                  tx.Kind(),
		amount:                tx.Money(),
		balanceAfter:          balanceAfter,
		externalTransactionID: externalID,
		isExternal:            isExternal,
	}, nil
}

// EventType names the event.
func (e WagerTransactionProcessed) EventType() string { return typeWagerTransactionProcessed }

// EventVersion is the schema version of the payload.
func (e WagerTransactionProcessed) EventVersion() int { return eventVersion }

// TransactionID returns the transaction that completed.
func (e WagerTransactionProcessed) TransactionID() TransactionID { return e.transactionID }

// WalletID returns the wallet the operation acted on.
func (e WagerTransactionProcessed) WalletID() WalletID { return e.walletID }

// PlayerID returns the player the operation belongs to.
func (e WagerTransactionProcessed) PlayerID() PlayerID { return e.playerID }

// Kind returns what the operation did.
func (e WagerTransactionProcessed) Kind() Kind { return e.kind }

// Money returns the amount the operation moved.
func (e WagerTransactionProcessed) Money() money.Money { return e.amount }

// BalanceAfter returns the balance reported back to the provider.
func (e WagerTransactionProcessed) BalanceAfter() money.Money { return e.balanceAfter }

// ExternalTransactionID returns the provider's identifier, and false for an
// opening.
func (e WagerTransactionProcessed) ExternalTransactionID() (ExternalTransactionID, bool) {
	return e.externalTransactionID, e.isExternal
}

// WagerTransactionRejected reports an operation a business rule settled
// against.
//
// It is emitted only for definitive rejections. A malformed submission never
// becomes a transaction, so there is nothing to report and nothing to publish.
type WagerTransactionRejected struct {
	transactionID         TransactionID
	walletID              WalletID
	playerID              PlayerID
	kind                  Kind
	amount                money.Money
	failureCode           failure.Code
	externalTransactionID ExternalTransactionID
	// isExternal is the transaction's own answer to whether it came from a
	// provider, kept rather than inferred from externalTransactionID: origin is
	// decided by the transaction, and an event that re-derived it from an empty
	// string would be guessing at it.
	isExternal bool
}

// NewWagerTransactionRejected reports tx as rejected.
func NewWagerTransactionRejected(tx *WagerTransaction) (WagerTransactionRejected, error) {
	if tx == nil {
		return WagerTransactionRejected{}, failure.New(failure.UninitializedValue, "transaction must be present")
	}
	if tx.Status() != Rejected {
		return WagerTransactionRejected{}, failure.New(failure.InvalidStateTransition,
			"cannot report %s as rejected", tx.Status())
	}
	code, ok := tx.FailureCode()
	if !ok {
		return WagerTransactionRejected{}, failure.New(failure.UninitializedValue,
			"a rejected transaction carries a failure code")
	}
	externalID, isExternal := tx.ExternalTransactionID()
	return WagerTransactionRejected{
		transactionID:         tx.ID(),
		walletID:              tx.WalletID(),
		playerID:              tx.PlayerID(),
		kind:                  tx.Kind(),
		amount:                tx.Money(),
		failureCode:           code,
		externalTransactionID: externalID,
		isExternal:            isExternal,
	}, nil
}

// EventType names the event.
func (e WagerTransactionRejected) EventType() string { return typeWagerTransactionRejected }

// EventVersion is the schema version of the payload.
func (e WagerTransactionRejected) EventVersion() int { return eventVersion }

// TransactionID returns the transaction that was rejected.
func (e WagerTransactionRejected) TransactionID() TransactionID { return e.transactionID }

// WalletID returns the wallet the operation would have acted on.
func (e WagerTransactionRejected) WalletID() WalletID { return e.walletID }

// PlayerID returns the player the operation belongs to.
func (e WagerTransactionRejected) PlayerID() PlayerID { return e.playerID }

// Kind returns what the operation would have done.
func (e WagerTransactionRejected) Kind() Kind { return e.kind }

// Money returns the amount the operation would have moved.
func (e WagerTransactionRejected) Money() money.Money { return e.amount }

// FailureCode returns the documented reason for the rejection.
func (e WagerTransactionRejected) FailureCode() failure.Code { return e.failureCode }

// ExternalTransactionID returns the provider's identifier, and false for an
// opening.
func (e WagerTransactionRejected) ExternalTransactionID() (ExternalTransactionID, bool) {
	return e.externalTransactionID, e.isExternal
}

// WalletBalanceChanged reports an effective change to a wallet's balance.
//
// Its payload is exactly a ledger entry's: the two describe the same moment,
// one for the audit trail and one for subscribers, and building the event from
// the entry keeps them from ever disagreeing.
type WalletBalanceChanged struct {
	walletID      WalletID
	transactionID TransactionID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion uint64
}

// NewWalletBalanceChanged reports the change an entry recorded.
//
// It refuses an entry that was never constructed. Go permits any package to
// write WalletLedgerEntry{} even though every field is unexported — a composite
// literal needs permission only to *name* a field, and that one names none — so
// "this entry came from [NewWalletLedgerEntry]" is a claim this constructor has
// to check rather than one the type system makes on its behalf. Announcing that
// a balance changed on a zero wallet, by a zero transaction, in no direction is
// worse than an error: subscribers have no way to tell it from a real change.
//
// Past that one check there is nothing left to validate. Any entry that is not
// the zero value came from [NewWalletLedgerEntry], which proved its arithmetic
// then and has held it immutable since.
func NewWalletBalanceChanged(entry WalletLedgerEntry) (WalletBalanceChanged, error) {
	if entry.IsZero() {
		return WalletBalanceChanged{}, failure.New(failure.UninitializedValue,
			"ledger entry must be present")
	}
	return WalletBalanceChanged{
		walletID:      entry.WalletID(),
		transactionID: entry.TransactionID(),
		direction:     entry.Direction(),
		amount:        entry.Amount(),
		balanceBefore: entry.BalanceBefore(),
		balanceAfter:  entry.BalanceAfter(),
		walletVersion: entry.WalletVersion(),
	}, nil
}

// EventType names the event.
func (e WalletBalanceChanged) EventType() string { return typeWalletBalanceChanged }

// EventVersion is the schema version of the payload.
func (e WalletBalanceChanged) EventVersion() int { return eventVersion }

// WalletID returns the wallet whose balance changed.
func (e WalletBalanceChanged) WalletID() WalletID { return e.walletID }

// TransactionID returns the transaction that caused the change.
func (e WalletBalanceChanged) TransactionID() TransactionID { return e.transactionID }

// Direction returns which way the money moved.
func (e WalletBalanceChanged) Direction() Direction { return e.direction }

// Money returns how much moved.
func (e WalletBalanceChanged) Money() money.Money { return e.amount }

// BalanceBefore returns the balance before the change.
func (e WalletBalanceChanged) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter returns the balance after the change.
func (e WalletBalanceChanged) BalanceAfter() money.Money { return e.balanceAfter }

// WalletVersion returns the version the change produced.
func (e WalletBalanceChanged) WalletVersion() uint64 { return e.walletVersion }

// WagerTransactionPendingReference reports that an operation is waiting for a
// transaction it depends on.
type WagerTransactionPendingReference struct {
	transactionID         TransactionID
	walletID              WalletID
	playerID              PlayerID
	kind                  Kind
	referenceExternalID   ExternalTransactionID
	attempts              int
	externalTransactionID ExternalTransactionID
}

// NewWagerTransactionPendingReference reports tx as waiting for its reference.
func NewWagerTransactionPendingReference(tx *WagerTransaction) (WagerTransactionPendingReference, error) {
	if tx == nil {
		return WagerTransactionPendingReference{}, failure.New(failure.UninitializedValue, "transaction must be present")
	}
	if tx.Status() != PendingReference {
		return WagerTransactionPendingReference{}, failure.New(failure.InvalidStateTransition,
			"cannot report %s as waiting for a reference", tx.Status())
	}
	referenceID, ok := tx.ReferenceExternalTransactionID()
	if !ok {
		return WagerTransactionPendingReference{}, failure.New(failure.ReferenceRequired,
			"a transaction waiting for a reference names one")
	}
	externalID, _ := tx.ExternalTransactionID()
	return WagerTransactionPendingReference{
		transactionID:         tx.ID(),
		walletID:              tx.WalletID(),
		playerID:              tx.PlayerID(),
		kind:                  tx.Kind(),
		referenceExternalID:   referenceID,
		attempts:              tx.ReferenceAttempts(),
		externalTransactionID: externalID,
	}, nil
}

// EventType names the event.
func (e WagerTransactionPendingReference) EventType() string {
	return typeWagerTransactionPendingReference
}

// EventVersion is the schema version of the payload.
func (e WagerTransactionPendingReference) EventVersion() int { return eventVersion }

// TransactionID returns the waiting transaction.
func (e WagerTransactionPendingReference) TransactionID() TransactionID { return e.transactionID }

// WalletID returns the wallet the operation will act on.
func (e WagerTransactionPendingReference) WalletID() WalletID { return e.walletID }

// PlayerID returns the player the operation belongs to.
func (e WagerTransactionPendingReference) PlayerID() PlayerID { return e.playerID }

// Kind returns what the operation will do.
func (e WagerTransactionPendingReference) Kind() Kind { return e.kind }

// ReferenceExternalTransactionID returns the transaction being waited for.
func (e WagerTransactionPendingReference) ReferenceExternalTransactionID() ExternalTransactionID {
	return e.referenceExternalID
}

// Attempts returns how many times the operation has been parked so far.
func (e WagerTransactionPendingReference) Attempts() int { return e.attempts }

// ExternalTransactionID returns the provider's identifier for the waiting
// operation.
func (e WagerTransactionPendingReference) ExternalTransactionID() ExternalTransactionID {
	return e.externalTransactionID
}
