package app

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// OperationFields is one operation as it arrived, still in strings.
//
// The transport hands over what it received and this layer parses it exactly
// once, so that HTTP and SQS cannot disagree about what a submission said. That
// matters more than it looks: the idempotency hash is taken over the parsed
// values, so two transports parsing differently would decide differently about
// whether a submission is a retry.
type OperationFields struct {
	Provider              string
	ExternalTransactionID string
	IdempotencyKey        string
	PlayerID              string
	RoundID               string
	GameID                string
	Kind                  string

	// Amount is a plain decimal with exactly two fraction digits, and Currency
	// three uppercase letters. Neither is normalised: the canonical payload is
	// the submitted payload, so a value repaired on the way in would be hashed
	// as something the provider never sent.
	Amount   string
	Currency string

	// ReferenceExternalTransactionID names the operation this one acts on, and
	// is empty when it names none.
	ReferenceExternalTransactionID string
}

// InboxKey is the address of one inbox row: the inbox holds one row per
// consumer per message, and the pair is that row's primary key.
//
// The two travel as one for the reason [Trace] and [SettledOperation] do. They
// are a single address, they are both strings, and two adjacent strings are
// transposable at a call site without the compiler noticing — which here would
// read as "not handled yet" and let a redelivery be processed twice.
type InboxKey struct {
	Consumer  string
	MessageID string
}

// InboxMessage is the identity of a queue message and the fingerprint of its
// body, supplied only on the SQS path.
type InboxMessage struct {
	Consumer  string
	MessageID string
	BodyHash  string
}

// Key is the row this message claims.
func (m InboxMessage) Key() InboxKey {
	return InboxKey{Consumer: m.Consumer, MessageID: m.MessageID}
}

// InboxRecord is a message already handled.
type InboxRecord struct {
	Consumer    string
	MessageID   string
	BodyHash    string
	ReceivedAt  time.Time
	CompletedAt time.Time
}

// SubmitOperation is one submission, from either transport.
//
// Inbox is nil on the HTTP path. Correlation is the thread tying everything done
// for one request together, and is required: an operation with no trace is one
// nobody can follow afterwards.
type SubmitOperation struct {
	Principal   Principal
	Correlation string
	Inbox       *InboxMessage
	Fields      OperationFields
}

// OpenWalletCommand creates a wallet for a player in a currency.
type OpenWalletCommand struct {
	Principal     Principal
	Correlation   string
	PlayerID      string
	InitialAmount string
	Currency      string
}

// OperationResult is what one operation came to.
//
// Balance is non-nil exactly when Status is PROCESSED, and is the balance
// observed at the moment the operation was processed — not the wallet's balance
// now. A replay returns the original, which is the only answer that makes a
// replay indistinguishable from the first submission, however far the wallet has
// moved since.
type OperationResult struct {
	TransactionID         wagering.TransactionID
	ExternalTransactionID wagering.ExternalTransactionID
	Kind                  wagering.Kind
	Status                wagering.Status
	Money                 money.Money
	Balance               *money.Money
	FailureCode           failure.Code

	// IdempotentReplay reports that this result was read rather than produced:
	// the same submission had already been settled.
	IdempotentReplay bool
}

// WalletView is a wallet as a caller sees it.
type WalletView struct {
	ID        wagering.WalletID
	PlayerID  wagering.PlayerID
	Balance   money.Money
	Version   uint64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// LedgerQuery asks for a page of a wallet's ledger. An empty cursor starts at
// the beginning.
type LedgerQuery struct {
	WalletID wagering.WalletID
	Cursor   string
	Limit    int
}

// LedgerPage is one page of a ledger. NextCursor is empty on the last page.
type LedgerPage struct {
	Entries    []wagering.WalletLedgerEntry
	NextCursor string
}

// Divergence is a wallet whose stored balance disagrees with its ledger.
//
// Difference is Stored less Reconstructed and MAY BE NEGATIVE — it is the one
// signed amount in the system. money.Money holds a negative and renders it, but
// refuses to marshal one, so a transport prints Difference from Amount() and
// never marshals it directly.
type Divergence struct {
	WalletID      wagering.WalletID
	Stored        money.Money
	Reconstructed money.Money
	Difference    money.Money
}

// Reconciliation is the result of checking a wallet against its ledger. It never
// alters the balance.
type Reconciliation struct {
	Divergence
	Consistent bool
}

// ResumeOutcome is what one turn of the resume worker did.
//
// Claimed is false when there was nothing due, or when the candidate stopped
// being due before the lock was taken — neither is a failure, and a worker
// treating them as one would alert on its own normal operation.
type ResumeOutcome struct {
	Claimed       bool
	Result        OperationResult
	Rescheduled   bool
	NextAttemptAt time.Time
	// Woke is how many operations waiting on this one became due in the same
	// commit.
	Woke int
}

// DueCandidate is a parked operation that looks due, and the wallet it belongs
// to. The wallet comes back with it because the lock on that wallet has to be
// taken before the row is claimed.
type DueCandidate struct {
	TransactionID wagering.TransactionID
	WalletID      wagering.WalletID
}

// SettledOperation names the provider operation that other operations were
// parked on, within the wallet whose lock the caller already holds.
//
// The three travel as one because they are a single address — a provider's
// operation is identified by its external id only within that provider, and only
// the waiters on this wallet may be woken.
type SettledOperation struct {
	WalletID wagering.WalletID
	Provider wagering.Provider
	External wagering.ExternalTransactionID
}

// StoredTransaction is a wager transaction as read, with the correlation it
// arrived under.
//
// The correlation travels beside the transaction rather than on it because it is
// not something the domain models: it is how this request is traced, and a
// transaction carrying one would make the domain responsible for a transport
// concern it has no rule about.
type StoredTransaction struct {
	Transaction *wagering.WagerTransaction
	Correlation string
}
