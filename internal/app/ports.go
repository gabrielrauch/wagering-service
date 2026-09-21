package app

import (
	"context"
	"errors"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The conditions a store reports by value rather than by message, because the
// use case branches on them.
//
// Each is a single sentinel covering every constraint that expresses the rule,
// deliberately. A duplicate submission collides on the idempotency key and on
// the provider's external id at once, and a database names only one of them —
// whichever index it happened to check first. Code that branched on the name
// would be correct only for the order the indexes were created in, so the port
// does not offer the name to branch on.
//
// # What an implementation must guarantee
//
// Each of these is matched with [errors.Is] on an error that has travelled back
// out through [TxManager], so an implementation must return them so that
// errors.Is still finds them there. Returning one directly, or wrapping it with
// %w, both do; wrapping it with %v, or replacing the callback's error with a
// rollback error of the manager's own, do not — and nothing fails loudly when
// they do not. The use case simply stops recognising the condition: a duplicate
// submission is reported as a generic failure instead of being resolved to the
// operation that won, so a race no provider should ever see becomes an error one
// does.
//
// Which door raises which:
//
//   - [TransactionStore.Record] raises ErrDuplicateSubmission.
//   - [Repos.Open] raises ErrWalletExists.
//   - [Repos.Settle] raises ErrReferenceAlreadyReversed.
var (
	// ErrDuplicateSubmission reports a submission colliding with one already
	// recorded, on either of the two keys that make a submission unique.
	ErrDuplicateSubmission = errors.New("app: submission already recorded")
	// ErrWalletExists reports a wallet already open for a player and currency.
	ErrWalletExists = errors.New("app: wallet already exists")
	// ErrReferenceAlreadyReversed reports a reference that already carries an
	// active reversal.
	//
	// Reaching it means the reference view was built wrongly: the domain decides
	// this under the wallet lock and rejects it properly, with a row and an
	// event. This is the backstop, and arriving here is worth investigating.
	ErrReferenceAlreadyReversed = errors.New("app: reference already reversed")
)

// Clock is where time comes from. Implementations return UTC truncated to
// microseconds, because that is the resolution timestamptz keeps and a value
// that loses precision on the way to storage would not compare equal to itself
// when read back.
type Clock interface {
	Now() time.Time
}

// IDs mints the identifiers this system owns.
//
// It is a port rather than a call to the domain's minting functions so that a
// test can fix them. Nothing here ever derives an identifier from input.
type IDs interface {
	WalletID() wagering.WalletID
	TransactionID() wagering.TransactionID
	LedgerEntryID() wagering.LedgerEntryID
	EventID() EventID
}

// ReconciliationObserver is told when a wallet's books do not balance.
//
// It returns nothing and cannot fail the read: reconciliation reports, and a
// metric or a log failing to record a divergence must not turn a successful
// finding into an error.
type ReconciliationObserver interface {
	WalletDiverged(ctx context.Context, d Divergence)
}

// DefectObserver is told when a parked operation could not be carried forward
// for a reason that is this system's fault rather than the provider's.
//
// It exists because that case has nowhere else to go. The provider is not
// waiting on the resume path, the row stays parked rather than failing, and
// without a hook the condition would be invisible until the wait budget ran out
// and the operation was rejected for a reason that was never true.
type DefectObserver interface {
	CannotCarryForward(ctx context.Context, id wagering.TransactionID, err error)
}

// TxManager opens and delimits the one transaction a command runs in.
//
// The callback form is deliberate: there is no Begin and no Commit to call, so a
// use case cannot leak a transaction, commit one twice, or hold one open across a
// return. The manager commits when the callback returns nil and rolls back when
// it does not, and the transaction exists only for as long as the callback runs.
//
// The transaction is not carried in the context. A repository reached through the
// bundle is already bound to the open transaction, which means a repository
// obtained anywhere else cannot accidentally run outside it — the usual failure
// of the context-carried approach, where the compiler has nothing to check.
type TxManager interface {
	// WithinMovement runs fn in a READ COMMITTED, READ WRITE transaction: the
	// isolation every command that moves money uses, with the wallet row lock
	// doing the serialising rather than the isolation level.
	WithinMovement(ctx context.Context, fn func(context.Context, *Repos) error) error
	// WithinSnapshot runs fn in a REPEATABLE READ, READ ONLY transaction: one
	// consistent view, no locks, and no possibility of a write.
	WithinSnapshot(ctx context.Context, fn func(context.Context, *ReadRepos) error) error
}

// ReadRepos is what a snapshot transaction can reach: readers only, so a query
// that tried to write would not compile.
type ReadRepos struct {
	Wallets      WalletReader
	Transactions TransactionReader
	Ledger       LedgerReader
}

// Repos is what a movement transaction can reach.
//
// # Lock order
//
// A movement transaction's whole write set is ONE wallet's rows plus that
// wallet's outbox counter, and it takes them in this order:
//
//	inbox -> wallet lock -> wager_transaction -> (trigger: active_reversal) -> outbox
//
// Outbox.Append is therefore the last statement of every callback, without
// exception. The outbox keeps a per-aggregate sequence counter, which is a second
// per-wallet serialisation point: removing the lock upgrade did not remove the
// obligation to take the two in a fixed order, and taking the counter first while
// another transaction holds the wallet and wants the counter is a deadlock.
//
// Wallets.Open is the one exemption from the wallet lock, because the row it
// would lock is the row it is creating.
type Repos struct {
	Wallets      WalletStore
	Transactions TransactionStore
	Inbox        InboxStore
	Outbox       OutboxWriter

	// Open writes a new wallet, and the opening transaction and its ledger entry
	// when the wallet was opened with money in it.
	//
	// It does NOT write the opening's events. Those are appended through Outbox
	// like every other event, last, because the outbox is a per-wallet
	// serialisation point and folding it into this door would hide that.
	//
	// Open and Settle are the only two doors onto the write path, and they are
	// functions on the bundle rather than methods spread across the stores for a
	// reason worth stating: a transaction, a wallet and a ledger entry written by
	// three independent callers is a convention, and a convention is exactly what
	// produced a PROCESSED bet reporting a balance the wallet never held, with no
	// ledger entry, that reconciliation still called consistent. One door makes
	// the three arrive together or not at all.
	Open func(ctx context.Context, w *wagering.Wallet, o wagering.Outcome, correlation string) error

	// Settle writes what an outcome produced: the transaction's new state, and
	// the wallet and ledger entry when money moved.
	//
	// nextAttemptAt is set exactly when the outcome's transaction is
	// PENDING_REFERENCE, and the zero time means NULL the column. The two are an
	// equivalence in the schema, so any write that leaves PENDING_REFERENCE must
	// clear the schedule in the same statement.
	//
	// It crosses as a value rather than as a *time.Time, so there is no pointee
	// for a store to retain and a caller to keep reading: the same reason
	// [Trace] and [SettledOperation] cross as values. Absence is the zero time,
	// which is the convention WagerTransaction.ReferenceDeadline and
	// BackoffPolicy.next already use.
	Settle func(ctx context.Context, o wagering.Outcome, nextAttemptAt time.Time) error
}

// WalletReader reads wallets. A wallet that does not exist is (nil, nil) rather
// than an error: absence is an answer every caller here has a branch for, and
// putting it in the error channel would mean every caller had to tell it apart
// from a database that is down.
type WalletReader interface {
	ByID(ctx context.Context, id wagering.WalletID) (*wagering.Wallet, error)
	ByKey(ctx context.Context, key wagering.WalletKey) (*wagering.Wallet, error)
}

// WalletStore reads wallets and takes the lock that serialises movement on one.
type WalletStore interface {
	WalletReader
	// LockForMovement takes the wallet's row lock and returns the wallet as it
	// stands under that lock, or (nil, nil) when there is none.
	//
	// It is an explicit operation rather than something a repository does on the
	// way past, because where in the transaction the lock is taken is the whole
	// of the concurrency design, and that decision belongs to the use case that
	// can see the rest of the statements.
	LockForMovement(ctx context.Context, key wagering.WalletKey) (*wagering.Wallet, error)
	// LockByID takes the same lock on a wallet already known by identifier.
	//
	// Two doors rather than one because the two write paths know the wallet
	// differently: a submission has a player and a currency and nothing else,
	// while the resume worker has read a row that names the wallet outright.
	// Making the worker look the key up in order to lock by it would add a read
	// whose only purpose is to restate something it already has.
	LockByID(ctx context.Context, id wagering.WalletID) (*wagering.Wallet, error)
}

// TransactionReader reads wager transactions. As with wallets, not found is
// (nil, nil).
type TransactionReader interface {
	ByID(ctx context.Context, id wagering.TransactionID) (*StoredTransaction, error)
	ByExternal(ctx context.Context, p wagering.Provider, e wagering.ExternalTransactionID) (*StoredTransaction, error)
	ByIdempotencyKey(ctx context.Context, p wagering.Provider, k wagering.IdempotencyKey) (*StoredTransaction, error)
	// ReferenceFor builds the view of the transaction an operation points at:
	// the reference itself, and the reversals pointing back at it.
	//
	// The active reversal must be rehydrated as a real transaction, not
	// signalled by a flag. ReferenceView.ActiveReversal skips a view whose
	// Transaction is nil, so a synthetic holder makes REFERENCE_ALREADY_REVERSED
	// unreachable in the domain — the rule would still be in the schema, and
	// nowhere else.
	//
	// A reference that is not found is (nil, nil), never NotFound. The domain
	// distinguishes "named a reference that has not arrived" from "named none",
	// and an error here would collapse the first into a failure before the
	// processor ever got to park the operation.
	ReferenceFor(
		ctx context.Context,
		p wagering.Provider,
		e wagering.ExternalTransactionID,
	) (*wagering.ReferenceView, error)
}

// TransactionStore reads and writes wager transactions.
type TransactionStore interface {
	TransactionReader
	// Record inserts a newly submitted operation, in PENDING. It claims the
	// idempotency key and the provider's external id before any money work, so
	// that a duplicate loses here rather than after moving a balance.
	Record(ctx context.Context, tx *wagering.WagerTransaction, correlation string) error
	// MakeDue brings forward every operation on this wallet that is waiting for
	// the named reference, and reports how many it woke.
	//
	// It is scoped to the wallet the caller already holds the lock on. Waking a
	// row on another wallet would be writing outside this transaction's declared
	// write set, which is what the lock order exists to prevent.
	MakeDue(ctx context.Context, on SettledOperation, at time.Time) (int, error)
	// NextDue names the parked operation that has been due longest, as a PLAIN
	// SELECT taking no row lock, and is (nil, nil) when there is none.
	//
	// Not locking here is load-bearing. Claiming the row first and then wanting
	// its wallet closes a cycle with a submission that holds that wallet and
	// wants the row, and the reference rules force both onto the same wallet, so
	// it is the common case rather than a corner of one.
	//
	// One candidate rather than a page, because a movement transaction's write
	// set is one wallet's rows and Resume claims at most one operation per call
	// — see docs/adr/0011. A slice would advertise a batch no caller may take,
	// and would spell absence as len == 0 where every other port here spells it
	// (nil, nil).
	//
	// Ties are broken by transaction id. Operations woken in one commit share an
	// instant exactly, so ordering on the schedule alone leaves the choice to
	// whatever order the rows came back in.
	NextDue(ctx context.Context, at time.Time) (*DueCandidate, error)
	// ClaimForUpdate re-reads a candidate under its row lock and re-asserts that
	// it is still parked and still due, returning (nil, nil) when it is not.
	ClaimForUpdate(ctx context.Context, id wagering.TransactionID, at time.Time) (*StoredTransaction, error)
	// Reschedule moves a parked operation's next attempt and touches nothing
	// else.
	//
	// It exists so that a failure to carry an operation forward does not have to
	// be written through the settle door. Settling restates the whole row from a
	// transaction, and the transaction here is exactly the one that could not be
	// carried forward — writing it back would count an attempt against a budget
	// that nothing was actually spent from.
	Reschedule(ctx context.Context, id wagering.TransactionID, at time.Time) error
	// Fail records a permanent infrastructure failure against a transaction the
	// caller has already moved with WagerTransaction.Fail.
	Fail(ctx context.Context, tx *wagering.WagerTransaction) error
}

// LedgerReader reads a wallet's ledger.
type LedgerReader interface {
	// Page returns entries after a wallet version, ascending, at most limit of
	// them. (walletId, walletVersion) orders the ledger exactly, which is what
	// makes the cursor stable.
	Page(ctx context.Context, w wagering.WalletID, afterVersion uint64, limit int) ([]wagering.WalletLedgerEntry, error)
	// All returns every entry for a wallet, for reconciliation. The sum is
	// order-independent, so this one does not promise an order.
	All(ctx context.Context, w wagering.WalletID) ([]wagering.WalletLedgerEntry, error)
}

// InboxReader reads the record of a message already handled.
type InboxReader interface {
	Find(ctx context.Context, k InboxKey) (*InboxRecord, error)
}

// InboxStore reads and records handled messages.
type InboxStore interface {
	InboxReader
	// Record writes the message as handled, in the same transaction as the work
	// it describes. There is no second write marking it complete: the row and the
	// domain changes commit together or not at all, so a row that exists is a
	// message that was handled.
	Record(ctx context.Context, m InboxMessage, at time.Time) error
}

// OutboxWriter appends events for a separate publisher to pick up. Nothing in
// this layer publishes.
type OutboxWriter interface {
	Append(ctx context.Context, envelopes []Envelope) error
}
