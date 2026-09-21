package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// errNegativeVersion reports a stored version that cannot be one: the schema
// keeps versions in a signed bigint and the domain holds them unsigned, so the
// conversion is the one place a negative could become an enormous positive.
var errNegativeVersion = errors.New("postgres: stored version is negative")

// The ports this package implements, asserted at compile time so that a change
// to one of them is a build failure here rather than a wiring failure in main.
var (
	_ app.TxManager        = (*TxManager)(nil)
	_ app.WalletStore      = wallets{}
	_ app.TransactionStore = transactions{}
	_ app.LedgerReader     = ledger{}
	_ app.InboxStore       = inbox{}
	_ app.OutboxWriter     = outbox{}
)

// TxManager opens and delimits the one transaction a command runs in.
//
// It holds the pool rather than a connection, so a command takes a connection
// for exactly as long as its transaction lasts. Nothing here exposes Begin or
// Commit: the only way to reach a repository is to be inside a callback, which
// is what makes "a repository cannot run outside a transaction" a fact about
// the types rather than a rule somebody has to follow.
type TxManager struct {
	pool             *pgxpool.Pool
	lockTimeout      time.Duration
	statementTimeout time.Duration
}

// TxConfig is what a transaction manager is built from.
//
// The two timeouts are separate because they bound different failures. A lock
// timeout bounds waiting for another command's wallet lock, which is normal
// contention and should be short; a statement timeout bounds a query that has
// started and will not finish, which is a different fault and a different
// budget. One value for both would force the lock wait to be as generous as the
// slowest legitimate query.
type TxConfig struct {
	// Pool is where a transaction's connection comes from, and it is held for
	// exactly as long as the transaction lasts.
	Pool *pgxpool.Pool
	// LockTimeout bounds how long a statement waits for a row lock.
	LockTimeout time.Duration
	// StatementTimeout bounds how long a statement may run once it is running.
	StatementTimeout time.Duration
}

// NewTxManager wires the transaction manager.
//
// Every dependency is refused when absent, at construction rather than at the
// first command: a manager built without a pool would fail when a provider was
// already waiting, and one built without timeouts would hold a wallet lock for
// as long as the network allowed.
func NewTxManager(cfg TxConfig) (*TxManager, error) {
	switch {
	case cfg.Pool == nil:
		return nil, errors.New("postgres: a transaction manager needs a pool")
	case cfg.LockTimeout <= 0:
		return nil, fmt.Errorf("postgres: a lock timeout must be positive, got %s", cfg.LockTimeout)
	case cfg.StatementTimeout <= 0:
		return nil, fmt.Errorf("postgres: a statement timeout must be positive, got %s",
			cfg.StatementTimeout)
	}
	return &TxManager{
		pool:             cfg.Pool,
		lockTimeout:      cfg.LockTimeout,
		statementTimeout: cfg.StatementTimeout,
	}, nil
}

// movementOptions is the isolation every command that moves money runs at.
//
// READ COMMITTED, not REPEATABLE READ: the wallet row lock is what serialises
// two commands on one wallet, and a higher isolation level would add
// serialization failures to a path that is already exclusive. The lock is the
// design; the isolation level only has to not get in its way.
var movementOptions = pgx.TxOptions{
	IsoLevel:   pgx.ReadCommitted,
	AccessMode: pgx.ReadWrite,
}

// snapshotOptions is one consistent view and no possibility of a write.
//
// REPEATABLE READ is what makes a reconciliation honest: the stored balance and
// the ledger it is compared against are read at one instant rather than two,
// so a movement landing between them cannot be reported as a divergence. READ
// ONLY is the second half — the database refuses a write here, so a reader that
// somehow reached a writing statement is stopped by the server as well as by
// the types.
var snapshotOptions = pgx.TxOptions{
	IsoLevel:   pgx.RepeatableRead,
	AccessMode: pgx.ReadOnly,
}

// WithinMovement runs fn in a READ COMMITTED, READ WRITE transaction.
func (m *TxManager) WithinMovement(
	ctx context.Context,
	fn func(context.Context, *app.Repos) error,
) error {
	return m.within(ctx, movementOptions, func(ctx context.Context, tx pgx.Tx) error {
		w := &writer{tx: tx}
		return fn(ctx, &app.Repos{
			Wallets:      wallets{tx: tx},
			Transactions: transactions{tx: tx},
			Inbox:        inbox{tx: tx},
			Outbox:       outbox{tx: tx},
			Open:         w.open,
			Settle:       w.settle,
		})
	})
}

// WithinSnapshot runs fn in a REPEATABLE READ, READ ONLY transaction.
func (m *TxManager) WithinSnapshot(
	ctx context.Context,
	fn func(context.Context, *app.ReadRepos) error,
) error {
	return m.within(ctx, snapshotOptions, func(ctx context.Context, tx pgx.Tx) error {
		return fn(ctx, &app.ReadRepos{
			Wallets:      wallets{tx: tx},
			Transactions: transactions{tx: tx},
			Ledger:       ledger{tx: tx},
		})
	})
}

// applyTimeouts is one round trip that bounds every statement the transaction
// will run.
//
// set_config with is_local true rather than SET LOCAL, because SET takes no
// placeholder: the values would have to be pasted into the statement text, and
// a configured duration is not user input today but the next person to add a
// setting here should not have to notice that it matters.
const applyTimeouts = `SELECT set_config('lock_timeout', $1, true), ` +
	`set_config('statement_timeout', $2, true)`

// within opens a transaction, runs body, and commits or rolls back.
//
// The rollback is deferred, which covers three exits with one statement: an
// error from body, a panic, and a commit that failed. A panic therefore rolls
// back and then carries on unwinding — there is no recover here, because
// recovering and re-panicking would replace the original stack with this
// function's, and the stack is most of what a panic is worth.
//
// The callback's error is returned exactly as it left the callback. The
// rollback's own error is discarded, and that is deliberate rather than
// careless: replacing the callback's error would break the errors.Is the use
// cases do on ErrDuplicateSubmission, ErrWalletExists and
// ErrReferenceAlreadyReversed, and nothing about a rollback failing changes
// what the command came to. A failed rollback also destroys the connection
// rather than returning it to the pool, which is pgx's behaviour and the right
// one.
//
// # The fourth exit, which is the ambiguous one
//
// A commit that fails having lost the connection does not say whether the
// server committed. The COMMIT may have been applied and the acknowledgement
// lost, so this returns an error for work that possibly landed — and classifies
// it Retryable, which invites the caller to send it again.
//
// That is safe here, and only because of what the schema does with the second
// attempt. A submission carries an idempotency key and a provider's external
// id, both unique, so a retry of work that did commit loses on them and is
// reported as ErrDuplicateSubmission — which the use case resolves by reading
// what is stored rather than by applying anything. The retry cannot double a
// balance; it can only discover which of the two outcomes actually happened.
// A movement with no such key would make this classification wrong, and there
// is none: every write this manager commits is reached through a door that
// claims one first.
func (m *TxManager) within(
	ctx context.Context,
	options pgx.TxOptions,
	body func(context.Context, pgx.Tx) error,
) error {
	tx, err := m.pool.BeginTx(ctx, options)
	if err != nil {
		return fail("begin", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// Detached from the caller's context so that a cancelled request still
		// issues its ROLLBACK, and bounded so that a server which has stopped
		// answering cannot hold this goroutine. Without the detachment pgx
		// would close the connection instead of rolling back, which works but
		// throws away a connection on every cancellation.
		unwind, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.statementTimeout)
		defer cancel()
		_ = tx.Rollback(unwind)
	}()

	if _, err := tx.Exec(ctx, applyTimeouts,
		milliseconds(m.lockTimeout), milliseconds(m.statementTimeout)); err != nil {
		return fail("bound the transaction", err)
	}

	if err := body(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fail("commit", err)
	}
	committed = true
	return nil
}

// milliseconds renders a duration the way PostgreSQL's timeout settings read
// one. They are counts of milliseconds, and a bare integer is the spelling that
// needs no unit parsing at the other end.
//
// It rounds UP, and that is the whole of this function.
//
// Zero does not mean "immediately" to PostgreSQL, it means DISABLED. Truncating
// would therefore turn any timeout under a millisecond into no timeout at all —
// silently, through a configuration NewTxManager accepted, producing exactly
// the condition its own documentation says it refuses a missing timeout to
// prevent: a wallet lock held for as long as the network allows. Rounding up
// makes that unreachable, because every positive duration renders as at least
// one millisecond.
//
// Rounding rather than refusing anything below a millisecond, because refusing
// only moves the boundary: 1500µs would still be silently truncated to 1ms on
// the other side of it. Rounding up is the only rule under which no
// configuration is quietly weakened.
func milliseconds(d time.Duration) string {
	const unit = time.Millisecond
	return strconv.FormatInt(int64((d+unit-1)/unit), 10)
}
