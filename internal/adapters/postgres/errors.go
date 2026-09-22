package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// ErrLostUpdate reports a wallet whose stored version was not the one the
// movement was computed from, so the write was refused rather than applied.
//
// It is this package's own rather than one of the port's sentinels because the
// port has no name for it: the application layer takes the wallet lock before
// it computes anything, so a lost update is not a condition it has a branch
// for. Reaching this means the lock was not held — and refusing the write is
// the only safe answer, because the balance the movement was derived from is
// not the balance the wallet holds.
//
// It is classified Retryable. The caller may send the operation again and get
// the right answer from the current balance; what it may not do is have this
// one applied.
var ErrLostUpdate = errors.New("postgres: wallet changed under the movement")

// portErrors maps the rules the use cases branch on to the sentinels they
// branch with.
//
// The key is the constraint name and never the SQLSTATE. Every rule in this
// schema names itself in the same field whether it is enforced by a unique
// index, a foreign key, a CHECK or a trigger, so this mapping survives a rule
// moving from one mechanism to another — which docs/schema.md states is not a
// change to the contract.
//
// Both unique keys on wager_transaction map to the one sentinel, deliberately.
// A true replay collides on the idempotency key and on the provider's external
// id at once and the database names whichever index it happened to check first,
// so a caller branching on the name would be correct only for the order the
// indexes were created in. The third entry is the same pair carrying the row's
// own id, which exists as the reference foreign key's target and can only be
// violated when the pair is; it is here because "whichever it checked first"
// includes that one.
//
// The two reversal rules map to one sentinel for a related reason. A reference
// is refused a second reversal either because one still holds it
// (active_reversal_pkey) or because one of this kind already succeeded, undone
// or not (wager_transaction_one_successful_reversal_per_kind). The domain
// answers both with REFERENCE_ALREADY_REVERSED from the reference view, so
// reaching either index means the same thing — the view was built wrongly —
// and the caller has one branch for it.
var portErrors = map[string]error{
	"wager_transaction_provider_external_key":            app.ErrDuplicateSubmission,
	"wager_transaction_provider_idempotency_key":         app.ErrDuplicateSubmission,
	"wager_transaction_provider_external_id_key":         app.ErrDuplicateSubmission,
	"wallet_player_currency_key":                         app.ErrWalletExists,
	"active_reversal_pkey":                               app.ErrReferenceAlreadyReversed,
	"wager_transaction_one_successful_reversal_per_kind": app.ErrReferenceAlreadyReversed,
}

// transientRules are the rules whose refusal means "somebody else got there
// first", where sending the work again produces a correct and different answer.
//
// There is one. Two consumers may legitimately see one message, and the loser
// of that race blocks on the inbox's primary key and is then told the row is
// taken — at which point the message it was handling has been handled, the
// redelivery will find the row and replay the settled result, and nothing it
// did was persisted. Every other rule in this schema refuses something that
// will be refused again.
var transientRules = map[string]bool{
	"inbox_pkey": true,
}

// transientCodes are the SQLSTATEs that mean the database declined this attempt
// rather than this work, on a connection that was ESTABLISHED.
//
// A refusal at connect time needs no entry here and must not have one: it is
// answered structurally in [transient], because the same code can mean one
// thing to a connection attempt and the opposite to a statement. Several of the
// entries below can only arrive at connect time and are kept as belt and
// braces rather than because that path depends on them.
//
// query_canceled is in the list and is the one worth explaining, because it
// arrives from two different places and this package deliberately does not try
// to tell them apart for the purpose of classifying. PostgreSQL raises it when
// statement_timeout fires, and pgx returns it — or the context error, depending
// which side noticed first — when the caller cancels. lock_timeout is the third
// case and is NOT this code: PostgreSQL raises lock_not_available for it, which
// is why both appear here.
//
// The distinction that matters to an operator is whether the query was
// abandoned by the server or by the client, and it is answerable: the caller's
// context is done in the second case and not in the first. It is not answerable
// from the SQLSTATE, and it does not change the answer — all three recorded
// nothing and all three may succeed on a second attempt — so it belongs in a
// log line and not in this switch.
var transientCodes = map[string]bool{
	pgerrcode.SerializationFailure: true,
	pgerrcode.DeadlockDetected:     true,
	pgerrcode.LockNotAvailable:     true,
	pgerrcode.QueryCanceled:        true,
	pgerrcode.TooManyConnections:   true,
	pgerrcode.AdminShutdown:        true,
	pgerrcode.CrashShutdown:        true,
	pgerrcode.CannotConnectNow:     true,
	pgerrcode.IdleSessionTimeout:   true,
}

// connectionExceptionClass is SQLSTATE class 08, every member of which is the
// connection failing rather than the statement being wrong. It is matched as a
// class because the class is closed and its members all mean the same thing to
// a caller.
const connectionExceptionClass = "08"

// fail turns a driver error into the error a use case is entitled to see.
//
// Three things happen here and the order is the contract:
//
//  1. A rule the use cases branch on becomes its sentinel, wrapped so that
//     errors.Is still finds it after the manager has rolled back and returned
//     it. The driver error is wrapped alongside rather than discarded, because
//     the constraint name is what an operator reads to find out which rule
//     refused the write.
//  2. Anything the database declined rather than refused is marked Retryable.
//  3. Everything else is Unretryable. An unclassified error is never an
//     invitation to retry: a failure nobody recognised is not one anybody has
//     established is safe to repeat.
//
// what names the work in progress, so a failure says which statement it came
// from without the caller having to recognise the SQL.
func fail(what string, err error) error {
	if err == nil {
		return nil
	}
	// Only a refusal that names a rule is looked up, and the guard is
	// structural rather than decorative: a connect-time refusal carries a
	// PgError with no constraint name, and without this the maps would be
	// asked about "" on the one path where the answer must come from
	// [transient] instead.
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.ConstraintName != "" {
		if sentinel, mapped := portErrors[pgErr.ConstraintName]; mapped {
			// Two %w: the sentinel for the use case to branch on, and the
			// driver error so the rule that refused the write survives into
			// whatever logs this.
			return fmt.Errorf("%s: %w: %w", what, sentinel, err)
		}
		if transientRules[pgErr.ConstraintName] {
			return app.AsRetryable(fmt.Errorf("%s: %w", what, err))
		}
	}
	if transient(err) {
		return app.AsRetryable(fmt.Errorf("%s: %w", what, err))
	}
	return app.AsUnretryable(fmt.Errorf("%s: %w", what, err))
}

// transient reports whether err is the database or the connection declining
// this attempt rather than refusing this work.
func transient(err error) bool {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.Is(err, pgx.ErrTxClosed), errors.Is(err, pgx.ErrTxCommitRollback):
		// The transaction ended under the statement. That is a connection that
		// went away or a manager bug, and in neither case did the statement
		// record anything.
		return true
	}

	// A failure to CONNECT is transient, whatever the server refused with.
	//
	// The order matters and is the whole of this function. A ConnectError
	// WRAPS the refusal it was handed, so a SQLSTATE test placed above this one
	// finds the inner PgError, answers from its code, and returns — and the
	// connect error is never reached in exactly the case it was written for.
	// That is not hypothetical: it sent every in-flight wager to a dead-letter
	// queue the first time a database was closed for maintenance.
	//
	// Unconditional, because the question Class asks is whether the attempt
	// recorded anything, and a connection that was never established carried no
	// statement that could have. That half does not depend on the code at all.
	//
	// And a code cannot answer it anyway, which is why this is structural
	// rather than a longer list. PostgreSQL refuses a closed database with
	// 55000 — the same 55000 that means object_not_in_prerequisite_state when
	// a statement raises it, which is permanent. The SQLSTATE is identical and
	// the right answer is opposite; only the fact that the connection was never
	// established tells the two apart, and that is what this wrapper is.
	//
	// The cost is named rather than hidden: an invalid password (28P01) is
	// transient here too, and will be retried by a service that can never
	// succeed. That is still the better answer. What Unretryable means to the
	// caller on the queue path is "leave this for the redrive policy", which
	// puts a provider's wager in the dead-letter queue — and a wrong password
	// in this service's own configuration is not a defect in that wager. The
	// redrive policy bounds the retrying either way; the difference is whether
	// the operation is shed silently or after an operator has watched the same
	// connection failure repeat.
	if _, refused := errors.AsType[*pgconn.ConnectError](err); refused {
		return true
	}

	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return transientCodes[pgErr.Code] || strings.HasPrefix(pgErr.Code, connectionExceptionClass)
	}
	return false
}

// lockTimeout reports a statement that gave up waiting for a row lock.
//
// PostgreSQL raises lock_not_available for it, and nothing else in this system
// does: the wallet lock is the only lock a command takes, and statement_timeout
// firing is query_canceled rather than this. That exactness is why it is worth
// counting on its own — see [TxManager.contention] for what the two contention
// counts mean apart.
//
// The chain is walked rather than the head examined, because by the time this
// is asked the error has been through [fail] and is a classified error wrapping
// the driver's.
func lockTimeout(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == pgerrcode.LockNotAvailable
}

// corrupt reports a stored row the domain refused to load.
//
// It is Unretryable whatever the refusal's own code says, and classifying it
// here rather than letting the code speak is the point. A rehydration failure
// can carry a correctable code — an identifier with surrounding whitespace, an
// amount that is wrong for its kind — and correctable means "repair the payload
// and send it again", which is an instruction nobody can act on for a row that
// is already stored. The code stays reachable through the chain for whoever
// investigates; the class says what the caller should do.
func corrupt(what string, err error) error {
	return app.AsUnretryable(fmt.Errorf("%s: %w", what, err))
}
