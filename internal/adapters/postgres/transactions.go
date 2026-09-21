package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// transactionColumns is the projection every wager transaction read takes, in
// the order [transactionRow.dest] expects it.
const transactionColumns = `id, wallet_id, player_id, currency, kind, status, amount_minor, ` +
	`provider, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, ` +
	`reference_external_transaction_id, resolved_reference_id, correlation_id, ` +
	`result_balance_minor, failure_code, reference_attempts, reference_deadline, ` +
	`created_at, updated_at`

// insertTransactionColumns adds the worker's schedule, which is written but
// never read back into the domain: RehydrateWagerTransaction reads the
// deadline, which is the wait budget, and ignores the next attempt, which is
// only when to look again.
const insertTransactionColumns = transactionColumns + `, reference_next_attempt_at`

const (
	selectTransactionByID = `SELECT ` + transactionColumns +
		` FROM wagering.wager_transaction WHERE id = $1`
	// Provider-scoped in SQL rather than filtered afterwards: the provider is
	// half of the key, so a row belonging to somebody else is never read, never
	// mind never returned.
	selectTransactionByExternal = `SELECT ` + transactionColumns +
		` FROM wagering.wager_transaction WHERE provider = $1 AND external_transaction_id = $2`
	selectTransactionByKey = `SELECT ` + transactionColumns +
		` FROM wagering.wager_transaction WHERE provider = $1 AND idempotency_key = $2`

	insertTransaction = `INSERT INTO wagering.wager_transaction (` + insertTransactionColumns +
		`) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, ` +
		`$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`

	// ON CONFLICT DO NOTHING is how a duplicate is detected, and it is doing
	// three things at once.
	//
	// It detects in one round trip: there is no read before the insert, so
	// there is no window between deciding the keys are free and claiming them.
	//
	// It BLOCKS on an in-flight duplicate. PostgreSQL has to know whether the
	// other transaction's row will exist before it can know whether this insert
	// conflicts, so it waits for that transaction to end and then either does
	// nothing or inserts. The loser therefore observes the winner's decision
	// rather than guessing at it.
	//
	// And it leaves this transaction usable, where a plain insert meeting a
	// unique violation would abort it. The use case still re-reads in a new
	// transaction, because it has to: a true replay collides on the idempotency
	// key and on the provider's external id at once and the database names only
	// one of them, so which key was hit says nothing about whether this is a
	// replay or a conflict. Both are re-checked, in order, by the caller.
	//
	// No conflict target, because a target names one index and there are two
	// keys to lose on. The cost of naming none is that a collision on the row's
	// own primary key would also read as a duplicate submission — which would
	// mean a minted UUID had repeated, and is not a case worth a second
	// statement to tell apart.
	insertTransactionIfNew = insertTransaction + ` ON CONFLICT DO NOTHING`

	// Scoped to one wallet, which is the rule rather than an optimisation:
	// waking a waiter on another wallet writes outside the transaction's
	// declared write set, and the lock order exists to make that impossible.
	//
	// updated_at is deliberately not touched. It records when the operation
	// last changed, and being woken changes only the worker's schedule — the
	// schema keeps reference_deadline and reference_next_attempt_at apart for
	// exactly that reason.
	makeDue = `UPDATE wagering.wager_transaction SET reference_next_attempt_at = $4 ` +
		`WHERE wallet_id = $1 AND status = 'PENDING_REFERENCE' ` +
		`AND provider = $2 AND reference_external_transaction_id = $3`

	// A plain SELECT, taking no row lock. Claiming here and then wanting the
	// wallet closes a cycle with a submission that holds that wallet and wants
	// this row, and because a reversal must agree with its reference on the
	// wallet the two are the same wallet by construction. See ADR-0011.
	//
	// The order matches wager_transaction_due_idx, and the trailing id makes it
	// total: MakeDue wakes every waiter in one commit with one instant, so
	// ordering on the schedule alone would leave the choice to whatever order
	// the rows came back in.
	selectNextDue = `SELECT id, wallet_id FROM wagering.wager_transaction ` +
		`WHERE status = 'PENDING_REFERENCE' AND reference_next_attempt_at <= $1 ` +
		`ORDER BY reference_next_attempt_at, id LIMIT 1`

	// FOR NO KEY UPDATE for the same reason the wallet takes it: a reversal
	// resolving this row, and a ledger entry naming it, both take FOR KEY SHARE
	// through their foreign keys, and neither should queue behind a claim. It
	// still conflicts with another worker's claim, which is the only exclusion
	// this needs.
	//
	// Under READ COMMITTED the predicate is re-checked once the lock is
	// granted, so a row another worker settled while this one waited comes back
	// as no rows — which is the answer the port asks for.
	claimForUpdate = `SELECT ` + transactionColumns + ` FROM wagering.wager_transaction ` +
		`WHERE id = $1 AND status = 'PENDING_REFERENCE' AND reference_next_attempt_at <= $2 ` +
		`FOR NO KEY UPDATE`

	rescheduleTransaction = `UPDATE wagering.wager_transaction ` +
		`SET reference_next_attempt_at = $2 WHERE id = $1 AND status = 'PENDING_REFERENCE'`

	// FAILED leaves PENDING_REFERENCE, and
	// wager_transaction_only_waiting_is_scheduled is an equivalence, so the
	// schedule has to be cleared in the same statement that changes the status.
	failTransaction = `UPDATE wagering.wager_transaction ` +
		`SET status = $2, updated_at = $3, reference_next_attempt_at = NULL WHERE id = $1`
)

// selectReference reads the transaction an operation points at and whatever
// currently holds it, in one round trip.
//
// The hold comes from active_reversal rather than from a scan of everything
// that references the row, because active_reversal IS the answer to "what holds
// this?" — one indexed row per held reference, maintained by the trigger and by
// nothing else (ADR-0007). A released hold is deleted rather than marked, so a
// row that is there is a hold that still stands and ReversalView.Reversed is
// always false.
//
// The two shapes are unioned rather than joined so that every row has the same
// columns. A LEFT JOIN would leave the holder's non-nullable columns NULL when
// there is no hold, which is a shape the row scanner would have to learn to
// tell apart from a real one.
var selectReference = `WITH reference AS (SELECT ` + transactionColumns +
	` FROM wagering.wager_transaction WHERE provider = $1 AND external_transaction_id = $2) ` +
	`SELECT true, r.* FROM reference r ` +
	`UNION ALL SELECT false, ` + qualified("h", transactionColumns) +
	` FROM wagering.active_reversal a ` +
	`JOIN wagering.wager_transaction h ON h.id = a.reversal_id ` +
	`JOIN reference ON reference.id = a.reference_id`

// qualified puts a table alias in front of every name in a projection, so the
// projection is written once and read in two places.
func qualified(alias, columns string) string {
	names := strings.Split(columns, ", ")
	for i, name := range names {
		names[i] = alias + "." + name
	}
	return strings.Join(names, ", ")
}

// transactions reads and writes wager transactions.
type transactions struct{ tx pgx.Tx }

// ByID reads one operation by this system's identifier for it.
func (t transactions) ByID(
	ctx context.Context,
	id wagering.TransactionID,
) (*app.StoredTransaction, error) {
	return t.one(ctx, "read transaction by id", selectTransactionByID, uuidOf(id))
}

// ByExternal reads one operation by the provider's identifier for it.
func (t transactions) ByExternal(
	ctx context.Context,
	p wagering.Provider,
	e wagering.ExternalTransactionID,
) (*app.StoredTransaction, error) {
	return t.one(ctx, "read transaction by external id", selectTransactionByExternal,
		string(p), string(e))
}

// ByIdempotencyKey reads the operation a key is bound to.
func (t transactions) ByIdempotencyKey(
	ctx context.Context,
	p wagering.Provider,
	k wagering.IdempotencyKey,
) (*app.StoredTransaction, error) {
	return t.one(ctx, "read transaction by idempotency key", selectTransactionByKey,
		string(p), string(k))
}

// ReferenceFor builds the view of the transaction an operation points at.
//
// A reference that is not found is (nil, nil), never a NotFound: the domain
// distinguishes "named a reference that has not arrived" from "named none", and
// collapsing the first into a failure would settle an operation the processor
// should have parked.
func (t transactions) ReferenceFor(
	ctx context.Context,
	p wagering.Provider,
	e wagering.ExternalTransactionID,
) (*wagering.ReferenceView, error) {
	const what = "read reference"
	rows, err := t.tx.Query(ctx, selectReference, string(p), string(e))
	if err != nil {
		return nil, fail(what, err)
	}
	defer rows.Close()

	var view wagering.ReferenceView
	for rows.Next() {
		var (
			row         transactionRow
			isReference bool
		)
		if err := rows.Scan(append([]any{&isReference}, row.dest()...)...); err != nil {
			return nil, fail(what, err)
		}
		tx, err := row.transaction()
		if err != nil {
			return nil, corrupt(what, err)
		}
		if isReference {
			view.Transaction = tx
			continue
		}
		// Rehydrated as a real transaction rather than signalled by a flag:
		// ReferenceView.ActiveReversal skips a view whose Transaction is nil,
		// so a synthetic holder would put REFERENCE_ALREADY_REVERSED out of the
		// domain's reach and leave the rule living only in the schema.
		view.Reversals = append(view.Reversals, wagering.ReversalView{Transaction: tx})
	}
	if err := rows.Err(); err != nil {
		return nil, fail(what, err)
	}
	if view.Transaction == nil {
		return nil, nil
	}
	return &view, nil
}

// Record inserts a newly submitted operation, claiming both of its unique keys
// before any money work.
func (t transactions) Record(
	ctx context.Context,
	tx *wagering.WagerTransaction,
	correlation string,
) error {
	const what = "record transaction"
	tag, err := t.tx.Exec(ctx, insertTransactionIfNew,
		transactionArgs(snapshotOf(tx), correlation, time.Time{})...)
	if err != nil {
		return fail(what, err)
	}
	if tag.RowsAffected() == 0 {
		// One sentinel for both keys. Which index the insert lost on is not
		// something a caller may branch on, because a true replay loses on
		// both and the database reports whichever it checked first.
		return fmt.Errorf("%s %s: %w", what, tx.ID(), app.ErrDuplicateSubmission)
	}
	return nil
}

// MakeDue brings forward every operation on this wallet waiting for the named
// reference, and reports how many it woke.
func (t transactions) MakeDue(
	ctx context.Context,
	on app.SettledOperation,
	at time.Time,
) (int, error) {
	tag, err := t.tx.Exec(ctx, makeDue,
		uuidOf(on.WalletID), string(on.Provider), string(on.External), at)
	if err != nil {
		return 0, fail("wake waiting operations", err)
	}
	return int(tag.RowsAffected()), nil
}

// NextDue names the parked operation that has been due longest, or (nil, nil)
// when there is none.
func (t transactions) NextDue(ctx context.Context, at time.Time) (*app.DueCandidate, error) {
	const what = "find the next due operation"
	var id, walletID pgtype.UUID
	if err := t.tx.QueryRow(ctx, selectNextDue, at).Scan(&id, &walletID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fail(what, err)
	}
	return &app.DueCandidate{
		TransactionID: idFrom[wagering.TransactionID](id),
		WalletID:      idFrom[wagering.WalletID](walletID),
	}, nil
}

// ClaimForUpdate re-reads a candidate under its row lock and re-asserts that it
// is still parked and still due.
func (t transactions) ClaimForUpdate(
	ctx context.Context,
	id wagering.TransactionID,
	at time.Time,
) (*app.StoredTransaction, error) {
	return t.one(ctx, "claim a due operation", claimForUpdate, uuidOf(id), at)
}

// Reschedule moves a parked operation's next attempt and touches nothing else.
//
// Nothing else, including updated_at and the attempt count: this is called when
// carrying an operation forward failed, and nothing was spent, so counting an
// attempt would consume a budget that exists to bound how long a reference may
// take to arrive.
func (t transactions) Reschedule(
	ctx context.Context,
	id wagering.TransactionID,
	at time.Time,
) error {
	const what = "reschedule a parked operation"
	tag, err := t.tx.Exec(ctx, rescheduleTransaction, uuidOf(id), at)
	if err != nil {
		return fail(what, err)
	}
	if tag.RowsAffected() == 0 {
		return app.AsUnretryable(fmt.Errorf("%s: %s is not parked", what, id))
	}
	return nil
}

// Fail records a permanent infrastructure failure against a transaction the
// caller has already moved with WagerTransaction.Fail.
func (t transactions) Fail(ctx context.Context, tx *wagering.WagerTransaction) error {
	const what = "fail a transaction"
	tag, err := t.tx.Exec(ctx, failTransaction, uuidOf(tx.ID()), tx.Status().String(), tx.UpdatedAt())
	if err != nil {
		return fail(what, err)
	}
	if tag.RowsAffected() == 0 {
		return app.AsUnretryable(fmt.Errorf("%s: no transaction %s", what, tx.ID()))
	}
	return nil
}

// one runs a single-row transaction query, answering (nil, nil) for no rows.
func (t transactions) one(
	ctx context.Context,
	what, query string,
	args ...any,
) (*app.StoredTransaction, error) {
	var row transactionRow
	if err := t.tx.QueryRow(ctx, query, args...).Scan(row.dest()...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fail(what, err)
	}
	stored, err := row.stored()
	if err != nil {
		return nil, corrupt(what, err)
	}
	return stored, nil
}

// transactionArgs renders a snapshot for [insertTransaction], in the order
// [insertTransactionColumns] names.
//
// A transaction with no provider side writes NULL into all six of the columns
// the provider owns, which is the count
// wager_transaction_origin_carries_its_fields checks: none for an opening, all
// six otherwise, and nothing in between.
func transactionArgs(
	snap wagering.TransactionSnapshot,
	correlation string,
	nextAttemptAt time.Time,
) []any {
	var external wagering.ExternalSnapshot
	if snap.External != nil {
		external = *snap.External
	}
	return []any{
		uuidOf(snap.ID),
		uuidOf(snap.WalletID),
		string(snap.PlayerID),
		snap.Money.Currency().String(),
		snap.Kind.String(),
		snap.Status.String(),
		minorOf(snap.Money),
		textOf(external.Provider),
		textOf(external.ExternalTransactionID),
		textOf(external.IdempotencyKey),
		textOf(external.PayloadHash),
		textOf(external.RoundID),
		textOf(external.GameID),
		textOf(external.ReferenceExternalTransactionID),
		nullableUUIDOf(external.ResolvedReferenceID),
		correlation,
		nullableMinorOf(snap.Result),
		textOf(snap.FailureCode),
		snap.ReferenceAttempts,
		timeOf(snap.ReferenceDeadline),
		snap.CreatedAt,
		snap.UpdatedAt,
		timeOf(nextAttemptAt),
	}
}

// snapshotOf reads the stored shape off a transaction.
//
// It is the inverse of [transactionRow.transaction] and exists because the
// domain hides its fields: a write has to ask the accessors, and asking them in
// one place keeps a column from being written from the wrong one.
func snapshotOf(tx *wagering.WagerTransaction) wagering.TransactionSnapshot {
	snap := wagering.TransactionSnapshot{
		ID:                tx.ID(),
		WalletID:          tx.WalletID(),
		PlayerID:          tx.PlayerID(),
		Kind:              tx.Kind(),
		Money:             tx.Money(),
		Status:            tx.Status(),
		CreatedAt:         tx.CreatedAt(),
		UpdatedAt:         tx.UpdatedAt(),
		ReferenceAttempts: tx.ReferenceAttempts(),
	}
	if result, ok := tx.Result(); ok {
		snap.Result = &result
	}
	// Both of these report the zero value when they report false, so assigning
	// unconditionally says exactly what a guard would have.
	snap.FailureCode, _ = tx.FailureCode()
	snap.ReferenceDeadline, _ = tx.ReferenceDeadline()

	if !tx.IsExternal() {
		return snap
	}
	provider, _ := tx.Provider()
	externalID, _ := tx.ExternalTransactionID()
	key, _ := tx.IdempotencyKey()
	hash, _ := tx.PayloadHash()
	round, _ := tx.RoundID()
	game, _ := tx.GameID()
	reference, _ := tx.ReferenceExternalTransactionID()
	resolved, _ := tx.ResolvedReferenceID()
	snap.External = &wagering.ExternalSnapshot{
		Provider:                       provider,
		ExternalTransactionID:          externalID,
		IdempotencyKey:                 key,
		PayloadHash:                    hash,
		RoundID:                        round,
		GameID:                         game,
		ReferenceExternalTransactionID: reference,
		ResolvedReferenceID:            resolved,
	}
	return snap
}
