package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

var insertWallet = insertInto("wagering.wallet", walletColumns)

const (
	// Settling is the only thing an update is for, so this statement names
	// exactly the columns settling produces and nothing else. Everything the
	// operation IS — the wallet, the player, the currency, the kind, the
	// amount, the whole provider side and the correlation it arrived under — is
	// absent, and wager_transaction_guard refuses a write that tried to restate
	// any of it.
	//
	// The schedule is written here rather than by a second statement because
	// wager_transaction_only_waiting_is_scheduled is an equivalence: leaving
	// PENDING_REFERENCE must clear the schedule in the same statement that
	// changes the status, or the row is momentarily a shape the schema forbids.
	settleTransaction = `UPDATE wagering.wager_transaction SET ` +
		`status = $2, result_balance_minor = $3, failure_code = $4, ` +
		`resolved_reference_id = $5, reference_attempts = $6, reference_deadline = $7, ` +
		`reference_next_attempt_at = $8, updated_at = $9 WHERE id = $1`

	// The wallet's new state, conditioned on the version the movement was
	// computed from.
	//
	// That condition is the lost-update detector. The wallet lock should have
	// made it unreachable — two movements on one wallet queue on FOR NO KEY
	// UPDATE — so a write that matches no row means the lock was not held, and
	// the balance this movement was derived from is not the balance the wallet
	// holds. wallet_guard would refuse it a moment later for advancing the
	// version by the wrong amount; catching it here says why.
	moveWallet = `UPDATE wagering.wallet SET balance_minor = $2, version = $3, updated_at = $4 ` +
		`WHERE id = $1 AND version = $5`
)

// writer is the two doors onto the write path, bound to one transaction.
//
// They are functions on the repository bundle rather than methods spread across
// the stores because the rows a movement produces are only correct together and
// the schema pairs just two of the three: a wager transaction can be written at
// PROCESSED reporting a balance the wallet never held, with no ledger entry,
// and reconciliation still calls the wallet consistent. One door makes the
// three arrive together or not at all. See ADR-0010.
type writer struct{ tx pgx.Tx }

// open writes a new wallet, and the opening transaction and its ledger entry
// when the wallet was opened with money in it.
//
// It does not write the opening's events. Those are appended through the outbox
// like every other event, last, because the outbox is a per-wallet
// serialisation point and folding it into this door would hide that.
//
// No wallet lock is taken, and none could be: the row that would be locked is
// the row being created. wallet_player_currency_key is what decides the race
// instead — a second opening for one player and currency blocks on the
// uncommitted key and is then refused, which is the answer the domain gives
// from the wallet it was handed, arrived at the other way.
func (w *writer) open(
	ctx context.Context,
	wallet *wagering.Wallet,
	outcome wagering.Outcome,
	correlation string,
) error {
	const what = "open a wallet"
	_, err := w.tx.Exec(ctx, insertWallet, walletArgs(wallet)...)
	if err != nil {
		return fail(what, err)
	}
	if !outcome.Recorded() {
		// A wallet opened at zero records no opening, so there is nothing else
		// to write. wallet_matches_ledger accepts exactly this shape at commit:
		// balance nought, version one, no entries.
		return nil
	}
	if _, err := w.tx.Exec(ctx, insertTransaction,
		transactionArgs(snapshotOf(outcome.Transaction), correlation, time.Time{})...); err != nil {
		return fail(what, err)
	}
	if outcome.LedgerEntry == nil {
		return app.AsUnretryable(fmt.Errorf(
			"%s: opening %s records a starting balance but no ledger entry",
			what, outcome.Transaction.ID()))
	}
	if _, err := w.tx.Exec(ctx, insertLedgerEntry, ledgerArgs(*outcome.LedgerEntry)...); err != nil {
		return fail(what, err)
	}
	return nil
}

// settle writes what an outcome produced: the transaction's new state, and the
// wallet and ledger entry when money moved.
//
// The transaction goes first, and that part is the lock order: the
// active_reversal trigger fires on its UPDATE, and that hold has to be taken
// while the wallet is already held. The wallet and the entry follow, and the
// outbox is appended by the caller afterwards.
//
// The wallet's new state is read out of the ledger entry rather than passed
// beside it. An entry already carries the balance after, the version the change
// produced and the instant it happened, so there is no second value that could
// disagree — which is why this door is not given the wallet at all.
func (w *writer) settle(
	ctx context.Context,
	outcome wagering.Outcome,
	nextAttemptAt time.Time,
) error {
	const what = "settle an operation"
	if !outcome.Recorded() {
		return app.AsUnretryable(fmt.Errorf("%s: an outcome with no transaction", what))
	}
	snap := snapshotOf(outcome.Transaction)
	tag, err := w.tx.Exec(ctx, settleTransaction,
		uuidOf(snap.ID),
		snap.Status.String(),
		nullableMinorOf(snap.Result),
		textOf(snap.FailureCode),
		nullableResolvedReference(snap),
		snap.ReferenceAttempts,
		timeOf(snap.ReferenceDeadline),
		timeOf(nextAttemptAt),
		snap.UpdatedAt,
	)
	if err != nil {
		return fail(what, err)
	}
	if tag.RowsAffected() == 0 {
		return app.AsUnretryable(fmt.Errorf("%s: no transaction %s to settle", what, snap.ID))
	}

	// No entry, no wallet write: an operation that settled without moving money
	// — a loss, or any rejection — leaves the balance and the ledger alone.
	if outcome.LedgerEntry == nil {
		return nil
	}
	entry := *outcome.LedgerEntry

	// The wallet before the entry, so that a movement computed from a stale
	// balance is refused by the version condition rather than by the ledger's
	// version chain. Both would refuse it and the transaction rolls back either
	// way; the difference is what the failure says. "The wallet is no longer at
	// version 4" names the fault, where "the ledger is at version 5, so the
	// next entry is 6, not 5" describes a symptom of it.
	//
	// The order is free to be chosen because neither write takes a lock the
	// other needs: the wallet row is already held, and the pairing between the
	// two is checked by deferred triggers at COMMIT rather than statement by
	// statement.
	version := int64(entry.WalletVersion())
	tag, err = w.tx.Exec(ctx, moveWallet,
		uuidOf(entry.WalletID()),
		minorOf(entry.BalanceAfter()),
		version,
		entry.CreatedAt(),
		version-1,
	)
	if err != nil {
		return fail(what, err)
	}
	if tag.RowsAffected() == 0 {
		return app.AsRetryable(fmt.Errorf("%s: wallet %s is no longer at version %d: %w",
			what, entry.WalletID(), version-1, ErrLostUpdate))
	}

	if _, err := w.tx.Exec(ctx, insertLedgerEntry, ledgerArgs(entry)...); err != nil {
		return fail(what, err)
	}
	return nil
}

// nullableResolvedReference renders the reference an operation resolved, and
// NULL when it resolved none.
//
// It is written on every settle rather than only when it changes, because
// wager_transaction_resolution_is_decided_once refuses a change and permits a
// restatement — and the snapshot always carries whatever the row already held,
// since the transaction being settled was rehydrated from that row.
func nullableResolvedReference(snap wagering.TransactionSnapshot) pgtype.UUID {
	if snap.External == nil {
		return nullableUUIDOf(wagering.TransactionID{})
	}
	return nullableUUIDOf(snap.External.ResolvedReferenceID)
}
