package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// ledgerColumns is the projection every ledger read takes, in the order
// [ledgerRow.dest] expects it.
const ledgerColumns = `id, wallet_id, transaction_id, currency, direction, amount_minor, ` +
	`balance_before_minor, balance_after_minor, wallet_version, created_at`

const (
	// The page is drawn on wallet_version, which is what makes the cursor
	// stable: (wallet_id, wallet_version) is unique and ordered, where a
	// creation instant ties and an offset shifts under a write. The predicate
	// and the order are both served by wallet_ledger_entry_version_key, so a
	// page costs one index range scan however deep into the ledger it is.
	selectLedgerPage = `SELECT ` + ledgerColumns + ` FROM wagering.wallet_ledger_entry ` +
		`WHERE wallet_id = $1 AND wallet_version > $2 ORDER BY wallet_version LIMIT $3`

	// Ordered even though reconciliation sums and a sum does not care. The
	// order costs nothing on this index, and an operator reading the entries a
	// divergence was found over reads them in the order they happened.
	selectLedgerAll = `SELECT ` + ledgerColumns + ` FROM wagering.wallet_ledger_entry ` +
		`WHERE wallet_id = $1 ORDER BY wallet_version`

	insertLedgerEntry = `INSERT INTO wagering.wallet_ledger_entry (` + ledgerColumns +
		`) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
)

// ledger reads a wallet's ledger. It has no writer: an entry is written only
// through the two doors on the repository bundle, beside the wallet it explains.
type ledger struct{ tx pgx.Tx }

// Page returns entries after a wallet version, ascending, at most limit of them.
//
// afterVersion is already decoded: the opaque cursor is the application
// layer's, and encoding it here as well would be a second spelling of the same
// position, free to disagree with the first.
func (l ledger) Page(
	ctx context.Context,
	w wagering.WalletID,
	afterVersion uint64,
	limit int,
) ([]wagering.WalletLedgerEntry, error) {
	// A uint64 above the signed range cannot name a version any wallet reached
	// — the column is a bigint — and clamping rather than erroring answers a
	// cursor past the end with an empty page, which is what a cursor past the
	// end means.
	after := int64(min(afterVersion, uint64(maxVersion)))
	return l.read(ctx, "page the ledger", selectLedgerPage, uuidOf(w), after, limit)
}

// All returns every entry for a wallet, for reconciliation.
func (l ledger) All(
	ctx context.Context,
	w wagering.WalletID,
) ([]wagering.WalletLedgerEntry, error) {
	return l.read(ctx, "read the ledger", selectLedgerAll, uuidOf(w))
}

// maxVersion is the largest version a bigint column can hold.
const maxVersion int64 = 1<<63 - 1

// read runs a ledger query and rehydrates every row.
func (l ledger) read(
	ctx context.Context,
	what, query string,
	args ...any,
) ([]wagering.WalletLedgerEntry, error) {
	rows, err := l.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fail(what, err)
	}
	defer rows.Close()

	var entries []wagering.WalletLedgerEntry
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(row.dest()...); err != nil {
			return nil, fail(what, err)
		}
		entry, err := row.entry()
		if err != nil {
			return nil, corrupt(what, err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fail(what, err)
	}
	return entries, nil
}

// ledgerArgs renders an entry for [insertLedgerEntry].
func ledgerArgs(e wagering.WalletLedgerEntry) []any {
	return []any{
		uuidOf(e.ID()),
		uuidOf(e.WalletID()),
		uuidOf(e.TransactionID()),
		e.Amount().Currency().String(),
		e.Direction().String(),
		minorOf(e.Amount()),
		minorOf(e.BalanceBefore()),
		minorOf(e.BalanceAfter()),
		int64(e.WalletVersion()),
		e.CreatedAt(),
	}
}
