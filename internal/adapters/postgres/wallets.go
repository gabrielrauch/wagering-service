package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// walletColumns is the projection every wallet read takes, in the order
// [walletRow.dest] expects it. Written once so that a column added to the row
// struct and forgotten in a query is a scan error at the first read rather than
// a field that is silently always zero.
const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

const (
	selectWalletByID  = `SELECT ` + walletColumns + ` FROM wagering.wallet WHERE id = $1`
	selectWalletByKey = `SELECT ` + walletColumns +
		` FROM wagering.wallet WHERE player_id = $1 AND currency = $2`

	// FOR NO KEY UPDATE, not FOR UPDATE.
	//
	// The difference is what the lock lets other transactions do while it is
	// held. FOR NO KEY UPDATE conflicts with another movement on this wallet,
	// which is the serialisation the whole design rests on, but it does NOT
	// conflict with FOR KEY SHARE — the lock a foreign key check takes. Every
	// wager transaction and every ledger entry references the wallet, so
	// FOR UPDATE here would block another wallet's command from inserting a row
	// that merely names this one, and would turn the outbox's aggregate
	// foreign key into a second queue behind the balance.
	//
	// The wait is bounded by the transaction's lock_timeout rather than by NOWAIT
	// or a SKIP LOCKED: a command that arrives second should wait its turn and
	// then proceed, not be told there is nothing to do.
	lockWalletByID  = selectWalletByID + ` FOR NO KEY UPDATE`
	lockWalletByKey = selectWalletByKey + ` FOR NO KEY UPDATE`
)

// wallets reads wallets and takes the lock that serialises movement on one.
//
// It holds the transaction, not the pool. A wallets value obtained anywhere but
// from inside a TxManager callback cannot be built.
type wallets struct{ tx pgx.Tx }

// ByID reads a wallet, answering (nil, nil) when there is none.
func (w wallets) ByID(ctx context.Context, id wagering.WalletID) (*wagering.Wallet, error) {
	return w.one(ctx, "read wallet by id", selectWalletByID, uuidOf(id))
}

// ByKey reads the wallet a player holds in a currency.
func (w wallets) ByKey(ctx context.Context, key wagering.WalletKey) (*wagering.Wallet, error) {
	return w.one(ctx, "read wallet by key", selectWalletByKey,
		string(key.PlayerID), key.Currency.String())
}

// LockForMovement takes the wallet's row lock and returns it as it stands under
// that lock.
func (w wallets) LockForMovement(
	ctx context.Context,
	key wagering.WalletKey,
) (*wagering.Wallet, error) {
	return w.one(ctx, "lock wallet by key", lockWalletByKey,
		string(key.PlayerID), key.Currency.String())
}

// LockByID takes the same lock on a wallet already known by identifier.
func (w wallets) LockByID(ctx context.Context, id wagering.WalletID) (*wagering.Wallet, error) {
	return w.one(ctx, "lock wallet by id", lockWalletByID, uuidOf(id))
}

// one runs a single-row wallet query.
//
// No rows is (nil, nil) and never an error, which is the port's contract: every
// caller here has a branch for a wallet that is not there, and putting absence
// in the error channel would mean each of them had to tell it apart from a
// database that is down.
func (w wallets) one(
	ctx context.Context,
	what, query string,
	args ...any,
) (*wagering.Wallet, error) {
	var row walletRow
	if err := w.tx.QueryRow(ctx, query, args...).Scan(row.dest()...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fail(what, err)
	}
	wallet, err := row.wallet()
	if err != nil {
		return nil, corrupt(what, err)
	}
	return wallet, nil
}
