package postgres

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The conversions between a row and the values the domain works in.
//
// They are gathered here rather than written at each call site because the
// mistakes they prevent are the silent kind: a UUID sent as sixteen numbers, a
// NULL scanned as an empty string that then fails a NOT NULL check three
// statements later, an amount that took a detour through a decimal string. Each
// one is stated once, so there is one place to read to know it is right.

// uuidOf renders one of the identifiers this system mints for a uuid column.
//
// pgx's uuid codec speaks pgtype.UUID and nothing else, and every identifier
// here is a distinct type over [16]byte, so the conversion is explicit at the
// boundary. Passing the identifier itself would fall through to pgx's generic
// handling and publish a UUID as an array of numbers.
func uuidOf[T ~[16]byte](id T) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(id), Valid: true}
}

// nullableUUIDOf renders an identifier that may name nothing, as NULL when it
// does. The zero identifier is the domain's spelling of absence, and NULL is
// the schema's.
func nullableUUIDOf[T ~[16]byte](id T) pgtype.UUID {
	if id == *new(T) {
		return pgtype.UUID{}
	}
	return uuidOf(id)
}

// idFrom reads an identifier back. A NULL becomes the zero identifier, which is
// the value every IsZero in the domain already answers for.
func idFrom[T ~[16]byte](v pgtype.UUID) T {
	if !v.Valid {
		return *new(T)
	}
	return T(v.Bytes)
}

// textOf renders a provider-owned identifier for a column that may hold none.
//
// Empty is NULL rather than an empty string. The opaque_id domain refuses an
// empty value, so the two are not interchangeable at this boundary even though
// the domain spells absence as "".
func textOf[T ~string](s T) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: string(s), Valid: true}
}

// textFrom reads one back, NULL becoming the empty identifier.
func textFrom[T ~string](v pgtype.Text) T {
	if !v.Valid {
		return ""
	}
	return T(v.String)
}

// timeOf renders an instant for a nullable timestamptz, the zero time being the
// absence convention the domain and the ports already use.
func timeOf(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}

// timeFrom reads one back.
func timeFrom(v pgtype.Timestamptz) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return v.Time
}

// minorOf renders an amount for a minor_amount column: the count of minor units
// and nothing else. The currency travels in its own column, so there is no
// decimal to format and no float to round.
func minorOf(m money.Money) int64 { return m.MinorUnits() }

// nullableMinorOf renders an amount that may be absent, which on this schema
// means a transaction that has not been processed and so reports no balance.
func nullableMinorOf(m *money.Money) pgtype.Int8 {
	if m == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: m.MinorUnits(), Valid: true}
}

// moneyFrom rebuilds an amount from the two columns that hold it.
func moneyFrom(minor int64, code string) (money.Money, error) {
	currency, err := money.ParseCurrency(code)
	if err != nil {
		return money.Money{}, err
	}
	return money.FromMinorUnits(minor, currency)
}

// walletRow is one row of wagering.wallet.
type walletRow struct {
	id        pgtype.UUID
	playerID  string
	currency  string
	balance   int64
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// dest is the scan target list, in the order [walletColumns] names them.
func (r *walletRow) dest() []any {
	return []any{&r.id, &r.playerID, &r.currency, &r.balance, &r.version, &r.createdAt, &r.updatedAt}
}

// wallet rehydrates the row.
//
// The version crosses the wire as a signed bigint and the domain holds it
// unsigned, so a negative would become an enormous version rather than an
// error. The schema's wallet_version_starts_at_one makes that unreachable; it
// is checked anyway, because the conversion is the one place this package could
// turn corruption into a plausible-looking value.
func (r *walletRow) wallet() (*wagering.Wallet, error) {
	balance, err := moneyFrom(r.balance, r.currency)
	if err != nil {
		return nil, err
	}
	if r.version < 0 {
		return nil, errNegativeVersion
	}
	return wagering.RehydrateWallet(wagering.WalletSnapshot{
		ID:        idFrom[wagering.WalletID](r.id),
		PlayerID:  wagering.PlayerID(r.playerID),
		Balance:   balance,
		Version:   uint64(r.version),
		CreatedAt: r.createdAt,
		UpdatedAt: r.updatedAt,
	})
}

// transactionRow is one row of wagering.wager_transaction, plus the correlation
// it arrived under.
type transactionRow struct {
	id                pgtype.UUID
	walletID          pgtype.UUID
	playerID          string
	currency          string
	kind              string
	status            string
	amount            int64
	provider          pgtype.Text
	externalID        pgtype.Text
	idempotencyKey    pgtype.Text
	payloadHash       pgtype.Text
	roundID           pgtype.Text
	gameID            pgtype.Text
	referenceExternal pgtype.Text
	resolvedReference pgtype.UUID
	correlation       string
	result            pgtype.Int8
	failureCode       pgtype.Text
	attempts          int32
	deadline          pgtype.Timestamptz
	createdAt         time.Time
	updatedAt         time.Time
}

// dest is the scan target list, in the order [transactionColumns] names them.
func (r *transactionRow) dest() []any {
	return []any{
		&r.id, &r.walletID, &r.playerID, &r.currency, &r.kind, &r.status, &r.amount,
		&r.provider, &r.externalID, &r.idempotencyKey, &r.payloadHash, &r.roundID, &r.gameID,
		&r.referenceExternal, &r.resolvedReference, &r.correlation,
		&r.result, &r.failureCode, &r.attempts, &r.deadline, &r.createdAt, &r.updatedAt,
	}
}

// transaction rehydrates the row.
//
// The provider side is present exactly when the row carries a provider, which
// wager_transaction_origin_carries_its_fields makes an all-or-nothing choice at
// the schema level — so testing one column answers for all six.
func (r *transactionRow) transaction() (*wagering.WagerTransaction, error) {
	amount, err := moneyFrom(r.amount, r.currency)
	if err != nil {
		return nil, err
	}
	snap := wagering.TransactionSnapshot{
		ID:                idFrom[wagering.TransactionID](r.id),
		WalletID:          idFrom[wagering.WalletID](r.walletID),
		PlayerID:          wagering.PlayerID(r.playerID),
		Kind:              wagering.Kind(r.kind),
		Money:             amount,
		Status:            wagering.Status(r.status),
		CreatedAt:         r.createdAt,
		UpdatedAt:         r.updatedAt,
		FailureCode:       textFrom[failure.Code](r.failureCode),
		ReferenceAttempts: int(r.attempts),
		ReferenceDeadline: timeFrom(r.deadline),
	}
	if r.result.Valid {
		reported, err := moneyFrom(r.result.Int64, r.currency)
		if err != nil {
			return nil, err
		}
		snap.Result = &reported
	}
	if r.provider.Valid {
		snap.External = &wagering.ExternalSnapshot{
			Provider:                       textFrom[wagering.Provider](r.provider),
			ExternalTransactionID:          textFrom[wagering.ExternalTransactionID](r.externalID),
			IdempotencyKey:                 textFrom[wagering.IdempotencyKey](r.idempotencyKey),
			PayloadHash:                    textFrom[wagering.PayloadHash](r.payloadHash),
			RoundID:                        textFrom[wagering.RoundID](r.roundID),
			GameID:                         textFrom[wagering.GameID](r.gameID),
			ReferenceExternalTransactionID: textFrom[wagering.ExternalTransactionID](r.referenceExternal),
			ResolvedReferenceID:            idFrom[wagering.TransactionID](r.resolvedReference),
		}
	}
	return wagering.RehydrateWagerTransaction(snap)
}

// stored rehydrates the row together with the correlation it arrived under,
// which is what every reader on the transaction port answers with.
func (r *transactionRow) stored() (*app.StoredTransaction, error) {
	tx, err := r.transaction()
	if err != nil {
		return nil, err
	}
	return &app.StoredTransaction{Transaction: tx, Correlation: r.correlation}, nil
}

// ledgerRow is one row of wagering.wallet_ledger_entry.
type ledgerRow struct {
	id            pgtype.UUID
	walletID      pgtype.UUID
	transactionID pgtype.UUID
	currency      string
	direction     string
	amount        int64
	balanceBefore int64
	balanceAfter  int64
	walletVersion int64
	createdAt     time.Time
}

// dest is the scan target list, in the order [ledgerColumns] names them.
func (r *ledgerRow) dest() []any {
	return []any{
		&r.id, &r.walletID, &r.transactionID, &r.currency, &r.direction, &r.amount,
		&r.balanceBefore, &r.balanceAfter, &r.walletVersion, &r.createdAt,
	}
}

// entry rehydrates the row, which applies the same arithmetic check a new entry
// is held to: an entry whose stated movement no longer adds up is corruption.
func (r *ledgerRow) entry() (wagering.WalletLedgerEntry, error) {
	amount, err := moneyFrom(r.amount, r.currency)
	if err != nil {
		return wagering.WalletLedgerEntry{}, err
	}
	before, err := moneyFrom(r.balanceBefore, r.currency)
	if err != nil {
		return wagering.WalletLedgerEntry{}, err
	}
	after, err := moneyFrom(r.balanceAfter, r.currency)
	if err != nil {
		return wagering.WalletLedgerEntry{}, err
	}
	if r.walletVersion < 0 {
		return wagering.WalletLedgerEntry{}, errNegativeVersion
	}
	return wagering.RehydrateWalletLedgerEntry(wagering.LedgerEntryInput{
		ID:            idFrom[wagering.LedgerEntryID](r.id),
		WalletID:      idFrom[wagering.WalletID](r.walletID),
		TransactionID: idFrom[wagering.TransactionID](r.transactionID),
		Direction:     wagering.Direction(r.direction),
		Amount:        amount,
		BalanceBefore: before,
		BalanceAfter:  after,
		WalletVersion: uint64(r.walletVersion),
		CreatedAt:     r.createdAt,
	})
}
