package postgres

import (
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// Everything that converts between a stored row and a domain value, in both
// directions.
//
// That is the rule this file is drawn on, and it is worth stating because the
// obvious alternative — "reads here, writes beside the statement that makes
// them" — is what the package drifted into: a ledger entry was read in this
// file and written in ledger.go, a wager transaction was read here and rendered
// in transactions.go, and the two halves of one mapping sat in different files
// with nothing to hold them level. A column added to a row struct now has one
// place to be added on the way out as well as on the way in.
//
// What lives here is therefore the row structs, their scan targets, the
// Rehydrate calls that turn them into domain values, and the argument lists
// that turn domain values back into rows. What does not is the statement text,
// which is in sql.go and beside the store that issues it.
//
// The scalar conversions below are gathered for a related reason: the mistakes
// they prevent are the silent kind — a UUID sent as sixteen numbers, a NULL
// scanned as an empty string that then fails a NOT NULL check three statements
// later, an amount that took a detour through a decimal string.

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

// walletColumns names every column a wallet read projects, in the order
// [walletRow.dest] expects them.
var walletColumns = []string{
	"id", "player_id", "currency", "balance_minor", "version", "created_at", "updated_at",
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

// walletArgs renders a wallet for an insert, in the order [walletColumns]
// names.
func walletArgs(w *wagering.Wallet) []any {
	return []any{
		uuidOf(w.ID()),
		string(w.PlayerID()),
		w.Currency().String(),
		minorOf(w.Balance()),
		int64(w.Version()),
		w.CreatedAt(),
		w.UpdatedAt(),
	}
}

// transactionColumns names every column a wager transaction read projects, in
// the order [transactionRow.dest] expects them.
var transactionColumns = []string{
	"id", "wallet_id", "player_id", "currency", "kind", "status", "amount_minor",
	"provider", "external_transaction_id", "idempotency_key", "payload_hash",
	"round_id", "game_id", "reference_external_transaction_id",
	"resolved_reference_id", "correlation_id", "result_balance_minor",
	"failure_code", "reference_attempts", "reference_deadline",
	"created_at", "updated_at",
}

// insertTransactionColumns adds the worker's schedule, which is written but
// never read back into the domain: RehydrateWagerTransaction reads the
// deadline, which is the wait budget, and ignores the next attempt, which is
// only when to look again.
var insertTransactionColumns = append(
	slices.Clone(transactionColumns), "reference_next_attempt_at")

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

// ledgerColumns names every column a ledger read projects, in the order
// [ledgerRow.dest] expects them.
var ledgerColumns = []string{
	"id", "wallet_id", "transaction_id", "currency", "direction", "amount_minor",
	"balance_before_minor", "balance_after_minor", "wallet_version", "created_at",
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

// transactionArgs renders a snapshot for an insert, in the order
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

// ledgerArgs renders an entry for an insert, in the order [ledgerColumns]
// names.
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
