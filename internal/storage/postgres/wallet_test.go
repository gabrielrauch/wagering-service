package postgres_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// base is the instant fixtures are created at. The schema takes every timestamp
// from its caller — there is no DEFAULT now() anywhere — because the domain
// takes now as an argument, and a database clock would be a second one that
// disagrees.
var base = time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)

const insertWallet = `
	INSERT INTO wagering.wallet (id, player_id, currency, balance_minor, version, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)`

// wallet is a fixture row, carrying what later statements need to name it.
type wallet struct {
	id       string
	playerID string
	currency string
}

// newWallet opens a wallet holding balance, the way OpenWallet does.
//
// A wallet with money in it and no ledger behind it cannot be committed, so a
// funded wallet arrives as three rows in one transaction: the wallet, the
// opening that records its starting balance, and the credit that moves it. A
// wallet opened at zero is one row and no opening, which is the other shape the
// domain produces.
func newWallet(t *testing.T, db *sql.DB, balance int64) wallet {
	t.Helper()
	w := wallet{
		id:       wagering.NewWalletID().String(),
		playerID: "player-" + wagering.NewWalletID().String(),
		currency: "BRL",
	}
	err := inTx(t, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), insertWallet, w.id, w.playerID, w.currency, balance, 1, base, base); err != nil {
			return err
		}
		if balance == 0 {
			return nil
		}
		opening := openingTx(w, balance)
		if _, err := tx.ExecContext(t.Context(), insertTransaction, opening.args()...); err != nil {
			return err
		}
		_, err := tx.ExecContext(t.Context(), insertLedgerEntry, credit(w, opening, balance, 0, 1, base).args()...)
		return err
	})
	if err != nil {
		t.Fatalf("open a wallet holding %d: %v", balance, err)
	}
	return w
}

// debitBalance takes money out of a wallet and records it, in one transaction.
func debitBalance(t *testing.T, db *sql.DB, w wallet, cause txn, amount, before, version int64, at time.Time) error {
	t.Helper()
	return inTx(t, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), moveWallet, w.id, before-amount, version, at); err != nil {
			return err
		}
		_, err := tx.ExecContext(t.Context(), insertLedgerEntry, debit(w, cause, amount, before, version, at).args()...)
		return err
	})
}

func TestWalletBalanceIsNeverNegative(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	id := wagering.NewWalletID().String()
	refuses(t, db, checkViolation, insertWallet,
		id, "player-"+id, "BRL", -1, 1, base, base)
}

func TestWalletVersionStartsAtOne(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	id := wagering.NewWalletID().String()
	refuses(t, db, checkViolation, insertWallet,
		id, "player-"+id, "BRL", 0, 0, base, base)
}

// TestWalletIsUniquePerPlayerAndCurrency is the database half of
// WALLET_ALREADY_EXISTS. The domain refuses a second wallet when it is handed
// the one that already exists, but a check against a value cannot win a race —
// this constraint is what actually decides it.
func TestWalletIsUniquePerPlayerAndCurrency(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	first := newWallet(t, db, 0)

	t.Run("a second wallet for the same player and currency", func(t *testing.T) {
		refuses(t, db, uniqueViolation, insertWallet,
			wagering.NewWalletID().String(), first.playerID, first.currency, 0, 1, base, base)
	})

	t.Run("the same player in another currency", func(t *testing.T) {
		accepts(t, db, insertWallet,
			wagering.NewWalletID().String(), first.playerID, "USD", 0, 1, base, base)
	})
}

// TestWalletVersionAdvancesWithTheBalance pins the trigger that makes the
// version column mean something.
//
// A version the application merely promises to increment is a number; a version
// the database refuses to let drift is an optimistic-concurrency token. The
// last case is the lost update the brief asks to be detectable: a writer holding
// a stale version computes the same next version as the writer that beat it,
// and cannot commit.
func TestWalletVersionAdvancesWithTheBalance(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	later := base.Add(time.Second)

	t.Run("a balance change advances the version by one", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		if err := debitBalance(t, db, w, processedBet(t, db, w), 2500, 10000, 2, later); err != nil {
			t.Errorf("a balance change advancing the version by one was refused: %v", err)
		}
	})

	t.Run("a balance change leaving the version behind", func(t *testing.T) {
		w := newWallet(t, db, 0)
		refusesRule(t, db, "wallet_version_advances_by_one", moveWallet, w.id, 2500, 1, later)
	})

	t.Run("a balance change skipping a version", func(t *testing.T) {
		w := newWallet(t, db, 0)
		refusesRule(t, db, "wallet_version_advances_by_one", moveWallet, w.id, 2500, 3, later)
	})

	t.Run("a balance change that does not move the clock", func(t *testing.T) {
		w := newWallet(t, db, 0)
		refusesRule(t, db, "wallet_clock_moves_forward", moveWallet, w.id, 2500, 2, base)
	})

	t.Run("an update that changes no balance leaves the version alone", func(t *testing.T) {
		w := newWallet(t, db, 0)
		accepts(t, db, `UPDATE wagering.wallet SET updated_at = $2 WHERE id = $1`, w.id, later)
	})

	t.Run("a version advanced without the balance moving", func(t *testing.T) {
		w := newWallet(t, db, 0)
		refusesRule(t, db, "wallet_version_advances_only_with_the_balance", moveWallet, w.id, 0, 2, later)
	})

	t.Run("a lost update", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		// The writer that arrives first debits and lands on version 2.
		if err := debitBalance(t, db, w, processedBet(t, db, w), 2500, 10000, 2, later); err != nil {
			t.Fatalf("the first writer was refused: %v", err)
		}
		// A writer that read version 1 before that, and computed 2 from it,
		// cannot now commit: the row is already at 2.
		refusesRule(t, db, "wallet_version_advances_by_one", moveWallet, w.id, 1000, 2, later.Add(time.Second))
	})
}

func TestWalletIdentityIsImmutable(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	cases := []struct {
		name  string
		rule  string
		query string
		arg   any
	}{
		{"the identifier", "wallet_identifier_is_immutable",
			`UPDATE wagering.wallet SET id = $2 WHERE id = $1`, wagering.NewWalletID().String()},
		{"the player", "wallet_player_is_immutable",
			`UPDATE wagering.wallet SET player_id = $2 WHERE id = $1`, "someone-else"},
		{"the currency", "wallet_currency_is_immutable",
			`UPDATE wagering.wallet SET currency = $2 WHERE id = $1`, "USD"},
		{"when it was opened", "wallet_opening_time_is_immutable",
			`UPDATE wagering.wallet SET created_at = $2 WHERE id = $1`, base.Add(time.Hour)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWallet(t, db, 0)
			refusesRule(t, db, c.rule, c.query, w.id, c.arg)
		})
	}
}

// TestWalletCurrencyUsesTheValueDomain checks that the wallet's currency column
// is held to the same rule as every other, rather than being a bare text column
// that happens to hold three letters today.
func TestWalletCurrencyUsesTheValueDomain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	id := wagering.NewWalletID().String()
	refuses(t, db, checkViolation, insertWallet,
		id, "player-"+id, "brl", 0, 1, base, base)
}
