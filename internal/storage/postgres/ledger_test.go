package postgres_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

const insertLedgerEntry = `
	INSERT INTO wagering.wallet_ledger_entry (
		id, wallet_id, transaction_id, currency, direction, amount_minor,
		balance_before_minor, balance_after_minor, wallet_version, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

// entry is a ledger row under construction.
//
// Typed, unlike txn: every column a ledger entry has is NOT NULL, so there is
// no nil for a field to have to carry.
type entry struct {
	id, walletID, transactionID, currency, direction string
	amount, before, after, version                   int64
	createdAt                                        time.Time
}

func (e entry) args() []any {
	return []any{
		e.id, e.walletID, e.transactionID, e.currency, e.direction,
		e.amount, e.before, e.after, e.version, e.createdAt,
	}
}

func (e entry) refuses(t *testing.T, db execer, state string) {
	t.Helper()
	refuses(t, db, state, insertLedgerEntry, e.args()...)
}

// refusedBy asserts which of the ledger's rules turned the entry down.
func (e entry) refusedBy(t *testing.T, db execer, rule string) {
	t.Helper()
	refusesRule(t, db, rule, insertLedgerEntry, e.args()...)
}

const moveWallet = `
	UPDATE wagering.wallet SET balance_minor = $2, version = $3, updated_at = $4
	WHERE id = $1`

// settles commits the entry together with the balance change it explains.
//
// The pairing is checked from both sides — wallet_matches_ledger when the wallet
// is written, ledger_matches_wallet when an entry is — and both are deferred, so
// the two statements have to arrive at one commit. An entry on its own is money
// the wallet never received, which is exactly what the pair refuses.
func (e entry) settles(t *testing.T, db *sql.DB) {
	t.Helper()
	err := inTx(t, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), insertLedgerEntry, e.args()...); err != nil {
			return err
		}
		_, err := tx.ExecContext(t.Context(), moveWallet, e.walletID, e.after, e.version, settleAt(e.version))
		return err
	})
	if err != nil {
		t.Errorf("an entry with the balance change it explains was refused: %v", err)
	}
}

// settleAt is where the wallet's clock lands after an entry at this version.
// A balance change has to move updated_at forward, and the version is the one
// number that always does.
func settleAt(version int64) time.Time {
	return base.Add(time.Duration(version) * time.Second)
}

// credit is the entry a crediting transaction produces.
func credit(w wallet, tx txn, amount, before int64, version int64, at time.Time) entry {
	return entry{
		id: wagering.NewLedgerEntryID().String(), walletID: w.id, transactionID: tx.id,
		currency: w.currency, direction: "CREDIT", amount: amount,
		before: before, after: before + amount, version: version, createdAt: at,
	}
}

// debit is the entry a debiting transaction produces.
func debit(w wallet, tx txn, amount, before int64, version int64, at time.Time) entry {
	return entry{
		id: wagering.NewLedgerEntryID().String(), walletID: w.id, transactionID: tx.id,
		currency: w.currency, direction: "DEBIT", amount: amount,
		before: before, after: before - amount, version: version, createdAt: at,
	}
}

// inTx runs body inside one transaction and returns the commit error, so a
// deferred constraint has somewhere to fire.
func inTx(t *testing.T, db *sql.DB, body func(tx *sql.Tx) error) error {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := body(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// commitRefusedBy asserts which deferred rule refused the commit. The deferred
// rules all raise at the same moment, so naming one is the only way to say which
// of them the test is about.
func commitRefusedBy(t *testing.T, db *sql.DB, rule string, body func(tx *sql.Tx) error) {
	t.Helper()
	assertRule(t, inTx(t, db, body), rule, "the transaction", "")
}

// TestLedgerArithmeticHolds is the reason NewWalletLedgerEntry exists, restated
// where storage can be reached directly: an entry that misreports a movement is
// unrepresentable rather than merely unlikely.
//
// Each case gets its own wallet, left in a state the chain accepts. The chain
// trigger is a BEFORE INSERT and so runs ahead of these CHECK constraints; a
// shared wallet would have every case after the first refused for a broken
// chain, and the arithmetic would never be reached.
func TestLedgerArithmeticHolds(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	// next returns an entry that follows correctly from a 100.00 opening, so
	// that the only thing a case changes is the thing under test.
	next := func(t *testing.T) (wallet, txn, entry) {
		t.Helper()
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)
		return w, bet, debit(w, bet, 2500, 10000, 2, base)
	}

	t.Run("a debit takes the amount off the balance", func(t *testing.T) {
		_, _, e := next(t)
		e.settles(t, db)
	})

	t.Run("a debit that does not add up", func(t *testing.T) {
		_, _, e := next(t)
		e.after = 8000
		e.refuses(t, db, checkViolation)
	})

	t.Run("a credit recorded as if it were a debit", func(t *testing.T) {
		w, bet, _ := next(t)
		e := credit(w, bet, 2500, 10000, 2, base)
		e.after = 7500
		e.refuses(t, db, checkViolation)
	})

	t.Run("an entry that moves nothing", func(t *testing.T) {
		w, bet, _ := next(t)
		credit(w, bet, 0, 10000, 2, base).refuses(t, db, checkViolation)
	})

	t.Run("an entry that moves a negative amount", func(t *testing.T) {
		w, bet, _ := next(t)
		credit(w, bet, -2500, 10000, 2, base).refuses(t, db, checkViolation)
	})

	t.Run("an entry leaving the balance below zero", func(t *testing.T) {
		w, bet, _ := next(t)
		debit(w, bet, 12500, 10000, 2, base).refuses(t, db, checkViolation)
	})

	t.Run("a direction that names no movement", func(t *testing.T) {
		_, _, e := next(t)
		e.direction = "SIDEWAYS"
		e.refuses(t, db, checkViolation)
	})
}

// TestLedgerEntriesChain is the O(1) half of the ledger-to-balance guarantee.
// Each entry links to its predecessor, so agreement between the wallet and the
// whole ledger follows by induction instead of being resummed on every write.
func TestLedgerEntriesChain(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	// A wallet opened at zero carries no opening entry and already stands at
	// version 1, so its first movement is version 2 — see TestAWalletOpenedAtZero
	// CanStillMoveMoney, which is the whole of why this is not 1.
	t.Run("the first entry starts from nothing at the wallet's next version", func(t *testing.T) {
		w := newWallet(t, db, 0)
		tx := processedBet(t, db, w)
		credit(w, tx, 2500, 0, 2, base).settles(t, db)
	})

	t.Run("a first entry that starts from a balance the wallet never held", func(t *testing.T) {
		w := newWallet(t, db, 0)
		tx := processedBet(t, db, w)
		credit(w, tx, 2500, 500, 2, base).refusedBy(t, db, "wallet_ledger_entry_first_starts_from_zero")
	})

	t.Run("a first entry at a version the wallet never reached", func(t *testing.T) {
		w := newWallet(t, db, 0)
		tx := processedBet(t, db, w)
		credit(w, tx, 2500, 0, 5, base).refusedBy(t, db, "wallet_ledger_entry_first_records_the_opening_version")
	})

	t.Run("an entry that does not start where the last one ended", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		tx := processedBet(t, db, w)
		// The opening left the balance at 10000 and the wallet at version 1.
		debit(w, tx, 2500, 9000, 2, base).refusedBy(t, db, "wallet_ledger_entry_follows_the_previous_balance")
	})

	t.Run("an entry that skips a version", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		tx := processedBet(t, db, w)
		debit(w, tx, 2500, 10000, 3, base).refusedBy(t, db, "wallet_ledger_entry_follows_the_previous_version")
	})

	t.Run("an entry in a currency the wallet is not denominated in", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		tx := processedBet(t, db, w)
		e := debit(w, tx, 2500, 10000, 2, base)
		e.currency = "USD"
		e.refusedBy(t, db, "wallet_ledger_entry_matches_the_wallet_currency")
	})
}

func TestLedgerRecordsATransactionOnce(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)
	bet := processedBet(t, db, w)

	debit(w, bet, 2500, 10000, 2, base).settles(t, db)

	t.Run("the same transaction writing a second entry", func(t *testing.T) {
		debit(w, bet, 1000, 7500, 3, base).refuses(t, db, uniqueViolation)
	})
}

// TestTwoEntriesCannotClaimOneVersion is the concurrency guard, and it has to
// be written as one.
//
// Serially the version index is unreachable: the chain trigger requires the next
// version, and the next version is by definition free. It is only two writers
// who both read the same committed ledger that can compute the same next
// version — and then the unique index is the only thing between them and two
// entries claiming one balance.
func TestTwoEntriesCannotClaimOneVersion(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	first := processedBet(t, db, w)
	second := processedBet(t, db, w)

	ahead, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	behind, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Both transactions are rolled back however the test ends. Cleanups run after
	// t.Context() is cancelled, which aborts whatever statement is still in flight,
	// so neither rollback waits on a lock the other holds and the order they are
	// registered in does not matter. That depends on the pair being opened against
	// t.Context(): on a background context, the blocked writer would deadlock here.
	t.Cleanup(func() { _ = ahead.Rollback() })
	t.Cleanup(func() { _ = behind.Rollback() })

	if _, err := ahead.ExecContext(t.Context(), insertLedgerEntry, debit(w, first, 2500, 10000, 2, base).args()...); err != nil {
		t.Fatalf("the first writer was refused: %v", err)
	}
	// The wallet moves with it, or the deferred pairing refuses the commit below
	// and this stops being a test about the version index.
	if _, err := ahead.ExecContext(t.Context(), moveWallet, w.id, int64(7500), int64(2), settleAt(2)); err != nil {
		t.Fatalf("the first writer could not move the balance: %v", err)
	}

	// The second writer still sees the committed ledger at version 1, so the
	// chain lets it through and it blocks on the index instead.
	refused := make(chan error, 1)
	go func() {
		_, err := behind.ExecContext(t.Context(), insertLedgerEntry, debit(w, second, 1000, 10000, 2, base).args()...)
		refused <- err
	}()

	// Waiting for the second writer to actually be blocked, rather than
	// sleeping and hoping. Committing too early would let its chain check run
	// against the committed ledger, and it would be refused for the version it
	// read rather than for the one it collided with — which is a different test
	// passing under this one's name.
	waitForBlockedWriter(t, db)

	if err := ahead.Commit(); err != nil {
		t.Fatalf("the first writer could not commit: %v", err)
	}

	assertState(t, <-refused, uniqueViolation, "the second writer's entry", "")
}

// TestLedgerIsAppendOnly covers both halves the brief asks for: the trigger
// that refuses the statement, and — in the privileges test — the grant that
// never lets it be attempted. Financial corrections are new entries.
func TestLedgerIsAppendOnly(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)
	bet := processedBet(t, db, w)

	e := debit(w, bet, 2500, 10000, 2, base)
	e.settles(t, db)

	t.Run("an update", func(t *testing.T) {
		refusesRule(t, db, "wallet_ledger_entry_is_append_only",
			`UPDATE wagering.wallet_ledger_entry SET amount_minor = 1 WHERE id = $1`, e.id)
	})

	t.Run("a delete", func(t *testing.T) {
		refusesRule(t, db, "wallet_ledger_entry_is_append_only",
			`DELETE FROM wagering.wallet_ledger_entry WHERE id = $1`, e.id)
	})
}

// TestWalletAgreesWithItsLedgerAtCommit is the deferred constraint: a balance
// change without a ledger entry is not something the database declines to do,
// it is something that cannot be committed.
func TestWalletAgreesWithItsLedgerAtCommit(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	later := base.Add(time.Second)

	t.Run("a balance that moves with an entry behind it", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)
		err := inTx(t, db, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(t.Context(), moveWallet, w.id, int64(7500), int64(2), later); err != nil {
				return err
			}
			_, err := tx.ExecContext(t.Context(), insertLedgerEntry, debit(w, bet, 2500, 10000, 2, later).args()...)
			return err
		})
		if err != nil {
			t.Errorf("a balance change recorded in the ledger was refused: %v", err)
		}
	})

	t.Run("a balance that moves with nothing behind it", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		commitRefusedBy(t, db, "wallet_matches_its_ledger", func(tx *sql.Tx) error {
			_, err := tx.ExecContext(t.Context(), moveWallet, w.id, int64(7500), int64(2), later)
			return err
		})
	})

	t.Run("a wallet opened with money but no opening entry", func(t *testing.T) {
		id := wagering.NewWalletID().String()
		commitRefusedBy(t, db, "wallet_with_no_ledger_holds_nothing", func(tx *sql.Tx) error {
			_, err := tx.ExecContext(t.Context(), insertWallet, id, "player-"+id, "BRL", 10000, 1, base, base)
			return err
		})
	})

	t.Run("a wallet opened at zero needs no entry", func(t *testing.T) {
		id := wagering.NewWalletID().String()
		err := inTx(t, db, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(t.Context(), insertWallet, id, "player-"+id, "BRL", 0, 1, base, base)
			return err
		})
		if err != nil {
			t.Errorf("a wallet opened at zero was refused: %v", err)
		}
	})
}

// TestLedgerReconstructsTheBalance checks the generated signed column and the
// function that is the SQL twin of the domain's Reconcile — an O(n) sum an
// operator runs deliberately, never a trigger on the write path.
func TestLedgerReconstructsTheBalance(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	later := base.Add(time.Second)
	bet := processedBet(t, db, w)
	if err := inTx(t, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), moveWallet, w.id, int64(7500), int64(2), later); err != nil {
			return err
		}
		_, err := tx.ExecContext(t.Context(), insertLedgerEntry, debit(w, bet, 2500, 10000, 2, later).args()...)
		return err
	}); err != nil {
		t.Fatalf("record a bet: %v", err)
	}

	t.Run("credits less debits", func(t *testing.T) {
		var sum int64
		err := db.QueryRowContext(t.Context(), `
			SELECT coalesce(sum(signed_minor), 0)
			FROM wagering.wallet_ledger_entry WHERE wallet_id = $1`, w.id).Scan(&sum)
		if err != nil {
			t.Fatalf("sum the ledger: %v", err)
		}
		if sum != 7500 {
			t.Errorf("the ledger sums to %d, the wallet holds 7500", sum)
		}
	})

	t.Run("reconciliation reports agreement", func(t *testing.T) {
		var agrees bool
		if err := db.QueryRowContext(t.Context(), `SELECT wagering.reconcile_wallet($1)`, w.id).Scan(&agrees); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if !agrees {
			t.Error("reconciliation reported a mismatch on a wallet that agrees with its ledger")
		}
	})
}

// waitForBlockedWriter blocks until another backend on this database is waiting
// on a lock.
func waitForBlockedWriter(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var blocked int
		err := db.QueryRowContext(t.Context(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
			  AND pid <> pg_backend_pid()`).Scan(&blocked)
		if err != nil {
			t.Fatalf("look for a blocked writer: %v", err)
		}
		if blocked > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no writer ever blocked on the version index")
}

// TestAWalletOpenedAtZeroCanStillMoveMoney is the regression for a three-way
// contradiction between ledger_chain, wallet_guard and wallet_matches_ledger.
//
// A wallet opened at zero records no opening — an OPENING has to carry a
// positive amount — so it stands at version 1 with an empty ledger behind it,
// and its first movement is therefore its second version. While ledger_chain
// assumed the first entry was always version 1, no assignment could satisfy all
// three rules at once: the entry wanted version 1, wallet_guard wanted the
// balance change to advance the version to 2, and wallet_matches_ledger wanted
// the two to agree. Every wallet opened empty was frozen for good, and nothing
// in the suite noticed because nothing ever moved money into one.
func TestAWalletOpenedAtZeroCanStillMoveMoney(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	t.Run("the first movement is the wallet's second version", func(t *testing.T) {
		w := newWallet(t, db, 0)
		win := processedBet(t, db, w)
		credit(w, win, 2500, 0, 2, base).settles(t, db)

		var balance, version int64
		var reconciles bool
		err := db.QueryRowContext(t.Context(), `
			SELECT balance_minor, version, wagering.reconcile_wallet(id)
			FROM wagering.wallet WHERE id = $1`, w.id).Scan(&balance, &version, &reconciles)
		if err != nil {
			t.Fatalf("read the wallet back: %v", err)
		}
		if balance != 2500 || version != 2 {
			t.Errorf("the wallet holds %d at version %d, wanted 2500 at version 2", balance, version)
		}
		if !reconciles {
			t.Error("the wallet does not reconcile with the ledger that moved it")
		}
	})

	// The anchor is the kind of the transaction that caused the entry, not a
	// count of rows, so an opening still records version 1 and only an opening
	// may. Without this the fix would merely move the wedge to funded wallets.
	t.Run("a first movement claiming version one is refused", func(t *testing.T) {
		w := newWallet(t, db, 0)
		win := processedBet(t, db, w)
		credit(w, win, 2500, 0, 1, base).refusedBy(t, db, "wallet_ledger_entry_first_records_the_opening_version")
	})

	t.Run("a wallet opened with money still anchors its opening at version one", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)
		debit(w, bet, 2500, 10000, 2, base).settles(t, db)
	})
}

// TestALedgerEntryBelongsToItsTransactionsWallet is the composite foreign key.
//
// Validated apart, wallet_id and transaction_id only say that each exists. An
// entry could therefore book one player's operation against another player's
// wallet, and nothing downstream would object: the arithmetic holds, the chain
// holds, and reconcile_wallet reports the receiving wallet as sound. It is money
// credited to a wallet no transaction authorised.
func TestALedgerEntryBelongsToItsTransactionsWallet(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	mine := newWallet(t, db, 10000)
	theirs := newWallet(t, db, 10000)
	bet := processedBet(t, db, mine)

	stolen := debit(theirs, bet, 2500, 10000, 2, base)
	stolen.refuses(t, db, foreignKeyViolation)
}

// TestLedgerAgreesWithItsWalletAtCommit is the other half of the pairing.
//
// wallet_matches_ledger is armed by writes to the wallet, so it was never
// evaluated for a transaction that appended an entry and left the wallet alone —
// and ledger_chain validates an entry against its predecessor entry, never
// against the balance the wallet is holding. Money could be added to the ledger
// that the wallet never received, the transaction committed, and
// reconcile_wallet then reported a mismatch nothing had refused.
func TestLedgerAgreesWithItsWalletAtCommit(t *testing.T) {
	t.Parallel()
	db := migrated(t)

	t.Run("an entry with no balance change behind it", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)
		commitRefusedBy(t, db, "wallet_matches_its_ledger", func(tx *sql.Tx) error {
			_, err := tx.ExecContext(t.Context(), insertLedgerEntry, debit(w, bet, 2500, 10000, 2, base).args()...)
			return err
		})
	})

	t.Run("an entry the wallet only partly followed", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)
		commitRefusedBy(t, db, "wallet_matches_its_ledger", func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(t.Context(), insertLedgerEntry, debit(w, bet, 2500, 10000, 2, base).args()...); err != nil {
				return err
			}
			// The wallet then makes a move that is legal in its own right --
			// the balance changes, the version advances by one, the clock goes
			// forward -- but lands somewhere the entry did not put it. Every
			// immediate guard is satisfied, so the deferred rule is the only
			// thing that can still object.
			_, err := tx.ExecContext(t.Context(), moveWallet, w.id, int64(8000), int64(2), settleAt(2))
			return err
		})
	})

	t.Run("an entry with the balance change it explains", func(t *testing.T) {
		w := newWallet(t, db, 10000)
		bet := processedBet(t, db, w)
		debit(w, bet, 2500, 10000, 2, base).settles(t, db)
	})
}
