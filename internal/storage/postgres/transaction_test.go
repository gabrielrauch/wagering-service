package postgres_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

const insertTransaction = `
	INSERT INTO wagering.wager_transaction (
		id, wallet_id, player_id, currency, kind, status, amount_minor,
		provider, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
		reference_external_transaction_id, resolved_reference_id, correlation_id,
		result_balance_minor, failure_code,
		reference_attempts, reference_deadline, reference_next_attempt_at,
		created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7,
		$8, $9, $10, $11, $12, $13,
		$14, $15, $16,
		$17, $18,
		$19, $20, $21,
		$22, $23
	)`

// txn is a row under construction. Every field is an any so that nil means SQL
// NULL, which is what most of these columns are for an opening and what all of
// the settlement columns are until something settles.
type txn struct {
	// id is the primary key: never null, so it is typed. The rest carry columns
	// that can be null, which is what any is doing in here.
	id                                                 string
	walletID, playerID, currency, kind, status, amount any
	provider, externalID, idempotencyKey, payloadHash  any
	roundID, gameID                                    any
	referenceExternalID, resolvedReferenceID           any
	correlation                                        any
	result, failureCode                                any
	attempts, deadline, nextAttempt                    any
	createdAt, updatedAt                               any
}

func (x txn) args() []any {
	return []any{
		x.id, x.walletID, x.playerID, x.currency, x.kind, x.status, x.amount,
		x.provider, x.externalID, x.idempotencyKey, x.payloadHash, x.roundID, x.gameID,
		x.referenceExternalID, x.resolvedReferenceID, x.correlation,
		x.result, x.failureCode,
		x.attempts, x.deadline, x.nextAttempt,
		x.createdAt, x.updatedAt,
	}
}

func (x txn) accepts(t *testing.T, db execer) {
	t.Helper()
	accepts(t, db, insertTransaction, x.args()...)
}

func (x txn) refuses(t *testing.T, db execer, state string) {
	t.Helper()
	refuses(t, db, state, insertTransaction, x.args()...)
}

// refusedBy asserts which rule turned the transaction down.
func (x txn) refusedBy(t *testing.T, db execer, rule string) {
	t.Helper()
	refusesRule(t, db, rule, insertTransaction, x.args()...)
}

// externalTx is a well-formed provider submission, pending, moving 25.00.
func externalTx(w wallet, kind string) txn {
	id := wagering.NewTransactionID().String()
	external := "ext-" + id
	return txn{
		id: id, walletID: w.id, playerID: w.playerID, currency: w.currency,
		kind: kind, status: "PENDING", amount: int64(2500),
		provider: "acme", externalID: external, idempotencyKey: "key-" + id,
		payloadHash: hashOf(external), roundID: "round-1", gameID: "game-1",
		correlation: "corr-" + id,
		attempts:    0, createdAt: base, updatedAt: base,
	}
}

// openingTx is the internal transaction that records a wallet's starting
// balance. It is born processed and carries no provider side at all.
func openingTx(w wallet, amount int64) txn {
	id := wagering.NewTransactionID().String()
	return txn{
		id: id, walletID: w.id, playerID: w.playerID,
		currency: w.currency, kind: "OPENING", status: "PROCESSED", amount: amount,
		// An opening carries no provider side, but it does carry a correlation:
		// the service opened this wallet on behalf of some request too.
		correlation: "corr-" + id,
		result:      amount, attempts: 0, createdAt: base, updatedAt: base,
	}
}

// TestEveryTransactionCarriesACorrelation pins the part of correlation_id that
// is not obvious from the column definition: it is required on an OPENING too.
//
// The provider fields are all-or-nothing by origin, so the reflex is to assume
// anything a provider did not send is nullable for an internal transaction. The
// correlation is ours rather than theirs -- the service opened that wallet on
// behalf of some request as well -- which is why it sits outside
// wager_transaction_origin_carries_its_fields and applies to every row.
func TestEveryTransactionCarriesACorrelation(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	t.Run("a provider submission", func(t *testing.T) {
		bet := externalTx(w, "BET")
		bet.correlation = nil
		bet.refuses(t, db, pgerrcode.NotNullViolation)
	})

	t.Run("an opening", func(t *testing.T) {
		opening := openingTx(w, 5000)
		opening.correlation = nil
		opening.refuses(t, db, pgerrcode.NotNullViolation)
	})
}

func hashOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestTransactionBelongsToItsWallet is the composite foreign key: a transaction
// names the wallet and the player together, so a row claiming a player the
// wallet does not have cannot exist. Player misattribution is made
// unrepresentable rather than merely checked.
//
// The currency is deliberately NOT part of the key since 000009. A provider
// names the wallet it addresses, so it can address a wallet the player holds
// in a currency the wallet is not denominated in, and the domain settles that
// as CURRENCY_MISMATCH — a REJECTED row carrying the money the provider asked
// for. A key that made such a row unrepresentable would make the rejection
// unrecordable.
func TestTransactionBelongsToItsWallet(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	t.Run("the wallet, player and currency it actually has", func(t *testing.T) {
		externalTx(w, "BET").accepts(t, db)
	})

	t.Run("another player's identifier", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.playerID = "somebody-else"
		x.refuses(t, db, foreignKeyViolation)
	})

	t.Run("a currency the wallet is not denominated in is the domain's to settle", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.currency = "USD"
		x.accepts(t, db)
	})

	t.Run("a wallet that does not exist", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.walletID = wagering.NewWalletID().String()
		x.refuses(t, db, foreignKeyViolation)
	})
}

// TestAProcessedTransactionIsInItsWalletsCurrency restores, as a trigger, the
// half of the old three-column key that 000009 could not keep.
//
// The key pinned every transaction's currency to its wallet's, which made a
// REJECTED CURRENCY_MISMATCH row unrepresentable — and a rejection that cannot
// be recorded is not a rejection. The key was narrowed to (wallet_id, player_id)
// so that the domain can settle the mismatch, and this trigger says what the
// key used to say about the rows that matter: nothing PROCESSED sits in a
// currency its wallet does not hold. The ledger already refuses such an entry;
// this reaches the kinds that write none, a LOSS above all.
func TestAProcessedTransactionIsInItsWalletsCurrency(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	const rule = "wager_transaction_processed_in_wallet_currency"

	t.Run("a processed loss in another currency", func(t *testing.T) {
		loss := externalTx(w, "LOSS")
		loss.currency, loss.amount = "USD", int64(0)
		loss.status, loss.result = "PROCESSED", int64(10000)
		loss.refusedBy(t, db, rule)
	})

	t.Run("a processed loss in the wallet's currency", func(t *testing.T) {
		loss := externalTx(w, "LOSS")
		loss.amount = int64(0)
		loss.status, loss.result = "PROCESSED", int64(10000)
		loss.accepts(t, db)
	})

	t.Run("a rejected bet in another currency is the domain's to settle", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.currency = "USD"
		x.status, x.failureCode = "REJECTED", "CURRENCY_MISMATCH"
		x.accepts(t, db)
	})

	t.Run("a pending bet in another currency may still be rejected", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.currency = "USD"
		x.accepts(t, db)
	})

	t.Run("settling that pending bet as processed", func(t *testing.T) {
		x := externalTx(w, "LOSS")
		x.currency, x.amount = "USD", int64(0)
		x.accepts(t, db)

		refusesRule(t, db, rule, `
			UPDATE wagering.wager_transaction
			SET status = 'PROCESSED', result_balance_minor = $2, updated_at = $3
			WHERE id = $1`, x.id, int64(10000), base.Add(time.Second))
	})

	t.Run("settling it as rejected", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.currency = "USD"
		x.accepts(t, db)

		accepts(t, db, `
			UPDATE wagering.wager_transaction
			SET status = 'REJECTED', failure_code = 'CURRENCY_MISMATCH', updated_at = $2
			WHERE id = $1`, x.id, base.Add(time.Second))
	})
}

// TestOriginDecidesTheProviderFields is validateOrigin as one counting
// constraint: an internal transaction carries none of the six provider-owned
// fields and an external one carries all six.
func TestOriginDecidesTheProviderFields(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	t.Run("an opening carries no provider fields", func(t *testing.T) {
		openingTx(w, 10000).accepts(t, db)
	})

	t.Run("an opening that claims a provider", func(t *testing.T) {
		x := openingTx(w, 10000)
		x.provider = "acme"
		x.refuses(t, db, checkViolation)
	})

	t.Run("an external operation missing one provider field", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.gameID = nil
		x.refuses(t, db, checkViolation)
	})

	t.Run("an external operation missing its payload hash", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.payloadHash = nil
		x.refuses(t, db, checkViolation)
	})

	t.Run("an opening is born processed", func(t *testing.T) {
		x := openingTx(w, 10000)
		x.status = "PENDING"
		x.result = nil
		x.refuses(t, db, checkViolation)
	})
}

// TestOriginIsDerivedFromTheKind checks the generated column: origin is
// queryable without being a second place the answer is stored.
func TestOriginIsDerivedFromTheKind(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	opening := openingTx(w, 10000)
	opening.accepts(t, db)
	bet := externalTx(w, "BET")
	bet.accepts(t, db)

	for _, c := range []struct{ id, want any }{
		{opening.id, "INTERNAL"},
		{bet.id, "EXTERNAL"},
	} {
		var origin string
		err := db.QueryRowContext(t.Context(), `SELECT origin FROM wagering.wager_transaction WHERE id = $1`, c.id).Scan(&origin)
		if err != nil {
			t.Fatalf("read origin: %v", err)
		}
		if origin != c.want {
			t.Errorf("origin is %q, wanted %q", origin, c.want)
		}
	}
}

// TestAmountFollowsTheKind is Kind.checkAmount: a LOSS moves nothing and
// everything else moves something.
func TestAmountFollowsTheKind(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	cases := []struct {
		name     string
		kind     string
		amount   int64
		accepted bool
	}{
		{"a bet moves money", "BET", 2500, true},
		{"a bet of nothing", "BET", 0, false},
		{"a loss moves nothing", "LOSS", 0, true},
		{"a loss that moves money", "LOSS", 2500, false},
		{"a win moves money", "WIN", 2500, true},
		{"a win of nothing", "WIN", 0, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := externalTx(w, c.kind)
			x.amount = c.amount
			if c.accepted {
				x.accepts(t, db)
			} else {
				x.refuses(t, db, checkViolation)
			}
		})
	}
}

// TestReferenceFollowsTheKind is Kind.checkReference: a reversal must name what
// it reverses, a win may, and nothing else may.
func TestReferenceFollowsTheKind(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	cases := []struct {
		name      string
		kind      string
		reference any
		accepted  bool
	}{
		{"a refund names what it returns", "REFUND", "ext-bet", true},
		{"a refund naming nothing", "REFUND", nil, false},
		{"a rollback names what it undoes", "ROLLBACK", "ext-bet", true},
		{"a rollback naming nothing", "ROLLBACK", nil, false},
		{"a win may name the bet it pays out on", "WIN", "ext-bet", true},
		{"a win need not name anything", "WIN", nil, true},
		{"a bet names nothing", "BET", nil, true},
		{"a bet that names something", "BET", "ext-bet", false},
		{"a loss that names something", "LOSS", "ext-bet", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := externalTx(w, c.kind)
			x.referenceExternalID = c.reference
			if c.kind == "LOSS" {
				x.amount = int64(0)
			}
			if c.accepted {
				x.accepts(t, db)
			} else {
				x.refuses(t, db, checkViolation)
			}
		})
	}

	t.Run("an operation cannot reference itself", func(t *testing.T) {
		x := externalTx(w, "REFUND")
		x.referenceExternalID = x.externalID
		x.refuses(t, db, checkViolation)
	})

	t.Run("nothing resolved that was never named", func(t *testing.T) {
		bet := externalTx(w, "BET")
		bet.accepts(t, db)

		x := externalTx(w, "WIN")
		x.referenceExternalID = nil
		x.resolvedReferenceID = bet.id
		x.refuses(t, db, checkViolation)
	})
}

// TestSettlementMatchesTheStatus is validateSettlement: the resulting balance
// is present exactly when processed, and the failure code exactly when
// rejected.
func TestSettlementMatchesTheStatus(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	t.Run("a processed transaction carries the balance it reported", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.status, x.result = "PROCESSED", int64(7500)
		x.accepts(t, db)
	})

	t.Run("a processed transaction without one", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.status = "PROCESSED"
		x.refuses(t, db, checkViolation)
	})

	t.Run("a pending transaction that already reports a balance", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.result = int64(7500)
		x.refuses(t, db, checkViolation)
	})

	t.Run("a rejected transaction carries the code that settled it", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.status, x.failureCode = "REJECTED", "INSUFFICIENT_FUNDS"
		x.accepts(t, db)
	})

	t.Run("a rejected transaction without one", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.status = "REJECTED"
		x.refuses(t, db, checkViolation)
	})

	t.Run("a failed transaction that names a code", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.status, x.failureCode = "FAILED", "INSUFFICIENT_FUNDS"
		x.refuses(t, db, checkViolation)
	})

	t.Run("a reported balance below zero", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.status, x.result = "PROCESSED", int64(-1)
		x.refuses(t, db, checkViolation)
	})
}

// TestOnlyASettlingCodeSettles is checkSettlingCode, enforced by a foreign key
// into the settling subset rather than by a list the schema has to maintain.
func TestOnlyASettlingCodeSettles(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	cases := []struct {
		name     string
		code     string
		accepted bool
	}{
		{"a definitive code", "INSUFFICIENT_FUNDS", true},
		{"another definitive code", "REFERENCE_ALREADY_REVERSED", true},
		{"a correctable code, which persists nothing", "MISSING_REQUIRED_FIELD", false},
		{"an audit code, which settles no operation", "LEDGER_BALANCE_MISMATCH", false},
		{"a code nobody declared", "SOMETHING_WENT_WRONG", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := externalTx(w, "BET")
			x.status, x.failureCode = "REJECTED", c.code
			if c.accepted {
				x.accepts(t, db)
			} else {
				x.refuses(t, db, foreignKeyViolation)
			}
		})
	}
}

// TestWaitBudgetIsConsistent is validateWaitBudget, plus the scheduling column
// the domain deliberately does not model.
func TestWaitBudgetIsConsistent(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	deadline := base.Add(time.Minute)

	t.Run("a transaction that has never waited carries no deadline", func(t *testing.T) {
		externalTx(w, "BET").accepts(t, db)
	})

	t.Run("a deadline with no attempts behind it", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.deadline = deadline
		x.refuses(t, db, checkViolation)
	})

	t.Run("attempts with no deadline behind them", func(t *testing.T) {
		x := externalTx(w, "REFUND")
		x.referenceExternalID = "ext-bet"
		x.attempts = 1
		x.refuses(t, db, checkViolation)
	})

	t.Run("negative attempts", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.attempts = -1
		x.refuses(t, db, checkViolation)
	})

	t.Run("a waiting transaction has waited at least once", func(t *testing.T) {
		x := externalTx(w, "REFUND")
		x.referenceExternalID, x.status = "ext-bet", "PENDING_REFERENCE"
		x.refuses(t, db, checkViolation)
	})

	t.Run("a waiting transaction is scheduled to be looked at again", func(t *testing.T) {
		x := externalTx(w, "REFUND")
		x.referenceExternalID, x.status = "ext-bet", "PENDING_REFERENCE"
		x.attempts, x.deadline, x.nextAttempt = 1, deadline, base.Add(time.Second)
		x.accepts(t, db)
	})

	t.Run("a waiting transaction with nothing scheduled", func(t *testing.T) {
		x := externalTx(w, "REFUND")
		x.referenceExternalID, x.status = "ext-bet", "PENDING_REFERENCE"
		x.attempts, x.deadline = 1, deadline
		x.refuses(t, db, checkViolation)
	})

	t.Run("a settled transaction still scheduled", func(t *testing.T) {
		x := externalTx(w, "REFUND")
		x.referenceExternalID = "ext-bet"
		x.status, x.result = "PROCESSED", int64(7500)
		x.attempts, x.deadline, x.nextAttempt = 1, deadline, base.Add(time.Second)
		x.refuses(t, db, checkViolation)
	})
}

// TestIdempotencyIsPersistent is the pair of unique constraints that make a
// resubmission find the transaction it already produced. The second insert
// blocking and then failing is also what serialises two concurrent duplicates:
// one row wins and the others see it after commit.
func TestIdempotencyIsPersistent(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	first := externalTx(w, "BET")
	first.accepts(t, db)

	t.Run("the same operation under a different key", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.externalID = first.externalID
		x.refuses(t, db, uniqueViolation)
	})

	t.Run("a different operation under the same key", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.idempotencyKey = first.idempotencyKey
		x.refuses(t, db, uniqueViolation)
	})

	t.Run("the same identifiers from another provider", func(t *testing.T) {
		x := externalTx(w, "BET")
		x.provider = "other-provider"
		x.externalID, x.idempotencyKey = first.externalID, first.idempotencyKey
		x.accepts(t, db)
	})

	t.Run("two openings, which carry no provider at all", func(t *testing.T) {
		openingTx(newWallet(t, db, 0), 10000).accepts(t, db)
		openingTx(newWallet(t, db, 0), 5000).accepts(t, db)
	})
}

func TestAWalletHasAtMostOneOpening(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 0)

	openingTx(w, 10000).accepts(t, db)
	openingTx(w, 10000).refuses(t, db, uniqueViolation)
}

// TestASettledTransactionCannotBeRewritten is the guard the other two financial
// tables always had and this one did not.
//
// The wallet has wallet_guard and the ledger has ledger_append_only, but the
// table holding every provider identity, kind and amount had no BEFORE UPDATE
// trigger at all while the application held UPDATE on all of its columns. The
// two unique constraints then only ever constrained the current tuple, and
// nothing pinned the tuple: a settled operation could be rewritten into a
// different one, and the idempotency key it was submitted under freed for a
// replay of the very operation it exists to make unrepeatable.
func TestASettledTransactionCannotBeRewritten(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)
	elsewhere := newWallet(t, db, 10000)

	// The guard splits these three ways: what the operation is, what the
	// provider submitted, and which way the clock runs. The rule each column
	// falls under is part of what is being pinned.
	const (
		operation = "wager_transaction_operation_is_immutable"
		provider  = "wager_transaction_provider_fields_are_immutable"
		clock     = "wager_transaction_clock_moves_forward"
	)

	for _, c := range []struct {
		name   string
		rule   string
		column string
		value  any
	}{
		{"the provider's own identifier", provider, "external_transaction_id", "ext-rewritten"},
		{"the idempotency key", provider, "idempotency_key", "key-recycled"},
		{"the provider", provider, "provider", "someone-else"},
		{"the payload hash", provider, "payload_hash", hashOf("other")},
		{"the round", provider, "round_id", "round-2"},
		{"the kind", operation, "kind", "WIN"},
		{"the amount", operation, "amount_minor", int64(999999)},
		{"the wallet", operation, "wallet_id", elsewhere.id},
		{"the creation time", operation, "created_at", base.Add(time.Hour)},
		{"the correlation it arrived under", operation, "correlation_id", "corr-rewritten"},
		{"the clock running backwards", clock, "updated_at", base.Add(-time.Hour)},
	} {
		t.Run(c.name, func(t *testing.T) {
			bet := processedBet(t, db, w)
			refusesRule(t, db, c.rule,
				`UPDATE wagering.wager_transaction SET `+c.column+` = $2 WHERE id = $1`, bet.id, c.value)
		})
	}

	t.Run("a terminal status", func(t *testing.T) {
		bet := processedBet(t, db, w)
		refusesRule(t, db, "wager_transaction_terminal_status_is_final", `
			UPDATE wagering.wager_transaction SET status = 'PENDING', result_balance_minor = NULL
			WHERE id = $1`, bet.id)
	})

	t.Run("returning to PENDING", func(t *testing.T) {
		waiting := externalTx(w, "REFUND")
		waiting.referenceExternalID = "ext-missing"
		waiting.status, waiting.attempts = "PENDING_REFERENCE", 1
		waiting.deadline, waiting.nextAttempt = base.Add(time.Minute), base.Add(time.Second)
		waiting.accepts(t, db)

		refusesRule(t, db, "wager_transaction_does_not_return_to_pending", `
			UPDATE wagering.wager_transaction SET status = 'PENDING', reference_next_attempt_at = NULL
			WHERE id = $1`, waiting.id)
	})

	t.Run("a resolution that has already been decided", func(t *testing.T) {
		bet := processedBet(t, db, w)
		other := processedBet(t, db, w)
		refund := reversalOf(w, "REFUND", bet)
		refund.accepts(t, db)

		refusesRule(t, db, "wager_transaction_resolution_is_decided_once",
			`UPDATE wagering.wager_transaction SET resolved_reference_id = $2 WHERE id = $1`,
			refund.id, other.id)
	})

	// The key stays bound to the operation it was submitted for, so the replay
	// the rewrite used to open up is still the duplicate it always was.
	t.Run("the key it was submitted under stays taken", func(t *testing.T) {
		bet := processedBet(t, db, w)
		replay := externalTx(w, "BET")
		replay.idempotencyKey = bet.idempotencyKey
		replay.refuses(t, db, uniqueViolation)
	})
}

// TestSettlingATransactionIsStillAllowed is the other side of the guard: it has
// to refuse a rewrite without refusing the one update the table exists for.
func TestSettlingATransactionIsStillAllowed(t *testing.T) {
	t.Parallel()
	db := migrated(t)
	w := newWallet(t, db, 10000)

	t.Run("pending to processed", func(t *testing.T) {
		pending := externalTx(w, "WIN")
		pending.accepts(t, db)
		accepts(t, db, `
			UPDATE wagering.wager_transaction
			SET status = 'PROCESSED', result_balance_minor = $2, updated_at = $3
			WHERE id = $1`, pending.id, int64(12500), base.Add(time.Second))
	})

	t.Run("pending to rejected", func(t *testing.T) {
		pending := externalTx(w, "BET")
		pending.accepts(t, db)
		accepts(t, db, `
			UPDATE wagering.wager_transaction
			SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS', updated_at = $2
			WHERE id = $1`, pending.id, base.Add(time.Second))
	})

	t.Run("parking to wait for a reference", func(t *testing.T) {
		pending := externalTx(w, "ROLLBACK")
		pending.referenceExternalID = "ext-not-here-yet"
		pending.accepts(t, db)
		accepts(t, db, `
			UPDATE wagering.wager_transaction
			SET status = 'PENDING_REFERENCE', reference_attempts = 1,
			    reference_deadline = $2, reference_next_attempt_at = $3, updated_at = $4
			WHERE id = $1`, pending.id, base.Add(time.Minute), base.Add(time.Second), base.Add(time.Second))
	})

	t.Run("resolving a reference that was not known at submission", func(t *testing.T) {
		bet := processedBet(t, db, w)
		waiting := externalTx(w, "REFUND")
		waiting.referenceExternalID = bet.externalID
		waiting.status, waiting.attempts = "PENDING_REFERENCE", 1
		waiting.deadline, waiting.nextAttempt = base.Add(time.Minute), base.Add(time.Second)
		waiting.accepts(t, db)

		accepts(t, db, `
			UPDATE wagering.wager_transaction
			SET status = 'PROCESSED', resolved_reference_id = $2, result_balance_minor = $3,
			    reference_next_attempt_at = NULL, updated_at = $4
			WHERE id = $1`, waiting.id, bet.id, int64(12500), base.Add(time.Second))

		heldBy(t, db, bet, waiting)
	})
}
