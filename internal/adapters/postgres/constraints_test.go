//go:build integration

package postgres

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// uniqueRules is every rule in this schema that a 23505 can name, and what each
// collision means to a caller.
//
// It is written out rather than derived so that the enumeration is a decision
// somebody made. A nil value means the collision is nothing a caller branches
// on: either it cannot happen without a minted identifier repeating, or it is
// derived state contradicting itself, and both are this system being wrong
// rather than a provider being told something.
//
// TestEveryUniqueRuleIsAccountedFor compares this against the live catalogue in
// both directions, so a unique index added to the schema without a decision
// here fails the build, and a decision here about a rule that no longer exists
// fails it too.
var uniqueRules = map[string]error{
	// A second wallet for a player and currency. The domain refuses this from
	// the wallet it was handed; this is what decides it under a race.
	"wallet_player_currency_key": app.ErrWalletExists,

	// The two keys a submission is unique on, and the third that carries the
	// pair plus the row's own id for the reference foreign key to target. All
	// three report one sentinel: a true replay collides on both keys at once
	// and the database names whichever index it checked first.
	"wager_transaction_provider_external_key":    app.ErrDuplicateSubmission,
	"wager_transaction_provider_idempotency_key": app.ErrDuplicateSubmission,
	"wager_transaction_provider_external_id_key": app.ErrDuplicateSubmission,

	// A reference has at most one active reversal, as a primary key rather than
	// a predicate.
	"active_reversal_pkey": app.ErrReferenceAlreadyReversed,

	// And never two successful reversals of one kind, whether or not the first
	// has since been undone. The same sentinel: both rules are refused by the
	// domain under REFERENCE_ALREADY_REVERSED, and a caller that reached either
	// index has the same wrongly built view to answer for.
	"wager_transaction_one_successful_reversal_per_kind": app.ErrReferenceAlreadyReversed,

	// One message, once, per consumer — and the serialisation point between two
	// consumers that both saw it. Not a sentinel: the loser has recorded
	// nothing and may send the work again, which errors.go says by classifying
	// it Retryable rather than by naming a condition the use cases would have
	// to learn.
	"inbox_pkey": nil,

	// Identifiers this system mints. A collision means a UUID repeated.
	"wallet_pkey":              nil,
	"wager_transaction_pkey":   nil,
	"wallet_ledger_entry_pkey": nil,
	"outbox_pkey":              nil,

	// Foreign-key targets: uniqueness the schema needs in order to point at
	// several columns at once, and which the primary key already guarantees.
	"wallet_player_key":                     nil,
	"wager_transaction_wallet_identity_key": nil,

	// A wallet has at most one opening credit, which only this system raises.
	"wager_transaction_one_opening_per_wallet": nil,

	// A reversal holds at most one thing. Maintained by the trigger, which
	// inserts exactly one row per reversal.
	"active_reversal_holds_one_reference": nil,

	// The ledger's own rules. One transaction moves a wallet's balance once,
	// and no two entries claim one version — the second being unreachable
	// serially, since the chain always demands the next version and the next
	// version is by definition free.
	"wallet_ledger_entry_transaction_key": nil,
	"wallet_ledger_entry_version_key":     nil,

	// Outbox numbering. Both are assigned by the database, so a collision is
	// the counter or the identity column being wrong.
	"outbox_sequence_key":            nil,
	"outbox_aggregate_sequence_key":  nil,
	"outbox_aggregate_sequence_pkey": nil,

	// The catalogues, which only a migration writes.
	"failure_code_pkey":          nil,
	"settling_failure_code_pkey": nil,
	"event_type_pkey":            nil,
}

// TestEveryUniqueRuleIsAccountedFor compares the mapping against the schema.
//
// Both directions matter and for different reasons. A rule in the schema that
// nobody decided about is a collision the adapter will report as an
// undifferentiated failure — which is how a duplicate submission would come to
// be reported as a server error. A rule in the mapping that the schema no
// longer has is worse: it is a branch that silently stops being taken, and
// nothing fails when it does.
func TestEveryUniqueRuleIsAccountedFor(t *testing.T) {
	t.Parallel()
	w := newWorld(t)

	const everyUniqueIndex = `SELECT i.relname FROM pg_index x ` +
		`JOIN pg_class i ON i.oid = x.indexrelid ` +
		`JOIN pg_class c ON c.oid = x.indrelid ` +
		`JOIN pg_namespace n ON n.oid = c.relnamespace ` +
		`WHERE n.nspname = 'wagering' AND x.indisunique AND c.relname <> 'schema_migrations'`

	rows, err := w.owner.Query(t.Context(), everyUniqueIndex)
	if err != nil {
		t.Fatalf("read the unique indexes: %v", err)
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan an index name: %v", err)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the unique indexes: %v", err)
	}

	for _, name := range slices.Sorted(maps.Keys(found)) {
		if _, decided := uniqueRules[name]; !decided {
			t.Errorf("the schema has %s and nothing here says what a collision on it means", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(uniqueRules)) {
		if !found[name] {
			t.Errorf("uniqueRules names %s, which the schema does not have", name)
		}
	}

	// And the adapter's own mapping is exactly the non-nil half.
	for name, sentinel := range uniqueRules {
		mapped, isMapped := portErrors[name]
		switch {
		case sentinel == nil && isMapped:
			t.Errorf("%s is mapped to %v but means nothing a caller branches on", name, mapped)
		case sentinel != nil && !isMapped:
			t.Errorf("%s should report %v and is not mapped", name, sentinel)
		case sentinel != nil && !errors.Is(mapped, sentinel):
			t.Errorf("%s is mapped to %v, wanted %v", name, mapped, sentinel)
		}
	}
	for name := range portErrors {
		if _, known := uniqueRules[name]; !known {
			t.Errorf("portErrors names %s, which is not a unique rule in this schema", name)
		}
	}
	for name := range transientRules {
		if _, known := uniqueRules[name]; !known {
			t.Errorf("transientRules names %s, which is not a unique rule in this schema", name)
		}
	}
}

// TestASecondWalletForOnePlayerAndCurrencyIsRefused proves the mapping for
// wallet_player_currency_key.
func TestASecondWalletForOnePlayerAndCurrencyIsRefused(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.openWallet(t, "player-twice", "100.00", "BRL")

	second, outcome := newWallet(t, "player-twice", "50.00", "BRL")
	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		return r.Open(ctx, second, outcome, "twice")
	})

	if !errors.Is(err, app.ErrWalletExists) {
		t.Fatalf("opening a second wallet reported %v, wanted ErrWalletExists", err)
	}
	// The rule that refused it survives into the chain, which is what an
	// operator reads when the sentinel alone does not say enough.
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.ConstraintName != "wallet_player_currency_key" {
		t.Fatalf("the refusing rule did not survive the wrapping: %v", err)
	}
	if got := w.count(t, `SELECT count(*) FROM wagering.wallet`); got != 1 {
		t.Fatalf("%d wallets exist, wanted 1", got)
	}
}

// TestADuplicateSubmissionIsRefusedOnEitherKey proves the mapping for both
// unique keys on wager_transaction, and that they report the same thing.
//
// The two cases are separated here and deliberately not distinguished by the
// adapter. A provider resubmitting the same operation under the same key
// collides on both at once, so a caller that branched on which index fired
// would be right only for the order the indexes happened to be created in.
func TestADuplicateSubmissionIsRefusedOnEitherKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		external string
		key      string
	}{
		{name: "the provider's own identifier", external: "ext-1", key: "key-other"},
		{name: "the idempotency key", external: "ext-other", key: "key-ext-1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			wallet := w.openWallet(t, "player-dup", "100.00", "BRL")
			w.apply(t, command(t, wagering.Bet, "player-dup", "ext-1", "10.00", "BRL"), at(1))

			second := command(t, wagering.Bet, "player-dup", c.external, "10.00", "BRL")
			second.ExternalTransactionID = wagering.ExternalTransactionID(c.external)
			second.IdempotencyKey = wagering.IdempotencyKey(c.key)

			err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
				tx, err := wagering.NewExternalTransaction(second, wallet.ID(), at(2))
				if err != nil {
					return err
				}
				return r.Transactions.Record(ctx, tx, "dup")
			})
			if !errors.Is(err, app.ErrDuplicateSubmission) {
				t.Fatalf("recording a duplicate reported %v, wanted ErrDuplicateSubmission", err)
			}
			if got := w.count(t,
				`SELECT count(*) FROM wagering.wager_transaction WHERE kind = 'BET'`); got != 1 {
				t.Fatalf("%d bets exist, wanted 1", got)
			}
		})
	}
}

// TestASecondReversalOfOneReferenceIsRefused proves the mapping for
// active_reversal_pkey.
//
// It has to reach past the domain to get there, and that is the point. The
// domain refuses a held reference properly — with a row, an event and
// REFERENCE_ALREADY_REVERSED — when the reference view shows the hold, so the
// only way to the trigger is a view built wrongly. This builds one, which is
// exactly the case app.ErrReferenceAlreadyReversed is documented as the
// backstop for: the rule then lives only in the schema, and the schema holds.
func TestASecondReversalOfOneReferenceIsRefused(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-reversed", "100.00", "BRL")

	bet := w.apply(t, command(t, wagering.Bet, "player-reversed", "ext-bet", "80.00", "BRL"), at(1))
	refund := reversingCommand(
		command(t, wagering.Refund, "player-reversed", "ext-refund", "80.00", "BRL"), "ext-bet")
	settled := w.apply(t, refund, at(2))
	if settled.Transaction.Status() != wagering.Processed {
		t.Fatalf("the refund is %s, wanted PROCESSED", settled.Transaction.Status())
	}

	rollback := reversingCommand(
		command(t, wagering.Rollback, "player-reversed", "ext-rollback", "80.00", "BRL"), "ext-bet")
	// The view the domain is given omits the hold that is really there.
	blind := &wagering.ReferenceView{Transaction: bet.Transaction}

	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		locked, err := r.Wallets.LockByID(ctx, wallet.ID())
		if err != nil {
			return err
		}
		tx, err := wagering.NewExternalTransaction(rollback, wallet.ID(), at(3))
		if err != nil {
			return err
		}
		if err := r.Transactions.Record(ctx, tx, "second-reversal"); err != nil {
			return err
		}
		outcome, err := processor(t).Continue(tx, rollback, locked, blind, at(3))
		if err != nil {
			return err
		}
		if outcome.Transaction.Status() != wagering.Processed {
			t.Fatalf("the blind rollback is %s, wanted PROCESSED", outcome.Transaction.Status())
		}
		return r.Settle(ctx, outcome, time.Time{})
	})

	if !errors.Is(err, app.ErrReferenceAlreadyReversed) {
		t.Fatalf("a second reversal reported %v, wanted ErrReferenceAlreadyReversed", err)
	}
	if got := w.count(t, `SELECT count(*) FROM wagering.active_reversal`); got != 1 {
		t.Fatalf("%d active reversals, wanted 1", got)
	}
	if got := w.count(t,
		`SELECT count(*) FROM wagering.wager_transaction WHERE kind = 'ROLLBACK'`); got != 0 {
		t.Fatalf("%d rollbacks survived, wanted 0", got)
	}
}

// TestASecondSuccessfulReversalOfOneKindIsRefused proves the mapping for
// wager_transaction_one_successful_reversal_per_kind, and reaches it the same
// way as the test above: with a view built wrongly.
//
// After BET → REFUND → ROLLBACK of the refund, nothing holds the bet, so a view
// that carries only the current holder shows the domain a free bet and a second
// refund goes through to the row. The adapter's real view carries the refund
// that was undone, flagged as reversed, and the domain refuses the repeat with
// a row and an event; this test hands the processor a view that has forgotten
// it, which is the only way to the index, and the index holds.
func TestASecondSuccessfulReversalOfOneKindIsRefused(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-rekind", "100.00", "BRL")

	bet := w.apply(t, command(t, wagering.Bet, "player-rekind", "ext-bet", "80.00", "BRL"), at(1))
	w.apply(t, reversingCommand(
		command(t, wagering.Refund, "player-rekind", "ext-refund", "80.00", "BRL"), "ext-bet"), at(2))
	w.apply(t, reversingCommand(
		command(t, wagering.Rollback, "player-rekind", "ext-undo", "80.00", "BRL"), "ext-refund"), at(3))
	if got := w.count(t, `SELECT count(*) FROM wagering.active_reversal WHERE reference_id = $1`,
		uuidOf(bet.Transaction.ID())); got != 0 {
		t.Fatalf("the bet is still held %d times after its refund was rolled back", got)
	}

	again := reversingCommand(
		command(t, wagering.Refund, "player-rekind", "ext-refund-2", "80.00", "BRL"), "ext-bet")
	// The view the domain is given has forgotten the refund that was undone.
	forgetful := &wagering.ReferenceView{Transaction: bet.Transaction}

	err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
		locked, err := r.Wallets.LockByID(ctx, wallet.ID())
		if err != nil {
			return err
		}
		tx, err := wagering.NewExternalTransaction(again, wallet.ID(), at(4))
		if err != nil {
			return err
		}
		if err := r.Transactions.Record(ctx, tx, "second-refund"); err != nil {
			return err
		}
		outcome, err := processor(t).Continue(tx, again, locked, forgetful, at(4))
		if err != nil {
			return err
		}
		if outcome.Transaction.Status() != wagering.Processed {
			t.Fatalf("the forgetful refund is %s, wanted PROCESSED", outcome.Transaction.Status())
		}
		return r.Settle(ctx, outcome, time.Time{})
	})

	if !errors.Is(err, app.ErrReferenceAlreadyReversed) {
		t.Fatalf("a second refund reported %v, wanted ErrReferenceAlreadyReversed", err)
	}
	refusedBy(t, err, "wager_transaction_one_successful_reversal_per_kind")
	if got := w.count(t,
		`SELECT count(*) FROM wagering.wager_transaction WHERE kind = 'REFUND' AND status = 'PROCESSED'`); got != 1 {
		t.Fatalf("%d processed refunds survived, wanted the first and nothing else", got)
	}
	if got := w.count(t, `SELECT balance_minor FROM wagering.wallet WHERE id = $1`,
		uuidOf(wallet.ID())); got != 2000 {
		t.Fatalf("the wallet holds %d minor units, wanted the 2000 the rolled-back refund left", got)
	}
}

// TestASecondConsumerOfOneMessageIsToldToTryAgain proves the mapping for
// inbox_pkey.
//
// Retryable rather than a sentinel. The loser recorded nothing, and by the time
// the message is redelivered the winner has committed — so the redelivery finds
// the row, sees the same body, and replays the settled result instead of
// reapplying it. Reporting it as a permanent failure would strand a message
// that is about to be handled correctly.
func TestASecondConsumerOfOneMessageIsToldToTryAgain(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	message := app.InboxMessage{Consumer: "wagering", MessageID: "m-1", BodyHash: bodyHash}

	record := func() error {
		return w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			return r.Inbox.Record(ctx, message, at(1))
		})
	}
	if err := record(); err != nil {
		t.Fatalf("record a message: %v", err)
	}
	classifies(t, record(), app.Retryable)

	if got := w.count(t, `SELECT count(*) FROM wagering.inbox`); got != 1 {
		t.Fatalf("%d inbox rows, wanted 1", got)
	}
}

// TestARuleNobodyBranchesOnIsPermanent is the other half of the mapping: every
// rule that is not one of the port's conditions classifies Unretryable, keeps
// the name of the rule that refused it, and does so whether a constraint or a
// trigger did the refusing.
func TestARuleNobodyBranchesOnIsPermanent(t *testing.T) {
	t.Parallel()

	t.Run("a foreign key", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		w.openWallet(t, "player-fk-rule", "100.00", "BRL")
		cmd := command(t, wagering.Bet, "player-fk-rule", "ext-nowhere", "10.00", "BRL")

		err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			// A wallet that does not exist, which is what a transaction booked
			// against another player's wallet would look like.
			tx, err := wagering.NewExternalTransaction(cmd, wagering.NewWalletID(), at(1))
			if err != nil {
				return err
			}
			return r.Transactions.Record(ctx, tx, "fk-rule")
		})
		classifies(t, err, app.Unretryable)
		refusedBy(t, err, "wager_transaction_wallet_fkey")
	})

	t.Run("a trigger", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		w.openWallet(t, "player-clock", "100.00", "BRL")
		// The opening stamped the wallet at at(0); a movement stamped with the
		// same instant does not move the clock forward.
		wallet := w.openWallet(t, "player-clock-2", "100.00", "BRL")
		cmd := command(t, wagering.Bet, "player-clock-2", "ext-clock", "10.00", "BRL")

		err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			locked, err := r.Wallets.LockByID(ctx, wallet.ID())
			if err != nil {
				return err
			}
			tx, err := wagering.NewExternalTransaction(cmd, wallet.ID(), at(0))
			if err != nil {
				return err
			}
			if err := r.Transactions.Record(ctx, tx, "clock"); err != nil {
				return err
			}
			outcome, err := processor(t).Continue(tx, cmd, locked, nil, at(0))
			if err != nil {
				return err
			}
			return r.Settle(ctx, outcome, time.Time{})
		})
		classifies(t, err, app.Unretryable)
		refusedBy(t, err, "wallet_clock_moves_forward")
	})
}

// bodyHash is a well-formed sha256_hex, which the inbox's domain requires. Its
// value says nothing; only its shape matters here.
const bodyHash = "0000000000000000000000000000000000000000000000000000000000000001"
