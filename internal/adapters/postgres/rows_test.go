//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// nonBreakingPlayerID is an identifier the opaque_id domain accepts and
// parseOpaque refuses, spelled as an escape rather than as the character
// itself: a test whose whole point is one invisible byte should not hide that
// byte in its own source.
const nonBreakingPlayerID = "\u00a0acme"

// TestAStoredRowTheDomainRefusesIsReportedRatherThanLoaded is the promise
// rehydration makes, tested from the only side it can be.
//
// Every read here goes through the domain's Rehydrate functions, so a row the
// domain would refuse is corruption. The alternative — loading it anyway —
// would make writing a row directly and reading it back a way around every
// invariant the model enforces on the way in.
//
// The refusal is Unretryable whatever code it carries, and that is the point of
// classifying it here rather than letting the code speak. The first case below
// raises a CORRECTABLE code, which means "repair the payload and send it
// again": an instruction nobody can act on for a row that is already stored.
// The code still has to survive, because it is what tells an operator which
// column is wrong — so this is also the one test that watches a failure.Code
// travel back out through TxManager.
func TestAStoredRowTheDomainRefusesIsReportedRatherThanLoaded(t *testing.T) {
	t.Parallel()

	t.Run("an identifier the schema accepts and the domain does not", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)

		// docs/schema.md records this divergence as a known one: PostgreSQL's
		// [[:space:]] is ASCII, where Go's strings.TrimSpace covers the whole
		// Unicode White_Space property. A non-breaking space therefore passes
		// the opaque_id domain and is refused by parseOpaque — which makes it
		// the one corrupt row that can be written without disarming anything.
		id := wagering.NewWalletID()
		w.exec(t, `INSERT INTO wagering.wallet `+
			`(id, player_id, currency, balance_minor, version, created_at, updated_at) `+
			`VALUES ($1, $2, 'BRL', 0, 1, $3, $3)`, uuidOf(id), nonBreakingPlayerID, at(0))

		var wallet *wagering.Wallet
		err := w.tm.WithinMovement(t.Context(), func(ctx context.Context, r *app.Repos) error {
			var err error
			wallet, err = r.Wallets.ByID(ctx, id)
			return err
		})

		if err == nil {
			t.Fatalf("a row the domain refuses was loaded as %v", wallet)
		}
		if wallet != nil {
			t.Fatalf("a refused read also returned a wallet: %v", wallet)
		}
		classifies(t, err, app.Unretryable)

		// The code and the field are still findable after the manager has rolled
		// back and returned the error, which is the whole of the requirement
		// that an error keeps its chain.
		if !failure.Is(err, failure.InvalidFieldFormat) {
			t.Fatalf("the refusal's code did not survive TxManager: %v", err)
		}
		code, ok := app.CodeOf(err)
		if !ok || code != failure.InvalidFieldFormat {
			t.Fatalf("the application layer reads the code as %q, wanted %s",
				code, failure.InvalidFieldFormat)
		}
		ferr, ok := errors.AsType[*failure.Error](err)
		if !ok || ferr.Field != "playerId" {
			t.Fatalf("the refusal names field %q, wanted playerId: %v", ferr, err)
		}
	})

	// A version stored below zero would become an enormous version rather than
	// an error, because the column is signed and the domain's is not. Both
	// checks below are guards on that one conversion, and reaching either takes
	// disarming TWO things: the CHECK that refuses the value, and — through
	// w.corrupt — the triggers that refuse the write, since wallet_guard will
	// not let a version move without the balance and the ledger is append-only.
	// Needing to take both down is exactly what says these guards are defence
	// in depth rather than the only line.
	//
	// Neither database is put back together afterwards. Each is this test's own
	// and is discarded with it, and a half-repaired schema would be a worse
	// thing to leave a failing test looking at than a deliberately broken one.
	t.Run("a wallet version that cannot be one", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		wallet := w.openWallet(t, "player-negative-wallet", "100.00", "BRL")

		w.exec(t, `ALTER TABLE wagering.wallet DROP CONSTRAINT wallet_version_starts_at_one`)
		w.corrupt(t, "wagering.wallet",
			`UPDATE wagering.wallet SET version = -1 WHERE id = $1`, uuidOf(wallet.ID()))

		err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
			_, err := r.Wallets.ByID(ctx, wallet.ID())
			return err
		})
		if !errors.Is(err, errNegativeVersion) {
			t.Fatalf("a negative wallet version reported %v, wanted it refused", err)
		}
		classifies(t, err, app.Unretryable)
	})

	t.Run("a ledger entry version that cannot be one", func(t *testing.T) {
		t.Parallel()
		w := newWorld(t)
		wallet := w.openWallet(t, "player-negative-entry", "100.00", "BRL")

		w.exec(t, `ALTER TABLE wagering.wallet_ledger_entry `+
			`DROP CONSTRAINT wallet_ledger_entry_version_starts_at_one`)
		w.corrupt(t, "wagering.wallet_ledger_entry",
			`UPDATE wagering.wallet_ledger_entry SET wallet_version = -1 WHERE wallet_id = $1`,
			uuidOf(wallet.ID()))

		err := w.tm.WithinSnapshot(t.Context(), func(ctx context.Context, r *app.ReadRepos) error {
			_, err := r.Ledger.All(ctx, wallet.ID())
			return err
		})
		if !errors.Is(err, errNegativeVersion) {
			t.Fatalf("a negative entry version reported %v, wanted it refused", err)
		}
		classifies(t, err, app.Unretryable)
	})
}
