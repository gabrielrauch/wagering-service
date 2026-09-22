//go:build multi

// The instrument every other scenario finishes with, and the proof that it is
// an instrument rather than a formality.
package multi

import (
	"context"
	"net/http"
	"testing"
)

// reconciled asserts, through the endpoint, that every wallet named holds what
// its ledger says it should.
//
// Every scenario in this suite ends with this call, over every wallet it
// touched. That is the ninth requirement, and it is deliberately asked through
// the API rather than with a SELECT: the endpoint is what an operator has, it
// rebuilds the balance from the ledger inside the service rather than in a
// test's arithmetic, and a suite that summed the entries itself would be
// checking its own addition against the service's.
//
// Three things are checked and not one. `consistent` is the endpoint's own
// verdict, the two amounts are what it compared, and `difference` is which way
// it is out — because an endpoint that always answered `consistent: true` would
// pass the first check and fail the other two.
func reconciled(t *testing.T, base string, wallets ...string) {
	t.Helper()
	for _, wallet := range wallets {
		report := reconcile(t, base, wallet)
		if !report.Consistent {
			t.Errorf("wallet %s does not balance: it holds %s and its ledger adds up to %s, "+
				"a difference of %s", wallet, report.StoredBalance.Amount,
				report.CalculatedBalance.Amount, report.Difference.Amount)
			continue
		}
		if report.StoredBalance != report.CalculatedBalance {
			t.Errorf("wallet %s is called consistent while holding %s against a ledger of %s",
				wallet, report.StoredBalance.Amount, report.CalculatedBalance.Amount)
		}
		if report.Difference.Amount != "0.00" {
			t.Errorf("wallet %s is called consistent with a difference of %s",
				wallet, report.Difference.Amount)
		}
	}
}

// reconcile asks the endpoint about one wallet.
//
// It answers 200 whether or not the wallet balances, so the status says the
// check ran and the body says what it found — which is why this insists on the
// status separately from reading the verdict.
func reconcile(t *testing.T, base, wallet string) reconciliation {
	t.Helper()
	got := send(t, call{
		base:   base,
		method: http.MethodPost,
		path:   "/wallets/" + wallet + "/reconciliation",
		token:  token(t, walletService),
	})
	if got.status != http.StatusOK {
		t.Fatalf("reconciling %s answered %s, wanted 200", wallet, got)
	}
	var report reconciliation
	decode(t, got, &report)
	return report
}

// TestTheReconciliationEndpointFindsAWalletThatDoesNotBalance moves a stored
// balance by one minor unit behind the service's back and asks the endpoint
// about it.
//
// Without this, the ninth requirement is a formality: every other scenario ends
// by asserting `consistent: true`, and an endpoint that answered `true`
// unconditionally would make all nine of them green. The single penny is
// deliberate — it is the smallest divergence the contract can express, so an
// endpoint that noticed it notices anything.
//
// # Producing a divergence is harder than it sounds, and that is a finding
//
// The service's own role cannot make this write at all — wagering_app has no
// UPDATE on the ledger — and the OWNER cannot make it either: the constraint
// trigger wallet_matches_ledger refuses to let a wallet's balance and the end
// of its ledger disagree at commit, and says which two numbers it compared.
// The schema is a second instrument, ahead of this one, and it is worth knowing
// that it holds.
//
// So the divergence is produced with the triggers switched off for one
// transaction — SET LOCAL session_replication_role, which is scoped to this
// transaction and to this connection and reaches nothing else in the run. What
// that models is the one way a divergence can really arrive: not through the
// service, which cannot write it, but through a restore, a manual repair or a
// replication stream, which is exactly what the reconciliation endpoint is for.
func TestTheReconciliationEndpointFindsAWalletThatDoesNotBalance(t *testing.T) {
	t.Parallel()
	w := newWorld(t, "audit")

	player := scoped("player-audit")
	wallet := openWallet(t, w.base, player, "100.00")
	staked := operationOf(t, submit(t, w.base, providerA,
		bet(providerA, scoped("audit-bet"), wallet, "30.00"), scoped("audit-key")))
	if staked.Status != processed {
		t.Fatalf("the bet is %s (%s), wanted %s", staked.Status, staked.FailureCode, processed)
	}
	reconciled(t, w.base, wallet.WalletID)

	moveBalanceBehindTheSchemasBack(t, w, wallet.WalletID, +1)

	divergent := reconcile(t, w.base, wallet.WalletID)
	if divergent.Consistent {
		t.Fatalf("the endpoint called a wallet consistent that holds %s against a ledger of "+
			"%s, so every other scenario's reconciliation proves nothing",
			divergent.StoredBalance.Amount, divergent.CalculatedBalance.Amount)
	}
	if divergent.StoredBalance.Amount != "70.01" || divergent.CalculatedBalance.Amount != "70.00" {
		t.Errorf("the endpoint reports %s stored against %s reconstructed, wanted 70.01 "+
			"against 70.00", divergent.StoredBalance.Amount, divergent.CalculatedBalance.Amount)
	}
	if divergent.Difference.Amount != "0.01" {
		t.Errorf("the endpoint reports a difference of %s, wanted 0.01",
			divergent.Difference.Amount)
	}

	// And back, so that the instrument is shown to answer both ways rather than
	// to have got stuck on the second one.
	moveBalanceBehindTheSchemasBack(t, w, wallet.WalletID, -1)
	reconciled(t, w.base, wallet.WalletID)
}

// moveBalanceBehindTheSchemasBack shifts a stored balance by minor units
// without touching the ledger, which nothing is allowed to do.
//
// The triggers are switched off for the transaction rather than dropped,
// because dropping one would leave the world's database different from every
// other database this schema produces — and this is the only write in the suite
// that the schema is entitled to refuse.
func moveBalanceBehindTheSchemasBack(t *testing.T, w *world, wallet string, by int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := w.owner.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the transaction that moves the balance: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// LOCAL, so it is undone by the commit below whatever happens, and so a
	// pooled connection cannot be handed back with the triggers still off.
	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = 'replica'"); err != nil {
		t.Fatalf("switch the triggers off: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE wagering.wallet SET balance_minor = balance_minor + $2, `+
			`version = version + 1, updated_at = now() WHERE id = $1`,
		wallet, by); err != nil {
		t.Fatalf("move the stored balance: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the moved balance: %v", err)
	}
}
