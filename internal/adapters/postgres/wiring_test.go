//go:build integration

package postgres

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// TestTheAdapterWiresIntoTheUseCases is the test this package would be useless
// without.
//
// Everything else here drives the ports directly, which proves each one behaves
// and proves nothing about whether they fit together the way the application
// expects. A repository that satisfies every interface and cannot be handed to
// NewWagering is a failed adapter, and the compiler only notices at the call
// site — which, until this test existed, was in a package nobody had written
// yet.
//
// The reads it makes are deliberately the boring ones. What the use cases do
// with money is their own tests' business; what is being checked here is that
// the manager, the stores and the two doors assemble, and that a value crossing
// the boundary in each direction arrives intact.
func TestTheAdapterWiresIntoTheUseCases(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	wallet := w.openWallet(t, "player-wired", "100.00", "BRL")
	w.apply(t, command(t, wagering.Bet, "player-wired", "ext-w-1", "10.00", "BRL"), at(1))
	w.apply(t, command(t, wagering.Bet, "player-wired", "ext-w-2", "20.00", "BRL"), at(2))

	// Through the same fixture the end-to-end scenarios wire themselves with,
	// so the package has one way of assembling the application layer rather
	// than two that could drift.
	u := w.wire(t, at(9))
	wagers, wallets := u.wagers, u.wallets
	service, provider := servicePrincipal(t), providerPrincipal(t)

	view, err := wallets.ByID(t.Context(), service, wallet.ID())
	if err != nil {
		t.Fatalf("read the wallet through the use case: %v", err)
	}
	if got := view.Balance.Amount(); got != "70.00" {
		t.Fatalf("the wallet reports %s, wanted 70.00", got)
	}

	// The cursor is the application layer's, opaque and encoded there. The
	// adapter's Page takes the decoded version, so this is the one assertion
	// that the two halves of pagination agree about what a position is.
	first, err := wallets.Ledger(t.Context(), service,
		app.LedgerQuery{WalletID: wallet.ID(), Limit: 2})
	if err != nil {
		t.Fatalf("page the ledger through the use case: %v", err)
	}
	if len(first.Entries) != 2 || first.NextCursor == "" {
		t.Fatalf("the first page holds %d entries with cursor %q, wanted 2 and a cursor",
			len(first.Entries), first.NextCursor)
	}
	second, err := wallets.Ledger(t.Context(), service, app.LedgerQuery{
		WalletID: wallet.ID(),
		Cursor:   first.NextCursor,
		Limit:    2,
	})
	if err != nil {
		t.Fatalf("page the ledger from a cursor: %v", err)
	}
	if len(second.Entries) != 1 {
		t.Fatalf("the second page holds %d entries, wanted 1", len(second.Entries))
	}
	if second.Entries[0].WalletVersion() != 3 {
		t.Fatalf("the second page starts at version %d, wanted 3",
			second.Entries[0].WalletVersion())
	}

	report, err := wallets.Reconcile(t.Context(), service, wallet.ID())
	if err != nil {
		t.Fatalf("reconcile through the use case: %v", err)
	}
	if !report.Consistent {
		t.Fatalf("the wallet does not reconcile: stored %s, ledger %s",
			report.Stored.Amount(), report.Reconstructed.Amount())
	}

	result, err := wagers.TransactionByExternalID(t.Context(), provider, "acme", "ext-w-1")
	if err != nil {
		t.Fatalf("read an operation through the use case: %v", err)
	}
	if result.Status != wagering.Processed {
		t.Fatalf("the operation is %s, wanted PROCESSED", result.Status)
	}
	if result.Balance == nil || result.Balance.Amount() != "90.00" {
		t.Fatalf("the operation reported %v, wanted the 90.00 it left behind", result.Balance)
	}

	// Nothing due, which is the resume worker's normal answer and the one a
	// wiring mistake in NextDue would turn into an error.
	resumed, err := wagers.Resume(t.Context(), service)
	if err != nil {
		t.Fatalf("resume through the use case: %v", err)
	}
	if resumed.Claimed {
		t.Fatalf("resume claimed %s with nothing parked", resumed.Result.TransactionID)
	}
}
