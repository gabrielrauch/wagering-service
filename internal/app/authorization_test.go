package app_test

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// Authorization is enforced here rather than at each transport, so that HTTP and
// SQS cannot drift apart, and it is enforced before any I/O so that a refusal is
// observably free of side effects: an unauthorized caller does not open a
// transaction, let alone write one.

func TestSubmitRefusesOperationsForAnotherProvider(t *testing.T) {
	f := newFixture(t)

	_, err := f.wagers.Submit(t.Context(), app.SubmitOperation{
		Principal:   providerPrincipal(t, acme),
		Correlation: "corr-1",
		Fields:      fields(rival, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}),
	})

	assertClass(t, err, app.Unauthorized)
	f.assertUntouched(t)
}

func TestSubmitRefusesTheServicePrincipal(t *testing.T) {
	f := newFixture(t)

	_, err := f.wagers.Submit(t.Context(), app.SubmitOperation{
		Principal:   servicePrincipal(t),
		Correlation: "corr-1",
		Fields:      fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}),
	})

	assertClass(t, err, app.Unauthorized)
	f.assertUntouched(t)
}

func TestResumeRefusesAnyoneButTheService(t *testing.T) {
	f := newFixture(t)

	_, err := f.wagers.Resume(t.Context(), providerPrincipal(t, acme))

	assertClass(t, err, app.Unauthorized)
	f.assertUntouched(t)
}

// Wallets are the service's to administer. A provider sees the operations it
// submitted and the balance each one reported, and nothing else.
func TestWalletOperationsRefuseAProvider(t *testing.T) {
	provider := providerPrincipal(t, acme)

	operations := map[string]func(*fixture) error{
		"open": func(f *fixture) error {
			_, _, err := f.wallets.Open(t.Context(), app.OpenWalletCommand{
				Principal: provider, Correlation: "corr-1",
				PlayerID: "player-1", InitialAmount: "100.00", Currency: "BRL",
			})
			return err
		},
		"by id": func(f *fixture) error {
			_, err := f.wallets.ByID(t.Context(), provider, wagering.NewWalletID())
			return err
		},
		"ledger": func(f *fixture) error {
			_, err := f.wallets.Ledger(t.Context(), provider, app.LedgerQuery{
				WalletID: wagering.NewWalletID(), Limit: 10,
			})
			return err
		},
		"reconcile": func(f *fixture) error {
			_, err := f.wallets.Reconcile(t.Context(), provider, wagering.NewWalletID())
			return err
		},
	}

	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			assertClass(t, operation(f), app.Unauthorized)
			f.assertUntouched(t)
		})
	}
}
