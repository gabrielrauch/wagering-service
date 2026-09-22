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

// Reading and submitting authorise differently, and the difference is the
// service: it submits as nobody and reads as anybody. A provider is held to
// itself either way.
func TestReadingByExternalIDAuthorisesDifferentlyFromSubmitting(t *testing.T) {
	cases := []struct {
		name      string
		principal func(*testing.T) app.Principal
		as        wagering.Provider
		refused   bool
	}{
		{
			name:      "a provider reading as itself",
			principal: func(t *testing.T) app.Principal { return providerPrincipal(t, acme) },
			as:        acme,
		},
		{
			name:      "a provider reading as another",
			principal: func(t *testing.T) app.Principal { return providerPrincipal(t, acme) },
			as:        rival,
			refused:   true,
		},
		{
			name:      "the service reading as a provider",
			principal: func(t *testing.T) app.Principal { return servicePrincipal(t) },
			as:        acme,
		},
		{
			name:      "the service reading as another provider",
			principal: func(t *testing.T) app.Principal { return servicePrincipal(t) },
			as:        rival,
		},
		{
			name:      "an unauthenticated caller",
			principal: func(*testing.T) app.Principal { return app.Principal{} },
			as:        acme,
			refused:   true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			principal := c.principal(t)

			// The rule on its own.
			err := principal.MayReadAs(c.as)
			switch {
			case c.refused && err == nil:
				t.Fatalf("%v was allowed to read as %q", principal.Kind(), c.as)
			case !c.refused && err != nil:
				t.Fatalf("%v was refused reading as %q: %v", principal.Kind(), c.as, err)
			}

			// And the door that asks it, which must refuse before any I/O: a
			// refusal that opened a snapshot would be observably different from
			// one that did not.
			f := newFixture(t)
			_, err = f.wagers.TransactionByExternalID(t.Context(), principal, c.as, "ext-1")
			if c.refused {
				assertClass(t, err, app.Unauthorized)
				f.assertUntouched(t)
				return
			}
			// Nothing was seeded, so the read is a miss rather than a refusal —
			// which is the point: the caller got as far as looking.
			assertClass(t, err, app.NotFound)
		})
	}
}

// The service reads what a provider submitted, by the provider's own identifier
// for it. Nothing else in this layer can answer that question: by id needs a
// transaction id, which is exactly what a caller holding an external id has
// not got.
func TestTheServiceReadsAProvidersOperationByItsExternalID(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	submitted := f.submit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
	}))

	read, err := f.wagers.TransactionByExternalID(t.Context(), servicePrincipal(t), acme, "ext-1")
	if err != nil {
		t.Fatalf("the service reading a provider's operation: %v", err)
	}
	if read.TransactionID != submitted.TransactionID {
		t.Errorf("read %s, want %s", read.TransactionID, submitted.TransactionID)
	}
}

// A provider is still scoped after the read as well as before it. The two
// checks answer different questions and the second one is the one that must
// stay indistinguishable from a miss.
func TestAProviderNamingItselfStillSeesOnlyItsOwnOperations(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
	}))

	_, err := f.wagers.TransactionByExternalID(t.Context(), providerPrincipal(t, rival), rival, "ext-1")

	assertClass(t, err, app.NotFound)
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
