//go:build integration

// Who may administer wallets, and who may read every provider's work.
package integration

import (
	"net/http"
	"testing"
)

// TestOnlyTheServiceMayAdministerWallets walks all four wallet routes with both
// identities.
//
// Wallets are the service's: a provider sees its own operations and the balance
// each one reported, which is everything it submitted and nothing it did not.
// 403 rather than 401, because a principal was established and it may not do
// this.
func TestOnlyTheServiceMayAdministerWallets(t *testing.T) {
	t.Parallel()
	s := newStack(t)

	opened := s.do(t, call{
		method: http.MethodPost,
		path:   "/wallets",
		body: encode(t, map[string]any{
			"playerId":       "player-administered",
			"initialBalance": amount{Amount: "100.00", Currency: currency},
		}),
		token: tokenFor(t, walletService),
	})
	if opened.status != http.StatusCreated {
		t.Fatalf("the service opening a wallet was answered %s, wanted 201", opened)
	}
	if location := opened.header.Get("Location"); location == "" {
		t.Fatal("a created wallet was not given a Location")
	}
	var wallet walletView
	decode(t, opened, &wallet)
	if wallet.Opening == nil || wallet.Balance.Amount != "100.00" {
		t.Fatalf("the wallet came back as %s", opened)
	}

	// Something in the ledger to page and to reconcile against.
	if got := s.submit(t, providerA,
		bet(providerA, "ext-administered", "player-administered", "10.00"),
		"key-administered"); got.status != http.StatusOK {
		t.Fatalf("seeding the ledger was answered %s", got)
	}

	for _, route := range []struct {
		name string
		call call
	}{
		{name: "open", call: call{
			method: http.MethodPost,
			path:   "/wallets",
			body: encode(t, map[string]any{
				"playerId":       "player-refused",
				"initialBalance": amount{Amount: "5.00", Currency: currency},
			}),
		}},
		{name: "read", call: call{
			method: http.MethodGet, path: "/wallets/" + wallet.WalletID,
		}},
		{name: "page", call: call{
			method: http.MethodGet, path: "/wallets/" + wallet.WalletID + "/ledger?limit=10",
		}},
		{name: "reconcile", call: call{
			method: http.MethodPost, path: "/wallets/" + wallet.WalletID + "/reconciliation",
		}},
	} {
		t.Run(route.name, func(t *testing.T) {
			// The service may.
			asService := route.call
			asService.token = tokenFor(t, walletService)
			if route.name == "open" {
				// A wallet is a player's balance in one currency, so a second
				// opening for the same player would be a conflict rather than a
				// refusal — this one opens a player of its own.
				asService.body = encode(t, map[string]any{
					"playerId":       "player-allowed-" + route.name,
					"initialBalance": amount{Amount: "5.00", Currency: currency},
				})
			}
			allowed := s.do(t, asService)
			if allowed.status != http.StatusOK && allowed.status != http.StatusCreated {
				t.Fatalf("the service was answered %s on %s", allowed, route.name)
			}

			// A provider may not, on the same route, with the same body.
			asProvider := route.call
			asProvider.token = tokenFor(t, providerA)
			body := refused(t, s.do(t, asProvider), http.StatusForbidden, "UNAUTHORIZED")
			if body.Message != "provider provider-a may not administer wallets" {
				t.Fatalf("%s refused provider-a with %q", route.name, body.Message)
			}
		})
	}
}

// TestTheServiceReadsEveryProvidersOperations.
//
// The internal identity reads all three wagering routes. The by-external-id one
// is the one that used to be unreachable for it — the application layer derived
// the provider from the principal, and the service names none — so it is
// asserted here explicitly rather than folded in with the others.
func TestTheServiceReadsEveryProvidersOperations(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	s.openWallet(t, "player-visible", "100.00")

	mine := operationOf(t, s.submit(t, providerA,
		bet(providerA, "ext-visible", "player-visible", "10.00"), "key-visible"))
	theirs := operationOf(t, s.submit(t, providerB,
		bet(providerB, "ext-visible", "player-visible", "20.00"), "key-visible-b"))

	service := tokenFor(t, walletService)
	for _, c := range []struct {
		name   string
		path   string
		wanted string
	}{
		{
			name:   "provider-a by transaction id",
			path:   "/wagering/transactions/" + mine.TransactionID,
			wanted: mine.TransactionID,
		},
		{
			name:   "provider-b by transaction id",
			path:   "/wagering/transactions/" + theirs.TransactionID,
			wanted: theirs.TransactionID,
		},
		{
			name:   "provider-a by external id",
			path:   "/providers/" + providerA + "/wagering/transactions/ext-visible",
			wanted: mine.TransactionID,
		},
		{
			name:   "provider-b by external id",
			path:   "/providers/" + providerB + "/wagering/transactions/ext-visible",
			wanted: theirs.TransactionID,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := operationOf(t, s.do(t, call{
				method: http.MethodGet, path: c.path, token: service,
			}))
			if got.TransactionID != c.wanted {
				t.Fatalf("the service read %s, wanted %s", got.TransactionID, c.wanted)
			}
		})
	}

	// And the service still may not submit. Reading takes nothing and moves
	// nothing; submitting as a provider it is not would make the check on the
	// submission path meaningless for the one identity that could bypass it.
	body := refused(t, s.submit(t, walletService,
		bet(providerA, "ext-by-the-service", "player-visible", "1.00"), "key-by-the-service"),
		http.StatusForbidden, "UNAUTHORIZED")
	if body.Message != "the service may not submit wager operations" {
		t.Fatalf("the service submitting was refused with %q", body.Message)
	}
}
