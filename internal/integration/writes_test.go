//go:build integration

// What a refused request leaves behind. Nothing, and this is where that is
// checked against the tables rather than against a status code.
package integration

import (
	"net/http"
	"slices"
	"testing"
)

// TestARefusedRequestWritesNothing.
//
// A status code says what the caller was told. It does not say what was
// recorded on the way to telling them, and "unauthorised calls produce no
// database writes" is a claim about the second. So this takes a digest of every
// row of every table a write could land in, fires every refusal this service
// has at it, and takes the digest again.
//
// The digest is over whole rows rather than over counts, because half of what
// this service does to a wallet is an UPDATE and a count would not move.
//
// The control at the end is what makes the rest of it mean anything: one
// authorised submission, and the snapshot has to notice. Without it a snapshot
// that read nothing at all would pass every assertion above.
func TestARefusedRequestWritesNothing(t *testing.T) {
	t.Parallel()
	s := newStack(t)

	// A populated database rather than an empty one. An empty one cannot tell
	// "nothing was written" from "nothing was read", and it is the UPDATEs to
	// existing rows that a weaker snapshot would miss.
	wallet := s.openWallet(t, "player-audited", "100.00")
	existing := operationOf(t, s.submit(t, providerA,
		bet(providerA, "ext-audited", "player-audited", "10.00"), "key-audited"))

	before := s.snapshot(t)
	if before["wallet"].rows == 0 || before["wager_transaction"].rows == 0 ||
		before["wallet_ledger_entry"].rows == 0 || before["outbox"].rows == 0 {
		t.Fatalf("the snapshot found an empty database and would notice nothing: %v", before)
	}

	forged := forgedToken(t, publishedSigningKeyID(t))
	expired := expiredToken(t)
	provider := tokenFor(t, providerA)
	other := tokenFor(t, providerB)

	// Every door, with every credential this service refuses, and with the
	// bodies that would have changed something had they been let through: a
	// wallet for a player who already holds one, a bet against a real wallet,
	// and a resubmission under a key that is already bound.
	openBody := encode(t, map[string]any{
		"playerId":       "player-audited",
		"initialBalance": amount{Amount: "50.00", Currency: currency},
	})
	betBody := encode(t, bet(providerA, "ext-unauthorised", "player-audited", "25.00"))
	replayBody := encode(t, bet(providerA, "ext-audited", "player-audited", "10.00"))

	for _, c := range []struct {
		name string
		call call
	}{
		{name: "open a wallet with no credential", call: call{
			method: http.MethodPost, path: "/wallets", body: openBody,
		}},
		{name: "open a wallet with a forged token", call: call{
			method: http.MethodPost, path: "/wallets", body: openBody, token: forged,
		}},
		{name: "open a wallet with an expired token", call: call{
			method: http.MethodPost, path: "/wallets", body: openBody, token: expired,
		}},
		{name: "open a wallet as a provider", call: call{
			method: http.MethodPost, path: "/wallets", body: openBody, token: provider,
		}},
		{name: "submit with no credential", call: call{
			method: http.MethodPost, path: "/wagering/transactions",
			body: betBody, idempotencyKey: "key-unauthorised",
		}},
		{name: "submit with a malformed credential", call: call{
			method: http.MethodPost, path: "/wagering/transactions",
			body: betBody, idempotencyKey: "key-unauthorised",
			rawAuthorization: "Bearer not-a-jwt",
		}},
		{name: "submit with a forged token", call: call{
			method: http.MethodPost, path: "/wagering/transactions",
			body: betBody, idempotencyKey: "key-unauthorised", token: forged,
		}},
		{name: "submit with an expired token", call: call{
			method: http.MethodPost, path: "/wagering/transactions",
			body: betBody, idempotencyKey: "key-unauthorised", token: expired,
		}},
		{name: "submit as another provider", call: call{
			method: http.MethodPost, path: "/wagering/transactions",
			body: betBody, idempotencyKey: "key-unauthorised", token: other,
		}},
		{name: "replay another provider's key as the service", call: call{
			method: http.MethodPost, path: "/wagering/transactions",
			body: replayBody, idempotencyKey: "key-audited",
			token: tokenFor(t, walletService),
		}},
		{name: "reconcile with no credential", call: call{
			method: http.MethodPost, path: "/wallets/" + wallet.WalletID + "/reconciliation",
		}},
		{name: "reconcile as a provider", call: call{
			method: http.MethodPost, path: "/wallets/" + wallet.WalletID + "/reconciliation",
			token: provider,
		}},
		{name: "read another provider's operation", call: call{
			method: http.MethodGet, path: "/wagering/transactions/" + existing.TransactionID,
			token: other,
		}},
		{name: "read a wallet as a provider", call: call{
			method: http.MethodGet, path: "/wallets/" + wallet.WalletID, token: provider,
		}},
		{name: "page a ledger with a forged token", call: call{
			method: http.MethodGet, path: "/wallets/" + wallet.WalletID + "/ledger",
			token: forged,
		}},
	} {
		got := s.do(t, c.call)
		if got.status < 400 {
			t.Fatalf("%s was answered %s, which is not a refusal", c.name, got)
		}
	}

	unchanged(t, before, s.snapshot(t), "the refused requests")

	// The control. One authorised submission, and the snapshot has to move —
	// otherwise everything above passed because the snapshot sees nothing.
	if got := s.submit(t, providerA,
		bet(providerA, "ext-control", "player-audited", "5.00"), "key-control"); got.status != http.StatusOK {
		t.Fatalf("the control submission was answered %s", got)
	}
	moved := movedTables(before, s.snapshot(t))
	for _, wanted := range []string{"wallet", "wager_transaction", "wallet_ledger_entry", "outbox"} {
		if !slices.Contains(moved, wanted) {
			t.Fatalf("one accepted bet moved %v, and the snapshot did not notice wagering.%s",
				moved, wanted)
		}
	}
}
