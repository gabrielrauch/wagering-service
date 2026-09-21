//go:build integration

// What the service does with the credential it was handed, or was not.
package integration

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
)

// TestTheRealmIssuesTheClaimsTheVerifierReads holds deploy/keycloak's export to
// the contract internal/adapters/oidc states.
//
// It is the first test in the suite on purpose. Every scenario below assumes
// that a client_credentials grant against this realm yields a token naming this
// issuer, this audience, this realm role and — for a provider — this
// providerId; when one of them goes red it is worth knowing straight away
// whether the realm changed or the service did.
func TestTheRealmIssuesTheClaimsTheVerifierReads(t *testing.T) {
	t.Parallel()
	requireIdentityProvider(t)

	for _, c := range []struct {
		client   string
		role     string
		provider string
	}{
		{client: providerA, role: "provider", provider: providerA},
		{client: providerB, role: "provider", provider: providerB},
		{client: walletService, role: "internal"},
		{client: expiringProvider, role: "provider", provider: expiringProvider},
	} {
		t.Run(c.client, func(t *testing.T) {
			t.Parallel()
			claims, err := claimsOf(tokenFor(t, c.client))
			if err != nil {
				t.Fatalf("read the claims of %s's token: %v", c.client, err)
			}

			// Byte for byte, and with no trailing slash: the verifier compares
			// it that way and normalising either side would accept a spelling
			// the realm never mints.
			if got := claims["iss"]; got != issuerURL() {
				t.Errorf("iss is %v, wanted %q", got, issuerURL())
			}
			if !audienceContains(claims["aud"], apiAudience) {
				t.Errorf("aud is %v, wanted it to name %q", claims["aud"], apiAudience)
			}
			if sub, _ := claims["sub"].(string); sub == "" {
				t.Error("the token names no subject")
			}
			if _, ok := claims["exp"].(float64); !ok {
				t.Errorf("exp is %v, wanted a number", claims["exp"])
			}

			// From realm_access and nowhere else. resource_access is never read
			// by the verifier, so a realm that granted the role there alone
			// would authenticate nobody.
			roles := realmRoles(t, claims)
			if !slices.Contains(roles, c.role) {
				t.Errorf("realm_access.roles is %v, wanted it to hold %q", roles, c.role)
			}
			if c.role == "provider" && slices.Contains(roles, "internal") {
				t.Errorf("realm_access.roles is %v: a token carrying both roles is refused", roles)
			}

			// A top-level string, which is the only place a provider's identity
			// is read from.
			switch provider, ok := claims["providerId"].(string); {
			case c.provider == "" && ok:
				t.Errorf("providerId is %q on a client that submits as no provider", provider)
			case c.provider != "" && provider != c.provider:
				t.Errorf("providerId is %v, wanted %q", claims["providerId"], c.provider)
			}
		})
	}
}

// TestAClientMayNotUseAnotherClientsSecret checks the realm keeps the two
// providers apart at the door as well as in the claims.
//
// Isolation on this service's side is worth nothing if one provider can obtain
// the other's token, and that half of it is the realm's to hold rather than the
// verifier's.
func TestAClientMayNotUseAnotherClientsSecret(t *testing.T) {
	t.Parallel()
	requireIdentityProvider(t)

	if status := refusedCredentials(t, providerA, secretFor(providerB)); status != http.StatusUnauthorized {
		t.Fatalf("provider-a presenting provider-b's secret was answered %d, wanted 401", status)
	}
	if status := refusedCredentials(t, providerA, secretFor(providerA)); status != http.StatusOK {
		t.Fatalf("provider-a presenting its own secret was answered %d, wanted 200", status)
	}
}

// TestTheHealthChecksAreReachableWithoutACredential is the other half of the
// rule below.
//
// A liveness probe that needed a token would restart a healthy process the
// moment the identity provider went away, and a readiness probe that needed one
// would take this service out of rotation for the same reason — which is
// exactly when its dependencies most need reporting on.
func TestTheHealthChecksAreReachableWithoutACredential(t *testing.T) {
	t.Parallel()
	s := newStack(t)

	live := s.do(t, call{method: http.MethodGet, path: "/health/live"})
	if live.status != http.StatusOK {
		t.Fatalf("/health/live answered %s with no credential, wanted 200", live)
	}
	ready := s.do(t, call{method: http.MethodGet, path: "/health/ready"})
	if ready.status != http.StatusOK {
		t.Fatalf("/health/ready answered %s with no credential, wanted 200", ready)
	}

	var report struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	decode(t, ready, &report)
	if report.Status != "ready" || report.Checks["postgres"] != "ok" {
		t.Fatalf("/health/ready reported %s, wanted ready with postgres ok", ready)
	}
}

// guarded names every route that is not a health check, with a body where one
// is needed, so that the credential scenarios below can be stated once over all
// of them rather than once per route.
//
// It is deliberately the whole list. A route added without authentication would
// be a hole this suite would not see if the list were a sample.
func guarded(t *testing.T) []call {
	t.Helper()
	const wallet = "0199aa00-0000-7000-8000-000000000001"
	const transaction = "0199aa00-0000-7000-8000-000000000002"
	return []call{
		{method: http.MethodPost, path: "/wallets", body: encode(t, map[string]any{
			"playerId":       "player-guarded",
			"initialBalance": amount{Amount: "10.00", Currency: currency},
		})},
		{method: http.MethodGet, path: "/wallets/" + wallet},
		{method: http.MethodGet, path: "/wallets/" + wallet + "/ledger"},
		{method: http.MethodPost, path: "/wallets/" + wallet + "/reconciliation"},
		{
			method:         http.MethodPost,
			path:           "/wagering/transactions",
			body:           encode(t, bet(providerA, "ext-guarded", "player-guarded", "1.00")),
			idempotencyKey: "key-guarded",
		},
		{method: http.MethodGet, path: "/wagering/transactions/" + transaction},
		{
			method: http.MethodGet,
			path:   "/providers/" + providerA + "/wagering/transactions/ext-guarded",
		},
	}
}

// TestEveryGuardedRouteRefusesARequestCarryingNoCredential.
func TestEveryGuardedRouteRefusesARequestCarryingNoCredential(t *testing.T) {
	t.Parallel()
	s := newStack(t)

	for _, c := range guarded(t) {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			unauthenticated(t, s.do(t, c))
		})
	}
}

// TestACredentialThisServiceCannotUseIsRefused covers the spellings that are
// not a token at all and the ones that are a token somebody else signed.
//
// Every case answers the same 401 with the same sentence, which is the point:
// which refusal happened is logged and never answered, because the one
// distinction a holder of a token cannot observe — a signature that did not
// check out against a key this process could not obtain — must not be handed to
// them.
func TestACredentialThisServiceCannotUseIsRefused(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	realKid := publishedSigningKeyID(t)

	for _, c := range []struct {
		name          string
		authorization string
	}{
		{name: "no scheme", authorization: "just-a-string"},
		{name: "the wrong scheme", authorization: "Basic cHJvdmlkZXItYTpzZWNyZXQ="},
		{name: "a bearer with nothing after it", authorization: "Bearer "},
		{name: "not a JWT at all", authorization: "Bearer not-a-jwt"},
		{name: "two segments", authorization: "Bearer eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJ4In0"},
		{name: "three segments of rubbish", authorization: "Bearer aaa.bbb.ccc"},
		{
			name:          "a key the realm never published",
			authorization: "Bearer " + forgedToken(t, "a-key-nobody-issued"),
		},
		{
			// The interesting one. The verifier finds a real public key under
			// this identifier and the signature does not check out, which is a
			// different refusal from finding no key at all — and both are 401.
			name:          "the realm's own key identifier over another key",
			authorization: "Bearer " + forgedToken(t, realKid),
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			unauthenticated(t, s.do(t, call{
				method:           http.MethodGet,
				path:             "/wagering/transactions/0199aa00-0000-7000-8000-000000000002",
				rawAuthorization: c.authorization,
			}))
		})
	}
}

// TestAnExpiredTokenIsRefused presents a credential the realm really issued,
// really signed, and that has really run out.
//
// provider-expiring's access tokens live one second — see the realm export —
// and the wait is computed from the token's own "exp" plus the skew this
// suite's authenticator was configured with, so it is the verifier's rule
// rather than a guess. Nothing else about the token is wrong: the issuer, the
// audience, the realm role and the providerId are all the ones a working
// provider's token carries, which is what makes the expiry the only thing this
// can be failing on.
func TestAnExpiredTokenIsRefused(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	wallet := s.openWallet(t, "player-expired", "100.00")

	token := expiredToken(t)
	claims, err := claimsOf(token)
	if err != nil {
		t.Fatalf("read the expired token's claims: %v", err)
	}
	if claims["iss"] != issuerURL() || !audienceContains(claims["aud"], apiAudience) {
		t.Fatalf("the expired token is not otherwise valid: %v", claims)
	}
	if !slices.Contains(realmRoles(t, claims), "provider") {
		t.Fatalf("the expired token carries no provider role: %v", claims)
	}

	before := s.snapshot(t)

	unauthenticated(t, s.do(t, call{
		method: http.MethodGet,
		path:   "/wallets/" + wallet.WalletID,
		token:  token,
	}))
	unauthenticated(t, s.do(t, call{
		method:         http.MethodPost,
		path:           "/wagering/transactions",
		body:           encode(t, bet(expiringProvider, "ext-expired", "player-expired", "1.00")),
		token:          token,
		idempotencyKey: "key-expired",
	}))

	unchanged(t, before, s.snapshot(t), "a submission under an expired token")

	// The control. A token from a client whose only difference is a five-minute
	// lifespan is accepted on the same route, so what was refused above was the
	// expiry and not the route, the realm or the suite's wiring.
	fresh := s.submit(t, providerA,
		bet(providerA, "ext-fresh", "player-expired", "1.00"), "key-fresh")
	if fresh.status != http.StatusOK {
		t.Fatalf("a live token was answered %s on the route an expired one was refused on", fresh)
	}
}

// audienceContains reports whether an "aud" claim names what this service is.
// The claim is one string or an array of them, and both arrive in practice.
func audienceContains(claim any, wanted string) bool {
	switch aud := claim.(type) {
	case string:
		return aud == wanted
	case []any:
		for _, one := range aud {
			if name, ok := one.(string); ok && name == wanted {
				return true
			}
		}
	}
	return false
}

// realmRoles reads realm_access.roles out of a decoded claim set.
func realmRoles(t *testing.T, claims map[string]any) []string {
	t.Helper()
	raw, err := json.Marshal(claims["realm_access"])
	if err != nil {
		t.Fatalf("re-encode realm_access: %v", err)
	}
	var access struct {
		Roles []string `json:"roles"`
	}
	if err := json.Unmarshal(raw, &access); err != nil {
		t.Fatalf("decode realm_access: %v", err)
	}
	return access.Roles
}
