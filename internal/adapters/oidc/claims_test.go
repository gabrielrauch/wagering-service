package oidc

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// TestPrincipalRefusesWhatItCannotPlace covers the three cases the task names
// and the one that pins the realm contract: a role assigned as a client role
// rather than a realm role is not a role this service reads.
func TestPrincipalRefusesWhatItCannotPlace(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss).start(t)
	now := h.clock.Now()

	cases := []struct {
		name   string
		claims map[string]any
		want   string
	}{
		{
			name: "both roles",
			claims: func() map[string]any {
				c := iss.claimsFor(now, "service-account-provider-a", "provider-a",
					providerRoleName, internalRoleName)
				c[providerIDClaim] = "provider-a"
				return c
			}(),
			want: "the token carries both the provider and the service role",
		},
		{
			name:   "neither role",
			claims: iss.claimsFor(now, "service-account-nobody", "nobody"),
			want:   "the token carries neither the provider nor the service role",
		},
		{
			name: "no realm_access at all",
			claims: func() map[string]any {
				c := iss.claimsFor(now, "service-account-nobody", "nobody")
				delete(c, "realm_access")
				return c
			}(),
			want: "the token carries neither the provider nor the service role",
		},
		{
			name: "the role granted on a client rather than on the realm",
			claims: func() map[string]any {
				c := iss.claimsFor(now, "service-account-provider-a", "provider-a")
				c["resource_access"] = map[string]any{
					"provider-a": map[string]any{"roles": []string{providerRoleName}},
				}
				c[providerIDClaim] = "provider-a"
				return c
			}(),
			want: "the token carries neither the provider nor the service role",
		},
		{
			name:   "the provider role with no providerId",
			claims: iss.claimsFor(now, "service-account-provider-a", "provider-a", providerRoleName),
			want:   "the provider role carries no usable " + providerIDClaim + " claim",
		},
		{
			name: "the provider role with a blank providerId",
			claims: func() map[string]any {
				c := iss.claimsFor(now, "service-account-provider-a", "provider-a",
					providerRoleName)
				c[providerIDClaim] = "   "
				return c
			}(),
			want: "the provider role carries no usable " + providerIDClaim + " claim",
		},
		{
			name: "the provider role with a providerId the domain refuses",
			claims: func() map[string]any {
				c := iss.claimsFor(now, "service-account-provider-a", "provider-a",
					providerRoleName)
				c[providerIDClaim] = "provider\u0000a"
				return c
			}(),
			want: "the provider role carries no usable " + providerIDClaim + " claim",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			token := iss.mint(t, key, c.claims)
			principal, err := h.Authenticate(t.Context(), token)
			assertRefused(t, err, ErrPrincipalUnresolved)
			if got := err.Error(); got != "oidc: "+c.want {
				t.Errorf("message = %q, want %q", got, "oidc: "+c.want)
			}
			if principal.Kind() != "" {
				t.Errorf("a refusal produced a %q principal", principal.Kind())
			}
			assertSaysNothingAbout(t, err, token)
		})
	}
}

// TestPrincipalIgnoresWhatDoesNotDecide states the other half of the contract:
// the role decides, so claims that do not are neither required nor consulted.
func TestPrincipalIgnoresWhatDoesNotDecide(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss).start(t)
	now := h.clock.Now()

	cases := []struct {
		name     string
		claims   map[string]any
		kind     app.PrincipalKind
		provider string
	}{
		{
			name: "the service role beside a providerId nobody asked for",
			claims: func() map[string]any {
				c := iss.serviceClaims(now, "service-account-wallet-service")
				c[providerIDClaim] = "provider-a"
				return c
			}(),
			kind: app.ServicePrincipal,
		},
		{
			name: "azp naming a client the providerId does not agree with",
			claims: func() map[string]any {
				c := iss.providerClaims(now, "service-account-provider-a", "some-other-client",
					"provider-a")
				return c
			}(),
			kind:     app.ProviderPrincipal,
			provider: "provider-a",
		},
		{
			name: "no azp at all",
			claims: func() map[string]any {
				c := iss.providerClaims(now, "service-account-provider-b", "provider-b",
					"provider-b")
				delete(c, "azp")
				return c
			}(),
			kind:     app.ProviderPrincipal,
			provider: "provider-b",
		},
		{
			name: "an aud carrying several audiences, one of them ours",
			claims: func() map[string]any {
				c := iss.serviceClaims(now, "service-account-wallet-service")
				c["aud"] = []string{"account", testAudience}
				return c
			}(),
			kind: app.ServicePrincipal,
		},
		{
			name: "roles this service has never heard of, beside one it has",
			claims: func() map[string]any {
				return iss.claimsFor(now, "service-account-wallet-service", "wallet-service",
					"uma_authorization", internalRoleName, "default-roles-wagering")
			}(),
			kind: app.ServicePrincipal,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			principal, err := h.Authenticate(t.Context(), iss.mint(t, key, c.claims))
			if err != nil {
				t.Fatalf("Authenticate: %v", err)
			}
			if principal.Kind() != c.kind {
				t.Fatalf("kind = %q, want %q", principal.Kind(), c.kind)
			}
			provider, isProvider := principal.Provider()
			if c.provider != "" && (!isProvider || string(provider) != c.provider) {
				t.Errorf("provider = %q (%v), want %q", provider, isProvider, c.provider)
			}
		})
	}
}

// TestClaimsRefuseAClaimSetItCannotRead covers the claims whose SHAPE is wrong
// rather than whose value is, which is a realm misconfiguration wearing a
// caller's clothes — and is refused as a token problem, before anything tries
// to read an identity out of it.
func TestClaimsRefuseAClaimSetItCannotRead(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss).start(t)
	now := h.clock.Now()

	cases := []struct {
		name  string
		spoil func(map[string]any)
	}{
		{
			// RFC 7519 permits a fractional NumericDate. Nothing that issues a
			// token to this service emits one, and accepting it would mean
			// choosing which way to round an expiry.
			name:  "an expiry with a fraction in it",
			spoil: func(c map[string]any) { c["exp"] = 1.5 },
		},
		{
			name:  "a providerId the mapper was configured to emit as a number",
			spoil: func(c map[string]any) { c[providerIDClaim] = 7 },
		},
		{
			name:  "realm_access that is not an object",
			spoil: func(c map[string]any) { c["realm_access"] = "provider" },
		},
		{
			name: "roles that are not strings",
			spoil: func(c map[string]any) {
				c["realm_access"] = map[string]any{"roles": []int{1, 2}}
			},
		},
		{
			name:  "a subject that is not a string",
			spoil: func(c map[string]any) { c["sub"] = 7 },
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			claims := iss.serviceClaims(now, "service-account-wallet-service")
			c.spoil(claims)

			_, err := h.Authenticate(t.Context(), iss.mint(t, key, claims))
			assertRefused(t, err, ErrTokenRejected)
			if got := err.Error(); got != "oidc: the token's claims are not readable" {
				t.Errorf("message = %q", got)
			}
		})
	}
}

// TestAudienceReadsBothSpellings: RFC 7519 allows "aud" to be a string or an
// array, and Keycloak emits the first when there is only one audience.
func TestAudienceReadsBothSpellings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "one string", raw: `"wagering-api"`, want: []string{"wagering-api"}},
		{name: "an array", raw: `["account","wagering-api"]`, want: []string{"account", "wagering-api"}},
		{name: "an empty array", raw: `[]`, want: []string{}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var got audience
			if err := got.UnmarshalJSON([]byte(c.raw)); err != nil {
				t.Fatalf("UnmarshalJSON(%s): %v", c.raw, err)
			}
			if len(got) != len(c.want) {
				t.Fatalf("aud = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("aud[%d] = %q, want %q", i, got[i], c.want[i])
				}
			}
		})
	}

	t.Run("anything else is refused", func(t *testing.T) {
		t.Parallel()
		var got audience
		if err := got.UnmarshalJSON([]byte(`{"aud":"x"}`)); err == nil {
			t.Error("expected an object to be refused")
		}
	})
}
