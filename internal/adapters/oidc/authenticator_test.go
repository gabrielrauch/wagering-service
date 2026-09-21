package oidc

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jws"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// inventedAlgorithm is a name no registry holds, used to prove that an
// attacker's own bytes are never echoed into a refusal.
const inventedAlgorithm = "MARTIAN-256-<script>"

// assertRefused holds every refusal to the two things the HTTP adapter and an
// operator each depend on: the class, which is what becomes a 401, and the
// sentinel, which is what never becomes anything a caller sees.
func assertRefused(t *testing.T, err error, sentinel error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a refusal, got none")
	}
	if class := app.ClassOf(err); class != app.Unauthorized {
		t.Errorf("class = %q, want %q (err: %v)", class, app.Unauthorized, err)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it to match %v", err, sentinel)
	}
}

// assertSaysNothingAbout guards the one thing a refusal must never render.
func assertSaysNothingAbout(t *testing.T, err error, secrets ...string) {
	t.Helper()
	rendered := err.Error()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(rendered, secret) {
			t.Errorf("the refusal renders %q: %s", secret, rendered)
		}
	}
}

func TestAuthenticateTranslatesEachIdentity(t *testing.T) {
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
		subject  string
	}{
		{
			name:     "provider-a",
			claims:   iss.providerClaims(now, "service-account-provider-a", "provider-a", "provider-a"),
			kind:     app.ProviderPrincipal,
			provider: "provider-a",
			subject:  "service-account-provider-a",
		},
		{
			name:     "provider-b",
			claims:   iss.providerClaims(now, "service-account-provider-b", "provider-b", "provider-b"),
			kind:     app.ProviderPrincipal,
			provider: "provider-b",
			subject:  "service-account-provider-b",
		},
		{
			name:    "wallet-service",
			claims:  iss.serviceClaims(now, "service-account-wallet-service"),
			kind:    app.ServicePrincipal,
			subject: "service-account-wallet-service",
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
				t.Errorf("kind = %q, want %q", principal.Kind(), c.kind)
			}
			if principal.Subject() != c.subject {
				t.Errorf("subject = %q, want %q", principal.Subject(), c.subject)
			}
			provider, isProvider := principal.Provider()
			switch {
			case c.provider == "" && isProvider:
				t.Errorf("provider = %q, want none", provider)
			case c.provider != "" && string(provider) != c.provider:
				t.Errorf("provider = %q, want %q", provider, c.provider)
			}
		})
	}
}

// TestAuthenticateAcceptsAnECDSAToken proves the allow-list is a family rather
// than one algorithm: the same verifier takes an ES256 token from a P-256 key.
func TestAuthenticateAcceptsAnECDSAToken(t *testing.T) {
	t.Parallel()

	key := ecdsaKey(t, "ec-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss).start(t)

	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	principal, err := h.Authenticate(t.Context(), iss.mint(t, key, claims))
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if principal.Kind() != app.ServicePrincipal {
		t.Errorf("kind = %q, want %q", principal.Kind(), app.ServicePrincipal)
	}
}

func TestAuthenticateRefusesTheToken(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	// An impostor published nowhere, signing under the published key's
	// identifier. That is what a forged signature looks like from here.
	impostor := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss).start(t)
	now := h.clock.Now()

	good := func() map[string]any {
		return iss.serviceClaims(now, "service-account-wallet-service")
	}

	cases := []struct {
		name  string
		token func(t *testing.T) string
		want  string
	}{
		{
			name:  "no credential",
			token: func(*testing.T) string { return "" },
			want:  "no credential was presented",
		},
		{
			name:  "blank credential",
			token: func(*testing.T) string { return "   " },
			want:  "no credential was presented",
		},
		{
			name:  "not a token at all",
			token: func(*testing.T) string { return "not-a-token" },
			want:  malformedToken,
		},
		{
			name:  "the right shape and nothing else",
			token: func(*testing.T) string { return "aaaa.bbbb.cccc" },
			want:  malformedToken,
		},
		{
			name: "longer than a credential may be",
			token: func(*testing.T) string {
				return strings.Repeat("a", maxCredentialBytes+1)
			},
			want: malformedToken,
		},
		{
			name: "signed by a key the issuer does not publish",
			token: func(t *testing.T) string {
				t.Helper()
				return iss.mint(t, impostor, good())
			},
			want: unverifiableSignature,
		},
		{
			name: "tampered payload",
			token: func(t *testing.T) string {
				t.Helper()
				parts := strings.Split(iss.mint(t, key, good()), ".")
				forged := good()
				forged["sub"] = "somebody-else"
				payload, err := json.Marshal(forged)
				if err != nil {
					t.Fatalf("encode the forged claims: %v", err)
				}
				parts[1] = base64.RawURLEncoding.EncodeToString(payload)
				return strings.Join(parts, ".")
			},
			want: unverifiableSignature,
		},
		{
			name: "expired",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				claims["exp"] = now.Add(-time.Hour).Unix()
				return iss.mint(t, key, claims)
			},
			want: "the token has expired",
		},
		{
			name: "no expiry at all",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				delete(claims, "exp")
				return iss.mint(t, key, claims)
			},
			want: "the token states no expiry",
		},
		{
			name: "not valid yet",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				claims["nbf"] = now.Add(time.Hour).Unix()
				return iss.mint(t, key, claims)
			},
			want: "the token is not valid yet",
		},
		{
			name: "issued in the future",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				claims["iat"] = now.Add(time.Hour).Unix()
				return iss.mint(t, key, claims)
			},
			want: "the token was issued in the future",
		},
		{
			name: "issued by somebody else",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				claims["iss"] = "https://impostor.example/realms/wagering"
				return iss.mint(t, key, claims)
			},
			want: "the token was issued by another issuer",
		},
		{
			name: "the issuer with a trailing slash is a different issuer",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				claims["iss"] = iss.url() + "/"
				return iss.mint(t, key, claims)
			},
			want: "the token was issued by another issuer",
		},
		{
			name: "addressed to another service",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				claims["aud"] = []string{"account", "some-other-api"}
				return iss.mint(t, key, claims)
			},
			want: "the token is not addressed to this service",
		},
		{
			name: "no audience at all",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				delete(claims, "aud")
				return iss.mint(t, key, claims)
			},
			want: "the token is not addressed to this service",
		},
		{
			name: "no subject",
			token: func(t *testing.T) string {
				t.Helper()
				claims := good()
				claims["sub"] = "  "
				return iss.mint(t, key, claims)
			},
			want: "the token names no subject",
		},
		{
			name: "claims that are not an object",
			token: func(t *testing.T) string {
				t.Helper()
				signed, err := jws.Sign([]byte(`"not an object"`),
					jws.WithKey(key.alg, key.private))
				if err != nil {
					t.Fatalf("sign: %v", err)
				}
				return string(signed)
			},
			want: "the token's claims are not readable",
		},
		{
			// jwx refuses an algorithm name it does not know while parsing the
			// header, so the name never reaches a message. That matters: the
			// only place an attacker's own bytes could otherwise be echoed back
			// is the algorithm this package names in its refusal.
			name: "a header naming an algorithm nobody has heard of",
			token: func(t *testing.T) string {
				t.Helper()
				payload, err := json.Marshal(good())
				if err != nil {
					t.Fatalf("encode the claims: %v", err)
				}
				header := base64.RawURLEncoding.EncodeToString(
					[]byte(`{"alg":"` + inventedAlgorithm + `","kid":"rotation-1"}`))
				return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".c2ln"
			},
			want: malformedToken,
		},
		{
			name: "a header naming no algorithm",
			token: func(t *testing.T) string {
				t.Helper()
				payload, err := json.Marshal(good())
				if err != nil {
					t.Fatalf("encode the claims: %v", err)
				}
				header := base64.RawURLEncoding.EncodeToString([]byte(`{"kid":"rotation-1"}`))
				return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".c2ln"
			},
			want: malformedToken,
		},
		{
			name: "no signing key named",
			token: func(t *testing.T) string {
				t.Helper()
				anonymous := rsaKey(t, "")
				if err := anonymous.private.Remove(jws.KeyIDKey); err != nil {
					t.Fatalf("remove the key id: %v", err)
				}
				return iss.mint(t, anonymous, good())
			},
			want: "the token names no signing key",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			token := c.token(t)
			_, err := h.Authenticate(t.Context(), token)
			assertRefused(t, err, ErrTokenRejected)
			if got := err.Error(); got != "oidc: "+c.want {
				t.Errorf("message = %q, want %q", got, "oidc: "+c.want)
			}
			assertSaysNothingAbout(t, err, token, "service-account-wallet-service",
				inventedAlgorithm)
		})
	}
}

// TestAuthenticateRefusesAnAlgorithmBeforeSelectingAKey is the confusion
// attack, and the assertion that matters is the request count.
//
// Each of these tokens names a key identifier the cache does not hold, and the
// cache is deliberately left empty so that nothing else could be what stops a
// fetch — no primed key to hit, and no rate limit yet in force. A verifier that
// selected a key before judging the algorithm would go and ask the issuer for
// it, so a single JWKS request here is the failure, not merely the refusal
// being absent.
func TestAuthenticateRefusesAnAlgorithmBeforeSelectingAKey(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("encode the claims: %v", err)
	}
	published, err := json.Marshal(key.public)
	if err != nil {
		t.Fatalf("encode the published key: %v", err)
	}

	headers := func(t *testing.T) jws.Headers {
		t.Helper()
		h := jws.NewHeaders()
		if err := h.Set(jws.KeyIDKey, "never-published"); err != nil {
			t.Fatalf("set the key id: %v", err)
		}
		return h
	}

	cases := []struct {
		name  string
		token func(t *testing.T) string
		want  string
	}{
		{
			name: "alg none",
			token: func(t *testing.T) string {
				t.Helper()
				header := base64.RawURLEncoding.EncodeToString(
					[]byte(`{"alg":"none","kid":"never-published"}`))
				return header + "." + base64.RawURLEncoding.EncodeToString(payload) + "."
			},
			want: `the token's "none" algorithm is not accepted`,
		},
		{
			name: "HMAC over the issuer's own public key",
			token: func(t *testing.T) string {
				t.Helper()
				signed, err := jws.Sign(payload,
					jws.WithKey(jwa.HS256(), published,
						jws.WithProtectedHeaders(headers(t))))
				if err != nil {
					t.Fatalf("sign with HMAC: %v", err)
				}
				return string(signed)
			},
			want: `the token's "HS256" algorithm is not accepted`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := h.Authenticate(t.Context(), c.token(t))
			assertRefused(t, err, ErrTokenRejected)
			if got := err.Error(); got != "oidc: "+c.want {
				t.Errorf("message = %q, want %q", got, "oidc: "+c.want)
			}
			if got := iss.certRequests.Load(); got != 0 {
				t.Errorf("the issuer was asked for keys %d time(s); the algorithm should have "+
					"been refused before any key was selected", got)
			}
		})
	}
}

// TestAuthenticateRefusesAKeyBoundToAnotherAlgorithm covers the level below the
// allow-list: the published key says which algorithm it is for, and a header
// asking for a different one does not change that.
func TestAuthenticateRefusesAKeyBoundToAnotherAlgorithm(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss).start(t)

	// Sign with PS256, which is allowed and which this RSA key can perform,
	// while the published key declares RS256.
	signer := *key
	signer.alg = jwa.PS256()
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")

	_, err := h.Authenticate(t.Context(), iss.mint(t, &signer, claims))
	assertRefused(t, err, ErrTokenRejected)
	if got := err.Error(); got != "oidc: "+unverifiableSignature {
		t.Errorf("message = %q, want %q", got, "oidc: "+unverifiableSignature)
	}
}

// TestAuthenticateRefusesTheJSONSerialisation keeps the choice of which
// signature to believe from ever arising.
func TestAuthenticateRefusesTheJSONSerialisation(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	other := rsaKey(t, "rotation-2")
	iss := newIssuer(t, key, other)
	h := newHarness(t, iss).start(t)

	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("encode the claims: %v", err)
	}
	serialised, err := jws.Sign(payload, jws.WithJSON(),
		jws.WithKey(key.alg, key.private), jws.WithKey(other.alg, other.private))
	if err != nil {
		t.Fatalf("sign in JSON serialisation: %v", err)
	}

	_, err = h.Authenticate(t.Context(), string(serialised))
	assertRefused(t, err, ErrTokenRejected)
}

// TestAuthenticateToleratesTheConfiguredSkew proves the skew is a bound rather
// than a suggestion: a token a shade past its expiry is taken, one past the
// bound is not.
func TestAuthenticateToleratesTheConfiguredSkew(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss, func(c *Config) { c.ClockSkew = 30 * time.Second }).start(t)

	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	claims["exp"] = h.clock.Now().Add(time.Minute).Unix()
	token := iss.mint(t, key, claims)

	h.clock.advance(time.Minute + 20*time.Second)
	if _, err := h.Authenticate(t.Context(), token); err != nil {
		t.Fatalf("within the skew: %v", err)
	}

	h.clock.advance(20 * time.Second)
	_, err := h.Authenticate(t.Context(), token)
	assertRefused(t, err, ErrTokenRejected)
}

func TestBearerToken(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		header  string
		want    string
		refused bool
	}{
		{name: "the usual spelling", header: "Bearer abc.def.ghi", want: "abc.def.ghi"},
		{name: "the scheme is case-insensitive", header: "bearer abc", want: "abc"},
		{name: "surrounded by whitespace", header: "  Bearer   abc  ", want: "abc"},
		{name: "no header at all", header: "", refused: true},
		{name: "the scheme on its own", header: "Bearer", refused: true},
		{name: "the scheme and nothing after it", header: "Bearer   ", refused: true},
		{name: "another scheme", header: "Basic dXNlcjpwYXNz", refused: true},
		{name: "the credential without a scheme", header: "abc.def.ghi", refused: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, err := BearerToken(c.header)
			if c.refused {
				assertRefused(t, err, ErrTokenRejected)
				assertSaysNothingAbout(t, err, c.header)
				return
			}
			if err != nil {
				t.Fatalf("BearerToken(%q): %v", c.header, err)
			}
			if got != c.want {
				t.Errorf("BearerToken(%q) = %q, want %q", c.header, got, c.want)
			}
		})
	}
}
