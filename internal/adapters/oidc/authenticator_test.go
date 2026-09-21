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

// assertSaysNothingAbout guards the thing a refusal must never carry, in the
// place it would actually be carried.
//
// It walks the whole chain rather than reading err.Error(), because the
// rendered message is "oidc: " plus a constant by construction and so is the
// one channel that cannot leak by accident. The causes are the channel that
// can: they hold library errors nobody here wrote, and internal/app warns in
// as many words that an adapter which renders a chain publishes whatever is in
// it. A guard that only read the message would be watching the locked door.
func assertSaysNothingAbout(t *testing.T, err error, secrets ...string) {
	t.Helper()
	for _, rendered := range chainOf(err) {
		for _, secret := range secrets {
			if secret != "" && strings.Contains(rendered, secret) {
				t.Errorf("the refusal carries %q, in: %s", secret, rendered)
			}
		}
	}
}

// chainOf renders every error reachable from err, itself included.
func chainOf(err error) []string {
	if err == nil {
		return nil
	}
	rendered := []string{err.Error()}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			rendered = append(rendered, chainOf(cause)...)
		}
		return rendered
	}
	return append(rendered, chainOf(errors.Unwrap(err))...)
}

// TestChainOfReachesACauseNobodyRenders proves the guard above looks where it
// says it does: a secret buried in a refusal's causes is found, although
// nothing renders it.
func TestChainOfReachesACauseNobodyRenders(t *testing.T) {
	t.Parallel()

	const credential = "eyJhbGciOiJSUzI1NiJ9.buried.signature"
	hidden := refuse(ErrTokenRejected, malformedToken, errors.New("parsing "+credential))

	if strings.Contains(hidden.Error(), credential) {
		t.Fatal("the credential is rendered, so this proves nothing about the chain")
	}
	found := false
	for _, rendered := range chainOf(hidden) {
		if strings.Contains(rendered, credential) {
			found = true
		}
	}
	if !found {
		t.Error("chainOf did not reach the cause holding the credential")
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
			// The algorithm is deliberately not in this list: jwx puts an
			// unrecognised one in its own parse error, which rides in the
			// chain and is never rendered. That the MESSAGE never names it is
			// asserted above, by comparing it to a constant.
			assertSaysNothingAbout(t, err, token, "service-account-wallet-service")
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

// TestAuthenticateRefusesTheJSONSerialisation covers two checks that would each
// look tested by the other.
//
// A plain JSON JWS has no dots, so it dies at the shape filter and never
// reaches the parser — which means it says nothing at all about the
// single-signature guard. Dressing it up with a member holding two dots gets it
// past the filter, and then the guard is the only thing between a token
// carrying somebody else's signature and a principal. The two cases are here so
// that deleting either check fails something.
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

	// padded dresses a JSON JWS up to exactly two dots, which is all the shape
	// filter looks at. Unknown members are ignored by the parser, so the
	// document still parses as the JWS it is.
	padded := func(t *testing.T, serialised []byte) string {
		t.Helper()
		var document map[string]json.RawMessage
		if err := json.Unmarshal(serialised, &document); err != nil {
			t.Fatalf("read back the JSON serialisation: %v", err)
		}
		document["padding"] = json.RawMessage(`".."`)
		dressed, err := json.Marshal(document)
		if err != nil {
			t.Fatalf("re-encode the JSON serialisation: %v", err)
		}
		if got := strings.Count(string(dressed), "."); got != 2 {
			t.Fatalf("the dressed document has %d dots, want 2 — it would be refused by "+
				"the shape filter and prove nothing about the guard below it", got)
		}
		return string(dressed)
	}

	cases := []struct {
		name  string
		token func(t *testing.T) string
	}{
		{
			name: "one signature, in JSON",
			token: func(t *testing.T) string {
				t.Helper()
				serialised, err := jws.Sign(payload, jws.WithJSON(),
					jws.WithKey(key.alg, key.private))
				if err != nil {
					t.Fatalf("sign in JSON serialisation: %v", err)
				}
				return string(serialised)
			},
		},
		{
			name: "two signatures, in JSON",
			token: func(t *testing.T) string {
				t.Helper()
				serialised, err := jws.Sign(payload, jws.WithJSON(),
					jws.WithKey(key.alg, key.private), jws.WithKey(other.alg, other.private))
				if err != nil {
					t.Fatalf("sign in JSON serialisation: %v", err)
				}
				return string(serialised)
			},
		},
		{
			name: "two signatures, dressed up to pass for compact",
			token: func(t *testing.T) string {
				t.Helper()
				serialised, err := jws.Sign(payload, jws.WithJSON(),
					jws.WithKey(key.alg, key.private), jws.WithKey(other.alg, other.private))
				if err != nil {
					t.Fatalf("sign in JSON serialisation: %v", err)
				}
				return padded(t, serialised)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			principal, err := h.Authenticate(t.Context(), c.token(t))
			assertRefused(t, err, ErrTokenRejected)
			if principal.Kind() != "" {
				t.Errorf("a refusal produced a %q principal", principal.Kind())
			}
		})
	}
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

// TestAuthenticateIsLenientAboutSpellingAndStrictAboutMeaning pins the one
// thing this package deliberately does not provide: that a credential has a
// single spelling.
//
// The parser reconstructs the signing input canonically, so padding, an
// embedded newline and an extra member in a JSON document all name the same
// token and all verify. Nothing here depends on the bytes being unique — the
// package doc says so, and this is what makes that statement checkable rather
// than merely written down. Anything that later remembers credentials, a replay
// cache or a denylist, has to key on a claim instead.
//
// The second half is the half that matters: every spelling that changes what
// the token MEANS is refused.
func TestAuthenticateIsLenientAboutSpellingAndStrictAboutMeaning(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	// A second published key, so that the "kid changed" case below names one
	// the cache really holds: the header is part of the signing input, so
	// rewriting it breaks the signature even when the key it now names is
	// perfectly good. Without this the case would only prove that an unknown
	// key identifier is refused, which is a different test.
	alternate := rsaKey(t, "rotation-9")
	iss := newIssuer(t, key, alternate)
	h := newHarness(t, iss).start(t)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	token := iss.mint(t, key, claims)
	segments := strings.Split(token, ".")

	// repad rewrites a segment in padded base64url, which is the same bytes
	// spelled differently.
	repad := func(t *testing.T, segment string) string {
		t.Helper()
		raw, err := base64.RawURLEncoding.DecodeString(segment)
		if err != nil {
			t.Fatalf("decode a segment: %v", err)
		}
		return base64.URLEncoding.EncodeToString(raw)
	}
	joined := func(head, payload, signature string) string {
		return head + "." + payload + "." + signature
	}

	spellings := []struct {
		name  string
		token func(t *testing.T) string
	}{
		{
			name:  "padded header",
			token: func(t *testing.T) string { return joined(repad(t, segments[0]), segments[1], segments[2]) },
		},
		{
			name:  "padded payload",
			token: func(t *testing.T) string { return joined(segments[0], repad(t, segments[1]), segments[2]) },
		},
		{
			name:  "padded signature",
			token: func(t *testing.T) string { return joined(segments[0], segments[1], repad(t, segments[2])) },
		},
		{
			name: "a newline inside a segment",
			token: func(*testing.T) string {
				return joined(segments[0], segments[1],
					segments[2][:5]+"\n"+segments[2][5:])
			},
		},
		{
			name:  "surrounded by whitespace",
			token: func(*testing.T) string { return " \n" + token + "\t " },
		},
		{
			name: "one signature, in a JSON document dressed to pass for compact",
			token: func(t *testing.T) string {
				t.Helper()
				payload, err := json.Marshal(claims)
				if err != nil {
					t.Fatalf("encode the claims: %v", err)
				}
				signed, err := jws.Sign(payload, jws.WithJSON(),
					jws.WithKey(key.alg, key.private))
				if err != nil {
					t.Fatalf("sign in JSON serialisation: %v", err)
				}
				var document map[string]json.RawMessage
				if err := json.Unmarshal(signed, &document); err != nil {
					t.Fatalf("read back the JSON serialisation: %v", err)
				}
				document["padding"] = json.RawMessage(`".."`)
				dressed, err := json.Marshal(document)
				if err != nil {
					t.Fatalf("re-encode the JSON serialisation: %v", err)
				}
				return string(dressed)
			},
		},
	}

	for _, c := range spellings {
		t.Run("accepts "+c.name, func(t *testing.T) {
			t.Parallel()
			principal, err := h.Authenticate(t.Context(), c.token(t))
			if err != nil {
				t.Fatalf("Authenticate: %v", err)
			}
			if principal.Kind() != app.ServicePrincipal {
				t.Errorf("kind = %q, want %q", principal.Kind(), app.ServicePrincipal)
			}
		})
	}

	meanings := []struct {
		name  string
		token func(t *testing.T) string
	}{
		{
			name: "a header re-serialised with a field added",
			token: func(t *testing.T) string {
				t.Helper()
				raw, err := base64.RawURLEncoding.DecodeString(segments[0])
				if err != nil {
					t.Fatalf("decode the header: %v", err)
				}
				var header map[string]json.RawMessage
				if err := json.Unmarshal(raw, &header); err != nil {
					t.Fatalf("read the header: %v", err)
				}
				header["cty"] = json.RawMessage(`"JWT"`)
				rewritten, err := json.Marshal(header)
				if err != nil {
					t.Fatalf("re-encode the header: %v", err)
				}
				return joined(base64.RawURLEncoding.EncodeToString(rewritten),
					segments[1], segments[2])
			},
		},
		{
			name: "a key identifier changed to one that is also published",
			token: func(t *testing.T) string {
				t.Helper()
				rewritten := strings.ReplaceAll(string(mustDecode(t, segments[0])),
					`"rotation-1"`, `"rotation-9"`)
				return joined(base64.RawURLEncoding.EncodeToString([]byte(rewritten)),
					segments[1], segments[2])
			},
		},
		{
			name: "a claim added to the payload",
			token: func(t *testing.T) string {
				t.Helper()
				forged := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
				forged[providerIDClaim] = "provider-a"
				payload, err := json.Marshal(forged)
				if err != nil {
					t.Fatalf("encode the forged claims: %v", err)
				}
				return joined(segments[0], base64.RawURLEncoding.EncodeToString(payload),
					segments[2])
			},
		},
	}

	for _, c := range meanings {
		t.Run("refuses "+c.name, func(t *testing.T) {
			t.Parallel()
			_, err := h.Authenticate(t.Context(), c.token(t))
			assertRefused(t, err, ErrTokenRejected)
		})
	}
}

// mustDecode reads a base64url segment or fails the test.
func mustDecode(t *testing.T, segment string) []byte {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("decode a segment: %v", err)
	}
	return raw
}
