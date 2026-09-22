package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwk"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// pastTheLimit moves the clock far enough that the next miss is allowed to
// refresh, which is how every case below separates "the limit held" from "the
// fetch failed".
const pastTheLimit = defaultMinRefreshInterval + time.Second

func TestKeySetRefreshesForAnUnknownKeyIdentifier(t *testing.T) {
	t.Parallel()

	current := rsaKey(t, "rotation-1")
	rotated := rsaKey(t, "rotation-2")
	iss := newIssuer(t, current)
	h := newHarness(t, iss).start(t)

	iss.publish(current, rotated)
	h.clock.advance(pastTheLimit)

	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	if _, err := h.Authenticate(t.Context(), iss.mint(t, rotated, claims)); err != nil {
		t.Fatalf("a token signed by the rotated key: %v", err)
	}
	if got := iss.certRequests.Load(); got != 2 {
		t.Errorf("the issuer was asked for keys %d time(s), want 2", got)
	}

	// The key that was already cached still verifies: a refresh replaces the
	// set, it does not narrow it to whatever was just missing.
	if _, err := h.Authenticate(t.Context(), iss.mint(t, current, claims)); err != nil {
		t.Fatalf("a token signed by the key that was already cached: %v", err)
	}
	if got := iss.certRequests.Load(); got != 2 {
		t.Errorf("the issuer was asked for keys %d time(s), want 2", got)
	}
}

// TestKeySetRateLimitsRefresh is the amplification guard. An attacker who can
// present invented key identifiers must not be able to turn each one into a
// request this service makes to its identity provider.
func TestKeySetRateLimitsRefresh(t *testing.T) {
	t.Parallel()

	current := rsaKey(t, "rotation-1")
	invented := rsaKey(t, "invented")
	iss := newIssuer(t, current)
	h := newHarness(t, iss).start(t)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")

	for range 20 {
		_, err := h.Authenticate(t.Context(), iss.mint(t, invented, claims))
		assertRefused(t, err, ErrKeyUnavailable)
	}
	if got := iss.certRequests.Load(); got != 1 {
		t.Errorf("the issuer was asked for keys %d time(s) during the limit, want 1", got)
	}

	// The limit is a rate, not a lockout: once it has passed, the next miss is
	// allowed its one fetch.
	h.clock.advance(pastTheLimit)
	_, err := h.Authenticate(t.Context(), iss.mint(t, invented, claims))
	assertRefused(t, err, ErrKeyUnavailable)
	if got := iss.certRequests.Load(); got != 2 {
		t.Errorf("the issuer was asked for keys %d time(s) after the limit passed, want 2", got)
	}
}

// TestKeySetKeepsWorkingKeysWhenARefreshFails keeps one outage from becoming
// two: an identity provider that is briefly unreachable must not cost this
// process the keys it can still verify with.
func TestKeySetKeepsWorkingKeysWhenARefreshFails(t *testing.T) {
	t.Parallel()

	current := rsaKey(t, "rotation-1")
	invented := rsaKey(t, "invented")
	iss := newIssuer(t, current)
	h := newHarness(t, iss).start(t)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")

	restore := iss.breakCerts()
	h.clock.advance(pastTheLimit)

	_, err := h.Authenticate(t.Context(), iss.mint(t, invented, claims))
	assertRefused(t, err, ErrKeyUnavailable)
	if got := iss.certRequests.Load(); got != 2 {
		t.Fatalf("the issuer was asked for keys %d time(s), want 2 — the refresh was not "+
			"attempted, so this proves nothing about what a failed one does", got)
	}

	if _, err := h.Authenticate(t.Context(), iss.mint(t, current, claims)); err != nil {
		t.Fatalf("a token signed by the cached key, after a failed refresh: %v", err)
	}
	restore()
}

// TestKeySetCollapsesConcurrentMisses asserts on the issuer's request count
// because that is the only place the property is visible.
//
// Nothing has been fetched when the callers start, so the rate limit is not
// what is holding them: the first through opens the fetch and the rest join it.
// Remove the single flight and every one of them opens its own.
func TestKeySetCollapsesConcurrentMisses(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	token := iss.mint(t, key, claims)

	const callers = 24
	release := iss.hold(t)
	started := make(chan struct{}, callers)
	errs := make(chan error, callers)

	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			started <- struct{}{}
			_, err := h.Authenticate(context.WithoutCancel(t.Context()), token)
			errs <- err
		})
	}
	for range callers {
		<-started
	}
	// Every caller has entered Authenticate; the first to reach the cache is
	// blocked in the handler, and the rest are queueing behind it one way or
	// another. Give them a moment to settle so that "one request" is a claim
	// about the flight rather than about scheduling.
	time.Sleep(100 * time.Millisecond)
	release()
	wg.Wait()

	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("a concurrent caller: %v", err)
		}
	}
	if got := iss.certRequests.Load(); got != 1 {
		t.Errorf("the issuer was asked for keys %d time(s), want 1", got)
	}
}

// TestAuthenticateReportsACallerGivingUpAsRetryable keeps a cancelled request
// out of the authentication failure count. A caller that hung up was not
// refused.
func TestAuthenticateReportsACallerGivingUpAsRetryable(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	h := newHarness(t, iss)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	token := iss.mint(t, key, claims)

	release := iss.hold(t)
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		_, err := h.Authenticate(ctx, token)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-done
	release()
	if err == nil {
		t.Fatal("expected the cancellation to be reported")
	}
	if class := app.ClassOf(err); class != app.Retryable {
		t.Errorf("class = %q, want %q (err: %v)", class, app.Retryable, err)
	}
	if errors.Is(err, ErrTokenRejected) || errors.Is(err, ErrKeyUnavailable) {
		t.Errorf("a cancelled request was recorded as an authentication failure: %v", err)
	}
}

// TestRefusalHidesWhetherTheKeyOrTheSignatureWasWrong is the property stated in
// the package doc, asserted directly: the two cases an attacker must not be
// able to tell apart render identically, and the two cases an operator must be
// able to tell apart carry different sentinels.
func TestRefusalHidesWhetherTheKeyOrTheSignatureWasWrong(t *testing.T) {
	t.Parallel()

	published := rsaKey(t, "rotation-1")
	impostor := rsaKey(t, "rotation-1")
	unknown := rsaKey(t, "never-published")
	iss := newIssuer(t, published)
	h := newHarness(t, iss).start(t)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")

	_, forged := h.Authenticate(t.Context(), iss.mint(t, impostor, claims))
	_, missing := h.Authenticate(t.Context(), iss.mint(t, unknown, claims))

	assertRefused(t, forged, ErrTokenRejected)
	assertRefused(t, missing, ErrKeyUnavailable)
	if forged.Error() != missing.Error() {
		t.Errorf("the two refusals read differently:\n  forged signature: %s\n  unknown key:      %s",
			forged.Error(), missing.Error())
	}
	if errors.Is(missing, ErrTokenRejected) || errors.Is(forged, ErrKeyUnavailable) {
		t.Error("the sentinels do not tell the two apart")
	}
}

func TestKeySetDropsKeysItMustNotVerifyWith(t *testing.T) {
	t.Parallel()

	signing := rsaKey(t, "signing")
	encryption := rsaKey(t, "encryption")
	if err := encryption.public.Set(jwk.KeyUsageKey, "enc"); err != nil {
		t.Fatalf("mark the key for encryption: %v", err)
	}
	shared, err := jwk.Import([]byte("a shared secret nobody should verify with"))
	if err != nil {
		t.Fatalf("import a symmetric key: %v", err)
	}
	if err := shared.Set(jwk.KeyIDKey, "symmetric"); err != nil {
		t.Fatalf("set the key id: %v", err)
	}
	encodedShared, err := json.Marshal(shared)
	if err != nil {
		t.Fatalf("encode the symmetric key: %v", err)
	}

	iss := newIssuer(t, signing, encryption)
	iss.publishRaw(encodedShared)
	h := newHarness(t, iss).start(t)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")

	if _, err := h.Authenticate(t.Context(), iss.mint(t, signing, claims)); err != nil {
		t.Fatalf("a token signed by the signing key: %v", err)
	}

	// ErrUnknownKey rather than merely ErrKeyUnavailable is the whole
	// assertion. It says the cache does not hold these keys at all — if the
	// filters were dropped the lookup would find one and fail later, at
	// verification, which is ErrTokenRejected and a different sentence about
	// what this service is willing to hold.
	for _, dropped := range []struct {
		name string
		kid  string
	}{
		{name: "the key published for encryption", kid: "encryption"},
		{name: "the symmetric key", kid: "symmetric"},
	} {
		t.Run(dropped.name, func(t *testing.T) {
			// Past the limit, so the miss gets its refresh and the answer is
			// about what the document yields rather than about the rate limit.
			h.clock.advance(pastTheLimit)

			_, err := h.Authenticate(t.Context(), iss.mint(t, rsaKey(t, dropped.kid), claims))
			assertRefused(t, err, ErrKeyUnavailable)
			if !errors.Is(err, ErrUnknownKey) {
				t.Errorf("err = %v, want the key to be absent from the cache entirely", err)
			}
		})
	}
}

// TestKeySetKeepsTheFirstEntryUnderADuplicateIdentifier: a document with two
// keys under one "kid" is already wrong, and the question is only which of them
// this service will verify with. The first stands, so an entry appended to the
// document cannot displace the key that is verifying today.
func TestKeySetKeepsTheFirstEntryUnderADuplicateIdentifier(t *testing.T) {
	t.Parallel()

	genuine := rsaKey(t, "rotation-1")
	impostor := rsaKey(t, "rotation-1")
	iss := newIssuer(t, genuine, impostor)
	h := newHarness(t, iss).start(t)
	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")

	if _, err := h.Authenticate(t.Context(), iss.mint(t, genuine, claims)); err != nil {
		t.Fatalf("a token signed by the first key published under the identifier: %v", err)
	}
	_, err := h.Authenticate(t.Context(), iss.mint(t, impostor, claims))
	assertRefused(t, err, ErrTokenRejected)
}

// TestKeySetFollowsNoRedirect is the attack the origin checks would otherwise
// only appear to stop: a jwks_uri that passes every test on its address and
// then answers 302 to somewhere else. An open redirect on the issuer's own
// origin is the ordinary way to get one.
//
// The assertion that matters is that the attacker's server was never asked.
func TestKeySetFollowsNoRedirect(t *testing.T) {
	t.Parallel()

	attackerKey := rsaKey(t, "rotation-1")
	var attackerRequests atomic.Int64
	attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attackerRequests.Add(1)
		encoded, err := json.Marshal(attackerKey.public)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[` + string(encoded) + `]}`))
	}))
	t.Cleanup(attacker.Close)

	// The issuer's own origin, answering the JWKS address with a redirect.
	var issuerURL string
	mux := http.NewServeMux()
	mux.HandleFunc(realmPath+discoveryPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer": issuerURL, "jwks_uri": issuerURL + "/protocol/openid-connect/certs",
		})
	})
	mux.HandleFunc(certsPath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, attacker.URL, http.StatusFound)
	})
	redirecting := httptest.NewServer(mux)
	t.Cleanup(redirecting.Close)
	issuerURL = redirecting.URL + realmPath

	for _, c := range []struct {
		name    string
		jwksURI string
	}{
		{name: "an address found by discovery", jwksURI: ""},
		{name: "an address that was configured", jwksURI: redirecting.URL + certsPath},
	} {
		t.Run(c.name, func(t *testing.T) {
			authenticator, err := NewAuthenticator(Config{
				Issuer:     issuerURL,
				Audience:   testAudience,
				JWKSURI:    c.jwksURI,
				HTTPClient: redirecting.Client(),
				Clock:      newFixedClock(),
			})
			if err != nil {
				t.Fatalf("NewAuthenticator: %v", err)
			}

			startErr := authenticator.OnStart(t.Context())
			if startErr == nil {
				t.Fatal("expected the redirect to be refused")
			}
			if !strings.Contains(startErr.Error(), "302") {
				t.Errorf("err = %v, want it to report the redirect it would not follow",
					startErr)
			}

			// And the key set it pointed at must not have been fetched — nor,
			// therefore, cached, nor able to verify anything.
			token := mintFor(t, attackerKey, issuerURL, "service-account-wallet-service")
			_, err = authenticator.Authenticate(t.Context(), token)
			assertRefused(t, err, ErrKeyUnavailable)
			if got := attackerRequests.Load(); got != 0 {
				t.Errorf("the redirect target was fetched %d time(s), want none", got)
			}
		})
	}
}

// TestAClockStepBackwardsCostsOneIntervalAndNoMore: the stamp is wall-clock —
// [systemClock] strips the monotonic reading — so a host corrected by NTP, or a
// container resumed from a snapshot, can leave it in the future. Left alone the
// subtraction in startRefresh would then decline every refresh for however far
// the clock stepped, and a rotation inside that window would be invisible for
// an hour. Clamping the stamp turns the step back into one ordinary interval.
func TestAClockStepBackwardsCostsOneIntervalAndNoMore(t *testing.T) {
	t.Parallel()

	current := rsaKey(t, "rotation-1")
	rotated := rsaKey(t, "rotation-2")
	iss := newIssuer(t, current)
	h := newHarness(t, iss).start(t)

	iss.publish(current, rotated)
	h.clock.advance(-time.Hour)

	// Immediately after the step the limit still holds, which is the rate limit
	// behaving normally rather than the step being ignored.
	rotatedToken := func() string {
		return iss.mint(t, rotated,
			iss.serviceClaims(h.clock.Now(), "service-account-wallet-service"))
	}
	_, err := h.Authenticate(t.Context(), rotatedToken())
	assertRefused(t, err, ErrKeyUnavailable)
	if !errors.Is(err, ErrRefreshDeclined) {
		t.Errorf("err = %v, want the rate limit to have declined the refresh", err)
	}
	if got := iss.certRequests.Load(); got != 1 {
		t.Errorf("the issuer was asked for keys %d time(s), want 1", got)
	}

	// One interval later — measured from the stepped-back clock, not from the
	// hour it went back — the refresh is allowed and the rotation lands.
	h.clock.advance(pastTheLimit)
	if _, err := h.Authenticate(t.Context(), rotatedToken()); err != nil {
		t.Fatalf("a token signed by the rotated key, one interval after the step: %v", err)
	}
	if got := iss.certRequests.Load(); got != 2 {
		t.Errorf("the issuer was asked for keys %d time(s), want 2", got)
	}
}

// TestKeySetIsNotBlockedByAFailedStart: a process that started while the
// identity provider was down must be able to pick the keys up as soon as it is
// back, rather than sitting out the refresh interval for a loop that startup is
// not.
func TestKeySetIsNotBlockedByAFailedStart(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	iss := newIssuer(t, key)
	restore := iss.breakCerts()
	h := newHarness(t, iss)

	if err := h.OnStart(t.Context()); err == nil {
		t.Fatal("expected the startup fetch to fail")
	}
	restore()

	claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
	if _, err := h.Authenticate(t.Context(), iss.mint(t, key, claims)); err != nil {
		t.Fatalf("the first token after the issuer recovered: %v", err)
	}
}

func TestOnStartFailsFast(t *testing.T) {
	t.Parallel()

	t.Run("the issuer is answering with errors", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t, rsaKey(t, "rotation-1"))
		iss.breakCerts()
		h := newHarness(t, iss)

		err := h.OnStart(t.Context())
		if err == nil {
			t.Fatal("expected a startup failure")
		}
		if !strings.Contains(err.Error(), "500") {
			t.Errorf("err = %v, want it to report what the issuer answered", err)
		}
	})

	t.Run("the issuer publishes nothing usable", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t)
		h := newHarness(t, iss)
		err := h.OnStart(t.Context())
		if err == nil {
			t.Fatal("expected a startup failure")
		}
		if !strings.Contains(err.Error(), "no usable signing key") {
			t.Errorf("err = %v, want it to say the document has no usable key", err)
		}
	})

	t.Run("the issuer is not answering at all", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t, rsaKey(t, "rotation-1"))
		h := newHarness(t, iss, func(c *Config) {
			c.JWKSURI = "http://127.0.0.1:1/realms/wagering/protocol/openid-connect/certs"
			c.HTTPClient = &http.Client{}
		})

		err := h.OnStart(t.Context())
		if err == nil {
			t.Fatal("expected a startup failure")
		}
		if !strings.Contains(err.Error(), "127.0.0.1:1") {
			t.Errorf("err = %v, want it to name the address it could not reach", err)
		}
	})

	t.Run("the caller gives up", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t, rsaKey(t, "rotation-1"))
		release := iss.hold(t)
		h := newHarness(t, iss)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- h.OnStart(ctx) }()
		time.Sleep(50 * time.Millisecond)
		cancel()

		err := <-done
		release()
		if err == nil {
			t.Fatal("expected the cancellation to be reported")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want the caller's cancellation", err)
		}
	})
}

func TestDiscovery(t *testing.T) {
	t.Parallel()

	discovering := func(c *Config) { c.JWKSURI = "" }

	t.Run("finds where the keys are published", func(t *testing.T) {
		t.Parallel()
		key := rsaKey(t, "rotation-1")
		iss := newIssuer(t, key)
		h := newHarness(t, iss, discovering).start(t)

		claims := iss.serviceClaims(h.clock.Now(), "service-account-wallet-service")
		if _, err := h.Authenticate(t.Context(), iss.mint(t, key, claims)); err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if got := iss.discoveryRequests.Load(); got != 1 {
			t.Errorf("the OpenID configuration was read %d time(s), want 1", got)
		}
	})

	t.Run("refuses a document that speaks for another issuer", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t, rsaKey(t, "rotation-1"))
		iss.setMetadata(map[string]any{
			"issuer":   "https://impostor.example/realms/wagering",
			"jwks_uri": iss.certsURL(),
		})
		h := newHarness(t, iss, discovering)

		err := h.OnStart(t.Context())
		if err == nil {
			t.Fatal("expected a startup failure")
		}
		if !strings.Contains(err.Error(), "not the configured") {
			t.Errorf("err = %v, want it to say the document names another issuer", err)
		}
	})

	t.Run("reports an issuer that is not answering", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t, rsaKey(t, "rotation-1"))
		iss.setMetadata(nil)
		iss.server.Close()
		h := newHarness(t, iss, discovering)

		err := h.OnStart(t.Context())
		if err == nil {
			t.Fatal("expected a startup failure")
		}
		if !strings.Contains(err.Error(), "OpenID configuration") {
			t.Errorf("err = %v, want it to say discovery is what failed", err)
		}
	})

	t.Run("refuses a document that publishes no usable address", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t, rsaKey(t, "rotation-1"))
		iss.setMetadata(map[string]any{"issuer": iss.url(), "jwks_uri": "/certs"})
		h := newHarness(t, iss, discovering)

		err := h.OnStart(t.Context())
		if err == nil {
			t.Fatal("expected a startup failure")
		}
		if !strings.Contains(err.Error(), "must be an absolute http or https URL") {
			t.Errorf("err = %v, want it to say the address is not absolute", err)
		}
	})

	t.Run("refuses keys published off the issuer's origin", func(t *testing.T) {
		t.Parallel()
		iss := newIssuer(t, rsaKey(t, "rotation-1"))
		iss.setMetadata(map[string]any{
			"issuer":   iss.url(),
			"jwks_uri": "https://elsewhere.example/keys",
		})
		h := newHarness(t, iss, discovering)

		err := h.OnStart(t.Context())
		if err == nil {
			t.Fatal("expected a startup failure")
		}
		if !strings.Contains(err.Error(), "off the issuer's own origin") {
			t.Errorf("err = %v, want it to say the keys are published elsewhere", err)
		}
		if got := iss.certRequests.Load(); got != 0 {
			t.Errorf("the keys were fetched %d time(s) from the address it refused", got)
		}
	})

	t.Run("reads a redundant port as the origin it plainly is", func(t *testing.T) {
		t.Parallel()
		// Refusing these would fail closed, which is safe — and would fail
		// closed on a correct configuration, which nobody meeting it could tell
		// from a real refusal.
		for _, c := range []struct {
			a, b string
			same bool
		}{
			{a: "http://idp/realms/w", b: "http://idp:80/certs", same: true},
			{a: "https://idp:443/realms/w", b: "https://idp/certs", same: true},
			{a: "http://idp/realms/w", b: "http://idp:8080/certs"},
			{a: "http://idp/realms/w", b: "https://idp/certs"},
			{a: "http://idp/realms/w", b: "http://elsewhere/certs"},
			{a: "http://idp:443/realms/w", b: "http://idp/certs"},
		} {
			if got := sameOrigin(c.a, c.b); got != c.same {
				t.Errorf("sameOrigin(%q, %q) = %v, want %v", c.a, c.b, got, c.same)
			}
		}
	})
}

// TestKeySetCollapsesConcurrentMissesForAKeyNobodyPublishes is the attack shape
// the single flight is really there for: many callers at once, each naming an
// identifier that will never be found, on a cache that is allowed to refresh.
// One fetch is the answer for all of them.
func TestKeySetCollapsesConcurrentMissesForAKeyNobodyPublishes(t *testing.T) {
	t.Parallel()

	key := rsaKey(t, "rotation-1")
	invented := rsaKey(t, "invented")
	iss := newIssuer(t, key)
	h := newHarness(t, iss).start(t)
	token := iss.mint(t, invented,
		iss.serviceClaims(h.clock.Now(), "service-account-wallet-service"))
	h.clock.advance(pastTheLimit)

	const callers = 24
	release := iss.hold(t)
	started := make(chan struct{}, callers)
	errs := make(chan error, callers)

	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			started <- struct{}{}
			_, err := h.Authenticate(context.WithoutCancel(t.Context()), token)
			errs <- err
		})
	}
	for range callers {
		<-started
	}
	time.Sleep(100 * time.Millisecond)
	release()
	wg.Wait()

	close(errs)
	for err := range errs {
		assertRefused(t, err, ErrKeyUnavailable)
	}
	if got := iss.certRequests.Load(); got != 2 {
		t.Errorf("the issuer was asked for keys %d time(s), want 2 — one to prime and one for "+
			"all %d misses together", got, callers)
	}
}

// TestKeySetRefusesAnOversizedDocument keeps an identity provider answering with
// an unbounded stream from costing this process its memory.
func TestKeySetRefusesAnOversizedDocument(t *testing.T) {
	t.Parallel()

	flood := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		block := make([]byte, 64<<10)
		for i := range block {
			block[i] = ' '
		}
		for range (maxDocumentBytes / len(block)) + 2 {
			if _, err := w.Write(block); err != nil {
				return
			}
		}
	}))
	t.Cleanup(flood.Close)

	authenticator, err := NewAuthenticator(Config{
		Issuer:     "https://idp.example/realms/wagering",
		Audience:   testAudience,
		JWKSURI:    flood.URL,
		HTTPClient: flood.Client(),
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}

	startErr := authenticator.OnStart(t.Context())
	if startErr == nil {
		t.Fatal("expected the oversized document to be refused")
	}
	if !strings.Contains(startErr.Error(), "more than") {
		t.Errorf("err = %v, want it to say the answer was too large", startErr)
	}
}

// TestDiscoveryRefusesADocumentItCannotRead covers the answer that is not
// metadata at all — a login page from a proxy, most often.
func TestDiscoveryRefusesADocumentItCannotRead(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html>sign in</html>"))
	}))
	t.Cleanup(server.Close)

	authenticator, err := NewAuthenticator(Config{
		Issuer:     server.URL + realmPath,
		Audience:   testAudience,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}

	startErr := authenticator.OnStart(t.Context())
	if startErr == nil {
		t.Fatal("expected the unreadable document to be refused")
	}
	if !strings.Contains(startErr.Error(), "OpenID configuration") {
		t.Errorf("err = %v, want it to say discovery is what failed", startErr)
	}
}

func TestVerifyingKeysKeepsOnlyWhatMayVerify(t *testing.T) {
	t.Parallel()

	usable := rsaKey(t, "usable")
	encoded := func(t *testing.T, key jwk.Key) string {
		t.Helper()
		raw, err := json.Marshal(key)
		if err != nil {
			t.Fatalf("encode a key: %v", err)
		}
		return string(raw)
	}

	anonymous := rsaKey(t, "anonymous")
	if err := anonymous.public.Remove(jwk.KeyIDKey); err != nil {
		t.Fatalf("remove the key id: %v", err)
	}

	cases := []struct {
		name     string
		document func(t *testing.T) string
		want     []string
		refused  bool
	}{
		{
			name: "a key that can verify",
			document: func(t *testing.T) string {
				return `{"keys":[` + encoded(t, usable.public) + `]}`
			},
			want: []string{"usable"},
		},
		{
			name: "a key with no identifier to select it by",
			document: func(t *testing.T) string {
				return `{"keys":[` + encoded(t, usable.public) + `,` +
					encoded(t, anonymous.public) + `]}`
			},
			want: []string{"usable"},
		},
		{
			name: "an entry this build cannot represent, beside one it can",
			document: func(t *testing.T) string {
				return `{"keys":[{"kty":"MARTIAN","kid":"alien"},` +
					encoded(t, usable.public) + `]}`
			},
			want: []string{"usable"},
		},
		{
			// Keycloak publishes an encryption key in the same document as its
			// signing key. A verifier that kept it would be willing to check a
			// signature against a key published for something else.
			name: "a key published for encryption, beside one for signing",
			document: func(t *testing.T) string {
				t.Helper()
				encryption := rsaKey(t, "encryption")
				if err := encryption.public.Set(jwk.KeyUsageKey, "enc"); err != nil {
					t.Fatalf("mark the key for encryption: %v", err)
				}
				return `{"keys":[` + encoded(t, usable.public) + `,` +
					encoded(t, encryption.public) + `]}`
			},
			want: []string{"usable"},
		},
		{
			// Nothing should reach a symmetric key — no symmetric algorithm can
			// be on the allow-list — and that is exactly why the cache refuses
			// to hold one: a cache with no shared secret in it cannot be talked
			// into verifying with one.
			name: "a symmetric key, beside an asymmetric one",
			document: func(t *testing.T) string {
				t.Helper()
				shared, err := jwk.Import([]byte("a shared secret nobody should verify with"))
				if err != nil {
					t.Fatalf("import a symmetric key: %v", err)
				}
				if err := shared.Set(jwk.KeyIDKey, "symmetric"); err != nil {
					t.Fatalf("set the key id: %v", err)
				}
				return `{"keys":[` + encoded(t, usable.public) + `,` + encoded(t, shared) + `]}`
			},
			want: []string{"usable"},
		},
		{
			// A key with no "use" at all is kept. The filter is "not something
			// else", not "says signing": an issuer is allowed to publish a key
			// without saying what it is for, and refusing those would refuse
			// most of the JWKS documents in the world.
			name: "a key that declares no use at all",
			document: func(t *testing.T) string {
				t.Helper()
				plain := rsaKey(t, "plain")
				if err := plain.public.Remove(jwk.KeyUsageKey); err != nil {
					t.Fatalf("remove the key use: %v", err)
				}
				return `{"keys":[` + encoded(t, plain.public) + `]}`
			},
			want: []string{"plain"},
		},
		{
			name:     "a document that is not a key set",
			document: func(*testing.T) string { return `"not a key set"` },
			refused:  true,
		},
		{
			name:     "a key set with nothing usable in it",
			document: func(*testing.T) string { return `{"keys":[]}` },
			refused:  true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			keys, err := verifyingKeys([]byte(c.document(t)))
			if c.refused {
				if err == nil {
					t.Fatalf("expected the document to be refused, got %d key(s)", len(keys))
				}
				return
			}
			if err != nil {
				t.Fatalf("verifyingKeys: %v", err)
			}
			if len(keys) != len(c.want) {
				t.Fatalf("kept %d key(s), want %d", len(keys), len(c.want))
			}
			for _, kid := range c.want {
				if _, ok := keys[kid]; !ok {
					t.Errorf("the key under %q was dropped", kid)
				}
			}
		})
	}
}
