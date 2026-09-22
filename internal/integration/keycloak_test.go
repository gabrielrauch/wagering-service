//go:build integration

// The identity provider: one Keycloak container for the package, importing the
// realm this service is deployed with, and the credentials this suite presents.
package integration

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
)

// The realm, as deploy/keycloak/realm-export.json declares it. Every one of
// these is a placeholder: the realm is for development and for this suite, and
// there is no real secret anywhere in it.
const (
	keycloakImage = "quay.io/keycloak/keycloak:26.4"
	realmName     = "wagering"
	apiAudience   = "wagering-api"

	// providerA and providerB are two game operators, so that isolation can be
	// shown between two of them rather than asserted about one.
	providerA = "provider-a"
	providerB = "provider-b"
	// walletService is this system acting for itself, holding the internal role.
	walletService = "wallet-service"
	// expiringProvider is a provider whose access tokens live five seconds. See
	// [shortLivedGrant].
	expiringProvider = "provider-expiring"

	// discoveryPath is the readiness signal: the realm answering for itself,
	// which is later than the container answering and is the thing a token
	// request needs.
	discoveryPath = "/realms/" + realmName + "/.well-known/openid-configuration"
	tokenPath     = "/realms/" + realmName + "/protocol/openid-connect/token"
	certsPath     = "/realms/" + realmName + "/protocol/openid-connect/certs"
)

// keycloakStartupBudget is how long the container has to import the realm and
// answer for it. See the note beside the wait strategy.
const keycloakStartupBudget = 3 * time.Minute

// expiryClockSkew is the tolerance the ONE authenticator that has to outlive a
// token's expiry runs with.
//
// One second rather than the package's thirty-second default, because the
// expired-token scenario has to wait the skew out and thirty-one seconds of
// sleep is a suite nobody runs twice. It is confined to the stacks that
// scenario builds, and every other stack here runs on the default, because the
// skew is not only a bound on "exp": oidc refuses a token whose "iat" is more
// than the skew in the FUTURE, so a host whose clock had stepped a second
// behind the container's would fail every test in the suite with "the token was
// issued in the future" — which reads as a broken realm and is a broken clock,
// and is exactly the case the thirty-second default was sized for.
//
// One second is safe for the two tests that take it, and the suite measured
// why: Keycloak's clock is this host's clock, so "iat" never runs ahead. The
// cost of being wrong about that is now two tests rather than fourteen.
const expiryClockSkew = time.Second

var (
	// identityBase is the address Keycloak was published on, and so the prefix
	// of the issuer, the discovery document and the JWKS. Keycloak 26 derives
	// all three from the request's host, so a container on a mapped port is
	// self-consistent and oidc.Config.JWKSURI is not needed.
	identityBase string
	identityErr  error
)

// startKeycloak brings up the identity provider with the realm imported.
//
// The export is the repository's own deploy/keycloak/realm-export.json rather
// than a fixture of this suite's, so that the realm under test is the artefact
// that ships and not a copy of it that could drift.
//
// docker-compose.yml's keycloak service imports the same file the same way —
// `start-dev --import-realm`, the export mounted under
// /opt/keycloak/data/import, the bootstrap administrator from the environment —
// so the arguments below are that service restated for a container this suite
// owns, on a port testcontainers chooses, rather than a second description of
// how the realm is loaded.
func startKeycloak(ctx context.Context) (testcontainers.Container, error) {
	export, err := repositoryFile("deploy", "keycloak", "realm-export.json")
	if err != nil {
		return nil, err
	}
	container, err := testcontainers.Run(ctx, keycloakImage,
		testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithCmd("start-dev", "--import-realm"),
		// The bootstrap administrator is what a developer will log into the
		// console with. It is a placeholder, it is never used by this suite,
		// and it is set here so that the container this suite runs is the
		// container the compose service will have to run.
		testcontainers.WithEnv(map[string]string{
			"KC_BOOTSTRAP_ADMIN_USERNAME": "admin",
			"KC_BOOTSTRAP_ADMIN_PASSWORD": "admin",
		}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      export,
			ContainerFilePath: "/opt/keycloak/data/import/realm-export.json",
			FileMode:          0o644,
		}),
		// The realm's own discovery document, not the container's health
		// endpoint. Keycloak accepts connections well before the import has
		// finished, so anything earlier than this would hand the first test a
		// realm that does not exist yet.
		//
		// AndDeadline, and that is not a stylistic choice. WithWaitStrategy is
		// WithWaitStrategyAndDeadline(60s, ...), and the deadline becomes a
		// context timeout wrapped AROUND the inner strategies — so a
		// WithStartupTimeout longer than it can never take effect, and the real
		// budget stays sixty seconds however large the inner one is written.
		// Keycloak boots in about twelve seconds unloaded and was measured at
		// seventy-three under `go test -race -tags integration ./...`, where it
		// competes with three other container suites for the same cores; at the
		// implicit sixty that failed two runs in three.
		testcontainers.WithWaitStrategyAndDeadline(keycloakStartupBudget,
			wait.ForHTTP(discoveryPath).
				WithPort("8080/tcp").
				WithStartupTimeout(keycloakStartupBudget)),
	)
	if err != nil {
		// The container is returned beside the error, because Run hands one
		// back even when the wait failed and it is the caller's to terminate.
		return container, fmt.Errorf("start keycloak: %w", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		return container, fmt.Errorf("keycloak host: %w", err)
	}
	port, err := container.MappedPort(ctx, "8080/tcp")
	if err != nil {
		return container, fmt.Errorf("keycloak port: %w", err)
	}
	identityBase = "http://" + host + ":" + port.Port()
	return container, nil
}

// requireIdentityProvider fails rather than skips. See main_test.go.
func requireIdentityProvider(t *testing.T) {
	t.Helper()
	if identityErr != nil {
		t.Fatalf("this suite needs a Keycloak and could not start one "+
			"(is Docker running?): %v", identityErr)
	}
}

// issuerURL is the value every token's "iss" carries and the authenticator is
// configured with, byte for byte.
func issuerURL() string { return identityBase + "/realms/" + realmName }

// identityClient is the transport this suite talks to Keycloak with. It is not
// the authenticator's — that one is built from the adapter's own configuration
// and refuses redirects.
var identityClient = &http.Client{Timeout: 10 * time.Second}

// The two [oidc.Authenticator]s this package has, each shared by every stack
// that asks for it.
//
// Shared rather than one per test, and that is the honest wiring as well as the
// fast one: the authenticator holds the key cache, which is process-wide in the
// service too, and giving each test a private one would mean no test ever
// exercised a cache that had already been used.
//
// Two rather than one because of the skew above. Twelve of the fourteen tests
// here take the default configuration; the two that watch a token expire take
// the tight one, and nothing else is different between them.
var (
	defaultAuthenticator = sync.OnceValues(func() (*oidc.Authenticator, error) {
		return newAuthenticator(0)
	})
	tightAuthenticator = sync.OnceValues(func() (*oidc.Authenticator, error) {
		return newAuthenticator(expiryClockSkew)
	})
)

// newAuthenticator builds and primes a verifier pointed at the realm. A zero
// skew means the package's own default, which is what production would run.
func newAuthenticator(skew time.Duration) (*oidc.Authenticator, error) {
	a, err := oidc.NewAuthenticator(oidc.Config{
		Issuer:     issuerURL(),
		Audience:   apiAudience,
		HTTPClient: identityClient,
		ClockSkew:  skew,
	})
	if err != nil {
		return nil, fmt.Errorf("new authenticator: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The same call the composition root makes, so a realm that published no
	// usable key fails here rather than as a wall of 401s.
	if err := a.OnStart(ctx); err != nil {
		return nil, fmt.Errorf("prime the authenticator: %w", err)
	}
	return a, nil
}

// grant is one client_credentials access token and the instant it stops being
// one.
type grant struct {
	token     string
	expiresAt time.Time
}

// held caches a token per client.
//
// A token lives five minutes and this suite issues dozens of requests, so
// minting one per request would spend most of the run talking to Keycloak
// rather than to the service.
var held = struct {
	mu     sync.Mutex
	grants map[string]grant
}{grants: make(map[string]grant)}

// tokenFor obtains an access token for a client, by a real client_credentials
// grant against the realm's own token endpoint.
func tokenFor(t *testing.T, clientID string) string {
	t.Helper()
	requireIdentityProvider(t)

	if g, ok := cachedGrant(clientID); ok {
		return g.token
	}
	// Outside the lock. Holding it across the round trip would serialise every
	// parallel test in the package behind one HTTP request to Keycloak, and the
	// worst a concurrent miss can do is mint a second token nobody needed.
	g, err := clientCredentials(t.Context(), clientID, secretFor(clientID))
	if err != nil {
		t.Fatalf("obtain a token for %s: %v", clientID, err)
	}
	held.mu.Lock()
	defer held.mu.Unlock()
	held.grants[clientID] = g
	return g.token
}

// cachedGrant answers the held token for a client while a minute of it is left,
// so a slow run never presents a credential that expired between being asked
// for and being used.
func cachedGrant(clientID string) (grant, bool) {
	held.mu.Lock()
	defer held.mu.Unlock()
	g, ok := held.grants[clientID]
	return g, ok && time.Now().Add(time.Minute).Before(g.expiresAt)
}

// secretFor is the client secret the realm declares, which is the client id
// with "-secret" after it. A placeholder, and documented as one.
func secretFor(clientID string) string { return clientID + "-secret" }

// clientCredentials performs the grant and reads the expiry out of the token
// itself rather than out of the response's expires_in, because "exp" is what
// the verifier reads and the two are not obliged to agree.
func clientCredentials(ctx context.Context, clientID, secret string) (grant, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {secret},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		identityBase+tokenPath, strings.NewReader(form.Encode()))
	if err != nil {
		return grant{}, fmt.Errorf("build the token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := identityClient.Do(req)
	if err != nil {
		return grant{}, fmt.Errorf("post to the token endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return grant{}, fmt.Errorf("read the token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return grant{}, fmt.Errorf("the token endpoint answered %d: %s", resp.StatusCode, body)
	}
	var answer struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return grant{}, fmt.Errorf("decode the token response: %w", err)
	}
	if answer.AccessToken == "" {
		return grant{}, errors.New("the token endpoint answered with no access_token")
	}
	claims, err := claimsOf(answer.AccessToken)
	if err != nil {
		return grant{}, err
	}
	exp, ok := claims["exp"].(float64)
	if !ok {
		return grant{}, errors.New("the access token states no expiry")
	}
	return grant{token: answer.AccessToken, expiresAt: time.Unix(int64(exp), 0)}, nil
}

// refusedCredentials reports the status the token endpoint answers a grant
// with, which is how this suite shows that the secrets really are per-client.
func refusedCredentials(t *testing.T, clientID, secret string) int {
	t.Helper()
	requireIdentityProvider(t)
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {secret},
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		identityBase+tokenPath, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build the token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := identityClient.Do(req)
	if err != nil {
		t.Fatalf("post to the token endpoint: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// shortLivedGrant obtains a token from provider-expiring, whose access tokens
// the realm gives a five-second lifespan.
//
// Never cached, and five seconds rather than one so that a caller has room to
// present it while it still works before waiting it out — which is what makes
// "this credential stopped working" a statement about time rather than about
// the client.
//
// Nothing else in the realm is short-lived. That is why this is a client of its
// own rather than an attribute on provider-a: an override there would put every
// other test in the suite one slow moment away from a credential that expired
// while it was being used.
func shortLivedGrant(t *testing.T) grant {
	t.Helper()
	requireIdentityProvider(t)

	g, err := clientCredentials(t.Context(), expiringProvider, secretFor(expiringProvider))
	if err != nil {
		t.Fatalf("obtain a short-lived token: %v", err)
	}
	return g
}

// waitOutExpiry sleeps until a grant is past the point the verifier stops
// accepting it.
//
// The instant is computed from the token's own "exp" plus the skew the
// authenticator was configured with, so it is the verifier's own rule rather
// than a sleep somebody guessed. The half-second on top is because "now" here
// and "now" inside the authenticator are two readings of one clock.
func waitOutExpiry(g grant) {
	deadline := g.expiresAt.Add(expiryClockSkew).Add(500 * time.Millisecond)
	if remaining := time.Until(deadline); remaining > 0 {
		time.Sleep(remaining)
	}
}

// expiredToken is the pair of them, for a caller with nothing to do in between.
func expiredToken(t *testing.T) string {
	t.Helper()
	g := shortLivedGrant(t)
	waitOutExpiry(g)
	return g.token
}

// forgery is the key this suite signs with, which the realm has never heard of.
//
// One key for the package because generating an RSA key is the slowest thing
// here, and because nothing about these scenarios needs a fresh one.
var forgery = sync.OnceValues(func() (jwk.Key, error) {
	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate an RSA key: %w", err)
	}
	key, err := jwk.Import(raw)
	if err != nil {
		return nil, fmt.Errorf("import the RSA key: %w", err)
	}
	return key, nil
})

// forgedToken signs a claim set this service would otherwise accept, with a key
// the realm never published, under whichever key identifier the caller names.
//
// Naming the realm's own kid is the interesting case: the verifier then finds a
// real public key and the signature does not check out, which is a different
// refusal from one where no key could be found at all, and both must be 401.
func forgedToken(t *testing.T, kid string) string {
	t.Helper()
	key, err := forgery()
	if err != nil {
		t.Fatal(err)
	}
	signing, err := key.Clone()
	if err != nil {
		t.Fatalf("clone the forging key: %v", err)
	}
	if err := signing.Set(jwk.KeyIDKey, kid); err != nil {
		t.Fatalf("set the key identifier: %v", err)
	}

	now := time.Now()
	claims := map[string]any{
		"iss": issuerURL(),
		"aud": apiAudience,
		"sub": "a-subject-nobody-issued",
		"azp": providerA,
		"typ": "Bearer",
		"iat": now.Add(-time.Minute).Unix(),
		"exp": now.Add(5 * time.Minute).Unix(),
		"realm_access": map[string]any{
			"roles": []string{"provider"},
		},
		"providerId": providerA,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("encode the forged claims: %v", err)
	}
	signed, err := jws.Sign(payload, jws.WithKey(jwa.RS256(), signing))
	if err != nil {
		t.Fatalf("sign the forged token: %v", err)
	}
	return string(signed)
}

// publishedSigningKeyID reads the key identifier the realm signs with, straight
// out of the JWKS the verifier fetches.
//
// The document holds two entries — Keycloak publishes an encryption key beside
// the signing one — so the "use" member is what picks, which is also the filter
// the adapter applies.
func publishedSigningKeyID(t *testing.T) string {
	t.Helper()
	requireIdentityProvider(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, identityBase+certsPath, nil)
	if err != nil {
		t.Fatalf("build the JWKS request: %v", err)
	}
	resp, err := identityClient.Do(req)
	if err != nil {
		t.Fatalf("fetch the JWKS: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var document struct {
		Keys []struct {
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		t.Fatalf("decode the JWKS: %v", err)
	}
	for _, key := range document.Keys {
		if key.Use == "sig" {
			return key.Kid
		}
	}
	t.Fatalf("the realm published no signing key: %+v", document.Keys)
	return ""
}

// claimsOf decodes a token's payload without checking anything about it, which
// is all a test asserting on what the realm minted needs.
func claimsOf(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("a token has three parts, this one has %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode the payload: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("decode the claims: %w", err)
	}
	return claims, nil
}
