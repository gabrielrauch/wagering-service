package oidc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
)

// The realm this suite pretends to be. The paths are Keycloak's, so the
// discovery document and the JWKS address look like the ones T5's container
// will actually serve.
const (
	realmPath    = "/realms/wagering"
	certsPath    = realmPath + "/protocol/openid-connect/certs"
	testAudience = "wagering-api"
)

// signingKey is a key pair the suite signs with and publishes.
type signingKey struct {
	kid     string
	alg     jwa.SignatureAlgorithm
	private jwk.Key
	public  jwk.Key
}

// rsaKey mints an RSA key pair published under kid and signed with RS256.
//
// 2048 bits rather than anything smaller because jwx enforces a minimum
// modulus, and rather than anything larger because every test here generates
// one and key generation is the slowest thing in the suite.
func rsaKey(t *testing.T, kid string) *signingKey {
	t.Helper()
	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate an RSA key: %v", err)
	}
	return keyPair(t, kid, jwa.RS256(), raw, raw.Public())
}

// ecdsaKey mints a P-256 key pair published under kid and signed with ES256.
func ecdsaKey(t *testing.T, kid string) *signingKey {
	t.Helper()
	raw, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate an ECDSA key: %v", err)
	}
	return keyPair(t, kid, jwa.ES256(), raw, raw.Public())
}

func keyPair(t *testing.T, kid string, alg jwa.SignatureAlgorithm, private, public any) *signingKey {
	t.Helper()
	privateJWK, err := jwk.Import(private)
	if err != nil {
		t.Fatalf("import the private key: %v", err)
	}
	publicJWK, err := jwk.Import(public)
	if err != nil {
		t.Fatalf("import the public key: %v", err)
	}
	for _, pair := range []struct {
		key jwk.Key
		set map[string]any
	}{
		{privateJWK, map[string]any{jwk.KeyIDKey: kid, jwk.AlgorithmKey: alg}},
		{publicJWK, map[string]any{
			jwk.KeyIDKey: kid, jwk.AlgorithmKey: alg, jwk.KeyUsageKey: signingUse,
		}},
	} {
		for field, value := range pair.set {
			if err := pair.key.Set(field, value); err != nil {
				t.Fatalf("set %s on a key: %v", field, err)
			}
		}
	}
	return &signingKey{kid: kid, alg: alg, private: privateJWK, public: publicJWK}
}

// issuer is a real HTTP server publishing a real JWKS document.
//
// It is not a mock of the verifier's collaborators: the tokens it mints are
// signed, the document it serves is parsed by the same code that parses
// Keycloak's, and every assertion about caching is made against the count of
// requests it actually received. What it is not is Keycloak — obtaining a token
// from a real identity provider is the integration suite's job.
type issuer struct {
	server *httptest.Server
	// certRequests counts the JWKS fetches this server answered, which is what
	// every caching assertion in this suite is made against.
	certRequests atomic.Int64
	// discoveryRequests counts the OpenID configuration reads.
	discoveryRequests atomic.Int64

	mu sync.Mutex
	// published is the key set the server currently serves.
	published []*signingKey
	// extra is appended to the served document verbatim, for the entries a
	// signing key cannot express.
	extra []json.RawMessage
	// broken makes the JWKS endpoint answer 500.
	broken bool
	// gate, when set, blocks each JWKS request until it is closed.
	gate chan struct{}
	// metadata overrides the discovery document.
	metadata map[string]any
}

// newIssuer starts a server publishing keys.
func newIssuer(t *testing.T, keys ...*signingKey) *issuer {
	t.Helper()
	iss := &issuer{published: keys}

	mux := http.NewServeMux()
	mux.HandleFunc(realmPath+discoveryPath, iss.serveDiscovery)
	mux.HandleFunc(certsPath, iss.serveCerts)
	iss.server = httptest.NewServer(mux)
	t.Cleanup(iss.server.Close)
	return iss
}

// url is the issuer identifier, which is also what "iss" carries.
func (i *issuer) url() string { return i.server.URL + realmPath }

// certsURL is where the keys are published.
func (i *issuer) certsURL() string { return i.server.URL + certsPath }

func (i *issuer) serveDiscovery(w http.ResponseWriter, _ *http.Request) {
	i.discoveryRequests.Add(1)
	i.mu.Lock()
	metadata := i.metadata
	i.mu.Unlock()
	if metadata == nil {
		metadata = map[string]any{"issuer": i.url(), "jwks_uri": i.certsURL()}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(metadata)
}

func (i *issuer) serveCerts(w http.ResponseWriter, _ *http.Request) {
	i.certRequests.Add(1)

	i.mu.Lock()
	gate, broken, keys, extra := i.gate, i.broken, i.published, i.extra
	i.mu.Unlock()

	if gate != nil {
		<-gate
	}
	if broken {
		http.Error(w, "the key set is unavailable", http.StatusInternalServerError)
		return
	}

	entries := make([]json.RawMessage, 0, len(keys)+len(extra))
	for _, key := range keys {
		encoded, err := json.Marshal(key.public)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		entries = append(entries, encoded)
	}
	entries = append(entries, extra...)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": entries})
}

// publish replaces the key set the server serves.
func (i *issuer) publish(keys ...*signingKey) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.published = keys
}

// publishRaw appends entries to the served document that a key pair cannot
// express — a symmetric key, or one published for encryption.
func (i *issuer) publishRaw(entries ...json.RawMessage) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.extra = append(i.extra, entries...)
}

// breakCerts makes the JWKS endpoint fail, and returns a function restoring it.
func (i *issuer) breakCerts() func() {
	i.mu.Lock()
	i.broken = true
	i.mu.Unlock()
	return func() {
		i.mu.Lock()
		i.broken = false
		i.mu.Unlock()
	}
}

// hold blocks every JWKS request until the returned function is called, which
// is how the concurrent callers in the single-flight test are made to overlap.
//
// The release is also registered as a cleanup: httptest.Server.Close waits for
// the requests it is still serving, so a test that failed before releasing
// would hang rather than report.
func (i *issuer) hold(t *testing.T) func() {
	t.Helper()
	gate := make(chan struct{})
	i.mu.Lock()
	i.gate = gate
	i.mu.Unlock()

	release := sync.OnceFunc(func() {
		i.mu.Lock()
		i.gate = nil
		i.mu.Unlock()
		close(gate)
	})
	t.Cleanup(release)
	return release
}

// setMetadata overrides the discovery document.
func (i *issuer) setMetadata(metadata map[string]any) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.metadata = metadata
}

// mint signs a token, starting from claims that would verify and applying
// whatever the case under test wanted different.
func (i *issuer) mint(t *testing.T, key *signingKey, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("encode the claims: %v", err)
	}
	signed, err := jws.Sign(payload, jws.WithKey(key.alg, key.private))
	if err != nil {
		t.Fatalf("sign the token: %v", err)
	}
	return string(signed)
}

// claimsFor builds the claim set a Keycloak client_credentials token carries,
// for a service account holding the given realm roles.
func (i *issuer) claimsFor(now time.Time, subject, client string, roles ...string) map[string]any {
	return map[string]any{
		"iss":          i.url(),
		"aud":          testAudience,
		"sub":          subject,
		"azp":          client,
		"exp":          now.Add(5 * time.Minute).Unix(),
		"iat":          now.Unix(),
		"typ":          "Bearer",
		"realm_access": map[string]any{"roles": append([]string{"offline_access"}, roles...)},
	}
}

// providerClaims is what provider-a and provider-b present.
func (i *issuer) providerClaims(now time.Time, subject, client, provider string) map[string]any {
	claims := i.claimsFor(now, subject, client, providerRoleName)
	claims[providerIDClaim] = provider
	return claims
}

// serviceClaims is what wallet-service presents.
func (i *issuer) serviceClaims(now time.Time, subject string) map[string]any {
	return i.claimsFor(now, subject, "wallet-service", internalRoleName)
}

// fixedClock holds time still so that an expiry can be crossed on purpose.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFixedClock() *fixedClock {
	return &fixedClock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
}

// Now implements app.Clock.
func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fixedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// harness is an authenticator wired to a running issuer, with the clock the
// test drives.
type harness struct {
	*Authenticator
	issuer *issuer
	clock  *fixedClock
}

// newHarness builds the authenticator under test. The JWKS address is
// configured rather than discovered unless the case says otherwise, so that a
// test about caching counts only the requests it is about.
func newHarness(t *testing.T, iss *issuer, adjust ...func(*Config)) *harness {
	t.Helper()
	clock := newFixedClock()
	cfg := Config{
		Issuer:     iss.url(),
		Audience:   testAudience,
		JWKSURI:    iss.certsURL(),
		HTTPClient: iss.server.Client(),
		Clock:      clock,
	}
	for _, adjustment := range adjust {
		adjustment(&cfg)
	}
	authenticator, err := NewAuthenticator(cfg)
	if err != nil {
		t.Fatalf("build the authenticator: %v", err)
	}
	return &harness{Authenticator: authenticator, issuer: iss, clock: clock}
}

// start runs the startup fetch and fails the test if it does not succeed.
func (h *harness) start(t *testing.T) *harness {
	t.Helper()
	if err := h.OnStart(t.Context()); err != nil {
		t.Fatalf("prime the key set: %v", err)
	}
	return h
}
