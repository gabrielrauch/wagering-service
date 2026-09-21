package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

const (
	// discoveryPath is where an OpenID provider publishes its metadata.
	discoveryPath = "/.well-known/openid-configuration"
	// maxDocumentBytes bounds a response body. An identity provider answering
	// with an unbounded stream should cost this process one failed refresh, not
	// its memory.
	maxDocumentBytes = 1 << 20
	// signingUse is the "use" a key must declare if it declares one at all.
	// Keycloak publishes an encryption key beside its signing key, and a
	// verifier that kept both would be willing to check a signature against a
	// key published for something else.
	signingUse = "sig"
)

// keySet is the issuer's published signing keys, cached.
//
// The four properties it exists for are stated in the package doc and
// implemented here rather than taken on trust from a library: an unknown key
// identifier triggers at most one refresh, refreshes are rate-limited,
// concurrent misses collapse into one fetch, and a failed refresh changes
// nothing.
type keySet struct {
	issuer       string
	client       *http.Client
	clock        app.Clock
	fetchTimeout time.Duration
	minInterval  time.Duration

	mu sync.Mutex
	// uri is the JWKS address: configured, or discovered on first use and kept.
	uri string
	// keys is what verifies today, by "kid".
	keys map[string]jwk.Key
	// lastFetch is when a fetch last finished, successfully or not. Both count:
	// an identity provider that is failing is exactly the one an attacker
	// presenting unknown key identifiers would otherwise get to hammer.
	lastFetch time.Time
	// inflight is closed when the fetch currently running has finished. It is
	// the single-flight: a caller that finds it waits on it rather than opening
	// a second connection to say the same thing.
	inflight chan struct{}
}

// newKeySet builds the cache. It performs no I/O: an authenticator is
// constructed without a network, and [Authenticator.OnStart] is where reaching
// the issuer is allowed to fail.
func newKeySet(s settings) *keySet {
	return &keySet{
		issuer:       s.issuer,
		client:       s.client,
		clock:        s.clock,
		fetchTimeout: s.fetchTimeout,
		minInterval:  s.minInterval,
		uri:          s.jwksURI,
	}
}

// prime fetches the key set once, bounded, honouring the caller's context.
//
// It is the startup path and deliberately not the refresh path: it ignores the
// rate limit, because the limit exists to stop a caller-driven loop and there
// is no caller here, and it reports the underlying failure verbatim, because
// the audience is whoever is starting the process rather than whoever is
// presenting a token.
func (s *keySet) prime(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.fetchTimeout)
	defer cancel()

	keys, err := s.fetch(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFetch = s.clock.Now()
	if err != nil {
		return err
	}
	s.keys = keys
	return nil
}

// key returns the public key published under kid, refreshing once if the cache
// does not hold it.
//
// A miss is not an error on its own — it is what a rotated key looks like from
// here — but a miss that survives a refresh is, and so is a miss that arrives
// too soon after the last fetch to justify another.
func (s *keySet) key(ctx context.Context, kid string) (jwk.Key, error) {
	if key, ok := s.cached(kid); ok {
		return key, nil
	}

	done, err := s.startRefresh()
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}

	if key, ok := s.cached(kid); ok {
		return key, nil
	}
	return nil, fmt.Errorf("oidc: the issuer publishes no key under that identifier: %w",
		errNoSuchKey)
}

// cached answers from what is already held.
func (s *keySet) cached(kid string) (jwk.Key, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.keys[kid]
	return key, ok
}

// The conditions key reports by value, so that [Authenticator] can tell a
// refusal it should render as "unverifiable" from one it should not reach.
var (
	// errNoSuchKey is a key identifier the issuer's published set does not
	// contain, after a refresh that was allowed to happen.
	errNoSuchKey = errors.New("oidc: unknown key identifier")
	// errRefreshTooSoon is a refresh the rate limit declined. It is what an
	// attacker presenting a stream of invented key identifiers gets, and it
	// costs the identity provider nothing.
	errRefreshTooSoon = errors.New("oidc: the key set was refreshed too recently")
)

// startRefresh joins the fetch already running, or starts one, or declines.
//
// Joining is checked before the rate limit on purpose. A caller that arrives
// while a fetch is in flight has not asked for a second one, so turning it away
// for asking too soon would refuse a token the fetch about to finish may well
// be able to verify.
func (s *keySet) startRefresh() (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inflight != nil {
		return s.inflight, nil
	}
	if !s.lastFetch.IsZero() && s.clock.Now().Sub(s.lastFetch) < s.minInterval {
		return nil, errRefreshTooSoon
	}

	done := make(chan struct{})
	s.inflight = done
	go s.runRefresh(done)
	return done, nil
}

// runRefresh performs the one fetch the callers waiting on done are sharing.
//
// The context is this fetch's own, bounded by the fetch timeout and rooted at
// no caller. The caller that happened to start it is not the only one waiting,
// so its giving up must not cancel the work the others are still waiting for;
// each of them leaves on its own context in [keySet.key] instead.
//
// A failure updates lastFetch and nothing else. Keys that still verify are not
// evicted because the identity provider was briefly unreachable, which would
// turn one outage into two.
func (s *keySet) runRefresh(done chan struct{}) {
	defer close(done)

	ctx, cancel := context.WithTimeout(context.Background(), s.fetchTimeout)
	defer cancel()
	keys, err := s.fetch(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFetch = s.clock.Now()
	s.inflight = nil
	if err == nil {
		s.keys = keys
	}
}

// fetch reads the issuer's published keys, discovering where they are if it has
// not already.
func (s *keySet) fetch(ctx context.Context) (map[string]jwk.Key, error) {
	uri, err := s.resolveURI(ctx)
	if err != nil {
		return nil, err
	}
	body, err := s.get(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("oidc: read the issuer's signing keys: %w", err)
	}
	keys, err := verifyingKeys(body)
	if err != nil {
		return nil, fmt.Errorf("oidc: read the issuer's signing keys: %w", err)
	}
	return keys, nil
}

// resolveURI returns where the keys are published, discovering it once.
//
// Discovery is not itself cached against failure: a discovery that fails
// returns an error and leaves the address unset, so the next fetch tries again.
// That is the same bounded, rate-limited path every other fetch takes, so it
// amplifies nothing.
func (s *keySet) resolveURI(ctx context.Context) (string, error) {
	s.mu.Lock()
	uri := s.uri
	s.mu.Unlock()
	if uri != "" {
		return uri, nil
	}

	discovered, err := s.discover(ctx)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.uri = discovered
	s.mu.Unlock()
	return discovered, nil
}

// discover reads the issuer's OpenID configuration for its jwks_uri.
//
// Two things are checked before the answer is used, and both are the same
// check: that the document really speaks for the configured issuer. Its own
// "issuer" must match, which is what the specification requires and what stops
// a redirect ending somewhere else from being taken as authoritative. Its
// jwks_uri must sit on the issuer's own scheme and host, because a document
// that could send key fetching to another origin could send it to an attacker's
// — and a deployment where the two genuinely differ configures
// [Config.JWKSURI] and does not discover at all.
func (s *keySet) discover(ctx context.Context) (string, error) {
	body, err := s.get(ctx, s.issuer+discoveryPath)
	if err != nil {
		return "", fmt.Errorf("oidc: read the issuer's OpenID configuration: %w", err)
	}

	var metadata struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(body, &metadata); err != nil {
		return "", fmt.Errorf("oidc: read the issuer's OpenID configuration: %w", err)
	}
	if metadata.Issuer != s.issuer {
		return "", fmt.Errorf(
			"oidc: the OpenID configuration at %s names issuer %q, not the configured %q",
			s.issuer+discoveryPath, metadata.Issuer, s.issuer)
	}
	if err := checkAbsolute(metadata.JWKSURI, "discovered JWKS URI"); err != nil {
		return "", err
	}
	if !sameOrigin(s.issuer, metadata.JWKSURI) {
		return "", fmt.Errorf(
			"oidc: the OpenID configuration publishes its keys at %q, off the issuer's own origin",
			metadata.JWKSURI)
	}
	return metadata.JWKSURI, nil
}

// sameOrigin reports whether two absolute URLs share a scheme and an authority.
func sameOrigin(a, b string) bool {
	parsedA, errA := url.Parse(a)
	parsedB, errB := url.Parse(b)
	if errA != nil || errB != nil {
		return false
	}
	return parsedA.Scheme == parsedB.Scheme && parsedA.Host == parsedB.Host
}

// get reads a bounded JSON document, within the caller's context.
func (s *keySet) get(ctx context.Context, uri string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")

	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s answered %s", uri, response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDocumentBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxDocumentBytes {
		return nil, fmt.Errorf("%s answered with more than %d bytes", uri, maxDocumentBytes)
	}
	return body, nil
}

// verifyingKeys turns a JWKS document into the keys this package will verify
// with, which is a strict subset of what it contains.
//
// Three filters, each closing something a published set can legitimately hold:
//
//   - A key with no "kid" is dropped, because key selection here is by "kid"
//     and a key that cannot be selected is a key that would have to be guessed.
//   - A symmetric key is dropped. Nothing should reach it — the allow-list has
//     no symmetric algorithm in it — and that is precisely why it is worth a
//     line: a cache that cannot hold a shared secret cannot be talked into
//     verifying with one.
//   - A key that declares a "use" other than signing is dropped. Keycloak
//     publishes an encryption key in the same document.
//
// What survives is stored as its public half, so private material could not be
// held here even if an issuer published some.
//
// Parse errors on individual entries are ignored rather than failing the
// document: a set is a bag of keys, and one entry this build cannot represent
// must not take down the others. An empty result is still a failed fetch, so a
// document that is entirely unusable never replaces one that works.
func verifyingKeys(document []byte) (map[string]jwk.Key, error) {
	set, err := jwk.Parse(document, jwk.WithIgnoreParseError(true))
	if err != nil {
		return nil, fmt.Errorf("parse the JWKS document: %w", err)
	}

	keys := make(map[string]jwk.Key, set.Len())
	for i := range set.Len() {
		key, ok := set.Key(i)
		if !ok {
			continue
		}
		kid, ok := key.KeyID()
		if !ok || strings.TrimSpace(kid) == "" {
			continue
		}
		if _, duplicate := keys[kid]; duplicate {
			continue
		}
		if key.KeyType() == jwa.OctetSeq() {
			continue
		}
		if use, ok := key.KeyUsage(); ok && use != signingUse {
			continue
		}
		public, err := key.PublicKey()
		if err != nil {
			continue
		}
		keys[kid] = public
	}
	if len(keys) == 0 {
		return nil, errors.New("the JWKS document publishes no usable signing key")
	}
	return keys, nil
}
