package oidc

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// The bounds this package applies when the configuration leaves them open.
//
// Each is a default rather than a requirement because each has one answer that
// is right for every deployment of this service, and a configuration file full
// of values nobody has a reason to change is a configuration file nobody reads.
const (
	// defaultClockSkew is how far apart this process's clock and the identity
	// provider's may be before a token is judged wrongly.
	//
	// Thirty seconds. Hosts that talk to an NTP server are within milliseconds
	// of each other, so this is not sized for drift — it is sized for a step: a
	// container resumed from a snapshot, or a host whose clock was corrected
	// while a token was in flight. It is also an order of magnitude below the
	// shortest access token lifetime anybody configures, which is the property
	// that matters: a skew approaching the token lifetime makes "exp"
	// decorative, and the point of a short-lived token is that it stops working.
	defaultClockSkew = 30 * time.Second
	// maxClockSkew is the widest skew that may be configured, for the reason
	// above. Five minutes is Keycloak's default access token lifespan; a skew
	// at or beyond it would accept every token twice over.
	maxClockSkew = 5 * time.Minute
	// defaultFetchTimeout bounds one attempt at obtaining the issuer's signing
	// keys, discovery included.
	defaultFetchTimeout = 5 * time.Second
	// defaultMinRefreshInterval is the shortest gap between two fetches of the
	// key set.
	//
	// Thirty seconds, which is a trade with two sides. It caps the amplification
	// an attacker gets from presenting unknown key identifiers at two requests a
	// minute per process, whatever rate they present them at. It also caps how
	// long this process stays blind to a key the issuer has just started signing
	// with — and thirty seconds is comfortably inside the overlap Keycloak keeps
	// a rotated key published for.
	defaultMinRefreshInterval = 30 * time.Second
)

// defaultAlgorithms is the allow-list applied when none is configured.
//
// It is "the asymmetric families", not one algorithm. What closes the confusion
// attack is asymmetry — that the key this service holds cannot produce a
// signature — and narrowing further to, say, RS256 alone buys nothing against
// that attack while buying an outage on the morning somebody rotates the realm
// to ES256. EdDSA is absent rather than forbidden: it is asymmetric and may be
// configured, and it is left out of the default because no realm this service
// is built for issues it.
var defaultAlgorithms = []string{
	jwa.RS256().String(), jwa.RS384().String(), jwa.RS512().String(),
	jwa.PS256().String(), jwa.PS384().String(), jwa.PS512().String(),
	jwa.ES256().String(), jwa.ES384().String(), jwa.ES512().String(),
}

// Config is everything an [Authenticator] needs. Named fields rather than a
// positional list, because half of these are durations and the compiler cannot
// tell a timeout from a skew.
type Config struct {
	// Issuer is the value a token's "iss" claim must carry, compared byte for
	// byte. For Keycloak that is <base>/realms/<realm>.
	//
	// Required, absolute, and refused with a trailing slash. The comparison is
	// exact and normalising it would be worse than refusing it: two spellings
	// of one issuer would then both be accepted here while the identity
	// provider only ever mints one of them, and the discovery URL built from it
	// would be a third.
	Issuer string
	// Audience is the value that must appear in a token's "aud" claim.
	//
	// Required. Keycloak does not put an API's own identifier in "aud" for a
	// client_credentials grant unless the realm says so with an audience
	// mapper, so this is the claim most likely to be missing from a realm that
	// was built without reading this.
	Audience string
	// JWKSURI is where the issuer publishes its signing keys. Empty means
	// discovery: the issuer's OpenID configuration document is read once and
	// its jwks_uri used.
	//
	// It exists as an escape hatch for the deployment where the issuer a token
	// names and the address this process can reach it at are not the same
	// string — a container published on a mapped port, most often — because
	// discovery insists that they are.
	JWKSURI string
	// HTTPClient is the transport used for discovery and for the key set.
	//
	// Required. http.DefaultClient would have been the convenient default and
	// is the wrong one: it has no timeout of its own, and it is shared with
	// every other caller in the process, so this package would have no say over
	// either the TLS trust it fetches keys under or the connection limits it
	// fetches them within.
	HTTPClient *http.Client
	// Algorithms is the signature algorithm allow-list, by JOSE name. Empty
	// means [defaultAlgorithms]. A symmetric algorithm, "none", or a name jwx
	// does not know is refused here, so an allow-list that could not close the
	// confusion attack cannot be built.
	Algorithms []string
	// ClockSkew is how far this process's clock may be from the issuer's before
	// a token is judged wrongly. Zero means [defaultClockSkew]; negative, and
	// anything past [maxClockSkew], is refused.
	ClockSkew time.Duration
	// FetchTimeout bounds one attempt at obtaining the key set, including the
	// discovery request when there is one. Zero means [defaultFetchTimeout].
	FetchTimeout time.Duration
	// MinRefreshInterval is the shortest gap between two fetches of the key set.
	// Zero means [defaultMinRefreshInterval]; negative is refused.
	MinRefreshInterval time.Duration
	// Clock is where time comes from.
	//
	// Optional, and the one dependency here that is. It is a seam so a test can
	// hold a token's expiry still, and the wall clock is the only answer
	// production has; requiring it would make every caller construct a value
	// with no decision in it.
	Clock app.Clock
}

// systemClock is the wall clock, in the resolution [app.Clock] asks for.
type systemClock struct{}

// Now reports the current time, UTC and truncated to microseconds.
func (systemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// settings is a Config that has been checked and had its defaults filled in, so
// that nothing downstream has to ask whether a value is present.
type settings struct {
	issuer       string
	audience     string
	jwksURI      string
	client       *http.Client
	algorithms   map[string]jwa.SignatureAlgorithm
	skew         time.Duration
	fetchTimeout time.Duration
	minInterval  time.Duration
	clock        app.Clock
}

// resolve checks the configuration and fills in what it left open.
func (c Config) resolve() (settings, error) {
	s := settings{
		issuer:       strings.TrimSpace(c.Issuer),
		audience:     strings.TrimSpace(c.Audience),
		jwksURI:      strings.TrimSpace(c.JWKSURI),
		client:       c.HTTPClient,
		skew:         c.ClockSkew,
		fetchTimeout: c.FetchTimeout,
		minInterval:  c.MinRefreshInterval,
		clock:        c.Clock,
	}

	if err := checkIssuer(s.issuer); err != nil {
		return settings{}, err
	}
	if s.audience == "" {
		return settings{}, errors.New("oidc: an authenticator needs an audience to require")
	}
	if s.jwksURI != "" {
		if err := checkAbsolute(s.jwksURI, "JWKS URI"); err != nil {
			return settings{}, err
		}
	}
	if s.client == nil {
		return settings{}, errors.New("oidc: an authenticator needs an HTTP client")
	}

	algorithms, err := checkAlgorithms(c.Algorithms)
	if err != nil {
		return settings{}, err
	}
	s.algorithms = algorithms

	switch {
	case s.skew < 0:
		return settings{}, fmt.Errorf("oidc: the clock skew must not be negative, got %s", s.skew)
	case s.skew > maxClockSkew:
		return settings{}, fmt.Errorf("oidc: the clock skew must be at most %s, got %s",
			maxClockSkew, s.skew)
	case s.skew == 0:
		s.skew = defaultClockSkew
	}
	switch {
	case s.fetchTimeout < 0:
		return settings{}, fmt.Errorf("oidc: the fetch timeout must not be negative, got %s",
			s.fetchTimeout)
	case s.fetchTimeout == 0:
		s.fetchTimeout = defaultFetchTimeout
	}
	switch {
	case s.minInterval < 0:
		return settings{}, fmt.Errorf(
			"oidc: the minimum refresh interval must not be negative, got %s", s.minInterval)
	case s.minInterval == 0:
		s.minInterval = defaultMinRefreshInterval
	}
	if s.clock == nil {
		s.clock = systemClock{}
	}
	return s, nil
}

// checkIssuer holds the issuer to what the "iss" comparison assumes of it.
func checkIssuer(issuer string) error {
	if issuer == "" {
		return errors.New("oidc: an authenticator needs an issuer")
	}
	if strings.HasSuffix(issuer, "/") {
		return fmt.Errorf("oidc: the issuer must not end in a slash, got %q", issuer)
	}
	return checkAbsolute(issuer, "issuer")
}

// checkAbsolute refuses anything this package could not turn into a request.
func checkAbsolute(raw, what string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("oidc: the %s is not a URL: %w", what, err)
	}
	if parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("oidc: the %s must be an absolute http or https URL, got %q", what, raw)
	}
	return nil
}

// checkAlgorithms turns the configured names into the table the verifier picks
// from, refusing anything that could not close the confusion attack.
//
// The refusal is here, at construction, rather than only at verification. An
// allow-list containing HS256 is not a deployment that occasionally accepts a
// forged token; it is a deployment that has no allow-list, and it should not
// start.
func checkAlgorithms(names []string) (map[string]jwa.SignatureAlgorithm, error) {
	if len(names) == 0 {
		names = defaultAlgorithms
	}
	allowed := make(map[string]jwa.SignatureAlgorithm, len(names))
	for _, name := range names {
		alg, ok := jwa.LookupSignatureAlgorithm(name)
		if !ok {
			return nil, fmt.Errorf("oidc: %q is not a signature algorithm", name)
		}
		if alg.IsSymmetric() || alg.String() == jwa.NoSignature().String() {
			return nil, fmt.Errorf(
				"oidc: %q may not be allowed: only asymmetric algorithms can tell this service's "+
					"tokens apart from ones it signed itself", name)
		}
		allowed[alg.String()] = alg
	}
	return allowed, nil
}
