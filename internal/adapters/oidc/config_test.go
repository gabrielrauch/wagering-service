package oidc

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
)

// valid is the smallest configuration that builds, which every case below
// spoils in exactly one way.
func valid() Config {
	return Config{
		Issuer:     "https://idp.example/realms/wagering",
		Audience:   "wagering-api",
		HTTPClient: &http.Client{},
	}
}

func TestNewAuthenticatorRefusesAConfigurationItCannotHonour(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		spoil  func(*Config)
		saying string
	}{
		{
			name:   "no issuer",
			spoil:  func(c *Config) { c.Issuer = "  " },
			saying: "needs an issuer",
		},
		{
			name:   "an issuer that is not absolute",
			spoil:  func(c *Config) { c.Issuer = "/realms/wagering" },
			saying: "must be an absolute http or https URL",
		},
		{
			name:   "an issuer that is not a URL at all",
			spoil:  func(c *Config) { c.Issuer = "https://idp.example/\x7f" },
			saying: "is not a URL",
		},
		{
			name:   "an issuer ending in a slash",
			spoil:  func(c *Config) { c.Issuer += "/" },
			saying: "must not end in a slash",
		},
		{
			name:   "no audience",
			spoil:  func(c *Config) { c.Audience = "" },
			saying: "needs an audience",
		},
		{
			name:   "no HTTP client",
			spoil:  func(c *Config) { c.HTTPClient = nil },
			saying: "needs an HTTP client",
		},
		{
			name:   "a JWKS URI that is not absolute",
			spoil:  func(c *Config) { c.JWKSURI = "certs" },
			saying: "must be an absolute http or https URL",
		},
		{
			name:   "an HMAC algorithm on the allow-list",
			spoil:  func(c *Config) { c.Algorithms = []string{"RS256", "HS256"} },
			saying: "only asymmetric algorithms",
		},
		{
			name:   "no signature at all on the allow-list",
			spoil:  func(c *Config) { c.Algorithms = []string{"none"} },
			saying: "only asymmetric algorithms",
		},
		{
			name:   "an algorithm nobody has heard of",
			spoil:  func(c *Config) { c.Algorithms = []string{"RS255"} },
			saying: "is not a signature algorithm",
		},
		{
			name:   "a negative clock skew",
			spoil:  func(c *Config) { c.ClockSkew = -time.Second },
			saying: "must not be negative",
		},
		{
			name:   "a clock skew wider than a token lives",
			spoil:  func(c *Config) { c.ClockSkew = maxClockSkew + time.Second },
			saying: "must be at most",
		},
		{
			name:   "a negative fetch timeout",
			spoil:  func(c *Config) { c.FetchTimeout = -time.Second },
			saying: "must not be negative",
		},
		{
			name:   "a negative refresh interval",
			spoil:  func(c *Config) { c.MinRefreshInterval = -time.Second },
			saying: "must not be negative",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid()
			c.spoil(&cfg)

			authenticator, err := NewAuthenticator(cfg)
			if err == nil {
				t.Fatal("expected the configuration to be refused")
			}
			if authenticator != nil {
				t.Error("a refused configuration produced an authenticator")
			}
			if !strings.Contains(err.Error(), c.saying) {
				t.Errorf("err = %v, want it to mention %q", err, c.saying)
			}
		})
	}
}

func TestResolveFillsInWhatTheConfigurationLeftOpen(t *testing.T) {
	t.Parallel()

	resolved, err := valid().resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if resolved.skew != defaultClockSkew {
		t.Errorf("skew = %s, want %s", resolved.skew, defaultClockSkew)
	}
	if resolved.fetchTimeout != defaultFetchTimeout {
		t.Errorf("fetch timeout = %s, want %s", resolved.fetchTimeout, defaultFetchTimeout)
	}
	if resolved.minInterval != defaultMinRefreshInterval {
		t.Errorf("refresh interval = %s, want %s", resolved.minInterval, defaultMinRefreshInterval)
	}
	if resolved.clock == nil {
		t.Error("no clock was filled in")
	}
	if len(resolved.algorithms) != len(defaultAlgorithms) {
		t.Errorf("allow-list holds %d algorithms, want %d",
			len(resolved.algorithms), len(defaultAlgorithms))
	}
	for _, refused := range []jwa.SignatureAlgorithm{
		jwa.HS256(), jwa.HS384(), jwa.HS512(), jwa.NoSignature(),
	} {
		if _, allowed := resolved.algorithms[refused.String()]; allowed {
			t.Errorf("the default allow-list admits %q", refused)
		}
	}
}

// TestNewAuthenticatorTouchesNoNetwork keeps the failure of an unreachable
// issuer where something can report it. Construction happens while a process is
// wiring itself up; OnStart happens where a startup failure is expected.
func TestNewAuthenticatorTouchesNoNetwork(t *testing.T) {
	t.Parallel()

	iss := newIssuer(t, rsaKey(t, "rotation-1"))
	if _, err := NewAuthenticator(Config{
		Issuer:     iss.url(),
		Audience:   testAudience,
		HTTPClient: iss.server.Client(),
	}); err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}

	if got := iss.certRequests.Load() + iss.discoveryRequests.Load(); got != 0 {
		t.Errorf("construction made %d request(s) to the issuer, want none", got)
	}
}

// TestSystemClockAnswersInTheResolutionThePortAsksFor: [app.Clock] promises UTC
// truncated to microseconds, and the default here is an implementation of it.
func TestSystemClockAnswersInTheResolutionThePortAsksFor(t *testing.T) {
	t.Parallel()

	now := systemClock{}.Now()
	if now.Location() != time.UTC {
		t.Errorf("location = %s, want UTC", now.Location())
	}
	if now.Truncate(time.Microsecond) != now {
		t.Errorf("now = %s, want it truncated to microseconds", now)
	}
}
