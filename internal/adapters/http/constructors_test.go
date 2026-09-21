package httpapi

import (
	"context"
	"testing"
	"time"
)

func TestNewRefusesAnAPIItCannotAnswerWith(t *testing.T) {
	t.Parallel()

	complete := func() Config {
		return Config{
			Wagering:         &fakeWagering{},
			Wallets:          &fakeWallets{},
			Authenticator:    &fakeAuthenticator{},
			Readiness:        map[string]ReadinessCheck{"postgres": &fakeCheck{}},
			ReadinessTimeout: time.Second,
			MaxBodyBytes:     testBodyLimit,
			Logger:           discard(),
		}
	}

	cases := []struct {
		name   string
		damage func(cfg *Config)
	}{
		{"no wagering service", func(cfg *Config) { cfg.Wagering = nil }},
		{"no wallet service", func(cfg *Config) { cfg.Wallets = nil }},
		{"no authenticator", func(cfg *Config) { cfg.Authenticator = nil }},
		{"no logger", func(cfg *Config) { cfg.Logger = nil }},
		{"no body limit", func(cfg *Config) { cfg.MaxBodyBytes = 0 }},
		{"no readiness budget", func(cfg *Config) { cfg.ReadinessTimeout = 0 }},
		{"a readiness check that is nothing", func(cfg *Config) {
			cfg.Readiness = map[string]ReadinessCheck{"sqs": nil}
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg := complete()
			c.damage(&cfg)

			// Refused at construction rather than at the first request, because
			// by the first request a provider is waiting.
			if _, err := New(cfg); err == nil {
				t.Fatal("an API was built without it")
			}
		})
	}

	if _, err := New(complete()); err != nil {
		t.Fatalf("a complete configuration was refused: %v", err)
	}
}

// A handler that ignored cancellation would keep a database transaction open
// for a caller that has already gone.
func TestACancelledRequestIsCancelledAllTheWayDown(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	ctx, cancel := context.WithCancel(t.Context())
	h.doWithContext(t, ctx, submission(submitBody), cancel)

	if err := h.wagering.lastContext.Err(); err == nil {
		t.Error("the use case was given a context that was still live")
	}
}

func TestTheRequestsContextReachesTheUseCaseLive(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	h.do(t, submission(submitBody))

	if err := h.wagering.lastContext.Err(); err != nil {
		t.Errorf("the use case was given a context that was already done: %v", err)
	}
}
