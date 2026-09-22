package fxmod

import (
	"net/http"

	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/config"
)

// OIDC is the verifier that turns a bearer credential into the principal an
// operation is submitted under. cmd/api only: nothing a worker does is
// authenticated, because a message on the inbound queue was authorised by
// whatever put it there.
func OIDC() fx.Option {
	return fx.Module("oidc",
		fx.Provide(
			newIdentityClient,
			newAuthenticator,
		),
	)
}

// identityClient is the transport the key set is fetched over.
//
// A named type rather than a bare *http.Client, for the reason the two queue
// types give: *http.Client is the most general type in the standard library,
// and a second module wanting one would either collide with this provider or
// silently inherit a client configured for a different job — this one's timeout
// is OIDC_HTTP_TIMEOUT and has nothing to say about anybody else's calls.
type identityClient struct{ *http.Client }

// newIdentityClient builds it.
//
// The adapter requires one and refuses to default, and the reason is worth
// keeping in sight here, where the default would have been written:
// http.DefaultClient has no timeout of its own and is shared with every other
// caller in the process, so key fetching would inherit both whatever trust and
// whatever connection limits somebody else had configured.
//
// Redirects are not disabled here. The adapter copies this client and takes
// them off its own copy, because a fetch that followed a redirect would make
// its origin checks guard an address rather than a destination — and that is not
// a property it is willing to leave to a client it was handed.
func newIdentityClient(cfg config.OIDC) identityClient {
	return identityClient{&http.Client{Timeout: cfg.HTTPTimeout}}
}

// newAuthenticator wires the verifier and makes fetching the key set a start-up
// check.
//
// Construction performs no I/O by design, so this is the one place reaching the
// identity provider is allowed to fail: an issuer nobody can reach, a realm that
// does not exist and a JWKS document with nothing usable in it all stop the
// process here rather than becoming a wall of 401s at the first request. OnStart
// bounds itself by the configured fetch timeout.
func newAuthenticator(
	lc fx.Lifecycle, cfg config.OIDC, client identityClient,
) (*oidc.Authenticator, error) {
	authenticator, err := oidc.NewAuthenticator(oidc.Config{
		Issuer:             cfg.Issuer,
		Audience:           cfg.Audience,
		JWKSURI:            cfg.JWKSURI,
		HTTPClient:         client.Client,
		Algorithms:         cfg.Algorithms,
		ClockSkew:          cfg.ClockSkew,
		FetchTimeout:       cfg.FetchTimeout,
		MinRefreshInterval: cfg.MinRefreshInterval,
		Clock:              systemClock{},
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStart: authenticator.OnStart})
	return authenticator, nil
}
