package httpapi

import (
	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/app"
)

// What a composition root will actually hand this package.
//
// The interfaces here are declared on the consumer's side, which means nothing
// in internal/app or in the other adapters knows it is satisfying them — so the
// only thing standing between a renamed method and a composition root that will
// not build is an assertion like this one, made where the interfaces live
// rather than in the wiring that is not written yet.
var (
	_ WageringService = (*app.Wagering)(nil)
	_ WalletService   = (*app.Wallets)(nil)
	_ Authenticator   = (*oidc.Authenticator)(nil)
	_ ReadinessCheck  = (*postgres.Health)(nil)
)
