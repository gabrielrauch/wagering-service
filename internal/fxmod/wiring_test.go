package fxmod

import (
	httpapi "github.com/gabrielrauch/wagering-service/internal/adapters/http"
	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/workers"
)

// The seams this package joins, asserted at compile time.
//
// They belong here and could not be anywhere else: internal/workers imports
// both adapters, so neither adapter can assert that it satisfies one of that
// package's interfaces, and internal/app declares no interface for a publisher
// at all. This is the one package that sees every side, which is also why a
// change to any of these contracts should be a build failure here rather than a
// wiring failure the first time a process starts.
//
// No build tag. A graph that will not assemble is not an integration concern.
var (
	_ workers.InboundQueue  = inboundQueue{}
	_ workers.OutboundQueue = outboundQueue{}
	_ workers.OutboxClaims  = (*postgres.OutboxClaims)(nil)
	_ workers.Submitter     = (*app.Wagering)(nil)
	_ workers.Resumer       = (*app.Wagering)(nil)

	_ httpapi.WageringService = (*app.Wagering)(nil)
	_ httpapi.WalletService   = (*app.Wallets)(nil)
	_ httpapi.Authenticator   = (*oidc.Authenticator)(nil)
	_ httpapi.ReadinessCheck  = (*postgres.Health)(nil)
	_ httpapi.ReadinessCheck  = (*sqs.Health)(nil)
	_ httpapi.ReadinessCheck  = queueReadiness{}

	_ app.Clock                  = systemClock{}
	_ app.IDs                    = mintedIDs{}
	_ app.DefectObserver         = loggedDefects{}
	_ app.ReconciliationObserver = loggedDivergences{}
	_ app.TxManager              = (*postgres.TxManager)(nil)
)
