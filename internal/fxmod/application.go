package fxmod

import (
	"log/slog"

	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// App is the two application services and the domain processor they decide
// with.
//
// Nothing in internal/app or internal/domain imports Fx, and this module is why
// it does not have to: the ports those packages declare are satisfied here, by
// values, and the services are built by ordinary constructor calls.
func App() fx.Option {
	return fx.Module("app",
		fx.Provide(
			newClock,
			newIDs,
			newDefects,
			newDivergences,
			newProcessor,
			newWagering,
			newWallets,
		),
	)
}

// newClock is where time comes from.
func newClock() app.Clock { return systemClock{} }

// newIDs is where identifiers come from.
func newIDs() app.IDs { return mintedIDs{} }

// newDefects is the hook for an operation this system could not carry forward.
func newDefects(logger *slog.Logger) app.DefectObserver {
	return loggedDefects{logger: logger}
}

// newDivergences is the hook for a wallet that does not balance.
func newDivergences(logger *slog.Logger) app.ReconciliationObserver {
	return loggedDivergences{logger: logger}
}

// newProcessor builds the domain's decision-maker with the wait budget an
// operation gets for a reference it depends on.
//
// Those two numbers are the domain's, not a worker's. How often the reference
// worker looks is [Reference]'s business and changing it cannot change whether
// an operation is eventually settled; how many attempts and how long it may
// wait at all is this, and changing it can.
func newProcessor(cfg config.Wagering) (*wagering.Processor, error) {
	policy, err := wagering.NewReferencePolicy(cfg.ReferenceAttempts, cfg.ReferenceTTL)
	if err != nil {
		return nil, err
	}
	return wagering.NewProcessor(policy)
}

// newWagering wires the write path, which the HTTP adapter and the consumer
// both call and the reference worker resumes through.
//
// Defects is a real observer and not a discarding one. app.NewWagering refuses
// nil deliberately, and its documentation says an adapter that genuinely wants
// no hook must pass one that discards and say so in its own code — this one does
// want the hook, because the condition it reports has no other channel at all.
func newWagering(
	tx *postgres.TxManager,
	processor *wagering.Processor,
	clock app.Clock,
	ids app.IDs,
	defects app.DefectObserver,
	cfg config.Wagering,
) (*app.Wagering, error) {
	return app.NewWagering(app.WageringDeps{
		Tx:        tx,
		Processor: processor,
		Clock:     clock,
		IDs:       ids,
		Backoff: app.BackoffPolicy{
			Initial: cfg.Backoff.Initial,
			Factor:  cfg.Backoff.Factor,
			Max:     cfg.Backoff.Max,
		},
		Defects: defects,
	})
}

// newWallets wires the wallet path.
//
// The observer is supplied rather than left nil, which the constructor permits.
// A divergence between a wallet's stored balance and its ledger is either a
// defect in this service or a write nobody made through it, and a reconciliation
// that found one and told nobody is a reconciliation that did not happen.
func newWallets(
	tx *postgres.TxManager,
	clock app.Clock,
	ids app.IDs,
	divergences app.ReconciliationObserver,
) (*app.Wallets, error) {
	return app.NewWallets(tx, clock, ids, divergences)
}
