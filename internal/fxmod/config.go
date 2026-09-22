package fxmod

import (
	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/config"
)

// Config supplies the configuration and hands each module the part of it that
// module reads.
//
// The value arrives already checked, from [config.Load], rather than being read
// from the environment by a provider in here. That is the difference between a
// process that refuses to start with every wrong variable named and one that
// gets as far as building a container before saying so — and it is what lets
// [Worker] decide which loops exist from the configuration, which a value
// produced inside the graph could not.
//
// It is decomposed rather than injected whole so that each constructor's
// signature names what it reads. A module taking the entire configuration would
// compile against every field in it, and "what does the SQS module depend on"
// would stop having an answer the compiler could give.
func Config(cfg config.Config) fx.Option {
	return fx.Module("config",
		fx.Supply(cfg),
		fx.Provide(
			telemetryConfig,
			postgresConfig,
			queueConfig,
			identityConfig,
			serverConfig,
			wageringConfig,
			consumerConfig,
			publisherConfig,
			referenceConfig,
		),
	)
}

func telemetryConfig(cfg config.Config) config.Telemetry { return cfg.Telemetry }
func postgresConfig(cfg config.Config) config.Postgres   { return cfg.Postgres }
func queueConfig(cfg config.Config) config.SQS           { return cfg.SQS }
func identityConfig(cfg config.Config) config.OIDC       { return cfg.OIDC }
func serverConfig(cfg config.Config) config.HTTP         { return cfg.HTTP }
func wageringConfig(cfg config.Config) config.Wagering   { return cfg.Wagering }
func consumerConfig(cfg config.Config) config.Consumer   { return cfg.Consumer }
func publisherConfig(cfg config.Config) config.Publisher { return cfg.Publisher }
func referenceConfig(cfg config.Config) config.Reference { return cfg.Reference }
