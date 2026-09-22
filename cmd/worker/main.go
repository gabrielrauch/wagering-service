// Command worker runs the three loops that sit beside the API: the consumer
// that takes wager operations off the inbound queue, the publisher that puts
// committed events on the outbound one, and the reference worker that carries
// parked operations forward.
//
// It takes no arguments. Each loop is switched on or off by its own variable —
// CONSUMER_ENABLED, PUBLISHER_ENABLED, REFERENCE_WORKER_ENABLED, all true by
// default — and a loop that is off is not built at all, so a worker running only
// the reference loop needs no queue. Switching all three off is refused: a
// process holding a pool open and consuming nothing looks exactly like a
// healthy deployment while its queues fill.
//
// Exit codes. Nought, one and two are cmd/migrate's:
//
//	0  started, ran and stopped cleanly
//	1  the graph would not build, or a dependency could not be reached at
//	   start-up
//	2  the environment is not one this service can be configured from
//	3  it ran, and then failed to give its work back inside STOP_TIMEOUT
//
// An interrupt or a SIGTERM begins the shutdown: each loop stops receiving at
// once and drains what it has in hand within its own drain timeout, handing
// back whatever it did not finish — messages released for immediate
// redelivery, outbox claims returned — then the database pool closes, then
// telemetry is flushed. A loop that ran out of drain time says so, and the
// process exits 3 rather than pretending the deployment was clean.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/fxmod"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(fxmod.Run(ctx, os.Stderr, "worker", config.Environment, fxmod.Worker))
}
