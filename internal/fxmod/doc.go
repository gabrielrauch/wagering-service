// Package fxmod is the composition root: the one place that knows every
// package in this tree, and the only one that imports a dependency-injection
// framework.
//
// The domain, the application layer, the adapters and the workers are all free
// of Fx by design and say so in their own documentation. What that buys is that
// each of them can be built by hand in a test — and every one of them is — so
// this package's failure mode is a graph that will not assemble rather than a
// component that cannot be exercised without a container.
//
// # The modules
//
//   - config supplies the checked [config.Config] and hands each module the
//     part of it that module reads.
//   - telemetry is the logger, and the place the OpenTelemetry SDK plugs into.
//   - postgres is the pool, the transaction manager, the outbox claims and the
//     readiness probe.
//   - sqs is the client and the two queues, each resolved at start-up.
//   - oidc is the token verifier, whose key set is fetched at start-up.
//   - app is the two application services, the domain processor they decide
//     with, and the four ports — clock, identifiers, defects, divergences —
//     that only a composition root can supply.
//   - httpserver is the API and the server carrying it. cmd/api only.
//   - consumer, outbox and reference are the three loops. cmd/worker only, and
//     each is included or left out by its own environment variable.
//
// # Start-up
//
// Three dependencies are validated before this process claims to be running,
// each bounded by its own timeout rather than only by the lifecycle's:
//
//   - PostgreSQL, by the same readiness probe /health/ready runs, so that a
//     process reporting itself up has already answered the question the
//     orchestrator is about to ask.
//   - SQS, by resolving each queue's name to its URL, so that a queue nobody
//     provisioned is a start-up failure with an operator watching rather than a
//     consumer that receives nothing and says nothing.
//   - the identity provider, by fetching the key set, so that a realm that does
//     not exist is a start-up failure rather than a wall of 401s.
//
// A hook that fails stops the start, Fx rolls back the hooks that had already
// run, and the process exits. Nothing starts half up.
//
// # Shutdown, and why nothing here orders it by hand
//
// Fx runs OnStop hooks in the reverse of the order they were appended, and this
// package relies on that deliberately rather than hand-rolling a sequence. What
// makes it reliable is that the order hooks are appended in is not a convention
// anybody has to maintain: a constructor cannot run before the constructors it
// depends on, so appending each hook where its component is built makes the
// dependency graph itself the ordering.
//
// The chain is [newLogger] -> [newPool] -> the transaction manager and the
// outbox claims -> the application services -> the loops or the server. Reversed,
// that is:
//
//	the server drains, or the loops stop and give their work back
//	  -> the pool closes
//	    -> telemetry is flushed
//
// Each of those is the only order that works. A pool closed while a publisher
// is still releasing its claims leaves those rows held until their hold expires,
// and because the outbox is head-of-line per wallet, each held row is a wallet's
// whole event stream waiting with it. Telemetry flushed before the components
// that emit into it have stopped loses the last thing they said, which is the
// part of a shutdown anybody reads.
//
// Every worker's Stop reports what it did not finish, and [stopWorker] carries
// that report out of the hook rather than logging it and returning nil: an Fx
// Stop that returns an error is a non-zero exit code, and a drain that ran out
// of time should not look like a clean deployment.
//
// # What telemetry is, today
//
// The logger and the flush hook's position. The OpenTelemetry SDK is a later
// step, and this package deliberately does not invent a shape for it: there is
// no exporter interface here, no provider abstraction, and nothing to implement.
// What is here is where it goes — see [Telemetry] and [telemetry.flush].
package fxmod
