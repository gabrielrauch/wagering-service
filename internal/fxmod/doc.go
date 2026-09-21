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
// run, and the process exits.
//
// All three are registered before any loop's Start hook, which does not happen
// on its own: Fx appends hooks in the order it constructs things, and the
// consumer module builds its queue and then appends its own Start, so the
// OUTBOUND queue would be resolved after the consumer had begun handling
// messages. [checks] is what orders them, and it is why "nothing starts half
// up" is a true sentence about this package rather than a hopeful one.
//
// Not every failure to reach a queue stops every process, and the line is the
// adapter's own classification rather than a judgement made here: a queue that
// does not exist is Unretryable and stops both binaries, while a queue that is
// momentarily unreachable is Retryable and stops only the binary that has
// nowhere to report it. See [queueStartUp], which is the whole of that
// reasoning.
//
// A loop is started on a context that outlives the start-up budget, not on the
// one the hook is handed. All three root their run context in whatever they are
// given, so a loop started on the start-up context stops receiving the moment
// START_TIMEOUT expires — seconds after the process came up, silently, with
// every Stop afterwards reporting a clean shutdown of a loop that had been dead.
// See [outlivesStartUp].
//
// # Shutdown, and why nothing here orders it by hand
//
// Fx runs OnStop hooks in the reverse of the order they were appended, and this
// package relies on that deliberately rather than hand-rolling a sequence.
// Three facts make the append order something nobody has to maintain, and it is
// worth being exact about which, because the obvious argument — that every
// hook-appending constructor takes the logger — is not true: four of them do
// not.
//
//  1. Only four things here append an OnStop hook at all: [newLogger],
//     [newPool], the three run* invokes and [serve]. The constructors that do
//     not take the logger — [newDatabaseHealth], [newInboundQueue],
//     [newOutboundQueue], [newAuthenticator] — append OnStart and nothing else,
//     and Fx skips a nil OnStop, so where they sit cannot matter.
//  2. [newLogger] is forced during fx.New by fx.WithLogger, before any invoke
//     runs and before any other constructor is asked for. Its hook is therefore
//     the first appended and the last run, unconditionally — a stronger
//     guarantee than depending on it would give.
//  3. Everything that appends an OnStop hook after that is built FROM the pool:
//     the loops through the application services and the outbox claims, the
//     server through the API. A constructor cannot run before the constructors
//     it depends on, so [newPool] runs before all of them and its hook runs
//     after all of theirs.
//
// The three run* hooks and [serve]'s are appended from fx.Invoke rather than
// from a constructor, and that is not in tension with the above: they are the
// leaves of the graph, nothing depends on them, so an invoke is the only thing
// that can force them into existence — and their position is fixed anyway by
// the fact that their dependencies were constructed first.
//
// Reversed, the order is:
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
