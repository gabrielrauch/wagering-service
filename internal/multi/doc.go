// Package multi holds the suite that runs this service as more than one
// process: three API instances and two workers, with independent connections
// and independent memory, against one database and one pair of queues.
//
// Everything below is behind the build tag `multi`. This file carries none, so
// that the package exists for `go build` and `go vet` without one.
//
// # How to run it, and what it costs
//
//	make test-multi          # go test -race -tags multi -count=1 ./...
//	go test -race -tags multi -count=1 ./internal/multi/
//
// Nothing has to be started first. TestMain brings the compose stack up
// itself — `docker compose up --build --detach --wait` on the five services it
// needs — which is idempotent: a stack that is already healthy costs about two
// seconds, and a cold one costs the fifty the whole stack takes to start.
//
// Then it builds cmd/api and cmd/worker with the race detector, because the
// binaries this suite drives are the thing under test and a data race in one of
// them should fail this run rather than some later one. That is the largest
// fixed cost, around a minute cold and a few seconds against a warm build
// cache.
//
// The scenarios themselves take about a minute. The seven that run against the
// compose replicas run first, one at a time: four are over in under a second
// between them, the two outage scenarios pause a container each and spend the
// deployment's own timeouts noticing, and the simultaneous cross-transport one
// waits on a worker replica. The seven that build a world of their own then run
// in parallel, so their cost is the slowest of the seven — a killed consumer,
// waiting out the visibility timeout on a message its process died holding.
//
// Measured on this tree. Which of these you get depends on one thing — how much
// Compose and the Go build cache have to redo:
//
//   - About sixty-five seconds for this package on its own, with the stack up
//     and the images current — some twenty of them the two outage scenarios,
//     which pause a container and wait for the stack to answer again. That is
//     the ordinary case.
//   - About ninety-five when one delivery to the deployment's own queue is
//     swallowed. A receive left open on a worker container Compose replaced
//     takes the message and never answers for it, so it comes back at the
//     deployment's own thirty-second visibility timeout. That is LocalStack's
//     rather than this service's, it is the single biggest source of spread
//     here, and it is why the cross-transport scenario waits under a budget of
//     its own rather than under settleBudget.
//   - Seventy seconds to a minute and three quarters for `go test ./...`, which runs the
//     rest of the tree beside it — including the suites that start PostgreSQL
//     containers of their own.
//   - About ninety seconds whenever Compose rebuilds the images, which it does
//     on any run after a file the Dockerfile copies has changed. The test files
//     are among them, so an edit here costs a rebuild.
//   - About three minutes from nothing at all: no stack, no images, no build
//     cache. That one is normal rather than a hang, and it is worth saying so
//     before somebody interrupts it at two.
//
// # Two mechanisms, and why there are two
//
// The task this suite answers allows either the real binaries as subprocesses
// or the compose replicas. Both are here, because the scenarios divide cleanly
// in two and each half is cheaper and more honest under a different one.
//
// Seven scenarios need a process to die at a named instant, need the wait
// budget to be seconds rather than minutes, or need to write something the
// schema is entitled to refuse. Those build a world of their own: a database
// created and migrated for the scenario, three FIFO queues created for the
// scenario — inbound, outbound and the dead letter queue the first one redrives
// to — and processes this suite starts, arms with FAULT_POINT, kills and
// replaces. All five fault points are killed at here: the consumer on
// either side of its commit (before_commit and after_commit_before_ack), the
// publisher at either end of a send, and the reference worker after it parks
// an operation. A world is isolated from the compose stack and from every
// other world, so a publisher that must be one of exactly two is one of
// exactly two, and a message that must be received by the process that is
// about to die is not taken by a replica that is not.
//
// Seven scenarios need none of that, and for them the deployment itself is the
// better fixture: api-1, api-2 and api-3 on 8081, 8082 and 8083 are three
// genuinely separate containers with separate pools, and the two worker
// replicas are the ones a deployment runs. Those scenarios talk to the real
// published ports with a real token from the real Keycloak, and read the two
// worker replicas' own logs back out of Compose. Two of them take a dependency
// away — `docker compose pause` on the database, then on LocalStack — and
// watch what the deployment's own processes do without it and once it is back;
// they are sequential, because a paused database is paused for everybody, and
// each restores the stack in a cleanup whether it passed or not.
//
// What the split gives up is stated plainly. The seven isolated scenarios do
// not exercise the container image, the health checks or the compose network;
// they exercise the same binaries on the host. The seven deployment scenarios
// do not get a private database, so they are careful to identify everything
// they assert on by a run-scoped identifier rather than by counting rows.
//
// # This is not a fourth copy of the PostgreSQL harness
//
// internal/adapters/postgres, internal/integration and internal/messaging each
// carry the same two hundred lines of testcontainers plumbing, and each says
// why it was not factored out. This package imports testcontainers-go not at
// all. It owns no container: it asks Compose for the deployment and then
// creates a database and three queues on what Compose started. The one thing it
// shares with those three in spirit — a database per test, dropped on success
// and kept on failure — is about sixty lines here rather than two hundred,
// because there is no cluster to start, no schema template to clone and nothing
// to share between tests that a `CREATE DATABASE` does not already give.
//
// What it does add, and what nothing else in this tree has, is process
// supervision: launch starts a real binary, journal reads its structured log
// back, and process.awaitExit asserts on the status it died with.
//
// # What is real
//
// All of it, and more of it than anywhere else here. PostgreSQL 16, Keycloak
// 26.4 with this repository's own realm, LocalStack with this repository's own
// queue definitions, the api and worker binaries built from this tree, tokens
// obtained by a genuine client_credentials grant, and submissions that cross a
// real listener and a real FIFO queue. Nothing stands in for anything.
//
// The two wire types this package declares — submission and envelope — are its
// own rather than the adapters'. That is the whole point of
// TestOneOperationOverHTTPAndOverTheQueueSettlesOnceInEitherOrder: the HTTP
// door takes the idempotency key in a HEADER and the queue envelope takes it as
// a MEMBER, and a suite that built both out of the structs those adapters
// decode into could not notice one of them moving.
//
// # Proving a replay was received rather than inferred
//
// Every scenario about a submission arriving twice asserts that the second
// arrival happened, not merely that the balance is what one arrival would
// leave. Three instruments do that, and which one is used depends on where the
// second arrival landed:
//
//   - Over HTTP, the answer itself. `idempotentReplay` is true exactly when the
//     use case found a stored outcome, so forty-nine of them is forty-nine
//     submissions that arrived and were recognised.
//   - Over the queue, the consumer's own line — "the operation was applied" —
//     which carries `replay` and the queue's `receiveCount`. A receiveCount of
//     two is the queue saying it delivered the message again.
//   - In the database, the inbox row keyed (consumer_name, message_id) with its
//     completed_at, and the outbox row's `attempts`, which counts claims and so
//     counts republications.
//
// # What this suite found
//
// One defect, and it is the best argument for the suite existing. Fifty
// concurrent submissions of one operation under one idempotency key were
// answered 409 CONFLICT between one and six times, for an operation recorded
// under exactly the key they sent — because Wagering.replay's two lookups do
// not share a snapshot on the write path, which is READ COMMITTED by design.
//
// The wallet was right either way. That is why nothing before this noticed, and
// it is the whole point: this was the first test here to ask whether all fifty
// callers got a usable ANSWER rather than whether the money came out right.
//
// Fixed in c941786, which reads the key off the row the second lookup found
// instead of inferring whose it is from which index found it. The interleaving
// is now held deterministically in internal/app/idempotency_test.go, driven by
// a step on the fake rather than by fifty goroutines racing; this package holds
// the same contract end to end across three processes that share nothing but a
// database. Both are worth having, and neither replaces the other. See the
// documentation on TestOneBetSubmittedFiftyTimesAcrossThreeInstancesDebitsOnce.
package multi
