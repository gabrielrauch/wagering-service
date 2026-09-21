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
// The scenarios themselves take about twenty seconds. The four that run against
// the compose replicas run first and are over in under a second between them;
// the six that build a world of their own run in parallel, and are bounded by
// the slowest — the killed consumer, waiting out the visibility timeout on a
// message its process died holding.
//
// Measured on this tree: forty-three seconds wall clock with the stack up and
// the build cache warm, about ninety when Compose has to rebuild the images —
// which it does on any run where a file the Dockerfile copies has changed. From
// nothing at all, three minutes.
//
// # Two mechanisms, and why there are two
//
// The task this suite answers allows either the real binaries as subprocesses
// or the compose replicas. Both are here, because the scenarios divide cleanly
// in two and each half is cheaper and more honest under a different one.
//
// Five scenarios need a process to die at a named instant, or need the wait
// budget to be seconds rather than minutes. Those build a world of their own:
// a database created and migrated for the scenario, a pair of FIFO queues
// created for the scenario, and processes this suite starts, arms with
// FAULT_POINT, kills and replaces. A world is isolated from the compose stack
// and from every other world, so a publisher that must be one of exactly two is
// one of exactly two, and a message that must be received by the process that
// is about to die is not taken by a replica that is not.
//
// Four scenarios need none of that, and for them the deployment itself is the
// better fixture: api-1, api-2 and api-3 on 8081, 8082 and 8083 are three
// genuinely separate containers with separate pools, and the two worker
// replicas are the ones a deployment runs. Those scenarios talk to the real
// published ports with a real token from the real Keycloak, and read the two
// worker replicas' own logs back out of Compose.
//
// What the split gives up is stated plainly. The five isolated scenarios do not
// exercise the container image, the health checks or the compose network; they
// exercise the same binaries on the host. The four deployment scenarios do not
// get a private database, so they are careful to identify everything they
// assert on by a run-scoped identifier rather than by counting rows.
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
// # What this suite finds today
//
// One of the ten scenarios does not pass, and the failure is the service's
// rather than the suite's: fifty concurrent submissions of one operation under
// one idempotency key are answered 409 CONFLICT between two and six times, for
// an operation recorded under exactly the key they sent. The wallet is right
// either way, which is why nothing before this noticed. See the documentation
// on TestOneBetSubmittedFiftyTimesAcrossThreeInstancesDebitsOnce for where it
// comes from.
package multi
