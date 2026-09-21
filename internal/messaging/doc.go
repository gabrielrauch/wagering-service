// Package messaging holds the integration suite for the message path: the SQS
// consumer, the outbox publisher and the reference worker, running as loops
// against a real LocalStack and a real PostgreSQL.
//
// # Why it is a package of its own
//
// It tests a seam, exactly as internal/integration does, and a different one.
// That suite stands outside the process with a bearer token and asks what a
// provider sees; this one stands where a queue message lands and asks what a
// wallet, an inbox row and an outbox row come to. Put in internal/workers these
// would be that package's suite asserting things about two adapters and the
// application layer it deliberately knows nothing about; put in either
// adapter's suite they would be one adapter asserting on the other.
//
// It is a sibling of internal/integration rather than more files inside it, and
// that is the one decision here worth arguing. The cost of the sibling is a
// third copy of the PostgreSQL harness — see the note in main_test.go, which
// says what should be done about it. What the alternative would have bought is
// less than it looks: that suite's newStack builds an HTTP server and an
// authenticator against a real Keycloak, and not one scenario below
// authenticates anything. A message on the queue is authorised by the queue
// itself, and the scenario that also submits over the application layer's own
// door does so as a provider principal rather than as a token — see
// stack.submitDirectly for what that keeps and what it gives up. So extending
// that package would have meant a second stack builder inside it either way,
// plus an identity provider in the start-up path of ten tests with no identity
// in them, plus a doc comment there that no longer describes what the package
// holds.
//
// The harness duplication is the real price and it is stated plainly rather
// than argued away. It is also the smaller of the two: the copy is inert and
// mechanical, where a suite named for end-to-end HTTP quietly becoming the home
// of the queue tests is the kind of drift nobody notices until the file is
// three thousand lines long.
//
// # What is real
//
// All of it. PostgreSQL 16 with the eight migrations applied and the service
// pool connected as wagering_app; LocalStack provisioned by this repository's
// own deploy/localstack/01-queues.sh; the real sqs.Queue, the real
// postgres.TxManager and postgres.OutboxClaims, the real app.Wagering and
// app.Wallets over the domain's own processor, and the real workers.Consumer,
// workers.Publisher and workers.ReferenceWorker driven through Start and Stop.
// Nothing here stands in for a database or for a queue.
//
// Two decorators do sit on the path, and neither replaces anything: watchedQueue
// forwards every call to the real queue and records what passed, and
// watchedSubmitter forwards every submission to the real use case and records
// what came back. They exist because several of these scenarios are about
// something that happened and left no row — a duplicate that was genuinely
// received, a message whose visibility was reset — and a suite that asserted
// only on the final balance would pass with that behaviour deleted, which is
// the failure this suite was written after.
//
// # What this suite cannot catch
//
// One thing, and it is worth naming at the top rather than leaving to be
// inferred. Nothing here drives the HTTP adapter: the submission that does not
// arrive on the queue reaches app.Wagering.Submit directly, so the handler's
// own half — the Idempotency-Key HEADER, the strict decode of a body that
// deliberately has no such member, the correlation it derives — is not
// exercised. The bug class that leaves open is the HTTP door and the queue
// envelope disagreeing about where the idempotency key lives, or a member
// renamed on one side only. internal/integration drives that door over a real
// listener with a token a real Keycloak issued; reaching it from here would
// mean something standing in at the credential gate, which is the substitute
// this tree's brief refuses. See the documentation on
// TestOneOperationWithAndWithoutAnInboxIdentityMovesMoneyOnceAndReplaysOnce.
//
// This file carries no build tag so that the package exists for `go build` and
// `go vet` without one. Everything else here is behind `integration`.
package messaging
