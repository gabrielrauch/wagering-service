// Package sqs is the queue side of this service: the client the consumer
// receives wager operations through, and the one the outbox publisher sends
// wallet events through.
//
// It is an adapter and nothing more. There is no consume loop, no publish loop
// and no schedule here — those live in internal/workers, where the transaction
// boundaries they turn on are visible. What this package owns is the six calls
// those loops make, the bounds SQS applies to them, and the translation of an
// AWS failure into the [app.Class] a worker branches on.
//
// # One type for both directions
//
// A [Queue] is a handle on one queue and can do everything SQS lets you do to
// one, because an SQS queue is symmetric — the same API receives and sends. The
// composition root builds two: the inbound one it hands the consumer, and the
// outbound one it hands the publisher. Splitting the type in two would have
// duplicated the resolution, the classification and the readiness probe to
// express a distinction that lives in the wiring, not in the queue.
//
// # Nothing talks to AWS during construction
//
// [NewQueue] performs no I/O; [Queue.OnStart] resolves the queue's name to its
// URL, once, bounded by its own timeout. That split is why a queue name nobody
// provisioned is a start-up failure with an operator watching, rather than a
// consumer that receives nothing and says nothing. Every other method refuses
// to run before it, so a missed OnStart cannot become a request against an
// empty URL.
//
// # Ordering and deduplication are the caller's to state
//
// Both queues are FIFO and neither has content-based deduplication, so every
// send names its own group and its own deduplication id. The adapter refuses a
// message that omits either. What those ids are is the system's contract and is
// written down in deploy/localstack/01-queues.sh, next to the queues it
// provisions: inbound, the group is the wallet id and the deduplication id is
// the envelope's message id; outbound, the group is the aggregate id and the
// deduplication id is the event id.
//
// # What a send reports
//
// SendMessageBatch succeeds partially — some entries accepted, some refused, in
// one 200 response — so [Queue.SendBatch] reports per entry rather than per
// call. A publisher must mark exactly the events that reached the queue, and a
// single error for the call would force it to choose between marking events
// that were refused and republishing events that were not.
//
// # Errors
//
// Every failure leaving this package is classified: throttling, a request that
// never reached a server, a timeout and a caller's own cancellation are
// [app.Retryable]; a malformed request and a refusal the service will repeat
// are [app.Unretryable]. Anything this package does not recognise is
// Unretryable, for the reason the PostgreSQL adapter gives: a failure nobody
// recognised is not one anybody has established is safe to repeat.
//
// # Credentials
//
// [NewClient] takes none. It loads the AWS SDK's default chain — environment,
// profile, instance role — so no credential is ever named in this service's
// configuration, written to a log line, or carried in an error.
package sqs
