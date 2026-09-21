// Package workers is the three loops that run beside the API: the consumer that
// takes wager operations off the inbound queue, the publisher that puts wallet
// events on the outbound one, and the reference worker that carries parked
// operations forward.
//
// Each of them is a loop around a call the adapters and the application layer
// already own. Nothing here parses money, decides an outcome, opens a
// transaction or talks to AWS or PostgreSQL directly; what this package owns is
// when a call is made, how many are made at once, what happens to the work when
// one fails, and what is given back when the process is asked to stop. That is
// also why the loops are here rather than in the adapters — the transaction
// boundary each of them turns on is only visible from where the call is made.
//
// There is no dependency-injection framework in this package. Every worker
// takes its dependencies as named fields, refuses the ones it cannot work
// without at construction, does no work until [Consumer.Start],
// [Publisher.Start] or [ReferenceWorker.Start], and stops within a deadline it
// was given. A composition root drives all three the same way.
//
// # Ordering
//
// The inbound queue is FIFO grouped by wallet id, so a wallet's operations must
// not overtake each other: a rollback that was sent after the bet it undoes must
// be handled after it. Concurrency here is therefore across groups and never
// within one, and it is bought in the one way that does not require this package
// to know which group a message belongs to.
//
// A consumer runs N independent receivers. Each receives its own batch and
// handles that batch strictly in the order it arrived, one message at a time; N
// is the bound on how many messages this instance handles at once. Two receivers
// cannot be given the same group, because SQS holds a group for whoever is
// holding one of its messages in flight. The group is never named in this
// package and never has to be — which matters, because the queue adapter does
// not carry MessageGroupId and could not be asked to without this package
// deciding what it would do with it.
//
// The same fact settles what happens to the rest of a batch when one of its
// messages is not deleted. A FIFO receive returns as many messages of one group
// as it can, so everything behind an undeleted message may be in its group, and
// handling it would be applying a wallet's operations out of order. A message
// the consumer does not delete therefore stops its batch: on a transient failure
// the messages behind it are hidden for the same backoff, so that the group
// comes back together and in order; on a permanent one they are left exactly as
// they are, and time out together.
//
// There is a backstop under all of that, and it is worth naming because it is
// what makes the ordering an optimisation rather than a correctness
// requirement. An operation whose reference has not arrived is not corrupted and
// is not refused — the domain parks it as PENDING_REFERENCE and the reference
// worker carries it forward when the reference lands. Out-of-order delivery
// costs latency here, not money.
//
// # Backoff
//
// [Backoff] is this package's, not [app.BackoffPolicy]. The two are answering
// different questions and one of them is not ours: that policy schedules a
// parked operation against the wait budget the domain owns, clamps itself to
// that budget's deadline, and jitters from the transaction's identifier — it
// needs a transaction, a deadline and a domain rule, and a queue message has
// none of the three. Its own documentation is the argument against reusing it:
// the schedule is kept apart from the budget so that tuning one cannot change
// whether an operation is eventually settled, and a worker that redelivered on
// the business policy's numbers would join them back together. Its constructor
// and its next-attempt function are unexported besides, so reusing it would mean
// changing internal/app to serve an operational concern it deliberately has no
// opinion about.
//
// One policy per worker covers two jobs: how long a piece of work waits before
// it is looked at again, and how long a loop waits when the thing it polls is
// not answering. They are the same shape — a failure that repeats should be
// asked about less often — and keeping them apart would be two numbers to tune
// where the operator has one question.
//
// # Shutdown
//
// Stopping is two deadlines, not one. Receiving stops immediately — the
// long poll is cancelled rather than waited out — while work already in hand
// keeps the context it started with until the drain deadline passes. Whatever is
// still in flight at that deadline is given back: the consumer resets the
// visibility of every message it has not finished deciding about to zero, so it
// is redelivered at once instead of waiting out a timeout that exists for the
// case where nobody released it, and the publisher hands back every outbox claim
// it holds. Both report what did not finish, because a drain that ran out of
// time and a drain that completed are different mornings.
//
// # Fault points
//
// Four windows in this system cannot be closed by a transaction, because
// something outside the database has to happen after one commits. Each is
// marked with a [faults.Hit] on the production path — not behind a flag, not
// behind a build tag — so that a recovery test can kill the process exactly
// there and prove what survives:
//
//   - [faults.AfterCommitBeforeAck] in the consumer, between the commit and the
//     delete.
//   - [faults.AfterClaimBeforePublish] in the publisher, between taking a claim
//     and sending anything.
//   - [faults.AfterPublishBeforeMark] in the publisher, between the send and the
//     outbox row being marked published.
//   - [faults.AfterPendingCommit] in the reference worker, after the commit that
//     parked or settled an operation.
package workers
