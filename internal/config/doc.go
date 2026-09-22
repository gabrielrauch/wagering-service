// Package config is every number and address this service is tuned with, read
// from the environment once and checked before anything is built from it.
//
// # Why it is its own package, and why it imports nothing of this tree
//
// It depends on the standard library alone — not on internal/app, not on
// internal/workers, not on any adapter — so that the whole of it is testable
// without a container, a database or a queue, and so that nothing here can
// quietly acquire an opinion about what a value is for. The composition root in
// internal/fxmod is the one place that knows a [Backoff] becomes an
// app.BackoffPolicy in one direction and a workers.Backoff in the other. That
// conversion is three fields, written three times, and it buys a configuration
// package that a test can exercise in microseconds.
//
// It is also not internal/fxmod. Fx is a wiring decision and the configuration
// is not: cmd/migrate already reads DATABASE_URL without one, and a package
// that read the environment through a dependency-injection container would make
// "what does this process need to be told" a question you have to start a
// container to answer.
//
// # Two layers of checking, not one
//
// This package turns text into values and refuses text that is not a value: a
// duration that does not parse, a count that is not positive, a factor that is
// not a number, a variable that is required and absent. What it deliberately
// does NOT do is restate the rules each constructor already enforces — that an
// SQS wait is at most twenty seconds, that a consumer name fits in an opaque
// identifier, that an OIDC algorithm allow-list cannot contain a symmetric
// algorithm. Those live with the code that depends on them, and a copy here
// would be a second place to change and a first place to disagree.
//
// There are two exceptions, and each is one for a reason worth writing down.
//
// The first is [Backoff.Factor]. strconv.ParseFloat("NaN", 64) succeeds with no error, so
// a factor of NaN is a value this package would otherwise hand on, and NaN is
// not less than anything — so a check of the form "the factor must be at least
// one" lets it straight through. Both consumers now refuse it where they use
// it, which is where the invariant belongs; this package refuses it as well,
// and the difference is what an operator is told. A constructor can say that a
// worker's backoff factor is not a number. Only the loader can say which
// VARIABLE carried it, and only the loader refuses it before anything has been
// built from it.
//
// The second is the publisher's two waits, PUBLISHER_HOLD and
// PUBLISHER_BACKOFF_MAX, which are refused at five minutes or more. That is not
// a constructor's rule restated — no constructor has it, because the number is
// not this tree's. It is SQS's: a FIFO queue deduplicates on the event id for
// exactly five minutes, so an event sent again after its claim expired or its
// backoff elapsed is one message on the wire only while that window is still
// open. Nothing downstream can see the window, and the loader is the only place
// that can refuse a configuration under which a publisher killed after sending
// would put a second copy of an event on the queue.
//
// # Every value is reported, not the first
//
// [Load] collects every problem and returns them joined, because an operator
// who has three variables wrong should learn that in one restart rather than in
// three. errors.Is still finds each one.
//
// # Secrets
//
// DATABASE_URL carries a password and is the only value here that does. It is
// never rendered into an error — a parse failure names the variable and not its
// contents — and [Postgres.Redacted] is what a start-up line is allowed to say.
// There is no String method on any type here, so there is nothing that prints a
// password by being passed to a %v.
//
// # A variable that is set but empty is a variable that is not set
//
// Every accessor treats an empty value as absent. Shell interpolation and
// compose files produce an empty string for a variable nobody defined —
// `FOO=${BAR}` with no BAR is `FOO=`, not an unset FOO — so the other reading
// would let one undefined variable in a compose file silently defeat every
// default in this package.
package config
