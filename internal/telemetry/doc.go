// Package telemetry is what every other package in this tree reports through:
// the tracer it opens spans on, the propagator that carries a trace across a
// boundary, and the instruments it counts on.
//
// It holds no SDK. Nothing here builds an exporter, reads an environment
// variable or decides whether telemetry is switched on — that is the
// composition root's, and internal/fxmod is where it lives. What this package
// owns is the vocabulary: which spans exist, what they are called, which
// attributes they carry, what the metrics are named and what may never be put
// in either.
//
// The split is the point. A worker, an adapter or a use case that wanted to
// count something would otherwise reach for a global meter and invent a name,
// and the names would then be discoverable only by grepping. Here they are
// declared once, in one file, with the units and the attribute keys beside
// them, so a dashboard can be built from this package rather than from a
// running process.
//
// # Nothing is required to be configured
//
// Every component takes a [*Telemetry] and treats nil as [Disabled], which is
// a fully formed value built on OpenTelemetry's own no-op providers. That is
// what "telemetry is disableable" means here: not a flag tested at each call
// site, but instruments that record into nothing and spans whose context is
// invalid. There is therefore no branch in any caller, no interface anybody has
// to implement, and no possibility of a component that silently stops
// reporting because a nil check was written the other way round.
//
// # What must never be in a span or a log line
//
// Three rules, and they are enforced here rather than remembered at each site:
//
//   - No error message ever reaches a span. [Telemetry.Failed] takes a CODE —
//     a [failure.Code], an app.Class or one of this package's own words — and
//     never an error, so there is no call that could render one. The reason is
//     app.ErrForeignOperation, which must never be shown to anybody: a
//     provider walking another provider's identifiers is told exactly what a
//     caller asking for something absent is told, and a span attribute is
//     shown to whoever can read the trace. The message is still written, once,
//     to the log, where the HTTP adapter already decides what may be said.
//   - No money, no balance, no player and no payload. The attributes this
//     package offers are identifiers and outcomes; there is no constructor
//     here for an amount, and adding one would be adding a financial payload to
//     a trace.
//   - No credential, ever. Nothing here takes a token, a header set or a claim.
//
// # The attribute names
//
// They are the log's own names — correlationId, messageId, transactionId,
// walletId, providerId, eventId — rather than OpenTelemetry's dotted
// convention. One vocabulary across logs and traces is worth more than
// matching a convention that has no entry for any of them, and it is what
// makes `{ .correlationId = "..." }` the same query an operator would grep the
// logs with.
package telemetry
