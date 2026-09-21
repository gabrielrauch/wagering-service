package workers

// The message attributes this service carries trace context in, in both
// directions.
//
// Trace context rides beside the body rather than only inside it so that
// anything routing, filtering or logging a message can follow a thread without
// parsing a financial payload it has no business reading. The names are the
// envelope's own member names, so that the attribute and the field a reader
// eventually finds in the body are spelled the same way.
//
// This is carriage and nothing more. Wiring these to a tracer is a later task,
// and a worker that invented a span here would be deciding that task's contract
// for it.
const (
	correlationAttribute = "correlationId"
	causationAttribute   = "causationId"
)

// usableCorrelation reports whether a supplied correlation is one this service
// may repeat.
//
// The rules are [opaqueID]'s, and they are the same rules because they are the
// same column: a correlation is stored in wagering.opaque_id, so a value the
// database would refuse has to be refused before a transaction has begun, or an
// operation that was otherwise perfectly good fails at its last statement. The
// control-character bound does a second job here — this value is written into
// every log line the message produces, so something that could put a newline in
// it would be writing those log lines.
//
// The disposition is what differs from the HTTP path, not the test. There, a
// correlation carrying a control character is REFUSED, because a caller is
// waiting and a log-injection attempt quietly repaired is the one case somebody
// needs to hear about. A queue message has nobody to hand a refusal to, so an
// unusable value is replaced and the substitution logged — see [Consumer] for
// what it is replaced with. Refusing the message instead would send a perfectly
// good operation to the dead-letter queue over a field that identifies nothing
// but a log line.
func usableCorrelation(correlation string) bool { return opaqueID(correlation) }
