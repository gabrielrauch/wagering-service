package workers

import (
	"strings"
	"unicode/utf8"
)

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

// What a correlation may be.
//
// The bounds are wagering.opaque_id's, which is the column every correlation is
// eventually stored in: a value the database would refuse must be refused
// before a transaction has begun, or an operation that was otherwise perfectly
// good fails at its last statement.
//
// The control-character bound does a second job, the same one it does on the
// HTTP path. This value is written into every log line the message produces, so
// something that could put a newline in it would be writing those log lines.
const (
	maxCorrelationBytes = 128
	asciiSpace          = 0x20
	asciiDelete         = 0x7f
)

// usableCorrelation reports whether a supplied correlation is one this service
// may repeat.
//
// A queue message has no caller to hand a refusal to, so an unusable value is
// replaced rather than rejected — see [Consumer] for what it is replaced with.
// Refusing the message instead would send a perfectly good operation to the
// dead-letter queue over a field that identifies nothing but a log line.
func usableCorrelation(correlation string) bool {
	if correlation == "" ||
		len(correlation) > maxCorrelationBytes ||
		!utf8.ValidString(correlation) ||
		strings.TrimSpace(correlation) != correlation {
		return false
	}
	for _, r := range correlation {
		if r < asciiSpace || r == asciiDelete {
			return false
		}
	}
	return true
}
