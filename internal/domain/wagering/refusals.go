package wagering

import "github.com/gabrielrauch/wagering-service/internal/domain/failure"

// The two refusals the model raises most often, as constructors rather than as
// an expression repeated at every guard.
//
// Both say the same thing about a field wherever they appear, so spelling them
// once keeps the code and the message a caller sees from drifting apart, and
// makes the guards themselves short enough to read as a list of requirements.
// They return *failure.Error so a caller may still attach more to them.

// missing reports a field the operation requires but that was absent or empty.
func missing(field string) *failure.Error {
	return failure.New(failure.MissingRequiredField, "must be present").WithField(field)
}

// uninitialized reports a domain value that was never built through its
// validating constructor, such as a zero-valued [money.Money].
func uninitialized(field string) *failure.Error {
	return failure.New(failure.UninitializedValue, "must be present").WithField(field)
}
