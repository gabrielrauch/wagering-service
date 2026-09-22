package httpapi

import (
	"encoding/json/v2"
	"log/slog"
	"net/http"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// renderFailureMessage is what a caller is told when the answer could not be
// rendered.
const renderFailureMessage = "the response could not be rendered"

// renderFailure is the body of last resort.
//
// It has no correlation, and it is the one response this package writes that
// does not — which is why it exists as a constant and is reached only when the
// three-string body below has ALSO failed to marshal. At that point nothing can
// be built from anything, and a body that breaks the shape is still better than
// a status line with nothing under it.
const renderFailure = `{"code":"UNRETRYABLE","message":"` + renderFailureMessage +
	`","correlationId":""}`

// writeJSON renders body and sends it.
//
// The value is marshalled before the status line is written, so a value that
// cannot be rendered is still answered with the status that says so. Writing
// the header first and discovering the problem afterwards would leave a 200
// already on the wire with half a body under it.
//
// Map members are ordered, so one answer has one spelling. Nothing here depends
// on that, but a caller diffing two health responses does.
func (a *API) writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	payload, err := json.Marshal(body, json.Deterministic(true))
	if err != nil {
		a.logger.ErrorContext(r.Context(), "a response could not be rendered",
			slog.String("method", r.Method),
			slog.String("route", r.Pattern),
			slog.String("error", err.Error()),
			slog.String("correlationId", correlationFrom(r.Context())))
		a.writeRenderFailure(w, r)
		return
	}

	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)
	if _, err := w.Write(payload); err != nil {
		// The status line is already gone; there is nothing left to tell the
		// caller. Recorded at debug because a client hanging up mid-response is
		// normal and alerting on it would alert on the network.
		a.logger.DebugContext(r.Context(), "a response could not be sent",
			slog.String("correlationId", correlationFrom(r.Context())),
			slog.String("error", err.Error()))
	}
}

// writeRenderFailure answers a response that could not be rendered, in the
// shape every other refusal takes.
//
// It builds the body rather than reaching for the constant, because the
// correlation is the whole point of that member: a caller reporting "your
// service returned a 500" with nothing to name the request by is a caller
// nobody can help. The three strings here cannot fail to marshal for the same
// reason a string is not a cycle — but the constant is still there for the
// answer if they somehow do.
func (a *API) writeRenderFailure(w http.ResponseWriter, r *http.Request) {
	payload, err := json.Marshal(errorBody{
		Code:          string(app.Unretryable),
		Message:       renderFailureMessage,
		CorrelationID: correlationFrom(r.Context()),
	}, json.Deterministic(true))
	if err != nil {
		payload = []byte(renderFailure)
	}
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write(payload)
}

// submissionStatus is the status a submitted operation is answered with.
//
// It says what the submission came to, which is why it applies to a submission
// and to nothing else. A rejection is 422 and arrives on a nil error, which is
// the whole of ADR-0012: a business rule settling an operation persisted a row,
// emitted an event and bound the idempotency key to that payload for good.
// Inferring it from an error would mean inferring it from something that never
// happens.
//
// PENDING and FAILED fall to 200. Neither is reachable through this door today
// — nothing commits PENDING, and a permanently failed operation can only be
// read back — but a mapping with a hole in it answers a status of zero, and the
// honest answer for "here is the operation, in the state it is in" is 200.
func submissionStatus(result app.OperationResult) int {
	switch result.Status {
	case wagering.PendingReference:
		return http.StatusAccepted
	case wagering.Rejected:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusOK
	}
}

// submitted answers a submission with what it came to.
func (a *API) submitted(w http.ResponseWriter, r *http.Request, result app.OperationResult) {
	a.writeJSON(w, r, submissionStatus(result), operationOf(result))
}

// read answers a read with the operation it found, always 200.
//
// A read that found the operation succeeded, whatever the operation came to,
// and the status line says so. The alternative — reusing [submissionStatus] —
// buys a consistency that costs the read its usable status: 422 on a GET tells
// a client that the request could not be processed when it was processed
// perfectly, and 202 tells it something has been accepted for processing when
// nothing has. Any client with generic HTTP error handling then treats a
// successful read as a failure. The branch a provider actually needs is the
// status member of the body, which is there either way.
func (a *API) read(w http.ResponseWriter, r *http.Request, result app.OperationResult) {
	a.writeJSON(w, r, http.StatusOK, operationOf(result))
}
