package httpapi

import (
	"encoding/json/v2"
	"log/slog"
	"net/http"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// renderFailure is the body written when a response could not be rendered.
//
// It is a constant rather than a marshalled value for the obvious reason: the
// path that reaches it is the one where marshalling did not work.
const renderFailure = `{"code":"UNRETRYABLE","message":"the response could not be rendered","correlationId":""}`

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
		w.Header().Set("Content-Type", contentTypeJSON)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(renderFailure))
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

// operationStatus is the status an [app.OperationResult] is answered with.
//
// One rule, wherever a result is rendered: a submission and a read of the same
// operation answer the same way, so a provider writes one branch rather than
// two that can disagree about the transaction they are both describing.
//
// A rejection is 422 and arrives here on a nil error, which is the whole of
// ADR-0012: a business rule settling an operation persisted a row, emitted an
// event and bound the idempotency key to that payload for good. Inferring it
// from an error would mean inferring it from something that never happens.
//
// PENDING and FAILED fall to 200. Neither is reachable through these doors
// today — nothing commits PENDING, and a permanently failed operation can only
// be read back — but a mapping with a hole in it answers a status of zero, and
// the honest answer for "here is the operation, in the state it is in" is 200.
func operationStatus(result app.OperationResult) int {
	switch result.Status {
	case wagering.PendingReference:
		return http.StatusAccepted
	case wagering.Rejected:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusOK
	}
}

// operation answers with one operation.
func (a *API) operation(w http.ResponseWriter, r *http.Request, result app.OperationResult) {
	a.writeJSON(w, r, operationStatus(result), operationOf(result))
}
