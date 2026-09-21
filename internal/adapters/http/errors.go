package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// The codes this package raises for itself.
//
// They name protocol conditions rather than business ones, which is why they
// are not [failure.Code]s: no wagering rule was consulted, nothing was
// persisted, and there is no catalogue entry that would describe them. Every
// other code on the wire is either a failure.Code or an [app.Class], both of
// which are already published vocabularies — see [codeFor].
const (
	codeNotFound         = "NOT_FOUND"
	codeMethodNotAllowed = "METHOD_NOT_ALLOWED"
	codeContentTooLarge  = "CONTENT_TOO_LARGE"
)

// retryAfter is what a 503 tells a caller to wait, in seconds.
//
// A retryable failure here is a lost connection, a statement timeout or a lock
// conflict, all of which are over in milliseconds, so one second is the
// smallest whole-second hint that is honest. It is a constant rather than a
// setting because a caller that needs a different pace has its own backoff, and
// a knob whose only correct value is "about a second" is a knob to get wrong.
const retryAfter = "1"

// contentTypeJSON is what every response this package writes is.
const contentTypeJSON = "application/json"

// The message a caller is given when the failure's own words are not theirs to
// read. Each is fixed, so nothing that varies with an infrastructure failure
// can be read off the wire.
const (
	unauthenticatedMessage = "the request carries no usable credential"
	forbiddenMessage       = "the caller may not do this"
	invalidMessage         = "the request was refused as malformed"
	conflictMessage        = "the request contradicts something already recorded"
	notFoundMessage        = "there is no such resource"
	retryableMessage       = "the service is temporarily unable to answer, try again"
	unretryableMessage     = "the request could not be completed"
	auditMessage           = "the stored records for this wallet disagree and need operator attention"
)

// errorBody is the one shape every refusal takes.
//
// Three members, always all three. Code is what a caller branches on and is
// stable; message is for whoever reads the log; correlationId is how the two
// sides talk about the same request afterwards.
//
// The field at fault is not a fourth member. It is prefixed to the message
// instead, so that one shape stays one shape and a caller parsing a refusal
// never has to ask whether this one has the extra key.
type errorBody struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlationId"`
}

// statusFor maps a class to the status a handler answers with.
//
// [app.Rejected] is absent, and its absence is the point: a rejection is an
// outcome and never arrives on an error, so the 422 it is answered with comes
// from the result — see [operationStatus].
//
// Unauthorized is 403 here and 401 in [API.unauthenticated]. The application
// layer cannot make that distinction, because it never sees a request that
// failed to authenticate; this package can, because authentication happens
// here, and by the time a handler is running a principal exists.
//
// Audit is 500. It is a finding about this service's stored records rather than
// about anything submitted: the caller has nothing to correct and nothing to
// retry, and repeating the request finds the same records. It keeps the
// finding's own code, so the name in the caller's log is the name in the alert.
func statusFor(class app.Class) int {
	switch class {
	case app.Invalid:
		return http.StatusBadRequest
	case app.Unauthorized:
		return http.StatusForbidden
	case app.NotFound:
		return http.StatusNotFound
	case app.Conflict:
		return http.StatusConflict
	case app.Retryable:
		return http.StatusServiceUnavailable
	default:
		// Unretryable, Audit, and anything that arrived unclassified. An error
		// nobody recognised is never an invitation to retry, which is the same
		// reasoning app.ClassOf applies to one carrying no code.
		return http.StatusInternalServerError
	}
}

// codeFor is the stable reason the body carries.
//
// A failure that names a catalogued reason uses it. One that does not uses its
// [app.Class], which is already a published vocabulary the application layer
// documents — inventing a code for each of those instead would put entries in
// the external contract describing nothing a provider could act on, while
// leaving the member every caller parses sometimes absent.
func codeFor(err error, class app.Class) string {
	if code, ok := app.CodeOf(err); ok {
		return string(code)
	}
	return string(class)
}

// messageFor is the sentence the body carries.
//
// Only the classes a caller can act on render the failure's own words. The rest
// answer with a fixed sentence, because their message comes from infrastructure
// and would carry whatever a driver, a socket or a query put in it.
func messageFor(err error, class app.Class) string {
	switch class {
	case app.Invalid, app.Conflict, app.NotFound, app.Unauthorized:
		if said := words(err); said != "" {
			return said
		}
	}
	switch class {
	case app.Invalid:
		return invalidMessage
	case app.Conflict:
		return conflictMessage
	case app.NotFound:
		return notFoundMessage
	case app.Unauthorized:
		return forbiddenMessage
	case app.Retryable:
		return retryableMessage
	case app.Audit:
		return auditMessage
	default:
		return unretryableMessage
	}
}

// words returns the refusal's own message, and "" when there is none to read.
//
// It reads it off the classified error rather than off whatever is holding it,
// which is what keeps a cause chain out of a response: [app.Error] renders its
// head and its message and never its cause, and [failure.Error] the same, so
// the string here is the one sentence that error wrote about itself.
//
// The head is removed rather than printed. It restates the class, the code and
// the field, and the body carries the first two in a member of their own; the
// field is put back in front of the message, because a caller told
// INVALID_FIELD_FORMAT and not which field cannot act on it.
func words(err error) string {
	if e, ok := errors.AsType[*app.Error](err); ok && e != nil {
		head := string(e.Class)
		if e.Code != "" {
			head += " " + string(e.Code)
		}
		if e.Field != "" {
			head += " [" + e.Field + "]"
		}
		said, found := strings.CutPrefix(e.Error(), head+": ")
		if !found {
			return ""
		}
		return withField(e.Field, said)
	}
	if f, ok := errors.AsType[*failure.Error](err); ok && f != nil {
		return withField(f.Field, f.Message())
	}
	return ""
}

// withField names the offending input in front of the message.
func withField(field, message string) string {
	if field == "" || message == "" {
		return message
	}
	return field + ": " + message
}

// fail answers an application failure in the contract's shape.
func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errTooLarge) {
		// Answered before it is classified, because there is no class for it:
		// nothing was submitted, so nothing was invalid, rejected or in
		// conflict, and 413 is the one status that tells a caller to send less.
		a.refuse(w, r, http.StatusRequestEntityTooLarge, codeContentTooLarge, tooLargeMessage)
		return
	}
	class := app.ClassOf(err)
	status := statusFor(class)
	a.record(r, err, class, status)
	a.writeError(w, r, status, codeFor(err, class), messageFor(err, class))
}

// refuse answers a refusal this package made for itself, at the status the
// protocol gives it.
func (a *API) refuse(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	a.logger.InfoContext(r.Context(), "request refused",
		slog.String("method", r.Method),
		slog.String("route", r.Pattern),
		slog.Int("status", status),
		slog.String("code", code),
		slog.String("correlationId", correlationFrom(r.Context())))
	a.writeError(w, r, status, code, message)
}

// unauthenticated answers a credential this service would not accept.
//
// The message is fixed. The refusals in internal/adapters/oidc do name a
// claim-level reason a caller may read, but which of them happened is not part
// of this API's contract, and whoever holds a token can read its expiry, its
// issuer and its audience out of the token itself — so nothing is withheld that
// the caller did not already have. What is withheld is the one distinction they
// could not have: a signature that did not check out and a key this process
// could not obtain are one answer here, and two different mornings in the log.
func (a *API) unauthenticated(w http.ResponseWriter, r *http.Request, err error) {
	a.logger.WarnContext(r.Context(), "credential refused",
		slog.String("method", r.Method),
		slog.String("route", r.Pattern),
		slog.String("reason", credentialReason(err)),
		slog.String("correlationId", correlationFrom(r.Context())))
	w.Header().Set("WWW-Authenticate", `Bearer realm="wagering"`)
	a.writeError(w, r, http.StatusUnauthorized, string(app.Unauthorized), unauthenticatedMessage)
}

// credentialReason names which credential refusal happened, for the log and
// never for the caller.
//
// The two refinements are tested before the condition they refine, because
// every one of them is also an ErrKeyUnavailable and the broader branch would
// swallow them: a stream of invented key identifiers and a rotation this
// process missed look identical from outside and are not the same morning.
func credentialReason(err error) string {
	switch {
	case errors.Is(err, oidc.ErrUnknownKey):
		return "unknown key identifier"
	case errors.Is(err, oidc.ErrRefreshDeclined):
		return "key set refreshed too recently"
	case errors.Is(err, oidc.ErrKeyUnavailable):
		return "no usable signing key"
	case errors.Is(err, oidc.ErrPrincipalUnresolved):
		return "token names no principal"
	case errors.Is(err, oidc.ErrTokenRejected):
		return "token rejected"
	default:
		return "unclassified"
	}
}

// record counts a failure where an operator can see it.
//
// A foreign operation is called out by name. It is answered byte for byte as an
// ordinary miss, so the log is the only place the two are ever told apart, and
// telling them apart is the entire reason the sentinel exists.
//
// Nothing here walks a cause chain. The rendered message is logged for the
// classes whose message the caller is not given, and that is the whole of it.
func (a *API) record(r *http.Request, err error, class app.Class, status int) {
	attrs := []any{
		slog.String("method", r.Method),
		slog.String("route", r.Pattern),
		slog.Int("status", status),
		slog.String("class", string(class)),
		slog.String("code", codeFor(err, class)),
		slog.String("correlationId", correlationFrom(r.Context())),
	}
	ctx := r.Context()
	switch {
	case errors.Is(err, app.ErrForeignOperation):
		a.logger.WarnContext(ctx, "a provider asked for another provider's operation", attrs...)
	case class == app.Retryable:
		a.logger.WarnContext(ctx, "request failed", append(attrs, slog.String("error", err.Error()))...)
	case status >= http.StatusInternalServerError:
		a.logger.ErrorContext(ctx, "request failed", append(attrs, slog.String("error", err.Error()))...)
	default:
		a.logger.InfoContext(ctx, "request refused", attrs...)
	}
}

// writeError renders the one error shape.
func (a *API) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", retryAfter)
	}
	a.writeJSON(w, r, status, errorBody{
		Code:          code,
		Message:       message,
		CorrelationID: correlationFrom(r.Context()),
	})
}
