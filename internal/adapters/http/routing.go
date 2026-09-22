package httpapi

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.opentelemetry.io/otel/trace"
)

// routes registers every pattern this service answers.
//
// The paths are written in net/http's wildcard notation. The task text spells
// them ":walletId"; that is how a path parameter is written down, not a
// requirement to take a routing dependency that reads it.
func (a *API) routes() *http.ServeMux {
	mux := http.NewServeMux()

	// Wallets are the service's to administer. There is no check for that here:
	// Principal.MayAdministerWallets is the one statement of the rule, it is
	// tested where it lives, and a second copy in this package could only ever
	// drift from it.
	mux.Handle("POST /wallets", dispatched(a.authenticated(a.openWallet)))
	mux.Handle("GET /wallets/{walletId}", dispatched(a.authenticated(a.readWallet)))
	mux.Handle("GET /wallets/{walletId}/ledger", dispatched(a.authenticated(a.readLedger)))
	mux.Handle("POST /wallets/{walletId}/reconciliation",
		dispatched(a.authenticated(a.reconcileWallet)))

	mux.Handle("POST /wagering/transactions", dispatched(a.authenticated(a.submitOperation)))
	mux.Handle("GET /wagering/transactions/{transactionId}",
		dispatched(a.authenticated(a.readOperation)))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		dispatched(a.authenticated(a.readProviderOperation)))

	// Public. A liveness probe that needed a credential would restart a healthy
	// process the moment the identity provider went away, and a readiness probe
	// that needed one would take this service out of rotation for the same
	// reason — which is exactly when its dependencies most need reporting on.
	mux.Handle("GET /health/live", dispatched(http.HandlerFunc(a.live)))
	mux.Handle("GET /health/ready", dispatched(http.HandlerFunc(a.ready)))

	return mux
}

// routed is the writer the mux dispatches through.
//
// It exists for the three responses the mux composes itself. ServeMux answers a
// path it does not know with plain text, a path it knows under another method
// with plain text and an Allow header, and a path that needs cleaning — "//wallets",
// "/wallets/x/../y" — with a 307 and an HTML body. None of them can be
// replaced: there is no hook on a mux for any of them, and registering a
// catch-all "/" would capture the first and destroy the second, because a
// pattern with no method matches every method and so turns every method
// mismatch into a miss.
//
// So they are recognised on the way out instead. A handler that ran marks this
// writer, and a 404, a 405 or a redirect arriving unmarked can only be the
// mux's own. The two refusals are restated in the contract's shape, with the
// Allow header the mux already set left where it is. The redirect keeps its
// status and its Location, because it is right — 307 preserves the method, so a
// submission survives it — and loses only its body: the Location header is the
// whole of the answer, and an HTML page is the one representation this API
// never serves.
//
// # What wrapping the writer costs
//
// http.Flusher, http.Hijacker and io.ReaderFrom are not forwarded, so a type
// assertion for any of them through this writer fails where it would have
// succeeded on net/http's own. Nothing in this package needs them — there is no
// streaming response, no upgrade and no sendfile path — and [routed.Unwrap] is
// what makes [net/http.ResponseController] reach the writer underneath, which
// is the supported way to ask for any of that. Middleware written against the
// bare assertions would silently do nothing instead.
type routed struct {
	http.ResponseWriter
	api     *API
	request *http.Request
	// matched reports that a registered handler ran.
	matched bool
	// restated reports that the mux's refusal has been answered, so the plain
	// text it is about to write is discarded.
	restated bool
}

// WriteHeader restates the mux's own responses and passes everything else
// through.
func (w *routed) WriteHeader(status int) {
	if !w.matched && !w.restated {
		switch {
		case status == http.StatusNotFound:
			w.restated = true
			w.api.refuse(w.ResponseWriter, w.request, status, codeNotFound,
				"no route answers this path")
			return
		case status == http.StatusMethodNotAllowed:
			w.restated = true
			w.api.refuse(w.ResponseWriter, w.request, status, codeMethodNotAllowed,
				"this path does not answer "+w.request.Method)
			return
		case status >= http.StatusMultipleChoices && status < http.StatusBadRequest:
			// The mux redirecting a path it can clean. Kept, minus the page it
			// would have drawn: Location is the answer, and the body would be
			// the one HTML this API ever wrote.
			w.restated = true
			w.ResponseWriter.Header().Del("Content-Type")
			w.ResponseWriter.WriteHeader(status)
			return
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

// Write discards the body of a response that has been restated, and passes
// everything else through.
func (w *routed) Write(b []byte) (int, error) {
	if w.restated {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap exposes the writer underneath, which is what [net/http.ResponseController]
// looks for.
func (w *routed) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// dispatched marks the writer as belonging to a route that matched, which is
// the whole of how [routed] tells a handler's 404 from the mux's.
//
// It also names the request's span, and this is the only place that can. The
// span is opened by otelhttp before the mux has matched anything, so the only
// name available to it is the method and the raw path — and the raw path
// carries wallet ids, transaction ids and external transaction ids, which would
// make one span name per wallet and a trace backend that indexes names into a
// list nobody can read. Here the PATTERN is known, so the span is renamed to
// the route: "GET /wallets/{walletId}", one name per route, for ever.
//
// The pattern is the span name unaltered, because net/http writes it as
// "GET /wallets/{walletId}" — method, space, template — which is already
// OpenTelemetry's own convention for naming an HTTP server span. Composing the
// method in front of it was the first version of this line and produced
// "GET GET /wallets/{walletId}".
//
// A request that reaches no route keeps [telemetry.SpanRequest], which is the
// honest answer: nothing matched, so there is no route to name.
func dispatched(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rw, ok := w.(*routed); ok {
			rw.matched = true
		}
		route := routeOf(r.Pattern)
		if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
			span.SetName(r.Pattern)
			span.SetAttributes(semconv.HTTPRoute(route))
		}
		// And on the request's METRICS, which is a separate act. otelhttp
		// records http.server.request.duration from the labeler it put in the
		// context plus the attributes it can derive from the request itself —
		// and the route is not among those, because otelhttp runs before any
		// mux has matched one. Without this line every endpoint collapses into
		// one series and "which route is slow" has no answer.
		//
		// It is the one place this package touches otelhttp, and the labeler is
		// the only supported way in: a handler cannot reach the instrument, and
		// the composition root cannot reach the route.
		if labeler, found := otelhttp.LabelerFromContext(r.Context()); found {
			labeler.Add(semconv.HTTPRoute(route))
		}
		h.ServeHTTP(w, r)
	})
}

// routeOf is the path template out of a registered pattern.
//
// net/http writes a pattern as "[METHOD ][HOST]/path", and http.route is the
// path alone: it is what a dashboard groups by, and a route that carried the
// method would be two series for one endpoint answered two ways.
func routeOf(pattern string) string {
	if _, path, found := strings.Cut(pattern, " "); found {
		return path
	}
	return pattern
}
