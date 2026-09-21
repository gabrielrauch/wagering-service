package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// traced is a harness whose API reports through the real SDK, with everything
// it produces kept in memory.
//
// The request span is opened by the test rather than by otelhttp, because the
// composition root is what wraps the API and this package's suite deliberately
// builds the API alone. What the wrapper does is open one span and hand the
// handler a context carrying it, which is exactly what [traced.send] does — so
// what is asserted below is what the adapter puts on a span otelhttp opened.
type traced struct {
	*harness
	telemetry *telemetry.Telemetry
	spans     *tracetest.SpanRecorder
	metrics   *sdkmetric.ManualReader
}

func newTracedHarness(t *testing.T) *traced {
	t.Helper()
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	built, err := telemetry.New(telemetry.Config{
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		MeterProvider:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	})
	if err != nil {
		t.Fatalf("build the telemetry: %v", err)
	}
	h := newHarnessWith(t, func(cfg *Config) { cfg.Telemetry = built })
	return &traced{harness: h, telemetry: built, spans: spans, metrics: reader}
}

// send makes one request inside a request span, the way the composition root's
// otelhttp wrapper does.
func (x *traced) send(t *testing.T, req request) *httptest.ResponseRecorder {
	t.Helper()
	ctx, span := x.telemetry.Start(t.Context(), telemetry.SpanRequest,
		oteltrace.WithSpanKind(oteltrace.SpanKindServer))
	defer span.End()

	var body *strings.Reader
	if req.body != "" {
		body = strings.NewReader(req.body)
	}
	var r *http.Request
	if body == nil {
		r = httptest.NewRequestWithContext(ctx, req.method, req.path, nil)
	} else {
		r = httptest.NewRequestWithContext(ctx, req.method, req.path, body)
	}
	r.Header.Set("Authorization", "Bearer "+testCredential)
	for name, value := range req.headers {
		r.Header.Set(name, value)
	}
	for _, name := range req.without {
		r.Header.Del(name)
	}
	recorder := httptest.NewRecorder()
	x.api.ServeHTTP(recorder, r)
	return recorder
}

// span is the one finished span with that name.
func (x *traced) span(t *testing.T, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found []sdktrace.ReadOnlySpan
	var names []string
	for _, span := range x.spans.Ended() {
		names = append(names, span.Name())
		if span.Name() == name {
			found = append(found, span)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d spans named %q finished; the request produced %q", len(found), name, names)
	}
	return found[0]
}

// everything a request said on every span it produced, for the assertions about
// what must never appear.
func (x *traced) rendered() string {
	var out strings.Builder
	for _, span := range x.spans.Ended() {
		out.WriteString(span.Name() + " " + span.Status().Description + "\n")
		for _, attr := range span.Attributes() {
			out.WriteString("  " + string(attr.Key) + "=" + attr.Value.String() + "\n")
		}
		for _, event := range span.Events() {
			out.WriteString("  event " + event.Name + "\n")
			for _, attr := range event.Attributes {
				out.WriteString("    " + string(attr.Key) + "=" + attr.Value.String() + "\n")
			}
		}
	}
	return out.String()
}

// counted is the sum one counter holds under exactly these attributes.
func (x *traced) counted(t *testing.T, name string, want map[string]string) int64 {
	t.Helper()
	var into metricdata.ResourceMetrics
	if err := x.metrics.Collect(context.Background(), &into); err != nil {
		t.Fatalf("collect the measurements: %v", err)
	}
	for _, scope := range into.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is a %T, wanted an int64 sum", name, m.Data)
			}
			for _, point := range sum.DataPoints {
				if exactly(point.Attributes.ToSlice(), want) {
					return point.Value
				}
			}
		}
	}
	return 0
}

func exactly(got []attribute.KeyValue, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, attr := range got {
		value, named := want[string(attr.Key)]
		if !named || attr.Value.String() != value {
			return false
		}
	}
	return true
}

// attributeOf reads one string attribute off a span.
func attributeOf(span sdktrace.ReadOnlySpan, key string) string {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.String()
		}
	}
	return ""
}

// TestNoSpanCarriesAForeignOperationsWords is the trace half of the rule the
// log tests already hold for log lines.
//
// app.ErrForeignOperation must never be rendered. It is answered byte for byte
// as an ordinary miss, the log is the only place the two are told apart, and a
// span is read by whoever can read the trace — so a span carrying the refusal's
// own message would be the rendering the sentinel exists to prevent, in the one
// place nobody reviews.
//
// The code is still there, because a code is a published vocabulary and is what
// somebody filters a trace by.
func TestNoSpanCarriesAForeignOperationsWords(t *testing.T) {
	t.Parallel()
	const message = `no operation "0199c0de-0000-7000-8000-000000000001"`
	x := newTracedHarness(t)
	x.auth.principal = providerPrincipal(t, "acme")
	x.wagering.err = classified(app.NotFound, "", message, app.ErrForeignOperation)

	id := wagering.NewTransactionID()
	recorder := x.send(t, request{
		method: http.MethodGet,
		path:   "/wagering/transactions/" + id.String(),
	})
	assertStatus(t, recorder, http.StatusNotFound)

	rendered := x.rendered()
	for _, forbidden := range []string{message, "ForeignOperation", "another provider"} {
		if strings.Contains(rendered, forbidden) {
			t.Errorf("a span carried %q:\n%s", forbidden, rendered)
		}
	}
	// The log still tells the two apart, which is the distinction this rule is
	// protecting rather than deleting.
	if x.logs.find("a provider asked for another provider's operation") == nil {
		t.Errorf("the foreign read was not called out in the log:\n%s", x.logs.rendered())
	}
	if got := x.span(t, telemetry.SpanTransactionByID).Status().Description; got != "NOT_FOUND" {
		t.Errorf("the use case's span says %q, wanted the code", got)
	}
}

// TestARequestNamesWhatItWasAboutOnItsOwnSpan pins where the identifiers go.
//
// On the ENTRY span, because that is what somebody searches for: a trace found
// by correlationId or by walletId has to be findable from the top rather than
// from a child three levels down. The documented Tempo query is
// `{ .correlationId = "..." }`, and it matches a span rather than a trace.
func TestARequestNamesWhatItWasAboutOnItsOwnSpan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		send func(*traced) *httptest.ResponseRecorder
		// route is the span name the request ends up under, which is the
		// pattern the mux matched.
		route string
		key   string
		want  func(*httptest.ResponseRecorder) string
	}{
		{
			name: "the correlation a caller supplied",
			send: func(x *traced) *httptest.ResponseRecorder {
				return x.send(t, request{
					method:  http.MethodGet,
					path:    "/wallets/" + wagering.NewWalletID().String(),
					headers: map[string]string{correlationHeader: "thread-1"},
				})
			},
			route: "GET /wallets/{walletId}",
			key:   telemetry.KeyCorrelation,
			want:  func(*httptest.ResponseRecorder) string { return "thread-1" },
		},
		{
			name: "the wallet a path names",
			send: func(x *traced) *httptest.ResponseRecorder {
				return x.send(t, request{
					method: http.MethodGet,
					path:   "/wallets/0199c0de-0000-7000-8000-000000000009",
				})
			},
			route: "GET /wallets/{walletId}",
			key:   telemetry.KeyWallet,
			want: func(*httptest.ResponseRecorder) string {
				return "0199c0de-0000-7000-8000-000000000009"
			},
		},
		{
			name: "the provider a submission acts as",
			send: func(x *traced) *httptest.ResponseRecorder {
				x.auth.principal = providerPrincipal(t, "acme")
				x.wagering.result = processed(t, false)
				return x.send(t, submission(submitBody))
			},
			route: "POST /wagering/transactions",
			key:   telemetry.KeyProvider,
			want:  func(*httptest.ResponseRecorder) string { return "acme" },
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			x := newTracedHarness(t)
			recorder := c.send(x)
			span := x.span(t, c.route)
			if got := attributeOf(span, c.key); got != c.want(recorder) {
				t.Errorf("the request span carries %s=%q, wanted %q\n%s",
					c.key, got, c.want(recorder), x.rendered())
			}
		})
	}
}

// TestACorrelationIsOnTheSpanEvenWhenTheRequestIsRefusedOutright pins the case
// the attribute exists for.
//
// A request refused before it reaches a route is the one somebody goes looking
// for, and it is the one a span set from inside a handler would not carry.
func TestACorrelationIsOnTheSpanEvenWhenTheRequestIsRefusedOutright(t *testing.T) {
	t.Parallel()
	x := newTracedHarness(t)

	recorder := x.send(t, request{
		method: http.MethodGet,
		path:   "/nothing/answers/this",
	})
	assertStatus(t, recorder, http.StatusNotFound)

	span := x.span(t, telemetry.SpanRequest)
	want := recorder.Header().Get(correlationHeader)
	if got := attributeOf(span, telemetry.KeyCorrelation); got == "" || got != want {
		t.Errorf("a refused request's span carries correlation %q, wanted %q", got, want)
	}
}

// TestASubmissionIsCountedByKindStatusAndFailureCode pins the counter a
// dashboard's headline panel is built from.
//
// A rejection keeps its catalogued reason, because "how many bets were refused
// for insufficient funds" is the question that metric exists to answer. A
// processed operation carries NONE rather than nothing, so the two are one
// series that adds up rather than two that do not.
func TestASubmissionIsCountedByKindStatusAndFailureCode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		result func(*testing.T) app.OperationResult
		want   map[string]string
	}{
		{
			name:   "an operation that moved money",
			result: func(t *testing.T) app.OperationResult { return processed(t, false) },
			want: map[string]string{
				"source": telemetry.SourceHTTP, "kind": "BET",
				"status": "PROCESSED", "failureCode": telemetry.NoFailureCode,
			},
		},
		{
			name:   "a replay of one",
			result: func(t *testing.T) app.OperationResult { return processed(t, true) },
			want: map[string]string{
				"source": telemetry.SourceHTTP, "kind": "BET",
				"status": "PROCESSED", "failureCode": telemetry.NoFailureCode,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			x := newTracedHarness(t)
			x.auth.principal = providerPrincipal(t, "acme")
			x.wagering.result = c.result(t)

			x.send(t, submission(submitBody))

			if got := x.counted(t, telemetry.MetricTransactions, c.want); got != 1 {
				t.Errorf("%s counted %d under %v, wanted 1",
					telemetry.MetricTransactions, got, c.want)
			}
		})
	}
}

// TestAReadIsNotCountedAsAnOperation pins the distinction that keeps the
// headline number meaning anything.
//
// A read returns the same shape as a submission and has decided nothing.
// Counting one would put "how many operations did this service perform" and
// "how many times did somebody look at one" in the same series, and the second
// is unbounded by anything the business does.
func TestAReadIsNotCountedAsAnOperation(t *testing.T) {
	t.Parallel()
	x := newTracedHarness(t)
	x.auth.principal = providerPrincipal(t, "acme")
	x.wagering.result = processed(t, false)

	x.send(t, request{
		method: http.MethodGet,
		path:   "/wagering/transactions/" + wagering.NewTransactionID().String(),
	})

	for _, want := range []map[string]string{
		{
			"source": telemetry.SourceHTTP, "kind": "BET",
			"status": "PROCESSED", "failureCode": telemetry.NoFailureCode,
		},
	} {
		if got := x.counted(t, telemetry.MetricTransactions, want); got != 0 {
			t.Errorf("%s counted %d for a read", telemetry.MetricTransactions, got)
		}
	}
}

// TestASpanIsNamedForTheRouteAndNeverForThePath pins the one decision that
// keeps a trace backend usable.
//
// The span is opened before the mux has matched anything, so the only name
// available at that point is the method and the RAW path — and the raw path
// carries wallet ids and transaction ids. Named that way there would be one
// span name per wallet, for ever, and the list a trace backend offers as
// "operations" would be unreadable within a day.
//
// http.route carries the template alone, without the method, because a
// dashboard groups by it and a route that carried the method would be two
// series for one endpoint answered two ways.
func TestASpanIsNamedForTheRouteAndNeverForThePath(t *testing.T) {
	t.Parallel()
	const walletID = "0199c0de-0000-7000-8000-000000000009"
	x := newTracedHarness(t)

	x.send(t, request{method: http.MethodGet, path: "/wallets/" + walletID})

	span := x.span(t, "GET /wallets/{walletId}")
	if strings.Contains(span.Name(), walletID) {
		t.Errorf("the span is named %q, which is one name per wallet", span.Name())
	}
	if got := attributeOf(span, "http.route"); got != "/wallets/{walletId}" {
		t.Errorf("http.route is %q, wanted the template without the method", got)
	}
}

// TestAWalletThatDisagreesWithItsLedgerIsCounted pins the one number in this
// system that should always be zero.
//
// Counted and never amounted: the difference is money, the report the caller
// holds carries it, and an alert needs to know that a wallet diverged rather
// than by how much.
func TestAWalletThatDisagreesWithItsLedgerIsCounted(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name       string
		consistent bool
		want       int64
	}{
		{name: "a wallet that balances", consistent: true, want: 0},
		{name: "a wallet that does not", consistent: false, want: 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			x := newTracedHarness(t)
			x.wallets.report = app.Reconciliation{Consistent: c.consistent}

			x.send(t, request{
				method: http.MethodPost,
				path:   "/wallets/" + wagering.NewWalletID().String() + "/reconciliation",
			})

			if got := x.counted(t, telemetry.MetricDivergences, map[string]string{}); got != c.want {
				t.Errorf("%s counted %d, wanted %d", telemetry.MetricDivergences, got, c.want)
			}
		})
	}
}

// TestTheRouteReachesTheRequestMetricsAndNotOnlyTheSpan pins the second half of
// naming a route, which is easy to write and easy to forget.
//
// otelhttp records http.server.request.duration from the labeler it puts in the
// context plus what it can derive from the request, and the route is not among
// those: it runs before any mux has matched one. A span attribute does not
// reach a metric, so without the labeler every endpoint collapses into one
// series — the measurement still exists, and "which route is slow" has no
// answer.
func TestTheRouteReachesTheRequestMetricsAndNotOnlyTheSpan(t *testing.T) {
	t.Parallel()
	x := newTracedHarness(t)

	labeler := &otelhttp.Labeler{}
	ctx, span := x.telemetry.Start(otelhttp.ContextWithLabeler(t.Context(), labeler),
		telemetry.SpanRequest)
	r := httptest.NewRequestWithContext(ctx, http.MethodGet,
		"/wallets/0199c0de-0000-7000-8000-000000000009", nil)
	r.Header.Set("Authorization", "Bearer "+testCredential)
	x.api.ServeHTTP(httptest.NewRecorder(), r)
	span.End()

	var routes []string
	for _, attr := range labeler.Get() {
		if string(attr.Key) == "http.route" {
			routes = append(routes, attr.Value.String())
		}
	}
	if len(routes) != 1 || routes[0] != "/wallets/{walletId}" {
		t.Errorf("the request's metrics are labelled %v, wanted one http.route of "+
			"/wallets/{walletId}", routes)
	}
}
