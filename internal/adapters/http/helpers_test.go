package httpapi

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The bounds every harness is built with. They are small on purpose: a body
// limit of a few hundred bytes is enough to prove the limit is enforced without
// a test allocating a megabyte to do it.
const (
	testBodyLimit       = 512
	testReadinessBudget = time.Second
	// testCredential is what the Authorization header carries. It is not a
	// token and never reaches anything that would parse one — the fake
	// authenticator answers from what the test set, not from what it was given
	// — which is exactly why the API depends on an interface here.
	testCredential = "a-credential"
)

// harness is an API built on fakes, with the fakes still in reach.
type harness struct {
	api      *API
	wagering *fakeWagering
	wallets  *fakeWallets
	auth     *fakeAuthenticator
	checks   map[string]ReadinessCheck
}

// newHarness builds an API whose every dependency is a fake, authenticating
// every credential as principal.
func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		wagering: &fakeWagering{},
		wallets:  &fakeWallets{},
		auth:     &fakeAuthenticator{principal: servicePrincipal(t)},
		checks:   map[string]ReadinessCheck{},
	}
	api, err := New(Config{
		Wagering:         h.wagering,
		Wallets:          h.wallets,
		Authenticator:    h.auth,
		Readiness:        h.checks,
		ReadinessTimeout: testReadinessBudget,
		MaxBodyBytes:     testBodyLimit,
		Logger:           discard(),
	})
	if err != nil {
		t.Fatalf("building the API: %v", err)
	}
	h.api = api
	return h
}

// request is one call to the API under test.
type request struct {
	method string
	path   string
	body   string
	// headers are set verbatim, empty values included: a header present and
	// empty is a different request from one that is absent, and both have to be
	// sendable.
	headers map[string]string
	// without names the headers to send none of, which is how a test sends no
	// credential at all.
	without []string
}

// do sends one request and returns what came back.
func (h *harness) do(t *testing.T, req request) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	if req.body != "" {
		body = strings.NewReader(req.body)
	}
	r := httptest.NewRequestWithContext(t.Context(), req.method, req.path, body)
	r.Header.Set("Authorization", "Bearer "+testCredential)
	for name, value := range req.headers {
		r.Header.Set(name, value)
	}
	for _, name := range req.without {
		r.Header.Del(name)
	}
	recorder := httptest.NewRecorder()
	h.api.ServeHTTP(recorder, r)
	return recorder
}

// doWithContext sends one request under ctx, running cancel just before the
// handler chain is entered, so that what the use case is handed is a context
// the caller has already given up on.
func (h *harness) doWithContext(
	t *testing.T, ctx context.Context, req request, cancel context.CancelFunc,
) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	if req.body != "" {
		body = strings.NewReader(req.body)
	}
	r := httptest.NewRequestWithContext(ctx, req.method, req.path, body)
	r.Header.Set("Authorization", "Bearer "+testCredential)
	for name, value := range req.headers {
		r.Header.Set(name, value)
	}
	cancel()
	recorder := httptest.NewRecorder()
	h.api.ServeHTTP(recorder, r)
	return recorder
}

// errorOf reads the one error shape out of a response, failing the test when
// the body is not one.
func errorOf(t *testing.T, recorder *httptest.ResponseRecorder) errorBody {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body,
		json.RejectUnknownMembers(true)); err != nil {
		t.Fatalf("reading the error body %q: %v", recorder.Body.String(), err)
	}
	return body
}

// bodyOf decodes a response into out.
func bodyOf(t *testing.T, recorder *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), out,
		json.RejectUnknownMembers(true)); err != nil {
		t.Fatalf("reading the body %q: %v", recorder.Body.String(), err)
	}
}

// assertStatus fails unless the response carries want.
func assertStatus(t *testing.T, recorder *httptest.ResponseRecorder, want int) {
	t.Helper()
	if recorder.Code != want {
		t.Fatalf("status = %d, want %d; body %s", recorder.Code, want, recorder.Body.String())
	}
}

// assertJSON fails unless the response is rendered as JSON, which every
// response this package writes is.
func assertJSON(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if got := recorder.Header().Get("Content-Type"); got != contentTypeJSON {
		t.Fatalf("Content-Type = %q, want %q", got, contentTypeJSON)
	}
}

// submitBody is a well-formed submission, so that a test changing one thing
// about a request changes one thing.
const submitBody = `{
	"provider": "acme",
	"externalTransactionId": "acme-tx-1",
	"playerId": "player-1",
	"roundId": "round-1",
	"gameId": "game-1",
	"kind": "BET",
	"money": {"amount": "25.00", "currency": "BRL"}
}`

// openWalletBody is a well-formed wallet opening.
const openWalletBody = `{"playerId": "player-1", "initialBalance": {"amount": "25.00", "currency": "BRL"}}`

// submitRequest builds a submission carrying an idempotency key.
func submission(body string) request {
	return request{
		method:  http.MethodPost,
		path:    "/wagering/transactions",
		body:    body,
		headers: map[string]string{idempotencyKeyHeader: "acme-key-1"},
	}
}
