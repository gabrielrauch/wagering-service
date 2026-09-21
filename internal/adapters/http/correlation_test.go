package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

func TestTheCorrelationSuppliedIsTheOneUsedEverywhere(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	const supplied = "provider-trace-7f3a"
	req := submission(submitBody)
	req.headers[correlationHeader] = supplied
	recorder := h.do(t, req)

	assertStatus(t, recorder, http.StatusOK)
	if got := recorder.Header().Get(correlationHeader); got != supplied {
		t.Errorf("echoed %q, want %q", got, supplied)
	}
	if got := h.wagering.submitted().Correlation; got != supplied {
		t.Errorf("the use case was given %q, want %q", got, supplied)
	}
}

func TestACorrelationIsMintedForARequestThatCarriesNone(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	req := submission(submitBody)
	req.without = []string{correlationHeader}
	recorder := h.do(t, req)

	echoed := recorder.Header().Get(correlationHeader)
	if echoed == "" {
		t.Fatal("no correlation was echoed")
	}
	// One value, not two: what the caller is told and what the outbox envelopes
	// and the logs are written under have to be the same thread.
	if got := h.wagering.submitted().Correlation; got != echoed {
		t.Errorf("the use case was given %q while the caller was told %q", got, echoed)
	}
}

func TestTheCorrelationAppearsInTheBodyOfARefusal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.wallets.err = classifiedNotFound()

	const supplied = "provider-trace-7f3a"
	recorder := h.do(t, request{
		method:  http.MethodGet,
		path:    "/wallets/" + wagering.NewWalletID().String(),
		headers: map[string]string{correlationHeader: supplied},
	})

	if got := errorOf(t, recorder).CorrelationID; got != supplied {
		t.Errorf("the body named %q, want %q", got, supplied)
	}
	if got := recorder.Header().Get(correlationHeader); got != supplied {
		t.Errorf("the header echoed %q, want %q", got, supplied)
	}
}

func classifiedNotFound() error {
	return classified("NOT_FOUND", "", "no wallet")
}

func TestACorrelationThatCannotBeUsedIsRefusedRatherThanReplaced(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, supplied string }{
		{"a newline, which would write the log", "trace\nlevel=error msg=\"fake\""},
		{"a control character", "trace\x00id"},
		{"surrounded by whitespace", " trace-1 "},
		{"longer than the column holds", strings.Repeat("t", maxCorrelationBytes+1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.principal = providerPrincipal(t, "acme")

			req := submission(submitBody)
			req.headers[correlationHeader] = c.supplied
			recorder := h.do(t, req)

			assertStatus(t, recorder, http.StatusBadRequest)
			body := errorOf(t, recorder)
			if body.Code != string(failure.InvalidFieldFormat) {
				t.Errorf("code = %q, want INVALID_FIELD_FORMAT", body.Code)
			}
			if !strings.HasPrefix(body.Message, correlationHeader+": ") {
				t.Errorf("message = %q, want it to name the header", body.Message)
			}
			// Refused, and still traceable: the refusal carries a correlation
			// of this service's own rather than the one it would not use.
			if body.CorrelationID == "" || body.CorrelationID == c.supplied {
				t.Errorf("correlationId = %q, want a fresh one", body.CorrelationID)
			}
			if got := recorder.Header().Get(correlationHeader); got != body.CorrelationID {
				t.Errorf("the header echoed %q while the body named %q", got, body.CorrelationID)
			}
			if h.wagering.called() != 0 {
				t.Error("a correlation the database would refuse still reached the use case")
			}
		})
	}
}

func TestEveryResponseCarriesACorrelation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	for _, req := range []request{
		{method: http.MethodGet, path: "/health/live"},
		{method: http.MethodGet, path: "/nothing/here"},
		{method: http.MethodDelete, path: "/wallets"},
	} {
		recorder := h.do(t, req)
		if recorder.Header().Get(correlationHeader) == "" {
			t.Errorf("%s %s answered with no correlation", req.method, req.path)
		}
	}
}
