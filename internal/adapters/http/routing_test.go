package httpapi

import (
	"net/http"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The two refusals the mux makes for itself. Left alone they are plain text,
// which is the one place this API would answer in a shape nothing else uses.
func TestTheRoutersOwnRefusalsTakeTheContractsShape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		req       request
		wantCode  int
		wantBody  string
		wantAllow string
	}{
		{
			name:     "a path no route answers",
			req:      request{method: http.MethodGet, path: "/nothing/here"},
			wantCode: http.StatusNotFound,
			wantBody: codeNotFound,
		},
		{
			name:      "a path that answers another method",
			req:       request{method: http.MethodGet, path: "/wallets"},
			wantCode:  http.StatusMethodNotAllowed,
			wantBody:  codeMethodNotAllowed,
			wantAllow: "POST",
		},
		{
			name:      "a wildcard path that answers another method",
			req:       request{method: http.MethodDelete, path: "/wallets/" + wagering.NewWalletID().String()},
			wantCode:  http.StatusMethodNotAllowed,
			wantBody:  codeMethodNotAllowed,
			wantAllow: "GET, HEAD",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)

			recorder := h.do(t, c.req)

			assertStatus(t, recorder, c.wantCode)
			assertJSON(t, recorder)
			body := errorOf(t, recorder)
			if body.Code != c.wantBody {
				t.Errorf("code = %q, want %q", body.Code, c.wantBody)
			}
			if body.CorrelationID == "" {
				t.Error("the router's refusal carried no correlation")
			}
			// Whatever the mux worked out about the route is kept.
			if got := recorder.Header().Get("Allow"); got != c.wantAllow {
				t.Errorf("Allow = %q, want %q", got, c.wantAllow)
			}
		})
	}
}

// A handler's own 404 is not the router's, and must not be restated as one.
func TestAHandlersNotFoundIsLeftAlone(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.wallets.err = classifiedNotFound()

	recorder := h.do(t, request{
		method: http.MethodGet, path: "/wallets/" + wagering.NewWalletID().String(),
	})

	assertStatus(t, recorder, http.StatusNotFound)
	if got := errorOf(t, recorder).Message; got != notFoundMessage {
		t.Errorf("message = %q, want the resource's answer and not the router's", got)
	}
}

func TestABodyLargerThanTheLimitIsRefusedBeforeItIsRead(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")

	oversized := `{"provider":"acme","externalTransactionId":"` + padding(testBodyLimit) + `"}`
	recorder := h.do(t, submission(oversized))

	assertStatus(t, recorder, http.StatusRequestEntityTooLarge)
	assertJSON(t, recorder)
	if got := errorOf(t, recorder).Code; got != codeContentTooLarge {
		t.Errorf("code = %q, want %q", got, codeContentTooLarge)
	}
	if h.wagering.called() != 0 {
		t.Error("an oversized body still reached the use case")
	}
}

func padding(n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = 'x'
	}
	return string(out)
}

// A body within the limit is read normally, so the limit is a limit and not a
// refusal of everything.
func TestABodyWithinTheLimitIsRead(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	recorder := h.do(t, submission(submitBody))

	assertStatus(t, recorder, http.StatusOK)
}
