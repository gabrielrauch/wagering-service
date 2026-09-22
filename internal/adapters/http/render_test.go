package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// unrenderable is a value no encoder can turn into JSON.
type unrenderable struct{}

func (unrenderable) MarshalJSON() ([]byte, error) {
	return nil, errors.New("this value has no JSON form")
}

// The one answer written without a status already on the wire. It still names
// the request: a caller reporting "your service returned a 500" with nothing to
// name it by is a caller nobody can help.
func TestAResponseThatCannotBeRenderedStillCarriesTheCorrelation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	const supplied = "provider-trace-7f3a"
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/wallets/x", nil)
	r = r.WithContext(withCorrelation(r.Context(), supplied))
	recorder := httptest.NewRecorder()

	h.api.writeJSON(recorder, r, http.StatusOK, unrenderable{})

	assertStatus(t, recorder, http.StatusInternalServerError)
	assertJSON(t, recorder)
	body := errorOf(t, recorder)
	if body.CorrelationID != supplied {
		t.Errorf("correlationId = %q, want %q", body.CorrelationID, supplied)
	}
	if body.Code != string(app.Unretryable) || body.Message != renderFailureMessage {
		t.Errorf("body = %+v, want the render failure in the contract's shape", body)
	}
	if h.logs.find("a response could not be rendered") == nil {
		t.Errorf("nothing recorded the failure:\n%s", h.logs.rendered())
	}
}
