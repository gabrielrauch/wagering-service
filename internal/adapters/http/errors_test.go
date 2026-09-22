package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// Every class the application layer can answer with, and what it becomes on the
// wire. Driven through a wallet read because the mapping belongs to the
// package and not to a route.
func TestAFailureBecomesTheStatusAndCodeItsClassNames(t *testing.T) {
	t.Parallel()

	// A real app.Error carrying a class, a code, a field and a message: the
	// shape the classification produces, and the one whose head has to be taken
	// off before the message is rendered.
	_, invalid := app.ParseEventID("nope")
	if invalid == nil {
		t.Fatal("an event id that is not a UUID was accepted")
	}

	cases := []struct {
		name        string
		err         error
		wantStatus  int
		wantCode    string
		wantMessage string
	}{
		{
			name:        "invalid",
			err:         invalid,
			wantStatus:  http.StatusBadRequest,
			wantCode:    string(failure.InvalidFieldFormat),
			wantMessage: `eventId: event id "nope" is not a UUID`,
		},
		{
			name:        "invalid, raised in the domain",
			err:         failure.New(failure.InvalidAmountScale, "must carry two fraction digits").WithField("money"),
			wantStatus:  http.StatusBadRequest,
			wantCode:    string(failure.InvalidAmountScale),
			wantMessage: "money: must carry two fraction digits",
		},
		{
			name:        "conflict",
			err:         failure.New(failure.IdempotencyPayloadConflict, "the key is bound to another payload"),
			wantStatus:  http.StatusConflict,
			wantCode:    string(failure.IdempotencyPayloadConflict),
			wantMessage: "the key is bound to another payload",
		},
		{
			name:        "not found",
			err:         classified(app.NotFound, "", "no wallet 0199"),
			wantStatus:  http.StatusNotFound,
			wantCode:    string(app.NotFound),
			wantMessage: notFoundMessage,
		},
		{
			name:        "unauthorized, by a principal that exists",
			err:         forbidden(t),
			wantStatus:  http.StatusForbidden,
			wantCode:    string(app.Unauthorized),
			wantMessage: "provider acme may not administer wallets",
		},
		{
			name:        "retryable",
			err:         app.AsRetryable(errBoom),
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    string(app.Retryable),
			wantMessage: retryableMessage,
		},
		{
			name:        "unretryable",
			err:         app.AsUnretryable(errBoom),
			wantStatus:  http.StatusInternalServerError,
			wantCode:    string(app.Unretryable),
			wantMessage: unretryableMessage,
		},
		{
			name:        "audit",
			err:         classified(app.Audit, failure.LedgerBalanceMismatch, "the wallet is out by 5.00"),
			wantStatus:  http.StatusInternalServerError,
			wantCode:    string(failure.LedgerBalanceMismatch),
			wantMessage: auditMessage,
		},
		{
			name:        "nobody classified it",
			err:         errBoom,
			wantStatus:  http.StatusInternalServerError,
			wantCode:    string(app.Unretryable),
			wantMessage: unretryableMessage,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.wallets.err = c.err

			recorder := h.do(t, request{
				method: http.MethodGet, path: "/wallets/" + wagering.NewWalletID().String(),
			})

			assertStatus(t, recorder, c.wantStatus)
			assertJSON(t, recorder)
			body := errorOf(t, recorder)
			if body.Code != c.wantCode {
				t.Errorf("code = %q, want %q", body.Code, c.wantCode)
			}
			if body.Message != c.wantMessage {
				t.Errorf("message = %q, want %q", body.Message, c.wantMessage)
			}
			if body.CorrelationID == "" {
				t.Error("the refusal carried no correlation")
			}
		})
	}
}

// An infrastructure failure says where it was trying to connect to. A caller is
// told none of it, whichever side of the retryable line the failure falls on.
func TestAnInfrastructureFailureIsNeverRenderedToACaller(t *testing.T) {
	t.Parallel()
	for _, err := range []error{errBoom, app.AsRetryable(errBoom), app.AsUnretryable(errBoom)} {
		h := newHarness(t)
		h.wallets.err = err

		recorder := h.do(t, request{
			method: http.MethodGet, path: "/wallets/" + wagering.NewWalletID().String(),
		})

		if got := recorder.Body.String(); strings.Contains(got, "10.0.0.5") ||
			strings.Contains(got, "connection refused") {
			t.Errorf("the response rendered the cause: %s", got)
		}
	}
}

func TestOnlyAnUnavailableServiceAsksTheCallerToWait(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"retryable", app.AsRetryable(errBoom), retryAfter},
		{"unretryable", app.AsUnretryable(errBoom), ""},
		{"not found", classified(app.NotFound, "", "no wallet"), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.wallets.err = c.err

			recorder := h.do(t, request{
				method: http.MethodGet, path: "/wallets/" + wagering.NewWalletID().String(),
			})

			if got := recorder.Header().Get("Retry-After"); got != c.want {
				t.Errorf("Retry-After = %q, want %q", got, c.want)
			}
		})
	}
}
