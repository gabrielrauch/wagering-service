package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The log is where the distinctions this package keeps off the wire survive.
// Both of the tests below assert something no response body can: that a
// difference deliberately erased from the answer was nonetheless written down.

// A provider walking another provider's identifiers and a caller asking for
// something absent are one answer. They are two mornings for whoever runs this.
func TestAForeignReadIsCalledOutByNameAndAnOrdinaryMissIsNot(t *testing.T) {
	t.Parallel()
	const foreignLine = "a provider asked for another provider's operation"
	const message = `no operation "0199c0de-0000-7000-8000-000000000001"`

	t.Run("foreign", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.auth.principal = providerPrincipal(t, "acme")
		h.wagering.err = classified(app.NotFound, "", message, app.ErrForeignOperation)

		recorder := h.do(t, request{
			method: http.MethodGet,
			path:   "/wagering/transactions/" + wagering.NewTransactionID().String(),
		})

		assertStatus(t, recorder, http.StatusNotFound)
		record := h.logs.find(foreignLine)
		if record == nil {
			t.Fatalf("a foreign read was logged as an ordinary refusal:\n%s", h.logs.rendered())
		}
		if got := attr(record, "correlationId"); got != recorder.Header().Get(correlationHeader) {
			t.Errorf("the line carries correlation %q, want the request's", got)
		}
		if got := attr(record, "status"); got != "404" {
			t.Errorf("the line carries status %q, want 404", got)
		}
	})

	t.Run("an ordinary miss", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.auth.principal = providerPrincipal(t, "acme")
		h.wagering.err = classified(app.NotFound, "", message)

		h.do(t, request{
			method: http.MethodGet,
			path:   "/wagering/transactions/" + wagering.NewTransactionID().String(),
		})

		if record := h.logs.find(foreignLine); record != nil {
			t.Errorf("an ordinary miss was reported as a provider walking identifiers:\n%s",
				h.logs.rendered())
		}
	})
}

// One sentence to the caller, five reasons in the log. The two refinements are
// also ErrKeyUnavailable, so a branch order that tested the broad one first
// would swallow them and a steady stream of invented key identifiers would be
// indistinguishable from a rotation this process missed.
func TestEachCredentialRefusalIsCountedUnderItsOwnName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		sentinels []error
		want      string
	}{
		{
			name:      "an invented key identifier",
			sentinels: []error{oidc.ErrUnknownKey, oidc.ErrKeyUnavailable},
			want:      "unknown key identifier",
		},
		{
			name:      "a refresh the rate limit turned away",
			sentinels: []error{oidc.ErrRefreshDeclined, oidc.ErrKeyUnavailable},
			want:      "key set refreshed too recently",
		},
		{
			name:      "no usable key, with no refinement",
			sentinels: []error{oidc.ErrKeyUnavailable},
			want:      "no usable signing key",
		},
		{
			name:      "a token naming no principal",
			sentinels: []error{oidc.ErrPrincipalUnresolved},
			want:      "token names no principal",
		},
		{
			name:      "a token this service examined and rejected",
			sentinels: []error{oidc.ErrTokenRejected},
			want:      "token rejected",
		},
		{
			name:      "a refusal carrying no sentinel at all",
			sentinels: nil,
			want:      "unclassified",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.auth.err = refusedCredential(c.sentinels...)

			recorder := h.do(t, submission(submitBody))

			// The caller is told the same thing every time.
			assertStatus(t, recorder, http.StatusUnauthorized)
			if got := errorOf(t, recorder).Message; got != unauthenticatedMessage {
				t.Errorf("message = %q, want the one sentence", got)
			}
			record := h.logs.find("credential refused")
			if record == nil {
				t.Fatalf("nothing recorded the refusal:\n%s", h.logs.rendered())
			}
			if got := attr(record, "reason"); got != c.want {
				t.Errorf("reason = %q, want %q", got, c.want)
			}
		})
	}
}

// A credential never reaches a log line, whether it was accepted or refused.
func TestNoLogLineCarriesTheCredential(t *testing.T) {
	t.Parallel()

	for _, refusal := range []error{nil, refusedCredential(oidc.ErrTokenRejected)} {
		h := newHarness(t)
		h.auth.principal = providerPrincipal(t, "acme")
		h.auth.err = refusal
		h.wagering.result = processed(t, false)

		h.do(t, submission(submitBody))

		if strings.Contains(h.logs.rendered(), testCredential) {
			t.Errorf("a log line carried the credential:\n%s", h.logs.rendered())
		}
	}
}

// An infrastructure failure is reported to an operator, since the caller is
// told nothing but a fixed sentence.
func TestAServerSideFailureIsRecordedWithItsMessage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.wallets.err = app.AsUnretryable(errors.New("the connection went away"))

	h.do(t, request{
		method: http.MethodGet, path: "/wallets/" + wagering.NewWalletID().String(),
	})

	record := h.logs.find("request failed")
	if record == nil {
		t.Fatalf("a 500 was not recorded:\n%s", h.logs.rendered())
	}
	if !strings.Contains(attr(record, "error"), "the connection went away") {
		t.Errorf("the line says %q, want the failure's own message", attr(record, "error"))
	}
}
