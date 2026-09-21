package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// Every request that is refused for its credential, and the one thing they all
// have in common: the application layer never hears about it. That is what
// "unauthorised calls produce no database writes" rests on — there is nothing
// to undo because nothing was done.
func TestACredentialThisServiceWillNotAcceptReachesNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		prepare func(h *harness, req *request)
	}{
		{
			name: "no Authorization header",
			prepare: func(_ *harness, req *request) {
				req.without = append(req.without, "Authorization")
			},
		},
		{
			name: "another scheme",
			prepare: func(_ *harness, req *request) {
				req.headers["Authorization"] = "Basic dXNlcjpwYXNz"
			},
		},
		{
			name: "the scheme with no credential",
			prepare: func(_ *harness, req *request) {
				req.headers["Authorization"] = "Bearer"
			},
		},
		{
			name: "a token the verifier rejected",
			prepare: func(h *harness, _ *request) {
				h.auth.err = refusedCredential(oidc.ErrTokenRejected)
			},
		},
		{
			name: "a signing key this process could not obtain",
			prepare: func(h *harness, _ *request) {
				h.auth.err = refusedCredential(oidc.ErrUnknownKey, oidc.ErrKeyUnavailable)
			},
		},
		{
			name: "a token naming no principal",
			prepare: func(h *harness, _ *request) {
				h.auth.err = refusedCredential(oidc.ErrPrincipalUnresolved)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			req := submission(submitBody)
			c.prepare(h, &req)

			recorder := h.do(t, req)

			assertStatus(t, recorder, http.StatusUnauthorized)
			assertJSON(t, recorder)
			if got := recorder.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer ") {
				t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", got)
			}
			body := errorOf(t, recorder)
			if body.Code != string(app.Unauthorized) {
				t.Errorf("code = %q, want UNAUTHORIZED", body.Code)
			}
			// One sentence for every refusal. Which of them happened is the
			// operator's to know and never the caller's.
			if body.Message != unauthenticatedMessage {
				t.Errorf("message = %q, want %q", body.Message, unauthenticatedMessage)
			}
			if h.wagering.called() != 0 || h.wallets.called() != 0 {
				t.Errorf("the application layer was reached: wagering %d, wallets %d",
					h.wagering.called(), h.wallets.called())
			}
		})
	}
}

// refusedCredential builds the shape internal/adapters/oidc returns: the
// sentinels ride in the chain beside the class, and the rendered message names
// neither.
func refusedCredential(sentinels ...error) error {
	causes := append([]error{&app.Error{Class: app.Unauthorized}}, sentinels...)
	return &classifiedError{message: "oidc: the credential was not accepted", causes: causes}
}

// A caller that hung up was not refused. Answering 401 would record an
// authentication failure that never happened.
func TestACallerThatGaveUpIsNotRecordedAsARefusal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.err = app.AsRetryable(context.Canceled)

	recorder := h.do(t, submission(submitBody))

	assertStatus(t, recorder, http.StatusServiceUnavailable)
	if got := errorOf(t, recorder).Code; got != string(app.Retryable) {
		t.Errorf("code = %q, want RETRYABLE", got)
	}
	if recorder.Header().Get("WWW-Authenticate") != "" {
		t.Error("a caller that hung up was challenged for a credential")
	}
}

func TestTheCredentialReachesTheAuthenticatorAsItArrived(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	req := submission(submitBody)
	// The scheme is matched without regard to case, as RFC 7235 requires, and
	// the credential itself is not touched.
	req.headers["Authorization"] = "bEaReR   a.b.c  "
	h.do(t, req)

	if h.auth.lastCredential != "a.b.c" {
		t.Errorf("the authenticator was given %q, want the credential", h.auth.lastCredential)
	}
}

func TestAnAuthenticatedPrincipalReachesTheUseCase(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.auth.principal = providerPrincipal(t, "acme")
	h.wagering.result = processed(t, false)

	h.do(t, submission(submitBody))

	got, isProvider := h.wagering.submitted().Principal.Provider()
	if !isProvider || got != "acme" {
		t.Errorf("the use case was given %v, want provider acme", h.wagering.submitted().Principal)
	}
}

func TestTheHealthEndpointsNeedNoCredential(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/health/live", "/health/ready"} {
		h := newHarness(t)
		h.auth.err = errors.New("the authenticator must not be asked")

		recorder := h.do(t, request{
			method: http.MethodGet, path: path, without: []string{"Authorization"},
		})

		assertStatus(t, recorder, http.StatusOK)
		if h.auth.calls != 0 {
			t.Errorf("%s asked the authenticator %d times", path, h.auth.calls)
		}
	}
}

// Every route but the health checks, refused for the same reason and reaching
// nothing. Named one by one so that a route added without authentication is a
// failing test rather than an open door.
func TestEveryApplicationRouteNeedsACredential(t *testing.T) {
	t.Parallel()
	id := wagering.NewWalletID().String()
	transaction := wagering.NewTransactionID().String()

	routes := []request{
		{method: http.MethodPost, path: "/wallets", body: openWalletBody},
		{method: http.MethodGet, path: "/wallets/" + id},
		{method: http.MethodGet, path: "/wallets/" + id + "/ledger"},
		{method: http.MethodPost, path: "/wallets/" + id + "/reconciliation"},
		{method: http.MethodPost, path: "/wagering/transactions", body: submitBody},
		{method: http.MethodGet, path: "/wagering/transactions/" + transaction},
		{method: http.MethodGet, path: "/providers/acme/wagering/transactions/acme-tx-1"},
	}
	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			route.without = []string{"Authorization"}

			recorder := h.do(t, route)

			assertStatus(t, recorder, http.StatusUnauthorized)
			if h.wagering.called() != 0 || h.wallets.called() != 0 {
				t.Error("an unauthenticated request reached the application layer")
			}
		})
	}
}
