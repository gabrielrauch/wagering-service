package httpapi

import (
	"net/http"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/app"
)

// principalHandler is a handler that runs only once a principal exists.
//
// The principal is a parameter rather than something fished out of the context,
// so a route registered without authentication does not compile rather than
// answering with the zero principal — which authorises nothing, but does so at
// the application layer, after the request has already reached it.
type principalHandler func(w http.ResponseWriter, r *http.Request, principal app.Principal)

// authenticated turns a credential into a principal, or answers without ever
// reaching the application layer.
//
// Nothing is read, written or counted against a wallet for a request that gets
// this far and no further: the authenticator does no I/O on this service's
// database, and the handler below it is not called. That is the guarantee
// "unauthorised calls produce no database writes" rests on, and it is the
// cheapest possible one — there is nothing to undo because nothing was done.
func (a *API) authenticated(h principalHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		credential, err := oidc.BearerToken(r.Header.Get("Authorization"))
		if err != nil {
			a.unauthenticated(w, r, err)
			return
		}
		principal, err := a.authenticator.Authenticate(r.Context(), credential)
		if err != nil {
			// The one error from an authenticator that is not a refusal is the
			// caller's own context ending, which classifies Retryable. A caller
			// that hung up was not refused, and answering 401 to it would
			// record an authentication failure that never happened — and would
			// put a credential refusal in the log for whoever is counting them.
			if app.ClassOf(err) != app.Unauthorized {
				a.fail(w, r, err)
				return
			}
			a.unauthenticated(w, r, err)
			return
		}
		h(w, r, principal)
	})
}
