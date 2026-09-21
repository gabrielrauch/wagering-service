package httpapi

import (
	"context"
	"log/slog"
	"net/http"
)

// What a health endpoint answers with.
const (
	statusAlive   = "alive"
	statusReady   = "ready"
	statusUnready = "unready"
	checkPassed   = "ok"
	checkFailed   = "failed"
)

// healthBody is what both health endpoints answer with.
//
// Checks is absent from a liveness answer, which has nothing to report on but
// the process it is running in.
type healthBody struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitzero"`
}

// live reports that this process is running.
//
// It asks nothing of anything else, and that is the whole of its contract. A
// liveness probe that checked a dependency restarts a healthy process whenever
// that dependency is down — which removes the one thing that was still working
// and, for a dependency every replica shares, does it to all of them at once.
func (a *API) live(w http.ResponseWriter, r *http.Request) {
	a.writeJSON(w, r, http.StatusOK, healthBody{Status: statusAlive})
}

// ready reports whether this process can serve.
//
// Every check is run even after one has failed, so the answer names all of
// them: a body that stopped at the first failure would send an operator after
// one dependency while a second was also down.
//
// They share one budget rather than each having their own. The budget is what
// stops a dependency that accepts a connection and then says nothing from
// holding the probe open forever — the answer an orchestrator can do least with
// is no answer — and a total is the bound that actually bounds the endpoint.
//
// A failing check's error is not in the body. It is logged: a readiness
// endpoint is usually reachable by more of a network than the service is, and
// the reason a database is unreachable is not something to publish there.
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.readinessTimeout)
	defer cancel()

	checks := make(map[string]string, len(a.readiness))
	ready := true
	for name, check := range a.readiness {
		if err := check.Ready(ctx); err != nil {
			ready = false
			checks[name] = checkFailed
			a.logger.WarnContext(ctx, "a readiness check failed",
				slog.String("check", name),
				slog.String("error", err.Error()),
				slog.String("correlationId", correlationFrom(r.Context())))
			continue
		}
		checks[name] = checkPassed
	}

	body := healthBody{Status: statusReady, Checks: checks}
	status := http.StatusOK
	if !ready {
		body.Status = statusUnready
		status = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", retryAfter)
	}
	a.writeJSON(w, r, status, body)
}
