package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
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
// Every check runs, and they run at the same time under one total budget. Both
// halves of that matter and the second is what a serial loop gets wrong. A body
// that stopped at the first failure would send an operator after one dependency
// while a second was also down — but so does a serial loop under a shared
// budget, by another route: the first check to hang spends the whole budget,
// every check behind it is handed a context that is already done, and they are
// all reported failed. Which ones those are then depends on the order a map
// happened to range in, so the same outage produces a different body each time
// it is asked.
//
// The budget itself stays. It is what stops a dependency that accepts a
// connection and then says nothing from holding the probe open forever — the
// answer an orchestrator can do least with is no answer. Fan-out is bounded by
// the number of checks the composition root registered, which is a constant
// this process chose, not something a caller can grow.
//
// A failing check's error is not in the body. It is logged: a readiness
// endpoint is usually reachable by more of a network than the service is, and
// the reason a database is unreachable is not something to publish there.
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.readinessTimeout)
	defer cancel()

	type answer struct {
		name string
		err  error
	}
	answers := make([]answer, len(a.readiness))
	var wg sync.WaitGroup
	i := 0
	for name, check := range a.readiness {
		// One slot each rather than a shared map, so nothing here needs a lock
		// to be race-free: no two goroutines write the same element.
		slot := &answers[i]
		slot.name = name
		i++
		wg.Go(func() { slot.err = check.Ready(ctx) })
	}
	wg.Wait()

	checks := make(map[string]string, len(answers))
	ready := true
	for _, got := range answers {
		if got.err != nil {
			ready = false
			checks[got.name] = checkFailed
			a.logger.WarnContext(ctx, "a readiness check failed",
				slog.String("check", got.name),
				slog.String("error", got.err.Error()),
				slog.String("correlationId", correlationFrom(r.Context())))
			continue
		}
		checks[got.name] = checkPassed
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
