package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLivenessReportsOnTheProcessAndNothingElse(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	postgres := &fakeCheck{err: errors.New("the database is down")}
	h.checks["postgres"] = postgres

	recorder := h.do(t, request{method: http.MethodGet, path: "/health/live"})

	assertStatus(t, recorder, http.StatusOK)
	assertJSON(t, recorder)
	var body healthBody
	bodyOf(t, recorder, &body)
	if body.Status != statusAlive {
		t.Errorf("status = %q, want %q", body.Status, statusAlive)
	}
	// A liveness probe that asked a dependency restarts a healthy process the
	// moment that dependency goes away, and does it to every replica at once.
	if postgres.called() != 0 {
		t.Errorf("liveness ran %d readiness checks", postgres.called())
	}
	if len(body.Checks) != 0 {
		t.Errorf("checks = %v, want none", body.Checks)
	}
}

func TestReadinessReportsEveryCheck(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		postgres    error
		queue       error
		wantStatus  int
		wantOverall string
		wantChecks  map[string]string
	}{
		{
			name:        "everything answering",
			wantStatus:  http.StatusOK,
			wantOverall: statusReady,
			wantChecks:  map[string]string{"postgres": checkPassed, "sqs": checkPassed},
		},
		{
			name:        "one dependency down",
			queue:       errors.New("the queue is unreachable"),
			wantStatus:  http.StatusServiceUnavailable,
			wantOverall: statusUnready,
			wantChecks:  map[string]string{"postgres": checkPassed, "sqs": checkFailed},
		},
		{
			name:        "both down",
			postgres:    errors.New("the database is unreachable"),
			queue:       errors.New("the queue is unreachable"),
			wantStatus:  http.StatusServiceUnavailable,
			wantOverall: statusUnready,
			wantChecks:  map[string]string{"postgres": checkFailed, "sqs": checkFailed},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			postgres, queue := &fakeCheck{err: c.postgres}, &fakeCheck{err: c.queue}
			h.checks["postgres"] = postgres
			h.checks["sqs"] = queue

			recorder := h.do(t, request{method: http.MethodGet, path: "/health/ready"})

			assertStatus(t, recorder, c.wantStatus)
			var body healthBody
			bodyOf(t, recorder, &body)
			if body.Status != c.wantOverall {
				t.Errorf("status = %q, want %q", body.Status, c.wantOverall)
			}
			for name, want := range c.wantChecks {
				if body.Checks[name] != want {
					t.Errorf("check %s = %q, want %q", name, body.Checks[name], want)
				}
			}
			// Every check runs even after one has failed, so an operator is
			// not sent after one dependency while a second is also down.
			if postgres.called() != 1 || queue.called() != 1 {
				t.Errorf("checks ran postgres %d, sqs %d times, want once each",
					postgres.called(), queue.called())
			}
			if c.wantStatus == http.StatusServiceUnavailable &&
				recorder.Header().Get("Retry-After") != retryAfter {
				t.Error("an unready service did not say when to ask again")
			}
		})
	}
}

func TestReadinessDoesNotPublishWhyADependencyIsDown(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.checks["postgres"] = &fakeCheck{
		err: errors.New(`dial tcp 10.0.0.5:5432: connect: connection refused`),
	}

	recorder := h.do(t, request{method: http.MethodGet, path: "/health/ready"})

	assertStatus(t, recorder, http.StatusServiceUnavailable)
	if got := recorder.Body.String(); strings.Contains(got, "10.0.0.5") {
		t.Errorf("readiness published the address of a dependency: %s", got)
	}
}

// blockingCheck never answers on its own. It is how a dependency that accepts a
// connection and then says nothing behaves.
type blockingCheck struct{ released chan struct{} }

func (c *blockingCheck) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.released:
		return nil
	}
}

func TestReadinessIsBoundedByItsOwnBudget(t *testing.T) {
	t.Parallel()
	h := newHarnessWith(t, func(cfg *Config) {
		// Short enough that a test waiting on it is not waiting long, and long
		// enough that a loaded machine does not trip it by accident.
		cfg.ReadinessTimeout = 50 * time.Millisecond
		cfg.Readiness = map[string]ReadinessCheck{
			"sqs": &blockingCheck{released: make(chan struct{})},
		}
	})

	started := time.Now()
	recorder := h.do(t, request{method: http.MethodGet, path: "/health/ready"})
	elapsed := time.Since(started)

	// The answer an orchestrator can do least with is no answer.
	assertStatus(t, recorder, http.StatusServiceUnavailable)
	if elapsed > time.Second {
		t.Errorf("readiness took %s, want it bounded by its own budget", elapsed)
	}
	var body healthBody
	bodyOf(t, recorder, &body)
	if body.Checks["sqs"] != checkFailed {
		t.Errorf("checks = %v, want the dependency that did not answer reported failed", body.Checks)
	}
}

// Serial checks under a shared budget report the slow one AND everything queued
// behind it as failed, and which ones those are depends on the order a map
// happened to range in. Run at the same time, one slow dependency spends the
// budget on its own and every other check still answers for itself.
func TestOneSlowDependencyDoesNotSpendAnotherCheckBudget(t *testing.T) {
	t.Parallel()
	const budget = 200 * time.Millisecond
	slow := &delayedCheck{delay: 4 * budget}
	quick := &delayedCheck{delay: time.Millisecond}

	h := newHarnessWith(t, func(cfg *Config) {
		cfg.ReadinessTimeout = budget
		cfg.Readiness = map[string]ReadinessCheck{"sqs": slow, "postgres": quick}
	})

	begun := time.Now()
	recorder := h.do(t, request{method: http.MethodGet, path: "/health/ready"})
	elapsed := time.Since(begun)

	assertStatus(t, recorder, http.StatusServiceUnavailable)
	var body healthBody
	bodyOf(t, recorder, &body)
	if body.Checks["postgres"] != checkPassed {
		t.Errorf("the quick dependency reported %q, want ok: it answered in a millisecond",
			body.Checks["postgres"])
	}
	if body.Checks["sqs"] != checkFailed {
		t.Errorf("the slow dependency reported %q, want failed", body.Checks["sqs"])
	}
	// Both were asked. A check that never ran is the failure mode a serial
	// loop has and this one must not.
	if slow.called() != 1 || quick.called() != 1 {
		t.Errorf("checks ran slow %d, quick %d times, want once each",
			slow.called(), quick.called())
	}
	// The budget bounds the endpoint, not the sum of the checks.
	if elapsed > 2*budget {
		t.Errorf("readiness took %s, want it bounded by the %s budget", elapsed, budget)
	}
}

func TestReadinessWithNothingToCheckSaysSo(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	recorder := h.do(t, request{method: http.MethodGet, path: "/health/ready"})

	assertStatus(t, recorder, http.StatusOK)
	var body healthBody
	bodyOf(t, recorder, &body)
	if body.Status != statusReady || len(body.Checks) != 0 {
		t.Errorf("body = %+v, want ready with an empty set", body)
	}
}
