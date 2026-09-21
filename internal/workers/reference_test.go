package workers

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// workerOver builds a reference worker with the test defaults filled in and
// stops it when the test ends.
func workerOver(t *testing.T, ctx context.Context, cfg ReferenceConfig) *ReferenceWorker {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "reference-worker"
	}
	if cfg.Logger == nil {
		cfg.Logger = discard()
	}
	if cfg.Interval == 0 {
		cfg.Interval = time.Millisecond
	}
	if cfg.Backoff == (Backoff{}) {
		cfg.Backoff = Backoff{Initial: time.Millisecond, Factor: 2, Max: 10 * time.Millisecond}
	}
	worker, err := NewReferenceWorker(cfg)
	if err != nil {
		t.Fatalf("build a reference worker: %v", err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatalf("start the worker: %v", err)
	}
	t.Cleanup(func() { _ = worker.Stop(context.WithoutCancel(ctx)) })
	return worker
}

// Nothing due is the worker's normal state, and a worker that treated it as a
// failure would alert on its own ordinary operation.
func TestNothingDueIsNotAFailure(t *testing.T) {
	log := &recorder{}
	resumer := newFakeResumer(func(int) (app.ResumeOutcome, error) {
		return app.ResumeOutcome{Claimed: false}, nil
	})
	workerOver(t, t.Context(), ReferenceConfig{Wagering: resumer, Logger: log.logger()})
	resumer.awaitTurns(t, 3)

	for _, unwanted := range []string{
		"a resume turn failed and will be tried again",
		"a resume turn failed for a reason retrying will not fix",
		"a parked operation was carried forward",
		"a parked operation is still waiting for its reference",
	} {
		if log.find(unwanted) != nil {
			t.Errorf("an idle worker said %q", unwanted)
		}
	}
}

// The resume door claims at most one operation per call, so a turn that claimed
// one goes straight round again rather than waiting out an interval with work
// still due.
func TestAClaimedTurnIsFollowedImmediately(t *testing.T) {
	resumer := newFakeResumer(func(n int) (app.ResumeOutcome, error) {
		if n < 3 {
			return app.ResumeOutcome{Claimed: true, Result: settledResult()}, nil
		}
		return app.ResumeOutcome{}, nil
	})
	// An interval no test could wait out, so a fourth turn can only have
	// happened because the worker did not wait between the first three.
	workerOver(t, t.Context(), ReferenceConfig{Wagering: resumer, Interval: time.Hour})
	resumer.awaitTurns(t, 4)

	if got := resumer.count(); got < 4 {
		t.Errorf("turns = %d, want the worker to have carried on while work was due", got)
	}
}

// What one claimed turn came to is two different facts, and they are reported
// as two.
func TestWhatAClaimedTurnReports(t *testing.T) {
	next := testTime().Add(30 * time.Second)

	cases := []struct {
		name    string
		outcome app.ResumeOutcome
		want    string
		// says names an attribute the line has to carry, and what it must say.
		says  string
		value string
	}{
		{
			name: "an operation still waiting names its next attempt",
			outcome: app.ResumeOutcome{
				Claimed:       true,
				Rescheduled:   true,
				NextAttemptAt: next,
				Result: app.OperationResult{
					TransactionID: wagering.NewTransactionID(),
					Kind:          wagering.Rollback,
					Status:        wagering.PendingReference,
				},
			},
			want:  "a parked operation is still waiting for its reference",
			says:  "status",
			value: "PENDING_REFERENCE",
		},
		{
			name: "an operation that was processed is carried forward",
			outcome: app.ResumeOutcome{
				Claimed: true,
				Result:  settledResult(),
				Woke:    2,
			},
			want:  "a parked operation was carried forward",
			says:  "woke",
			value: "2",
		},
		{
			name: "an exhausted wait budget is an ordinary settled outcome",
			outcome: app.ResumeOutcome{
				Claimed: true,
				Result: app.OperationResult{
					TransactionID: wagering.NewTransactionID(),
					Kind:          wagering.Rollback,
					Status:        wagering.Rejected,
					FailureCode:   failure.ReferenceNotFound,
				},
			},
			want:  "a parked operation was carried forward",
			says:  "failureCode",
			value: failure.ReferenceNotFound.String(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			log := &recorder{}
			resumer := newFakeResumer(func(n int) (app.ResumeOutcome, error) {
				if n == 0 {
					return c.outcome, nil
				}
				return app.ResumeOutcome{}, nil
			})
			workerOver(t, t.Context(), ReferenceConfig{
				Wagering: resumer, Logger: log.logger(), Interval: time.Hour,
			})
			resumer.awaitTurns(t, 2)

			record := log.await(t, c.want)
			if got, ok := attr(record, c.says); !ok || got != c.value {
				t.Errorf("%s = %q, want %q", c.says, got, c.value)
			}
			if c.outcome.Rescheduled {
				if got, ok := attr(record, "nextAttemptAt"); !ok || got == "" {
					t.Error("a rescheduled operation's line does not say when it is next due")
				}
			}
			// A settled turn is not an alert: nothing here is a failure.
			if log.find("a resume turn failed and will be tried again") != nil {
				t.Error("a settled outcome was reported as a failure")
			}
		})
	}
}

// A turn that failed is reported at the level its class deserves and the worker
// keeps going.
func TestAFailedTurnIsReportedAndRetried(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "a retryable failure is a warning",
			err:  app.AsRetryable(errors.New("the connection was lost")),
			want: "a resume turn failed and will be tried again",
		},
		{
			name: "anything else needs somebody to look",
			err:  app.AsUnretryable(errors.New("the row could not be rebuilt")),
			want: "a resume turn failed for a reason retrying will not fix",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			log := &recorder{}
			resumer := newFakeResumer(func(n int) (app.ResumeOutcome, error) {
				if n == 0 {
					return app.ResumeOutcome{}, c.err
				}
				return app.ResumeOutcome{}, nil
			})
			workerOver(t, t.Context(), ReferenceConfig{Wagering: resumer, Logger: log.logger()})
			resumer.awaitTurns(t, 2)

			log.await(t, c.want)
			if got := resumer.count(); got < 2 {
				t.Errorf("turns = %d, want the worker to have tried again", got)
			}
		})
	}
}

// The worker acts as the service, which is the only identity the application
// layer lets resume at all.
func TestTheWorkerResumesAsTheService(t *testing.T) {
	resumer := newFakeResumer(nil)
	workerOver(t, t.Context(), ReferenceConfig{Wagering: resumer, Name: "reference-worker"})
	resumer.awaitTurns(t, 1)

	seen := resumer.actedAs()

	if got := seen.Kind(); got != app.ServicePrincipal {
		t.Errorf("principal kind = %q, want %q", got, app.ServicePrincipal)
	}
	if err := seen.MayResume(); err != nil {
		t.Errorf("the worker's principal may not resume: %v", err)
	}
	if got := seen.Subject(); got != "reference-worker" {
		t.Errorf("subject = %q, want the worker's name", got)
	}
}

func TestStoppingTheWorkerEndsTheLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	resumer := newFakeResumer(nil)
	worker, err := NewReferenceWorker(ReferenceConfig{
		Wagering: resumer, Name: "reference-worker", Logger: discard(),
		Interval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	resumer.awaitTurns(t, 2)

	if err := worker.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	stopped := resumer.count()
	time.Sleep(20 * time.Millisecond)
	if got := resumer.count(); got != stopped {
		t.Errorf("the worker took %d more turns after it was stopped", got-stopped)
	}
	if err := worker.Start(ctx); err == nil {
		t.Error("a stopped worker was started again")
	}
}

// settledResult is an operation that reached a terminal status.
func settledResult() app.OperationResult {
	return app.OperationResult{
		TransactionID: wagering.NewTransactionID(),
		Kind:          wagering.Rollback,
		Status:        wagering.Processed,
	}
}

// The reference worker's Stop is bounded the same way. There is nothing to give
// back — a resume claim lasts only as long as its transaction — so the deadline
// is the whole of what Stop owes the caller.
func TestTheWorkerStopsEvenWhenATurnIgnoresCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var (
		admitted = make(chan struct{})
		stuck    = make(chan struct{})
		finished = make(chan struct{})
		once     sync.Once
	)
	resumer := newFakeResumer(func(int) (app.ResumeOutcome, error) {
		once.Do(func() {
			close(admitted)
			<-stuck
			close(finished)
		})
		return app.ResumeOutcome{}, nil
	})
	worker, err := NewReferenceWorker(ReferenceConfig{
		Wagering: resumer, Name: "reference-worker", Logger: discard(),
		Interval: time.Hour, DrainTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-admitted

	returned := make(chan error, 1)
	go func() { returned <- worker.Stop(ctx) }()

	select {
	case stopped := <-returned:
		if stopped == nil {
			t.Fatal("a shutdown against a turn that never came back reported success")
		}
		if !strings.Contains(stopped.Error(), "still running") {
			t.Errorf("the shutdown error %q does not say a turn was still running", stopped)
		}
	case <-time.After(settledWithin):
		t.Fatal("Stop never returned")
	}

	close(stuck)
	<-finished
}
