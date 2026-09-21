package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/faults"
)

// What a [ReferenceConfig] leaves open.
const (
	// defaultResumeInterval is how long the worker waits when nothing was due.
	// A second: an operation waiting on a reference is a provider waiting for
	// an answer, and a plain unlocked SELECT once a second costs nothing
	// measurable.
	defaultResumeInterval = time.Second

	// The default backoff, which here is only ever applied to the worker's own
	// failures — a lost connection, a lock conflict — and never to an operation.
	// How long an operation waits is the application layer's schedule and the
	// domain's budget; see [ReferenceWorker].
	defaultResumeInitial = 2 * time.Second
	defaultResumeFactor  = 2
	defaultResumeMax     = 60 * time.Second
)

// ReferenceConfig is everything the reference worker is built from.
type ReferenceConfig struct {
	// Wagering is the application's worker door. Required.
	Wagering Resumer
	// Name is the subject the service principal acts under, kept for the audit
	// trail and never for a decision. Required.
	Name string
	// Interval is how long the worker waits when nothing was due. Zero means
	// [defaultResumeInterval]. A turn that carried an operation forward does
	// not wait, because there may be another behind it.
	Interval time.Duration
	// Backoff is how long the worker waits after a turn that failed. The zero
	// value means the documented default; anything else is taken as given and
	// checked in full.
	//
	// It is the worker's own schedule and not the operation's. An operation
	// that could not be carried forward has already been rescheduled inside the
	// transaction that tried, on the application layer's policy.
	Backoff Backoff
	// DrainTimeout is how long [ReferenceWorker.Stop] waits for the turn in
	// progress. Zero means [defaultDrainTimeout].
	DrainTimeout time.Duration
	// Logger is where the worker reports what it did. Required.
	Logger *slog.Logger
}

// ReferenceWorker carries operations that are waiting for a reference forward.
//
// One turn is one call to [app.Wagering.Resume], which claims at most one due
// operation, re-reads it under its wallet's lock, and either settles it or
// schedules the next attempt — all inside one transaction. This worker decides
// when to make that call and nothing about what it does.
//
// # Where the numbers live
//
// Three schedules meet on this path and only one of them is configured here:
//
//   - The worker's poll interval and failure backoff are [ReferenceConfig]'s.
//     They are about this loop and have no effect on any operation.
//   - How long an operation waits between attempts is app.BackoffPolicy, given
//     to the application layer by the composition root. The schedule is kept
//     apart from the budget below so that tuning it cannot change whether an
//     operation is eventually settled.
//   - How many attempts an operation gets and how long it may wait at all are
//     wagering.ReferencePolicy's MaxAttempts and TTL, given to the domain's
//     processor. When either runs out the operation is settled — rejected for
//     the reference it waited for, with a row and an event — and this worker
//     sees that as an ordinary outcome, because that is what it is.
type ReferenceWorker struct {
	wagering  Resumer
	principal app.Principal
	name      string
	interval  time.Duration
	drain     time.Duration
	backoff   Backoff
	logger    *slog.Logger

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewReferenceWorker wires the reference worker.
//
// The principal is built here rather than taken as a dependency, so that a
// worker holding an identity that may not resume cannot be constructed at all.
// It performs no I/O and resumes nothing; work begins at
// [ReferenceWorker.Start].
func NewReferenceWorker(cfg ReferenceConfig) (*ReferenceWorker, error) {
	switch {
	case cfg.Wagering == nil:
		return nil, errors.New("workers: the reference worker needs a wagering service")
	case cfg.Logger == nil:
		return nil, errors.New("workers: the reference worker needs a logger")
	case cfg.Interval < 0:
		return nil, fmt.Errorf("workers: the reference worker needs a positive interval, got %s",
			cfg.Interval)
	case cfg.DrainTimeout < 0:
		return nil, fmt.Errorf(
			"workers: the reference worker needs a positive drain timeout, got %s",
			cfg.DrainTimeout)
	}
	principal, err := app.NewServicePrincipal(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("workers: the reference worker needs a principal: %w", err)
	}

	backoff := cfg.Backoff
	if backoff == (Backoff{}) {
		backoff = Backoff{
			Initial: defaultResumeInitial,
			Factor:  defaultResumeFactor,
			Max:     defaultResumeMax,
		}
	}
	if err := backoff.validate("reference worker"); err != nil {
		return nil, err
	}

	return &ReferenceWorker{
		wagering:  cfg.Wagering,
		principal: principal,
		name:      cfg.Name,
		interval:  orDefaultDuration(cfg.Interval, defaultResumeInterval),
		drain:     orDefaultDuration(cfg.DrainTimeout, defaultDrainTimeout),
		backoff:   backoff,
		logger:    cfg.Logger,
	}, nil
}

// Start begins resuming. A worker is started once.
func (w *ReferenceWorker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.stopped:
		return errors.New("workers: the reference worker has already been stopped")
	case w.started:
		return errors.New("workers: the reference worker is already running")
	}

	runCtx, cancel := context.WithCancel(ctx)
	w.cancel, w.started = cancel, true
	w.wg.Add(1)
	go w.run(runCtx)

	w.logger.InfoContext(ctx, "carrying parked operations forward",
		slog.String("worker", w.name),
		slog.Duration("interval", w.interval))
	return nil
}

// Stop ends the loop and waits for the turn in progress, within the drain
// deadline.
//
// There is nothing to give back. A resume claim lasts exactly as long as the
// transaction that took it, so a worker that stops mid-turn leaves the row
// parked and due, and the next worker to look finds it — which is the same thing
// that happens when one crashes. That is also why a turn that overruns the
// deadline is reported rather than waited on: there is no state here that
// waiting longer would protect.
func (w *ReferenceWorker) Stop(ctx context.Context) error {
	w.mu.Lock()
	if !w.started || w.stopped {
		w.mu.Unlock()
		return nil
	}
	w.stopped = true
	cancel := w.cancel
	w.mu.Unlock()

	cancel()
	finished := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(finished)
	}()
	if !awaitStopped(ctx, finished, w.drain) {
		w.logger.WarnContext(ctx, "the drain deadline passed with a resume turn still running",
			slog.String("worker", w.name))
		return fmt.Errorf("workers: the reference worker's drain deadline of %s passed with a "+
			"turn still running", w.drain)
	}
	w.logger.InfoContext(ctx, "stopped carrying parked operations forward",
		slog.String("worker", w.name))
	return nil
}

// run is the worker's loop.
//
// A turn that claimed something goes straight round again — there may be another
// operation due, and Resume takes at most one. A turn that claimed nothing waits
// the interval, and a turn that failed waits the backoff for however many turns
// have failed in a row.
//
// The consecutive-failure count is in memory, and that is not the in-memory
// state this system forbids. It decides when this process next asks a question,
// not whether any work has been done: it is discarded when the process stops and
// nothing reads it but the timer below.
func (w *ReferenceWorker) run(ctx context.Context) {
	defer w.wg.Done()

	failures := 0
	for ctx.Err() == nil {
		claimed, err := w.turn(ctx)
		switch {
		case err != nil:
			failures++
			if !wait(ctx, w.backoff.after(failures)) {
				return
			}
		case claimed:
			failures = 0
		default:
			failures = 0
			if !wait(ctx, w.interval) {
				return
			}
		}
	}
}

// turn is one call to the resume door, and reports whether it claimed anything.
func (w *ReferenceWorker) turn(ctx context.Context) (bool, error) {
	outcome, err := w.wagering.Resume(ctx, w.principal)
	if err != nil {
		if ctx.Err() != nil {
			// The worker is stopping. Reported as a failure so the loop stops
			// rather than spinning, and not logged, because a cancelled turn is
			// a shutdown and not a fault.
			return false, err
		}
		class := app.ClassOf(err)
		attrs := []any{
			slog.String("worker", w.name),
			slog.String("class", string(class)),
			slog.String("error", err.Error()),
		}
		if class == app.Retryable {
			w.logger.WarnContext(ctx, "a resume turn failed and will be tried again", attrs...)
		} else {
			w.logger.ErrorContext(ctx, "a resume turn failed for a reason retrying will not fix",
				attrs...)
		}
		return false, err
	}
	if !outcome.Claimed {
		// Nothing was due, or the candidate stopped being due before its lock
		// was taken. Neither is a failure, and a worker that treated one as one
		// would alert on its own normal operation — which is why this returns
		// no error and says nothing.
		return false, nil
	}

	// The transaction has committed: the operation is settled, or parked again
	// with its next attempt written. A process killed here must find the
	// operation again when it comes back.
	faults.Hit(faults.AfterPendingCommit)
	w.report(ctx, outcome)
	return true, nil
}

// report says what one claimed turn came to.
//
// Two lines rather than one, because the two outcomes are different facts. An
// operation that was rescheduled is still waiting and will be looked at again;
// one that was not has reached a status it will never leave — including the
// rejection that ends a wait budget, which arrives here as an ordinary settled
// outcome because that is exactly what it is.
func (w *ReferenceWorker) report(ctx context.Context, outcome app.ResumeOutcome) {
	attrs := []any{
		slog.String("worker", w.name),
		slog.String("transactionId", outcome.Result.TransactionID.String()),
		slog.String("kind", outcome.Result.Kind.String()),
		slog.String("status", outcome.Result.Status.String()),
		slog.Int("woke", outcome.Woke),
	}
	if outcome.Rescheduled {
		w.logger.InfoContext(ctx, "a parked operation is still waiting for its reference",
			append(attrs, slog.Time("nextAttemptAt", outcome.NextAttemptAt))...)
		return
	}
	w.logger.InfoContext(ctx, "a parked operation was carried forward",
		append(attrs, slog.String("failureCode", outcome.Result.FailureCode.String()))...)
}
