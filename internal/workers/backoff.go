package workers

import (
	"context"
	"fmt"
	"math"
	"time"
)

// Backoff is how long a worker waits before work it could not finish is looked
// at again.
//
// Exponential from Initial, multiplied by Factor once per attempt already made,
// and capped at Max. The cap is not optional and there is no way to leave it
// open: every queue this backoff defers work on is ordered per group, so an
// unbounded wait on one message is an unbounded wait for that wallet.
//
// There is no jitter, unlike [app.BackoffPolicy], and the reason is the unit
// rather than the herd. A consumer's delay becomes an SQS visibility timeout,
// which has a resolution of one second, so the tenth of a delay that policy
// spreads would be rounded away on every wait short of ten seconds. What would
// herd here is bounded anyway: a group holds one delivery at a time and the
// redrive policy ends the message after a handful of them.
//
// See the package documentation for why this is not [app.BackoffPolicy].
type Backoff struct {
	// Initial is the wait after the first failed attempt.
	Initial time.Duration
	// Factor multiplies the wait after each further attempt, and must be at
	// least 1.
	Factor float64
	// Max caps it, and must be at least Initial.
	Max time.Duration
}

// validate refuses a policy that would not back anything off.
//
// what names the worker being built, so that a process refusing to start says
// which of its three loops was misconfigured rather than leaving an operator to
// find out by elimination.
func (b Backoff) validate(what string) error {
	switch {
	case b.Initial <= 0:
		return fmt.Errorf("workers: the %s backoff needs a positive initial delay, got %s",
			what, b.Initial)
	case b.Factor < 1:
		return fmt.Errorf("workers: a %s backoff factor below 1 shortens each wait, got %v",
			what, b.Factor)
	case b.Max < b.Initial:
		return fmt.Errorf("workers: the %s backoff maximum of %s is below its initial delay of %s",
			what, b.Max, b.Initial)
	}
	return nil
}

// after is how long to wait having already made attempt attempts.
//
// The count is the caller's own — ApproximateReceiveCount for a message, the
// outbox row's attempts for an event — and both report one on a first delivery,
// so the first wait is Initial. Anything below one is read as one: a delay
// shorter than the first is not a thing this policy has, and a zero would come
// from a counter that was not set rather than from a delivery that did not
// happen.
func (b Backoff) after(attempt int) time.Duration {
	delay := float64(b.Initial) * math.Pow(b.Factor, float64(max(attempt, 1)-1))
	// Compared as a float before the conversion. A factor large enough to
	// overflow an int64 would wrap to a negative duration, which is a wait in
	// the past — the one value that turns a backoff into a busy loop.
	if delay >= float64(b.Max) {
		return b.Max
	}
	return time.Duration(delay)
}

// wholeSeconds rounds a delay up to the next whole second, with a floor of one.
//
// SQS states every one of its own bounds in seconds and the queue adapter
// refuses anything finer, so this is where a policy expressed in durations meets
// the resolution the queue actually has. Up rather than nearest, because
// rounding down would hand a message back sooner than the backoff asked for; and
// never to zero, because zero is an immediate redelivery and a backoff that
// waits no time at all is a busy loop against the queue.
func wholeSeconds(d time.Duration) time.Duration {
	if d <= time.Second {
		return time.Second
	}
	return (d + time.Second - 1).Truncate(time.Second)
}

// wait sleeps for d, or returns early when ctx ends.
//
// It reports whether the wait finished. A worker that is being stopped gets
// false and returns rather than holding the shutdown open for the rest of a
// poll interval it no longer has any reason to observe.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
