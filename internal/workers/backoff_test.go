package workers

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	policy := Backoff{Initial: 2 * time.Second, Factor: 2, Max: 30 * time.Second}

	cases := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{name: "an attempt count below one is read as the first", attempt: 0, want: 2 * time.Second},
		{name: "the first delivery waits the initial delay", attempt: 1, want: 2 * time.Second},
		{name: "the second doubles", attempt: 2, want: 4 * time.Second},
		{name: "the third doubles again", attempt: 3, want: 8 * time.Second},
		{name: "the fourth doubles again", attempt: 4, want: 16 * time.Second},
		{name: "the fifth is capped", attempt: 5, want: 30 * time.Second},
		{name: "a far later attempt is still capped", attempt: 40, want: 30 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := policy.after(c.attempt); got != c.want {
				t.Errorf("after(%d) = %s, want %s", c.attempt, got, c.want)
			}
		})
	}
}

// A factor large enough to overflow an int64 must still answer with the cap. A
// wait that wrapped negative is a wait in the past, which is a busy loop.
func TestAnOverflowingBackoffIsStillCapped(t *testing.T) {
	policy := Backoff{Initial: time.Hour, Factor: 1e6, Max: 12 * time.Hour}
	if got := policy.after(20); got != 12*time.Hour {
		t.Errorf("after(20) = %s, want the cap of %s", got, 12*time.Hour)
	}
}

func TestBackoffRefusesAPolicyThatWouldNotBackAnythingOff(t *testing.T) {
	cases := []struct {
		name   string
		policy Backoff
		why    string
	}{
		{
			name:   "no initial delay",
			policy: Backoff{Factor: 2, Max: time.Minute},
			why:    "positive initial delay",
		},
		{
			name:   "a factor below one shortens each wait",
			policy: Backoff{Initial: time.Second, Factor: 0.5, Max: time.Minute},
			why:    "below 1 shortens",
		},
		{
			name:   "a maximum below the initial delay",
			policy: Backoff{Initial: time.Minute, Factor: 2, Max: time.Second},
			why:    "is below its initial delay",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.policy.validate("consumer")
			if err == nil {
				t.Fatal("a policy that backs nothing off was accepted")
			}
			if !strings.Contains(err.Error(), c.why) || !strings.Contains(err.Error(), "consumer") {
				t.Errorf("refusal = %q, want it to name the consumer and say %q", err, c.why)
			}
		})
	}
}

// SQS states its bounds in whole seconds and the queue adapter refuses anything
// finer, so a visibility change has to be rounded up and never to zero.
func TestWholeSecondsRoundsUpAndNeverToZero(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "zero becomes one second", in: 0, want: time.Second},
		{name: "a negative becomes one second", in: -time.Minute, want: time.Second},
		{name: "part of a second becomes one", in: 250 * time.Millisecond, want: time.Second},
		{name: "a whole second is left alone", in: time.Second, want: time.Second},
		{name: "one and a half rounds up", in: 1500 * time.Millisecond, want: 2 * time.Second},
		{name: "two seconds are left alone", in: 2 * time.Second, want: 2 * time.Second},
		{name: "two and a bit rounds up", in: 2001 * time.Millisecond, want: 3 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := wholeSeconds(c.in); got != c.want {
				t.Errorf("wholeSeconds(%s) = %s, want %s", c.in, got, c.want)
			}
		})
	}
}

func TestWaitingEndsWhenTheContextDoes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if wait(ctx, time.Hour) {
		t.Error("a wait on an ended context reported that it finished")
	}
	if !wait(t.Context(), time.Millisecond) {
		t.Error("a wait that ran to its end reported that it did not")
	}
}
