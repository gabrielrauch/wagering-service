package app

import (
	"encoding/binary"
	"math"
	"time"
	"uuid"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// BackoffPolicy is how long a parked operation waits before it is looked at
// again.
//
// It is this layer's, not the domain's. The domain owns the wait budget — how
// many attempts and for how long, after which the operation is rejected — while
// when to make the next attempt is an operational choice with no business
// meaning. The two are kept apart so that tuning the schedule cannot change
// whether an operation is eventually settled.
type BackoffPolicy struct {
	// Initial is the wait before the second attempt.
	Initial time.Duration
	// Factor multiplies the wait after each attempt, and must be at least 1.
	Factor float64
	// Max caps it.
	Max time.Duration
}

func (p BackoffPolicy) validate() error {
	if p.Initial <= 0 {
		return defect("a backoff policy needs a positive initial delay, got %s", p.Initial)
	}
	if p.Factor < 1 {
		return defect("a backoff factor below 1 shortens each wait, got %v", p.Factor)
	}
	if p.Max < p.Initial {
		return defect("a backoff maximum of %s is below its initial delay of %s", p.Max, p.Initial)
	}
	return nil
}

// jitterFraction is how much of a delay is spread out. A tenth is enough to
// break up a batch of operations parked in the same commit — which is the
// realistic way a herd forms here — without meaningfully changing when any one
// of them is looked at.
const jitterFraction = 0.1

// next is when a parked operation should be looked at again.
//
// The jitter is derived from the transaction's own identifier rather than from a
// random source, so the schedule is reproducible: the same operation parked at
// the same instant always comes back at the same instant, in a test and in
// production. A UUIDv7's tail is random, which is exactly the property wanted.
//
// The result is clamped to the deadline. Waiting past the point where the domain
// will refuse to park the operation again would mean an operation whose budget
// expired at noon is not looked at until one, and is then rejected for having run
// out of time an hour earlier.
func (p BackoffPolicy) next(attempts int, now, deadline time.Time, id wagering.TransactionID) time.Time {
	attempts = max(attempts, 1)
	delay := min(float64(p.Initial)*math.Pow(p.Factor, float64(attempts-1)), float64(p.Max))
	delay *= 1 + jitterFraction*jitterOf(id)

	at := now.Add(time.Duration(delay))
	if !deadline.IsZero() && at.After(deadline) {
		return deadline
	}
	return at
}

// jitterOf reads a fraction in [0, 1) out of the random tail of a UUIDv7.
func jitterOf(id wagering.TransactionID) float64 {
	raw := uuid.UUID(id)
	tail := binary.BigEndian.Uint64(raw[8:])
	return float64(tail%1_000_000) / 1_000_000
}
