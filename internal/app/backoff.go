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

// validate refuses a policy that cannot mean what it says.
//
// The factor that is not a number is the case worth writing down, because
// nothing else here catches it and its effect is the opposite of a backoff. NaN
// is not less than one, math.Pow carries it through, the min against Max
// propagates it, and converting it to a Duration yields zero — so the first
// attempt is scheduled normally, because Pow(x, 0) is 1 for any x, and every
// attempt after it is scheduled for now, for ever. A parked operation would
// then be looked at as fast as the worker could ask until its wait budget ran
// out, which is the one failure a schedule has to be incapable of.
//
// It is reachable without anybody typing it. strconv.ParseFloat("NaN", 64)
// succeeds with no error, so a factor read from a deployment's environment
// arrives here. The configuration loader refuses it too, naming the offending
// variable, which is the report an operator can act on; this is the invariant
// stated where the value is used, so a caller that never went through the
// loader cannot get past it.
//
// An infinite factor needs no case of its own. [BackoffPolicy.next] takes the
// smaller of the scaled delay and Max, so +Inf becomes Max, which is what a
// factor growing without bound should come to.
func (p BackoffPolicy) validate() error {
	if p.Initial <= 0 {
		return defect("a backoff policy needs a positive initial delay, got %s", p.Initial)
	}
	if math.IsNaN(p.Factor) {
		return defect("a backoff factor that is not a number leaves every wait after the "+
			"first at zero, got %v", p.Factor)
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
