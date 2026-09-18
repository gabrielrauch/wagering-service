package wagering

import (
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// ReferencePolicy bounds how long an operation may wait for a transaction it
// depends on.
//
// The domain owns the rule — wait, then give up and reject with
// [failure.ReferenceNotFound] — while the numbers are configuration and arrive
// as a value.
type ReferencePolicy struct {
	// MaxAttempts is how many times an operation may be parked waiting.
	MaxAttempts int
	// TTL is how long after the first wait the operation stops waiting.
	TTL time.Duration
}

// NewReferencePolicy builds a policy, refusing values that would make waiting
// meaningless.
func NewReferencePolicy(maxAttempts int, ttl time.Duration) (ReferencePolicy, error) {
	p := ReferencePolicy{MaxAttempts: maxAttempts, TTL: ttl}
	if err := p.validate(); err != nil {
		return ReferencePolicy{}, err
	}
	return p, nil
}

func (p ReferencePolicy) validate() error {
	if p.MaxAttempts < 1 {
		return failure.New(failure.InvalidFieldFormat, "must be at least 1, got %d", p.MaxAttempts).WithField("maxAttempts")
	}
	if p.TTL <= 0 {
		return failure.New(failure.InvalidFieldFormat, "must be positive, got %s", p.TTL).WithField("ttl")
	}
	return nil
}
