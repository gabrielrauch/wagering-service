package sqs

import (
	httpapi "github.com/gabrielrauch/wagering-service/internal/adapters/http"
)

// The interfaces this package promises to satisfy, asserted at compile time so
// that a signature drifting away from one of them is a build failure here
// rather than a wiring failure in the composition root.
//
// The HTTP layer's readiness map is the promise this settles: /health/ready
// says it reports on PostgreSQL and SQS, and until now only half of that had a
// check that could be put in the map.
var _ httpapi.ReadinessCheck = (*Health)(nil)
