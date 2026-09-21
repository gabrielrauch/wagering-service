package workers

import (
	"context"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/adapters/postgres"
	"github.com/gabrielrauch/wagering-service/internal/adapters/sqs"
	"github.com/gabrielrauch/wagering-service/internal/app"
)

// The seams these loops are built on.
//
// Every one of them is declared here rather than taken as the concrete type
// that satisfies it, and names exactly the calls the loop above makes. That is
// what lets a unit test drive a whole loop — its ordering, its backoff, its
// shutdown — without a container, and it is why each interface is as narrow as
// it is: a method admitted here that no loop calls would be a door this package
// advertises and does not use.
//
// The message and event types they carry are the adapters' own. Restating those
// as types of this package's would make the composition root translate between
// two spellings of the same record, and a translation is a place for a receipt
// handle to be attached to the wrong body.

// InboundQueue is the part of the queue adapter the consumer reaches.
// *sqs.Queue satisfies it.
type InboundQueue interface {
	// Receive takes the next batch, waiting the adapter's configured long-poll
	// time. An empty batch is not an error.
	Receive(ctx context.Context) ([]sqs.Message, error)
	// Delete removes a message, and is called only after the transaction that
	// handled it has committed.
	Delete(ctx context.Context, receiptHandle string) error
	// ChangeVisibility hides a message for a further duration, counted from
	// now. This is how a transient failure is handed back.
	ChangeVisibility(ctx context.Context, receiptHandle string, in time.Duration) error
	// Release hands a message straight back for immediate redelivery. This is
	// the shutdown path.
	Release(ctx context.Context, receiptHandle string) error
}

// OutboundQueue is the part of the queue adapter the publisher reaches.
// *sqs.Queue satisfies it.
type OutboundQueue interface {
	// SendBatch puts every message on the queue and reports what became of
	// each one, positionally. The error is non-nil only when nothing could be
	// attempted at all.
	SendBatch(ctx context.Context, messages []sqs.Outbound) ([]sqs.SendResult, error)
}

// OutboxClaims is the publisher's side of the outbox. *postgres.OutboxClaims
// satisfies it.
//
// It is not one of the internal/app ports and could not be: that package
// explicitly does not publish, so it declares no interface for this and never
// learns that a publisher exists.
type OutboxClaims interface {
	// Claim takes a bounded batch of due, unpublished, head-of-line events.
	Claim(ctx context.Context, req postgres.ClaimRequest) ([]postgres.ClaimedEvent, error)
	// MarkPublished records that one event reached the queue, reporting
	// whether this call was the one that did it.
	MarkPublished(ctx context.Context, id app.EventID, at time.Time) (bool, error)
	// Reschedule puts an event back with a later next attempt, and reports
	// whether it was still this publisher's to move.
	Reschedule(ctx context.Context, by string, id app.EventID, at time.Time) (bool, error)
	// ReleaseClaims hands back everything this publisher holds.
	ReleaseClaims(ctx context.Context, by string) (int, error)
}

// Submitter is the application's write path as the consumer reaches it.
//
// It is the same door and the same use case the HTTP adapter calls, with an
// inbox identity supplied where HTTP supplies none. *app.Wagering satisfies it.
type Submitter interface {
	Submit(ctx context.Context, cmd app.SubmitOperation) (app.OperationResult, error)
}

// Resumer is the application's worker door as the reference worker reaches it.
// *app.Wagering satisfies it.
type Resumer interface {
	Resume(ctx context.Context, principal app.Principal) (app.ResumeOutcome, error)
}
