package httpapi

import (
	"net/http"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// idempotencyKeyHeader carries the provider's key for a submission.
//
// A header rather than a body member, because it is about this delivery of the
// operation rather than about the operation — see [submitRequest].
const idempotencyKeyHeader = "Idempotency-Key"

// submitOperation records one operation and applies it, idempotently.
//
// Nothing a provider sent is touched on the way through. Every field, including
// the amount and the currency, is handed over as the bytes that arrived: the
// application layer parses a submission exactly once so that HTTP and SQS
// cannot disagree about what it said, and the idempotency hash is taken over
// those parsed values.
func (a *API) submitOperation(w http.ResponseWriter, r *http.Request, principal app.Principal) {
	// Before the body is read. A submission with no key cannot be made
	// idempotent whatever it says, and reading a body in order to refuse it is
	// work a provider that forgot a header should not be charged for.
	key := r.Header.Get(idempotencyKeyHeader)
	if key == "" {
		a.fail(w, r, failure.New(failure.MissingRequiredField,
			"a submission must carry the provider's key for it").WithField(idempotencyKeyHeader))
		return
	}

	var body submitRequest
	if err := decodeBody(r, &body); err != nil {
		a.fail(w, r, err)
		return
	}

	result, err := a.wagering.Submit(r.Context(), app.SubmitOperation{
		Principal:   principal,
		Correlation: correlationFrom(r.Context()),
		// Inbox is nil: an HTTP request is not a queue message, has no message
		// identity to deduplicate on, and is made idempotent by the key above.
		Fields: app.OperationFields{
			Provider:                       body.Provider,
			ExternalTransactionID:          body.ExternalTransactionID,
			IdempotencyKey:                 key,
			PlayerID:                       body.PlayerID,
			RoundID:                        body.RoundID,
			GameID:                         body.GameID,
			Kind:                           body.Kind,
			Amount:                         body.Money.Amount,
			Currency:                       body.Money.Currency,
			ReferenceExternalTransactionID: body.ReferenceExternalTransactionID,
		},
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.operation(w, r, result)
}

// readOperation reads one operation by this system's identifier for it.
//
// There is no provider check here. The application layer scopes the read from
// the principal, and answers a provider asking about somebody else's operation
// exactly as it answers one asking about an operation that does not exist —
// which is what keeps this route from being an oracle a provider could walk a
// competitor's identifiers through.
func (a *API) readOperation(w http.ResponseWriter, r *http.Request, principal app.Principal) {
	id, err := wagering.ParseTransactionID(r.PathValue("transactionId"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	result, err := a.wagering.TransactionByID(r.Context(), principal, id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.operation(w, r, result)
}

// readProviderOperation reads one operation by the provider's identifier for
// it.
//
// The provider in the path is checked against the provider the token names, and
// nothing else is done with it: the read itself is scoped by the principal, so
// the path segment can only ever agree with the token or be refused.
//
// A mismatch is 403 rather than 404, although a refused read of somebody else's
// operation is 404 everywhere else here. The two are different questions. This
// one is answered from the token alone, before anything is looked up, so it is
// the same answer for every identifier and reveals nothing about any of them; a
// 404 would instead say the operation is absent, which this route never went to
// find out.
func (a *API) readProviderOperation(
	w http.ResponseWriter, r *http.Request, principal app.Principal,
) {
	// A principal that names no provider is not refused here. It is refused by
	// the application layer, which owns that rule: reading by a provider's own
	// identifier is a provider's door, and an internal caller reads the same
	// operation by this system's identifier for it instead.
	if provider, isProvider := principal.Provider(); isProvider &&
		provider.String() != r.PathValue("providerId") {
		a.fail(w, r, &app.Error{Class: app.Unauthorized})
		return
	}
	external, err := wagering.NewExternalTransactionID(r.PathValue("externalTransactionId"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	result, err := a.wagering.TransactionByExternalID(r.Context(), principal, external)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.operation(w, r, result)
}
