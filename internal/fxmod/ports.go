package fxmod

import (
	"context"
	"log/slog"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The four ports nothing else in this tree can supply.
//
// A clock, an identifier source and two observers are all things the
// application layer takes so that a test can fix them, and production has
// exactly one answer for each. They live here rather than in an adapter because
// there is no outside system on the other side of any of them: the wall clock,
// the UUID minting the domain already owns, and the logger this process was
// built with.

// systemClock is the wall clock, at the resolution [app.Clock] asks for.
//
// UTC and truncated to microseconds, because that is what timestamptz keeps: a
// value stored at nanosecond precision does not compare equal to itself when it
// is read back, and the one place that matters is an idempotent retry deciding
// whether it is looking at its own earlier write.
type systemClock struct{}

// Now reports the current time.
func (systemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// mintedIDs mints a fresh identifier on every call.
//
// Every one of them is a UUIDv7, whose leading bits are the timestamp — so rows
// arrive in the order they were made and an index on the primary key does not
// fragment. Nothing here derives an identifier from anything a provider sent.
type mintedIDs struct{}

// WalletID mints a wallet identifier.
func (mintedIDs) WalletID() wagering.WalletID { return wagering.NewWalletID() }

// TransactionID mints a wager transaction identifier.
func (mintedIDs) TransactionID() wagering.TransactionID { return wagering.NewTransactionID() }

// LedgerEntryID mints a ledger entry identifier.
func (mintedIDs) LedgerEntryID() wagering.LedgerEntryID { return wagering.NewLedgerEntryID() }

// EventID mints an outbox event identifier.
func (mintedIDs) EventID() app.EventID { return app.NewEventID() }

// loggedDefects is the hook for a parked operation that could not be carried
// forward for a reason that is this system's fault.
//
// It logs at error, and that is the whole of it: the port returns nothing and
// cannot fail the call, because the operation stays parked either way. What an
// absent hook would cost is the reason this is not nil — the provider is not
// waiting on the resume path and the row does not fail, so without this the
// condition is invisible until the wait budget runs out and the operation is
// rejected for a reason that was never true. app.NewWagering refuses a nil one
// for exactly that reason.
type loggedDefects struct{ logger *slog.Logger }

// CannotCarryForward reports one such operation.
func (d loggedDefects) CannotCarryForward(
	ctx context.Context, id wagering.TransactionID, err error,
) {
	d.logger.ErrorContext(ctx, "a parked operation could not be carried forward",
		slog.String("transactionId", id.String()),
		slog.String("error", err.Error()))
}

// loggedDivergences is the hook for a wallet whose stored balance and ledger
// disagree.
//
// The amounts are rendered with money.Money's own String, which is a
// two-decimal string built from minor units; there is no float on this path
// either, and a divergence reported as 10.299999999999999 would be a divergence
// nobody could match against a row.
//
// Error rather than warn. A reconciliation that finds a difference has found
// either a defect in this service or a write nobody made through it, and both
// are worth waking somebody for.
type loggedDivergences struct{ logger *slog.Logger }

// WalletDiverged reports one divergence.
func (o loggedDivergences) WalletDiverged(ctx context.Context, d app.Divergence) {
	o.logger.ErrorContext(ctx, "a wallet does not balance against its ledger",
		slog.String("walletId", d.WalletID.String()),
		slog.String("stored", d.Stored.String()),
		slog.String("reconstructed", d.Reconstructed.String()),
		slog.String("difference", d.Difference.String()))
}
