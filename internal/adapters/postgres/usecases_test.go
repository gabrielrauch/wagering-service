//go:build integration

// The application's use cases, end to end, against the real adapter.
//
// Every other test in this package drives one port and asserts on what that
// port did. These four drive app.Wagering and app.Wallets — the services
// production builds, over this adapter's transaction manager and the domain's
// own processor — and assert on what a COMMAND came to. The difference matters:
// a suite of ports that each behave can still add up to a command that leaves a
// balance moved and no ledger entry, and only a test that submits an operation
// can see that.
//
// Nothing here stands in for anything. The clock and the identifier source are
// the two ports this adapter does not implement, and both are real
// implementations a test can steer rather than substitutes for a database.

package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// injection is one way of making a command fail after it has written everything
// it was going to write, and the proof that it did.
type injection struct {
	ids app.IDs
	// ctx is what the submission runs under. A case whose failure comes from
	// the server side leaves it as the test's own.
	ctx context.Context
	// reachedTheEnd asserts that the command really did get past the last of
	// its writes before it failed. Without it a case would prove only that
	// something went wrong, not that there was anything left to undo — and a
	// failure injected before the first write proves nothing at all.
	reachedTheEnd func(t *testing.T, err error)
}

// cancellingIDs mints identifiers and cancels the request as it hands over the
// first event id.
//
// That instant is chosen rather than convenient. EventID is asked for in the
// application's emit, which runs after Settle has written the transaction row,
// the wallet and the ledger entry and before the outbox append — so the command
// fails with its whole write set already in the transaction. It records that it
// was asked, because a test that could not tell "cancelled at the end" from
// "cancelled before anything happened" would be asserting on the wrong failure.
type cancellingIDs struct {
	app.IDs
	cancel context.CancelFunc
	once   sync.Once
	asked  atomic.Bool
}

// EventID mints an event identifier, and on the first call cancels the request
// that asked for it.
func (i *cancellingIDs) EventID() app.EventID {
	i.once.Do(func() {
		i.asked.Store(true)
		i.cancel()
	})
	return app.NewEventID()
}

// TestACommandThatFailsPartwayLeavesNothingBehind is atomicity stated as the
// absence of rows rather than as the presence of an error.
//
// A submission writes into five tables — the inbox, the wallet, the wager
// transaction, the ledger and the outbox — in that order, and the two cases
// below both fail at the far end of it, with everything but the last statement
// already written. What is asserted is that none of it survives: not the row
// the command is about, not the inbox row that claimed the message, and not a
// minor unit of the balance it had already moved.
//
// The failures are engineered, and where they land is the whole of the design.
// One is the database refusing the append; the other is the caller going away
// while the money is moving. Both are ordinary production failures, and both
// leave a full write set behind to be undone.
func TestACommandThatFailsPartwayLeavesNothingBehind(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// inject installs the failure and says what the service should be
		// wired with to meet it.
		inject func(t *testing.T, w *world) injection
	}{
		{
			name: "the database refuses the outbox append",
			inject: func(t *testing.T, w *world) injection {
				// The privilege is taken away rather than a statement broken.
				// The outbox append is the last thing a command does, so a
				// service that may no longer write there fails with the inbox
				// row, the transaction, the wallet and the ledger entry all
				// already written. It stands in for the database refusing that
				// statement for any reason at all — what matters is where the
				// refusal lands, not which of the many possible ones it was.
				w.exec(t, `REVOKE INSERT ON wagering.outbox FROM wagering_app`)
				return injection{
					ids: mintedIDs{},
					ctx: t.Context(),
					reachedTheEnd: func(t *testing.T, err error) {
						t.Helper()
						// The outbox insert is the only statement in the
						// command the revoke touches, so an insufficient
						// privilege here is the failure landing where this
						// case says it does rather than somewhere earlier.
						refusedWith(t, err, pgerrcode.InsufficientPrivilege)
					},
				}
			},
		},
		{
			name: "the caller goes away as the events are wrapped",
			inject: func(t *testing.T, w *world) injection {
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				ids := &cancellingIDs{IDs: mintedIDs{}, cancel: cancel}
				return injection{
					ids: ids,
					ctx: ctx,
					reachedTheEnd: func(t *testing.T, err error) {
						t.Helper()
						if !ids.asked.Load() {
							t.Fatal("the command failed before it had an event to wrap, " +
								"so there was nothing written for the rollback to undo")
						}
						// Retryable whichever side noticed the cancellation
						// first: the context error and the server's
						// query_canceled both mean the statement recorded
						// nothing, which errors.go is explicit about.
						classifies(t, err, app.Retryable)
					},
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			// Opened before the failure is installed. The opening writes to
			// four of the same five tables, so a fixture set up afterwards
			// would fail in place of the thing under test.
			opened := w.wire(t, at(0))
			wallet := opened.open(t, "player-atomic", "100.00")
			before := w.footprintOf(t, wallet.ID)

			injected := c.inject(t, w)
			failing := wireOnto(t, w.tm, injected.ids, at(1))

			bet := carrying(
				submission(t, wagering.Bet, "player-atomic", "ext-atomic", "key-atomic", "40.00"),
				"message-atomic")
			result, err := failing.wagers.Submit(injected.ctx, bet)
			if err == nil {
				t.Fatalf("the submission came to %s, wanted it to fail partway", result.Status)
			}
			injected.reachedTheEnd(t, err)

			if got := w.footprintOf(t, wallet.ID); got != before {
				t.Fatalf("the failed command left %+v behind, wanted %+v", got, before)
			}
			// Named table by table as well. The footprint above would also be
			// satisfied by a command that never reached the database at all;
			// these say which rows in particular are not there.
			absences := []struct {
				what  string
				query string
				args  []any
			}{
				{
					what:  "an inbox row",
					query: `SELECT count(*) FROM wagering.inbox WHERE message_id = $1`,
					args:  []any{"message-atomic"},
				},
				{
					what: "a wager transaction",
					query: `SELECT count(*) FROM wagering.wager_transaction ` +
						`WHERE external_transaction_id = $1`,
					args: []any{"ext-atomic"},
				},
				{
					what: "a ledger entry",
					query: `SELECT count(*) FROM wagering.wallet_ledger_entry ` +
						`WHERE wallet_id = $1 AND direction = 'DEBIT'`,
					args: []any{uuidOf(wallet.ID)},
				},
				{
					// By instant rather than by name: the command would have
					// emitted two events and only one of them carries the
					// provider's identifier, so a predicate on that would pass
					// over the balance change.
					what: "an outbox event",
					query: `SELECT count(*) FROM wagering.outbox ` +
						`WHERE aggregate_id = $1 AND occurred_at > $2`,
					args: []any{uuidOf(wallet.ID), at(0)},
				},
			}
			for _, absent := range absences {
				if got := w.count(t, absent.query, absent.args...); got != 0 {
					t.Fatalf("the failed command left %d of %s behind", got, absent.what)
				}
			}
			// And the balance is the one the wallet opened with, read back
			// through the use case rather than off the row.
			if got := opened.balanceOf(t, wallet.ID); got != "100.00" {
				t.Fatalf("the wallet holds %s, wanted the 100.00 it opened with", got)
			}
		})
	}
}

// TestTwoConcurrentBetsSpendOneBalanceOnce is the scenario the whole design is
// for: a wallet holding 100.00 and two genuine bets of 80.00 arriving at once.
//
// They are two bets and not a duplicate — different external ids, different
// idempotency keys — so idempotency has nothing to say about them. What decides
// is the wallet lock: one of them takes it, spends 80.00 and leaves 20.00, and
// the other then reads the balance that actually exists rather than the one it
// was submitted against. There is no reading of a stale balance to be had,
// because neither bet reads the wallet until it holds the lock.
//
// The rejection is an OUTCOME. It comes back with a nil error, a persisted row
// and a code a provider can act on, because a bet that cannot be afforded is a
// business fact and not a failure of this system.
//
// # How this one is known to contend
//
// The wallet's lock is held by a third transaction before either submission
// starts, so both queue on it and both are in flight at the same instant.
// [contended] asserts exactly that of each: it started before the lock was
// released and it had not answered by then. Submitting them one after another
// instead was tried, and it fails there — the second bet's window cannot begin
// until the first has answered, and the first cannot answer until the release.
func TestTwoConcurrentBetsSpendOneBalanceOnce(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	u := w.wire(t, at(0))
	wallet := u.open(t, "player-spec", "100.00")
	u.clock.moveTo(at(1))

	bets := []app.SubmitOperation{
		submission(t, wagering.Bet, "player-spec", "ext-spec-a", "key-spec-a", "80.00"),
		submission(t, wagering.Bet, "player-spec", "ext-spec-b", "key-spec-b", "80.00"),
	}

	release := w.holdWallet(t, "player-spec")
	collect := u.racing(t, bets)
	// Long enough that a submission which was not blocked would have finished.
	time.Sleep(contentionPause)
	releasedAt := release()
	attempts := collect()

	processed, rejected := 0, 0
	for i, a := range attempts {
		if a.err != nil {
			t.Fatalf("bet %d failed: %v", i, a.err)
		}
		switch a.result.Status {
		case wagering.Processed:
			processed++
			if a.result.Balance == nil || a.result.Balance.Amount() != "20.00" {
				t.Fatalf("the processed bet reported %v, wanted the 20.00 it left behind",
					a.result.Balance)
			}
		case wagering.Rejected:
			rejected++
			if got := a.result.FailureCode; got != failure.InsufficientFunds {
				t.Fatalf("the rejected bet carries %q, wanted %s", got, failure.InsufficientFunds)
			}
		default:
			t.Fatalf("bet %d came to %s, wanted PROCESSED or REJECTED", i, a.result.Status)
		}
		contended(t, a, releasedAt, i)
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("%d processed and %d rejected, wanted exactly one of each", processed, rejected)
	}

	if got := u.balanceOf(t, wallet.ID); got != "20.00" {
		t.Fatalf("the wallet holds %s, wanted 20.00", got)
	}
	if got := w.debitsOf(t, wallet.ID); got != 1 {
		t.Fatalf("the ledger holds %d debits, wanted the one bet that was paid for", got)
	}
	settled := w.footprintOf(t, wallet.ID)

	// Resent unchanged, under the same keys. The rejected one too: a definitive
	// rejection binds its idempotency key to that payload for good, so the
	// replay must be the same rejection rather than a fresh attempt against a
	// wallet that has moved since.
	for i, bet := range bets {
		again, err := u.wagers.Submit(t.Context(), bet)
		if err != nil {
			t.Fatalf("resending bet %d failed: %v", i, err)
		}
		first := attempts[i].result
		switch {
		case !again.IdempotentReplay:
			t.Fatalf("resending bet %d was treated as a new submission", i)
		case again.TransactionID != first.TransactionID:
			t.Fatalf("resending bet %d answered %s, wanted %s",
				i, again.TransactionID, first.TransactionID)
		case again.Status != first.Status:
			t.Fatalf("resending bet %d answered %s, wanted %s", i, again.Status, first.Status)
		case again.FailureCode != first.FailureCode:
			t.Fatalf("resending bet %d answered %q, wanted %q",
				i, again.FailureCode, first.FailureCode)
		}
	}

	if got := u.balanceOf(t, wallet.ID); got != "20.00" {
		t.Fatalf("the resends moved the wallet to %s, wanted it still at 20.00", got)
	}
	// Not a row, not an entry, not an event: a replay reads and writes nothing,
	// which is a wider statement than "the balance is the same" and the one
	// worth making, because a replay that recorded a second transaction would
	// leave the balance alone too.
	if got := w.footprintOf(t, wallet.ID); got != settled {
		t.Fatalf("the resends left %+v behind, wanted the %+v the two bets settled at",
			got, settled)
	}
}

// TestFiftyIdenticalBetsProduceOneDebit is the same operation sent fifty times
// at once: one provider, one external id, one idempotency key, one payload.
//
// The wallet moves once, the ledger gains one entry, and all fifty callers are
// told the same transaction id — one of them because it produced it and
// forty-nine because they read it. That last count is the assertion that
// matters. "Exactly one debit" would also hold if the winner had simply been
// handed a wallet nobody else wanted; "forty-nine replays" only holds if
// forty-nine submissions each found the work already done.
//
// # How this one is known to contend
//
// As with the two bets above, the wallet's lock is held before any submission
// starts, so all fifty queue on it rather than arriving one after another, and
// [contended] says so of each of them. A submission that arrived after the
// winner had committed would replay without ever having contended, and a suite
// of forty-nine of those is the weaker test this one is written not to be:
// sending them one after another instead was tried, and fails on the second.
func TestFiftyIdenticalBetsProduceOneDebit(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	const bets = 50
	// A pool as wide as the race, so that fifty submissions are fifty
	// connections rather than fifty goroutines taking turns on sixteen, and a
	// lock timeout that bounds the queue rather than one hand-off.
	u := wireOnto(t, w.wideManager(t, bets+1, time.Minute), mintedIDs{}, at(0))
	wallet := u.open(t, "player-fifty", "100.00")
	u.clock.moveTo(at(1))

	// One operation, fifty times. The identifier source mints a fresh
	// transaction id on every call, so the fifty submissions reach the database
	// with fifty distinct ids and only the winner's is ever stored — which is
	// what makes "all fifty were answered with one identifier" a fact about the
	// database rather than about the fixture. A source that handed the same id
	// to all of them would make this test pass for the wrong reason.
	one := submission(t, wagering.Bet, "player-fifty", "ext-fifty", "key-fifty", "10.00")
	identical := make([]app.SubmitOperation, bets)
	for i := range identical {
		identical[i] = one
	}

	release := w.holdWallet(t, "player-fifty")
	collect := u.racing(t, identical)
	time.Sleep(contentionPause)
	releasedAt := release()
	attempts := collect()

	var (
		originals int
		replays   int
		id        wagering.TransactionID
	)
	for i, a := range attempts {
		if a.err != nil {
			t.Fatalf("submission %d failed: %v", i, a.err)
		}
		if a.result.Status != wagering.Processed {
			t.Fatalf("submission %d came to %s, wanted PROCESSED", i, a.result.Status)
		}
		if i == 0 {
			id = a.result.TransactionID
		}
		if a.result.TransactionID != id {
			t.Fatalf("submission %d was answered as operation %s, and submission 0 as %s; "+
				"one operation sent fifty times has one identifier",
				i, a.result.TransactionID, id)
		}
		if a.result.IdempotentReplay {
			replays++
		} else {
			originals++
		}
		contended(t, a, releasedAt, i)
	}
	if originals != 1 || replays != bets-1 {
		t.Fatalf("%d submissions were the original and %d were replays, wanted 1 and %d",
			originals, replays, bets-1)
	}

	if got := w.debitsOf(t, wallet.ID); got != 1 {
		t.Fatalf("the ledger holds %d debits, wanted 1", got)
	}
	if got := u.balanceOf(t, wallet.ID); got != "90.00" {
		t.Fatalf("the wallet holds %s, wanted the 90.00 one 10.00 bet leaves", got)
	}
	if got := w.count(t,
		`SELECT count(*) FROM wagering.wager_transaction WHERE external_transaction_id = $1`,
		"ext-fifty"); got != 1 {
		t.Fatalf("%d rows were recorded for the operation, wanted 1", got)
	}
	// One commit's worth of events, not fifty: a processed bet announces itself
	// and the balance change it caused, and a replay announces nothing. Matched
	// on the operation's own identifier, which is the one field both events
	// carry — the balance change names no provider.
	if got := w.count(t,
		`SELECT count(*) FROM wagering.outbox WHERE aggregate_id = $1 `+
			`AND payload->'data'->>'transactionId' = $2`,
		uuidOf(wallet.ID), id.String()); got != 2 {
		t.Fatalf("the operation emitted %d events, wanted the 2 one commit produces", got)
	}
}

// TestASecondReversalIsRejectedAsAnOutcome takes a bet, returns it with a
// refund, and then tries to reverse the same bet again — twice, once with
// another refund and once with a rollback, because the rule is about reversals
// and not about one kind of them.
//
// Both are REJECTED under REFERENCE_ALREADY_REVERSED, and rejected is the
// operative word. A refusal would come back as an error with nothing stored and
// the idempotency key still free; a rejection is persisted, announced and
// final. The difference is visible here as a row, an event and a nil error.
//
// The domain is what decides it. app.ErrReferenceAlreadyReversed exists as the
// backstop for a reference view built wrongly, and reaching it would mean the
// rule now lives only in the schema — so an error from either attempt is a
// finding about the adapter's reference view rather than a result to assert.
func TestASecondReversalIsRejectedAsAnOutcome(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	u := w.wire(t, at(0))
	wallet := u.open(t, "player-reversed", "100.00")

	u.clock.moveTo(at(1))
	u.submit(t, submission(t, wagering.Bet, "player-reversed", "ext-bet", "key-bet", "30.00"),
		wagering.Processed)
	u.clock.moveTo(at(2))
	u.submit(t, against(
		submission(t, wagering.Refund, "player-reversed", "ext-refund", "key-refund", "30.00"),
		"ext-bet"), wagering.Processed)

	// The stake is back and the bet is held by the refund that returned it.
	if got := u.balanceOf(t, wallet.ID); got != "100.00" {
		t.Fatalf("the wallet holds %s, wanted the 100.00 the refund restored", got)
	}
	settled := w.footprintOf(t, wallet.ID)

	attempts := []struct {
		name     string
		kind     wagering.Kind
		external string
		key      string
	}{
		{name: "a second refund", kind: wagering.Refund, external: "ext-refund-2", key: "key-refund-2"},
		{name: "a rollback of the same bet", kind: wagering.Rollback, external: "ext-rollback", key: "key-rollback"},
	}
	for i, attempt := range attempts {
		t.Run(attempt.name, func(t *testing.T) {
			u.clock.moveTo(at(3 + i))
			result, err := u.wagers.Submit(t.Context(), against(
				submission(t, attempt.kind, "player-reversed",
					attempt.external, attempt.key, "30.00"),
				"ext-bet"))
			if err != nil {
				t.Fatalf("%s was refused rather than rejected, which means the reference "+
					"view was built wrongly and the rule now lives only in the schema: %v",
					attempt.name, err)
			}
			if result.Status != wagering.Rejected {
				t.Fatalf("%s came to %s, wanted REJECTED", attempt.name, result.Status)
			}
			if got := result.FailureCode; got != failure.ReferenceAlreadyReversed {
				t.Fatalf("%s carries %q, wanted %s",
					attempt.name, got, failure.ReferenceAlreadyReversed)
			}

			// Stored, and readable by the provider that sent it.
			stored, err := u.wagers.TransactionByExternalID(t.Context(), providerPrincipal(t),
				wagering.ExternalTransactionID(attempt.external))
			if err != nil {
				t.Fatalf("read %s back: %v", attempt.name, err)
			}
			switch {
			case stored.TransactionID != result.TransactionID:
				t.Fatalf("%s reads back as operation %s, wanted %s",
					attempt.name, stored.TransactionID, result.TransactionID)
			case stored.Status != wagering.Rejected:
				t.Fatalf("%s is stored as %s, wanted REJECTED", attempt.name, stored.Status)
			case stored.FailureCode != failure.ReferenceAlreadyReversed:
				t.Fatalf("%s is stored under %q, wanted %s",
					attempt.name, stored.FailureCode, failure.ReferenceAlreadyReversed)
			}

			// And announced. A rejection a consumer never hears about is
			// indistinguishable from a submission that never arrived.
			events := w.count(t,
				`SELECT count(*) FROM wagering.outbox WHERE aggregate_id = $1 `+
					`AND event_type = 'WagerTransactionRejected' `+
					`AND payload->'data'->>'externalTransactionId' = $2 `+
					`AND payload->'data'->>'failureCode' = $3`,
				uuidOf(wallet.ID), attempt.external, failure.ReferenceAlreadyReversed.String())
			if events != 1 {
				t.Fatalf("%s emitted %d rejection events, wanted 1", attempt.name, events)
			}

			// The bet is still held by the refund that returned it, and by
			// nothing else: a rejected reversal takes no slot, which is what
			// keeps the next attempt refusable for the same reason rather than
			// for a new one.
			held := w.count(t,
				`SELECT count(*) FROM wagering.active_reversal a `+
					`JOIN wagering.wager_transaction r ON r.id = a.reversal_id `+
					`WHERE r.external_transaction_id = $1`, "ext-refund")
			total := w.count(t, `SELECT count(*) FROM wagering.active_reversal`)
			if held != 1 || total != 1 {
				t.Fatalf("after %s there are %d holds on a reference, %d of them the "+
					"refund's; wanted the refund's one and nothing else",
					attempt.name, total, held)
			}

			// Exactly one more operation and one more event than the settled
			// pair left behind, and not a minor unit of movement: the balance,
			// the wallet version and the ledger are all where the refund left
			// them.
			want := settled
			want.Transactions += i + 1
			want.Events += i + 1
			if got := w.footprintOf(t, wallet.ID); got != want {
				t.Fatalf("after %s the wallet shows %+v, wanted %+v", attempt.name, got, want)
			}
		})
	}
}

// contended asserts that one attempt was in flight while the wallet's lock was
// held, which is what makes a set of them a race rather than a sequence.
//
// Both halves are needed. Starting before the release says the submission was
// already waiting; answering after it says it was waiting for THIS lock. A test
// whose submissions were issued one at a time fails the first half for every
// one of them but the first.
func contended(t *testing.T, a attempt, releasedAt time.Time, i int) {
	t.Helper()
	if !a.startedAt.Before(releasedAt) {
		t.Fatalf("submission %d started at %s, after the wallet was released at %s: "+
			"it never contended for the lock", i, a.startedAt, releasedAt)
	}
	if !a.finishedAt.After(releasedAt) {
		t.Fatalf("submission %d answered at %s, before the wallet was released at %s: "+
			"it was never blocked on the lock", i, a.finishedAt, releasedAt)
	}
}
