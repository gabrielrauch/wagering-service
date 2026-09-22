package app_test

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// A replay answers what the first submission answered, however far the wallet
// has moved since. Reporting the balance now would make a retry a different
// operation from the one it is retrying.
func TestReplayReportsTheBalanceObservedAtTheOriginalProcessing(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	first := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
	if first.Balance.Amount() != "75.00" {
		t.Fatalf("first bet left %s, want 75.00", first.Balance.Amount())
	}
	// The wallet moves on.
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-2", Key: "key-2", Amount: "25.00"}))

	replay := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	if !replay.IdempotentReplay {
		t.Error("the same submission again is a replay")
	}
	if replay.TransactionID != first.TransactionID {
		t.Errorf("replay names %s, want the original %s", replay.TransactionID, first.TransactionID)
	}
	if got := replay.Balance.Amount(); got != "75.00" {
		t.Errorf("replay reports %s, want the original 75.00", got)
	}
	if got := f.db.transactionCount(); got != 3 {
		t.Errorf("%d transactions, want the opening and two bets — a replay records nothing", got)
	}
}

// A movement transaction is READ COMMITTED, so the two idempotency lookups see
// two instants. A submission racing its own twin can read the key before the
// winner commits and the external id after it, and the row it then finds is its
// own.
//
// This is the interleaving fifty parallel submissions of one operation produced
// against three API instances, where between one and six of them were answered
// 409 "already recorded under another idempotency key" for an operation recorded
// under exactly the key they sent. It is driven here by a step rather than by
// goroutines and a hope, because the answer has to be the same every time and a
// race that reproduces four runs in five is not a regression test.
//
// It is a contract violation and not merely an unhelpful answer: CONTEXT.md
// defines a Conflict as a refusal that will be refused again when the same
// submission is sent unchanged, and sending any of these again is answered as
// the replay it always was.
func TestASubmissionThatFindsItsOwnRowAcrossTheSnapshotWindowIsAReplay(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	first := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	// The next key lookup answers from before the winner committed; the external
	// id lookup that follows it answers from after.
	f.db.hideNextKeyLookup()
	replay, err := f.trySubmit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00",
	}))
	if err != nil {
		t.Fatalf("a submission that found its own row was refused: %v", err)
	}

	if !replay.IdempotentReplay {
		t.Error("a submission that found its own row is a replay")
	}
	if replay.TransactionID != first.TransactionID {
		t.Errorf("replay names %s, want the original %s", replay.TransactionID, first.TransactionID)
	}
	if got := replay.Balance.Amount(); got != "75.00" {
		t.Errorf("replay reports %s, want the original 75.00", got)
	}
	if got := f.db.transactionCount(); got != 2 {
		t.Errorf("%d transactions, want the opening and one bet — a replay records nothing", got)
	}
}

// The window is closed by recognising the row, not by ignoring the second
// lookup: an operation genuinely submitted under two different keys is still
// the conflict it always was, whichever lookup finds it.
func TestAnExternalIDUnderADifferentKeyIsStillAConflictAcrossTheSameWindow(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	f.db.hideNextKeyLookup()
	_, err := f.trySubmit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-2", Amount: "25.00",
	}))

	assertClass(t, err, app.Conflict)
	if got := f.db.transactionCount(); got != 2 {
		t.Errorf("%d transactions, want the opening and one bet — a conflict records nothing", got)
	}
}

// And a submission that reuses one key for a different payload is told so from
// either lookup. Which index found the row is an accident of timing, and a
// provider must not learn a different outcome from it.
func TestAReusedKeyIsAPayloadConflictFromEitherLookup(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	// Found by the key, in one instant.
	_, direct := f.trySubmit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "30.00",
	}))
	// Found by the external id, across the window.
	f.db.hideNextKeyLookup()
	_, acrossWindow := f.trySubmit(t, fields(acme, submission{
		Kind: "BET", External: "ext-1", Key: "key-1", Amount: "30.00",
	}))

	for name, err := range map[string]error{"by key": direct, "by external id": acrossWindow} {
		t.Run(name, func(t *testing.T) {
			assertClass(t, err, app.Conflict)
			assertCode(t, err, failure.IdempotencyPayloadConflict)
		})
	}
}

func TestReplayOfARejectionKeepsItsCode(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "10.00", "BRL")

	first := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
	replay := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	if !replay.IdempotentReplay {
		t.Error("a settled rejection replays like any other terminal result")
	}
	assertStatus(t, replay, wagering.Rejected, failure.InsufficientFunds)
	if replay.Balance != nil {
		t.Error("a rejection reports no balance, on the first answer or the second")
	}
	if replay.TransactionID != first.TransactionID {
		t.Error("a replay names the original transaction")
	}
}

// A parked operation replays as it stands. Carrying it forward here would spend
// the wait budget on a provider's retry, so an impatient provider could exhaust
// in minutes a budget meant to last hours.
func TestReplayOfAParkedOperationDoesNotCarryItForward(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	parked := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-1",
		Key:      "key-1",
		Amount:   "25.00", Reference: "never-arrived",
	}))
	if parked.Status != wagering.PendingReference {
		t.Fatalf("status %s, want %s", parked.Status, wagering.PendingReference)
	}
	attemptsBefore := f.row(t, parked.TransactionID).snap.ReferenceAttempts

	replay := f.submit(t, fields(acme, submission{
		Kind:     "REFUND",
		External: "ext-1",
		Key:      "key-1",
		Amount:   "25.00", Reference: "never-arrived",
	}))

	if !replay.IdempotentReplay {
		t.Error("want a replay")
	}
	if replay.Status != wagering.PendingReference {
		t.Errorf("status %s, want %s", replay.Status, wagering.PendingReference)
	}
	if replay.Balance != nil {
		t.Error("an operation that has not settled reports no balance")
	}
	if row := f.row(t, parked.TransactionID); row.snap.ReferenceAttempts != attemptsBefore {
		t.Errorf("attempts moved %d -> %d: the replay carried the operation forward",
			attemptsBefore, row.snap.ReferenceAttempts)
	}
}

// One key, two different operations. The key is already bound to the first, and
// a settled payload is not something a provider may revise.
func TestSameKeyWithADifferentPayloadIsAConflict(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	_, err := f.trySubmit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "30.00"}))

	assertClass(t, err, app.Conflict)
	assertCode(t, err, failure.IdempotencyPayloadConflict)
}

// The same operation under a second key. It carries no failure code, and that is
// deliberate: the catalogue describes outcomes a provider can act on, and nothing
// is persisted for this one.
func TestSameOperationUnderAnotherKeyIsAConflictWithoutACode(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")
	f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	_, err := f.trySubmit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-2", Amount: "25.00"}))

	assertClass(t, err, app.Conflict)
	if code, ok := app.CodeOf(err); ok {
		t.Errorf("carries code %s, want none", code)
	}
}

// Two submissions racing: the first commits in the window between the second's
// read and its insert, so the second finds nothing, collides, and is resolved by
// re-reading rather than by inspecting which constraint was named.
func TestConcurrentDuplicatesResolveToOneOperationAndReplays(t *testing.T) {
	f := newFixture(t)
	wallet := f.db.seedWallet(t, "player-1", "100.00", "BRL")

	var winner app.OperationResult
	f.db.duringRecord = func() {
		winner = f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))
	}

	loser := f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-1", Amount: "25.00"}))

	if winner.IdempotentReplay {
		t.Error("the submission that got there first is not a replay")
	}
	if !loser.IdempotentReplay {
		t.Error("the submission that lost the race is a replay")
	}
	if loser.TransactionID != winner.TransactionID {
		t.Errorf("loser reports %s, want the winner's %s", loser.TransactionID, winner.TransactionID)
	}
	if got := loser.Balance.Amount(); got != "75.00" {
		t.Errorf("loser reports %s, want the winner's 75.00", got)
	}

	// One operation, applied once.
	if got := f.db.balanceOf(t, wallet).Amount(); got != "75.00" {
		t.Errorf("wallet holds %s, want 75.00 — the bet was applied twice", got)
	}
	if got := f.db.transactionCount(); got != 2 {
		t.Errorf("%d transactions, want the opening and one bet", got)
	}
	if got := len(f.db.eventTypes()); got != 2 {
		t.Errorf("published %d events, want 2 — a replay publishes nothing", got)
	}
}

// The same collision, with the winner having taken the external id under another
// key. One sentinel produced both this and the replay above: the decision comes
// from re-reading both keys, never from which index the database happened to
// check first.
func TestLosingARaceToAnotherKeyIsAConflict(t *testing.T) {
	f := newFixture(t)
	f.db.seedWallet(t, "player-1", "100.00", "BRL")

	f.db.duringRecord = func() {
		f.submit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-winner", Amount: "25.00"}))
	}

	_, err := f.trySubmit(t, fields(acme, submission{Kind: "BET", External: "ext-1", Key: "key-loser", Amount: "25.00"}))

	assertClass(t, err, app.Conflict)
	if code, ok := app.CodeOf(err); ok {
		t.Errorf("carries code %s, want none", code)
	}
}
