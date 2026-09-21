//go:build multi

// One operation arriving many times at many instances, and two operations
// arriving at once that cannot both be applied.
//
// Both run against the deployment — api-1, api-2 and api-3 as Compose starts
// them — because both are about what three separate processes with three
// separate pools do to one row, and the deployment is three separate processes
// with three separate pools.
package multi

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// TestOneBetSubmittedFiftyTimesAcrossThreeInstancesDebitsOnce sends one
// submission fifty times at once, spread evenly over the three instances.
//
// What makes this a multi-instance scenario rather than a concurrency one is
// where the answer has to come from. Each instance has its own pool, its own
// memory and no idea the other two exist, so the only thing that can make
// forty-nine of these fifty answer with the outcome the fiftieth reached is the
// database — and it has to answer with the SAME transaction identifier and the
// SAME balance, because a provider that retried a request it never saw the
// answer to is entitled to the answer it would have had.
//
// The assertion that would fail if idempotency were deleted is not the balance.
// It is the count of `idempotentReplay` answers: exactly one submission is new
// and exactly forty-nine are replays, which is the API saying, forty-nine
// times, that a duplicate genuinely arrived and was recognised as one. A
// service that applied all fifty would report fifty non-replays and fifty
// debits; a suite that only checked the balance could not tell that from a
// service that dropped forty-nine requests on the floor.
//
// # What it finds today
//
// This test does not pass, and the failure is the service's rather than the
// test's. Between two and six of the fifty are refused 409 CONFLICT — "operation
// %q is already recorded under another idempotency key" — for an operation
// recorded under exactly the key they sent. It reproduces on roughly two runs
// in three.
//
// The refusal comes from the first of the two calls to Wagering.replay, the one
// inside WithinMovement. That transaction is READ COMMITTED, so its two lookups
// — ByIdempotencyKey and then ByExternal — take a fresh snapshot each, and a
// submission whose first lookup runs before the winner commits and whose second
// runs after sees no row for its key and a row for its external identifier. The
// same function reads correctly from resolveDuplicate, which runs under
// WithinSnapshot and therefore sees one instant.
//
// The contract it breaks is CONTEXT.md's own: a Conflict is "refused because it
// contradicts one already recorded... sending it again unchanged will be refused
// again", and sending any of these again unchanged is answered 200 with the
// outcome the operation reached. The wallet is never wrong — one debit either
// way — which is why nothing before this suite noticed.
func TestOneBetSubmittedFiftyTimesAcrossThreeInstancesDebitsOnce(t *testing.T) {
	requireStack(t)

	const submissions = 50
	player := scoped("player-burst")
	external := scoped("burst-bet")
	key := scoped("burst-key")

	wallet := openWallet(t, instances[0], player, "500.00")
	body := encode(t, bet(providerA, external, player, "7.00"))
	credential := token(t, providerA)

	// Every goroutine built and ready before any of them sends, so that fifty
	// submissions really are fifty submissions at once rather than fifty
	// submissions as fast as one loop can start goroutines.
	released := make(chan struct{})
	answers := make([]answer, submissions)
	failures := make([]error, submissions)
	var wg sync.WaitGroup
	for i := range submissions {
		wg.Go(func() {
			<-released
			answers[i], failures[i] = attempt(call{
				base:           instances[i%len(instances)],
				method:         http.MethodPost,
				path:           "/wagering/transactions",
				body:           body,
				token:          credential,
				idempotencyKey: key,
			})
		})
	}
	close(released)
	wg.Wait()

	var (
		applied  int
		replayed int
		refused  []string
		first    operationAnswer
	)
	for i, got := range answers {
		if failures[i] != nil {
			t.Fatalf("submission %d to %s failed: %v", i, instances[i%len(instances)], failures[i])
		}
		if got.status != http.StatusOK {
			// Collected rather than fatal, so that the failure says how many of
			// the fifty were refused rather than which one was refused first.
			refused = append(refused, fmt.Sprintf("%s answered %s",
				instances[i%len(instances)], got))
			continue
		}
		op := operationOf(t, got)
		if op.Status != processed {
			t.Errorf("submission %d is %s (%s), wanted %s",
				i, op.Status, op.FailureCode, processed)
			continue
		}
		if first.TransactionID == "" {
			first = op
		}
		if op.TransactionID != first.TransactionID {
			t.Fatalf("submission %d answered transaction %s, and submission 0 answered %s: "+
				"fifty submissions of one operation are one wager transaction",
				i, op.TransactionID, first.TransactionID)
		}
		if op.Balance == nil || op.Balance.Amount != "493.00" {
			t.Errorf("submission %d answered balance %v, wanted 493.00", i, op.Balance)
		}
		if op.IdempotentReplay {
			replayed++
		} else {
			applied++
		}
	}
	if len(refused) > 0 {
		t.Errorf("%d of %d submissions of ONE operation under ONE idempotency key were "+
			"refused.\nEvery one of them is entitled to the outcome the operation "+
			"reached, and sending any of them again is answered with it — which is "+
			"the opposite of what a CONFLICT tells the provider.\nThe refusals:\n  %s",
			len(refused), submissions, strings.Join(refused, "\n  "))
	}
	if applied != 1 || replayed != submissions-1-len(refused) {
		t.Errorf("%d submissions were applied and %d were answered as replays, "+
			"wanted 1 and %d", applied, replayed, submissions-1-len(refused))
	}

	operations := countRows(t, owner,
		`SELECT count(*) FROM wagering.wager_transaction `+
			`WHERE provider = $1 AND external_transaction_id = $2`, providerA, external)
	if operations != 1 {
		t.Errorf("the table holds %d wager transactions for %s, wanted 1", operations, external)
	}
	debits := countRows(t, owner,
		`SELECT count(*) FROM wagering.wallet_ledger_entry `+
			`WHERE wallet_id = $1 AND direction = 'DEBIT'`, wallet.WalletID)
	if debits != 1 {
		t.Errorf("the wallet has %d debits, wanted 1", debits)
	}
	if got, want := balanceOf(t, owner, wallet.WalletID), minor(t, "493.00"); got != want {
		t.Errorf("the stored balance is %d minor units, want %d", got, want)
	}
	reconciled(t, instances[2], wallet.WalletID)
}

// TestTwoConcurrentBetsForMoreThanTheBalanceLeaveExactlyOneRejected submits two
// different operations, each for more than half the wallet, through two
// different instances at the same instant.
//
// Only one of them can be applied, and which one is not this test's business.
// What is asserted is that the pair is settled as a pair: one PROCESSED, one
// REJECTED for INSUFFICIENT_FUNDS, twenty left in the wallet, one ledger entry
// between them. Two instances that did not take the same row lock would both
// read a hundred, both decide eighty was affordable, and either leave the
// wallet at minus sixty or have one of the two writes disappear — and the
// balance-only version of this assertion would pass on the second of those.
//
// The resends are the second half and they are what makes this a statement
// about replays rather than about locking. Each submission is sent again,
// unchanged, to the THIRD instance — one that saw neither the first time — and
// both come back with the answer they came back with before, flagged as
// replays, having moved nothing. A rejection is an outcome the system stands
// by, not a failure to be retried into a different answer.
func TestTwoConcurrentBetsForMoreThanTheBalanceLeaveExactlyOneRejected(t *testing.T) {
	requireStack(t)

	player := scoped("player-contended")
	wallet := openWallet(t, instances[0], player, "100.00")

	type attemptedBet struct {
		external string
		key      string
		instance string
		got      answer
		err      error
	}
	bets := []*attemptedBet{
		{external: scoped("contended-one"), key: scoped("contended-key-one"), instance: instances[0]},
		{external: scoped("contended-two"), key: scoped("contended-key-two"), instance: instances[1]},
	}
	credential := token(t, providerA)

	released := make(chan struct{})
	var wg sync.WaitGroup
	for _, b := range bets {
		body := encode(t, bet(providerA, b.external, player, "80.00"))
		wg.Go(func() {
			<-released
			b.got, b.err = attempt(call{
				base:           b.instance,
				method:         http.MethodPost,
				path:           "/wagering/transactions",
				body:           body,
				token:          credential,
				idempotencyKey: b.key,
			})
		})
	}
	close(released)
	wg.Wait()

	outcomes := make([]operationAnswer, len(bets))
	for i, b := range bets {
		if b.err != nil {
			t.Fatalf("the bet %s failed: %v", b.external, b.err)
		}
		outcomes[i] = operationOf(t, b.got)
	}
	winners, losers := partition(outcomes)
	if len(winners) != 1 || len(losers) != 1 {
		t.Fatalf("%d were processed and %d were rejected, wanted one of each: %+v",
			len(winners), len(losers), outcomes)
	}
	if losers[0].FailureCode != insufficientFunds {
		t.Errorf("the rejected bet is coded %q, wanted %q",
			losers[0].FailureCode, insufficientFunds)
	}
	if winners[0].Balance == nil || winners[0].Balance.Amount != "20.00" {
		t.Errorf("the processed bet answered balance %v, wanted 20.00", winners[0].Balance)
	}

	// One ledger entry between the two of them. The wallet has two in total —
	// the opening credit is the other — so this counts the entries belonging to
	// these two transactions rather than the entries the wallet has.
	entries := ledgerOf(t, owner, wallet.WalletID)
	var moved []ledgerRow
	for _, entry := range entries {
		if entry.transactionID == winners[0].TransactionID ||
			entry.transactionID == losers[0].TransactionID {
			moved = append(moved, entry)
		}
	}
	if len(moved) != 1 {
		t.Fatalf("the two bets produced %d ledger entries, wanted 1: %+v", len(moved), moved)
	}
	if moved[0].transactionID != winners[0].TransactionID ||
		moved[0].direction != "DEBIT" || moved[0].amountMinor != minor(t, "80.00") {
		t.Errorf("the one ledger entry is %+v, wanted an 80.00 DEBIT for %s",
			moved[0], winners[0].TransactionID)
	}
	if got, want := balanceOf(t, owner, wallet.WalletID), minor(t, "20.00"); got != want {
		t.Errorf("the stored balance is %d minor units, want %d", got, want)
	}

	before := ledgerOf(t, owner, wallet.WalletID)
	for i, b := range bets {
		again := operationOf(t, submit(t, instances[2], providerA,
			bet(providerA, b.external, player, "80.00"), b.key))
		if !again.IdempotentReplay {
			t.Errorf("resending %s was not answered as a replay, so the resend was not "+
				"recognised as one", b.external)
		}
		if again.TransactionID != outcomes[i].TransactionID ||
			again.Status != outcomes[i].Status ||
			again.FailureCode != outcomes[i].FailureCode {
			t.Errorf("resending %s answered %+v, and the first answer was %+v",
				b.external, again, outcomes[i])
		}
	}
	if got, want := balanceOf(t, owner, wallet.WalletID), minor(t, "20.00"); got != want {
		t.Errorf("the resends left the balance at %d minor units, want %d", got, want)
	}
	if after := ledgerOf(t, owner, wallet.WalletID); len(after) != len(before) {
		t.Errorf("the resends added %d ledger entries", len(after)-len(before))
	}
	reconciled(t, instances[2], wallet.WalletID)
}

// partition splits outcomes into the ones that were applied and the ones that
// were refused, so that a scenario which does not care which of two won does
// not have to say so twice.
func partition(outcomes []operationAnswer) (processedOnes, rejectedOnes []operationAnswer) {
	for _, outcome := range outcomes {
		switch outcome.Status {
		case processed:
			processedOnes = append(processedOnes, outcome)
		case rejected:
			rejectedOnes = append(rejectedOnes, outcome)
		}
	}
	return processedOnes, rejectedOnes
}

// scoped names a value so that it belongs to this run and to no other.
//
// The deployment's database is shared and outlives a run — nothing drops it —
// so every row these scenarios assert on is found by an identifier carrying the
// run rather than by counting what is in a table.
func scoped(what string) string { return fmt.Sprintf("%s-%s", what, runID) }
