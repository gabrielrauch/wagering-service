//go:build multi

// A reversal that arrives before the operation it reverses, on the two ends
// that wait can reach: the reference turns up, or the budget runs out.
//
// Both run two workers against one world, because that is where the
// multi-instance question is. The consumer that parks an operation and the
// reference worker that carries it forward need not be in the same process, and
// here they routinely are not: whichever replica's loop gets there first does
// the work, and the operation is a row rather than anything either of them
// holds.
package multi

import (
	"testing"
	"time"
)

// TestARefundDeliveredBeforeItsBetIsCarriedForwardByWhicheverWorkerGetsThere
// sends a reversal for an operation nobody has submitted yet.
//
// Out of order is the ordinary case on this path rather than a fault: a
// provider's two messages can be produced in either order, and the domain's
// answer is to park the reversal rather than refuse it. Both go into one FIFO
// group — the wallet — so the order they arrive in is the queue's guarantee and
// not this test's hope.
//
// What is asserted is that the refund really did park before the bet existed —
// its attempt count says so, and a refund that had arrived second would have
// none — and that it was then carried forward to the bet it names, leaving the
// wallet where it started.
func TestARefundDeliveredBeforeItsBetIsCarriedForwardByWhicheverWorkerGetsThere(t *testing.T) {
	t.Parallel()
	w := newWorld(t, "reference-in-order")

	player := scoped("player-early-refund")
	stake := scoped("early-refund-bet")
	reversal := scoped("early-refund-refund")
	round := scoped("early-refund-round")
	wallet := openWallet(t, w.base, player, "100.00")

	workers := w.twoWorkers(t, patientReference)

	w.put(t, onTheQueue(inRound(refund(providerA, reversal, wallet, "25.00", stake), round),
		scoped("early-refund-refund-message"), scoped("early-refund-refund-key")),
		wallet.WalletID)

	awaitOperation(t, w.owner, providerA, reversal, pendingReference)
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "100.00"); got != want {
		t.Errorf("the wallet holds %d minor units while the refund waits, want %d untouched",
			got, want)
	}

	w.put(t, onTheQueue(inRound(bet(providerA, stake, wallet, "25.00"), round),
		scoped("early-refund-bet-message"), scoped("early-refund-bet-key")),
		wallet.WalletID)

	staked := awaitOperation(t, w.owner, providerA, stake, processed)
	settled := awaitOperation(t, w.owner, providerA, reversal, processed)
	if settled.referenceAttempts < 1 {
		t.Errorf("the refund reports %d attempts at its reference: it never waited, so it "+
			"did not arrive first", settled.referenceAttempts)
	}
	if settled.resolvedReference == nil || *settled.resolvedReference != staked.id {
		t.Errorf("the refund resolved to %v, want the bet %s",
			settled.resolvedReference, staked.id)
	}

	// The stake went out and came back: three entries with the opening, and a
	// balance where it started.
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "100.00"); got != want {
		t.Errorf("the wallet holds %d minor units, want %d", got, want)
	}
	if entries := ledgerOf(t, w.owner, wallet.WalletID); len(entries) != 3 {
		t.Errorf("the wallet has %d ledger entries, want 3: the opening, the bet and the "+
			"refund", len(entries))
	}
	w.awaitEmpty(t, w.inboundURL, "the inbound queue")
	w.noneDeadLettered(t)
	workers.stop(t)
	reconciled(t, w.base, wallet.WalletID)
}

// TestARefundWhoseBetNeverArrivesIsRejectedWhenTheBudgetRunsOut is the other
// end of the same wait.
//
// The bet is never sent, so the refund is parked, looked at again, parked
// again, and finally settled — rejected for the reference it waited for, with a
// row of its own, because that is an outcome and not a failure.
//
// The rejection is asserted to have taken at least the wait budget, and that
// assertion is the one that matters. Without it this scenario also passes
// against a domain that refused the first attempt outright, which is the
// opposite of the behaviour: an operation that gives up immediately and one
// that waits its budget out reach the same row. The attempt count says the same
// thing from the other side — it was looked at more than once.
func TestARefundWhoseBetNeverArrivesIsRejectedWhenTheBudgetRunsOut(t *testing.T) {
	t.Parallel()
	w := newWorld(t, "reference-expiry")

	// The domain's wait budget. Short, because what is being asserted is that
	// it is enforced and not how long it is. The attempt count is generous so
	// that the TTL is what ends the wait: the two bounds are separate rules and
	// only one of them is this scenario's.
	const ttl = 4 * time.Second
	budget := map[string]string{
		"REFERENCE_MAX_ATTEMPTS":   "500",
		"REFERENCE_TTL":            ttl.String(),
		"WAGERING_BACKOFF_INITIAL": "300ms",
		"WAGERING_BACKOFF_FACTOR":  "2",
		"WAGERING_BACKOFF_MAX":     "600ms",
	}

	player := scoped("player-lost-refund")
	missing := scoped("expiry-bet-never-sent")
	reversal := scoped("expiry-refund")
	round := scoped("expiry-round")
	wallet := openWallet(t, w.base, player, "100.00")

	workers := w.twoWorkers(t, budget)

	sent := time.Now()
	w.put(t, onTheQueue(inRound(refund(providerA, reversal, wallet, "25.00", missing), round),
		scoped("expiry-refund-message"), scoped("expiry-refund-key")),
		wallet.WalletID)

	settled := awaitOperation(t, w.owner, providerA, reversal, rejected)
	if waited := time.Since(sent); waited < ttl {
		t.Errorf("the refund was rejected after %s, inside its %s wait budget: it gave up "+
			"rather than waiting", waited, ttl)
	}
	if settled.failureCode != referenceNotFound {
		t.Errorf("the refund is coded %q, want %q", settled.failureCode, referenceNotFound)
	}
	if settled.referenceAttempts < 2 {
		t.Errorf("the refund reports %d attempts, want more than one: it was parked and "+
			"looked at again before it was given up on", settled.referenceAttempts)
	}
	if settled.resolvedReference != nil {
		t.Errorf("the refund resolved to %s, and the operation it named was never submitted",
			*settled.resolvedReference)
	}

	// Nothing moved.
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "100.00"); got != want {
		t.Errorf("the wallet holds %d minor units, want %d untouched", got, want)
	}
	if entries := ledgerOf(t, w.owner, wallet.WalletID); len(entries) != 1 {
		t.Errorf("the wallet has %d ledger entries, want only the opening's", len(entries))
	}
	w.awaitEmpty(t, w.inboundURL, "the inbound queue")
	w.noneDeadLettered(t)
	workers.stop(t)
	reconciled(t, w.base, wallet.WalletID)
}

// pair is the two worker replicas a scenario runs, so that stopping them is one
// call rather than two that can drift apart.
type pair struct{ one, two *process }

// twoWorkers starts the two replicas a deployment runs, both consuming and both
// resuming, under the wait budget given.
//
// Two rather than one, and neither of them told which half of the work it is
// for. Which replica consumes a message and which one carries the operation it
// parked forward is whichever asks first, which is the property this is here to
// exercise: the parked operation is a row, and no process owns it.
func (w *world) twoWorkers(t *testing.T, budget map[string]string) pair {
	t.Helper()
	return pair{
		one: w.startWorker(t, "worker-one", workerSettings{
			consumes: true, resumes: true, extra: budget,
		}),
		two: w.startWorker(t, "worker-two", workerSettings{
			consumes: true, resumes: true, extra: budget,
		}),
	}
}

// stop shuts both replicas down the way a deployment does, and insists both go
// cleanly.
func (p pair) stop(t *testing.T) {
	t.Helper()
	p.one.stop(t)
	p.two.stop(t)
}
