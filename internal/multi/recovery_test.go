//go:build multi

// Two processes killed between a commit and the thing that was supposed to
// follow it, and what the process that comes after them finds.
//
// # How a killed process comes back, and why it is a different process
//
// In both scenarios the work is picked up by a SECOND process started after the
// first has died, rather than by a peer that was running alongside it. That is
// a deliberate loss of realism bought for determinism, and it costs nothing the
// scenarios are about.
//
// A peer running alongside would race the armed process for the message or for
// the parked operation, and which of the two got it would be a coin toss: half
// the runs would kill nothing and pass. Nothing in either property depends on
// the second process having been alive earlier — what is being asserted is that
// a DIFFERENT process, with its own pool, its own memory and no idea what the
// first one had in flight, finds the work and does not do it twice. The
// database is what tells it, and the database does not know when the second
// process started.
package multi

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/faults"
)

// The wait budget scenario 8 runs under.
//
// Generous in both bounds, because that scenario is not about the budget: the
// refund has to still be waiting when the bet arrives, and a budget that ran
// out while a process was being killed and replaced would settle the operation
// for a reason the scenario is not testing. The schedule underneath it is fast
// for the opposite reason — how often a parked operation is looked at is a
// setting with no business meaning, and this scenario is waiting on exactly
// that loop.
var patientReference = map[string]string{
	"REFERENCE_MAX_ATTEMPTS":   "500",
	"REFERENCE_TTL":            "3m",
	"WAGERING_BACKOFF_INITIAL": "200ms",
	"WAGERING_BACKOFF_FACTOR":  "2",
	"WAGERING_BACKOFF_MAX":     "500ms",
}

// TestAConsumerKilledBetweenTheCommitAndTheDeleteAppliesTheMessageOnce kills a
// consumer in the one window where the queue and the database disagree.
//
// The window is real and cannot be designed away: the wager transaction has
// committed and the message has not been deleted, so the queue will deliver it
// again to whoever asks next. What must not happen is the operation being
// applied twice, and the inbox row that committed inside the same transaction
// as the work is the whole of why it is not.
//
// Three things are asserted and the middle one is the one a balance-only test
// would miss. The wallet moved once. The queue DELIVERED THE MESSAGE AGAIN —
// the replacement's own line says receiveCount is two and replay is true, which
// is the queue and the inbox each reporting a duplicate they actually saw. And
// the message is gone at the end, because a replay that did not delete it would
// leave the message going round until the redrive policy gave up on it.
func TestAConsumerKilledBetweenTheCommitAndTheDeleteAppliesTheMessageOnce(t *testing.T) {
	t.Parallel()
	w := newWorld(t, "consumer-kill")

	player := scoped("player-killed-consumer")
	external := scoped("killed-bet")
	message := scoped("killed-message")
	wallet := openWallet(t, w.base, player, "100.00")

	armed := w.startWorker(t, "consumer-armed", workerSettings{
		consumes:   true,
		faultPoint: faults.AfterCommitBeforeAck,
	})
	w.put(t, onTheQueue(bet(providerA, external, player, "30.00"), message,
		scoped("killed-key")), wallet.WalletID)
	diedAtFaultPoint(t, armed, faults.AfterCommitBeforeAck, faultBudget)

	// It committed before it died, which is what makes the redelivery below a
	// duplicate rather than the first attempt.
	applied := awaitOperation(t, w.owner, providerA, external, processed)
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "70.00"); got != want {
		t.Fatalf("the wallet holds %d minor units after the commit, want %d", got, want)
	}
	recorded, found := inboxFor(t, w.owner, consumerName, message)
	if !found {
		t.Fatalf("the inbox holds no row for %s, so nothing will stop the redelivery being "+
			"applied again", message)
	}
	if !recorded.completed {
		t.Errorf("the inbox row for %s is not completed, and the work it covers committed",
			message)
	}

	replacement := w.startWorker(t, "consumer-replacement", workerSettings{consumes: true})
	line := replacement.awaitLine(t, settleBudget,
		"being delivered "+message+" again", func(e entry) bool {
			return e.message() == "the operation was applied" && e.text("messageId") == message
		})
	if count, stated := line.number("receiveCount"); !stated || count < 2 {
		t.Errorf("the replacement saw %v as the delivery count for %s, wanted at least 2: "+
			"the queue did not report this as a redelivery", line.fields["receiveCount"], message)
	}
	if replay, stated := line.flag("replay"); !stated || !replay {
		t.Errorf("the replacement applied %s with replay=%v: the inbox did not recognise a "+
			"message it had already handled", message, line.fields["replay"])
	}
	if got := line.text("transactionId"); got != applied.id {
		t.Errorf("the replacement answered transaction %s and the killed process committed "+
			"%s: one message became two operations", got, applied.id)
	}

	// The message is deleted this time, so nothing is left going round.
	w.awaitEmpty(t, w.inboundURL, "the inbound queue")
	w.noneDeadLettered(t)

	debits := countRows(t, w.owner,
		`SELECT count(*) FROM wagering.wallet_ledger_entry `+
			`WHERE wallet_id = $1 AND direction = 'DEBIT'`, wallet.WalletID)
	if debits != 1 {
		t.Errorf("the wallet has %d debits after the redelivery, wanted 1", debits)
	}
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "70.00"); got != want {
		t.Errorf("the wallet holds %d minor units after the redelivery, want %d", got, want)
	}
	replacement.stop(t)
	reconciled(t, w.base, wallet.WalletID)
}

// TestAReferenceWorkerKilledAfterItParkedAnOperationIsResumedByAnother kills
// the worker that has just written down when to look at a parked operation
// again, before it has done anything about it.
//
// A refund arrives before the bet it reverses, so it is parked. The worker
// picks it up, finds the bet still absent, parks it again with a new attempt
// count and a new time to look — and dies on the commit that recorded that.
// The operation is now in a state nothing is watching: no process holds it, no
// message carries it, and the only thing that knows it exists is a row.
//
// What proves the recovery is not the refund eventually being processed — a
// system that lost the parked operation and re-derived it from the bet would
// also end up with the right balance. It is `reference_attempts`, which is
// greater than zero before the second worker starts and which the second worker
// carries on from: the operation the replacement settled is the one the dead
// process parked, not a new one.
func TestAReferenceWorkerKilledAfterItParkedAnOperationIsResumedByAnother(t *testing.T) {
	t.Parallel()
	w := newWorld(t, "reference-kill")

	player := scoped("player-killed-reference")
	stake := scoped("killed-reference-bet")
	reversal := scoped("killed-reference-refund")
	// One round for both, because a reversal and the operation it reverses must
	// agree on it.
	round := scoped("killed-reference-round")
	wallet := openWallet(t, w.base, player, "100.00")

	armed := w.startWorker(t, "reference-armed", workerSettings{
		consumes:   true,
		resumes:    true,
		faultPoint: faults.AfterPendingCommit,
		extra:      patientReference,
	})
	// The refund first, naming a bet nobody has submitted. The group is the
	// wallet, so the order these two arrive in is the queue's guarantee rather
	// than this test's hope.
	w.put(t, onTheQueue(inRound(refund(providerA, reversal, player, "40.00", stake), round),
		scoped("killed-reference-refund-message"), scoped("killed-reference-refund-key")),
		wallet.WalletID)

	diedAtFaultPoint(t, armed, faults.AfterPendingCommit, faultBudget)

	parked, recorded := operationFor(t, w.owner, providerA, reversal)
	if !recorded {
		t.Fatalf("the refund was never recorded, so nothing was parked to resume")
	}
	if parked.status != pendingReference {
		t.Fatalf("the refund is %s, wanted %s: the process did not die with an operation "+
			"parked", parked.status, pendingReference)
	}
	if parked.referenceAttempts < 1 {
		t.Fatalf("the refund reports %d attempts at its reference, wanted at least one: the "+
			"process died before it had resumed anything", parked.referenceAttempts)
	}

	// The bet arrives, and a different process finds both it and the operation
	// the dead one left parked.
	w.put(t, onTheQueue(inRound(bet(providerA, stake, player, "40.00"), round),
		scoped("killed-reference-bet-message"), scoped("killed-reference-bet-key")),
		wallet.WalletID)
	replacement := w.startWorker(t, "reference-replacement", workerSettings{
		consumes: true,
		resumes:  true,
		extra:    patientReference,
	})

	staked := awaitOperation(t, w.owner, providerA, stake, processed)
	settled := awaitOperation(t, w.owner, providerA, reversal, processed)
	if settled.resolvedReference == nil || *settled.resolvedReference != staked.id {
		t.Errorf("the refund resolved to %v, want the bet %s", settled.resolvedReference,
			staked.id)
	}
	if settled.referenceAttempts < parked.referenceAttempts {
		t.Errorf("the refund reports %d attempts and had %d before the process died: the "+
			"replacement started a new wait rather than continuing the parked one",
			settled.referenceAttempts, parked.referenceAttempts)
	}

	// The stake went out and came back.
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "100.00"); got != want {
		t.Errorf("the wallet holds %d minor units, want %d", got, want)
	}
	w.awaitEmpty(t, w.inboundURL, "the inbound queue")
	w.noneDeadLettered(t)
	replacement.stop(t)
	reconciled(t, w.base, wallet.WalletID)
}
