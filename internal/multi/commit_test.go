//go:build multi

// A consumer killed on the other side of the line recovery_test.go kills on:
// before the commit rather than after it.
//
// The two scenarios are a pair and they fail in opposite directions. A process
// killed AFTER the commit and before the delete has done the work and not said
// so; the risk is doing it twice, and the inbox is what prevents that. A
// process killed BEFORE the commit has taken the message and done nothing that
// counts; the risk is the opposite — that something of the aborted transaction
// survives, or that the redelivery is mistaken for a replay of work that never
// landed. The transaction being one transaction is what prevents the first,
// and the inbox row having been part of it is what prevents the second.
package multi

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/faults"
)

// TestAConsumerKilledBeforeTheCommitLeavesNothingAndTheRedeliveryAppliesOnce
// kills a consumer after every statement of its command has run and before
// the COMMIT.
//
// Three things are asserted, in order. While the process is dead, NOTHING of
// the bet exists: no wager transaction, no ledger entry, no inbox row, no
// outbox row — every table a movement writes holds exactly what the opening
// left in it — and the message is still the queue's, in flight rather than
// deleted. Then a different process is delivered the message again — the
// queue says so, with a receive count of two — and applies it as a FIRST
// application: replay is false, because the inbox row that would have made it
// a replay died with the transaction it was written in. And at the end the
// wallet moved once, the message is gone, and nothing was dead-lettered.
func TestAConsumerKilledBeforeTheCommitLeavesNothingAndTheRedeliveryAppliesOnce(t *testing.T) {
	t.Parallel()
	w := newWorld(t, "commit-kill")

	player := scoped("player-killed-before-commit")
	external := scoped("uncommitted-bet")
	message := scoped("uncommitted-message")
	wallet := openWallet(t, w.base, player, "100.00")

	// What the opening left behind, which is what every table must still hold
	// after the death: one ledger entry and two events.
	const openingEntries, openingEvents = 1, 2
	if got := len(ledgerOf(t, w.owner, wallet.WalletID)); got != openingEntries {
		t.Fatalf("the wallet has %d ledger entries before the bet, want %d", got, openingEntries)
	}
	if got := eventsFor(t, w, wallet.WalletID); got != openingEvents {
		t.Fatalf("the outbox holds %d events before the bet, want %d", got, openingEvents)
	}

	armed := w.startWorker(t, "consumer-armed", workerSettings{
		consumes:   true,
		faultPoint: faults.BeforeCommit,
	})
	w.put(t, onTheQueue(bet(providerA, external, wallet, "30.00"), message,
		scoped("uncommitted-key")), wallet.WalletID)
	diedAtFaultPoint(t, armed, faults.BeforeCommit, faultBudget)

	// Nothing of it landed.
	if _, recorded := operationFor(t, w.owner, providerA, external); recorded {
		t.Fatalf("the bet %s is recorded, and the process died before it committed", external)
	}
	if _, found := inboxFor(t, w.owner, consumerName, message); found {
		t.Errorf("the inbox holds a row for %s, and the transaction it was written in never "+
			"committed: the redelivery would be answered as a replay of nothing", message)
	}
	if got := len(ledgerOf(t, w.owner, wallet.WalletID)); got != openingEntries {
		t.Errorf("the wallet has %d ledger entries after the death, want the opening's %d",
			got, openingEntries)
	}
	if got := eventsFor(t, w, wallet.WalletID); got != openingEvents {
		t.Errorf("the outbox holds %d events after the death, want the opening's %d",
			got, openingEvents)
	}
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "100.00"); got != want {
		t.Errorf("the wallet holds %d minor units after the death, want %d untouched", got, want)
	}
	// And the message is still the queue's: taken by the dead process and
	// neither deleted nor given back, so it is in flight until the visibility
	// timeout returns it.
	if visible, hidden := depth(t, w.inboundURL); visible+hidden != 1 {
		t.Errorf("the inbound queue holds %d visible and %d in flight, want the one message "+
			"the dead process was holding", visible, hidden)
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
	if replay, stated := line.flag("replay"); !stated || replay {
		t.Errorf("the replacement applied %s with replay=%v: it was answered from work that "+
			"never committed", message, line.fields["replay"])
	}
	if got := line.text("status"); got != processed {
		t.Errorf("the replacement settled %s as %s, wanted %s", message, got, processed)
	}

	applied := awaitOperation(t, w.owner, providerA, external, processed)
	if got := line.text("transactionId"); got != applied.id {
		t.Errorf("the replacement answered transaction %s and the table holds %s", got, applied.id)
	}
	recorded, found := inboxFor(t, w.owner, consumerName, message)
	if !found {
		t.Fatalf("the inbox holds no row for %s after the redelivery was applied", message)
	}
	if !recorded.completed {
		t.Errorf("the inbox row for %s is not completed", message)
	}

	// Applied once: one debit, and a balance that moved once.
	debits := countRows(t, w.owner,
		`SELECT count(*) FROM wagering.wallet_ledger_entry `+
			`WHERE wallet_id = $1 AND direction = 'DEBIT'`, wallet.WalletID)
	if debits != 1 {
		t.Errorf("the wallet has %d debits after the redelivery, wanted 1", debits)
	}
	if got, want := balanceOf(t, w.owner, wallet.WalletID), minor(t, "70.00"); got != want {
		t.Errorf("the wallet holds %d minor units after the redelivery, want %d", got, want)
	}
	if got := eventsFor(t, w, wallet.WalletID); got != openingEvents+2 {
		t.Errorf("the outbox holds %d events after the redelivery, want %d: the bet's two, once",
			got, openingEvents+2)
	}

	w.awaitEmpty(t, w.inboundURL, "the inbound queue")
	w.noneDeadLettered(t)
	replacement.stop(t)
	reconciled(t, w.base, wallet.WalletID)
}

// eventsFor counts the outbox rows one wallet has, published or not.
func eventsFor(t *testing.T, w *world, wallet string) int {
	t.Helper()
	return countRows(t, w.owner,
		`SELECT count(*) FROM wagering.outbox WHERE aggregate_id = $1`, wallet)
}
