package wagering

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// TestEventConstructorsFixTypeAndVersion covers the rule that neither can be
// set by a caller: there is no field to set, only a constructor that decides.
func TestEventConstructorsFixTypeAndVersion(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "100.00")

	processedOut := mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime)
	processed, err := NewWagerTransactionProcessed(processedOut.Transaction)
	if err != nil {
		t.Fatalf("NewWagerTransactionProcessed: %v", err)
	}

	rejectedOut := submitOK(t, p, w, command(t, Bet, "10000.00"), nil, baseTime)
	rejected, err := NewWagerTransactionRejected(rejectedOut.Transaction)
	if err != nil {
		t.Fatalf("NewWagerTransactionRejected: %v", err)
	}

	waitingOut := submitOK(t, p, w,
		command(t, Refund, "25.00", withReference("ext-missing")), &ReferenceView{}, baseTime)
	waiting, err := NewWagerTransactionPendingReference(waitingOut.Transaction)
	if err != nil {
		t.Fatalf("NewWagerTransactionPendingReference: %v", err)
	}

	balance, err := NewWalletBalanceChanged(*processedOut.LedgerEntry)
	if err != nil {
		t.Fatalf("NewWalletBalanceChanged: %v", err)
	}

	events := map[Event]string{
		processed: "WagerTransactionProcessed",
		rejected:  "WagerTransactionRejected",
		waiting:   "WagerTransactionPendingReference",
		balance:   "WalletBalanceChanged",
	}
	for event, wantType := range events {
		if got := event.EventType(); got != wantType {
			t.Errorf("EventType() = %q, want %q", got, wantType)
		}
		if got := event.EventVersion(); got != 1 {
			t.Errorf("%s EventVersion() = %d, want 1", wantType, got)
		}
	}
}

// TestWalletBalanceChangedCarriesTheSpecifiedPayload checks the one event whose
// payload the brief sets out in full.
func TestWalletBalanceChangedCarriesTheSpecifiedPayload(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "100.00")
	out := mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime)
	entry := *out.LedgerEntry

	event, ok := out.Events[1].(WalletBalanceChanged)
	if !ok {
		t.Fatalf("second event is %T, want WalletBalanceChanged", out.Events[1])
	}

	// The event and the entry describe the same moment, so building one from the
	// other keeps them from disagreeing.
	if event.WalletID() != entry.WalletID() {
		t.Error("walletId disagrees with the ledger entry")
	}
	if event.TransactionID() != entry.TransactionID() {
		t.Error("transactionId disagrees with the ledger entry")
	}
	if event.Direction() != entry.Direction() {
		t.Error("direction disagrees with the ledger entry")
	}
	if !event.Money().Equal(entry.Amount()) {
		t.Error("money disagrees with the ledger entry")
	}
	if !event.BalanceBefore().Equal(entry.BalanceBefore()) {
		t.Error("balanceBefore disagrees with the ledger entry")
	}
	if !event.BalanceAfter().Equal(entry.BalanceAfter()) {
		t.Error("balanceAfter disagrees with the ledger entry")
	}
	if event.WalletVersion() != entry.WalletVersion() {
		t.Error("walletVersion disagrees with the ledger entry")
	}
	if event.WalletVersion() != w.Version() {
		t.Errorf("walletVersion = %d, want the wallet's %d", event.WalletVersion(), w.Version())
	}
}

// TestEventsCannotContradictTheirTransaction covers the property that an event
// is built from an aggregate already in the state it reports.
func TestEventsCannotContradictTheirTransaction(t *testing.T) {
	t.Parallel()

	pending := txInStatus(t, Pending)

	if _, err := NewWagerTransactionProcessed(pending); !failure.Is(err, failure.InvalidStateTransition) {
		t.Errorf("reporting a pending transaction as processed = %v, want %v",
			err, failure.InvalidStateTransition)
	}
	if _, err := NewWagerTransactionRejected(pending); !failure.Is(err, failure.InvalidStateTransition) {
		t.Errorf("reporting a pending transaction as rejected = %v, want %v",
			err, failure.InvalidStateTransition)
	}
	if _, err := NewWagerTransactionPendingReference(pending); !failure.Is(err, failure.InvalidStateTransition) {
		t.Errorf("reporting a pending transaction as waiting = %v, want %v",
			err, failure.InvalidStateTransition)
	}

	for name, build := range map[string]func() error{
		"processed": func() error { _, err := NewWagerTransactionProcessed(nil); return err },
		"rejected":  func() error { _, err := NewWagerTransactionRejected(nil); return err },
		"waiting":   func() error { _, err := NewWagerTransactionPendingReference(nil); return err },
		"balance":   func() error { _, err := NewWalletBalanceChanged(WalletLedgerEntry{}); return err },
	} {
		if err := build(); !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("%s event from a missing source = %v, want %v",
				name, err, failure.UninitializedValue)
		}
	}
}

// TestWalletBalanceChangedRefusesAnEntryNobodyConstructed covers the one gap
// unexported fields leave open. Every field of a [WalletLedgerEntry] is
// unexported, but a composite literal needs permission only to name a field,
// and WalletLedgerEntry{} names none — so any package at all can write one, and
// the event constructor is the only thing standing between that value and a
// published claim that a balance changed.
//
// What makes it worth a guard rather than a comment is that the resulting event
// is not obviously broken. It reports a real direction ("") and a real amount
// (uninitialised) on a real wallet (the zero UUID), so a subscriber that
// credits what it is told would act on it.
func TestWalletBalanceChangedRefusesAnEntryNobodyConstructed(t *testing.T) {
	t.Parallel()

	// Exactly the value any caller outside this package can produce: the type
	// with no field named, which is the only composite literal of it they can
	// write.
	_, err := NewWalletBalanceChanged(WalletLedgerEntry{})
	if !failure.Is(err, failure.UninitializedValue) {
		t.Fatalf("an unconstructed entry = %v, want %v", err, failure.UninitializedValue)
	}

	// The control: an entry that did come from the constructor still reports,
	// so the guard is not simply refusing everything.
	p, w := newProcessor(t), newWallet(t, "100.00")
	out := mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime)
	event, err := NewWalletBalanceChanged(*out.LedgerEntry)
	if err != nil {
		t.Fatalf("a constructed entry: %v", err)
	}
	if event.WalletID() != w.ID() {
		t.Error("the event names a different wallet from the entry it was built from")
	}
}

// TestEventOrderIsDeterministic pins the order the brief wrote for wallet
// opening, applied to every operation so the slice can be asserted.
func TestEventOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	t.Run("wallet opening", func(t *testing.T) {
		t.Parallel()
		_, out, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet: %v", err)
		}
		assertEventTypes(t, out, typeWagerTransactionProcessed, typeWalletBalanceChanged)
	})

	t.Run("an operation that moves money", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		out := mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime)
		assertEventTypes(t, out, typeWagerTransactionProcessed, typeWalletBalanceChanged)
	})
}

// TestOpeningEventsReportAnInternalOrigin covers the fact that an opening has no
// provider side to report.
func TestOpeningEventsReportAnInternalOrigin(t *testing.T) {
	t.Parallel()

	_, out, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}

	event, ok := out.Events[0].(WagerTransactionProcessed)
	if !ok {
		t.Fatalf("first event is %T, want WagerTransactionProcessed", out.Events[0])
	}
	if event.Kind() != Opening {
		t.Errorf("kind = %s, want %s", event.Kind(), Opening)
	}
	if id, present := event.ExternalTransactionID(); present {
		t.Errorf("an opening reported an external transaction id, %q", id)
	}
	if got := event.BalanceAfter().Amount(); got != "100.00" {
		t.Errorf("balanceAfter = %s, want 100.00", got)
	}
}

// TestEventsMirrorTheirTransaction reads every accessor back, since an event is
// a projection: anything it reports that the transaction does not is a lie the
// subscriber has no way to detect.
//
// Each subtest builds its own wallet. A wallet is an aggregate, not a
// concurrency primitive — where it is stored, its version is what serialises
// concurrent writers — so nothing shares one across parallel subtests.
func TestEventsMirrorTheirTransaction(t *testing.T) {
	t.Parallel()

	t.Run("processed", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		out := mustSubmit(t, p, w, command(t, Win, "25.00"), nil, baseTime)
		tx := out.Transaction
		e := out.Events[0].(WagerTransactionProcessed)

		if e.TransactionID() != tx.ID() || e.WalletID() != tx.WalletID() || e.PlayerID() != tx.PlayerID() {
			t.Error("the event names a different transaction, wallet or player")
		}
		if e.Kind() != tx.Kind() || !e.Money().Equal(tx.Money()) {
			t.Error("the event reports a different kind or amount")
		}
		result, _ := tx.Result()
		if !e.BalanceAfter().Equal(result) {
			t.Error("the event reports a different balance from the transaction's result")
		}
		if id, ok := e.ExternalTransactionID(); !ok || id != externalID(t, tx) {
			t.Error("the event reports a different external identifier")
		}
	})

	t.Run("rejected", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "1.00")
		out := submitOK(t, p, w, command(t, Bet, "25.00"), nil, baseTime)
		tx := out.Transaction
		e := out.Events[0].(WagerTransactionRejected)

		if e.TransactionID() != tx.ID() || e.WalletID() != tx.WalletID() || e.PlayerID() != tx.PlayerID() {
			t.Error("the event names a different transaction, wallet or player")
		}
		if e.Kind() != tx.Kind() || !e.Money().Equal(tx.Money()) {
			t.Error("the event reports a different kind or amount")
		}
		code, _ := tx.FailureCode()
		if e.FailureCode() != code {
			t.Error("the event reports a different failure code")
		}
		if id, ok := e.ExternalTransactionID(); !ok || id != externalID(t, tx) {
			t.Error("the event reports a different external identifier")
		}
	})

	t.Run("waiting for a reference", func(t *testing.T) {
		t.Parallel()
		p, w := newProcessor(t), newWallet(t, "100.00")
		const missing = ExternalTransactionID("ext-not-here")
		out := submitOK(t, p, w,
			command(t, Rollback, "25.00", withReference(missing)), &ReferenceView{}, baseTime)
		tx := out.Transaction
		e := out.Events[0].(WagerTransactionPendingReference)

		if e.TransactionID() != tx.ID() || e.WalletID() != tx.WalletID() || e.PlayerID() != tx.PlayerID() {
			t.Error("the event names a different transaction, wallet or player")
		}
		if e.Kind() != tx.Kind() {
			t.Error("the event reports a different kind")
		}
		if e.ReferenceExternalTransactionID() != missing {
			t.Errorf("the event is waiting on %q, want %q", e.ReferenceExternalTransactionID(), missing)
		}
		if e.Attempts() != tx.ReferenceAttempts() || e.Attempts() != 1 {
			t.Errorf("attempts = %d, want the transaction's %d", e.Attempts(), tx.ReferenceAttempts())
		}
		if e.ExternalTransactionID() != externalID(t, tx) {
			t.Error("the event reports a different external identifier")
		}
	})
}

// TestIdentifiersRenderAsUUIDs covers the string forms used in logs, messages
// and persistence.
func TestIdentifiersRenderAsUUIDs(t *testing.T) {
	t.Parallel()

	const uuidLength = 36

	walletID, txID, entryID := NewWalletID(), NewTransactionID(), NewLedgerEntryID()
	for name, rendered := range map[string]string{
		"wallet":      walletID.String(),
		"transaction": txID.String(),
		"ledgerEntry": entryID.String(),
	} {
		if len(rendered) != uuidLength {
			t.Errorf("%s renders as %q, want a %d-character UUID", name, rendered, uuidLength)
		}
	}

	parsedTx, err := ParseTransactionID(txID.String())
	if err != nil || parsedTx != txID {
		t.Errorf("ParseTransactionID round trip = %v, %v", parsedTx, err)
	}
	parsedEntry, err := ParseLedgerEntryID(entryID.String())
	if err != nil || parsedEntry != entryID {
		t.Errorf("ParseLedgerEntryID round trip = %v, %v", parsedEntry, err)
	}
}

func TestRejectedEventCarriesTheFailureCode(t *testing.T) {
	t.Parallel()

	p, w := newProcessor(t), newWallet(t, "10.00")
	out := submitOK(t, p, w, command(t, Bet, "25.00"), nil, baseTime)

	event, ok := out.Events[0].(WagerTransactionRejected)
	if !ok {
		t.Fatalf("first event is %T, want WagerTransactionRejected", out.Events[0])
	}
	if event.FailureCode() != failure.InsufficientFunds {
		t.Errorf("failureCode = %s, want %s", event.FailureCode(), failure.InsufficientFunds)
	}
	if !event.FailureCode().Definitive() {
		t.Error("a published rejection carries a correctable code")
	}
}
