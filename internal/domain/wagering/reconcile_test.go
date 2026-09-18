package wagering

import (
	"errors"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// ledgerOf runs a sequence of operations and collects the entries they wrote,
// which is what a repository would later hand back to reconciliation.
func ledgerOf(t *testing.T, w *Wallet, outcomes ...Outcome) []WalletLedgerEntry {
	t.Helper()
	var entries []WalletLedgerEntry
	for _, out := range outcomes {
		if out.LedgerEntry != nil {
			entries = append(entries, *out.LedgerEntry)
		}
	}
	return entries
}

func TestLedgerBalance(t *testing.T) {
	t.Parallel()

	t.Run("an empty ledger sums to zero in the given currency", func(t *testing.T) {
		t.Parallel()
		total, err := LedgerBalance(currency(t, "BRL"), nil)
		if err != nil {
			t.Fatalf("LedgerBalance: %v", err)
		}
		if got := total.Amount(); got != "0.00" {
			t.Errorf("empty ledger = %s, want 0.00", got)
		}
		if total.Currency() != currency(t, "BRL") {
			t.Errorf("currency = %s, want BRL", total.Currency())
		}
	})

	t.Run("credits less debits", func(t *testing.T) {
		t.Parallel()
		p := newProcessor(t)
		in := openInput(t, "100.00")
		w, opening, err := OpenWallet(in, nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet: %v", err)
		}
		bet := mustSubmit(t, p, w, command(t, Bet, "30.00"), nil, baseTime)
		win := mustSubmit(t, p, w, command(t, Win, "10.00"), nil, baseTime)

		entries := ledgerOf(t, w, opening, bet, win)
		total, err := LedgerBalance(w.Currency(), entries)
		if err != nil {
			t.Fatalf("LedgerBalance: %v", err)
		}
		if got := total.Amount(); got != "80.00" {
			t.Errorf("ledger sums to %s, want 80.00", got)
		}
		if !total.Equal(w.Balance()) {
			t.Errorf("ledger sums to %s but the wallet holds %s", total, w.Balance())
		}
	})

	// The sum is order-independent, which is why a ledger needs no ordering to
	// be reconcilable.
	t.Run("is order-independent", func(t *testing.T) {
		t.Parallel()
		p := newProcessor(t)
		in := openInput(t, "100.00")
		w, opening, err := OpenWallet(in, nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet: %v", err)
		}
		bet := mustSubmit(t, p, w, command(t, Bet, "30.00"), nil, baseTime)
		win := mustSubmit(t, p, w, command(t, Win, "10.00"), nil, baseTime)

		forwards := ledgerOf(t, w, opening, bet, win)
		backwards := ledgerOf(t, w, win, bet, opening)

		first, err := LedgerBalance(w.Currency(), forwards)
		if err != nil {
			t.Fatalf("LedgerBalance: %v", err)
		}
		second, err := LedgerBalance(w.Currency(), backwards)
		if err != nil {
			t.Fatalf("LedgerBalance: %v", err)
		}
		if !first.Equal(second) {
			t.Errorf("order changed the sum: %s then %s", first, second)
		}
	})

	t.Run("refuses an entry in another currency", func(t *testing.T) {
		t.Parallel()
		p := newProcessor(t)
		w := newWallet(t, "100.00")
		bet := mustSubmit(t, p, w, command(t, Bet, "30.00"), nil, baseTime)

		_, err := LedgerBalance(currency(t, "USD"), ledgerOf(t, w, bet))
		if !failure.Is(err, failure.CurrencyMismatch) {
			t.Errorf("LedgerBalance = %v, want %v", err, failure.CurrencyMismatch)
		}
	})

	t.Run("refuses an entry that was never constructed", func(t *testing.T) {
		t.Parallel()
		_, err := LedgerBalance(currency(t, "BRL"), []WalletLedgerEntry{{}})
		if !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("LedgerBalance = %v, want %v", err, failure.UninitializedValue)
		}
	})
}

// TestReconcileAgreesAfterAFullSequence exercises the brief's rule that the
// stored balance is always reconstructible from the ledger, including the
// opening, across every kind that moves money.
func TestReconcileAgreesAfterAFullSequence(t *testing.T) {
	t.Parallel()

	p := newProcessor(t)
	w, opening, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}

	bet := mustSubmit(t, p, w, command(t, Bet, "30.00"), nil, baseTime)
	win := mustSubmit(t, p, w, command(t, Win, "12.00"), nil, baseTime)
	loss := mustSubmit(t, p, w, command(t, Loss, "0.00"), nil, baseTime)

	betTx := bet.Transaction
	refund := mustSubmit(t, p, w,
		command(t, Refund, "30.00", withReference(externalID(t, betTx))), referenceTo(betTx), baseTime)
	refundTx := refund.Transaction
	rollback := mustSubmit(t, p, w,
		command(t, Rollback, "30.00", withReference(externalID(t, refundTx))), referenceTo(refundTx), baseTime)

	// 100 - 30 + 12 + 30 - 30 = 82
	assertBalance(t, w, "82.00")
	if loss.LedgerEntry != nil {
		t.Error("the loss wrote a ledger entry")
	}

	entries := ledgerOf(t, w, opening, bet, win, loss, refund, rollback)
	if len(entries) != 5 {
		t.Fatalf("ledger has %d entries, want 5 (the loss writes none)", len(entries))
	}
	if err := Reconcile(w, entries); err != nil {
		t.Errorf("Reconcile: %v", err)
	}
}

func TestReconcileOnAWalletOpenedAtZero(t *testing.T) {
	t.Parallel()

	w := newWallet(t, "0.00")
	if err := Reconcile(w, nil); err != nil {
		t.Errorf("Reconcile of an empty ledger: %v", err)
	}
}

func TestReconcileReportsDisagreement(t *testing.T) {
	t.Parallel()

	p := newProcessor(t)
	w, opening, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	mustSubmit(t, p, w, command(t, Bet, "30.00"), nil, baseTime)

	// The bet's entry is missing from the ledger handed back.
	err = Reconcile(w, ledgerOf(t, w, opening))

	mismatch, ok := errors.AsType[*ReconciliationError](err)
	if !ok {
		t.Fatalf("Reconcile = %v, want a ReconciliationError", err)
	}
	if mismatch.Expected().Amount() != "100.00" || mismatch.Actual().Amount() != "70.00" {
		t.Errorf("reported %s expected against %s actual, want 100.00 against 70.00",
			mismatch.Expected().Amount(), mismatch.Actual().Amount())
	}
	// Reconciliation reports; it never corrects.
	assertBalance(t, w, "70.00")
	assertVersion(t, w, 2)
}

// TestNewReconciliationErrorRefusesANonFinding covers the zero-value rule on
// the one type here that used to be exempt from it.
//
// A finding asserts that a wallet's books do not balance, which is a claim that
// starts an investigation. With exported fields, any package could write a fully
// populated one naming any wallet and any two amounts, and nothing about the
// value would tell it apart from a finding this package made — so the fix was
// not a guard but unexported fields and this constructor, which reduces what an
// outsider can forge to the empty value.
//
// The last case is the one worth having: two amounts that agree assert a
// disagreement that is not there, which is the only way to build a finding that
// is internally consistent and still false.
func TestNewReconciliationErrorRefusesANonFinding(t *testing.T) {
	t.Parallel()

	walletID := NewWalletID()

	tests := []struct {
		name             string
		id               WalletID
		expected, actual money.Money
		want             failure.Code
	}{
		{"no wallet", WalletID{}, brl(t, "100.00"), brl(t, "70.00"), failure.MissingRequiredField},
		{"no expected balance", walletID, money.Money{}, brl(t, "70.00"), failure.UninitializedValue},
		{"no actual balance", walletID, brl(t, "100.00"), money.Money{}, failure.UninitializedValue},
		{"balances in different currencies", walletID, brl(t, "100.00"), usd(t, "70.00"), failure.CurrencyMismatch},
		{"balances that agree", walletID, brl(t, "100.00"), brl(t, "100.00"), failure.InvalidFieldFormat},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewReconciliationError(tc.id, tc.expected, tc.actual)
			if !failure.Is(err, tc.want) {
				t.Fatalf("NewReconciliationError = %v, want %v", err, tc.want)
			}
			if got != nil {
				t.Error("a refused finding was still returned")
			}
		})
	}

	t.Run("a real disagreement is recorded", func(t *testing.T) {
		t.Parallel()
		got, err := NewReconciliationError(walletID, brl(t, "100.00"), brl(t, "70.00"))
		if err != nil {
			t.Fatalf("NewReconciliationError: %v", err)
		}
		if got.WalletID() != walletID {
			t.Error("the finding names a different wallet")
		}
		if got.Expected().Amount() != "100.00" || got.Actual().Amount() != "70.00" {
			t.Errorf("reported %s against %s, want 100.00 against 70.00",
				got.Expected().Amount(), got.Actual().Amount())
		}
	})

	// The empty value is the one thing another package can still write. It has
	// to read as no finding rather than as a real one with its numbers missing,
	// which is how a blank would otherwise be taken.
	t.Run("the empty value says so", func(t *testing.T) {
		t.Parallel()
		empty := &ReconciliationError{}
		if got := empty.Error(); !strings.Contains(got, "never constructed") {
			t.Errorf("Error() = %q, want it to report that nothing was constructed", got)
		}
	})
}

// TestReconciliationErrorIsReachableByCode covers the rule that every refusal
// the domain raises names a documented code reachable through both errors.Is
// and errors.As.
//
// Reconcile returns five findings. Four are built with failure.New and carry a
// code; the disagreement — the one the function exists to detect — was a bare
// struct, so a caller switching on failure.CodeOf fell into its unknown branch
// for the most serious result and handled the lesser four. Worse, the unknown
// branch is the one a caller is least likely to have thought about.
//
// Both reachability directions are asserted, because they serve different
// callers: a transport layer maps the code to a response, while an operator
// tool needs the two balances that disagree. Neither may cost the other.
func TestReconciliationErrorIsReachableByCode(t *testing.T) {
	t.Parallel()

	p := newProcessor(t)
	w, opening, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	mustSubmit(t, p, w, command(t, Bet, "30.00"), nil, baseTime)

	// The bet's entry is missing from the ledger handed back.
	err = Reconcile(w, ledgerOf(t, w, opening))
	if err == nil {
		t.Fatal("Reconcile agreed with a ledger missing an entry")
	}

	if !failure.Is(err, failure.LedgerBalanceMismatch) {
		t.Errorf("failure.Is = false, want the finding to carry %s", failure.LedgerBalanceMismatch)
	}
	if code, ok := failure.CodeOf(err); !ok || code != failure.LedgerBalanceMismatch {
		t.Errorf("CodeOf = %q, %v, want %s, true", code, ok, failure.LedgerBalanceMismatch)
	}
	if !errors.Is(err, failure.New(failure.LedgerBalanceMismatch, "any message at all")) {
		t.Error("errors.Is did not match the finding against its own code")
	}
	coded, ok := errors.AsType[*failure.Error](err)
	if !ok {
		t.Fatal("errors.AsType did not find a failure.Error")
	}
	if coded.Code != failure.LedgerBalanceMismatch {
		t.Errorf("Code = %s, want %s", coded.Code, failure.LedgerBalanceMismatch)
	}

	// The structured finding must survive the change, or the fix traded one
	// caller's needs for another's.
	mismatch, ok := errors.AsType[*ReconciliationError](err)
	if !ok {
		t.Fatal("errors.AsType no longer finds the ReconciliationError")
	}
	if mismatch.Expected().Amount() != "100.00" || mismatch.Actual().Amount() != "70.00" {
		t.Errorf("reported %s expected against %s actual, want 100.00 against 70.00",
			mismatch.Expected().Amount(), mismatch.Actual().Amount())
	}
	if mismatch.WalletID() != w.ID() {
		t.Error("the finding names a different wallet")
	}

	// A corrupt ledger is never an invitation to resubmit anything.
	if failure.Correctable(err) {
		t.Error("a balance disagreement reports as correctable")
	}
	// And it settles no transaction: nothing was in flight for it to settle.
	if !failure.LedgerBalanceMismatch.Audit() {
		t.Error("the code is not marked as an audit finding")
	}

	// The message still says which balances disagree, now under its code.
	if got := err.Error(); !strings.Contains(got, "100.00") ||
		!strings.Contains(got, "70.00") ||
		!strings.HasPrefix(got, string(failure.LedgerBalanceMismatch)) {
		t.Errorf("Error() = %q, want it to lead with the code and name both balances", got)
	}
}

func TestReconcileRefuses(t *testing.T) {
	t.Parallel()

	t.Run("two entries for one transaction", func(t *testing.T) {
		t.Parallel()
		p := newProcessor(t)
		w, opening, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet: %v", err)
		}
		bet := mustSubmit(t, p, w, command(t, Bet, "30.00"), nil, baseTime)

		// Upstream this cannot happen — an operation yields at most one entry —
		// so finding it means a defect somewhere else.
		entries := ledgerOf(t, w, opening, bet, bet)
		err = Reconcile(w, entries)
		if !failure.Is(err, failure.InvalidFieldFormat) {
			t.Errorf("Reconcile = %v, want %v", err, failure.InvalidFieldFormat)
		}
	})

	t.Run("an entry belonging to another wallet", func(t *testing.T) {
		t.Parallel()
		p := newProcessor(t)
		w := newWallet(t, "100.00")
		other := newWallet(t, "100.00")
		foreign := mustSubmit(t, p, other, command(t, Bet, "30.00"), nil, baseTime)

		err := Reconcile(w, ledgerOf(t, other, foreign))
		if !failure.Is(err, failure.ReferenceMismatch) {
			t.Errorf("Reconcile = %v, want %v", err, failure.ReferenceMismatch)
		}
	})

	t.Run("a missing wallet", func(t *testing.T) {
		t.Parallel()
		if err := Reconcile(nil, nil); !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("Reconcile(nil) = %v, want %v", err, failure.UninitializedValue)
		}
	})
}
