package wagering

import (
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

var testPolicy = ReferencePolicy{MaxAttempts: 3, TTL: time.Minute}

// txInStatus builds an external transaction already moved to the given status.
func txInStatus(t *testing.T, status Status) *WagerTransaction {
	t.Helper()
	tx, err := NewExternalTransaction(command(t, Bet, "10.00"), NewWalletID(), baseTime)
	if err != nil {
		t.Fatalf("NewExternalTransaction: %v", err)
	}
	switch status {
	case Pending:
	case PendingReference:
		err = tx.MarkPendingReference(testPolicy, baseTime)
	case Processed:
		err = tx.MarkProcessed(brl(t, "90.00"), baseTime)
	case Rejected:
		err = tx.Reject(failure.InsufficientFunds, baseTime)
	case Failed:
		err = tx.Fail(baseTime)
	default:
		t.Fatalf("unknown status %s", status)
	}
	if err != nil {
		t.Fatalf("moving to %s: %v", status, err)
	}
	return tx
}

// budgetSpent reads the wait budget, failing the test if the policy is refused.
// Whether a meaningless policy is refused is asserted separately, by
// TestZeroValueProcessorRefusesInsteadOfSettling.
func budgetSpent(t *testing.T, tx *WagerTransaction, now time.Time, policy ReferencePolicy) bool {
	t.Helper()
	spent, err := tx.ReferenceBudgetExhausted(policy, now)
	if err != nil {
		t.Fatalf("ReferenceBudgetExhausted(%v): %v", policy, err)
	}
	return spent
}

// TestStateMachine is the documented transition table, asserted directly.
func TestStateMachine(t *testing.T) {
	t.Parallel()

	allowed := map[Status]map[Status]bool{
		Pending:          {Processed: true, PendingReference: true, Rejected: true, Failed: true},
		PendingReference: {Processed: true, PendingReference: true, Rejected: true, Failed: true},
		Processed:        {},
		Rejected:         {},
		Failed:           {},
	}
	for _, from := range Statuses() {
		for _, to := range Statuses() {
			want := allowed[from][to]
			if got := from.CanTransitionTo(to); got != want {
				t.Errorf("%s -> %s = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestTerminalStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range Statuses() {
		want := status == Processed || status == Rejected || status == Failed
		if got := status.IsTerminal(); got != want {
			t.Errorf("%s.IsTerminal() = %v, want %v", status, got, want)
		}
	}
}

// TestTerminalTransactionsNeverMove covers the brief's rule that a settled
// transaction stays settled, and that refusing is an error rather than a panic.
func TestTerminalTransactionsNeverMove(t *testing.T) {
	t.Parallel()

	for _, status := range []Status{Processed, Rejected, Failed} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			moves := map[string]func(*WagerTransaction) error{
				"MarkProcessed": func(tx *WagerTransaction) error {
					return tx.MarkProcessed(brl(t, "1.00"), baseTime)
				},
				"MarkPendingReference": func(tx *WagerTransaction) error {
					return tx.MarkPendingReference(testPolicy, baseTime)
				},
				"Reject": func(tx *WagerTransaction) error {
					return tx.Reject(failure.InsufficientFunds, baseTime)
				},
				"Fail": func(tx *WagerTransaction) error { return tx.Fail(baseTime) },
			}
			for name, move := range moves {
				tx := txInStatus(t, status)
				err := move(tx)
				if !failure.Is(err, failure.InvalidStateTransition) {
					t.Errorf("%s on a %s transaction = %v, want %v",
						name, status, err, failure.InvalidStateTransition)
				}
				if tx.Status() != status {
					t.Errorf("%s moved a %s transaction to %s", name, status, tx.Status())
				}
			}
		})
	}
}

func TestExternalTransactionStartsPending(t *testing.T) {
	t.Parallel()

	cmd := command(t, Bet, "25.00")
	walletID := NewWalletID()
	tx, err := NewExternalTransaction(cmd, walletID, baseTime)
	if err != nil {
		t.Fatalf("NewExternalTransaction: %v", err)
	}

	if tx.Status() != Pending {
		t.Errorf("status = %s, want %s", tx.Status(), Pending)
	}
	if !tx.IsExternal() {
		t.Error("an external submission reports as internal")
	}
	for name, present := range map[string]bool{
		"provider":    hasProvider(tx),
		"externalId":  hasExternalID(tx),
		"key":         hasKey(tx),
		"payloadHash": hasHash(tx),
		"round":       hasRound(tx),
		"game":        hasGame(tx),
	} {
		if !present {
			t.Errorf("an external submission carries no %s", name)
		}
	}
	if tx.WalletID() != walletID {
		t.Error("the transaction names a different wallet")
	}
	if _, ok := tx.Result(); ok {
		t.Error("a pending transaction already carries a result")
	}
	if _, ok := tx.FailureCode(); ok {
		t.Error("a pending transaction already carries a failure code")
	}
}

func hasProvider(tx *WagerTransaction) bool   { _, ok := tx.Provider(); return ok }
func hasExternalID(tx *WagerTransaction) bool { _, ok := tx.ExternalTransactionID(); return ok }
func hasKey(tx *WagerTransaction) bool        { _, ok := tx.IdempotencyKey(); return ok }
func hasHash(tx *WagerTransaction) bool       { _, ok := tx.PayloadHash(); return ok }
func hasRound(tx *WagerTransaction) bool      { _, ok := tx.RoundID(); return ok }
func hasGame(tx *WagerTransaction) bool       { _, ok := tx.GameID(); return ok }

// TestOpeningIsRefusedFromAProvider covers the rule that an opening belongs to
// wallet creation. Its origin follows from its kind, so a provider submitting
// one is making an impossible request rather than a rejected one — nothing is
// recorded and the idempotency key stays free.
func TestOpeningIsRefusedFromAProvider(t *testing.T) {
	t.Parallel()

	cmd := command(t, Bet, "25.00")
	cmd.Kind = Opening

	tx, err := NewExternalTransaction(cmd, NewWalletID(), baseTime)
	if !failure.Is(err, failure.UnsupportedTransactionKind) {
		t.Fatalf("NewExternalTransaction with an opening = %v, %v, want %v",
			tx, err, failure.UnsupportedTransactionKind)
	}
	if !failure.Correctable(err) {
		t.Error("an externally submitted opening is definitive; nothing was recorded, so it must be correctable")
	}
	if tx != nil {
		t.Error("a refused opening produced a transaction")
	}
}

func TestMarkProcessedRecordsTheReportedBalance(t *testing.T) {
	t.Parallel()

	tx := txInStatus(t, Pending)
	if err := tx.MarkProcessed(brl(t, "75.00"), baseTime.Add(time.Second)); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	result, ok := tx.Result()
	if !ok {
		t.Fatal("a processed transaction carries no result")
	}
	if got := result.Amount(); got != "75.00" {
		t.Errorf("result = %s, want 75.00", got)
	}
	if !tx.UpdatedAt().Equal(baseTime.Add(time.Second)) {
		t.Error("the transition did not advance updatedAt")
	}
	if !tx.CreatedAt().Equal(baseTime) {
		t.Error("the transition changed createdAt")
	}
}

// TestMarkProcessedRefusesAResultInAnotherCurrency covers the gap between the
// two amounts a processed transaction carries. What it moved and what it
// reported back are separate values, and nothing tied them together: a
// transaction could record that it moved R$10,00 and left the wallet holding
// US$90.00 — a balance no wallet it was allowed to touch has ever held.
//
// The processor only ever passes the wallet's own balance, so nothing produces
// this today. It is the type declining to defend an invariant that only a
// caller's discipline was holding up, which is the one thing a value-carrying
// aggregate exists to make impossible.
//
// The zero-amount kind is covered too: the currency of a loss lives in its
// amount like any other, so a guard that only looked at money that moved would
// leave losses undefended.
func TestMarkProcessedRefusesAResultInAnotherCurrency(t *testing.T) {
	t.Parallel()

	for _, kind := range []Kind{Bet, Loss} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()

			amount := "10.00"
			if kind.RequiresZeroAmount() {
				amount = "0.00"
			}
			tx, err := NewExternalTransaction(command(t, kind, amount), NewWalletID(), baseTime)
			if err != nil {
				t.Fatalf("NewExternalTransaction(%s): %v", kind, err)
			}

			settled := baseTime.Add(time.Second)
			err = tx.MarkProcessed(usd(t, "90.00"), settled)
			if !failure.Is(err, failure.CurrencyMismatch) {
				t.Fatalf("a USD balance on a %s transaction = %v, want %v",
					tx.Currency(), err, failure.CurrencyMismatch)
			}

			// Refusing has to happen before the transition, or the argument is
			// rejected and the status settles anyway.
			if got := tx.Status(); got != Pending {
				t.Errorf("status = %s after a refused MarkProcessed, want %s", got, Pending)
			}
			if _, ok := tx.Result(); ok {
				t.Error("a refused MarkProcessed recorded a result")
			}
			if !tx.UpdatedAt().Equal(baseTime) {
				t.Error("a refused MarkProcessed advanced updatedAt")
			}

			// The control, so the guard is not simply refusing everything.
			if err := tx.MarkProcessed(brl(t, "90.00"), settled); err != nil {
				t.Fatalf("a BRL balance on a BRL transaction: %v", err)
			}
		})
	}
}

// TestMarkProcessedRefusesABalanceNoWalletCouldHold covers the other half of
// the result contract. A wallet never holds less than nothing — Wallet.move and
// NewWalletLedgerEntry both refuse to leave it there — so a transaction
// reporting a negative balance back to the provider describes a wallet state
// the rest of the model has made unreachable.
//
// The code matches the one the ledger already uses for the same condition,
// because a caller that maps failures to responses should not have to learn two
// names for "a balance went below zero".
func TestMarkProcessedRefusesABalanceNoWalletCouldHold(t *testing.T) {
	t.Parallel()

	tx := txInStatus(t, Pending)
	settled := baseTime.Add(time.Second)

	if err := tx.MarkProcessed(negative(t, "5.00"), settled); !failure.Is(err, failure.InsufficientFunds) {
		t.Fatalf("a negative resulting balance = %v, want %v", err, failure.InsufficientFunds)
	}
	if got := tx.Status(); got != Pending {
		t.Errorf("status = %s after a refused MarkProcessed, want %s", got, Pending)
	}
	if _, ok := tx.Result(); ok {
		t.Error("a refused MarkProcessed recorded a result")
	}

	// Zero is a balance a wallet really can hold, so the guard must stop at
	// negative rather than at "not positive".
	if err := tx.MarkProcessed(brl(t, "0.00"), settled); err != nil {
		t.Fatalf("a zero resulting balance: %v", err)
	}
}

func TestReferenceWaitBudget(t *testing.T) {
	t.Parallel()

	t.Run("counts attempts and sets the deadline once", func(t *testing.T) {
		t.Parallel()
		tx := txInStatus(t, Pending)

		if err := tx.MarkPendingReference(testPolicy, baseTime); err != nil {
			t.Fatalf("first wait: %v", err)
		}
		deadline, ok := tx.ReferenceDeadline()
		if !ok {
			t.Fatal("the first wait set no deadline")
		}
		if !deadline.Equal(baseTime.Add(testPolicy.TTL)) {
			t.Errorf("deadline = %s, want %s", deadline, baseTime.Add(testPolicy.TTL))
		}
		if tx.ReferenceAttempts() != 1 {
			t.Errorf("attempts = %d, want 1", tx.ReferenceAttempts())
		}

		if err := tx.MarkPendingReference(testPolicy, baseTime.Add(time.Second)); err != nil {
			t.Fatalf("second wait: %v", err)
		}
		if tx.ReferenceAttempts() != 2 {
			t.Errorf("attempts = %d, want 2", tx.ReferenceAttempts())
		}
		again, _ := tx.ReferenceDeadline()
		if !again.Equal(deadline) {
			t.Error("a later wait moved the deadline")
		}
	})

	t.Run("is spent once the attempts run out", func(t *testing.T) {
		t.Parallel()
		tx := txInStatus(t, Pending)
		for i := range testPolicy.MaxAttempts {
			if err := tx.MarkPendingReference(testPolicy, baseTime); err != nil {
				t.Fatalf("wait %d: %v", i+1, err)
			}
		}
		if !budgetSpent(t, tx, baseTime, testPolicy) {
			t.Fatal("the budget is not spent after the last allowed attempt")
		}
		if err := tx.MarkPendingReference(testPolicy, baseTime); !failure.Is(err, failure.InvalidStateTransition) {
			t.Errorf("waiting past the budget = %v, want %v", err, failure.InvalidStateTransition)
		}
	})

	t.Run("is spent once the deadline passes", func(t *testing.T) {
		t.Parallel()
		tx := txInStatus(t, Pending)
		if err := tx.MarkPendingReference(testPolicy, baseTime); err != nil {
			t.Fatalf("first wait: %v", err)
		}
		if budgetSpent(t, tx, baseTime.Add(testPolicy.TTL-time.Nanosecond), testPolicy) {
			t.Error("the budget is spent before the deadline")
		}
		if !budgetSpent(t, tx, baseTime.Add(testPolicy.TTL), testPolicy) {
			t.Error("the budget is not spent at the deadline")
		}
	})
}

func TestRejectRequiresAKnownCode(t *testing.T) {
	t.Parallel()

	tx := txInStatus(t, Pending)
	if err := tx.Reject("MADE_UP", baseTime); !failure.Is(err, failure.InvalidFieldFormat) {
		t.Errorf("Reject with an undeclared code = %v, want %v", err, failure.InvalidFieldFormat)
	}
	if tx.Status() != Pending {
		t.Errorf("a refused rejection moved the transaction to %s", tx.Status())
	}
}

// TestRejectRefusesACorrectableCode pins the rule ADR-0002 rests on: a rejected
// transaction is persisted, published and binds its idempotency key for good,
// so only a code that settles the operation may produce one. A correctable code
// means nothing was persisted and the provider may repair the payload and
// resubmit under the same key — recording one here would bind the key to a
// payload the provider is still entitled to correct, and answer every later
// retry with IDEMPOTENCY_PAYLOAD_CONFLICT.
//
// Regression: Reject checked only Known(), so every correctable code settled a
// transaction and emitted WagerTransactionRejected under it.
func TestRejectRefusesACorrectableCode(t *testing.T) {
	t.Parallel()

	correctable := make([]failure.Code, 0, len(failure.All()))
	for _, code := range failure.All() {
		if code.Correctable() {
			correctable = append(correctable, code)
		}
	}
	if len(correctable) == 0 {
		t.Fatal("the catalogue declares no correctable codes, so this rule cannot be tested")
	}

	for _, code := range correctable {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			tx := txInStatus(t, Pending)

			if err := tx.Reject(code, baseTime); !failure.Is(err, failure.InvalidFieldFormat) {
				t.Fatalf("Reject(%s) = %v, want %v", code, err, failure.InvalidFieldFormat)
			}
			if tx.Status() != Pending {
				t.Errorf("a refused rejection moved the transaction to %s", tx.Status())
			}
			if got, ok := tx.FailureCode(); ok {
				t.Errorf("a refused rejection recorded the code %s", got)
			}
			if _, err := NewWagerTransactionRejected(tx); err == nil {
				t.Error("a refused rejection still produced a WagerTransactionRejected event")
			}
		})
	}

	// An audit code is definitive, so the rule above lets it through, but it
	// reports corruption found in stored state rather than the outcome of a
	// submission: there was no operation for it to settle, and a transaction
	// rejected under one would name a reason its provider never caused.
	t.Run("an audit code is refused as well", func(t *testing.T) {
		t.Parallel()

		var audited int
		for _, code := range failure.All() {
			if !code.Audit() {
				continue
			}
			audited++
			if !code.Definitive() {
				t.Errorf("%s is an audit code but not definitive, so this rule adds nothing", code)
			}
			tx := txInStatus(t, Pending)
			if err := tx.Reject(code, baseTime); !failure.Is(err, failure.InvalidFieldFormat) {
				t.Errorf("Reject(%s) = %v, want %v", code, err, failure.InvalidFieldFormat)
			}
			if tx.Status() != Pending {
				t.Errorf("a refused rejection moved the transaction to %s", tx.Status())
			}
			if got, ok := tx.FailureCode(); ok {
				t.Errorf("a refused rejection recorded the code %s", got)
			}
		}
		if audited == 0 {
			t.Skip("the catalogue declares no audit codes")
		}
	})

	t.Run("every code that settles an operation is still accepted", func(t *testing.T) {
		t.Parallel()
		for _, code := range failure.All() {
			if !code.Definitive() || code.Audit() {
				continue
			}
			tx := txInStatus(t, Pending)
			if err := tx.Reject(code, baseTime); err != nil {
				t.Errorf("Reject(%s) = %v, want nil", code, err)
				continue
			}
			if got, ok := tx.FailureCode(); !ok || got != code {
				t.Errorf("failure code = %s (present %v), want %s", got, ok, code)
			}
		}
	})
}

// TestResolveReferenceNeverRewritesASettledTransaction pins the append-only
// rule: a transaction that has reached a terminal status never changes again,
// and reversal history is recorded by adding transactions that point at it
// rather than by writing to it. Resolution is also write-once, so a reference
// cannot be silently repointed at a different transaction.
//
// Regression: ResolveReference had no status guard and no already-resolved
// guard, so it rewrote PROCESSED, REJECTED and FAILED transactions in place.
func TestResolveReferenceNeverRewritesASettledTransaction(t *testing.T) {
	t.Parallel()

	// refundInStatus builds a refund — a kind that names a reference — already
	// moved to the given status, with its reference resolved while it still
	// could be.
	refundInStatus := func(t *testing.T, status Status) (*WagerTransaction, TransactionID) {
		t.Helper()
		cmd := command(t, Refund, "10.00", withReference("ext-bet-1"))
		tx, err := NewExternalTransaction(cmd, NewWalletID(), baseTime)
		if err != nil {
			t.Fatalf("NewExternalTransaction: %v", err)
		}
		resolved := NewTransactionID()
		if err := tx.ResolveReference(resolved); err != nil {
			t.Fatalf("resolving while pending: %v", err)
		}
		switch status {
		case Processed:
			err = tx.MarkProcessed(brl(t, "90.00"), baseTime)
		case Rejected:
			err = tx.Reject(failure.ReferenceAlreadyReversed, baseTime)
		case Failed:
			err = tx.Fail(baseTime)
		default:
			t.Fatalf("%s is not terminal", status)
		}
		if err != nil {
			t.Fatalf("moving to %s: %v", status, err)
		}
		return tx, resolved
	}

	for _, status := range Statuses() {
		if !status.IsTerminal() {
			continue
		}
		t.Run("terminal/"+status.String(), func(t *testing.T) {
			t.Parallel()
			tx, resolved := refundInStatus(t, status)

			err := tx.ResolveReference(NewTransactionID())
			if !failure.Is(err, failure.InvalidStateTransition) {
				t.Fatalf("ResolveReference on a %s transaction = %v, want %v",
					status, err, failure.InvalidStateTransition)
			}
			got, ok := tx.ResolvedReferenceID()
			if !ok || got != resolved {
				t.Errorf("the reference moved to %s, want %s unchanged", got, resolved)
			}
			if tx.Status() != status {
				t.Errorf("a refused resolution moved the transaction to %s", tx.Status())
			}
		})
	}

	t.Run("repointing an already resolved reference is refused", func(t *testing.T) {
		t.Parallel()
		cmd := command(t, Refund, "10.00", withReference("ext-bet-1"))
		tx, err := NewExternalTransaction(cmd, NewWalletID(), baseTime)
		if err != nil {
			t.Fatalf("NewExternalTransaction: %v", err)
		}
		first := NewTransactionID()
		if err := tx.ResolveReference(first); err != nil {
			t.Fatalf("first resolution: %v", err)
		}
		if err := tx.ResolveReference(NewTransactionID()); !failure.Is(err, failure.InvalidStateTransition) {
			t.Fatalf("repointing = %v, want %v", err, failure.InvalidStateTransition)
		}
		if got, _ := tx.ResolvedReferenceID(); got != first {
			t.Errorf("the reference moved to %s, want %s unchanged", got, first)
		}
	})

	t.Run("resolving again to the same reference is a no-op", func(t *testing.T) {
		t.Parallel()
		// Continue re-runs the rules against a transaction that already exists,
		// so an operation whose earlier attempt found its reference must be able
		// to resolve to the same one again.
		cmd := command(t, Refund, "10.00", withReference("ext-bet-1"))
		tx, err := NewExternalTransaction(cmd, NewWalletID(), baseTime)
		if err != nil {
			t.Fatalf("NewExternalTransaction: %v", err)
		}
		resolved := NewTransactionID()
		if err := tx.ResolveReference(resolved); err != nil {
			t.Fatalf("first resolution: %v", err)
		}
		if err := tx.ResolveReference(resolved); err != nil {
			t.Fatalf("resolving to the same reference = %v, want nil", err)
		}
		if got, _ := tx.ResolvedReferenceID(); got != resolved {
			t.Errorf("reference = %s, want %s", got, resolved)
		}
	})
}

func TestAssertSamePayload(t *testing.T) {
	t.Parallel()

	cmd := command(t, Bet, "25.00")
	tx, err := NewExternalTransaction(cmd, NewWalletID(), baseTime)
	if err != nil {
		t.Fatalf("NewExternalTransaction: %v", err)
	}
	hash, err := cmd.PayloadHash()
	if err != nil {
		t.Fatalf("PayloadHash: %v", err)
	}

	t.Run("accepts an identical resubmission", func(t *testing.T) {
		t.Parallel()
		if err := tx.AssertSamePayload(hash); err != nil {
			t.Errorf("a repeat of the same payload = %v, want no error", err)
		}
	})

	t.Run("refuses a different payload under the same key", func(t *testing.T) {
		t.Parallel()
		conflicting := cmd
		conflicting.Money = brl(t, "26.00")
		other, err := conflicting.PayloadHash()
		if err != nil {
			t.Fatalf("PayloadHash: %v", err)
		}
		err = tx.AssertSamePayload(other)
		if !failure.Is(err, failure.IdempotencyPayloadConflict) {
			t.Errorf("a conflicting payload = %v, want %v", err, failure.IdempotencyPayloadConflict)
		}
		// The key is already bound to a recorded transaction, so a corrected
		// resubmission under it cannot be accepted.
		if failure.Correctable(err) {
			t.Error("a payload conflict reports as correctable")
		}
	})
}

// snapshotOf reads a transaction back out through its exported accessors, the
// way a storage layer would before writing a row.
func snapshotOf(t *testing.T, tx *WagerTransaction) TransactionSnapshot {
	t.Helper()
	s := TransactionSnapshot{
		ID:                tx.ID(),
		WalletID:          tx.WalletID(),
		PlayerID:          tx.PlayerID(),
		Kind:              tx.Kind(),
		Money:             tx.Money(),
		Status:            tx.Status(),
		CreatedAt:         tx.CreatedAt(),
		UpdatedAt:         tx.UpdatedAt(),
		ReferenceAttempts: tx.ReferenceAttempts(),
	}
	if result, ok := tx.Result(); ok {
		s.Result = &result
	}
	if code, ok := tx.FailureCode(); ok {
		s.FailureCode = code
	}
	if deadline, ok := tx.ReferenceDeadline(); ok {
		s.ReferenceDeadline = deadline
	}
	if tx.IsExternal() {
		provider, _ := tx.Provider()
		externalID, _ := tx.ExternalTransactionID()
		key, _ := tx.IdempotencyKey()
		hash, _ := tx.PayloadHash()
		round, _ := tx.RoundID()
		game, _ := tx.GameID()
		external := &ExternalSnapshot{
			Provider:              provider,
			ExternalTransactionID: externalID,
			IdempotencyKey:        key,
			PayloadHash:           hash,
			RoundID:               round,
			GameID:                game,
		}
		if ref, ok := tx.ReferenceExternalTransactionID(); ok {
			external.ReferenceExternalTransactionID = ref
		}
		if resolved, ok := tx.ResolvedReferenceID(); ok {
			external.ResolvedReferenceID = resolved
		}
		s.External = external
	}
	return s
}

// assertSameTransaction compares two transactions across every exported
// accessor, so a round trip that quietly drops a field is caught.
func assertSameTransaction(t *testing.T, got, want *WagerTransaction) {
	t.Helper()
	gotResult, gotHasResult := got.Result()
	wantResult, wantHasResult := want.Result()
	gotCode, gotHasCode := got.FailureCode()
	wantCode, wantHasCode := want.FailureCode()
	gotRef, gotHasRef := got.ReferenceExternalTransactionID()
	wantRef, wantHasRef := want.ReferenceExternalTransactionID()
	gotResolved, gotHasResolved := got.ResolvedReferenceID()
	wantResolved, wantHasResolved := want.ResolvedReferenceID()

	for _, f := range []struct {
		field    string
		got, exp any
	}{
		{"id", got.ID(), want.ID()},
		{"walletId", got.WalletID(), want.WalletID()},
		{"playerId", got.PlayerID(), want.PlayerID()},
		{"kind", got.Kind(), want.Kind()},
		{"money", got.Money().String(), want.Money().String()},
		{"status", got.Status(), want.Status()},
		{"createdAt", got.CreatedAt(), want.CreatedAt()},
		{"updatedAt", got.UpdatedAt(), want.UpdatedAt()},
		{"isExternal", got.IsExternal(), want.IsExternal()},
		{"result", gotResult.String() + presence(gotHasResult), wantResult.String() + presence(wantHasResult)},
		{"failureCode", string(gotCode) + presence(gotHasCode), string(wantCode) + presence(wantHasCode)},
		{"reference", string(gotRef) + presence(gotHasRef), string(wantRef) + presence(wantHasRef)},
		{"resolvedReference", gotResolved.String() + presence(gotHasResolved), wantResolved.String() + presence(wantHasResolved)},
		{"referenceAttempts", got.ReferenceAttempts(), want.ReferenceAttempts()},
	} {
		if f.got != f.exp {
			t.Errorf("%s = %v, want %v", f.field, f.got, f.exp)
		}
	}
}

// presence renders a presence flag alongside the value it guards, so "absent"
// and "present but zero" cannot compare equal.
func presence(present bool) string {
	if present {
		return " (present)"
	}
	return " (absent)"
}

// TestRehydrateAcceptsExactlyWhatConstructionProduces is the other half of
// TestRehydrateWagerTransaction's corruption table.
//
// The table proves rehydration is not too lax. This proves it is not too strict:
// every state the constructors and the state machine can actually reach must
// survive a round trip through storage unchanged. Tightening rehydration without
// this test would be how a legitimate stored row becomes unloadable.
//
// Regression: rehydration used to validate presence and enum membership only, so
// it accepted a LOSS that moved money, an OPENING left PENDING, a PROCESSED
// transaction with no resulting balance, a REJECTED one with no code or a
// correctable one, an external transaction with every provider field empty, and
// a negative wait count. Each of those is now refused — and each kind of row
// that storage legitimately holds still loads.
func TestRehydrateAcceptsExactlyWhatConstructionProduces(t *testing.T) {
	t.Parallel()

	built := map[string]func(t *testing.T) *WagerTransaction{
		"opening": func(t *testing.T) *WagerTransaction {
			t.Helper()
			_, out, err := OpenWallet(openInput(t, "100.00"), nil, baseTime)
			if err != nil {
				t.Fatalf("OpenWallet: %v", err)
			}
			return out.Transaction
		},
		"pending bet": func(t *testing.T) *WagerTransaction {
			t.Helper()
			return txInStatus(t, Pending)
		},
		"processed bet": func(t *testing.T) *WagerTransaction {
			t.Helper()
			p, w := newProcessor(t), newWallet(t, "100.00")
			return mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime).Transaction
		},
		"processed loss": func(t *testing.T) *WagerTransaction {
			t.Helper()
			p, w := newProcessor(t), newWallet(t, "100.00")
			return mustSubmit(t, p, w, command(t, Loss, "0.00"), nil, baseTime).Transaction
		},
		"processed win": func(t *testing.T) *WagerTransaction {
			t.Helper()
			p, w := newProcessor(t), newWallet(t, "100.00")
			return mustSubmit(t, p, w, command(t, Win, "25.00"), nil, baseTime).Transaction
		},
		"processed refund with a resolved reference": func(t *testing.T) *WagerTransaction {
			t.Helper()
			p, w := newProcessor(t), newWallet(t, "100.00")
			bet := mustSubmit(t, p, w, command(t, Bet, "25.00"), nil, baseTime).Transaction
			cmd := command(t, Refund, "25.00", withReference(externalID(t, bet)))
			return mustSubmit(t, p, w, cmd, referenceTo(bet), baseTime).Transaction
		},
		"rejected bet": func(t *testing.T) *WagerTransaction {
			t.Helper()
			p, w := newProcessor(t), newWallet(t, "10.00")
			out := submitOK(t, p, w, command(t, Bet, "999.00"), nil, baseTime)
			assertStatus(t, out, Rejected)
			return out.Transaction
		},
		"refund parked waiting for its reference": func(t *testing.T) *WagerTransaction {
			t.Helper()
			p, w := newProcessor(t), newWallet(t, "100.00")
			cmd := command(t, Refund, "25.00", withReference("ext-not-here-yet"))
			out := submitOK(t, p, w, cmd, &ReferenceView{}, baseTime)
			assertStatus(t, out, PendingReference)
			return out.Transaction
		},
		"failed transaction": func(t *testing.T) *WagerTransaction {
			t.Helper()
			return txInStatus(t, Failed)
		},
	}

	for name, build := range built {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want := build(t)

			got, err := RehydrateWagerTransaction(snapshotOf(t, want))
			if err != nil {
				t.Fatalf("a state the domain produced could not be rehydrated: %v", err)
			}
			assertSameTransaction(t, got, want)

			// Rehydrating what rehydration produced must also hold, or storage
			// could not read its own writes twice.
			again, err := RehydrateWagerTransaction(snapshotOf(t, got))
			if err != nil {
				t.Fatalf("the second round trip failed: %v", err)
			}
			assertSameTransaction(t, again, want)
		})
	}
}

func TestRehydrateWagerTransaction(t *testing.T) {
	t.Parallel()

	valid := TransactionSnapshot{
		ID:        NewTransactionID(),
		WalletID:  NewWalletID(),
		PlayerID:  testPlayer,
		Kind:      Bet,
		Money:     brl(t, "25.00"),
		Status:    Processed,
		CreatedAt: baseTime,
		UpdatedAt: baseTime.Add(time.Second),
		Result:    new(brl(t, "75.00")),
		External: &ExternalSnapshot{
			Provider:              testProvider,
			ExternalTransactionID: "ext-1",
			IdempotencyKey:        "key-1",
			PayloadHash:           "abc123",
			RoundID:               testRound,
			GameID:                testGame,
		},
	}

	t.Run("restores the stored state without replaying anything", func(t *testing.T) {
		t.Parallel()
		tx, err := RehydrateWagerTransaction(valid)
		if err != nil {
			t.Fatalf("RehydrateWagerTransaction: %v", err)
		}
		// A terminal status is restored as it was stored, not re-entered.
		if tx.Status() != Processed {
			t.Errorf("status = %s, want %s", tx.Status(), Processed)
		}
		if got, ok := tx.Result(); !ok || got.Amount() != "75.00" {
			t.Errorf("result = %v, %v, want 75.00", got, ok)
		}
		if !tx.IsExternal() {
			t.Error("a rehydrated provider submission reports as internal")
		}
	})

	t.Run("refuses corrupt state", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name   string
			mutate func(*TransactionSnapshot)
			want   failure.Code
		}{
			{"no id", func(s *TransactionSnapshot) { s.ID = TransactionID{} }, failure.MissingRequiredField},
			{"no wallet", func(s *TransactionSnapshot) { s.WalletID = WalletID{} }, failure.MissingRequiredField},
			{"no player", func(s *TransactionSnapshot) { s.PlayerID = "" }, failure.MissingRequiredField},
			{"unknown kind", func(s *TransactionSnapshot) { s.Kind = "TRANSFER" }, failure.InvalidFieldFormat},
			{"unknown status", func(s *TransactionSnapshot) { s.Status = "SETTLING" }, failure.InvalidFieldFormat},
			{"uninitialised money", func(s *TransactionSnapshot) { s.Money = money.Money{} }, failure.UninitializedValue},
			{"no createdAt", func(s *TransactionSnapshot) { s.CreatedAt = time.Time{} }, failure.MissingRequiredField},
			{
				"updatedAt before createdAt",
				func(s *TransactionSnapshot) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) },
				failure.InvalidFieldFormat,
			},
			{
				"unknown failure code",
				func(s *TransactionSnapshot) { s.FailureCode = "MADE_UP" },
				failure.InvalidFieldFormat,
			},
			{
				// An opening has no provider side, so storage claiming one is
				// corruption rather than an unusual case.
				"opening carrying provider fields",
				func(s *TransactionSnapshot) { s.Kind = Opening },
				failure.InvalidFieldFormat,
			},
			{
				"external kind missing provider fields",
				func(s *TransactionSnapshot) { s.External = nil },
				failure.MissingRequiredField,
			},

			// Per-kind amount policy. Construction refuses both of these, so
			// storage holding one means the row did not come from this model.
			{
				"a loss that moved money",
				func(s *TransactionSnapshot) { s.Kind = Loss },
				failure.InvalidAmountForKind,
			},
			{
				"a bet of nothing",
				func(s *TransactionSnapshot) { s.Money = brl(t, "0.00") },
				failure.InvalidAmountForKind,
			},
			{
				"a negative amount",
				func(s *TransactionSnapshot) { s.Money = negative(t, "25.00") },
				failure.InvalidAmountForKind,
			},

			// An opening is born processed and never moves.
			{
				"an opening left pending",
				func(s *TransactionSnapshot) {
					s.Kind, s.External, s.Status, s.Result = Opening, nil, Pending, nil
				},
				failure.InvalidFieldFormat,
			},

			// Status must agree with what settling produced.
			{
				"processed without the balance it reported",
				func(s *TransactionSnapshot) { s.Result = nil },
				failure.MissingRequiredField,
			},
			{
				"rejected without a failure code",
				func(s *TransactionSnapshot) { s.Status, s.Result = Rejected, nil },
				failure.MissingRequiredField,
			},
			{
				// The back door onto WagerTransaction.Reject, which refuses a
				// correctable code: a stored rejection under one describes a
				// transaction that should never have been persisted at all.
				"rejected under a correctable code",
				func(s *TransactionSnapshot) {
					s.Status, s.Result, s.FailureCode = Rejected, nil, failure.InvalidAmountFormat
				},
				failure.InvalidFieldFormat,
			},
			{
				// The same back door, on the other half of the rule. An audit
				// code is definitive, so the correctable check above lets it
				// through; it still describes corruption found in stored state
				// rather than anything a provider submitted.
				"rejected under an audit code",
				func(s *TransactionSnapshot) {
					s.Status, s.Result, s.FailureCode = Rejected, nil, failure.LedgerBalanceMismatch
				},
				failure.InvalidFieldFormat,
			},
			{
				"unsettled but carrying a balance",
				func(s *TransactionSnapshot) { s.Status = Pending },
				failure.InvalidFieldFormat,
			},
			{
				// The back door onto MarkProcessed's result rules. A transaction
				// acts on one wallet, and a wallet holds one currency.
				"a balance reported in another currency",
				func(s *TransactionSnapshot) {
					s.Result = new(usd(t, "90.00"))
				},
				failure.CurrencyMismatch,
			},
			{
				"a balance below what a wallet can hold",
				func(s *TransactionSnapshot) {
					s.Result = new(negative(t, "5.00"))
				},
				failure.InsufficientFunds,
			},
			{
				"processed but carrying a failure code",
				func(s *TransactionSnapshot) { s.FailureCode = failure.InsufficientFunds },
				failure.InvalidFieldFormat,
			},

			// Provider-owned fields are validated for shape, as a submission's are.
			{
				"no provider",
				func(s *TransactionSnapshot) { s.External.Provider = "" },
				failure.MissingRequiredField,
			},
			{
				"no idempotency key",
				func(s *TransactionSnapshot) { s.External.IdempotencyKey = "" },
				failure.MissingRequiredField,
			},
			{
				"no payload hash",
				func(s *TransactionSnapshot) { s.External.PayloadHash = "" },
				failure.MissingRequiredField,
			},
			{
				"no round",
				func(s *TransactionSnapshot) { s.External.RoundID = "" },
				failure.MissingRequiredField,
			},

			// Reference fields must match what the kind allows.
			{
				"a reversal naming no reference",
				func(s *TransactionSnapshot) { s.Kind = Refund },
				failure.ReferenceRequired,
			},
			{
				"a bet naming a reference",
				func(s *TransactionSnapshot) { s.External.ReferenceExternalTransactionID = "ext-2" },
				failure.ReferenceNotApplicable,
			},
			{
				"a reference resolved but never named",
				func(s *TransactionSnapshot) { s.External.ResolvedReferenceID = NewTransactionID() },
				failure.InvalidFieldFormat,
			},
			{
				"an operation referencing itself",
				func(s *TransactionSnapshot) {
					s.Kind = Refund
					s.External.ReferenceExternalTransactionID = s.External.ExternalTransactionID
				},
				failure.InvalidFieldFormat,
			},

			// The record of waiting must be one waiting could have left behind.
			{
				"a negative wait count",
				func(s *TransactionSnapshot) { s.ReferenceAttempts = -5 },
				failure.InvalidFieldFormat,
			},
			{
				"waits counted with no deadline",
				func(s *TransactionSnapshot) { s.ReferenceAttempts = 2 },
				failure.InvalidFieldFormat,
			},
			{
				"a deadline with no waits counted",
				func(s *TransactionSnapshot) { s.ReferenceDeadline = baseTime.Add(time.Minute) },
				failure.InvalidFieldFormat,
			},
			{
				"parked for a reference without ever having waited",
				func(s *TransactionSnapshot) {
					s.Kind, s.Status, s.Result = Refund, PendingReference, nil
					s.External.ReferenceExternalTransactionID = "ext-bet-1"
				},
				failure.InvalidFieldFormat,
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				// A shallow copy would share External and Result with every other
				// case, so parallel subtests mutating them would corrupt each
				// other's fixture.
				snapshot := valid
				if valid.External != nil {
					snapshot.External = new(*valid.External)
				}
				if valid.Result != nil {
					snapshot.Result = new(*valid.Result)
				}
				tc.mutate(&snapshot)
				tx, err := RehydrateWagerTransaction(snapshot)
				if err == nil {
					t.Fatalf("RehydrateWagerTransaction = %v, want an error", tx)
				}
				if !failure.Is(err, tc.want) {
					t.Errorf("RehydrateWagerTransaction = %v, want code %v", err, tc.want)
				}
			})
		}
	})
}
