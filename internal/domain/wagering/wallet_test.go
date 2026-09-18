package wagering

import (
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

func TestOpenWalletWithABalance(t *testing.T) {
	t.Parallel()

	in := openInput(t, "100.00")
	w, out, err := OpenWallet(in, nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}

	assertBalance(t, w, "100.00")
	// The opening is part of creating the wallet, not a change to one that
	// already existed, so the version it lands on is 1.
	assertVersion(t, w, 1)
	if w.Currency() != currency(t, "BRL") {
		t.Errorf("currency = %s, want BRL", w.Currency())
	}

	if out.Transaction == nil {
		t.Fatal("opening with a balance produced no transaction")
	}
	if got := out.Transaction.Kind(); got != Opening {
		t.Errorf("kind = %s, want %s", got, Opening)
	}
	if got := out.Transaction.Status(); got != Processed {
		t.Errorf("status = %s, want %s", got, Processed)
	}
	if !out.Transaction.IsInternal() {
		t.Error("an opening reports as external")
	}
	if _, ok := out.Transaction.Provider(); ok {
		t.Error("an opening carries a provider")
	}
	if _, ok := out.Transaction.ExternalTransactionID(); ok {
		t.Error("an opening carries an external transaction id")
	}
	if _, ok := out.Transaction.PayloadHash(); ok {
		t.Error("an opening carries a payload hash")
	}
	if _, ok := out.Transaction.RoundID(); ok {
		t.Error("an opening carries a round")
	}
	if _, ok := out.Transaction.GameID(); ok {
		t.Error("an opening carries a game")
	}
	if result, ok := out.Transaction.Result(); !ok || result.Amount() != "100.00" {
		t.Errorf("result = %v, %v, want 100.00 BRL", result, ok)
	}

	if out.LedgerEntry == nil {
		t.Fatal("opening with a balance produced no ledger entry")
	}
	entry := *out.LedgerEntry
	if entry.Direction() != Credit {
		t.Errorf("direction = %s, want %s", entry.Direction(), Credit)
	}
	if got := entry.BalanceBefore().Amount(); got != "0.00" {
		t.Errorf("balanceBefore = %s, want 0.00", got)
	}
	if got := entry.BalanceAfter().Amount(); got != "100.00" {
		t.Errorf("balanceAfter = %s, want 100.00", got)
	}
	if entry.WalletVersion() != 1 {
		t.Errorf("entry walletVersion = %d, want 1", entry.WalletVersion())
	}
	if entry.TransactionID() != in.TransactionID {
		t.Error("the entry names a different transaction from the opening")
	}

	assertEventTypes(t, out, typeWagerTransactionProcessed, typeWalletBalanceChanged)
}

func TestOpenWalletAtZero(t *testing.T) {
	t.Parallel()

	w, out, err := OpenWallet(openInput(t, "0.00"), nil, baseTime)
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}

	assertBalance(t, w, "0.00")
	assertVersion(t, w, 1)
	// A zero balance still carries the currency, which is why the wallet needs
	// no separate currency field.
	if w.Currency() != currency(t, "BRL") {
		t.Errorf("currency = %s, want BRL", w.Currency())
	}

	// An opening records a starting balance, and a wallet opened at zero has
	// none — so this is the one successful outcome in the domain that carries no
	// transaction, and Recorded is how a caller is meant to find that out.
	if out.Recorded() {
		t.Error("opening at zero reported a recorded outcome")
	}
	if out.Transaction != nil {
		t.Error("opening at zero produced an opening transaction")
	}
	if out.LedgerEntry != nil {
		t.Error("opening at zero produced a ledger entry")
	}
	if len(out.Events) != 0 {
		t.Errorf("opening at zero produced events %v", eventTypes(out.Events))
	}
}

func TestOpenWalletRefuses(t *testing.T) {
	t.Parallel()

	valid := openInput(t, "100.00")

	tests := []struct {
		name  string
		input func() OpenWalletInput
		now   time.Time
		want  failure.Code
	}{
		{
			name: "no wallet id",
			input: func() OpenWalletInput {
				in := valid
				in.WalletID = WalletID{}
				return in
			},
			want: failure.MissingRequiredField,
		},
		{
			name: "no player",
			input: func() OpenWalletInput {
				in := valid
				in.PlayerID = ""
				return in
			},
			want: failure.MissingRequiredField,
		},
		{
			name: "player with surrounding whitespace",
			input: func() OpenWalletInput {
				in := valid
				in.PlayerID = " player-1 "
				return in
			},
			want: failure.InvalidFieldFormat,
		},
		{
			name: "uninitialised balance",
			input: func() OpenWalletInput {
				in := valid
				in.InitialBalance = money.Money{}
				return in
			},
			want: failure.UninitializedValue,
		},
		{
			name: "negative balance",
			input: func() OpenWalletInput {
				in := valid
				negative, err := money.FromMinorUnits(-1, currency(t, "BRL"))
				if err != nil {
					t.Fatalf("FromMinorUnits: %v", err)
				}
				in.InitialBalance = negative
				return in
			},
			want: failure.InvalidAmountForKind,
		},
		{
			name: "balance but no opening transaction",
			input: func() OpenWalletInput {
				in := valid
				in.TransactionID = TransactionID{}
				return in
			},
			want: failure.MissingRequiredField,
		},
		{
			name: "balance but no ledger entry",
			input: func() OpenWalletInput {
				in := valid
				in.LedgerEntryID = LedgerEntryID{}
				return in
			},
			want: failure.MissingRequiredField,
		},
		{
			// Nothing is opened, so identifiers for an opening describe a
			// transaction that will never exist.
			name: "zero balance with an opening transaction",
			input: func() OpenWalletInput {
				in := valid
				in.InitialBalance = brl(t, "0.00")
				return in
			},
			want: failure.ReferenceNotApplicable,
		},
		{
			name:  "no clock",
			input: func() OpenWalletInput { return valid },
			now:   time.Time{},
			want:  failure.MissingRequiredField,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := baseTime
			if tc.name == "no clock" {
				now = tc.now
			}
			w, out, err := OpenWallet(tc.input(), nil, now)
			if err == nil {
				t.Fatalf("OpenWallet = %v, %v, want an error", w, out)
			}
			if !failure.Is(err, tc.want) {
				t.Errorf("OpenWallet = %v, want code %v", err, tc.want)
			}
			if w != nil {
				t.Error("a refused opening returned a wallet")
			}
		})
	}
}

func TestDebitAndCredit(t *testing.T) {
	t.Parallel()

	t.Run("credit raises the balance and the version", func(t *testing.T) {
		t.Parallel()
		w := newWallet(t, "100.00")
		entry, err := w.Credit(Movement{EntryID: NewLedgerEntryID(), TransactionID: NewTransactionID(), Amount: brl(t, "25.00"), At: baseTime})
		if err != nil {
			t.Fatalf("Credit: %v", err)
		}
		assertBalance(t, w, "125.00")
		assertVersion(t, w, 2)
		if entry.WalletVersion() != 2 {
			t.Errorf("entry walletVersion = %d, want 2", entry.WalletVersion())
		}
		if entry.BalanceBefore().Amount() != "100.00" || entry.BalanceAfter().Amount() != "125.00" {
			t.Errorf("entry spans %s to %s, want 100.00 to 125.00",
				entry.BalanceBefore().Amount(), entry.BalanceAfter().Amount())
		}
	})

	t.Run("debit lowers the balance and raises the version", func(t *testing.T) {
		t.Parallel()
		w := newWallet(t, "100.00")
		entry, err := w.Debit(Movement{EntryID: NewLedgerEntryID(), TransactionID: NewTransactionID(), Amount: brl(t, "25.00"), At: baseTime})
		if err != nil {
			t.Fatalf("Debit: %v", err)
		}
		assertBalance(t, w, "75.00")
		assertVersion(t, w, 2)
		if entry.Direction() != Debit {
			t.Errorf("direction = %s, want %s", entry.Direction(), Debit)
		}
	})

	t.Run("a debit to exactly zero is allowed", func(t *testing.T) {
		t.Parallel()
		w := newWallet(t, "100.00")
		if _, err := w.Debit(Movement{EntryID: NewLedgerEntryID(), TransactionID: NewTransactionID(), Amount: brl(t, "100.00"), At: baseTime}); err != nil {
			t.Fatalf("Debit: %v", err)
		}
		assertBalance(t, w, "0.00")
	})
}

// TestBalanceNeverGoesNegative covers the wallet's central invariant, and that
// a refused movement leaves the aggregate exactly as it was.
func TestBalanceNeverGoesNegative(t *testing.T) {
	t.Parallel()

	w := newWallet(t, "100.00")
	_, err := w.Debit(Movement{EntryID: NewLedgerEntryID(), TransactionID: NewTransactionID(), Amount: brl(t, "100.01"), At: baseTime})
	if !failure.Is(err, failure.InsufficientFunds) {
		t.Fatalf("Debit beyond the balance = %v, want %v", err, failure.InsufficientFunds)
	}
	assertBalance(t, w, "100.00")
	assertVersion(t, w, 1)
}

func TestMovementRefuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entryID LedgerEntryID
		txID    TransactionID
		amount  money.Money
		now     time.Time
		want    failure.Code
	}{
		{"no entry id", LedgerEntryID{}, NewTransactionID(), brl(t, "1.00"), baseTime, failure.MissingRequiredField},
		{"no transaction id", NewLedgerEntryID(), TransactionID{}, brl(t, "1.00"), baseTime, failure.MissingRequiredField},
		{"uninitialised amount", NewLedgerEntryID(), NewTransactionID(), money.Money{}, baseTime, failure.UninitializedValue},
		{"zero amount", NewLedgerEntryID(), NewTransactionID(), brl(t, "0.00"), baseTime, failure.InvalidAmountForKind},
		{"foreign currency", NewLedgerEntryID(), NewTransactionID(), usd(t, "1.00"), baseTime, failure.CurrencyMismatch},
		{"no clock", NewLedgerEntryID(), NewTransactionID(), brl(t, "1.00"), time.Time{}, failure.MissingRequiredField},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := newWallet(t, "100.00")
			_, err := w.Credit(Movement{EntryID: tc.entryID, TransactionID: tc.txID, Amount: tc.amount, At: tc.now})
			if !failure.Is(err, tc.want) {
				t.Errorf("Credit = %v, want code %v", err, tc.want)
			}
			assertBalance(t, w, "100.00")
			assertVersion(t, w, 1)
		})
	}
}

// TestOpenWalletRefusesADuplicate covers the pair that makes a wallet unique.
// The domain cannot look a wallet up, so whatever exists for the key arrives as
// a value — the same way a reversal is handed its reference.
func TestOpenWalletRefusesADuplicate(t *testing.T) {
	t.Parallel()

	t.Run("a wallet already exists for the player and currency", func(t *testing.T) {
		t.Parallel()
		existing := newWallet(t, "250.00")

		w, out, err := OpenWallet(openInput(t, "100.00"), existing, baseTime)
		if !failure.Is(err, failure.WalletAlreadyExists) {
			t.Fatalf("OpenWallet = %v, %v, %v, want %v", w, out, err, failure.WalletAlreadyExists)
		}
		// There is no key to rebind and no payload to correct, so the conflict is
		// settled rather than something the caller can fix and retry.
		if failure.Correctable(err) {
			t.Error("a duplicate wallet reports as correctable")
		}
		if w != nil {
			t.Error("a refused opening returned a wallet")
		}
		// The wallet that already existed is untouched.
		assertBalance(t, existing, "250.00")
		assertVersion(t, existing, 1)
	})

	t.Run("nothing exists yet", func(t *testing.T) {
		t.Parallel()
		if _, _, err := OpenWallet(openInput(t, "100.00"), nil, baseTime); err != nil {
			t.Errorf("OpenWallet with no existing wallet = %v, want no error", err)
		}
	})

	// A player may hold one wallet per currency, so the same player in another
	// currency is a different wallet, not a conflict.
	t.Run("the same player in another currency is not a conflict", func(t *testing.T) {
		t.Parallel()
		existing := newWallet(t, "250.00")

		in := OpenWalletInput{
			WalletID:       NewWalletID(),
			PlayerID:       existing.PlayerID(),
			InitialBalance: usd(t, "0.00"),
		}
		w, _, err := OpenWallet(in, nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet in a second currency: %v", err)
		}
		if w.Currency() != currency(t, "USD") {
			t.Errorf("currency = %s, want USD", w.Currency())
		}
		if w.Key() == existing.Key() {
			t.Error("two wallets in different currencies share a key")
		}
	})

	// Handing over a wallet under some other key means the lookup and the
	// request disagree, which is a caller defect rather than a conflict.
	t.Run("an unrelated wallet is a caller defect", func(t *testing.T) {
		t.Parallel()
		unrelated, _, err := OpenWallet(OpenWalletInput{
			WalletID:       NewWalletID(),
			PlayerID:       "someone-else",
			InitialBalance: brl(t, "0.00"),
		}, nil, baseTime)
		if err != nil {
			t.Fatalf("OpenWallet: %v", err)
		}

		_, _, err = OpenWallet(openInput(t, "100.00"), unrelated, baseTime)
		if !failure.Is(err, failure.ReferenceMismatch) {
			t.Errorf("OpenWallet with an unrelated wallet = %v, want %v", err, failure.ReferenceMismatch)
		}
	})
}

func TestWalletKey(t *testing.T) {
	t.Parallel()

	w := newWallet(t, "100.00")
	want := WalletKey{PlayerID: testPlayer, Currency: currency(t, "BRL")}
	if w.Key() != want {
		t.Errorf("Key() = %v, want %v", w.Key(), want)
	}

	// The pair is what makes a wallet unique, so a second currency for the same
	// player is a different wallet rather than a conflict.
	other := WalletKey{PlayerID: testPlayer, Currency: currency(t, "USD")}
	if w.Key() == other {
		t.Error("wallets in different currencies share a key")
	}
}

func TestRehydrateWallet(t *testing.T) {
	t.Parallel()

	valid := WalletSnapshot{
		ID:        NewWalletID(),
		PlayerID:  testPlayer,
		Balance:   brl(t, "250.00"),
		Version:   7,
		CreatedAt: baseTime,
		UpdatedAt: baseTime.Add(time.Hour),
	}

	t.Run("restores the stored state without replaying anything", func(t *testing.T) {
		t.Parallel()
		w, err := RehydrateWallet(valid)
		if err != nil {
			t.Fatalf("RehydrateWallet: %v", err)
		}
		assertBalance(t, w, "250.00")
		// The version is taken from storage, not recomputed: rehydration
		// reapplies no movement and so advances nothing.
		assertVersion(t, w, 7)
		if w.ID() != valid.ID || w.PlayerID() != valid.PlayerID {
			t.Error("rehydration changed the wallet's identity")
		}
		if !w.CreatedAt().Equal(valid.CreatedAt) || !w.UpdatedAt().Equal(valid.UpdatedAt) {
			t.Error("rehydration changed the wallet's timestamps")
		}
	})

	t.Run("refuses corrupt state", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name   string
			mutate func(*WalletSnapshot)
			want   failure.Code
		}{
			{"no id", func(s *WalletSnapshot) { s.ID = WalletID{} }, failure.MissingRequiredField},
			{"no player", func(s *WalletSnapshot) { s.PlayerID = "" }, failure.MissingRequiredField},
			{"uninitialised balance", func(s *WalletSnapshot) { s.Balance = money.Money{} }, failure.UninitializedValue},
			{"version zero", func(s *WalletSnapshot) { s.Version = 0 }, failure.InvalidFieldFormat},
			{"no createdAt", func(s *WalletSnapshot) { s.CreatedAt = time.Time{} }, failure.MissingRequiredField},
			{"no updatedAt", func(s *WalletSnapshot) { s.UpdatedAt = time.Time{} }, failure.MissingRequiredField},
			{
				"updatedAt before createdAt",
				func(s *WalletSnapshot) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) },
				failure.InvalidFieldFormat,
			},
			{
				"negative stored balance",
				func(s *WalletSnapshot) {
					negative, err := money.FromMinorUnits(-1, currency(t, "BRL"))
					if err != nil {
						t.Fatalf("FromMinorUnits: %v", err)
					}
					s.Balance = negative
				},
				failure.InsufficientFunds,
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				snapshot := valid
				tc.mutate(&snapshot)
				w, err := RehydrateWallet(snapshot)
				if err == nil {
					t.Fatalf("RehydrateWallet = %v, want an error", w)
				}
				if !failure.Is(err, tc.want) {
					t.Errorf("RehydrateWallet = %v, want code %v", err, tc.want)
				}
			})
		}
	})
}
