package wagering

import (
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

func validEntry(t *testing.T) LedgerEntryInput {
	t.Helper()
	return LedgerEntryInput{
		ID:            NewLedgerEntryID(),
		WalletID:      NewWalletID(),
		TransactionID: NewTransactionID(),
		Direction:     Debit,
		Amount:        brl(t, "25.00"),
		BalanceBefore: brl(t, "100.00"),
		BalanceAfter:  brl(t, "75.00"),
		WalletVersion: 2,
		CreatedAt:     baseTime,
	}
}

func TestLedgerEntryRecordsTheMovement(t *testing.T) {
	t.Parallel()

	in := validEntry(t)
	entry, err := NewWalletLedgerEntry(in)
	if err != nil {
		t.Fatalf("NewWalletLedgerEntry: %v", err)
	}

	if entry.ID() != in.ID || entry.WalletID() != in.WalletID || entry.TransactionID() != in.TransactionID {
		t.Error("the entry does not report the identifiers it was built with")
	}
	if entry.Direction() != Debit {
		t.Errorf("direction = %s, want %s", entry.Direction(), Debit)
	}
	if entry.Amount().Amount() != "25.00" {
		t.Errorf("amount = %s, want 25.00", entry.Amount().Amount())
	}
	if entry.WalletVersion() != 2 {
		t.Errorf("walletVersion = %d, want 2", entry.WalletVersion())
	}
	if !entry.CreatedAt().Equal(baseTime) {
		t.Error("the entry does not report the time it was built with")
	}
	if entry.IsZero() {
		t.Error("a constructed entry reports as never constructed")
	}
}

// TestLedgerEntryValidatesItsOwnArithmetic is the reason the constructor exists:
// an entry that misreports a movement is unrepresentable, not merely unlikely.
func TestLedgerEntryValidatesItsOwnArithmetic(t *testing.T) {
	t.Parallel()

	t.Run("a debit must subtract", func(t *testing.T) {
		t.Parallel()
		in := validEntry(t)
		in.BalanceAfter = brl(t, "125.00") // added instead of subtracted
		if _, err := NewWalletLedgerEntry(in); !failure.Is(err, failure.InvalidFieldFormat) {
			t.Errorf("a debit that added = %v, want %v", err, failure.InvalidFieldFormat)
		}
	})

	t.Run("a credit must add", func(t *testing.T) {
		t.Parallel()
		in := validEntry(t)
		in.Direction = Credit
		in.BalanceAfter = brl(t, "75.00") // subtracted instead of added
		if _, err := NewWalletLedgerEntry(in); !failure.Is(err, failure.InvalidFieldFormat) {
			t.Errorf("a credit that subtracted = %v, want %v", err, failure.InvalidFieldFormat)
		}
	})

	t.Run("the amounts must line up exactly", func(t *testing.T) {
		t.Parallel()
		in := validEntry(t)
		in.BalanceAfter = brl(t, "75.01") // out by a single minor unit
		if _, err := NewWalletLedgerEntry(in); !failure.Is(err, failure.InvalidFieldFormat) {
			t.Errorf("an entry out by 0.01 = %v, want %v", err, failure.InvalidFieldFormat)
		}
	})

	t.Run("a correct credit is accepted", func(t *testing.T) {
		t.Parallel()
		in := validEntry(t)
		in.Direction = Credit
		in.BalanceAfter = brl(t, "125.00")
		if _, err := NewWalletLedgerEntry(in); err != nil {
			t.Errorf("a correct credit = %v, want no error", err)
		}
	})
}

func TestLedgerEntryRefuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*LedgerEntryInput)
		want   failure.Code
	}{
		{"no id", func(in *LedgerEntryInput) { in.ID = LedgerEntryID{} }, failure.MissingRequiredField},
		{"no wallet", func(in *LedgerEntryInput) { in.WalletID = WalletID{} }, failure.MissingRequiredField},
		{"no transaction", func(in *LedgerEntryInput) { in.TransactionID = TransactionID{} }, failure.MissingRequiredField},
		{"no createdAt", func(in *LedgerEntryInput) { in.CreatedAt = time.Time{} }, failure.MissingRequiredField},
		{"version zero", func(in *LedgerEntryInput) { in.WalletVersion = 0 }, failure.InvalidFieldFormat},
		{"unknown direction", func(in *LedgerEntryInput) { in.Direction = "TRANSFER" }, failure.InvalidFieldFormat},
		{
			"uninitialised amount",
			func(in *LedgerEntryInput) { in.Amount = money.Money{} },
			failure.UninitializedValue,
		},
		{
			"uninitialised balance",
			func(in *LedgerEntryInput) { in.BalanceBefore = money.Money{} },
			failure.UninitializedValue,
		},
		{
			// An entry exists because money moved, so a zero movement is an
			// entry for something that did not happen.
			"zero amount",
			func(in *LedgerEntryInput) {
				in.Amount = brl(t, "0.00")
				in.BalanceAfter = in.BalanceBefore
			},
			failure.InvalidAmountForKind,
		},
		{
			"negative resulting balance",
			func(in *LedgerEntryInput) {
				in.Amount = brl(t, "150.00")
				negative, err := money.FromMinorUnits(-5000, currency(t, "BRL"))
				if err != nil {
					t.Fatalf("FromMinorUnits: %v", err)
				}
				in.BalanceAfter = negative
			},
			failure.InsufficientFunds,
		},
		{
			"foreign currency",
			func(in *LedgerEntryInput) { in.Amount = usd(t, "25.00") },
			failure.CurrencyMismatch,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := validEntry(t)
			tc.mutate(&in)
			entry, err := NewWalletLedgerEntry(in)
			if err == nil {
				t.Fatalf("NewWalletLedgerEntry = %v, want an error", entry)
			}
			if !failure.Is(err, tc.want) {
				t.Errorf("NewWalletLedgerEntry = %v, want code %v", err, tc.want)
			}
		})
	}
}

// TestRehydrateLedgerEntryValidatesTheSameWay covers the fact that an entry is
// immutable: there is no difference between a valid new one and a valid stored
// one, so stored corruption is reported rather than loaded.
func TestRehydrateLedgerEntryValidatesTheSameWay(t *testing.T) {
	t.Parallel()

	in := validEntry(t)
	if _, err := RehydrateWalletLedgerEntry(in); err != nil {
		t.Fatalf("RehydrateWalletLedgerEntry: %v", err)
	}

	in.BalanceAfter = brl(t, "80.00") // arithmetic that no longer holds
	if _, err := RehydrateWalletLedgerEntry(in); !failure.Is(err, failure.InvalidFieldFormat) {
		t.Errorf("rehydrating a corrupt entry = %v, want %v", err, failure.InvalidFieldFormat)
	}
}

func TestDirection(t *testing.T) {
	t.Parallel()

	t.Run("opposites", func(t *testing.T) {
		t.Parallel()
		if got, ok := Debit.Opposite(); !ok || got != Credit {
			t.Errorf("Debit.Opposite() = %s, %v, want %s, true", got, ok, Credit)
		}
		if got, ok := Credit.Opposite(); !ok || got != Debit {
			t.Errorf("Credit.Opposite() = %s, %v, want %s, true", got, ok, Debit)
		}
		if _, ok := Direction("TRANSFER").Opposite(); ok {
			t.Error("an unknown direction reported an opposite")
		}
	})

	t.Run("parsing", func(t *testing.T) {
		t.Parallel()
		for _, want := range []Direction{Debit, Credit} {
			got, err := ParseDirection(string(want))
			if err != nil || got != want {
				t.Errorf("ParseDirection(%q) = %s, %v", want, got, err)
			}
		}
		if _, err := ParseDirection("debit"); !failure.Is(err, failure.InvalidFieldFormat) {
			t.Errorf("ParseDirection(%q) = %v, want %v", "debit", err, failure.InvalidFieldFormat)
		}
	})
}
