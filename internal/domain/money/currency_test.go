package money

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

func TestParseCurrency(t *testing.T) {
	t.Parallel()

	t.Run("accepts three uppercase ASCII letters", func(t *testing.T) {
		t.Parallel()
		for _, code := range []string{"BRL", "USD", "EUR", "JPY", "ZZZ"} {
			c, err := ParseCurrency(code)
			if err != nil {
				t.Errorf("ParseCurrency(%q) = %v, want no error", code, err)
				continue
			}
			if got := c.String(); got != code {
				t.Errorf("ParseCurrency(%q).String() = %q, want %q", code, got, code)
			}
			if c.IsZero() {
				t.Errorf("ParseCurrency(%q).IsZero() = true, want false", code)
			}
		}
	})

	t.Run("refuses anything else", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name  string
			input string
		}{
			{"empty", ""},
			{"too short", "BR"},
			{"too long", "BRLX"},
			{"lowercase", "brl"},
			{"mixed case", "Brl"},
			{"trailing lowercase", "BRl"},
			{"digits", "BR1"},
			{"leading space", " BR"},
			{"trailing space", "BR "},
			{"all spaces", "   "},
			{"symbol", "BR$"},
			{"non-ASCII letter", "BRÇ"},
			{"non-ASCII digits", "１２３"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				c, err := ParseCurrency(tc.input)
				if err == nil {
					t.Fatalf("ParseCurrency(%q) = %v, want an error", tc.input, c)
				}
				if !failure.Is(err, failure.UnsupportedCurrency) {
					t.Errorf("ParseCurrency(%q) code = %v, want %v", tc.input, err, failure.UnsupportedCurrency)
				}
				if !c.IsZero() {
					t.Errorf("ParseCurrency(%q) returned %v on failure, want the zero value", tc.input, c)
				}
			})
		}
	})
}

func TestCurrencyZeroValue(t *testing.T) {
	t.Parallel()

	var c Currency
	if !c.IsZero() {
		t.Error("zero Currency.IsZero() = false, want true")
	}
	if got := c.String(); got != "" {
		t.Errorf("zero Currency.String() = %q, want %q", got, "")
	}
}

// TestCurrencyIsComparable pins the property the domain relies on when it
// compares a movement's currency with a wallet's.
func TestCurrencyIsComparable(t *testing.T) {
	t.Parallel()

	brl, usd := mustCurrency(t, "BRL"), mustCurrency(t, "USD")
	again := mustCurrency(t, "BRL")

	if brl != again {
		t.Error("two parses of BRL are not equal, want equal")
	}
	if brl == usd {
		t.Error("BRL equals USD, want not equal")
	}
	if _, ok := map[Currency]int{brl: 1}[again]; !ok {
		t.Error("Currency is not usable as a map key")
	}
}
