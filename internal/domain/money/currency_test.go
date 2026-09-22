package money

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

func TestParseCurrency(t *testing.T) {
	t.Parallel()

	t.Run("accepts a supported currency with two minor-unit digits", func(t *testing.T) {
		t.Parallel()
		// MGA and MRU are the two whose minor unit is not decimal in practice
		// (a fifth of the major unit); ISO 4217 nonetheless lists both at two
		// digits, and that listing is what the allowlist follows.
		for _, code := range []string{"BRL", "USD", "EUR", "GBP", "MGA", "MRU"} {
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
			// Well formed, but not a currency this scale can hold.
			{"zero minor-unit digits", "JPY"},
			{"three minor-unit digits", "KWD"},
			{"four minor-unit digits", "CLF"},
			{"the no-currency code", "XXX"},
			{"three letters naming nothing", "ABC"},

			// Malformed. These never reach the allowlist.
			{"empty", ""},
			{"too short", "BR"},
			{"too long", "BRLL"},
			{"lowercase", "brl"},
			{"mixed case", "Brl"},
			{"trailing lowercase", "BRl"},
			{"digits", "BR1"},
			{"leading space", " BRL"},
			{"trailing space", "BRL "},
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

	t.Run("a refusal names the rule and not the list", func(t *testing.T) {
		t.Parallel()
		_, err := ParseCurrency("JPY")
		if err == nil {
			t.Fatal("ParseCurrency(\"JPY\") = nil, want an error")
		}
		msg := err.Error()
		for _, want := range []string{"JPY", "ISO 4217", "two minor-unit digits"} {
			if !strings.Contains(msg, want) {
				t.Errorf("ParseCurrency(\"JPY\") = %q, want it to mention %q", msg, want)
			}
		}
		// A provider is told which rule it broke, not handed the catalogue.
		for _, leaked := range []string{"BRL", "USD", "EUR"} {
			if strings.Contains(msg, leaked) {
				t.Errorf("ParseCurrency(\"JPY\") = %q, which leaks the allowlist (%q)", msg, leaked)
			}
		}
	})
}

// TestSupportedCurrenciesIsSortedUniqueAndWellFormed pins the three properties
// ParseCurrency relies on: the list is searched by bisection, so it must be
// sorted; it is a set, so no code may appear twice; and every entry must pass
// the form check that runs in front of it, or the entry could never be reached.
func TestSupportedCurrenciesIsSortedUniqueAndWellFormed(t *testing.T) {
	t.Parallel()

	if len(supportedCurrencies) == 0 {
		t.Fatal("supportedCurrencies is empty")
	}
	if !slices.IsSorted(supportedCurrencies) {
		t.Error("supportedCurrencies is not sorted")
	}
	seen := make(map[string]bool, len(supportedCurrencies))
	form := regexp.MustCompile(`^[A-Z]{3}$`)
	for _, code := range supportedCurrencies {
		if seen[code] {
			t.Errorf("supportedCurrencies lists %q twice", code)
		}
		seen[code] = true
		if !form.MatchString(code) {
			t.Errorf("supportedCurrencies lists %q, which is not three uppercase ASCII letters", code)
		}
	}
	for _, code := range []string{"BRL", "USD", "EUR"} {
		if !seen[code] {
			t.Errorf("supportedCurrencies is missing %s", code)
		}
	}
}

// TestSupportedCurrenciesHoldsOnlyScaleTwoCurrencies pins the reason the list
// exists. Every ISO 4217 code whose minor unit is not two digits, and every
// code that names no currency at all, must be absent — an entry here would be
// accepted and then misrepresented by a factor of ten, a hundred, or a
// hundredth.
func TestSupportedCurrenciesHoldsOnlyScaleTwoCurrencies(t *testing.T) {
	t.Parallel()

	excluded := map[string][]string{
		"zero minor-unit digits": {
			"BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW",
			"PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF",
		},
		"three minor-unit digits": {"BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND"},
		"four minor-unit digits":  {"CLF", "UYW"},
		"no currency at all":      {"XXX", "XTS", "XAU", "XAG", "XPT", "XPD", "XDR", "XSU", "XUA"},
		// ISO 4217 also assigns codes to funds — units of account such as the
		// Bolivian Mvdol or the WIR Euro. They carry two digits but no wallet
		// is denominated in one, and the list is of currencies.
		"a fund rather than a currency": {"BOV", "CHE", "CHW", "COU", "MXV", "USN", "UYI"},
	}
	for reason, codes := range excluded {
		for _, code := range codes {
			if _, found := slices.BinarySearch(supportedCurrencies, code); found {
				t.Errorf("supportedCurrencies lists %s, which has %s", code, reason)
			}
		}
	}
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
