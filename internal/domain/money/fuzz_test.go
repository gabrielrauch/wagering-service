package money

import (
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// FuzzParse holds against unchosen bytes what TestParseAccepts and
// TestParseRefuses assert case by case.
//
// Parse accepts only the canonical rendering of an amount — no sign, no leading
// zero, no whitespace, exactly two fraction digits — so whatever it accepts must
// come back out of Amount unchanged. That makes the set of accepted strings and
// the set of rendered strings one set, which is what lets an idempotency payload
// be rebuilt from a stored amount and hash to what the provider first sent.
func FuzzParse(f *testing.F) {
	seeds := []struct{ amount, currency string }{
		// Accepted, including both ends of the representable range.
		{"0.00", "BRL"}, {"0.01", "BRL"}, {"0.99", "USD"}, {"1.00", "BRL"},
		{"25.50", "EUR"}, {"1234567.89", "BRL"}, {maxAmount, "BRL"},

		// Refused. Each is a class the parser has to keep turning down, kept in
		// the corpus so a mutation of it starts from somewhere interesting.
		{"", "BRL"}, {"25", "BRL"}, {"25.0", "BRL"}, {"25.000", "BRL"},
		{"25.", "BRL"}, {".25", "BRL"}, {"25..00", "BRL"}, {"007.00", "BRL"},
		{"-25.00", "BRL"}, {"+25.00", "BRL"}, {" 25.00", "BRL"}, {"25.00 ", "BRL"},
		{"1e2", "BRL"}, {"NaN", "BRL"}, {"Infinity", "BRL"},
		{"92233720368547758.08", "BRL"}, // one minor unit past the maximum
		{"25.00", ""}, {"25.00", "brl"}, {"25.00", "BR"}, {"25.00", "BRLL"},
		{"25.00", "B1L"}, {"25.00", "BRÇ"},
	}
	for _, s := range seeds {
		f.Add(s.amount, s.currency)
	}

	f.Fuzz(func(t *testing.T, amount, currency string) {
		m, err := Parse(amount, currency)
		if err != nil {
			// A refusal names the rule that was broken; a provider is never
			// handed an error it cannot act on.
			if _, ok := failure.CodeOf(err); !ok {
				t.Fatalf("Parse(%q, %q) refused with a code-less error: %v", amount, currency, err)
			}
			return
		}

		if got := m.Amount(); got != amount {
			t.Fatalf("Parse(%q, %q).Amount() = %q, want the input back unchanged",
				amount, currency, got)
		}
		if got := m.Currency().String(); got != currency {
			t.Fatalf("Parse(%q, %q).Currency() = %q, want the input back unchanged",
				amount, currency, got)
		}

		again, err := Parse(m.Amount(), m.Currency().String())
		if err != nil {
			t.Fatalf("Parse refused what it had just rendered: Parse(%q, %q) = %v",
				m.Amount(), m.Currency(), err)
		}
		// Compared with ==, not Equal: this is an identity check on the value,
		// and Equal deliberately reports false for uninitialised operands.
		if again != m {
			t.Fatalf("a round trip changed the value: %d minor units became %d",
				m.MinorUnits(), again.MinorUnits())
		}
	})
}
