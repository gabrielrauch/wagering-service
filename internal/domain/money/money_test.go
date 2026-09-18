package money

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// The extremes of the representation, spelled out so the limit tests read as
// documentation rather than as arithmetic.
const (
	maxAmount     = "92233720368547758.07"
	overMaxAmount = "92233720368547758.08"
	minAmount     = "-92233720368547758.08"
)

func mustCurrency(t *testing.T, code string) Currency {
	t.Helper()
	c, err := ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q): %v", code, err)
	}
	return c
}

func mustParse(t *testing.T, amount, currency string) Money {
	t.Helper()
	m, err := Parse(amount, currency)
	if err != nil {
		t.Fatalf("Parse(%q, %q): %v", amount, currency, err)
	}
	return m
}

func mustUnits(t *testing.T, units int64, currency string) Money {
	t.Helper()
	m, err := FromMinorUnits(units, mustCurrency(t, currency))
	if err != nil {
		t.Fatalf("FromMinorUnits(%d, %q): %v", units, currency, err)
	}
	return m
}

func TestParseAccepts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		amount string
		units  int64
	}{
		{"0.00", 0},
		{"0.01", 1},
		{"0.99", 99},
		{"1.00", 100},
		{"25.00", 2500},
		{"25.50", 2550},
		{"1234567.89", 123456789},
		{maxAmount, math.MaxInt64},
	}
	for _, tc := range tests {
		t.Run(tc.amount, func(t *testing.T) {
			t.Parallel()
			m := mustParse(t, tc.amount, "BRL")
			if m.MinorUnits() != tc.units {
				t.Errorf("Parse(%q).MinorUnits() = %d, want %d", tc.amount, m.MinorUnits(), tc.units)
			}
			if got := m.Amount(); got != tc.amount {
				t.Errorf("Parse(%q).Amount() = %q, want the input back unchanged", tc.amount, got)
			}
			if m.Currency().String() != "BRL" {
				t.Errorf("Parse(%q).Currency() = %q, want BRL", tc.amount, m.Currency())
			}
		})
	}
}

// TestParseRefuses covers every class the brief calls out by name: empty
// values, NaN, Infinity, scientific notation, excess scale and negatives.
func TestParseRefuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		amount string
		want   failure.Code
	}{
		{"empty", "", failure.InvalidAmountFormat},
		{"no fraction digits", "25", failure.InvalidAmountScale},
		{"one fraction digit", "25.0", failure.InvalidAmountScale},
		{"three fraction digits", "25.000", failure.InvalidAmountScale},
		{"many fraction digits", "0.123456789", failure.InvalidAmountScale},
		{"trailing dot", "25.", failure.InvalidAmountFormat},
		{"leading dot", ".25", failure.InvalidAmountFormat},
		{"lone dot", ".", failure.InvalidAmountFormat},
		{"double dot", "25..00", failure.InvalidAmountFormat},
		{"two dots", "2.5.0", failure.InvalidAmountFormat},
		{"leading zero", "007.00", failure.InvalidAmountFormat},
		{"single leading zero", "025.00", failure.InvalidAmountFormat},
		{"explicit plus", "+25.00", failure.InvalidAmountFormat},
		{"negative", "-25.00", failure.InvalidAmountFormat},
		{"negative zero", "-0.00", failure.InvalidAmountFormat},
		{"leading space", " 25.00", failure.InvalidAmountFormat},
		{"trailing space", "25.00 ", failure.InvalidAmountFormat},
		{"inner space", "25. 00", failure.InvalidAmountFormat},
		{"scientific lower", "1e2", failure.InvalidAmountFormat},
		{"scientific upper", "1E2", failure.InvalidAmountFormat},
		{"scientific with fraction", "2.50e1", failure.InvalidAmountFormat},
		{"NaN", "NaN", failure.InvalidAmountFormat},
		{"Infinity", "Infinity", failure.InvalidAmountFormat},
		{"Inf", "Inf", failure.InvalidAmountFormat},
		{"negative Inf", "-Inf", failure.InvalidAmountFormat},
		{"hex", "0x19", failure.InvalidAmountFormat},
		{"comma separator", "25,00", failure.InvalidAmountFormat},
		{"thousands separator", "1,234.00", failure.InvalidAmountFormat},
		{"currency symbol", "R$25.00", failure.InvalidAmountFormat},
		{"letters", "abc", failure.InvalidAmountFormat},
		{"non-ASCII digits", "２５.００", failure.InvalidAmountFormat},
		{"underscore separator", "1_000.00", failure.InvalidAmountFormat},
		{"above maximum", overMaxAmount, failure.AmountOutOfRange},
		{"far above maximum", "99999999999999999999.00", failure.AmountOutOfRange},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := Parse(tc.amount, "BRL")
			if err == nil {
				t.Fatalf("Parse(%q) = %v, want an error", tc.amount, m)
			}
			if !failure.Is(err, tc.want) {
				t.Errorf("Parse(%q) = %v, want code %v", tc.amount, err, tc.want)
			}
			if !failure.Correctable(err) {
				t.Errorf("Parse(%q) produced a definitive failure; malformed input must be correctable", tc.amount)
			}
		})
	}
}

// TestParseValidatesCurrencyFirst pins the error a provider sees when the
// submission is wrong in both respects, so the response is deterministic.
func TestParseValidatesCurrencyFirst(t *testing.T) {
	t.Parallel()

	_, err := Parse("not-a-number", "brl")
	if !failure.Is(err, failure.UnsupportedCurrency) {
		t.Errorf("Parse with a bad amount and a bad currency = %v, want %v", err, failure.UnsupportedCurrency)
	}
}

func TestZeroAndFromMinorUnits(t *testing.T) {
	t.Parallel()

	brl := mustCurrency(t, "BRL")

	t.Run("Zero is the zero amount in the currency", func(t *testing.T) {
		t.Parallel()
		z, err := Zero(brl)
		if err != nil {
			t.Fatalf("Zero: %v", err)
		}
		if !z.IsZero() || z.MinorUnits() != 0 || z.Amount() != "0.00" {
			t.Errorf("Zero(BRL) = %v, want 0.00 BRL", z)
		}
	})

	t.Run("FromMinorUnits accepts negatives", func(t *testing.T) {
		t.Parallel()
		m := mustUnits(t, -2500, "BRL")
		if !m.IsNegative() || m.Amount() != "-25.00" {
			t.Errorf("FromMinorUnits(-2500) = %v, want -25.00 BRL", m)
		}
	})

	t.Run("both refuse an uninitialised currency", func(t *testing.T) {
		t.Parallel()
		if _, err := Zero(Currency{}); !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("Zero(zero currency) = %v, want %v", err, failure.UninitializedValue)
		}
		if _, err := FromMinorUnits(1, Currency{}); !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("FromMinorUnits(zero currency) = %v, want %v", err, failure.UninitializedValue)
		}
	})
}

func TestAmountRendering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		units int64
		want  string
	}{
		{0, "0.00"},
		{1, "0.01"},
		{-1, "-0.01"},
		{99, "0.99"},
		{100, "1.00"},
		{-2500, "-25.00"},
		{math.MaxInt64, maxAmount},
		{math.MinInt64, minAmount},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := mustUnits(t, tc.units, "BRL").Amount(); got != tc.want {
				t.Errorf("FromMinorUnits(%d).Amount() = %q, want %q", tc.units, got, tc.want)
			}
		})
	}
}

func TestArithmetic(t *testing.T) {
	t.Parallel()

	t.Run("Add", func(t *testing.T) {
		t.Parallel()
		sum, err := mustParse(t, "25.00", "BRL").Add(mustParse(t, "0.75", "BRL"))
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		if got := sum.Amount(); got != "25.75" {
			t.Errorf("25.00 + 0.75 = %q, want 25.75", got)
		}
	})

	t.Run("Sub may go negative", func(t *testing.T) {
		t.Parallel()
		diff, err := mustParse(t, "10.00", "BRL").Sub(mustParse(t, "25.00", "BRL"))
		if err != nil {
			t.Fatalf("Sub: %v", err)
		}
		if got := diff.Amount(); got != "-15.00" {
			t.Errorf("10.00 - 25.00 = %q, want -15.00", got)
		}
		if !diff.IsNegative() {
			t.Error("10.00 - 25.00 is not reported as negative")
		}
	})

	t.Run("Neg", func(t *testing.T) {
		t.Parallel()
		neg, err := mustParse(t, "25.00", "BRL").Neg()
		if err != nil {
			t.Fatalf("Neg: %v", err)
		}
		if got := neg.Amount(); got != "-25.00" {
			t.Errorf("-(25.00) = %q, want -25.00", got)
		}
		back, err := neg.Neg()
		if err != nil {
			t.Fatalf("Neg twice: %v", err)
		}
		if !back.Equal(mustParse(t, "25.00", "BRL")) {
			t.Errorf("-(-(25.00)) = %v, want 25.00 BRL", back)
		}
	})

	t.Run("Cmp", func(t *testing.T) {
		t.Parallel()
		small, large := mustParse(t, "10.00", "BRL"), mustParse(t, "25.00", "BRL")
		for _, tc := range []struct {
			name string
			a, b Money
			want int
		}{
			{"less", small, large, -1},
			{"greater", large, small, 1},
			{"equal", small, small, 0},
		} {
			got, err := tc.a.Cmp(tc.b)
			if err != nil {
				t.Fatalf("Cmp %s: %v", tc.name, err)
			}
			if got != tc.want {
				t.Errorf("Cmp %s = %d, want %d", tc.name, got, tc.want)
			}
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()
		brl25, brl10 := mustParse(t, "25.00", "BRL"), mustParse(t, "10.00", "BRL")
		usd25 := mustParse(t, "25.00", "USD")

		if !brl25.Equal(mustParse(t, "25.00", "BRL")) {
			t.Error("25.00 BRL is not equal to itself")
		}
		if brl25.Equal(brl10) {
			t.Error("25.00 BRL equals 10.00 BRL")
		}
		if brl25.Equal(usd25) {
			t.Error("25.00 BRL equals 25.00 USD; equality must span the currency")
		}
		if (Money{}).Equal(Money{}) {
			t.Error("two uninitialised values compare equal; they name no amount")
		}
	})
}

// TestCurrencyMismatch covers the brief's requirement that arithmetic and
// comparison refuse incompatible currencies, even though the main flows are
// BRL only.
func TestCurrencyMismatch(t *testing.T) {
	t.Parallel()

	brl, usd := mustParse(t, "25.00", "BRL"), mustParse(t, "25.00", "USD")

	t.Run("Add", func(t *testing.T) {
		t.Parallel()
		if _, err := brl.Add(usd); !failure.Is(err, failure.CurrencyMismatch) {
			t.Errorf("BRL.Add(USD) = %v, want %v", err, failure.CurrencyMismatch)
		}
	})
	t.Run("Sub", func(t *testing.T) {
		t.Parallel()
		if _, err := brl.Sub(usd); !failure.Is(err, failure.CurrencyMismatch) {
			t.Errorf("BRL.Sub(USD) = %v, want %v", err, failure.CurrencyMismatch)
		}
	})
	t.Run("Cmp", func(t *testing.T) {
		t.Parallel()
		if _, err := brl.Cmp(usd); !failure.Is(err, failure.CurrencyMismatch) {
			t.Errorf("BRL.Cmp(USD) = %v, want %v", err, failure.CurrencyMismatch)
		}
	})
}

func TestUninitialisedValueIsRefused(t *testing.T) {
	t.Parallel()

	var zero Money
	brl := mustParse(t, "25.00", "BRL")

	if !zero.IsUninitialized() {
		t.Fatal("zero Money is not reported as uninitialised")
	}
	if zero.IsZero() || zero.IsPositive() || zero.IsNegative() {
		t.Error("uninitialised Money reports a sign; it names no amount")
	}

	operations := map[string]func() error{
		"Add into":    func() error { _, err := brl.Add(zero); return err },
		"Add from":    func() error { _, err := zero.Add(brl); return err },
		"Sub into":    func() error { _, err := brl.Sub(zero); return err },
		"Sub from":    func() error { _, err := zero.Sub(brl); return err },
		"Cmp":         func() error { _, err := brl.Cmp(zero); return err },
		"Neg":         func() error { _, err := zero.Neg(); return err },
		"MarshalJSON": func() error { _, err := zero.MarshalJSON(); return err },
	}
	for name, op := range operations {
		if err := op(); !failure.Is(err, failure.UninitializedValue) {
			t.Errorf("%s uninitialised money = %v, want %v", name, err, failure.UninitializedValue)
		}
	}
}

func TestOverflow(t *testing.T) {
	t.Parallel()

	max := mustUnits(t, math.MaxInt64, "BRL")
	min := mustUnits(t, math.MinInt64, "BRL")
	one := mustParse(t, "0.01", "BRL")

	t.Run("Add past the maximum", func(t *testing.T) {
		t.Parallel()
		if _, err := max.Add(one); !failure.Is(err, failure.AmountOutOfRange) {
			t.Errorf("max + 0.01 = %v, want %v", err, failure.AmountOutOfRange)
		}
	})

	t.Run("Add past the minimum", func(t *testing.T) {
		t.Parallel()
		negOne, err := one.Neg()
		if err != nil {
			t.Fatalf("Neg: %v", err)
		}
		if _, err := min.Add(negOne); !failure.Is(err, failure.AmountOutOfRange) {
			t.Errorf("min + -0.01 = %v, want %v", err, failure.AmountOutOfRange)
		}
	})

	t.Run("Sub past the minimum", func(t *testing.T) {
		t.Parallel()
		if _, err := min.Sub(one); !failure.Is(err, failure.AmountOutOfRange) {
			t.Errorf("min - 0.01 = %v, want %v", err, failure.AmountOutOfRange)
		}
	})

	t.Run("Sub past the maximum", func(t *testing.T) {
		t.Parallel()
		negOne, err := one.Neg()
		if err != nil {
			t.Fatalf("Neg: %v", err)
		}
		if _, err := max.Sub(negOne); !failure.Is(err, failure.AmountOutOfRange) {
			t.Errorf("max - -0.01 = %v, want %v", err, failure.AmountOutOfRange)
		}
	})

	// Negating the most negative value has no representable result. This is the
	// classic int64 trap and the reason Neg returns an error at all.
	t.Run("Neg of the most negative value", func(t *testing.T) {
		t.Parallel()
		if _, err := min.Neg(); !failure.Is(err, failure.AmountOutOfRange) {
			t.Errorf("-(min) = %v, want %v", err, failure.AmountOutOfRange)
		}
	})

	t.Run("boundaries themselves are fine", func(t *testing.T) {
		t.Parallel()
		zero, err := Zero(mustCurrency(t, "BRL"))
		if err != nil {
			t.Fatalf("Zero: %v", err)
		}
		if _, err := max.Add(zero); err != nil {
			t.Errorf("max + 0.00 = %v, want no error", err)
		}
		if _, err := min.Sub(zero); err != nil {
			t.Errorf("min - 0.00 = %v, want no error", err)
		}
	})
}

func TestJSON(t *testing.T) {
	t.Parallel()

	t.Run("marshals to the external contract", func(t *testing.T) {
		t.Parallel()
		data, err := json.Marshal(mustParse(t, "25.00", "BRL"))
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		const want = `{"amount":"25.00","currency":"BRL"}`
		if string(data) != want {
			t.Errorf("Marshal = %s, want %s", data, want)
		}
	})

	t.Run("round-trips", func(t *testing.T) {
		t.Parallel()
		for _, amount := range []string{"0.00", "0.01", "25.00", maxAmount} {
			original := mustParse(t, amount, "BRL")
			data, err := json.Marshal(original)
			if err != nil {
				t.Fatalf("Marshal(%s): %v", amount, err)
			}
			var back Money
			if err := json.Unmarshal(data, &back); err != nil {
				t.Fatalf("Unmarshal(%s): %v", data, err)
			}
			if !back.Equal(original) {
				t.Errorf("round-trip of %s produced %v", amount, back)
			}
		}
	})

	t.Run("refuses a negative amount", func(t *testing.T) {
		t.Parallel()
		if _, err := json.Marshal(mustUnits(t, -1, "BRL")); err == nil {
			t.Error("marshalling a negative amount succeeded; the contract has no form for it")
		}
	})

	t.Run("unmarshal applies the same validation as Parse", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name string
			data string
			want failure.Code
		}{
			{"under-scale amount", `{"amount":"25","currency":"BRL"}`, failure.InvalidAmountScale},
			{"negative amount", `{"amount":"-25.00","currency":"BRL"}`, failure.InvalidAmountFormat},
			{"lowercase currency", `{"amount":"25.00","currency":"brl"}`, failure.UnsupportedCurrency},
			{"missing currency", `{"amount":"25.00"}`, failure.UnsupportedCurrency},
			{"unknown field", `{"amount":"25.00","currency":"BRL","note":"x"}`, failure.InvalidFieldFormat},
			{"amount as number", `{"amount":25.00,"currency":"BRL"}`, failure.InvalidFieldFormat},
			{"not an object", `"25.00 BRL"`, failure.InvalidFieldFormat},
			// Repeated names are refused rather than resolved. Under
			// encoding/json v1 the last value wins, so these two would arrive
			// as 999999.00 and 999 respectively with nothing to say a second
			// value had been seen at all.
			{"repeated amount", `{"amount":"1.00","amount":"999999.00","currency":"BRL"}`, failure.InvalidFieldFormat},
			{"repeated currency", `{"amount":"1.00","currency":"BRL","currency":"USD"}`, failure.InvalidFieldFormat},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				var m Money
				err := json.Unmarshal([]byte(tc.data), &m)
				if err == nil {
					t.Fatalf("Unmarshal(%s) = %v, want an error", tc.data, m)
				}
				if !failure.Is(err, tc.want) {
					t.Errorf("Unmarshal(%s) = %v, want code %v", tc.data, err, tc.want)
				}
			})
		}
	})
}

// TestImmutability guards the promise that operations never write through the
// receiver, which is what lets Money be passed around freely.
func TestImmutability(t *testing.T) {
	t.Parallel()

	original := mustParse(t, "25.00", "BRL")
	snapshot := original

	if _, err := original.Add(mustParse(t, "5.00", "BRL")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := original.Sub(mustParse(t, "5.00", "BRL")); err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if _, err := original.Neg(); err != nil {
		t.Fatalf("Neg: %v", err)
	}
	if original != snapshot {
		t.Errorf("operations mutated the receiver: %v, want %v", original, snapshot)
	}
}
