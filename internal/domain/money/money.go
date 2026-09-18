// Package money provides an exact, immutable monetary value object.
//
// # Representation
//
// A [Money] holds a signed count of minor units in an int64, paired with the
// [Currency] it is denominated in. The scale is fixed at two decimal places, so
// one minor unit is one hundredth of a major unit and "25.00" is held as 2500.
//
// Nothing here passes through float32 or float64 at any point — not in parsing,
// arithmetic, comparison or serialisation. Amounts are parsed digit by digit
// into integers and rendered back the same way.
//
// # Limits
//
// The representable range is the int64 range divided by the scale:
//
//	minimum  -92,233,720,368,547,758.08
//	maximum   92,233,720,368,547,758.07
//
// Parsing, addition, subtraction and negation all detect overflow and report
// [failure.AmountOutOfRange] rather than wrapping. Negating the most negative
// value overflows and is refused, since its magnitude has no positive
// counterpart.
//
// Minor units map directly onto a BIGINT column, and the currency onto a short
// CHAR, so a persisted value reproduces both exactly with no decimal parsing in
// the database.
//
// # Accepted input
//
// [Parse] is the external-contract parser and accepts exactly one spelling of
// any value:
//
//	(0|[1-9][0-9]*)\.[0-9]{2}
//
// No sign, no leading zeros, no whitespace, no exponent. "25.00" is accepted;
// "25", "25.0", "25.000", "25.", ".25", "007.00", "+25.00", "-25.00", " 25.00 ",
// "1e2", "NaN", "Infinity" and any non-ASCII digit are all refused. Nothing is
// ever silently rounded or padded.
//
// Because exactly one spelling is valid, and because [ParseCurrency] is
// case-strict, no normalisation is applied before an amount reaches the
// idempotency hash: the canonical form is the submitted form.
//
// Negative values cannot be parsed. They arise only from [Money.Sub] and
// [Money.Neg], which exist for differences in internal calculation, and they
// are refused by [Money.MarshalJSON] because the external contract has no
// representation for them.
package money

import (
	"cmp"
	"encoding/json/v2"
	"math"
	"strconv"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

const (
	// Scale is the fixed number of decimal places every amount carries.
	Scale = 2
	// minorPerMajor is the number of minor units in one major unit.
	minorPerMajor = 100
)

// Money is an exact monetary amount in a single currency.
//
// It is immutable: every operation returns a new value and none mutates the
// receiver. Its zero value carries no currency and is refused by every
// operation, so an uninitialised Money can never take part in a calculation.
type Money struct {
	units    int64
	currency Currency
}

// Parse builds a Money from the external contract's representation of an
// amount and a currency.
//
// The currency is validated first, so a submission wrong in both respects
// always reports [failure.UnsupportedCurrency]. That ordering keeps the error a
// provider sees deterministic.
func Parse(amount, currency string) (Money, error) {
	c, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	units, err := parseAmount(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{units: units, currency: c}, nil
}

// Zero returns the zero amount in c.
func Zero(c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, failure.New(failure.UninitializedValue, "currency is uninitialised")
	}
	return Money{currency: c}, nil
}

// FromMinorUnits builds a Money from a count of minor units.
//
// Unlike [Parse] it accepts negative values, because it is the entry point for
// values reconstructed from storage and for differences computed internally.
func FromMinorUnits(units int64, c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, failure.New(failure.UninitializedValue, "currency is uninitialised")
	}
	return Money{units: units, currency: c}, nil
}

// MinorUnits returns the amount as a signed count of minor units, the form in
// which it should be persisted.
func (m Money) MinorUnits() int64 { return m.units }

// Currency returns the currency the amount is denominated in.
func (m Money) Currency() Currency { return m.currency }

// IsZero reports whether the amount is exactly zero. It is false for the
// uninitialised value, which has no amount at all; use [Money.IsUninitialized]
// to test for that.
func (m Money) IsZero() bool { return !m.IsUninitialized() && m.units == 0 }

// IsPositive reports whether the amount is greater than zero.
func (m Money) IsPositive() bool { return !m.IsUninitialized() && m.units > 0 }

// IsNegative reports whether the amount is less than zero.
func (m Money) IsNegative() bool { return !m.IsUninitialized() && m.units < 0 }

// IsUninitialized reports whether m is the zero value, carrying no currency.
func (m Money) IsUninitialized() bool { return m.currency.IsZero() }

// Add returns m + other.
//
// Both values must be initialised and share a currency. Overflow is refused
// rather than wrapped.
func (m Money) Add(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}
	if other.units > 0 && m.units > math.MaxInt64-other.units {
		return Money{}, overflow("adding %s to %s", other, m)
	}
	if other.units < 0 && m.units < math.MinInt64-other.units {
		return Money{}, overflow("adding %s to %s", other, m)
	}
	return Money{units: m.units + other.units, currency: m.currency}, nil
}

// Sub returns m - other, which may be negative.
//
// Both values must be initialised and share a currency. Overflow is refused
// rather than wrapped.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}
	if other.units < 0 && m.units > math.MaxInt64+other.units {
		return Money{}, overflow("subtracting %s from %s", other, m)
	}
	if other.units > 0 && m.units < math.MinInt64+other.units {
		return Money{}, overflow("subtracting %s from %s", other, m)
	}
	return Money{units: m.units - other.units, currency: m.currency}, nil
}

// Neg returns -m.
//
// Negating the most negative representable amount overflows, because its
// magnitude exceeds the largest positive value, and is refused.
func (m Money) Neg() (Money, error) {
	if m.IsUninitialized() {
		return Money{}, failure.New(failure.UninitializedValue, "money value is uninitialised")
	}
	if m.units == math.MinInt64 {
		return Money{}, overflow("negating %s", m)
	}
	return Money{units: -m.units, currency: m.currency}, nil
}

// Cmp compares m with other, returning -1, 0 or +1 as m is less than, equal to
// or greater than other.
//
// Both values must be initialised and share a currency: amounts in different
// currencies are not ordered.
func (m Money) Cmp(other Money) (int, error) {
	if err := m.compatible(other); err != nil {
		return 0, err
	}
	return cmp.Compare(m.units, other.units), nil
}

// Equal reports whether m and other are the same amount in the same currency.
//
// Unlike [Money.Cmp] it does not report an error for mismatched currencies:
// two amounts in different currencies are simply not equal. Uninitialised
// values are never equal to anything, including each other.
func (m Money) Equal(other Money) bool {
	if m.IsUninitialized() || other.IsUninitialized() {
		return false
	}
	return m == other
}

// maxAmountLength is the longest canonical form any amount can take: a sign,
// the seventeen digits of the largest representable major part, the point and
// the two fraction digits — "-92233720368547758.08".
const maxAmountLength = len("-92233720368547758.08")

// Amount returns the canonical decimal form of the amount, without the
// currency: "25.00", "0.00", "-25.00".
//
// This is the exact string the external contract carries, and the exact string
// the idempotency hash is taken over.
func (m Money) Amount() string {
	var digits [maxAmountLength]byte
	return string(m.amountInto(&digits))
}

// amountInto renders the canonical decimal form into buf and returns the part
// of it that was filled.
//
// The buffer is the caller's, and a fixed-size array rather than a slice,
// because the longest amount is known at compile time: the rendering itself
// therefore allocates nothing, and only the caller deciding to keep the result
// — as a string, or appended to a larger buffer — costs anything.
//
// Digits are laid down from the right, which is the order division produces
// them in, so nothing has to be reversed afterwards.
func (m Money) amountInto(buf *[maxAmountLength]byte) []byte {
	// Take the magnitude through uint64 so the most negative value, whose
	// positive counterpart is unrepresentable, still renders correctly.
	magnitude := uint64(m.units)
	if m.units < 0 {
		magnitude = -magnitude
	}
	major, minor := magnitude/minorPerMajor, magnitude%minorPerMajor

	i := len(buf)
	for range Scale {
		i--
		buf[i] = '0' + byte(minor%10)
		minor /= 10
	}
	i--
	buf[i] = '.'
	for {
		i--
		buf[i] = '0' + byte(major%10)
		major /= 10
		if major == 0 {
			break
		}
	}
	if m.units < 0 {
		i--
		buf[i] = '-'
	}
	return buf[i:]
}

// AppendAmount appends the canonical decimal form of the amount to dst and
// returns the extended buffer, in the manner of [strconv.AppendInt].
//
// It exists for callers assembling a larger buffer — the idempotency payload is
// the one that matters — which would otherwise take an intermediate string from
// [Money.Amount] only to copy it in and throw it away.
func (m Money) AppendAmount(dst []byte) []byte {
	var digits [maxAmountLength]byte
	return append(dst, m.amountInto(&digits)...)
}

// String returns the amount and its currency, for logs and test failures.
func (m Money) String() string {
	if m.IsUninitialized() {
		return "<uninitialised money>"
	}
	return m.Amount() + " " + m.currency.String()
}

// contract is the external representation of a monetary value, used to read
// one. Writing one goes through [Money.MarshalJSON], which assembles the object
// directly.
type contract struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// jsonOverhead is the length of the rendered object with both values empty,
// which is everything in it that is not a value.
const jsonOverhead = len(`{"amount":"","currency":""}`)

// MarshalJSON renders the value as {"amount":"25.00","currency":"BRL"}.
//
// Uninitialised and negative values are refused. The contract has no
// representation for a negative amount, and every amount that crosses the
// boundary — a movement, a balance, an event payload — is non-negative by
// construction.
//
// The object is assembled directly rather than marshalled from [contract].
// Neither value can contain a character JSON would have to escape — an amount
// is digits and a point, a currency is three uppercase letters — so there is
// nothing for an encoder to decide, and the exact length of the result is known
// before the first byte is written.
func (m Money) MarshalJSON() ([]byte, error) {
	if m.IsUninitialized() {
		return nil, failure.New(failure.UninitializedValue, "money value is uninitialised")
	}
	if m.units < 0 {
		return nil, failure.New(failure.InvalidAmountFormat,
			"negative amount %s has no external representation", m)
	}

	var digits [maxAmountLength]byte
	amount := m.amountInto(&digits)

	out := make([]byte, 0, jsonOverhead+len(amount)+len(m.currency.code))
	out = append(out, `{"amount":"`...)
	out = append(out, amount...)
	out = append(out, `","currency":"`...)
	out = append(out, m.currency.code...)
	return append(out, `"}`...), nil
}

// UnmarshalJSON reads {"amount":"25.00","currency":"BRL"} through the same
// validation [Parse] applies. A payload is refused for naming a field this type
// does not have, and for naming one of them twice.
//
// The repeat matters as much as the unknown key. Decoding through encoding/json
// v1, {"amount":"1.00","amount":"999999.00"} is accepted and the last value
// wins, so two amounts that differ by six orders of magnitude arrive as one
// without anything to detect the repeat. v2 rejects duplicate names outright,
// which is why the decoding goes through it.
func (m *Money) UnmarshalJSON(data []byte) error {
	var raw contract
	if err := json.Unmarshal(data, &raw, json.RejectUnknownMembers(true)); err != nil {
		return failure.Wrap(err, failure.InvalidFieldFormat, "money must be an object with amount and currency")
	}
	parsed, err := Parse(raw.Amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

// compatible reports whether m and other may take part in the same operation.
func (m Money) compatible(other Money) error {
	if m.IsUninitialized() || other.IsUninitialized() {
		return failure.New(failure.UninitializedValue, "money value is uninitialised")
	}
	if m.currency != other.currency {
		return failure.New(failure.CurrencyMismatch,
			"cannot combine %s with %s", m.currency, other.currency)
	}
	return nil
}

func overflow(format string, a ...any) error {
	return failure.New(failure.AmountOutOfRange, "overflow "+format, a...)
}

// parseAmount validates the canonical decimal form and returns its minor units.
func parseAmount(s string) (int64, error) {
	if s == "" {
		return 0, failure.New(failure.InvalidAmountFormat, "amount must not be empty")
	}

	// A single pass over the bytes rejects signs, exponents, separators,
	// whitespace, NaN, Infinity and every non-ASCII digit at once: only ASCII
	// digits and one decimal point survive it.
	dot := -1
	for i := range len(s) {
		switch ch := s[i]; {
		case ch >= '0' && ch <= '9':
		case ch == '.' && dot < 0:
			dot = i
		default:
			return 0, failure.New(failure.InvalidAmountFormat,
				"amount %q must be a plain non-negative decimal", s)
		}
	}

	whole, frac := s, ""
	if dot >= 0 {
		whole, frac = s[:dot], s[dot+1:]
	}
	switch {
	case whole == "":
		return 0, failure.New(failure.InvalidAmountFormat, "amount %q has no integer part", s)
	case dot >= 0 && frac == "":
		return 0, failure.New(failure.InvalidAmountFormat, "amount %q has no fraction digits", s)
	case len(whole) > 1 && whole[0] == '0':
		return 0, failure.New(failure.InvalidAmountFormat, "amount %q has a leading zero", s)
	case len(frac) != Scale:
		return 0, failure.New(failure.InvalidAmountScale,
			"amount %q must carry exactly %d decimal places", s, Scale)
	}

	major, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, failure.Wrap(err, failure.AmountOutOfRange,
			"amount %q exceeds the representable range", s)
	}
	minor, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		// Unreachable: frac is exactly Scale ASCII digits.
		return 0, failure.Wrap(err, failure.InvalidAmountFormat, "amount %q has invalid fraction digits", s)
	}
	if major > (math.MaxInt64-minor)/minorPerMajor {
		return 0, failure.New(failure.AmountOutOfRange,
			"amount %q exceeds the representable range", s)
	}
	return major*minorPerMajor + minor, nil
}
