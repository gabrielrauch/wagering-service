package money

import "github.com/gabrielrauch/wagering-service/internal/domain/failure"

// currencyCodeLength is the length of an ISO 4217 alphabetic code.
const currencyCodeLength = 3

// Currency is an ISO 4217 alphabetic currency code.
//
// The code is held in an unexported field, so a Currency cannot be produced
// outside this package by conversion: [ParseCurrency] is the only way in, and
// every Currency in existence has therefore been validated. Its zero value is
// invalid and is rejected wherever it appears.
//
// Validation is on form alone — three uppercase ASCII letters. No list of
// supported currencies is kept, because the service has no notion of one: a
// wallet is denominated in whatever currency it was opened with, and every
// movement must match it.
//
// Matching is case-strict. "brl" is refused rather than upcased, so each
// currency has exactly one spelling and no normalisation stands between a
// submitted payload and its idempotency hash.
type Currency struct {
	code string
}

// ParseCurrency validates an ISO 4217 alphabetic code and returns the currency
// it names.
//
// The input must be exactly three uppercase ASCII letters. Lowercase input,
// surrounding whitespace, digits and non-ASCII letters are all refused with
// [failure.UnsupportedCurrency].
func ParseCurrency(s string) (Currency, error) {
	if s == "" {
		return Currency{}, failure.New(failure.UnsupportedCurrency, "currency must not be empty")
	}
	if len(s) != currencyCodeLength {
		return Currency{}, failure.New(failure.UnsupportedCurrency,
			"currency %q must be exactly %d characters", s, currencyCodeLength)
	}
	for i := range len(s) {
		if ch := s[i]; ch < 'A' || ch > 'Z' {
			return Currency{}, failure.New(failure.UnsupportedCurrency,
				"currency %q must be three uppercase ASCII letters", s)
		}
	}
	return Currency{code: s}, nil
}

// IsZero reports whether c is the zero value, which names no currency.
func (c Currency) IsZero() bool { return c.code == "" }

// String returns the ISO 4217 code, or the empty string for the zero value.
func (c Currency) String() string { return c.code }
