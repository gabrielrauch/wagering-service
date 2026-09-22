package money

import (
	"slices"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// currencyCodeLength is the length of an ISO 4217 alphabetic code.
const currencyCodeLength = 3

// supportedCurrencies is every ISO 4217 alphabetic code whose minor unit is
// exactly two digits: precisely the currencies a [Money] at [Scale] can hold
// without misrepresenting them. It is the gate, and the schema's
// wagering.currency_code domain, which checks form alone, is not.
//
// The list is closed by arithmetic rather than by policy. A code is absent
// because scale 2 would misstate it — JPY carries no minor unit, KWD three,
// CLF four — or because ISO 4217 says it names no currency: XXX, the testing
// code XTS, the precious metals and the IMF's XDR. ISO 4217's fund codes (the
// Bolivian Mvdol, the WIR Euro and Franc, and their kin) are left out too:
// they carry two digits but are units of account, and no wallet is denominated
// in one. Adding an entry is only ever right for a two-digit currency; anything
// else needs a per-currency scale, which is a different design (ADR-0001).
//
// The slice is sorted, because [ParseCurrency] bisects it, and a test pins the
// ordering along with the uniqueness and the form of every entry.
var supportedCurrencies = []string{
	"AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN",
	"BAM", "BBD", "BDT", "BGN", "BMD", "BND", "BOB", "BRL", "BSD", "BTN", "BWP", "BYN", "BZD",
	"CAD", "CDF", "CHF", "CNY", "COP", "CRC", "CUP", "CVE", "CZK",
	"DKK", "DOP", "DZD",
	"EGP", "ERN", "ETB", "EUR",
	"FJD", "FKP",
	"GBP", "GEL", "GHS", "GIP", "GMD", "GTQ", "GYD",
	"HKD", "HNL", "HTG", "HUF",
	"IDR", "ILS", "INR", "IRR",
	"JMD",
	"KES", "KGS", "KHR", "KPW", "KYD", "KZT",
	"LAK", "LBP", "LKR", "LRD", "LSL",
	"MAD", "MDL", "MGA", "MKD", "MMK", "MNT", "MOP", "MRU", "MUR", "MVR", "MWK", "MXN", "MYR", "MZN",
	"NAD", "NGN", "NIO", "NOK", "NPR", "NZD",
	"PAB", "PEN", "PGK", "PHP", "PKR", "PLN",
	"QAR",
	"RON", "RSD", "RUB",
	"SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP", "SLE", "SOS", "SRD", "SSP", "STN", "SVC", "SYP", "SZL",
	"THB", "TJS", "TMT", "TOP", "TRY", "TTD", "TWD", "TZS",
	"UAH", "USD", "UYU", "UZS",
	"VED", "VES",
	"WST",
	"XCD", "XCG",
	"YER",
	"ZAR", "ZMW", "ZWG",
}

// Currency is an ISO 4217 alphabetic currency code that [Money]'s fixed scale
// of two can represent.
//
// The code is held in an unexported field, so a Currency cannot be produced
// outside this package by conversion: [ParseCurrency] is the only way in, and
// every Currency in existence has therefore been validated. Its zero value is
// invalid and is rejected wherever it appears.
//
// Validation is in two steps. The form comes first — three uppercase ASCII
// letters — and then the code must be one of the currencies whose ISO 4217
// minor unit is exactly two digits. That second step is what keeps the scale
// honest: a JPY amount held at scale 2 would be wrong by a factor of a
// hundred, so JPY is refused rather than misrepresented. The service still has
// no notion of a configured or preferred currency — a wallet is denominated in
// whatever it was opened with, and every movement must match it — but the set
// it can be opened with is the set this scale can hold.
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
// The input must be exactly three uppercase ASCII letters naming a currency
// whose ISO 4217 minor unit is two digits. Lowercase input, surrounding
// whitespace, digits, non-ASCII letters, codes that name no currency and
// currencies with zero, three or four minor-unit digits are all refused with
// [failure.UnsupportedCurrency]. The refusal says which rule was broken and
// never lists what would have been accepted.
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
	if _, found := slices.BinarySearch(supportedCurrencies, s); !found {
		return Currency{}, failure.New(failure.UnsupportedCurrency,
			"currency %q is not a supported ISO 4217 currency with two minor-unit digits", s)
	}
	return Currency{code: s}, nil
}

// IsZero reports whether c is the zero value, which names no currency.
func (c Currency) IsZero() bool { return c.code == "" }

// String returns the ISO 4217 code, or the empty string for the zero value.
func (c Currency) String() string { return c.code }
