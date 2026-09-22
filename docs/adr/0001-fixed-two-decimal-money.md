# Money is fixed at two decimal places, with no currency table

`Money` holds an `int64` of minor units at a fixed scale of two, and `ParseCurrency`
validates an ISO 4217 code on form alone — three uppercase ASCII letters — against no
list of any kind.

  > **Amended 2026-09-22.** The scale decision stands; the "no list" half does not.
  > `ParseCurrency` now also checks the code against the list of currencies that scale 2
  > can hold. See the amendment at the end.

## Why this needs recording

"A fixed scale of two decimal places" and "ISO 4217 currency codes" are in tension.
ISO 4217 is not uniformly two-exponent: JPY, KRW, VND, CLP, ISK and the African franc
family carry no minor unit at all; the Gulf dinars — BHD, IQD, JOD, KWD, LYD, OMR, TND —
carry three; CLF and UYW carry four. Roughly a tenth of the standard cannot be held
faithfully at scale 2.

A reader who sees `ParseCurrency("JPY")` succeed will wonder whether anyone noticed.

## What we considered

**A full per-currency exponent table.** Every ISO 4217 code accepted at its true scale,
so ¥2500 is 2500 minor units rather than 250000. Correct, and roughly 180 lines of table,
a parse grammar whose fraction-digit count varies by currency, per-currency numeric
limits, and a `Currency` that must be unforgeable or the table can be lied to.

**A closed allowlist of supported currencies.** Fails closed on anything unsupported,
but the requirements say there is no notion of a supported-currency list, and a wallet is
denominated in whatever it was opened with.

## What we chose, and the consequence

Neither. The requirements say three separate times that only BRL need work — there is no
requirement to support other currencies, operating only in BRL in the main flows is
explicitly allowed, and the scale is stated as fixed at two. BRL is a two-exponent
currency, so every specified example is served exactly.

The consequence, stated plainly: **currencies whose ISO 4217 exponent is not 2 are out of
scope.** Opening a `JPY` wallet would succeed and would misrepresent every amount in it by
a factor of 100. Supporting them means a per-currency scale, which changes parsing,
rendering, the limits and the persisted representation — it is not a configuration change,
which is the real reason no list is kept: a list would imply currencies can be added, and
most cannot.

  > **Amended 2026-09-22.** Opening a `JPY` wallet now fails with `UNSUPPORTED_CURRENCY`
  > instead of succeeding and misstating every amount in it. The objection to a list is
  > answered below, not overruled.

What the type does carry is the currency itself. Arithmetic and comparison refuse to cross
currencies, a wallet is identified by player and currency, and every movement must match
its wallet — so the model never assumes BRL even though only BRL flows through it.

## Amendment (2026-09-22): the list is kept after all

`ParseCurrency` now accepts a code only if it appears in `supportedCurrencies`, an
unexported, sorted list in `internal/domain/money/currency.go` of every ISO 4217 alphabetic
code whose minor unit is exactly two digits. The form check runs first and is unchanged. A
well-formed code that is not on the list is refused with `UNSUPPORTED_CURRENCY`, and the
refusal names the rule that was broken rather than reciting the list.

This is the closed allowlist the record above rejected, and the objection was that a list
would imply currencies can be added when most cannot. The objection does not survive
saying what the list is. It is not a list of currencies an operator chose to support; it is
exactly the set of currencies a fixed scale of two can hold faithfully. Nothing can be added
to it that the scale does not already serve, and nothing on it needs the per-currency scale
the exponent table would have required. So the list resolves the objection rather than
ignoring it: the decision that the scale is fixed at two stands, and the list is that
decision written out as a set. The title of this record is now half true — there is a list,
though not a table, since it carries no exponent and no rounding rule.

What changes for a reader of the sections above:

- `ParseCurrency("JPY")` fails. A `JPY` wallet cannot be opened, so no amount is ever held
  at the wrong scale; the consequence stated above has become a refusal at the boundary.
  KWD, CLF and their kin are refused the same way.
- `ABC` and `XXX` fail too. Three uppercase letters that named no currency were accepted
  before. The specification asks for an ISO 4217 code, and that is now what is checked.
- The schema's `wagering.currency_code` domain stays form-only, `^[A-Z]{3}$`. It cannot know
  which codes carry two digits without carrying the list itself, and a list kept in two
  places drifts. The domain type is the gate and the schema domain is the coarser net;
  `TestCurrencyCodeDomain` records the values on which they differ, in that one direction
  only.
- ISO 4217's fund codes — BOV, CHE, CHW, COU, MXV, USN, UYI — are not on the list. Most
  carry two digits, but they are units of account, not money a wallet is denominated in.

What does not change: there is still no per-currency exponent, no rounding rule and no
notion of a configured or preferred currency. A wallet is denominated in whatever it was
opened with, and every movement must match it. The day a currency with a different exponent
is wanted, this amendment is not where it goes — that is the first alternative above, and
it remains a change to parsing, rendering, the limits and the persisted representation.
