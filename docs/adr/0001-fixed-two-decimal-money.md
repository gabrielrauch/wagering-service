# Money is fixed at two decimal places, with no currency table

`Money` holds an `int64` of minor units at a fixed scale of two, and `ParseCurrency`
validates an ISO 4217 code on form alone — three uppercase ASCII letters — against no
list of any kind.

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

What the type does carry is the currency itself. Arithmetic and comparison refuse to cross
currencies, a wallet is identified by player and currency, and every movement must match
its wallet — so the model never assumes BRL even though only BRL flows through it.
