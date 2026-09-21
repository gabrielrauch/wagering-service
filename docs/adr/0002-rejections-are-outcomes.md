# A business rejection is a return value, not a Go error

`Processor.Submit` and `Processor.Continue` return `(Outcome, error)`. A non-nil error means
the submission was malformed and nothing was recorded. A nil error means a
`WagerTransaction` exists and reached `PROCESSED`, `REJECTED` or `PENDING_REFERENCE` — so
insufficient funds, a currency mismatch, an already-reversed bet and a balance that cannot
grow all come back with `err == nil`.

## Why this needs recording

It reads backwards to a Go audience. `InsufficientFunds` looks exactly like an error, and
the obvious implementation returns one. A reviewer will assume it was an oversight.

## What it buys

Every failure carries a `failure.Code`, and `Code.Correctable()` splits them in two. That
split turns out to be exactly the question *"did we persist a payload hash under this
idempotency key?"*:

- **Correctable** — no transaction exists, so the key is still free. A provider can repair
  the payload and resubmit under the same key.
- **Definitive** — a rejected transaction is persisted, `WagerTransactionRejected` is
  emitted, and the key is bound to that payload for good. Resubmitting a corrected payload
  under it is `IDEMPOTENCY_PAYLOAD_CONFLICT`.

A definitive rejection is therefore a durable business fact that must be written down, an
event, and an answer the provider will get again on every retry. Returning it as an
`error` invites a caller to log it and move on, which loses the record and leaves the key
unbound — so the next retry would be processed as if it were new.

A second axis, `Code.Audit()`, marks the few codes that report on stored state rather than
on a submission. They are definitive — there is no payload to repair — but they settle
nothing, because no operation was in flight. See ADR-0006; the axis exists so that
"definitive" keeps meaning exactly one thing here.

The axis marks codes, not findings, and the two are not the same set. Reconciliation reads
stored state and can report under an ordinary catalogue code — two ledger entries for one
transaction is `INVALID_FIELD_FORMAT`, which is correctable — so the application layer decides
that such a finding is an audit finding from where it was found rather than from the code it
carries, and says so in its class. `Code.Audit()` stays a property of codes and stays the gate
`Reject` uses. See ADR-0013.

## Considered and rejected

Returning everything as an `error` and letting the caller decide what to persist. Simpler
signature, but it makes "this rejection must be stored" a convention rather than something
the type system carries, and the convention is the part that matters.

## Consequence

Callers must check `Outcome.Transaction.Status()`, not just `err`. An `err == nil` return
does not mean the money moved. `Outcome.Recorded()` reports whether an outcome carries a
transaction at all: opening a wallet at zero is the one case that carries none alongside a
nil error.

The rule is enforced rather than documented. `WagerTransaction.Reject` refuses a correctable
code outright, so a transaction cannot be persisted under one, and `RehydrateWagerTransaction`
refuses to load a stored row that names one — otherwise writing the row directly and reading
it back would be the way around the rule. Both refuse audit codes for the opposite reason.

The split also decides which side of the line a failure falls on, and the answer is *could
the provider have caused this?*

- A movement that overflows was caused by the amount they sent, so it settles: it is
  `BALANCE_OUT_OF_RANGE`, definitive, rather than the correctable `AMOUNT_OUT_OF_RANGE` that
  the underlying arithmetic reports. The amount is well formed and the balance is what it is,
  so there is nothing to repair.
- A wallet belonging to another player cannot have been caused by them — the command names a
  player and choosing the wallet is this service's work — so it refuses before a transaction
  exists, and the key stays free. See ADR-0005.
