# An audit finding is marked by its class, not by the code it carries

ADR-0006 added `Code.Audit()` to mark the codes that report on stored state rather than on a
submission, and said there was one: `LEDGER_BALANCE_MISMATCH`. That is still true of the
catalogue. It is not true of the findings.

`wagering.Reconcile` reports five things about stored state and only one of them carries an
audit code:

| finding | code | `Correctable()` | `Definitive()` | `Audit()` |
| --- | --- | --- | --- | --- |
| an entry that was never constructed | `UNINITIALIZED_VALUE` | **true** | false | false |
| an entry belonging to another wallet | `REFERENCE_MISMATCH` | false | true | false |
| two ledger entries for one transaction | `INVALID_FIELD_FORMAT` | **true** | false | false |
| an entry in the wrong currency | `CURRENCY_MISMATCH` | false | true | false |
| the balance does not add up | `LEDGER_BALANCE_MISMATCH` | false | true | **true** |

So `app.Wallets.Reconcile` decides what an audit finding is from **where it was found** — it
read stored state, so whatever it found is an operator's problem — and sets `Class: Audit`
explicitly. `auditFinding` keeps the finding's own code and field.

## Why this needs recording

The application layer used to stamp `LEDGER_BALANCE_MISMATCH` on every finding that was not a
`*ReconciliationError`, which made the table above invisible: everything left `Reconcile`
looking like an audit code, so class and code agreed and nobody had to choose between them.

They no longer agree, and the reason for preferring the class is not obvious from either side.
Worse, the pairing this produces — `Class=AUDIT` over a **correctable** code — is the exact
pairing ADR-0006 considered and rejected, in a section that is still correct about the thing it
was deciding. A reader who finds that section and this code will think one of them is a
mistake.

## Why the code cannot carry it instead

The obvious repair is to move the four structural codes into the `audit` map so the marker
lives on the domain error. It does not work, and the reason is worth writing down so it is not
attempted twice.

`CURRENCY_MISMATCH` and `REFERENCE_MISMATCH` are **live settling reasons on the submission
path**. `Processor` rejects a mismatched currency with the first (`processor.go:192`) and a
reference that does not agree with the second (`reference.go:126`, `:148`). `Reject` gates on
`checkSettlingCode`, which refuses audit codes — that gate is ADR-0006's whole point. Marking
either code audit would make the domain refuse legitimate business rejections, which is a
strictly worse failure than the one being fixed.

The only remaining route is four new codes — `LEDGER_ENTRY_DUPLICATED` and friends. That means
four additions to a published external contract, describing conditions `Reconcile` itself
documents as unreachable by construction, that no provider can act on and none will ever see.
ADR-0006 closes by saying the axis "is not expected to grow quickly"; quadrupling it for
findings with no external audience is not what it had in mind.

## Considered and rejected: keep stamping one code

It is what the code did, and it kept class and code in agreement. It was rejected because the
agreement was purchased by lying. The catalogue defines `LEDGER_BALANCE_MISMATCH` as a stored
balance disagreeing with its ledger summed, and `Reconcile` returns the four structural
findings *before* it sums anything — so an operator paged for a balance that is out goes
looking for money that is not missing, while the duplicated entry that was actually found is
visible only to somebody who reads past the code into the message.

## Consequences

- **`Class` is what a caller acts on, and it is authoritative.** `ClassOf` returns a classified
  error's own class without consulting its code, which is what makes this work at all.

- **`failure.Correctable` must not be consulted on an error already classified `Audit`.** It
  returns `true` for a duplicated ledger entry, which would read as "the provider may repair
  the payload and resubmit" for a finding that has no payload and no provider. Nothing in this
  repository does so — `ClassOf` reaches the code only for an error nobody has classified — but
  an adapter that unwraps to the code and asks the catalogue instead of reading the class will
  get an answer that is worse than useless. The rule is: **read the class; the code names what
  was found, not what to do about it.**

- **An `AUDIT` error may carry a definitive code that is not an audit code.** That is the same
  sharp edge from the other side, and it is why `Code.Audit()` and `Class == Audit` are
  deliberately not the same predicate and must not be used interchangeably.

- `Code.Audit()` is unchanged, stays a property of codes, and stays the gate `Reject` and
  rehydration use. This ADR does not touch the settling rule; it only records that the
  application layer's notion of an audit *finding* is broader than the catalogue's notion of an
  audit *code*.

- `TestAStructuralReconciliationFindingKeepsItsOwnCodeAndField` pins the pairing: a duplicated
  ledger entry leaves `Wallets.Reconcile` as `Class=AUDIT`, `Code=INVALID_FIELD_FORMAT`,
  `Field=transactionId`, and explicitly not as a balance mismatch.
