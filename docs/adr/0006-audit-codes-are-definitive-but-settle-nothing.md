# Audit codes are definitive but settle nothing

`failure.Code` carries a second axis, `Code.Audit()`, marking the codes that report on
stored state rather than on a submission. There is one today: `LEDGER_BALANCE_MISMATCH`,
raised when a wallet's stored balance disagrees with its ledger.

## Why this needs recording

ADR-0002 says the catalogue splits in two, and that the split is exactly the question
*"did we persist a payload hash under this idempotency key?"*. A reconciliation finding
answers neither half — there is no submission, no payload and no key — so a reader will
reasonably ask why it is in the catalogue at all, and why it is filed as definitive when
nothing was settled.

## Why it is in the catalogue

Because leaving it out was worse. `Reconcile` returns five findings. Four are built with
`failure.New` and carry a code; the disagreement — the one the function exists to detect —
was a bare struct carrying none. A caller switching on `failure.CodeOf` therefore handled
the four lesser findings and fell into its unknown branch for the most serious one, which
is the branch least likely to have been thought about.

"Every refusal names a documented code, reachable through `errors.Is` and `errors.As`" has
to hold across the whole surface or it is not a contract. It held everywhere except the one
place that mattered most.

## Considered and rejected: filing it under Definitive, with no second axis

Simpler — one classification, one predicate, no new concept. It was rejected because
`WagerTransaction.Reject` gates on `Code.Definitive()`, so the code would have become a
legal way to settle a wager transaction: a `REJECTED` row naming a reason no provider ever
caused, with `WagerTransactionRejected` published under it and the provider's idempotency
key bound to it for good.

That is verified rather than assumed. Removing the guard makes
`Reject(LEDGER_BALANCE_MISMATCH)` return nil and move the transaction to `REJECTED`.

The deeper objection is that "definitive" would have had to mean two things at once —
*binds the idempotency key* for every other member, and *there is no key* for this one. A
predicate that means different things for different members of its own domain has stopped
being a classification. The second axis costs a map and a method, and keeps the first one
saying exactly what it has always said.

## Considered and rejected: reusing an existing code

Nothing in the catalogue describes it. The nearest by shape, `INVALID_FIELD_FORMAT`, is
correctable — which would report a corrupt ledger as something a provider may repair and
resubmit.

  > **Amended by ADR-0013.** The pairing rejected here now exists, deliberately, for a
  > neighbouring finding. `Reconcile` reports structural corruption as well as a balance that
  > does not add up — a duplicated ledger entry, an entry belonging to another wallet, one in
  > the wrong currency — and those keep their own codes rather than being stamped with this
  > one, so a duplicate leaves `Wallets.Reconcile` as `Class=AUDIT`,
  > `Code=INVALID_FIELD_FORMAT`, `Field=transactionId`.
  >
  > What answers the objection above is the **class**, set explicitly rather than derived from
  > the code: a caller acting on `Class` is told this is an operator's finding and never that
  > a provider may resubmit. A caller reading the code alone is not, and `failure.Correctable`
  > on such an error returns `true` — which is why ADR-0013 states the guard rule.
  >
  > The decision recorded here is untouched. `LEDGER_BALANCE_MISMATCH` is still the only code
  > that carries "this is about stored state" in the code itself, it is still what the balance
  > disagreement reports under, and it is still refused by `Reject`.

## Consequences

- Adding a code now means answering two questions rather than one. `correctable` and
  `audit` are both explicit maps, membership in neither is inferred, and the catalogue's
  totality test covers both — so a new code cannot become an audit finding by accident, nor
  quietly fail to be one.

- A caller deciding whether a code may settle a transaction needs both halves: definitive
  **and** not an audit code. `Correctable()` alone is no longer sufficient to answer it.

- `Reject` and rehydration both refuse audit codes, through one shared check, so the front
  door and the storage door cannot answer the question differently.

- `ReconciliationError` derives its code rather than storing it in a field, so a finding
  built anywhere carries it. A stored code would be empty in every value that did not come
  from `Reconcile`, which is a guarantee that lasts until the first caller who needs to
  build one.

- The axis is open to more members but is not expected to grow quickly. It describes
  findings about state this service already holds, which is a small category by design:
  reconciliation is the only thing in the domain that inspects stored state rather than
  deciding about an operation.
