# A rejection is never inferred from an error

ADR-0002 settled that a business rejection is a return value rather than a Go error. This
amends it for the application boundary, where the same fact has to survive one more
translation: `app.ClassOf` maps any error to a class an HTTP handler or an SQS consumer can act
on, and `Rejected` is not among the classes it can ever produce.

A rejection exists when the processor returned a **nil** error and
`outcome.Transaction.Status()` is `wagering.Rejected`. Every `failure.Code` arriving on an
error path means the submission never became anything.

## Why this needs recording

The mapping writes itself wrongly. Where the class is derived from the code at all — the
fallback `ClassOf` applies to an error nobody has classified — correctable means `Invalid` and
an audit code means `Audit`, so the obvious next line is "definitive means `Rejected`", and it
is wrong in a way that unit tests over the happy path will not catch.

Derivation is only half of `ClassOf`. A class set explicitly, where the context to set it
existed, is returned as it stands and beats the code: `Retryable`, `Unauthorized` and
`NotFound` are never derived from a code at all, and an audit finding is `Audit` over whatever
code it happens to carry, correctable ones included (ADR-0013).

## What it buys

`LEDGER_BALANCE_MISMATCH` is the proof, and it is not a contrived one. It is:

- **definitive**, because there is no payload to repair;
- an **audit** code, because it reports stored state rather than a submission (ADR-0006);
- what `wagering.Reconcile` returns when the books do not add up, and the only one of its
  findings that is an audit code — the four structural findings it can return first carry
  ordinary catalogue codes and reach the boundary under them (ADR-0013); and
- refused by `WagerTransaction.Reject`, so it can never be a transaction's failure code.

Under "definitive means `Rejected`", asking the service to reconcile a wallet whose books do
not balance returns a *rejected wager operation*. There is no operation. A provider is not
waiting. The audience is an operator, and the answer they need is that a stored balance
disagrees with its ledger — which is what `Audit` says and what `Rejected` hides.

Ordering the checks fixes the symptom and not the cause: putting `Audit()` before
`Definitive()` gets that one code right and leaves the rule wrong for the next definitive code
that reaches an error path. The rule has to be stated as a rule. An error carrying
`INSUFFICIENT_FUNDS` does not mean a bet was rejected; it means something asked a question
about insufficient funds and failed, and nothing was written. Those are different answers and
only one of them has a row behind it.

## Considered and rejected

Deriving the class from the code alone, with `Audit` checked first. Correct today, and correct
only for as long as nobody adds a definitive code that some helper can return. The failure mode
is that a caller is told an operation was rejected when none exists, which is the worst kind of
wrong answer here: it is plausible, it is specific, and it names an outcome with a durable
record that is not there.

## Consequence

`ClassOf` has eight classes and produces seven of them. `Rejected` is declared because callers
need to name it — an HTTP adapter maps it to its own response, distinct from a conflict and
distinct from a malformed request — but it is only ever reached by reading
`OperationResult.Status`, never by classifying an error.

`Conflict` usually carries a code and `Rejected` never appears, which is the other half of the
same split. The one conflict with no code is an external id already recorded under a different
key: the catalogue describes no outcome for it because nothing is persisted for it, and a
provider acts on the class rather than on a code invented for the occasion.

`WALLET_ALREADY_EXISTS` and `IDEMPOTENCY_PAYLOAD_CONFLICT` are both definitive and both
arrive as errors, because in both cases nothing new was persisted: something already recorded
says otherwise. They keep their codes, because a provider can act on them, and they are
classified apart from the codes that settle an operation.

`TestNoErrorEverClassifiesAsRejected` walks `failure.All()` and asserts the rule for every code
in the catalogue, so no code added later can acquire `Rejected` from the derivation. The
derivation is only half of `ClassOf`: a class set at a constructor is returned as it stands,
and what keeps `Rejected` out of those is that no constructor here names it and none should —
a rejection has no error to construct.
