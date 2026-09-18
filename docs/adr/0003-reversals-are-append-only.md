# Reversals are append-only; "active reversal" is derived, never stored

A reversed transaction is never written to. A `REFUND` or `ROLLBACK` is simply a new
transaction pointing at the one it undoes, and whether a transaction is currently reversed
is computed from the reversals pointing at it:

> a transaction has an active reversal when some processed reversal points at it and has
> not itself been reversed.

## The rule this implements

At most one **active** reversal per transaction. A bet already refunded cannot also be
rolled back — that would return the same debit twice. But `BET → REFUND → ROLLBACK of that
refund` nets to the bet standing debited, and releases the bet to be reversed again.

A `ROLLBACK` can never itself be reversed, so one applied straight to a bet holds it
permanently, while a `REFUND` can always be undone. The asymmetry is deliberate: a rollback
is a provider asserting the operation never happened; a refund is a business decision that
may be revisited.

## Considered and rejected: a mutable slot

Store `reversedBy` on the reversed transaction, set it when reversed and **clear** it when
its reverser is itself reversed. Smaller inputs — the processor would need only the
immediate reference.

It was rejected for three reasons:

1. It writes to a `PROCESSED` transaction, colliding with the rule that a terminal
   transaction never changes. Under the append-only model the question does not arise.
2. The invariant stops being a unique index, because the column is cleared and re-set, so
   it needs a conditional index plus optimistic locking on rows believed terminal.
3. "Released again" becomes state maintained by hand across two transactions per rollback,
   rather than something that falls out of the data.

## Why the derived form is cheap

The recursion is provably bounded at two levels. A rollback cannot be reversed, and a
refund can only be reversed by a rollback, so asking "does this bet have an active
reversal?" never needs to walk an open-ended chain. `ReferenceView` therefore carries the
reference plus its reversals, each flagged with whether it has been reversed — and that is
always enough.

`ReferenceView.ActiveReversal` also ignores anything that is not a reversal. A repository
implementing "find transactions referencing X" naturally returns wins too, and a win must
not hold the bet it pays out on.

## Consequences for the layer that stores this

- Nothing that stores a reversal ever updates a prior transaction. Reversal history is
  purely additive and fully auditable.
- Only `PROCESSED` reversals hold a reference. A rejected or still-pending one must not
  block a legitimate retry, so "reserved" is not a state.
- Nothing *in the domain* prevents two concurrent rollbacks of one bet from both seeing a
  free slot — except that every reversal moves money, so both must write the same wallet,
  and the wallet's **version** is the serialisation point. The loser retries and sees the
  winner. The version field is load-bearing for reversal correctness, not just bookkeeping.

  > **Amended by ADR-0007.** The schema now decides this directly, and more strongly.
  > `wagering.active_reversal` has `reference_id` as its **primary key**, so a second
  > reversal of one bet collides on that key rather than on the wallet — which means the
  > guarantee no longer depends on both reversals moving money.
  >
  > That distinction is not hypothetical. `TestTwoReversalsCannotRaceForOneReference`
  > races a refund and a rollback of the same bet where neither writes a ledger entry and
  > neither touches the balance, so the wallet version never comes into it, and the
  > reference is still held exactly once. The reasoning above remains an accurate
  > description of what the domain can guarantee on its own, with no database underneath
  > it.
- `BET → REFUND → ROLLBACK → REFUND → …` is unbounded, each pair netting zero. Every step
  is individually valid, audited, and leaves the balance correct, so no cap is imposed; a
  cap would be a number with no business meaning.
