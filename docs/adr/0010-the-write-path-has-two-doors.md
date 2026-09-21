# The write path has two doors, not five ports

The application layer declares readers for wallets, wager transactions, the ledger, the inbox
and the outbox, but it does not declare five matching writers. Every write that settles an
operation goes through one of two functions on the repository bundle: `Repos.Open`, which
creates a wallet together with its opening, and `Repos.Settle`, which writes what an outcome
produced — the transaction's new state, and the wallet and ledger entry when money moved.

## Why this needs recording

The brief asked for "wallet, transaction, ledger, inbox and outbox persistence" ports, and the
obvious reading is five interfaces with `Save` on each. A reviewer will see two function fields
on a struct where they expected symmetry, and assume it was laziness.

## What it buys

The three rows a movement produces are only correct together, and the schema pairs two of the
three. `wallet_matches_ledger` and `ledger_matches_wallet` are deferred constraints that fire
at COMMIT, so a wallet cannot move without its entry and an entry cannot exist without the
matching wallet. Nothing pairs the **wager transaction** with either.

That gap is reachable, and was demonstrated rather than reasoned about: a `BET` can be written
at `PROCESSED`, reporting a `result_balance_minor` the wallet never held, with no ledger entry
and no change to the balance — and `reconcile_wallet()` still returns true, because the wallet
and the ledger agree with each other perfectly. The operation is a lie and every check
downstream passes.

Five independent writers make "call all three, with consistent arguments" a convention, and a
convention is exactly what produces that row. One door makes it structural: there is no call
that writes a transaction without also being handed the outcome that says whether an entry
belongs with it.

The narrowing goes further than the signature suggests. `Settle` is **not** given the wallet.
A ledger entry already carries `balanceAfter`, `walletVersion` and `createdAt`, so the wallet's
new state is read out of the entry rather than passed beside it — which removes the last value
that could disagree. There is no argument an adapter could get wrong, because there is no
argument.

## Considered and rejected

Five write ports plus a test asserting they are always called together. The test can only
assert it for the call sites that exist when it is written, and the failure it is guarding
against is a call site somebody adds later.

A third deferred constraint pairing the transaction with the ledger. It would have to be
conditional on kind — a `LOSS` and every rejection write no entry — which makes it a predicate
over four columns and a status, evaluated at COMMIT, reporting a violation with no useful
context. ADR-0007 took the opposite approach for the same reason: prefer a shape that cannot
express the wrong thing over a rule that catches it afterwards.

## Consequence

The Postgres adapter implements two functions rather than five write methods, and they are the
only places an `INSERT` or `UPDATE` against `wallet` or `wallet_ledger_entry` appears — the
only places, therefore, where a transaction's new state is written together with the rows that
must agree with it. `wager_transaction` is also written alone, by `TransactionStore.Record`,
`MakeDue`, `Reschedule` and `Fail`; none of those touches a balance. `Record` claims the two
keys before any money work, and the other three move only the schedule or the terminal status,
so none of them can produce the row this ADR exists to make unrepresentable. `Repos.Inbox.Record` and `Repos.Outbox.Append` remain ordinary
methods: neither participates in the invariant, and both are genuinely independent writes.

`Settle` also carries the schedule, as `nextAttemptAt time.Time`, set exactly when the outcome
is `PENDING_REFERENCE`. `wager_transaction_only_waiting_is_scheduled` is an equivalence, so any
write that leaves `PENDING_REFERENCE` must clear the schedule in the same statement — and that
is only enforceable if one statement writes both.

It crosses as a value rather than as a `*time.Time`, and absence is the zero time. That is the
convention `WagerTransaction.ReferenceDeadline` and `BackoffPolicy.next` already use, so the
pointer was the odd one out in its own neighbourhood; it also left a pointee the store could
retain and the caller could go on reading, which a value makes unrepresentable rather than
merely discouraged.
