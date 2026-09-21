# A movement transaction writes one wallet, and takes its locks in one order

Every transaction that moves money has the same write set: one wallet's rows, plus that
wallet's outbox sequence counter. It takes them in one order:

    inbox -> wallet lock -> wager_transaction -> (trigger: active_reversal) -> outbox

Appending to the outbox is the last statement of every callback. The resume worker finds its
work with a **plain SELECT** that takes no row lock, locks the wallet first, and only then
re-reads the row under its own lock.

## Why this needs recording

The outbox right next door claims work with `FOR UPDATE ... SKIP LOCKED`, which is the standard
way to hand rows to competing workers. `TransactionStore.NextDue` looks like the same
problem and deliberately does not do the same thing, so it reads like an oversight — and
"optimising" it to `SKIP LOCKED` reintroduces a deadlock that is not rare.

## What it buys

Claiming the parked row first and then wanting its wallet closes a cycle with a submission that
holds that wallet and wants the row:

| | holds | wants |
|---|---|---|
| Resume | parked row `P` | wallet `W` |
| Submit | wallet `W` | parked row `P`, to wake it |

That is `40P01`, and it was demonstrated rather than predicted. It is also not a corner case.
`referenceAgrees` requires a reversal and its reference to agree on the wallet, so the
operation that wakes `P` is on `W` **by construction** — the two transactions are the same
wallet every time, not occasionally.

Taking the wallet first makes the cycle unrepresentable: both parties want `W` first, so one
waits and neither holds anything the other needs. The unlocked SELECT is what makes that
possible, and the re-read under the lock (`ClaimForUpdate`) is what makes it safe — between the
two, the row may have settled or been taken by another worker, and under READ COMMITTED the
re-read sees that.

The outbox is the second half of the rule, and the less obvious half. The per-aggregate
sequence counter is a row that every commit touching a wallet must update, which makes it a
**second per-wallet serialisation point**. ADR-0008 removed a lock *upgrade* there; it did not
remove the obligation to take the wallet and the counter in a fixed order. Taking the counter
first while another command holds the wallet and wants the counter is the same deadlock with
different rows.

## Considered and rejected

`FOR UPDATE SKIP LOCKED` on the due query, matching the outbox. It is the right tool when the
claimed row is the whole write set, which is true for the outbox and false here: a parked
operation cannot be carried forward without touching its wallet.

Ordering by wallet id across all wallets, the usual answer to lock-ordering deadlocks. It costs
nothing here because the write set is already one wallet — so the cheaper rule, "one wallet,
and take it first", is the one worth stating.

## Consequence

`TransactionStore.MakeDue` is scoped to a wallet the caller already holds the lock on. Waking a
waiter on another wallet would write outside the declared write set, which is the thing this
whole arrangement exists to prevent — so an operation that becomes available wakes only the
waiters on its own wallet, and waiters elsewhere are found by their own schedule instead.

`Resume` therefore claims at most one operation per call, and reports `Claimed: false` when
nothing was due or the candidate stopped being due before the lock was taken. Neither is a
failure, and a worker that alerted on them would alert on its own normal operation.

That bound is in the port and not only in this document: `NextDue` returns one candidate, or
`(nil, nil)` when nothing is due. A slice with a limit would advertise a batch no caller may
take — the second candidate belongs to a second wallet, and the write set is one — and it
would spell absence as `len == 0` where every other reader here spells it `(nil, nil)`.

Ties are broken by transaction id, because `MakeDue` stamps every operation it wakes with a
single instant: two waiters woken by one commit are due at exactly the same time, and ordering
on the schedule alone leaves the choice to whatever order the rows are returned in.
`wager_transaction_due_idx` is already `(reference_next_attempt_at, id)`, so a total order was
available before it was asked for — the signature caught up with the index rather than the
other way round.

Nothing but SQL runs inside the callback. A hook into an adapter — a metrics push, an error
tracker, a divergence observer — is called only once the transaction has closed. `Resume`
records a failure to carry a parked operation forward in a `carriedForwardDefect` and calls
`DefectObserver.CannotCarryForward` after `WithinMovement` returns; `Wallets.Reconcile` calls
`ReconciliationObserver.WalletDiverged` after its snapshot closes. Calling either from inside
would make the length of the wallet lock an adapter's decision, and would announce a write that
has not committed and may yet roll back.
`TestTheCarryForwardDefectIsReportedAfterTheTransactionCommits` counts the store's commits at
the moment of the call, so this is asserted rather than described.

None of this is testable with in-memory fakes. Lock ordering, deadlock-freedom, `SKIP LOCKED`
and the COMMIT-time deferred triggers are container tests or nothing, and the unit tests say so
rather than pretending to cover them.
