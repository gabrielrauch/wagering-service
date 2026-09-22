# Architecture

This document is the design and its reasoning. [`CONTEXT.md`](CONTEXT.md) is the
vocabulary it uses; [`docs/adr/`](docs/adr) holds the thirteen decisions that
have an alternative worth recording; [`docs/schema.md`](docs/schema.md) is the
schema in detail. [`README.md`](README.md) is how to run it.

| | |
|---|---|
| [Money](#money) | representation, limits, and the one currency assumption |
| [The write path](#the-write-path) | the transaction boundary, the library, two doors, locking |
| [Idempotency](#idempotency) | the key, the hash, and the four answers |
| [The state machine](#the-state-machine) | the statuses, and what counts as transient |
| [Pending references](#pending-references) | the wait budget and how parked work is carried forward |
| [Reversals](#reversals) | the single-reversal rule, and every combination |
| [The inbox and the outbox](#the-inbox-and-the-outbox) | claims, backoff, recovery |
| [SQS](#sqs) | the queues' parameters, and the ordering they buy |
| [Authentication and authorisation](#authentication-and-authorisation) | the model, and why Keycloak |
| [Composition and shutdown](#composition-and-shutdown) | Fx, start-up checks, the stop order |
| [The HTTP API](#the-http-api) | the status table, and every endpoint with a captured body |
| [Observability](#observability) | the dashboard, the metric catalogue, finding a trace |
| [Failure codes](#failure-codes) | twenty-five codes on two axes |
| [Interpretations](#interpretations) | forty-five decisions taken where the specification was absent |
| [Limitations](#limitations) | what is bounded, and by what |
| [Not completed](#not-completed) | nothing, and the evidence for saying so |

## Money

An amount is an `int64` count of **minor units** at a fixed scale of two,
carried with the currency it is denominated in. `"25.00"` is held as `2500`;
`money.Money` is the only thing in this system that converts between the two
representations.

**Nothing passes through `float32` or `float64` at any point** — not in parsing,
arithmetic, comparison, JSON, or the round trip to storage. Amounts are parsed
digit by digit into integers and rendered back the same way. The database holds
`BIGINT` minor units beside a `currency_code`, so a persisted value reproduces
both exactly with no decimal parsing in the database and no decimal string in
the middle.

| | |
|---|---|
| Representable range | −92,233,720,368,547,758.08 to 92,233,720,368,547,758.07 |
| Accepted input | `(0\|[1-9][0-9]*)\.[0-9]{2}` — one spelling per value |
| Currency | `^[A-Z]{3}$`, against **no list of any kind** |

`"25"`, `"25.0"`, `"25.000"`, `"25."`, `".25"`, `"007.00"`, `"+25.00"`,
`"-25.00"`, `" 25.00 "`, `"1e2"`, `"NaN"`, `"Infinity"` and any non-ASCII digit
are all refused. Nothing is ever silently rounded or padded, and there is
exactly one valid spelling of any value — which is what lets the idempotency
hash be taken over the submitted bytes with no normalisation step in front of
it.

Parsing, addition, subtraction and negation all detect overflow rather than
wrapping, and every one of them reports the same code: `AMOUNT_OUT_OF_RANGE`,
which is correctable, because the provider sent the amount and can send another.
The **processor** is what promotes that to `BALANCE_OUT_OF_RANGE` where it was
the resulting balance rather than the amount that could not be represented — and
that one is definitive, because the amount is fine and the balance is what it
is, so the same payload resubmitted would overflow again. It is the ceiling to
`INSUFFICIENT_FUNDS`'s floor.

Negative values exist but can be neither **parsed** nor **published**. They
come from `Sub`, from `Neg`, and from `FromMinorUnits`, which is the entry
point for a value reconstructed out of storage and for a difference computed
internally; `MarshalJSON` refuses them, because the external contract has no
representation for one. The reconciliation `difference` is the system's single
signed amount and is therefore written by a view built from `money.Money`'s own
rendering rather than by marshalling a `money.Money` — a package with two ways
to write an amount would eventually write one of them the wrong way.

**The limit worth stating out loud is the scale.** ISO 4217 is not uniformly
two-exponent: JPY, KRW, VND, CLP, ISK and the African franc family carry no
minor unit at all, the Gulf dinars carry three, and CLF and UYW carry four —
roughly a tenth of the standard cannot be held faithfully at scale 2.
**Currencies whose exponent is not 2 are out of scope.** Opening a `JPY` wallet
would succeed and would misrepresent every amount in it by a factor of a
hundred. No allowlist is kept, and that is the point rather than an omission: a
list would imply currencies can be added, and most cannot — supporting them
means a per-currency scale, which changes parsing, rendering, the limits and
the persisted representation. ADR-0001 records the two alternatives and why
neither was taken.

What the type does carry is the currency. Arithmetic and comparison refuse to
cross currencies, a wallet is identified by player **and** currency, and every
movement must match its wallet — so the model never assumes BRL even though only
BRL flows through it.

## The write path

### One transaction per attempt

Every command runs inside exactly one `app.TxManager` callback. There is no
`Begin` and no `Commit` for a use case to call, so a transaction cannot be
leaked, committed twice, or held open across a return; the manager commits when
the callback returns nil and rolls back when it does not.

| | Isolation | Reaches |
|---|---|---|
| `WithinMovement` | READ COMMITTED, READ WRITE | `*Repos` — the stores, and the two write doors |
| `WithinSnapshot` | REPEATABLE READ, READ ONLY | `*ReadRepos` — readers only, so a query that tried to write would not compile |

READ COMMITTED for movement, with the **wallet row lock** doing the serialising
rather than the isolation level. REPEATABLE READ for a read, so that a
reconciliation or a paged ledger sees one instant.

Exactly one command needs a second transaction: a submission that loses a
duplicate-key race. The unique violation aborts the first transaction, so the
operation that won can only be read in a new one. That is a *re-classification*
rather than a retry — the work is not attempted again, the winner is read and
reported — and it is bounded at one.

**The transaction is not carried in the context**, deliberately and against the
usual Go idiom. Every repository is built from a `pgx.Tx` and holds it in a
field, and there is no constructor that takes a pool — so a repository obtained
anywhere other than from a `TxManager` callback does not exist. That is the
guarantee a context-carried transaction can only ask for: here the compiler
enforces it, where there it is a check somebody has to remember at every entry
point.

### The library, and why the SQL is written out

`pgx`, with explicit SQL and positional parameters. No ORM and no query builder.

Every statement in the adapter is load-bearing in a way a generated one could
not be: the wallet lock is `FOR NO KEY UPDATE` rather than `FOR UPDATE`, the
duplicate insert is `ON CONFLICT DO NOTHING` rather than a pre-read, the
due-work query takes **no** row lock at all, and the outbox claim carries a
head-of-line predicate. A builder would let any of those be changed by somebody
who did not know why they were written that way — and three of the four are the
difference between a deadlock and a queue.

Rows become domain objects only through the domain's `Rehydrate` functions. A
row the domain would refuse is corruption, and corruption is reported rather
than loaded.

### Two doors, not five ports

The application layer declares readers for wallets, transactions, the ledger
and the inbox — and for the outbox a writer only, since nothing in that layer
publishes. What it does **not** declare is a matching writer per store. Every
write that settles an operation goes through one of two functions on the
repository bundle:

- `Repos.Open` — a new wallet, together with its opening transaction and that
  opening's ledger entry when the wallet was opened with money in it.
- `Repos.Settle` — what an outcome produced: the transaction's new state, and
  the wallet and the ledger entry when money moved.

The three rows a movement produces are only correct **together**, and the schema
pairs two of the three: `wallet_matches_ledger` and `ledger_matches_wallet` are
deferred constraints that fire at COMMIT. Nothing pairs the wager transaction
with either, and that gap was demonstrated rather than reasoned about — a `BET`
can be written at `PROCESSED` reporting a balance the wallet never held, with no
ledger entry, and `reconcile_wallet()` still returns true because the wallet and
the ledger agree with each other perfectly. Five independent writers make "call
all three, with consistent arguments" a convention, and a convention is exactly
what produces that row.

`Settle` is not even given the wallet: a ledger entry already carries
`balanceAfter`, `walletVersion` and `createdAt`, so the wallet's new state is
read out of the entry. There is no argument an adapter could get wrong, because
there is no argument. ADR-0010 is the whole of it.

### Locking, and how a lost update is prevented

A movement transaction's entire write set is **one wallet's rows plus that
wallet's outbox sequence counter**, taken in one order:

```
inbox -> wallet lock -> wager_transaction -> (trigger: active_reversal) -> outbox
```

`Outbox.Append` is the last statement of every callback, without exception. The
outbox keeps a per-aggregate sequence counter, which makes it a **second**
per-wallet serialisation point; taking it before the wallet while another
command holds the wallet and wants the counter is a deadlock with different
rows.

Three mechanisms stop a balance being computed from a value that has moved:

1. **The row lock.** `LockForMovement` / `LockByID` take `FOR NO KEY UPDATE` on
   the wallet before its balance is read and before the domain is asked anything,
   so concurrent movements on one wallet queue rather than interleave. The inbox
   and the two idempotency lookups run ahead of it, exactly as the order above
   says, and none of them reads a balance. It is an explicit operation rather than
   something a repository does on the way past, because *where* in the
   transaction the lock is taken is the whole of the concurrency design.
2. **The version condition.** The wallet update is conditioned on the version it
   was read at, and the settle door writes the wallet before the ledger entry so
   that the version-conditioned update is the first gate on a stale balance and
   the failure names the real fault. A version conflict means the lock was *not*
   held, which should be unreachable — which is why it is counted apart from a
   lock timeout, whose meaning is the opposite: the lock working, and the wait
   being too long.
3. **The ledger chain.** A trigger requires each entry to follow its
   predecessor's version and balance, and `UNIQUE (wallet_id, wallet_version)`
   refuses a second entry at one version. That makes ledger-to-balance agreement
   hold **by induction**, at O(1) per write, rather than by resumming the ledger
   — and the unique index is unreachable serially, since the chain always demands
   the next version and the next version is by definition free, so it is purely a
   concurrency guard.

**The resume worker obeys the same order, which is why it finds its work with a
plain `SELECT` that takes no row lock.** Claiming the parked row first and then
wanting its wallet closes a cycle with a submission that holds that wallet and
wants the row — and because a reversal and its reference must agree on the
wallet, the two are the same wallet *by construction* rather than by
coincidence. Taking the wallet first makes the cycle unrepresentable; the
unlocked `SELECT` is what makes that possible, and the re-read under the lock
(`ClaimForUpdate`) is what makes it safe. `FOR UPDATE SKIP LOCKED` on that query
— which looks like the obvious optimisation, and matches what the outbox does —
reintroduces a `40P01` that was demonstrated rather than predicted. ADR-0011.

`DATABASE_LOCK_TIMEOUT` (3s) and `DATABASE_STATEMENT_TIMEOUT` (15s) are
separate budgets because they are separate faults: one is ordinary contention
on a wallet, the other is a query that will not finish.

## Idempotency

### The key

Provider-chosen. Over HTTP it is the `Idempotency-Key` **header**; on the queue
it is the `idempotencyKey` **member** of `data`, because there is no header on a
queue. It is never trimmed, never defaulted, and never computed from the body —
deriving one would make every distinct payload its own key, which is the
opposite of what the key is for. An `idempotencyKey` in an HTTP body is an
unknown member and a 400.

Uniqueness is the **database's**, not a cache's. A submission claims two keys
before any money work: `(provider, idempotency_key)` and
`(provider, external_transaction_id)`. `Record` uses `INSERT … ON CONFLICT DO
NOTHING` rather than a pre-read or a caught `23505` — one round trip, it blocks
on an in-flight duplicate until that duplicate commits, and it leaves the
transaction usable.

### The hash

`wagering.PayloadHash` is the **lowercase hex SHA-256** of a canonical JSON
object containing the operation's business fields and nothing else, keys in
ASCII order, no insignificant whitespace:

```json
{"externalTransactionId":"…","gameId":"…","kind":"BET",
 "money":{"amount":"25.00","currency":"BRL"},
 "playerId":"…","provider":"…",
 "referenceExternalTransactionId":"…","roundId":"…"}
```

**Included:** `provider`, `externalTransactionId`, `kind`, `money`, `playerId`,
`roundId`, `gameId`, and `referenceExternalTransactionId` when the operation
names one. The reference key is **omitted entirely** when there is none, rather
than emitted as `null`, so that "no reference" has one representation.

**Excluded:** the idempotency key itself — the hash exists to decide whether one
key has been reused for two different operations, and including it would make
every submission trivially unique. The wallet id, because it is derived from the
player and currency rather than submitted. And all transport metadata:
timestamps, correlation and causation ids, queue message ids, headers, delivery
counts, retry attempts. None of them says anything about what the provider asked
for.

Strings are escaped minimally per RFC 8259 — quote, backslash and the control
characters — with no HTML escaping and no `\u` encoding of anything else. Every
other byte is copied through exactly as it arrived, **including one that is not
valid UTF-8**: the encoder never substitutes a replacement character, because a
hash that quietly repaired its input would be a hash of something nobody
submitted. The encoding is written out by hand rather than delegated to a JSON
library for that reason — it must be reproducible byte for byte by any
transport, without depending on a library's defaults.

**Normalisation: none, and none is needed.** Every input has exactly one valid
spelling by the time it reaches the hash — an amount carries exactly two
fraction digits, a currency is uppercase, identifiers are refused rather than
trimmed. The canonical form is therefore the submitted form, and no
transformation stands between what a provider sent and what is hashed.

### HTTP and SQS hash the same bytes

The queue envelope's `data` members are the HTTP body's member names, exactly.
The business fields are parsed **once**, by the application layer, and
`Command.PayloadHash` computes the hash there rather than accepting one from a
caller — which is what guarantees that two transports cannot disagree about
whether a submission is a retry. A transport that repaired a value on the way in
would be hashing something nobody submitted, so neither one does: `money.Parse`
is reached once per submission, inside the application layer, from both doors.

The one difference is where the key rides, and it is the only one.
`TestOneOperationOverHTTPAndOverTheQueueSettlesOnceInEitherOrder` drives one
operation over a real listener and a real FIFO queue in both orders, and
declares its own two wire types rather than reusing the adapters' structs — so a
rename on either side is caught rather than compiled away.

### The four outcomes

| What arrived | Answer | What is persisted |
|---|---|---|
| A new key, a new operation | the outcome it reached — 200, 202 or 422 | everything: the transaction, the ledger entry if money moved, the events |
| The same key, the same payload | **replay**: the stored outcome, `idempotentReplay: true`, including the balance observed *then* | nothing new |
| The same key, a **different** payload | 409 `IDEMPOTENCY_PAYLOAD_CONFLICT` | nothing new |
| A **different** key, the same `externalTransactionId` | 409 `CONFLICT`, with no failure code | nothing new |

The fourth has no code because the catalogue describes no outcome for it —
nothing is persisted for it — and inventing one would put an entry in the
external contract describing nothing a provider can act on.

A **still-parked** original is replayed as it stands and is deliberately not
carried forward by the replay: continuing is the worker's job, and doing it on a
provider's retry would spend the wait budget twice as fast as the policy says.

A replay is not the same thing as a retry. A *replay* returns an outcome that
was already reached; a *retry* follows a failure that recorded nothing, where
there is no stored outcome to answer from. The split that decides which a
provider is entitled to is `failure.Code.Correctable()` — see **Failure codes**
below.

## The state machine

An external operation is recorded as `PENDING`. An opening is born `PROCESSED`,
because a wallet's starting balance is applied as part of creating the wallet
and there is no moment at which the opening is awaiting anything.

```
                     ┌──────────────┐
                     │  (external)  │
                     └──────┬───────┘
                            ▼
                       ┌─────────┐
       ┌───────────────┤ PENDING ├───────────────┐
       │               └────┬────┘               │
       ▼                    ▼                    ▼
┌─────────────┐   ┌───────────────────┐   ┌──────────┐
│  PROCESSED  │   │ PENDING_REFERENCE │◀─┐│  FAILED  │
│ (terminal)  │   └─────────┬─────────┘  ││(terminal)│
└─────────────┘             │            │└──────────┘
       ▲                    └────────────┘
       │                   resume, attempt++
       │                          │
       └──────────────────────────┼──────────▶ ┌──────────┐
                                  └──────────▶ │ REJECTED │
                                               │(terminal)│
                                               └──────────┘
```

| Status | |
|---|---|
| `PENDING` | recorded, processing not finished. **Never committed**: `Record` writes it and the same transaction goes on to settle or park it, so a committed row is never `PENDING`. |
| `PENDING_REFERENCE` | waiting for the transaction it points at. The only non-terminal status ever committed, and it is **always** written with a schedule — `wager_transaction_only_waiting_is_scheduled` is an equivalence in the schema, which is why one statement writes both. |
| `PROCESSED` | completed. Terminal. |
| `REJECTED` | a business rule settled it. Terminal. |
| `FAILED` | a permanent infrastructure failure, recorded for audit. Terminal. |

A transaction that has reached a terminal status never moves again, and an
attempt to move it reports `INVALID_STATE_TRANSITION` rather than panicking.
**Recording that a transaction has been reversed is not a transition** — nothing
about a reversed transaction changes.

### Transient or permanent

The word means the same thing at three layers, and each one answers it from what
it can actually see.

**In the PostgreSQL adapter** the answer is structural before it is a list. A
refused *connection* is transient whatever SQLSTATE the server refused with,
because `*pgconn.ConnectError` wraps the refusal it was handed and the connect
test is made **ahead of** the SQLSTATE test. That ordering is the whole of the
function: `55000` from a connection attempt means "not accepting connections"
and ends, while `55000` from a statement means an object is in the wrong state
and does not — same SQLSTATE, opposite correct answers, and only whether a
connection was established tells them apart. On an established connection the
transient SQLSTATEs are serialisation failure, deadlock detected, lock not
available, query cancelled, too many connections, admin and crash shutdown,
cannot connect now, idle session timeout, and the whole of connection-exception
class `08`. One *constraint* is transient — `inbox_pkey` — because the loser of
a legitimate two-consumer race persisted nothing and its redelivery will find
the row and replay the settled result. Every other rule in the schema refuses
something that will be refused again.

The classification keys on `pgconn.PgError.ConstraintName` and never on the
SQLSTATE, because every SQLSTATE class holds both ordinary business outcomes and
signs that something is wrong: `23505` on `active_reversal_pkey` is a reference
already reversed, while `P0001` on `wallet_ledger_entry_is_append_only` means
somebody tried to rewrite the ledger. The name tells them apart; the class does
not. Every rule in the schema names itself, triggers included, so the caller
reads one field.

**In the SQS adapter** five things are `Retryable`, tested in this order: the
caller's own cancellation, a throttle, any HTTP status at or above 500 together
with 408 and 429, a fault the service itself declared to be the *server's*, and
a request that never reached a server at all — a failed connection, a timeout on
the wire. The status is tested outside the API-error branch, because a response
this process could not parse into an API error still carries one, and a 503 with
an unreadable body is still a 503. A malformed request and a refusal the service
will repeat are `Unretryable`, and so is anything unrecognised, for the same
reason the PostgreSQL adapter gives — a failure nobody recognised is not one
anybody has established is safe to repeat.

**In the consumer**, where the decision is whether a message is worth another
delivery, `app.Retryable` is the obvious one and `app.NotFound` is the
interesting one. The only way this system produces `NotFound` on that path is a
submission for a player who holds no wallet in that currency: nothing is
persisted, the key is still free, and the same message succeeds unchanged once
the wallet is opened. Sending it straight to the dead-letter queue would turn an
ordering race between opening a wallet and the first operation on it into an
operator's morning. It stays bounded — the redrive policy ends the message after
its five deliveries, exactly as it would have. **Everything else is permanent,
including every business refusal** — and those are not failures at all: a
rejection is an outcome with a row and an event behind it, and it never reaches
that function.

## Pending references

A `WIN`, `REFUND` or `ROLLBACK` may name a transaction that has not arrived
yet — a queue reordered them, or the provider sent them out of order. The
operation is not refused and is not corrupted: the domain **parks** it as
`PENDING_REFERENCE` with a schedule, and the reference worker carries it forward
when the reference lands.

What the reference says decides which of three things happens, and the checks
run in a fixed order so that the code a provider receives is deterministic:
first whether the reference is available at all, then whether it describes the
same piece of business, then whether it is the right kind to act on, then
whether the amounts agree, and only then whether history has already spent it.

| The reference is | |
|---|---|
| not found | **wait** |
| `PENDING` or `PENDING_REFERENCE` | **wait** — it may still be processed, and acting now would guess at its outcome |
| `REJECTED` or `FAILED` | **reject**: `REFERENCE_NOT_PROCESSED`. There is nothing to act on, and no amount of waiting changes that. |
| `PROCESSED`, but disagreeing on provider, player, wallet, currency or round | **reject**: `REFERENCE_MISMATCH` |
| `PROCESSED`, wrong kind for this reversal | **reject**: `REFERENCE_NOT_REVERSIBLE` |
| `PROCESSED`, a reversal of a different amount | **reject**: `REVERSAL_AMOUNT_MISMATCH` — partial reversals do not exist |
| `PROCESSED` and already actively reversed | **reject**: `REFERENCE_ALREADY_REVERSED` |
| `PROCESSED`, and a **`WIN`** naming anything that is not a `BET` | **reject**: `REFERENCE_MISMATCH` |
| `PROCESSED` and agreeing | **apply** |

`REFERENCE_NOT_REVERSIBLE`, `REVERSAL_AMOUNT_MISMATCH` and
`REFERENCE_ALREADY_REVERSED` are the reversal branch, asked in that order. The
`WIN` branch has exactly one check of its own — the reference must be a bet —
because a win need not match the stake and does not hold what it points at.

### The wait budget

The **domain** owns the budget — how many attempts, and for how long — and the
**application layer** owns the schedule. They are kept apart so that tuning when
an operation is looked at again cannot change whether it is eventually settled.

| | Default | |
|---|---|---|
| `REFERENCE_MAX_ATTEMPTS` | 20 | how many times an operation may be parked |
| `REFERENCE_TTL` | 5m | how long after the **first** wait it stops waiting |
| `WAGERING_BACKOFF_INITIAL` | 1s | the wait before the second attempt |
| `WAGERING_BACKOFF_FACTOR` | 2 | multiplies the wait after each attempt |
| `WAGERING_BACKOFF_MAX` | 1m | caps it |

Whichever runs out first settles the operation, as a **definitive rejection**
with `REFERENCE_NOT_FOUND`: a row, an event, and the idempotency key bound to
the payload for good. The deadline is set on the first wait and never moved, so
a provider retrying does not extend it.

The schedule is `initial × factor^(attempts−1)`, capped at `max`, with up to a
further tenth added as jitter — enough to break up a batch of operations parked
in one commit, which is the realistic way a herd forms here, without
meaningfully changing when any one of them is looked at. The jitter is derived
from the **transaction's own identifier**
rather than from a random source, so the schedule is reproducible: the same
operation parked at the same instant comes back at the same instant, in a test
and in production. A UUIDv7's tail is random, which is exactly the property
wanted. The result is clamped to the deadline, because waiting past the point
where the domain will refuse to park the operation again would mean an operation
whose budget expired at noon is not looked at until one and is then rejected for
having run out of time an hour earlier.

The policy refuses a factor that is **not a number**. `Factor < 1` is false for
NaN, `math.Pow` carries it through, and the conversion to a `Duration` yields
zero — so every attempt after the first would be scheduled for *now*, for ever,
which is the one thing a backoff has to be incapable of. It is reachable without
anybody typing it, because `strconv.ParseFloat("NaN", 64)` succeeds and a factor
read from a deployment's environment arrives here.

### How a parked operation is carried forward

Two routes, and they meet at the same door:

- **Woken.** When an operation settles, `MakeDue` brings forward every operation
  **on that wallet** waiting for it, in the same transaction. It is scoped to the
  wallet the caller already holds the lock on — waking a row on another wallet
  would be writing outside the declared write set. Waiters elsewhere are found by
  their own schedule instead.
- **Scheduled.** The reference worker polls, one candidate per turn, ordered by
  `(reference_next_attempt_at, id)`. Ties break on the identifier because
  operations woken in one commit share an instant *exactly*.

`Resume` claims at most one operation per call and reports `Claimed: false`
when nothing was due, or when the candidate stopped being due before the lock
was taken. Neither is a failure, and a worker that alerted on them would alert
on its own normal operation.

A failure to carry an operation forward that is *this system's* fault is
rescheduled rather than settled, and reported through `DefectObserver` — the
provider is not waiting on that path, the row stays parked while its deadline
holds — past it the operation is moved to `FAILED`, which is the one place that
status is reached — and without the hook the condition would be invisible until
the budget ran out and the operation was rejected for a reason that was never
true. `Reschedule`
touches only the schedule, so it does not count an attempt against a budget that
nothing was spent from.

## Reversals

A reversal is a `REFUND` or a `ROLLBACK`: an operation whose purpose is to undo
another. **Reversals are append-only.** A reversed transaction is never written
to; the reversal is a new transaction pointing at it, and whether a transaction
is currently reversed is derived:

> a transaction has an **active reversal** when some processed reversal points
> at it and has not itself been reversed.

| | reverses | can itself be reversed |
|---|---|---|
| `REFUND` | a `BET`, in full | yes, by a `ROLLBACK` |
| `ROLLBACK` | a `BET`, a `WIN` or a `REFUND`, in full | **no** |

The asymmetry is deliberate: a rollback is a provider asserting the operation
never happened, while a refund is a business decision that may be revisited. So
a rollback applied straight to a bet holds that bet permanently, and a refund
never does.

A `WIN` may also name a transaction — the bet it pays out on, in the same round
— and that is **not** a reversal. It need not match the stake, several wins may
point at one bet, and it does not hold the bet. A repository implementing "find
transactions referencing X" naturally returns wins too, so `ActiveReversal`
ignores anything that is not a reversal; otherwise a bet would become
unreturnable the moment it paid out.

### The combinations

Starting from a processed `BET` of 25.00 on a wallet opened at 100.00:

| Sequence | Result | Balance |
|---|---|---|
| `BET` | debited | 75.00 |
| `BET → REFUND` | the stake is back; the refund now holds the bet | 100.00 |
| `BET → REFUND → REFUND` | **rejected** `REFERENCE_ALREADY_REVERSED` — the same 25.00 twice | 100.00 |
| `BET → REFUND → ROLLBACK` *of the bet* | **rejected** `REFERENCE_ALREADY_REVERSED`, for the same reason | 100.00 |
| `BET → REFUND → ROLLBACK` *of the refund* | the refund is undone, the bet stands debited — **and is reversible again** | 75.00 |
| `… → ROLLBACK` *of the bet*, now | applied; the bet is held permanently | 100.00 |
| `BET → ROLLBACK` *of the bet* | applied; the bet is held permanently, because a rollback cannot be reversed | 100.00 |
| `BET → REFUND → ROLLBACK → REFUND → …` | unbounded, each pair netting zero | alternates |
| `BET → REFUND` of 20.00 | **rejected** `REVERSAL_AMOUNT_MISMATCH` | 75.00 |
| `REFUND` of a `WIN` | **rejected** `REFERENCE_NOT_REVERSIBLE` — a refund returns a bet and nothing else | — |
| `ROLLBACK` of a `ROLLBACK` | **rejected** `REFERENCE_NOT_REVERSIBLE` | — |

The unbounded case is not capped. Every step is individually valid, audited, and
leaves the balance correct; a cap would be a number with no business meaning.

### Where the rule is enforced

In the **schema**, as a primary key. `wagering.active_reversal` has
`reference_id` as its primary key and `reversal_id` unique, maintained by two
triggers on `wager_transaction` — one on insert, one on settle — sharing one
`SECURITY DEFINER` function; a second active reversal of one bet is a duplicate
key, surfacing as `23505` on `active_reversal_pkey`. The trigger deletes the
row keyed on the reversal being reversed before inserting the new one, which is
exactly "rolling back a refund releases the bet".

The literal reading of the rule — a partial unique index on
`resolved_reference_id WHERE status = 'PROCESSED' AND kind IN ('REFUND',
'ROLLBACK')` — is one line and is **wrong**, in a way that only shows up on a
sequence the business explicitly wants. After `BET → REFUND → ROLLBACK of that
refund`, the bet has two processed reversals pointing at it, and a later
rollback of the bet is legitimate; the literal index refuses it, the domain
produces a valid outcome, the processor reports success, and the write fails at
`COMMIT`. ADR-0007 records why the derived table was preferred, and ADR-0003 why
a mutable `reversedBy` slot was not.

The domain's own guarantee is weaker and still holds: every reversal moves
money, so two concurrent reversals must write the same wallet, and the wallet's
version serialises them. The schema's is the stronger of the two because it
does not depend on that — `TestTwoReversalsCannotRaceForOneReference` races a
refund and a rollback that write no ledger entry and touch no balance, so the
version is never consulted, and the reference is still held exactly once.

The recursion is provably bounded at two levels: a rollback cannot be reversed
and a refund can only be reversed by a rollback, so "does this bet have an
active reversal?" never walks an open-ended chain. `ReferenceView` carries the
reference plus its reversals, each flagged with whether it has been reversed,
and that is always enough.

## The inbox and the outbox

### The inbox

`PRIMARY KEY (consumer_name, message_id)` — the identity **and** the
serialisation point. Two consumers may legitimately see one message, so the
consumer is half of the identity; `CONSUMER_NAME` must therefore be identical
across replicas, or each replica would get its own rows and apply the same
message once each.

The row is written **in the same transaction as the work it describes**. There
is no second write marking it complete: a row that exists is a message that was
handled, because it and the domain changes commit together or not at all.

The hash the inbox stores is the **SHA-256 of the raw body bytes**, not of the
parsed fields. That is a different question from the idempotency key's: the key
answers "has this *operation* been recorded", the inbox answers "has this
*message* been handled", and only a raw-byte hash makes one `messageId` carrying
two different bodies detectable.

The application layer names the pair as one value, `app.InboxKey`, rather than
passing two adjacent strings. Two strings side by side are transposable at a
call site without the compiler noticing, and a transposed lookup here finds
nothing — which reads as "not handled yet" and lets a redelivery be processed
twice.

### The outbox

An event is written in the same breath as the change that caused it, so there is
no moment at which one exists without the other. **Nothing in the application
layer publishes**; a separate worker reads the outbox afterwards.

| Constraint | Invariant |
|---|---|
| `outbox_pkey (event_id)` | republication preserves event identity — it is an update to this row, never a new one |
| `UNIQUE (aggregate_id, aggregate_sequence)` | contiguous per-wallet ordering, so a consumer can tell a gap from an ending |
| `outbox_claim_is_whole` | a claim is all three of `claimed_by`, `claimed_at`, `claim_expires_at`, or none |
| `outbox_event_type_fkey` | only a declared event can be published |

**Numbering is per aggregate, and the aggregate is the wallet.** The obvious
outbox — `BIGSERIAL` plus `FOR UPDATE SKIP LOCKED` — has a hole that does not
show up in testing: transaction A takes sequence 6 and B takes 7, B commits
first, a publisher reads 7 and sends it, and only later sees 6. SQS FIFO does
not save you, because it preserves the order messages were *sent* within a
group. So a trigger assigns the next number from a durable counter row held
**per aggregate**, in `wagering.outbox_aggregate_sequence`.

The counter is a table rather than `max()` over the outbox because the outbox
is prunable and the numbering is not: once the last row for a quiet wallet is
pruned, a `max()` derivation starts at 1 again and the unique index cannot
object, because the rows it would collide with are the ones that were deleted.
It is also not the wallet row — `FOR UPDATE` there is a lock *upgrade* that two
ordinary writers for one wallet can deadlock on. ADR-0008.

### The claim

```sql
UPDATE wagering.outbox SET
    claimed_by = $1, claimed_at = $2, claim_expires_at = $3, attempts = attempts + 1
WHERE event_id IN (
    SELECT o.event_id FROM wagering.outbox o
    WHERE o.published_at IS NULL
      AND o.next_attempt_at <= $2
      AND (o.claim_expires_at IS NULL OR o.claim_expires_at <= $2)
      AND NOT EXISTS (
          SELECT 1 FROM wagering.outbox p
          WHERE p.aggregate_id = o.aggregate_id
            AND p.published_at IS NULL
            AND p.aggregate_sequence < o.aggregate_sequence)
    ORDER BY o.next_attempt_at, o.sequence
    FOR UPDATE SKIP LOCKED
    LIMIT $4)
RETURNING …, payload - '$trace'::text, payload -> '$trace', …
```

The `NOT EXISTS` is the **head-of-line rule**: a wallet's second event is not
claimable until its first is published, so the publisher cannot send them out
of order however it is scheduled. `SKIP LOCKED` keeps publishers off each
other, and gives up *global* ordering across wallets, which nothing needs. One
stuck event delays that wallet and nothing else — which is what FIFO means.

A claim expires by **wall clock** (`PUBLISHER_HOLD`, 30s), so work abandoned by
a crashed publisher returns to the pool visibly rather than waiting on a
connection that may never close. That is what makes `PUBLISHER_NAME` have to be
distinct per process: it scopes a reschedule to the publisher holding the row,
and two publishers sharing a name put back each other's claims.

`$trace` is the W3C trace context, written into the `jsonb` payload by the
adapter and taken back out by the claim. An operation and the event it causes
have to be one trace, and the outbox is the handover that breaks it — the
publisher runs minutes later, in another process, with no memory of the request.
**Nothing published ever carries it**: the claim returns `payload - '$trace'` as
the body.

### Backoff and recovery

A turn that filled its batch does not wait; one that did not waits
`PUBLISHER_INTERVAL` (1s). A turn that **published nothing** is not progress,
and that counts entries the *queue accepted* rather than rows the outbox marked
— without which a publisher whose sends are all failing spins, measured at
around fifty thousand claim round trips in two hundred milliseconds. Failures
back off from
`PUBLISHER_BACKOFF_INITIAL` (2s) by a factor of 2 to `PUBLISHER_BACKOFF_MAX`
(5m).

`SendMessageBatch` succeeds **partially** — some entries accepted, some
refused, in one 200 response — so the adapter reports per entry rather than per
call. A publisher must mark exactly the events that reached the queue, and a
single error for the call would force it to choose between marking events that
were refused and republishing events that were not.

Recovery is the four fault points, each marked with a `faults.Hit` on the
**production** path — not behind a flag, not behind a build tag — so that a test
can kill the process exactly there and prove what survives:

| | Where | What survives |
|---|---|---|
| `AfterCommitBeforeAck` | consumer, between the commit and the delete | the message is redelivered and the inbox absorbs it |
| `AfterClaimBeforePublish` | publisher, between taking a claim and sending | the claim expires and another publisher takes the row |
| `AfterPublishBeforeMark` | publisher, between the send and marking published | the event is sent again under the same `MessageDeduplicationId` — and a claim expires in `PUBLISHER_HOLD`, well inside SQS's five-minute deduplication window — so the queue drops the second copy |
| `AfterPendingCommit` | reference worker, after the commit that parked or settled | the schedule is durable; another worker picks it up |

### Shutdown

Stopping is **two deadlines, not one**. Receiving stops immediately — the long
poll is cancelled rather than waited out — while work already in hand keeps the
context it started with until the drain deadline passes. Whatever is still in
flight at that deadline is **given back**: the consumer resets the visibility
of every message it has not finished deciding about to zero, so it is
redelivered at once instead of waiting out a timeout that exists for the case
where nobody released it, and the publisher hands back every claim it holds.
Both report what did not finish, because a drain that ran out of time and a
drain that completed are different mornings.

Every wait is bounded twice — by the caller's context, so a lifecycle hook
keeps the budget it was given, and by the worker's own drain timeout, so a
caller given none still gets a bound. A deadline that only decided when to
*cancel* would not be a deadline at all: the caller would still be waiting when
it passed.

## SQS

Both queues are FIFO, provisioned by
[`deploy/localstack/01-queues.sh`](deploy/localstack/01-queues.sh).

| | `wager-transactions.fifo` | `wallet-events.fifo` |
|---|---|---|
| `MessageGroupId` | the **wallet id** | the **aggregate id**, which is always a wallet |
| `MessageDeduplicationId` | the envelope's **`messageId`** | the **`eventId`** |
| `ContentBasedDeduplication` | false | false |
| `VisibilityTimeout` | 30s | not set — nothing in this deployment receives from it |
| `ReceiveMessageWaitTimeSeconds` | 20s | 20s |
| `MessageRetentionPeriod` | 14 days | 14 days |
| Redrive | `maxReceiveCount` 5, to `wager-transactions-dlq.fifo` | none |

The group is the wallet because a wallet is the unit whose operations must not
overtake each other — a rollback that arrived after the bet it undoes must be
handled after it. Grouping by anything wider would serialise every wallet
behind every other; by anything narrower — the transaction — would order
nothing at all.

Deduplication is **not** content-based, on purpose. The envelope's `messageId`
is the identity the inbox keys on, so the queue's five-minute window and the
database's permanent record agree on what "the same message" means; a content
hash would make two bodies differing only in whitespace into two messages.
Outbound, the `eventId` is stable across republication, so a publisher killed
between sending and marking cannot put a second copy of an event on the wire.

Five deliveries because the failures worth retrying here are transient — a lock
conflict, a connection lost, the database restarting — and a handful of attempts
spans them. A message that has failed five times is failing for a reason no
further delivery will change, and leaving it in the queue would put it at the
head of its wallet's group for ever.

### Concurrency, and what is never named

A consumer runs `CONSUMER_CONCURRENCY` (4) **independent receivers**. Each
receives its own batch and handles that batch strictly in the order it arrived,
one message at a time. Concurrency is therefore across groups and never within
one — and it is bought without the consumer ever reading `MessageGroupId`, or
even being able to: SQS will not deliver another message of a group while one
of that group's messages is in flight, and it tracks *that* a message is in
flight rather than who holds it, so the guarantee reaches across replicas and
not merely across the receivers of one process.

The same fact settles what happens to the rest of a batch when one message is
not deleted. A FIFO receive returns as many messages of one group as it can, so
everything behind an undeleted message **may** be in its group, and handling it
would be applying a wallet's operations out of order. A message the consumer
does not delete therefore stops its batch: on a transient failure the messages
behind it are hidden for the same backoff, so the group comes back together and
in order; on a permanent one they are left as they are and time out together. A
permanent failure is left alone rather than released to zero, because releasing
would spend the whole delivery budget in one burst.

What that costs, and the precondition it rests on, are both in **Limitations**.

### Credentials

The SQS client takes none. It loads the AWS SDK's default chain — environment,
profile, instance role — so no credential is ever named in this service's own
configuration, written to a log line, or carried in an error. LocalStack accepts
any pair; a deployment sets neither and uses an instance role.

## Authentication and authorisation

### The model

A bearer token becomes an `app.Principal`, and a principal is one of exactly
two things: a **provider acting as itself**, or **the service acting for
itself**. Every authorisation question is answered by a method on that value, in
the application layer:

| | `MaySubmitAs` | `MayReadAs` | `MayAdministerWallets` | `MayResume` |
|---|---|---|---|---|
| provider | itself only | itself only | no | no |
| service (`internal`) | **no provider** | any provider | yes | yes |

`MayResume` is the worker's door and is authorised by identity alone, because
it names no operation: what it carries forward is whatever is due, and the
authority to act on a given row comes from that row's own provider once it has
been read, never from a claim the caller made.

The service reads as any provider although it submits as none, because reading
takes nothing and moves nothing. A provider naming another provider is refused
**before any row is looked for**, so the answer is the same for every external id
and confirms the existence of none of them.

The split between `Wagering` and `Wallets` *is* the authorisation boundary, so
"a provider cannot reach a wallet" is visible in the type a caller holds rather
than in a check it has to remember.

`app.ErrForeignOperation` — a provider walking another provider's identifiers —
is matched and logged, and **never rendered**. Such a caller is told
exactly what a caller asking for something absent is told, byte for byte and
header for header; the distinction rides in the error chain, where `errors.Is`
finds it and a rendered message does not.

### Why Keycloak, and why `client_credentials`

The callers here are **machines**. A game provider's integration is a server
talking to a server: there is no browser, no user to consent, no redirect to
land on, and nothing a refresh token would refresh. `client_credentials` is the
OAuth 2.0 grant for exactly that case — the client authenticates as itself, gets
an access token, and that is the whole exchange. Any grant with a user in it
would be modelling a person who does not exist.

Keycloak rather than a hand-rolled issuer, or a shared secret in a header,
because the thing being avoided is this service owning credentials at all. With
an identity provider in front, this service holds **no** secret, verifies with a
public key it fetches, and a provider's access is revoked by disabling a client
in a realm rather than by a deployment here. It is also ordinary OIDC, so the
realm can be swapped for whatever an operator already runs — the adapter reads
standard claims and one custom one.

Keycloak specifically: it imports a realm from a file, which is what makes the
integration and multi suites able to run against a **real** identity provider
rather than a stub, with this repository's own realm. A test that verified
tokens it minted itself would be testing its own key pair.

### What the verifier guarantees

The library is `lestrrat-go/jwx` — its `jwk`, `jws` and `jwa` packages, and
deliberately not its `jwt` — rather than `coreos/go-oidc`. The shorter route
was rejected on two concrete points: its remote key set owns the cache and the
refresh and hands back a verified payload, so there is no seam at which an
algorithm could be refused **before** a key is chosen and none at which an
unknown `kid` could be rate-limited — and both of those are the entire security
content of this adapter. Its provider constructor also performs discovery on
the spot, which would make building an authenticator a network call in a place
with nothing to report a failure to.

- **The algorithm allow-list is applied to the JOSE header before a key is looked
  up**, and the algorithm handed to the verifier is the one from the adapter's
  own table rather than the one the token named. `none` and the HMAC family are
  refused *at construction*, so an allow-list that admitted them could not be
  built, and the classic confusion attack dies at the header. The default
  allow-list is nine algorithms — `RS`, `PS` and `ES`, each at 256, 384 and 512
  — rather than RS256 alone: what closes that attack is asymmetry, and narrowing
  further only buys an outage on key rotation.
- **Only asymmetric public keys are ever cached.** A JWKS entry of type `oct` is
  dropped at parse time, as is one that declares itself published for encryption
  rather than for signing.
  Keycloak publishes an encryption key beside its signing key, so that filter is
  load-bearing rather than theoretical.
- Issuer, audience, expiry, not-before and issued-at are all checked, **after**
  the signature and never before it. The issuer is compared byte for byte and is
  not normalised.
- **Exactly one signature is accepted.** A JWS may carry several over one
  payload, and taking the first would let anybody append a signature of their own
  to a token somebody else's key signed.
- **Nothing on the key-fetching path follows a redirect.** The adapter copies the
  supplied client and overrides `CheckRedirect`, rather than documenting a
  requirement a caller could get wrong — the origin checks test a URL, and a
  client that followed redirects would be guarding an address and not a
  destination.
- A token naming an unknown `kid` triggers **one** rate-limited refresh, without
  which anybody who can present arbitrary key identifiers has a free
  amplification channel pointed at the identity provider, paid for with this
  service's reputation. Concurrent misses collapse into a single fetch, and a
  refresh that fails **leaves the cached keys alone** — a momentary outage at the
  identity provider must not become a wave of 401s for tokens this process can
  still verify.

### The claims

| Claim | |
|---|---|
| `iss` | must equal `OIDC_ISSUER`, byte for byte |
| `aud` | must contain `OIDC_AUDIENCE`, and may name others. Keycloak does not put an API's own identifier there for a `client_credentials` grant unless the realm says so with an audience mapper. |
| `sub` | the service account's user id. Audit trail, never a decision. |
| `realm_access.roles` | `provider` or `internal` — what sort of principal this is |
| `providerId` | a hardcoded claim the provider clients carry, and the **only** place a provider's identity is read from |

**`resource_access` is never read, and neither is `azp`.** `resource_access` is
keyed by client id, so reading a role out of it means choosing the bucket with
`azp` — which puts the identity decision back on the claim that was just
rejected for making it. `azp` names the client that asked for the token, not
the grant the token carries, and an identity decided by it would be an identity
decided by which client happened to authenticate rather than by what the realm
granted. A realm role is one array, in one place, and moving it takes a
deliberate change to the realm rather than a rename of a client.

A token carrying both roles, neither role, or `provider` without a usable
`providerId` is **refused**. Resolving it to one of the two would make the
adapter pick which authorisation boundary applies, and that pick is exactly the
decision the application layer keeps for itself.

A token, a claim set and key material never appear in an error, a message, a log
line or anything this adapter hands back.

### On the queue

There is no token on a queue. **The queue is the authorisation boundary**: the
principal is a provider principal minted from `data.provider`, and whoever can
send to the queue is trusted to name the provider. That is a deployment's
decision, expressed as an SQS resource policy, not this service's.

## Composition and shutdown

`internal/fxmod` is the composition root: the one package that knows every other
package in this tree, and the **only** one whose production code imports a
dependency-injection framework — `cmd/api` and `cmd/worker` are a `main` that
hands it the environment, and their own tests are the only other place Fx is
named. The domain, the application layer, the adapters and the workers are
all free of Fx by design — which is what lets every one of them be built by hand
in a test, and every one of them is. This package's failure mode is a graph that
will not assemble, rather than a component that cannot be exercised without a
container.

| Module | |
|---|---|
| `config` | the checked `config.Config`, handing each module the part it reads |
| `telemetry` | the logger and the OpenTelemetry SDK: trace and meter providers, the propagator |
| `postgres` | the pool, the transaction manager, the outbox claims, the readiness probe |
| `sqs` | the client, and the queues a binary actually asks for, each resolved at start-up |
| `oidc` | the token verifier, whose key set is fetched at start-up |
| `app` | the two services, the domain processor, and the four ports only a composition root can supply — clock, identifiers, defects, divergences |
| `httpserver` | the API and the server carrying it. `cmd/api` only. |
| `consumer`, `outbox`, `reference` | the three loops. `cmd/worker` only, each included or left out by its own environment variable. |

### Start-up

Each binary validates what it will actually use **before it claims to be
running**, each check bounded by its own timeout as well as by `START_TIMEOUT`.
`cmd/api` makes all three below; `cmd/worker` builds no identity provider at
all, because it verifies no token, and resolves only the queues its enabled
loops ask for:

- **PostgreSQL**, by the same readiness probe `/health/ready` runs — so a process
  reporting itself up has already answered the question the orchestrator is about
  to ask.
- **SQS**, by resolving each queue's name to its URL — so a queue nobody
  provisioned is a start-up failure with an operator watching, rather than a
  consumer that receives nothing and says nothing.
- **The identity provider**, by fetching the key set — so a realm that does not
  exist is a start-up failure rather than a wall of 401s.

They are registered **before any loop's Start hook**, which does not happen on
its own: Fx appends hooks in the order it constructs things, and the consumer
module builds its queue and then appends its own Start, so the *outbound* queue
would be resolved after the consumer had begun handling messages.

**Start-up leniency differs by binary**, and the line is the adapter's own
classification rather than a judgement made here. A queue that does not exist
is `Unretryable` and stops both binaries. A queue that is momentarily
unreachable is `Retryable` and stops only `cmd/worker` — `cmd/api` stays up and
**unready**, because the outbox is the buffer that exists for exactly this,
while the worker serves no readiness probe and every one of its loops *is* the
queue.

A worker with all three loops disabled is refused outright: a process that holds
a pool open, reports itself up and consumes nothing is indistinguishable from a
healthy deployment.

A loop is started on a context that **outlives the start-up budget**, not on
the one its hook is handed. A loop rooted in the start-up context stops
receiving the moment `START_TIMEOUT` expires — seconds after the process came
up, silently, with every Stop afterwards reporting a clean shutdown of a loop
that had been dead.

### Shutdown ordering

Fx runs `OnStop` hooks in the reverse of the order they were appended, and this
package relies on that deliberately rather than hand-rolling a sequence. Four
facts make the append order something nobody has to maintain:

1. Only a handful of constructors append an `OnStop` hook at all, and Fx skips
   a nil one — so where everything else sits cannot matter.
2. The logger is forced during `fx.New` by `fx.WithLogger`, before any invoke
   runs and before any other constructor is asked for — so its hook is the first
   appended and the last run, unconditionally.
3. The SDK's two providers come next, because the pool takes the `Telemetry` they
   are reached through.
4. Everything that appends a hook after that is built **from** the pool: the
   loops through the application services and the outbox claims, the server
   through the API. A constructor cannot run before its dependencies.

Reversed, the order is:

```
the server drains, or the loops stop and give their work back
  -> the pool closes
    -> the spans and the measurements are flushed
      -> the last line is written
```

Each of those is the only order that works. A pool closed while a publisher is
still releasing its claims leaves those rows held until the hold expires — and
because the outbox is head-of-line per wallet, each held row is a wallet's whole
event stream waiting with it. Telemetry flushed before the components that emit
into it have stopped loses the last thing they said, which is the part of a
shutdown anybody reads.

Every worker's Stop reports what it did not finish, and that report is carried
out of the hook rather than logged and swallowed: an Fx Stop that returns an
error is a non-zero exit code, and a drain that ran out of time should not look
like a clean deployment.

## The HTTP API

`internal/adapters/http` (package `httpapi`) is the HTTP side of the two
application services. It routes, parses, authenticates, and maps what the
application layer answers onto status codes and bodies. It decides no business
rule and parses no value a provider sent.

Every response carries `X-Correlation-Id` and `Content-Type: application/json`,
except a router redirect, which carries only `Location`.

### Response mapping

`internal/app` classifies every error it returns. The class is what this adapter
maps; nothing here re-derives what happened.

| `app.Class` | Status | Notes |
|---|---|---|
| `INVALID` | 400 Bad Request | A malformed submission. Nothing was persisted and the idempotency key is still free. |
| `UNAUTHORIZED` | 401 / **403** | 401 when no principal was established (the credential was refused); **403** when one was and it may not do this. The application layer cannot tell these apart — it never sees a request that failed to authenticate. |
| `NOT_FOUND` | 404 Not Found | Also the answer for a provider reading another provider's operation, byte for byte. |
| `CONFLICT` | 409 Conflict | Covers both `WALLET_ALREADY_EXISTS` and `IDEMPOTENCY_PAYLOAD_CONFLICT` by one rule rather than two special cases. |
| `AUDIT` | 500 Internal Server Error | A finding about this service's stored records. The caller has nothing to correct and nothing to retry; the finding keeps its own code. |
| `RETRYABLE` | 503 Service Unavailable | Carries `Retry-After: 1`. |
| `UNRETRYABLE` | 500 Internal Server Error | Also where an error nobody classified goes. |
| `REJECTED` | — | Never arrives on an error. See below. |

Four statuses come from the transport rather than from a class, because no
business rule was consulted: **405** with `Allow` for a method a known path does
not answer, **413** for a body refused before it was read, **404** for a path no
route answers, and **307** with `Location` for a path the router can clean.

#### A submitted operation

A rejection is an outcome, not an error: it arrives on a **nil** error with an
`app.OperationResult` whose `Status` is `REJECTED` (ADR-0012). The status of a
submission therefore comes from the result.

| `OperationResult.Status` | Status |
|---|---|
| `PROCESSED` | 200 OK, with `idempotentReplay` set from the result |
| `PENDING_REFERENCE` | 202 Accepted |
| `REJECTED` | 422 Unprocessable Content |
| `PENDING`, `FAILED` | 200 OK |

**A read is always 200.** `GET /wagering/transactions/{transactionId}` and
`GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`
answer 200 whatever the operation came to: the read succeeded, and the `status`
member of the body says what was read. Reusing the submission mapping would mean
answering 422 — "I could not process your request" — to a request that was
processed perfectly, and 202 — "accepted for processing" — where nothing was
accepted and nothing is being processed.

### The error body

Every refusal has the same three members, always all three:

```json
{"code": "…", "message": "…", "correlationId": "…"}
```

- **`code`** is the `failure.Code` when the failure names a catalogued reason,
  and the `app.Class` when it does not. Not every refusal has a code, and
  inventing one for those would put entries in the external contract describing
  nothing a provider can act on, while leaving the member every caller parses
  sometimes absent. The three transport codes above (`NOT_FOUND`,
  `METHOD_NOT_ALLOWED`, `CONTENT_TOO_LARGE`) are the only additions.
- **`message`** is the failure's own words for the classes a caller can act on
  (`INVALID`, `CONFLICT`, `NOT_FOUND`, `UNAUTHORIZED`), with the offending field
  prefixed when the failure names one. Everything else answers with a fixed
  sentence, because its message comes from infrastructure and would carry
  whatever a driver, a socket or a query put in it.
- **`correlationId`** is the thread this request was handled under.

An error's cause chain is never rendered, in any form. `app.ErrForeignOperation`
and the refusals in `internal/adapters/oidc` both keep a distinction an operator
needs *inside* the error chain precisely because a rendered message does not
show it. Rendering the chain would publish both distinctions and turn a scoped
read back into the existence oracle it was built to deny.

### Correlation

`X-Correlation-Id` is accepted, echoed on every response, and propagated to the
use case, where it stamps the outbox envelopes and every log line. A request
that carries none is given one.

A supplied value is held to `wagering.opaque_id`'s shape — the column it is
eventually stored in — and what happens when it does not fit depends on what is
wrong with it:

- A control character, invalid UTF-8, or surrounding whitespace is **refused**
  with 400. The value is written into every log line the request produces and
  echoed in a response header, so a caller that could put a newline in it would
  be writing those log lines; quietly repairing that would silence exactly the
  case somebody needs to hear about.
- A value longer than 128 bytes is **replaced** and the request goes on. That
  bound is a storage fact this service never published, and failing a
  well-formed identifier against a number the caller cannot know would fail a
  good request. The replacement is logged and the minted value is echoed.

### The endpoints

Every route but the two health checks requires a bearer token. A request refused
for its credential never reaches the application layer.

---

#### `POST /wagering/transactions` — role `provider`, `Idempotency-Key` required

The key is read from the header and nowhere else, is never trimmed, and is never
computed from the body. Deriving one would make every distinct payload its own
key, which is the opposite of what the header is for.

The member names are exactly the ones `wagering.CanonicalPayload` hashes, so the
bytes a provider sends, the bytes that are hashed and the bytes that come back
are one vocabulary. `referenceExternalTransactionId` is optional.

```json
{"provider":"acme","externalTransactionId":"acme-tx-1","playerId":"player-1",
 "roundId":"round-1","gameId":"game-1","kind":"BET",
 "money":{"amount":"25.00","currency":"BRL"}}
```

**200 — processed**

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-1","kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

**200 — replay.** The same submission arriving again answers what the first one
answered, including the balance observed then rather than the wallet's balance
now.

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-1","kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":true}
```

**202 — waiting for its reference**

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-2","kind":"REFUND","status":"PENDING_REFERENCE","money":{"amount":"25.00","currency":"BRL"},"idempotentReplay":false}
```

**422 — rejected by a business rule.** Persisted, published, and the idempotency
key is bound to this payload for good.

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-3","kind":"BET","status":"REJECTED","money":{"amount":"25.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS","idempotentReplay":false}
```

**400 — validation**

```json
{"code":"INVALID_AMOUNT_SCALE","message":"money: must carry exactly two fraction digits, got \"25.0\"","correlationId":"01a0c512-b9b1-74d4-a232-7371b01dccc8"}
```

**400 — a body this endpoint cannot read.** Unknown members, a member named
twice, a value that is not an object and a second value after the first are all
refused rather than resolved.

```json
{"code":"INVALID_FIELD_FORMAT","message":"body: the request body names a field this endpoint does not have","correlationId":"01a0c512-b9b1-778d-8751-f1a7e3c13654"}
```

The other two: `body: the request body names the same field twice` and
`body: the request body is not a JSON object this endpoint can read`.

**400 — `OPENING` submitted as a kind.** The handler passes the kind through
untouched; `wagering.Command.Validate` refuses it before any transaction opens.

```json
{"code":"UNSUPPORTED_TRANSACTION_KIND","message":"kind: OPENING is raised only when a wallet is opened","correlationId":"01a0c513-79c5-70b4-a350-4858d3c449c2"}
```

**400 — no `Idempotency-Key`**

```json
{"code":"MISSING_REQUIRED_FIELD","message":"Idempotency-Key: a submission must carry the provider's key for it","correlationId":"01a0c512-b9b1-7887-bc02-98d2321340d3"}
```

**401 — no usable credential.** With `WWW-Authenticate: Bearer realm="wagering"`.
One sentence whichever of the five credential refusals happened: which one it
was is logged, never answered.

```json
{"code":"UNAUTHORIZED","message":"the request carries no usable credential","correlationId":"01a0c512-b9b1-7981-aede-2cfb9e9cd0ee"}
```

**409 — the key is bound to another payload**

```json
{"code":"IDEMPOTENCY_PAYLOAD_CONFLICT","message":"idempotency key \"acme-key-1\" is already bound to another operation","correlationId":"01a0c512-b9b1-7a7e-a26a-52633f063ab0"}
```

**503 — temporarily unable.** With `Retry-After: 1`.

```json
{"code":"RETRYABLE","message":"the service is temporarily unable to answer, try again","correlationId":"01a0c512-b9b1-7b5c-929d-7f6e627b05f7"}
```

**500 — permanently unable**

```json
{"code":"UNRETRYABLE","message":"the request could not be completed","correlationId":"01a0c512-b9b1-7c56-8017-6c09944f9a98"}
```

**413 — the body was larger than this service accepts**

```json
{"code":"CONTENT_TOO_LARGE","message":"the request body is larger than this service accepts","correlationId":"01a0c512-b9b3-7018-a756-56ecd6e25293"}
```

---

#### `GET /wagering/transactions/{transactionId}` — a provider sees its own, `internal` sees all

**200 — whatever the operation came to**

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-1","kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-3","kind":"BET","status":"REJECTED","money":{"amount":"25.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS","idempotentReplay":false}
```

**404 — absent, or belonging to another provider.** The two answers are
identical, byte for byte and header for header: the application layer builds
both from the same sentence and keeps the difference in the error chain, where
`errors.Is` finds it and a rendered message does not.

```json
{"code":"NOT_FOUND","message":"no operation \"acme-tx-1\"","correlationId":"01a0c512-b9b1-7ec4-b8aa-c4852b7e7e37"}
```

A refusal that carries no message of its own falls back to the class's fixed
sentence, `there is no such resource`.

**400 — an identifier that is not one**

```json
{"code":"INVALID_FIELD_FORMAT","message":"transactionId: \"not-a-uuid\" is not a UUID","correlationId":"01a0c513-79c5-7b1a-80f9-cea2d773cbb0"}
```

---

#### `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`

The provider comes from the path because an external id names an operation only
within the provider that issued it. `app.Principal.MayReadAs` decides whether
this caller may read as that provider: a provider may read only as itself, the
service may read as any.

**200** — the same operation view as above, for a provider naming itself or for
`internal` naming any provider.

**403 — a provider naming another provider.** Answered from the token and the
path alone, before anything is looked for, so it is the same answer for every
external id and confirms the existence of none of them. (404 would instead say
the operation is absent, which this route never went to find out.)

```json
{"code":"UNAUTHORIZED","message":"provider acme may not read as \"rival\"","correlationId":"01a0c512-b9b2-70c8-a383-1cdb047d05b5"}
```

**400 — a provider that is not a well-formed identifier**

```json
{"code":"INVALID_FIELD_FORMAT","message":"provider: must not be surrounded by whitespace","correlationId":"01a0c513-79c5-7da1-a440-fd274d36b4b2"}
```

---

#### `POST /wallets` — role `internal`

```json
{"playerId":"player-1","initialBalance":{"amount":"25.00","currency":"BRL"}}
```

`initialBalance` is required and names the currency, because a wallet is a
player's balance in one currency. A wallet with nothing in it is opened with
`"0.00"`.

**201 Created**, with `Location: /wallets/0199aa00-0000-7000-8000-000000000001`

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","playerId":"player-1","balance":{"amount":"25.00","currency":"BRL"},"version":1,"createdAt":"2026-09-21T12:00:00Z","updatedAt":"2026-09-21T12:00:00Z","opening":{"transactionId":"0199aa00-0000-7000-8000-000000000002","kind":"OPENING","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"25.00","currency":"BRL"},"idempotentReplay":false}}
```

`opening` is absent for a wallet opened at `"0.00"`: an opening records a
starting balance, and a wallet opened at zero has none.

**409 — the player already holds a wallet in that currency**

```json
{"code":"WALLET_ALREADY_EXISTS","message":"player \"player-1\" already holds a BRL wallet","correlationId":"01a0c512-b9b2-74b4-89e3-fd72c1b7cb18"}
```

---

#### `GET /wallets/{walletId}` — role `internal`

**200**

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","playerId":"player-1","balance":{"amount":"25.00","currency":"BRL"},"version":1,"createdAt":"2026-09-21T12:00:00Z","updatedAt":"2026-09-21T12:00:00Z"}
```

**403 — a provider asking**

```json
{"code":"UNAUTHORIZED","message":"provider acme may not administer wallets","correlationId":"01a0c512-b9b2-76e5-87cf-56927d5f6ed2"}
```

**404 — no such wallet**

```json
{"code":"NOT_FOUND","message":"no wallet 0199aa00-0000-7000-8000-000000000001","correlationId":"…"}
```

**400 — an identifier that is not one**

```json
{"code":"INVALID_FIELD_FORMAT","message":"walletId: \"not-a-uuid\" is not a UUID","correlationId":"01a0c513-79c5-7c6e-b299-0c8191cfdcf7"}
```

---

#### `GET /wallets/{walletId}/ledger?cursor=&limit=` — role `internal`

Oldest first, ordered by wallet version. The cursor is opaque and is carried
through untouched; the application layer decodes it and refuses one belonging to
another wallet. An absent `limit` is passed as unspecified so the application
layer's default applies; a `limit` that is not a whole number is 400.

**200**

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","entries":[{"ledgerEntryId":"0199aa00-0000-7000-8000-000000000003","transactionId":"0199aa00-0000-7000-8000-000000000002","direction":"CREDIT","money":{"amount":"25.00","currency":"BRL"},"balanceBefore":{"amount":"0.00","currency":"BRL"},"balanceAfter":{"amount":"25.00","currency":"BRL"},"walletVersion":1,"createdAt":"2026-09-21T12:00:00Z"}],"nextCursor":"MDE5OWFhMDAtMDAwMC03MDAwLTgwMDAtMDAwMDAwMDAwMDAxOjE"}
```

`nextCursor` is absent on the last page; its absence is how a caller knows to
stop. `entries` is `[]` on an empty page, never `null`.

**400 — a limit that is not a number**

```json
{"code":"INVALID_FIELD_FORMAT","message":"limit: must be a whole number","correlationId":"…"}
```

---

#### `POST /wallets/{walletId}/reconciliation` — role `internal`

**200 — whether or not the wallet balances.** The check ran and the report says
what it found; reconciliation never corrects anything. `difference` is stored
less reconstructed and may be negative.

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","consistent":false,"stored":{"amount":"20.00","currency":"BRL"},"reconstructed":{"amount":"25.00","currency":"BRL"},"difference":{"amount":"-5.00","currency":"BRL"}}
```

**500 — an audit finding.** The stored records are wrong in a way that is not a
mere imbalance. The finding keeps its own code.

```json
{"code":"LEDGER_BALANCE_MISMATCH","message":"the stored records for this wallet disagree and need operator attention","correlationId":"01a0c512-b9b2-7a8f-aa69-8cab89c842eb"}
```

---

#### `GET /health/live` — public

**200.** Reports on the process and asks nothing of anything else: a liveness
probe that checked a dependency restarts a healthy process whenever that
dependency is down, and does it to every replica at once.

```json
{"status":"alive"}
```

#### `GET /health/ready` — public

The checks run at the same time under one total budget, and every one of them
runs, so the body names them all. A failing check's error is logged and never
published: a readiness endpoint is usually reachable by more of a network than
the service is.

**200**

```json
{"status":"ready","checks":{"postgres":"ok","sqs":"ok"}}
```

**503**, with `Retry-After: 1`

```json
{"status":"unready","checks":{"postgres":"ok","sqs":"failed"}}
```

---

#### Router responses

**404 — no route answers this path**

```json
{"code":"NOT_FOUND","message":"no route answers this path","correlationId":"01a0c512-b9b2-7e9f-804e-b44f887e8770"}
```

**405**, with `Allow: POST`

```json
{"code":"METHOD_NOT_ALLOWED","message":"this path does not answer GET","correlationId":"01a0c512-b9b2-7f4f-9a40-cc6edcbaca57"}
```

**307**, with `Location: /wallets` and no body, for a path the router can clean
(`//wallets`, `/wallets/x/../y`). 307 preserves the method, so a submission
survives it; the HTML page the router would have drawn does not, because it is
the one representation this API never serves.

---

## Observability

`docker-compose.yml` brings up four more containers beside the service: an
OpenTelemetry Collector, Tempo, Prometheus and Grafana. Every API replica and
every worker exports over OTLP/gRPC to the collector and knows nothing else
about any of them — one address, `OTEL_EXPORTER_OTLP_ENDPOINT`. The collector
forwards traces to Tempo and holds metrics on `:8889`, which Prometheus scrapes
every five seconds. Their configuration is in `deploy/otel`, `deploy/tempo`,
`deploy/prometheus` and `deploy/grafana`.

That variable is **empty by default**, and empty means nothing is exported at
all: a process with no collector to talk to does not retry one. The compose
stack sets it; `.env.example` sets it to the same collector seen from the host.

| | Address | |
|---|---|---|
| Grafana | <http://localhost:3000> | the dashboard, already loaded |
| Prometheus | <http://localhost:9090> | |
| Tempo | <http://localhost:3200> | the search API below |
| Collector | `localhost:4317` gRPC, `4318` HTTP | what the service exports to |

### The SDK

The OpenTelemetry SDK is built in `internal/fxmod` and nowhere else. Every
component in this tree takes a `telemetry.Telemetry` and describes its own work
through it; which providers carry that — the real exporters, or OpenTelemetry's
no-ops when there is nowhere to export to — is the composition root's decision
and is made from the environment. `OTEL_SDK_DISABLED=true` switches the SDK off
entirely, and an empty endpoint means nothing is exported; the difference
between them is that a disabled process makes no network call and writes no
error, where a process pointed at a collector that is not answering keeps
running and reports each failed export.

There is **no sampler**, so the provider is `ParentBased(AlwaysSample)`: every
trace this service starts is recorded, and a trace arriving with a sampled
`traceparent` is honoured. That is a deliberate deferral, and the number it
defers is large enough to be written down in `fxmod.Telemetry` — an *idle*
worker replica exports of the order of eight hundred thousand spans a day,
nearly all of it the two one-second loops. A head sampler in the process throws
away the traces an operator most wants, because whether a trace is interesting
is known at its **end**; the collector is where that decision belongs, and
`deploy/otel/collector.yaml` carries no `tail_sampling` for a reason it states
in place — a laptop keeps everything, because the trace somebody is
demonstrating is never the one a rule would have kept.

An idle **API** replica exports no spans at all: the only ones it opens are per
request, and the two health endpoints are filtered out before `otelhttp` sees
them — otherwise a three-second container healthcheck would be the whole of the
trace store.

### Trace propagation

One operation is one trace across three hand-overs, each of which had to be made
deliberately because each crosses a process boundary.

| Hand-over | How |
|---|---|
| **HTTP in** | `otelhttp` extracts W3C `traceparent` from the request headers. A provider that sends none starts a new trace at the server span. |
| **Use case to SQL** | in-process context. The transaction gets a `postgres movement` span and each statement a `postgres.query` child, so the time a movement spent waiting for a wallet lock is visible rather than inferred. |
| **Outbox** | the adapter writes the W3C trace context into the event's `jsonb` payload under **`$trace`** — the only place a value can be added without a migration — and the claim takes it back out with `payload - '$trace'`. The publisher runs minutes later, in another process, with no memory of the request, so without this the event's publication would be a trace of its own. **Nothing published ever carries it.** |
| **SQS out and in** | the propagator injects into **message attributes**, beside `correlationId` and `causationId`. Trace context rides beside the body rather than only inside it so that anything routing, filtering or logging a message can follow a thread without parsing a financial payload it has no business reading. |

The publish span is therefore a **child** of the trace the event was stored
under, and is **linked** to the batch that carried it — a parent for causality,
a link for the batching, which are genuinely different relations.

The thread a person follows is `correlationId` — the value `X-Correlation-Id`
echoes and the error body returns. It is set on the span that **opens** each
door, and on those only: the HTTP server span, the consume span and the
publish-event span. Nothing beneath writes it again, because a value written
twice is a value that can differ; a TraceQL match on any one span returns the
whole trace, which is what makes searching by it work anyway. Log lines carry
`traceId` and `spanId` instead, which is how a line is tied back to the span
that produced it.

On the queue path the correlation is the `correlationId` message attribute when
the producer set a usable one, and the envelope's `messageId` when it did not. A
value the `opaque_id` domain would refuse is **replaced** there rather than
refused — a queue message has nobody to hand a refusal to, and refusing it would
send a perfectly good operation to the dead-letter queue over a field that
identifies nothing but a log line. The HTTP path refuses a *dangerous* value
instead, because there a caller is waiting; an over-long one is replaced on both
paths alike.

### The metric catalogue

Every instrument, its type, and the attributes it divides by. The names below
are the OpenTelemetry names; Prometheus renders them with `.` as `_`, appends
`_total` to a counter and `_seconds` to a duration, so
`wagering.processing.duration` is scraped as
`wagering_processing_duration_seconds_bucket`.

| Instrument | Type | Attributes |
|---|---|---|
| `wagering.transactions` | counter | `source`, `kind`, `status`, `failureCode` |
| `wagering.idempotent_replays` | counter | `source`, `kind` |
| `wagering.processing.duration` | histogram, **seconds** | `source`, `kind`, `status`, `failureCode` |
| `wagering.inbox.duplicates` | counter | `consumer` |
| `wagering.sqs.retries` | counter | `consumer`, `class` |
| `wagering.sqs.dead_letters` | counter | `consumer` |
| `wagering.lock_timeouts` | counter | `transaction` — `movement` or `snapshot` |
| `wagering.version_conflicts` | counter | `transaction` |
| `wagering.outbox.lag` | observable gauge, **seconds** | none |
| `wagering.outbox.publish_attempts` | counter | `outcome` — `published` or `refused` |
| `wagering.reconciliation.divergences` | counter | none |

`source` is one of `http`, `sqs` or `reference`: three doors into the write
path, and three answers to "where did this come from".

`failureCode` carries the literal **`NONE`** for an operation that was not
rejected, rather than being absent. A counter's attribute set decides its time
series, so an instrument that sometimes carries a key and sometimes does not is
two series that no dashboard adds up — and "processed operations" would silently
exclude nothing while looking like it excluded something.

**No instrument here carries an amount, a balance or a player, and none ever
will.** A metric label is a dimension of a time series, and an amount has as
many distinct values as there are amounts. What these carry is what a dashboard
divides by; what an amount belongs to is a ledger.

Beside the attributes above, every series arrives carrying the resource — the
scraped sample is `exported_job="wagering"`, `exported_instance="<hostname>"`,
`service_name`, `service_instance_id` and the SDK's own three. The two
`exported_` renames are Prometheus's collision rule and are explained under
**The dashboard**.

`wagering.transactions` does **not** count failures. An operation that could
not be applied has no kind, no status and no failure code, and counting it here
would put a database outage in the same series as a rejected bet.

`wagering.lock_timeouts` and `wagering.version_conflicts` are kept apart
although both are contention, because they mean opposite things: a lock timeout
is the lock *working* and the wait being too long; a version conflict is the
lock **not** having been held, which should be unreachable.

`wagering.outbox.publish_attempts` deliberately does **not** name the publisher,
although every span and every log line about a publish does. A publisher's name
must be distinct per process and falls back to `HOSTNAME` — a pod name with a
random suffix, a container id — which as a metric attribute is an unbounded
label: the series count grows by two per pod lifetime for ever, and Prometheus
does not reclaim them inside its retention. Dropping it from the instrument and
putting `service.instance.id` on the **resource** are the same rule read in two
directions: process identity belongs where Prometheus expects it, not
multiplying a business counter's series.

`wagering.outbox.lag` is **observed on collection** rather than recorded on a
loop, so a publisher whose sends are all failing still reports a growing
backlog: a gauge written from a turn's own work would be silent in exactly the
outage it exists to show. An outbox with nothing waiting is observed as **zero**;
a callback that **fails** records nothing at all for that interval, because a
zero there would mean "the outbox is empty", which is the opposite of what a
database that would not answer is evidence for.

`wagering.processing.duration` names its bucket boundaries explicitly, in
seconds, and that is the whole instrument rather than a refinement of it. See
**The dashboard** below for what happened when it did not.

### The dashboard

Grafana starts with both datasources and the dashboard **Wagering service**
provisioned from `deploy/grafana`. There is nothing to import and nothing to
configure. `make dashboards` prints the address and the credentials.

Those credentials are `GRAFANA_USER` and `GRAFANA_PASSWORD` in `.env.example` —
`admin` / `admin`, which are `docker-compose.yml`'s `GF_SECURITY_ADMIN_USER` and
`GF_SECURITY_ADMIN_PASSWORD`. They are placeholders in a repository, on a stack
reachable from nowhere but the laptop running it. **Reading the dashboard needs
no sign-in**: anonymous access is on, and the credentials are what anything that
writes has to present.

The panels, and the query behind each. Every selector below is
`{exported_job="wagering"}`, left out to keep the column readable:

| Panel | Query |
|---|---|
| Transaction outcomes | `sum by (status) (rate(wagering_transactions_total[$__rate_interval]))` |
| Outcomes since start | `sum by (status) (wagering_transactions_total)` |
| Rejections by failure code | `sum by (failureCode) (wagering_transactions_total{status="REJECTED"})` |
| Processing latency percentiles | `histogram_quantile(0.50 \| 0.95 \| 0.99, sum by (le) (rate(wagering_processing_duration_seconds_bucket[$__rate_interval])))` |
| Idempotent replays | `sum by (source, kind) (rate(wagering_idempotent_replays_total[$__rate_interval]))` |
| Inbox duplicates | `sum by (consumer) (rate(wagering_inbox_duplicates_total[$__rate_interval]))` |
| Lock conflicts | `sum by (transaction) (rate(wagering_lock_timeouts_total[$__rate_interval]))`, and the same over `wagering_version_conflicts_total` |
| Outbox lag | `max(wagering_outbox_lag_seconds)` |
| Outbox publish attempts | `sum by (outcome) (rate(wagering_outbox_publish_attempts_total[$__rate_interval]))` |
| SQS retries | `sum by (class) (rate(wagering_sqs_retries_total[$__rate_interval]))` |
| Dead letters | `sum(wagering_sqs_dead_letters_total) or vector(0)` |
| Reconciliation divergences | `sum(wagering_reconciliation_divergences_total) or vector(0)` |

The last two are the panels that must read zero. `or vector(0)` is there
because a counter that has never been incremented has no series at all, and a
panel that says "No data" where it should say "none" is a panel nobody believes
the second time.

**`exported_job`, not `job`.** The collector maps each service's `service.name`
onto a `job` label, so everything it exports arrives labelled `job="wagering"`.
That collides with the name of Prometheus's own scrape job, and Prometheus keeps
its own and renames the incoming one. Every query above therefore selects
`exported_job="wagering"`; `job="wagering"` matches nothing, silently. The same
series also carries `service_name="wagering"`, from the resource attribute the
collector copies onto each metric.

**`exported_instance` is the replica**, by the same rule twice over. Each process
reports a `service.instance.id` — its hostname, which the container runtime makes
distinct per replica — the collector maps that onto `instance`, and Prometheus
renames it where it meets the scrape target's own, which is
`otel-collector:8889`. Nothing above selects it: every query aggregates across
processes with `sum by (...)` or `max(...)`, which is what makes three API
replicas one number. A panel that wanted one replica would select
`exported_instance`, and that is also the label that says how many processes are
reporting at all.

A rate over a counter that appeared once and never moved is zero — Prometheus
cannot tell a counter's first sample from a counter that was always at that
value. Three requests by hand therefore leave the rate panels flat; "Outcomes
since start" is the panel that shows them at all. Traffic spread over more than
one scrape interval fills the rest.

This dashboard found two defects in the instrumentation that no test had,
because both let the export succeed and only the numbers were wrong: the latency
histogram carried millisecond-shaped default buckets for a value recorded in
seconds, so every quantile was an interpolation inside one bucket that held
everything and p50 read 2.5s where the mean was 0.006s; and every process
exported the same resource identity, so five of them were one series and three
BETs, one per replica, moved the counter by one. Both are fixed — the boundaries
are named in the unit the instrument declares, and the resource carries
`service.instance.id`. Neither fix changed a query here, which is the argument
for reading a dashboard against a system you can make do something.

### Finding a trace by `correlationId`

Every span carries `correlationId`: the value `X-Correlation-Id` echoes, the
error body returns and every log line names. One trace spans the HTTP request,
the use case, the SQL transaction and the wallet events the outbox published
from it afterwards — the publish span is a child of the trace the event was
stored under, and is linked to the batch that carried it.

**In Grafana** — *Explore*, the **Tempo** datasource, the **TraceQL** tab:

```
{ .correlationId = "8d126071-1a25-4831-af62-7e61e80c59cb" }
```

The same query works on `.transactionId`, `.walletId`, `.providerId`,
`.messageId` and `.eventId`.

**From a shell**:

```
make trace CORRELATION=8d126071-1a25-4831-af62-7e61e80c59cb
```

which is Tempo's search API, and the whole of it:

```
curl --get --data-urlencode 'q={ .correlationId = "…" }' \
     --data "start=$(( $(date +%s) - 3600 ))" \
     --data "end=$(date +%s)" \
     http://localhost:3200/api/search
```

`start` and `end` are not optional. Outside its default window Tempo answers an
empty result rather than an error, so a trace that is not there and a trace that
is there but older look exactly alike. `GET /api/traces/<traceID>` then returns
the trace itself, in OTLP JSON — where the trace and span identifiers are
**base64**, not the hex the search result just printed.

One BET, found that way, is nineteen spans:

```
SPAN_KIND_SERVER    POST /wagering/transactions   correlationId=… kind=BET transactionId=…
SPAN_KIND_INTERNAL  Wagering.Submit
SPAN_KIND_CLIENT    postgres movement
SPAN_KIND_CLIENT    postgres.query                ×13
SPAN_KIND_PRODUCER  publish wallet event          eventId=…  → links to `publish outbox batch`
SPAN_KIND_PRODUCER  publish wallet event          eventId=…
```

The two producer spans are two seconds after the server span closed, in the
worker, in another container. They are in this trace because that is where the
event came from.

## Failure codes

Twenty-five codes, and the reason the catalogue is worth a table rather than a
list is that it carries **two independent axes**. Every refusal in the domain
names one; the string form is part of the external contract and does not change
once published.

- **`Correctable()`** — the submission was malformed. **Nothing is persisted**,
  so the idempotency key is still free and a provider may repair the payload and
  send it again under the same key. This is exactly the question *"did we persist
  a payload hash under this key?"*.
- **`Definitive()`** — a business rule settled it. A rejected wager transaction
  **is** persisted, `WagerTransactionRejected` is emitted, and the key is bound to
  that payload for good; a corrected resubmission under it is
  `IDEMPOTENCY_PAYLOAD_CONFLICT`.
- **`Audit()`** — the code reports on **stored state** rather than on a
  submission. Audit codes are definitive, since there is no payload to repair, but
  they settle nothing, because no operation was in flight for them to settle.

The two axes exist so the first one stays honest. Without the second,
"definitive" would have to mean *binds the idempotency key* for every member and
*there is no key* for one of them — a predicate that means different things for
different members of its own domain has stopped being a classification.
`WagerTransaction.Reject` gates on definitive **and not audit**, so a caller
deciding whether a code may settle a transaction needs both halves.

Membership in both maps is explicit rather than inferred. `correctable` is
**total** over the catalogue and a test says so, so a code added later cannot
inherit a classification by accident; `audit` lists only its members, and its
test asserts that everything in it is definitive and not correctable, and that
the axis separates something at all. The same two columns are seeded into
`wagering.failure_code`, and `wagering.settling_failure_code` is **derived**
from them — a transaction's `failure_code` is a foreign key into that subset,
so a correctable or audit code cannot be stored as one. Writing the twelve
settling codes out a second time would be a second place for the classification
to live.

### Correctable — nothing was persisted, the key is free

| Code | |
|---|---|
| `UNINITIALIZED_VALUE` | a domain value never built through its validating constructor |
| `INVALID_AMOUNT_FORMAT` | not a plain non-negative decimal: a sign, an exponent, `NaN`, a leading zero, whitespace, a missing part |
| `INVALID_AMOUNT_SCALE` | a well-formed decimal with the wrong number of fraction digits, too few or too many |
| `AMOUNT_OUT_OF_RANGE` | an amount that cannot be represented, or arithmetic that would overflow the minor-unit representation |
| `INVALID_AMOUNT_FOR_KIND` | zero for a `BET`, `WIN`, `REFUND` or `ROLLBACK`; non-zero for a `LOSS` |
| `UNSUPPORTED_CURRENCY` | not three uppercase ASCII letters |
| `MISSING_REQUIRED_FIELD` | a field the kind requires, absent or empty |
| `INVALID_FIELD_FORMAT` | present but malformed: surrounding whitespace, an over-long identifier, an unknown enum value |
| `UNSUPPORTED_TRANSACTION_KIND` | an `OPENING` submitted as an external operation |
| `REFERENCE_REQUIRED` | a `REFUND` or `ROLLBACK` naming nothing to reverse |
| `REFERENCE_NOT_APPLICABLE` | a reference on a kind that cannot carry one |
| `INVALID_STATE_TRANSITION` | a status a transaction cannot reach, including any move out of a terminal one |

### Definitive — a row exists, an event was published, the key is spent

| Code | |
|---|---|
| `INSUFFICIENT_FUNDS` | a `BET` whose debit would take the wallet below zero |
| `REVERSAL_INSUFFICIENT_FUNDS` | a **reversal** whose debit would. Deliberately distinct: a player betting beyond their balance is routine, whereas a reversal that cannot be applied means money has already left the wallet and an operator needs to know. |
| `BALANCE_OUT_OF_RANGE` | the resulting balance would not be representable — the ceiling to `INSUFFICIENT_FUNDS`'s floor |
| `CURRENCY_MISMATCH` | money in a currency the wallet is not denominated in, or arithmetic across two |
| `REFERENCE_NOT_FOUND` | the reference never arrived before the wait budget was spent |
| `REFERENCE_NOT_PROCESSED` | the reference exists but ended unsuccessfully, so there is nothing to reverse |
| `REFERENCE_NOT_REVERSIBLE` | the reference's kind cannot be reversed by the submitted kind |
| `REFERENCE_ALREADY_REVERSED` | the reference already carries an active reversal, which would return the same money twice |
| `REFERENCE_MISMATCH` | the reference disagrees on provider, player, wallet, currency or round |
| `REVERSAL_AMOUNT_MISMATCH` | a reversal of a different amount. Partial reversals do not exist. |
| `IDEMPOTENCY_PAYLOAD_CONFLICT` | one key reused for a different set of business fields |
| `WALLET_ALREADY_EXISTS` | a second wallet for a player and currency that already has one |

The last two are the pair that arrive as **errors** rather than as rejections,
because in both cases nothing new was persisted: something already recorded says
otherwise. They keep their codes, because a provider can act on them, and they
are classified `CONFLICT` apart from the codes that settle an operation.

### Audit — a finding about stored state

| Code | |
|---|---|
| `LEDGER_BALANCE_MISMATCH` | a wallet whose stored balance does not equal its ledger summed, credits less debits, including the opening |

**A `failure.Code.Audit()` code and an `app.Class` of `AUDIT` are not the same
predicate**, and must not be used interchangeably. `wagering.Reconcile` reports
five things about stored state and only that one carries an audit *code*. An
entry that was never constructed is `UNINITIALIZED_VALUE` and a duplicated
ledger entry is `INVALID_FIELD_FORMAT`, both of which are **correctable**; an
entry belonging to another wallet is `REFERENCE_MISMATCH` and an entry in the
wrong currency is `CURRENCY_MISMATCH`, both definitive and neither an audit
code. The application layer decides that a finding is an audit finding from
**where it was found** — it read stored state, so whatever it found is an
operator's problem — and sets the class explicitly, keeping the finding's own
code and field.

Moving those four codes into the audit map would not work, and the reason is
worth stating so it is not attempted twice: `CURRENCY_MISMATCH` and
`REFERENCE_MISMATCH` are live **settling** reasons on the submission path, and
`Reject` refuses audit codes — so marking either would make the domain refuse
legitimate business rejections. The alternative — four new codes, for findings
no provider can act on and none will ever see — would take the audit axis from
one member to five in order to describe conditions with no external audience at
all.

The rule a caller follows is therefore: **read the class; the code names what
was found, not what to do about it.** `failure.Correctable` must not be
consulted on an error already classified `AUDIT` — it returns `true` for a
duplicated ledger entry, which would read as "the provider may repair the
payload and resubmit" for a finding that has no payload and no provider.
ADR-0006 and ADR-0013.

### And the one class no error ever carries

`app.ClassOf` maps any error to one of eight classes and produces seven of them.
**`REJECTED` is never derived from an error.** A rejection exists when the
processor returned a **nil** error and the transaction's status is `REJECTED`;
every `failure.Code` arriving on an error path means the submission never became
anything.

The mapping writes itself wrongly, which is why it is stated as a rule rather
than fixed by ordering the checks: correctable derives to `INVALID` and an
audit code to `AUDIT`, so the obvious next line is "definitive means
`REJECTED`" — and under it, asking the service to reconcile a wallet whose
books do not balance returns a *rejected wager operation*. There is no
operation and no provider waiting; the audience is an operator.
`TestNoErrorEverClassifiesAsRejected` walks the whole catalogue and asserts it
for every code, so no code added later can acquire `REJECTED` from the
derivation. ADR-0002 and ADR-0012.

## Interpretations

The original challenge specification is not in this repository and could not be
recovered, so its section 9 — "contracts exactly as in the challenge spec" —
does not exist. The following were derived from the task text plus the
`internal/app` surface. Each names what was chosen and why.

1. **The HTTP package is `httpapi` in `internal/adapters/http`.** The directory
   is what the task specifies. The package is not called `http` because every
   file would then have to alias `net/http`, and `stdhttp.StatusOK` beside
   `http.StatusOK` in a package whose whole subject is HTTP is a rename waiting
   to be got wrong.

2. **`code` is the `failure.Code` when the failure carries one, and the
   `app.Class` when it does not.** Both are already published vocabularies. No
   code was invented for refusals that name none.

3. **`message` renders the failure's own words only for `INVALID`, `CONFLICT`,
   `NOT_FOUND` and `UNAUTHORIZED`.** The rest answer with a fixed sentence per
   class. The `app.Error` head — class, code, field — is stripped before the
   message is rendered, because the body carries the first two in a member of
   their own, and the field is put back in front of the sentence.

4. **The error body has exactly three members.** The offending field is prefixed
   to the message rather than added as a fourth, so that one shape stays one
   shape and a caller parsing a refusal never has to ask whether this one has
   the extra key.

5. **401 for a credential that was refused, 403 for a principal that may not do
   this.** `app.Unauthorized` covers both and the application layer cannot tell
   them apart, because it never sees a request that failed to authenticate.

6. **The 401 message is fixed.** Which of the five credential refusals happened
   is logged and never answered. Nothing is withheld that the holder of a token
   could not already read out of it; what is withheld is the one distinction
   they could not observe — a signature that did not check out and a key this
   process could not obtain.

7. **`AUDIT` is 500, keeping the finding's own code.** The finding is about this
   service's stored records: the caller has nothing to correct and nothing to
   retry, and repeating the request finds the same records.

8. **A submission's status says what it came to; a read is always 200.** The
   rule in the task text is stated over the outcome of a submission. Applying it
   to reads as well would answer 422 to a request that was processed perfectly
   and 202 where nothing was accepted, so any client with generic HTTP error
   handling would treat a successful read as a failure. The branch a provider
   needs — `status` in the body — is there either way.

9. **`POST /wallets` answers 201 with `Location`.** The statuses the task
   enumerates describe what a submitted *operation* came to, and opening a
   wallet is not one of them; 201 is what a POST that creates an addressable
   resource returns. The 409 in that same list is the conflict half of the same
   story and is answered the way every conflict here is.

10. **`POST /wallets/{id}/reconciliation` answers 200 even when the wallet does
    not balance.** The check ran; the report is the answer. It is a POST because
    the reconciliation is being produced, and because a check that reads every
    entry a wallet has is not something a caching intermediary should be invited
    to repeat on its own.

11. **Three transport codes exist beside the catalogue** — `NOT_FOUND`,
    `METHOD_NOT_ALLOWED`, `CONTENT_TOO_LARGE` — with **413** for an oversized
    body and **405 with `Allow`** for a method mismatch. Neither has an
    application class behind it: nothing was submitted, so nothing was invalid,
    rejected or in conflict.

12. **The router's own responses are intercepted and restated.** `ServeMux`
    composes three answers of its own — no route, a method mismatch, and a
    redirect for a path it can clean. A catch-all `/` was rejected as the fix:
    a pattern with no method matches every method, so it would capture the miss
    and turn every method mismatch into one too. The two refusals are rewritten
    in the contract's shape; the redirect keeps its status and `Location` and
    loses its HTML body.

13. **A supplied `X-Correlation-Id` is refused when it is dangerous and replaced
    when it is merely too long.** Control characters, invalid UTF-8 and
    surrounding whitespace are 400: the value is written into every log line the
    request produces, so quietly repairing it would silence a log-injection
    attempt. Over 128 bytes is replaced and logged: that bound is
    `wagering.opaque_id`'s and this service never published it, so failing a
    well-formed identifier against it would fail a good request over a storage
    fact.

14. **`OPENING` is refused by the domain, not by the handler.** The kind is
    passed through unmodified and `wagering.Command.Validate` — reached through
    `PayloadHash`, before any I/O — answers `UNSUPPORTED_TRANSACTION_KIND`.
    Duplicating the rule in the transport could only drift from it.

15. **The submission body carries `provider`.** The handler does not fill it
    from the token, so `app.Principal.MaySubmitAs` remains a live check rather
    than one trivially satisfied by the transport.

16. **`app.Principal.MayReadAs` was added to `internal/app`, and
    `Wagering.TransactionByExternalID` now takes the provider it reads as.**
    This is the one change made to the application layer, and it was made
    because the task requires `internal` to read all three of the wagering
    routes and the layer could not answer the by-external-id one: it derived the
    provider from the principal, so the service — which names no provider — was
    refused outright, and `TransactionByID` does not rescue the case because it
    needs a transaction id, which is exactly what a caller holding an external
    id has not got. The port underneath (`TransactionReader.ByExternal`) already
    took the provider explicitly and the route already carried it in the path.
    `MayReadAs` is deliberately not `MaySubmitAs`: a provider reads and submits
    only as itself, but the service reads as any provider although it submits as
    none, because reading takes nothing and moves nothing. A provider naming
    another provider is refused before any row is looked for, so the scoping
    guarantee is unchanged. Nothing else in `internal/app` was touched.

17. **Money crosses the wire through one view built from `money.Money`'s own
    rendering**, never by marshalling `money.Money` itself. The reconciliation
    difference is the system's one signed amount and `money.Money` refuses to
    marshal a negative, so one amount on this contract cannot be written by the
    type that writes the others — and a package with two ways to write an amount
    would eventually write that one the wrong way. No float appears on any path.

18. **`Idempotency-Key` is a header and never a body member.** An
    `idempotencyKey` in the body is an unknown member and is a 400. The header
    is never trimmed and never computed.

19. **`Retry-After` is the constant `1`.** A retryable failure here is a lost
    connection, a statement timeout or a lock conflict, all of which are over in
    milliseconds. A caller that needs a different pace has its own backoff.

20. **`net/http`'s `ServeMux` was chosen over a routing dependency.** Its
    method-and-wildcard patterns match every route this API has.

The rest of the system made its own, in the same way and for the same reason.

### Structure

21. **`internal/storage/postgres` and `internal/adapters/postgres` are two
    packages.** The first keeps the migration runner and the schema-conformance
    suite; the second is the repository adapter the application layer's ports are
    implemented in. Two packages, two jobs, and a schema test that fails says so
    without implicating the adapter.

22. **`internal/adapters/sqs` is `package sqs`, with the AWS SDK aliased
    `awssqs` internally.** The same collision as `httpapi`, solved the other way
    round, because here the caller's reading is what matters: `sqs.NewQueue` in a
    composition root says what it is. The composition root aliases the SDK the
    same way where it needs it, which is one import line beside the adapter's.

23. **Integration suites that span packages live in packages of their own** —
    `internal/integration` for the authenticated HTTP path, `internal/messaging`
    for the queue path, `internal/multi` for more than one process. A failure's
    first question is "whose fault?", and the file's location should not answer
    it wrongly.

### Persistence

24. **`TxManager` does not carry the transaction in the context**, against the
    task text's wording. The port forbids it in prose and the application layer
    is tested against that port: a repository reached through the `*Repos` bundle
    is already bound to the open transaction, so one obtained anywhere else
    cannot accidentally run outside it. The compiler enforces what the
    context-carried idiom can only hope for. The adapter still refuses writes
    outside a transaction — it has no other way to be constructed — which is the
    guarantee the task text was asking for.

25. **`Record` uses `INSERT … ON CONFLICT DO NOTHING`** rather than catching
    `23505`. One round trip, it blocks on an in-flight duplicate until that
    duplicate commits, and it leaves the transaction usable — where a caught
    unique violation has already aborted it.

26. **`Settle` writes the wallet before the ledger entry**, so the
    version-conditioned update is the first gate on a stale balance and the
    failure names the real fault. The wager transaction is written before either,
    because the `active_reversal` trigger fires on its `UPDATE` and must fire
    while the wallet is already held.

27. **The due-work claim is a plain `SELECT`, not `FOR UPDATE SKIP LOCKED`**,
    against the task text. ADR-0011 records `SKIP LOCKED` as considered and
    rejected: it reintroduces the resume/submit deadlock, which was demonstrated
    rather than predicted and is the common case rather than a corner of one.
    `SKIP LOCKED` is used on the **outbox** claim, where the claimed row is the
    whole write set and no such cycle exists.

28. **A refused connection is transient whatever SQLSTATE the server refused
    with** — answered structurally, by testing for a connect failure first,
    rather than by a longer list of codes. Enumerating `55000` would have been
    wrong rather than merely incomplete: it means "not accepting connections"
    from a connection attempt and "an object is in the wrong state" from a
    statement, and only whether a connection was established tells them apart.

### Authentication

29. **Roles are realm roles, read from `realm_access.roles`.**
    `resource_access` is never read and neither is `azp`. Reading a role out of
    `resource_access` means choosing the bucket with `azp`, which puts the
    identity decision back on the claim that was rejected for making it.

30. **The algorithm allow-list is the whole RSA and ECDSA family, not RS256
    alone.** What closes the confusion attack is asymmetry, applied at the JOSE
    header before a key is looked up; narrowing further buys nothing extra and
    costs an outage the day the realm rotates to a different family.

31. **The JWKS address must not be reached through a redirect.** The adapter
    copies the supplied client and overrides `CheckRedirect`, rather than
    documenting a requirement a caller could get wrong — the origin checks test a
    URL, and a client that followed redirects would be guarding an address and
    not a destination.

### Messaging

32. **The envelope `type` has one accepted value, `WagerTransactionSubmitted`.**
    The spelling is this implementation's. It is PascalCase to match the event
    types the outbound envelope already carries, and it names what the message
    *is* — a provider asking for an operation to be applied — rather than which
    operation, because the kind lives in `data.kind` exactly as on HTTP and one
    payload should not be described in two places.

33. **The inbox body hash is SHA-256 of the raw body bytes, not of the parsed
    fields.** The idempotency key answers "has this operation been recorded"; the
    inbox answers "has this *message* been handled". Only a raw-byte hash makes
    one `messageId` carrying two different bodies detectable.

34. **The principal on the queue path is a provider principal minted from
    `data.provider`.** There is no token on a queue, so the queue is the
    authorisation boundary and who may send to it is a deployment's decision.

35. **`app.NotFound` is transient on the queue path.** Nothing is persisted and
    the key is still free, so the same message succeeds unchanged once the wallet
    is opened — and the only way this layer produces it there is an operation for
    a player who holds no wallet in that currency. It stays bounded by the redrive
    policy.

36. **A permanent failure leaves its message alone rather than releasing it to
    zero.** Releasing would spend the whole delivery budget in one burst, turning
    five chances into one.

37. **`ContentBasedDeduplication` is off on all three queues.** The envelope's
    own `messageId` is the identity the inbox keys on, and a body differing only
    in whitespace must not become a second message.

38. **Consumer concurrency is N independent receivers, each handling its batch
    strictly serially, and nothing reads `MessageGroupId`.** SQS holds a group
    while any of its messages is in flight and tracks *that* rather than who holds
    it, so the ordering guarantee reaches across replicas. The queue adapter does
    not carry the group and could not be asked to without `internal/workers`
    deciding what it would do with it.

### Composition and deployment

39. **A worker with all three loops disabled is refused.** A process that holds
    a pool open, reports itself up and consumes nothing is indistinguishable from
    a healthy deployment.

40. **SQS start-up leniency differs by binary.** A queue that does not exist is
    `Unretryable` and refuses to start everywhere. A queue that is unreachable is
    `Retryable` and leaves `cmd/api` up and unready — the outbox is the buffer
    that exists for exactly this — but stops `cmd/worker`, which serves no
    readiness probe and whose every loop *is* the queue.

41. **`CONSUMER_NAME` is identical across replicas and is never derived from
    `HOSTNAME`; `PUBLISHER_NAME` has no fixed default at all.** The first is half
    of an inbox row's identity, so a per-replica name lets one message be applied
    once by each replica. The second scopes a claim, so a shared name lets two
    publishers put back each other's rows — and a fixed default would collide
    silently the day a second replica deployed. It falls back to `HOSTNAME`, then
    refuses to start.

42. **A variable that is set and empty counts as unset**, so `FOO=${BAR}` with no
    `BAR` does not defeat a default. `Load` collects every problem it finds and
    reports them together.

43. **Compose deliberately does not read `.env.example`.** That file is the
    host's view of these addresses, and pointing `env_file` at it would also
    inherit one `PUBLISHER_NAME` into both worker replicas.

44. **Three API replicas are three service definitions sharing a YAML anchor,
    not `deploy.replicas: 3`.** Replicas of one service all publish the same host
    port, and a port range binds but assigns by start order — which a
    demonstration that submits to one replica and reads back from another cannot
    name.

45. **The worker has no healthcheck.** It serves nothing, and its start-up — the
    pool, the queue resolution, the loops — is its check.

## Limitations

### The HTTP API

- **No panic recovery middleware.** `net/http` already contains a handler panic
  to the one connection it happened on, so the blast radius is bounded, but a
  panicking handler drops that connection with no correlated response and no
  entry in the error body's shape.
- **No access log.** Only refusals, failures and the distinctions kept off the
  wire are logged. A request log belongs to the composition root, which owns the
  logger.
- **`routed` does not forward `http.Flusher`, `http.Hijacker` or
  `io.ReaderFrom`.** Nothing in this API streams, upgrades or sends a file, and
  `http.ResponseController` reaches the writer underneath through `Unwrap`, but
  middleware written against the bare type assertions would silently do nothing.

### The outbox

- **There is no door that parks a row permanently.** An event the queue refuses
  with an `Unretryable` per-entry code is claimed, refused and rescheduled for
  ever — and because the outbox is head-of-line per wallet, that holds the head
  of *that wallet's* stream indefinitely, so a downstream projection of it
  freezes rather than lags. The error log names a remedy that **is not in the
  code**: `OutboxClaims` has `Claim`, `OldestUnpublished`, `MarkPublished`,
  `Reschedule` and `ReleaseClaims`, and nothing that parks, so the recourse
  today is SQL.

  It is near-unreachable in practice, and the arithmetic of why is worth
  stating. The payload is a marshalled `app.Envelope` out of `jsonb`, so it is
  not malformed. And a **per-entry** refusal is `Unretryable` only when its code
  is in the adapter's own fault list — `InvalidMessageContents`,
  `InvalidParameterValue` and their neighbours, which describe the entry rather
  than the moment — or when SQS declares a sender fault. A per-entry code this
  adapter does not recognise defaults to `Retryable`, which is the opposite of
  the whole-call rule and is deliberate: rescheduling it is right.

### The consumer's ordering guarantee

- **Its precondition is undefended.** SQS holds a group only while a message is
  *genuinely* in flight, which is to say while its visibility timeout has not
  expired. Ten messages against a thirty-second visibility, handled strictly
  serially, is an average of three seconds each — with **no visibility heartbeat
  and no per-message timeout**. One slow message expires the tail's visibility
  while this receiver is still holding it; the group is released, another
  receiver or another replica is given that wallet, and this receiver's now-stale
  receipt handles fail their delete or their visibility change.

  The consequence is **latency, not money**, and that is what makes it a
  limitation rather than a defect: the wallet's row lock serialises the balance
  whoever applies the operation and in whatever order, a reordered reversal parks
  as `PENDING_REFERENCE` rather than being refused, and the inbox absorbs a
  message applied twice. Ordering here buys latency and tidy event streams; it is
  not what makes the money right.

- **The cost of never naming the group.** A receive fills a batch from the head
  group and then from others, so a batch spanning groups is the ordinary case
  whenever the head group has fewer than ten messages waiting — and one poison
  message at position one therefore delays up to nine unrelated wallets by a full
  visibility timeout. The consumer cannot tell which of the nine are behind the
  poison message and which merely arrived in the same response, and guessing
  wrongly reorders a wallet.

### Configuration the types cannot check

- **`CONSUMER_DRAIN_TIMEOUT < SQS_VISIBILITY_TIMEOUT` is a convention.**
  `internal/workers` declines to be told the visibility timeout at all, because a
  second place to configure one is a place for the two to disagree — so nothing
  enforces the relation.
- **`SQS_VISIBILITY_TIMEOUT` and `SQS_MAX_RECEIVE_COUNT` must match
  `deploy/localstack/01-queues.sh` by hand.** Nothing cross-checks the
  provisioned queue's attributes at start-up.

### The test harnesses

- **Three near-duplicate PostgreSQL harnesses** —
  `internal/adapters/postgres`, `internal/integration` and `internal/messaging` —
  each carrying its own copy of roughly two hundred lines of testcontainers
  plumbing, most of it line for line the same. They are not collapsed
  because a helper shared between two packages' `_test.go` files cannot itself be
  a test file, and extracting it would make `testcontainers-go` a **non-test
  import of the module** — changing what `go mod graph`, an SBOM and a
  vulnerability scanner consider shipped, to save duplication in tests.
  `internal/multi` does not add a fourth: it owns no container and asks Compose
  for the deployment instead.
- **`internal/storage/postgres` skips rather than fails when no PostgreSQL is
  reachable**, so `go test ./...` on a machine without Docker is green with the
  whole schema-conformance suite never having run. That is deliberate — a
  migration test that *passed* because there was no database to migrate would be
  worse — but the green run is the thing to be aware of. `README.md` says Docker
  is a prerequisite for that reason.
- **Neither `internal/adapters/postgres` nor `internal/integration` drops its
  test databases on failure.** Also deliberate: a failing test can be
  investigated against the database it failed on. Both drop on success, so a
  green run leaves nothing.
- **The `fxmod` integration suite runs a second Keycloak** alongside
  `internal/integration`'s. `go test -race -tags integration ./...` therefore
  starts ten containers — five PostgreSQL, three LocalStack, two Keycloak, plus
  testcontainers' own reaper — and takes about a minute wall clock. Comfortable
  on eight CPUs; ten containers is where a four-CPU CI runner starts to matter.
- **The `integration` and `multi` suites contend if they are chained.** Each
  passes repeatedly alone, but running one immediately after the other fails
  intermittently while the daemon is still busy — `dependency localstack failed
  to start`, and a `No such container` from Compose. The two suites own their
  containers by different models: testcontainers hands cleanup to a reaper that
  force-removes by label *after* the test process has exited, so `go test`
  returns while the daemon is still deleting, while the `multi` suite expects
  Compose to own its project's containers and network from the first command.
  The second `up` lands in the middle of the first teardown. Nothing here is
  wrong; the suites are simply not meant to overlap, and the README says to run
  them as separate steps.
- **CI does not run the `multi` suite.** `.github/workflows/ci.yml` runs the
  untagged suite and the `integration` suite as two separate steps — which is
  also why it never meets the contention above — but it has no Docker Compose
  stage, and the `multi` suite brings the stack up itself. So the one suite that
  proves three API instances and two workers behave correctly together is a
  local step, and a regression in it would not fail a pull request. Adding a
  Compose stage is the fix; it was not in this effort's scope.

## Not completed

**Nothing** — measured against the task texts this work was given, since the
original specification is not in this repository and **Interpretations** above
is where that is accounted for. Everything asked for is implemented and is
covered by a test that runs against the real thing. The section is kept rather
than deleted because its emptiness is the claim, and a reader is entitled to
see it made rather than to infer it from a heading that is not there.

What stood here until recently was the one genuine gap: **a cross-transport
test proving that one operation submitted over real HTTP and over a real queue
produces one movement and one replay.** `internal/messaging` proves one use
case reached by two callers, which is not the same thing, and its own package
documentation says so under "What this suite cannot catch" — a note that still
stands, because it is still true of that suite. `internal/multi`
closed it. `TestOneOperationOverHTTPAndOverTheQueueSettlesOnceInEitherOrder`
drives a real `client_credentials` token against a real listener and a real
FIFO queue, in both orders, and declares its own two wire types rather than
reusing the adapters' structs — so a rename on either side is caught rather
than compiled away.

For the avoidance of doubt, in the terms the brief set:

- **No test is skipped** to make a suite green. There are three `t.Skip` calls
  in this tree and each is conditional on something that is not the case here:
  two on a PostgreSQL cluster being unreachable — the condition named under
  Limitations — and one on the catalogue declaring no audit codes at all, which
  it does not, since it declares `LEDGER_BALANCE_MISMATCH`.
- **No mock, fake or in-memory substitute stands in for PostgreSQL, SQS or
  Keycloak** in any integration test. Every one of them runs against a real
  container, and `internal/multi` runs against the compose deployment itself.
- **No feature is stubbed.** There is no `TODO`, `FIXME` or unimplemented branch
  anywhere in the tree.

What is *not here* is scope nobody asked for, and it is listed so that its
absence is a decision rather than a gap:

- **No consumer of `wallet-events.fifo`.** Nothing in this deployment reads it,
  which is also why it has no dead-letter queue: one for a destination nobody
  reads would collect nothing.
- **No outbox retention job.** The schema prescribes the statement, the
  application role holds the `DELETE` to run it, and
  `TestAggregateNumberingSurvivesRetention` proves the numbering survives it —
  but scheduling it is a deployment's, and the aggregate sequence counter is a
  table precisely so that it can be.
- **No per-currency scale**, as **Money** above states at length.
- **No rate limiting**, and **no authorisation of who may send to the inbound
  queue.** The latter is an SQS resource policy; both belong to a deployment
  rather than to this service.
