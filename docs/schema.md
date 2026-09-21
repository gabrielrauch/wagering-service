# The wagering schema

PostgreSQL 16. Every object lives in the `wagering` schema. There are no extensions.

The rule this schema is built to: **every invariant the domain keeps is enforced here too**,
as a constraint, an index, a trigger or a privilege — not as a convention the application
promises to follow. Where the two could drift, a test compares them and fails the build.

- [Applying and reverting](#applying-and-reverting)
- [Roles](#roles)
- [Value domains](#value-domains)
- [How a refusal identifies itself](#how-a-refusal-identifies-itself)
- [Tables](#tables)
- [The queries the indexes serve](#the-queries-the-indexes-serve)
- [What is deliberately not here](#what-is-deliberately-not-here)

## Applying and reverting

Migrations are numbered pairs of plain SQL under `migrations/`, embedded in the binary by
`migrations/embed.go`. Every version has an up and a down, and the pair is tested by
applying everything, reverting everything and applying again.

```
make migrate-up        # apply every migration not yet applied
make migrate-down      # revert every applied migration
make migrate-version   # report the applied version
```

Each target runs `go run ./cmd/migrate`, which takes the connection from `-database` or
`DATABASE_URL`:

```
go run ./cmd/migrate -database "$DATABASE_URL" up
go run ./cmd/migrate -database "$DATABASE_URL" steps -1    # revert one version
go run ./cmd/migrate -database "$DATABASE_URL" version
```

`version` prints the applied version, or fails if the database is dirty — a migration
stopped part way, and the next thing to do is look at what it left behind rather than run
another one.

To try it locally: `make db-up`, `make migrate-up`, `make db-down`.

**Which role runs what.** `migrate` connects as a role that is a member of
`wagering_migrator`; the service connects as a member of `wagering_app`, which has no DDL.
Creating the schema's objects requires that membership, so the migrating login must have it
(`GRANT wagering_migrator TO deploy_user`).

**What the migrator does before migrating.** It runs `CREATE SCHEMA IF NOT EXISTS wagering`.
golang-migrate writes `schema_migrations` into that schema before it runs anything, so the
schema is the namespace the migrations live in — like the database itself — rather than
something a migration creates. That is also why a full revert leaves the schema in place
holding exactly `schema_migrations`: a later apply reads that table to know it is starting
from nothing.

**What a revert deliberately leaves standing.** The two roles. They are cluster-wide and a
migration is per-database, so a revert cannot know whether a sibling database is still using
them — and PostgreSQL will not let it find out: `DROP OWNED BY` reaches only the current
database, so `DROP ROLE` raises `2BP01` the moment another database still holds a grant. That
failure lands mid-migration and leaves the database being reverted recorded at version `-1`
and dirty, with its objects already gone. `DROP OWNED BY` is also unrunnable by the role
deployment actually uses, which is a member of `wagering_migrator` and nothing else. Role
lifetime belongs to deployment, alongside the login users and credentials already kept out of
here; `0001` creates them idempotently precisely so that finding them is normal. See
**ADR-0009**, which also records what the symmetric version broke and how to remove the roles
for real.

**Cancellation and connections.** `cmd/migrate` takes a `context.Context` and stops on an
interrupt at the next version boundary rather than in the middle of one, so a cancelled run
leaves the database at a version rather than dirty between two. `Migrator` opens the
connection it uses and must be `Close`d: golang-migrate's driver leases a connection for its
whole lifetime, so a borrowed pool would lose one for the life of the process.

## Roles

| Role | What it is |
|---|---|
| `wagering_migrator` | `NOLOGIN` group role. Owns the schema, `active_reversal` and the trigger function that maintains it. |
| `wagering_app` | `NOLOGIN` group role. The service. No DDL, no `UPDATE`/`DELETE` on the ledger, no write access to derived state or the catalogues. |

Both are created idempotently by `0001`, catching **both** `duplicate_object` and
`unique_violation`: two deployments migrating different databases at the same moment race
on the cluster-wide role, and which error the loser gets depends on whether the winner had
committed yet.

Deployment creates the real login users and grants membership. Credentials never appear in
a migration.

| Object | `wagering_app` holds |
|---|---|
| `wallet` | `SELECT, INSERT, UPDATE` |
| `wager_transaction` | `SELECT, INSERT, UPDATE` |
| `wallet_ledger_entry` | `SELECT, INSERT` — no `UPDATE`, `DELETE` or `TRUNCATE` |
| `active_reversal` | `SELECT` |
| `failure_code`, `settling_failure_code`, `event_type` | `SELECT` |
| `inbox`, `outbox` | `SELECT, INSERT, UPDATE, DELETE` |
| `reconcile_wallet(uuid)` | `EXECUTE` |
| `maintain_active_reversal()`, `outbox_assign_sequence()` | nothing — `EXECUTE` revoked from `PUBLIC` |
| `outbox_aggregate_sequence` | nothing |
| schema `wagering` | `USAGE`, explicitly not `CREATE` |

## Value domains

One rule, stated once, reused by every column that holds such a value.

| Domain | Rule | Mirrors |
|---|---|---|
| `currency_code` | `^[A-Z]{3}$`, against no allowlist | `money.ParseCurrency`; ADR-0001 |
| `opaque_id` | 1–128 **octets**, no control characters, no surrounding whitespace | `wagering.parseOpaque` |
| `sha256_hex` | `^[0-9a-f]{64}$` | `wagering.PayloadHash` |
| `minor_amount` | `bigint`, no constraint | says *these are minor units at scale 2*; sign and range belong to the table |

`opaque_id` is bounded in octets rather than characters so that it matches Go's `len`: a
65-character string of two-byte runes is 130 octets and is refused, where a character count
would let it through.

**One known divergence.** PostgreSQL's `[[:space:]]` is ASCII; Go's `strings.TrimSpace`
covers the whole Unicode `White_Space` property. A value such as `"\u00a0acme"` is accepted
by the database and refused by the domain. The database is the coarser net, which means a
hand-written `INSERT` could store an identifier `RehydrateWagerTransaction` would refuse to
load. `TestOpaqueIDDomain` pins this as a known case rather than leaving it to be
discovered.

A NUL is refused harder than designed: PostgreSQL `text` cannot hold one, so it fails with
`22021` before the domain's CHECK is reached.

## How a refusal identifies itself

Every rule here names itself, and the name arrives in the same place whatever enforced it.
PostgreSQL fills the error's `constraint_name` for a unique index, a foreign key and a
CHECK; every trigger fills it too, by raising with `USING CONSTRAINT`. So a caller reads one
field — `pgconn.PgError.ConstraintName` — to learn which rule refused a write, and never has
to know whether that rule happens to be a constraint or a trigger today. Moving a rule from
one to the other is not a change to this contract.

The SQLSTATE still says which mechanism refused: `23505` a unique index, `23503` a foreign
key, `23514` a CHECK, `P0001` a trigger. That is worth having for diagnosis and is the wrong
thing to branch on, because every class holds both ordinary business outcomes and signs that
something is wrong — `23505` on `active_reversal_pkey` is a reference already reversed, while
`P0001` on `wallet_ledger_entry_is_append_only` means someone tried to rewrite the ledger.
The name is what tells them apart.

These are the trigger-enforced rules, which without a name would all arrive as an
undifferentiated `P0001`:

| Rule | Refuses |
|---|---|
| `wallet_identifier_is_immutable` | a wallet's identifier being changed |
| `wallet_player_is_immutable` | a wallet changing hands |
| `wallet_currency_is_immutable` | a wallet being redenominated |
| `wallet_opening_time_is_immutable` | a wallet's opening time being restated |
| `wallet_version_advances_by_one` | a balance change that does not advance the version by exactly one |
| `wallet_clock_moves_forward` | a balance change that does not move `updated_at` forward |
| `wallet_version_advances_only_with_the_balance` | a version advancing while the balance stands still |
| `wager_transaction_operation_is_immutable` | the wallet, player, currency, kind, amount or creation time being restated |
| `wager_transaction_provider_fields_are_immutable` | the submission as it arrived being rewritten |
| `wager_transaction_resolution_is_decided_once` | a resolved reference being changed (ADR-0005) |
| `wager_transaction_terminal_status_is_final` | leaving `PROCESSED`, `REJECTED` or `FAILED` |
| `wager_transaction_does_not_return_to_pending` | returning to `PENDING` |
| `wager_transaction_clock_moves_forward` | `updated_at` running backwards |
| `active_reversal_reference_is_reversible` | refunding a reversal, or rolling back a rollback |
| `wallet_ledger_entry_matches_the_wallet_currency` | an entry in a currency the wallet is not denominated in |
| `wallet_ledger_entry_first_records_the_opening_version` | a first entry at a version the wallet never reached |
| `wallet_ledger_entry_first_starts_from_zero` | a first entry starting from a balance the wallet never held |
| `wallet_ledger_entry_follows_the_previous_version` | an entry that skips a version |
| `wallet_ledger_entry_follows_the_previous_balance` | an entry that does not start where the last one ended |
| `wallet_ledger_entry_is_append_only` | any `UPDATE`, `DELETE` or `TRUNCATE` of the ledger |
| `wallet_ledger_entry_names_an_existing_wallet` | an entry naming a wallet that does not exist |
| `wallet_with_no_ledger_holds_nothing` | a wallet holding money with no ledger behind it |
| `wallet_matches_its_ledger` | a wallet and its ledger ending in different places |
| `outbox_aggregate_sequence_is_assigned` | an aggregate sequence supplied rather than assigned |

The last three are deferred to commit, so they refuse the `COMMIT` rather than a statement.

## Tables

### `wallet`

A player's balance in a single currency, and the root of the financial aggregate. There is
no separate currency-of-the-balance column: the wallet's currency *is* the one its balance
is denominated in, which a zero balance carries just as well as a positive one.

| Constraint | Invariant |
|---|---|
| `wallet_player_currency_key` `UNIQUE (player_id, currency)` | one wallet per player per currency — `WALLET_ALREADY_EXISTS`. The domain refuses a second wallet when handed the first, but a check against a value cannot win a race; this decides it. |
| `wallet_identity_key` `UNIQUE (id, player_id, currency)` | redundant with the primary key, and the foreign-key target that makes `CURRENCY_MISMATCH` and player misattribution unrepresentable in `wager_transaction`. |
| `wallet_balance_is_never_negative` | a wallet never holds less than nothing |
| `wallet_version_starts_at_one` | a wallet that exists has stood at one balance |
| `wallet_updated_at_follows_created_at` | |
| trigger `wallet_guard` (BEFORE UPDATE) | `id`, `player_id`, `currency`, `created_at` immutable; a balance change advances the version by **exactly one** and moves `updated_at` forward; a version does not advance without one. |
| constraint trigger `wallet_matches_ledger` (DEFERRED) | at commit, the balance and version equal the latest ledger entry's — or, with no entries, balance 0 at version 1. |

`wallet_guard` is what makes the version an optimistic-concurrency token rather than a
number the application promises to increment: a writer holding a stale version computes the
same next version as the writer that beat it, and **cannot commit**. That is the lost
update, detected.

`wallet_matches_ledger` is the strongest guarantee in the schema. A balance change without
a ledger entry is not something the database declines to do — it is something that cannot
be committed.

### `wager_transaction`

One table, not two. An opening and a provider submission differ only in whether they carry a
provider side, which the domain models as a nullable struct on a single type.

| Constraint | Invariant |
|---|---|
| `wager_transaction_wallet_fkey` `(wallet_id, player_id, currency)` → `wallet` | the transaction's currency and player are the wallet's. `CURRENCY_MISMATCH` made unrepresentable. |
| `wager_transaction_kind_is_known`, `..._status_is_known` | the closed sets, checked against `wagering.Kinds()` and `Statuses()` by test |
| `wager_transaction_origin_carries_its_fields` | `num_nonnulls(provider, external_transaction_id, idempotency_key, payload_hash, round_id, game_id) = 0` for an `OPENING` and `6` otherwise. One line; exactly `validateOrigin`. |
| `correlation_id` `NOT NULL` | the trace the operation arrived under. Required on **every** row, openings included, and deliberately outside the constraint above: that one counts the six fields the *provider* owns, and this one is ours. Causation is not stored — on the queue path it is the message id, which already has an inbox row, and a resumed operation has no causing event with an identity. |
| `wager_transaction_opening_is_born_processed` | an opening is applied as part of creating the wallet; no other status was ever reachable |
| `wager_transaction_amount_follows_kind` | `LOSS` moves nothing, everything else moves something — `Kind.checkAmount` |
| `wager_transaction_reference_follows_kind` | a reversal must name what it undoes, a win may, nothing else may — `Kind.checkReference` |
| `wager_transaction_does_not_reference_itself` | |
| `wager_transaction_resolves_only_what_it_names` | nothing resolved that was never named |
| `wager_transaction_processed_reversal_is_resolved` | a settled reversal names what it undid. Both maintainer triggers are gated on `resolved_reference_id`, so without this a `PROCESSED` `REFUND` carrying only the provider's external name takes no hold and the bet can be returned again |
| `wager_transaction_resolves_the_one_it_names` `(provider, reference_external_transaction_id, resolved_reference_id)` → `(provider, external_transaction_id, id)` | the resolved reference *is* the transaction the row names, not merely some transaction |
| `wager_transaction_wallet_identity_key` `UNIQUE (id, wallet_id)` | the FK target tying a ledger entry to its transaction's wallet |
| trigger `wager_transaction_guard` (BEFORE UPDATE) | the operation's own terms — including the `correlation_id` it arrived under — and the whole provider side are immutable; a resolution is decided once; a terminal status is terminal and nothing returns to `PENDING`; `updated_at` moves forward |
| `wager_transaction_processed_reports_a_balance` | the reported balance is present **exactly** when processed |
| `wager_transaction_result_is_never_negative` | storage is not a way around `MarkProcessed`'s guards |
| `wager_transaction_rejected_names_a_code` | the failure code is present **exactly** when rejected |
| `wager_transaction_settling_code_fkey` → `settling_failure_code` | `checkSettlingCode`: a correctable code persisted nothing and an audit code settles no operation, so neither can stand as a rejection's reason |
| `wager_transaction_deadline_accompanies_attempts` | the deadline is set by the first wait and every wait is counted — `validateWaitBudget` |
| `wager_transaction_waiting_has_waited` | a parked transaction has waited at least once |
| `wager_transaction_only_waiting_is_scheduled` | settled work leaves the scheduler's index |
| `wager_transaction_provider_external_key` `UNIQUE (provider, external_transaction_id)` | persistent idempotency; a financial operation cannot be reapplied under another key |
| `wager_transaction_provider_idempotency_key` `UNIQUE (provider, idempotency_key)` | one key, one transaction |

**Concurrent duplicates** are serialised by those two unique constraints and nothing else.
The second `INSERT` blocks on the uncommitted duplicate key; once the first commits it
receives `23505`, which aborts its transaction — so the winner is read in a new one, never
where the violation was caught. One row wins, the others observe it after commit. No advisory
lock, and no reliance on SQS FIFO deduplication.

`wager_transaction_guard` is `wallet_guard` for the other financial table. Without it the two
unique constraints only ever constrain the current tuple and nothing pins the tuple: a settled
operation could be rewritten into a different one, and the idempotency key it was submitted
under freed for a replay of the very operation it exists to make unrepeatable. Settling is the
only thing an update is for.

`origin` is a **generated** column (`INTERNAL` when the kind is `OPENING`, `EXTERNAL`
otherwise). Origin is decided by the kind, so a column the application could set would be a
second answer free to contradict the first.

**Two time columns, on purpose.** `reference_deadline` is the domain's wait budget — when
waiting stops for good. `reference_next_attempt_at` is the worker's schedule — when to look
again. `RehydrateWagerTransaction` reads the first and ignores the second.

### `active_reversal`

Which reversal currently holds which reference. Derived state: one row per held reference,
maintained by trigger and by nothing else.

| Constraint | Invariant |
|---|---|
| `active_reversal_pkey` `PRIMARY KEY (reference_id)` | **a reference has at most one active reversal.** This is the whole rule, as a key rather than a predicate. |
| `active_reversal_holds_one_reference` `UNIQUE (reversal_id)` | a reversal holds at most one thing |
| `active_reversal_is_not_itself` | |
| `active_reversal_kind_is_a_reversal` | `reversal_kind` is `REFUND` or `ROLLBACK` — what the maintainer needs to tell a releasable hold from a permanent one |
| both FKs → `wager_transaction (id)` | the reference and the reversal are real transactions |
| trigger `maintain_active_reversal_on_insert` / `..._on_settle` | fires when a reversal reaches `PROCESSED`, whether it was born there or settled later |
| privileges | `wagering_app` holds `SELECT` only |

The delete is **conditional**, and that condition is the whole of ADR-0003's asymmetry. Keyed
on `reversal_id` alone it releases *any* hold, including a `ROLLBACK`'s — and a rollback
applied straight to a bet is permanent, because nothing may undo a rollback, so an
unconditional delete hands back a bet the domain holds forever. `reversal_kind` lets the
maintainer tell the two apart without reading `wager_transaction`, which the role it runs as
deliberately holds nothing on.

The trigger deletes the row keyed on the reversal *being reversed*, then inserts the new
hold. That delete is what releases a bet when its refund is rolled back — the sequence
`BET → REFUND → ROLLBACK of the refund` leaves the bet free, and a later `ROLLBACK of the
bet` can take it.

Two triggers rather than one, because a `WHEN` clause cannot mention `OLD` on an insert, and
the update trigger needs it: `UPDATE OF status` fires whenever the column is assigned, even
to the value it already held, and without the comparison a harmless restatement would try to
take the reference twice.

`SECURITY DEFINER` with `SET search_path = wagering, pg_temp`, owned by
`wagering_migrator` — as is the table, because a definer function has only its definer's
privileges. A derived table the application can write to is not derived, and the pinned
search path is what stops a caller's own schema from turning the definer's privileges
against it.

#### What `active_reversal_pkey` decides

`REFERENCE_ALREADY_REVERSED` surfaces as `23505` on this key, in both the serial and the
concurrent case:

- **Serially**, the second reversal's trigger finds the row already there.
- **Concurrently**, the second reversal's trigger passes its `DELETE` (nothing committed
  matches), then blocks on the uncommitted primary key. When the first commits, the second
  receives `23505` and the whole operation fails. One reversal wins; the other sees it.

That second case is the one worth knowing about, because ADR-0003 reasoned the race would be
decided elsewhere — by the wallet's `version`, since every reversal moves money and so both
writers must touch the same wallet row. That reasoning is sound for the domain on its own,
but it is the **weaker** guarantee: it only holds while both reversals move money.
`active_reversal_pkey` holds regardless. `TestTwoReversalsCannotRaceForOneReference` races a
refund and a rollback of one bet where neither writes a ledger entry, so the version is never
consulted, and the bet is still reversed exactly once.

**There is deliberately no unique index on `resolved_reference_id`.** The literal reading of
the brief would forbid `BET → REFUND → ROLLBACK → ROLLBACK of the bet`, which ADR-0003
permits. See **ADR-0007**.

### `wallet_ledger_entry`

Append-only and immutable.

| Constraint | Invariant |
|---|---|
| `wallet_ledger_entry_arithmetic_holds` | `balance_after = balance_before ± amount`, by direction |
| `wallet_ledger_entry_moves_something` | a zero-amount entry would be a movement that did not happen |
| `wallet_ledger_entry_balances_are_never_negative` | |
| `wallet_ledger_entry_transaction_key` `UNIQUE (transaction_id, wallet_id)` | one transaction moves a wallet's balance once; transaction-leading so the same index answers "which entry did this transaction produce?" |
| `wallet_ledger_entry_version_key` `UNIQUE (wallet_id, wallet_version) INCLUDE (signed_minor)` | no two entries claim one version; the stable pagination cursor; index-only `sum(signed_minor)` |
| `wallet_ledger_entry_transaction_fkey` `(transaction_id, wallet_id)` → `wager_transaction` | the entry's wallet is its transaction's wallet. Validated apart, the two only say each exists, and one player's operation could be booked against another player's wallet with every check downstream agreeing |
| trigger `ledger_chain` (BEFORE INSERT) | the first entry records version 1 when it *is* the opening and version 2 otherwise, from balance 0; every other links to its predecessor exactly; the currency is the wallet's |
| constraint trigger `ledger_matches_wallet` (DEFERRED) | the same agreement as `wallet_matches_ledger`, armed by the entry instead of the wallet |
| triggers `ledger_append_only`, `ledger_no_truncate` | `UPDATE`, `DELETE` and `TRUNCATE` all raise |
| privileges | the same again, earlier: the application cannot attempt it |

`signed_minor` is generated (`-amount` for a debit, `+amount` for a credit). It turns
balance reconstruction into `sum(signed_minor)` over one covering index.

**Which version the first entry records depends on how the wallet was opened.** Opened with
money, the opening credit is part of creating the wallet, so the wallet is born at version 1
and its entry records 1. Opened at zero there is no opening at all — an `OPENING` must carry a
positive amount — so the wallet stands at version 1 with an empty ledger and its first
movement is its *second* version. Assuming 1 in both cases wedges every wallet opened empty
for good: the entry wants version 1, `wallet_guard` wants the balance change to advance the
version to 2, and `wallet_matches_ledger` wants the two to agree, and nothing satisfies all
three.

**The pairing is checked from both sides.** `wallet_matches_ledger` is armed by writes to the
wallet, `ledger_matches_wallet` by writes to the ledger. With only the first, a transaction
that appended an entry and left the wallet alone was never checked — `ledger_chain` compares an
entry against its predecessor, never against the balance the wallet holds — so money could be
added to the ledger that the wallet never received, and `reconcile_wallet` would afterwards
report a mismatch nothing had refused.

`ledger_chain` plus the per-entry arithmetic give full ledger-to-balance agreement **by
induction**, at O(1) per write — which is what lets `wallet_matches_ledger` compare against
the latest entry alone instead of resumming.

One consequence worth knowing: `UNIQUE (wallet_id, wallet_version)` is *unreachable
serially*. The chain always demands the next version, and the next version is by definition
free. It is purely a concurrency guard, and `TestTwoEntriesCannotClaimOneVersion` exercises
it as one.

`wagering.reconcile_wallet(uuid)` is the O(n) SQL twin of the domain's `Reconcile`: it
returns whether the stored balance equals the ledger summed. A **function**, not a trigger —
paying O(n) on every movement would make a busy wallet quadratic. It reports; it never
corrects. Disagreement is `LEDGER_BALANCE_MISMATCH`.

### `inbox`

`PRIMARY KEY (consumer_name, message_id)` — the identity *and* the serialisation point, the
same mechanic as a duplicate wager transaction. Two consumers may legitimately see one
message, so the consumer is half of the identity. `completed_at` is nullable and must not
precede `received_at`.

The pair is a single address, and the application layer names it as one: `app.InboxKey`, which
`InboxReader.Find` takes instead of two adjacent strings. Two strings side by side are
transposable at a call site without the compiler noticing, and a transposed lookup here finds
nothing — which reads as "not handled yet" and lets a redelivery be processed twice.

Nothing here opens a transaction of its own, so the row and the domain changes it causes
commit together.

### `outbox`

| Constraint | Invariant |
|---|---|
| `outbox_pkey (event_id)` | republication preserves event identity — it is an update to this row, never a new one |
| `outbox_aggregate_sequence_key` `UNIQUE (aggregate_id, aggregate_sequence)` | contiguous per-wallet ordering; a consumer can tell a gap from an ending |
| trigger `outbox_assign_sequence` | takes the next number from `outbox_aggregate_sequence`, a durable counter, with `ON CONFLICT DO UPDATE`. Writers for one aggregate queue on that row; different aggregates never contend, and the wallet is not touched at all. |
| `outbox_event_type_fkey (event_type, event_version)` | only a declared event can be published |
| `outbox_aggregate_fkey` | the aggregate is a wallet that exists |
| `outbox_payload_is_an_object` | |
| `outbox_claim_is_whole` | a claim is all three of `claimed_by`, `claimed_at`, `claim_expires_at`, or none |
| `outbox_claim_expires_after_it_is_taken` | |

**The counter is a table, not `max()` over the outbox.** The outbox is prunable and the
numbering is not: retention eventually removes every row for a quiet wallet, and a `max()`
derivation has no memory of the numbers it already issued, so the next event starts at 1 again
— with `outbox_aggregate_sequence_key` unable to object, because the rows it would collide
with are the ones that were deleted.

**And it is not the wallet row.** Taking `FOR UPDATE` there was a lock upgrade: a balance
change holds `FOR NO KEY UPDATE` and every foreign key into `wallet` holds `FOR KEY SHARE`,
and `FOR UPDATE` may join neither, so two ordinary writers for one wallet could each wait on
the other and be broken apart by the deadlock detector — the opposite of the queueing the lock
was taken for. The counter row conflicts only with itself.

The aggregate is the wallet: all four domain events name one, it is the root of the
financial aggregate, and it is the natural FIFO group key for a queue.

Payload is `jsonb`. Money crosses this boundary as a string (`"25.00"`), so there is no
number for jsonb's handling to touch, and an operator can query it.

**The claim query.** This is the supported way to take work:

```sql
UPDATE wagering.outbox SET
    claimed_by = $1, claimed_at = $2, claim_expires_at = $3, attempts = attempts + 1
WHERE event_id IN (
    SELECT o.event_id
    FROM wagering.outbox o
    WHERE o.published_at IS NULL
      AND o.next_attempt_at <= $2
      AND (o.claim_expires_at IS NULL OR o.claim_expires_at <= $2)
      AND NOT EXISTS (
          SELECT 1 FROM wagering.outbox p
          WHERE p.aggregate_id = o.aggregate_id
            AND p.published_at IS NULL
            AND p.aggregate_sequence < o.aggregate_sequence
      )
    ORDER BY o.next_attempt_at, o.sequence
    FOR UPDATE SKIP LOCKED
    LIMIT $4
)
RETURNING event_id, aggregate_id, aggregate_sequence, event_type, payload;
```

The `NOT EXISTS` is the head-of-line rule: a wallet's second event is not claimable until
its first is published, so the publisher cannot send them out of order however it is
scheduled. `SKIP LOCKED` keeps publishers off each other. A claim expires by **wall clock**,
so work abandoned by a crashed publisher returns to the pool visibly rather than waiting on
a connection that may never close. See **ADR-0008**.

Cross-aggregate ordering is not guaranteed and is not needed.

### `failure_code`, `settling_failure_code`, `event_type`

Tables rather than CHECK constraints, because failure codes carry two independent
classifications and an operator woken at 3am needs to know whether the provider may retry.
`settling_failure_code` is the subset a rejection may name — a separate relation because
PostgreSQL has no partial foreign key and a CHECK cannot consult another table. It is
**derived** from `failure_code` by the seeding migration rather than listed a second time.

`TestFailureCodeCatalogueMatchesTheDomain` compares the seeded rows against `failure.All()`
with both axes, so adding a code without a migration fails the build.

## The queries the indexes serve

| Index | Query |
|---|---|
| `wallet_player_currency_key` | find a player's wallet in a currency |
| `wager_transaction_provider_external_key` | resolve a reference by `(provider, external_transaction_id)`; detect a duplicate submission |
| `wager_transaction_provider_idempotency_key` | find the transaction an idempotency key is bound to |
| `wager_transaction_one_opening_per_wallet` | a wallet has at most one opening credit |
| `wager_transaction_reference_idx` | "what points at this operation?" — building a `ReferenceView` |
| `wager_transaction_resolved_reference_idx` | the resolved side of the same; backs the self-referencing FK |
| `wager_transaction_due_idx` | workers claiming due `PENDING_REFERENCE` work, in claim order; the trailing `id` makes that order **total**, which matters because `MakeDue` wakes every waiter in one commit with one instant |
| `wager_transaction_wallet_history_idx` | a wallet's operations, newest first |
| `wager_transaction_correlation_idx` | everything that happened under one trace |
| `wallet_ledger_entry_version_key` | cursor pagination per wallet; `sum(signed_minor)` as an index-only scan |
| `wallet_ledger_entry_transaction_key` | "which entry did this transaction produce?", from its leading column |
| `inbox_unfinished_idx` | work that arrived and never finished |
| `outbox_due_idx` | unpublished work that is due, in claim order |
| `outbox_aggregate_pending_idx` | the head-of-line `NOT EXISTS` test |

## What is deliberately not here

- **No `DEFAULT`s on timestamps, identifiers or counters.** The domain takes `now` as an
  argument and mints its own UUIDs; a database default would be a second clock, disagreeing.
  The only generated values are the outbox's `sequence` and the `aggregate_sequence` the
  trigger assigns, both of which the application must not choose.
- **No retention job.** Published outbox rows and completed inbox rows are pruned
  operationally — `DELETE FROM wagering.outbox WHERE published_at < now() - interval '30 days'`
  — because a retention policy is a decision that should not be frozen into a migration. The
  ledger is never pruned.
- **No partitioning.** A guess at a shape production has not yet shown.
- **No `NUMERIC` and no floating point anywhere.** Money is `bigint` minor units at a fixed
  scale of two plus an ISO 4217 code, matching `money.Money` exactly. See ADR-0001 for why
  the scale is fixed and the currency list is open.
