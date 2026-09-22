# The active reversal is a derived table, not a unique index

The schema enforces "a reference receives at most one active reversal" with a
`wagering.active_reversal (reference_id PRIMARY KEY, reversal_id UNIQUE)` table, maintained
by a trigger on `wager_transaction`. There is deliberately **no** unique index on
`resolved_reference_id`.

  > **Amended 2026-09-22.** The derived table stands, and the index this record rejects
  > is still not there. A unique index keyed on `(resolved_reference_id, kind)` is: it
  > enforces the specification's *other* rule, that a reference never receives two
  > successful reversals of the same kind, which the derived table alone did not. See the
  > amendment at the end.

## Why this needs recording

The brief says, in as many words, that "a processed transaction has at most one reversal
(`REFUND` or `ROLLBACK`) in `PROCESSED` referencing it". Read literally that is one line:

```sql
CREATE UNIQUE INDEX ON wagering.wager_transaction (resolved_reference_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
```

A reader comparing the brief to the schema will not find that index and will reasonably
assume it was forgotten. It was not. That index is wrong, and it is wrong in a way that
only shows up on a sequence the business explicitly wants to support.

## What the literal index forbids

ADR-0003 settles on at most one **active** reversal — one that has not itself been
reversed. So `BET → REFUND → ROLLBACK of that refund` nets to the bet standing debited and
**releases the bet to be reversed again**. A later `ROLLBACK` of the bet is legitimate.

At that point the bet has two processed reversals pointing at it: the refund and the
rollback. The literal index refuses the second one. The domain would produce a valid
outcome, the processor would report success, and the write would fail at `COMMIT` — a
correct business flow turned into an error by a constraint that misstates the rule.

## What we considered

**The literal partial unique index.** One line, and silently removes the refund-then-
rollback-then-reverse-again path the business chose. Rejected.

**A trigger-maintained `holds_reference` boolean** on the reversal row, with a unique index
on `(resolved_reference_id) WHERE holds_reference`. Works, and keeps everything on one
table — but it puts a derived flag on a `PROCESSED` transaction, which ADR-0003 went out of
its way to avoid writing to.

**A derived table.** The invariant becomes a primary key rather than a predicate: one row
per held reference, so a second active reversal is a duplicate key. The trigger deletes the
row keyed on the reversal being reversed before inserting the new one, which is exactly
"rolling back a refund releases the bet". It also answers, as a plain query, the question an
operator actually asks — *what currently holds this bet?*

## Consequences

- `REFERENCE_ALREADY_REVERSED` surfaces as `23505` on `active_reversal_pkey`.
- The table is owned by `wagering_migrator` and its maintainer is `SECURITY DEFINER` with a
  pinned `search_path`. The application holds `SELECT` only: a derived table the application
  can write to is not derived.
- ADR-0003 notes that concurrent reversals are serialised by the wallet's version, because
  both must move money. That still holds, and this is now the stronger guarantee of the two:
  it does not depend on both reversals touching the balance.
  `TestTwoReversalsCannotRaceForOneReference` is the proof — it races two reversals that
  write no ledger entry, so the version is never consulted, and the bet is still reversed
  once. ADR-0003 carries a note pointing here.
- Adding a third reversal kind changes the trigger's `WHEN` clause and nothing else.

  > **Amended 2026-09-22.** And the `kind IN (...)` predicate of the per-kind index below.

## Amendment (2026-09-22): the index that is right, keyed by kind

This record rejects a partial unique index on `resolved_reference_id` because it forbids
`BET → REFUND → ROLLBACK of the refund → ROLLBACK of the bet`. That reasoning holds and the
index is still not there. What the record did not say is that the derived table, on its
own, permits `BET → REFUND → ROLLBACK of the refund → REFUND`: the maintainer deletes the
refund's hold when the refund is rolled back, the second refund finds the primary key free,
and the bet has now been refunded twice — two `PROCESSED` refunds of one reference, which
the specification forbids in as many words. The literal index would have caught that and
was rejected for the sequence it also caught; the derived table permits that sequence and
misses this one. Neither alone is the rule.

Migration `0010` adds the index that the literal one should have been:

```sql
CREATE UNIQUE INDEX wager_transaction_one_successful_reversal_per_kind
    ON wagering.wager_transaction (resolved_reference_id, kind)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
```

Keyed by kind as well as by reference, it refuses exactly the repeat and nothing else. After
the rollback of the refund the bet carries one successful `REFUND`; a `ROLLBACK` of the bet
is a different pair and goes through, which is the sequence this record exists to permit; a
second `REFUND` is the same pair and is a duplicate key. It is partial on success because a
rejected or still-waiting reversal returned nothing and must not block the one that
eventually takes effect, and `resolved_reference_id` is never `NULL` on a row it covers,
which `wager_transaction_processed_reversal_is_resolved` guarantees.

The two objects are deliberately kept apart rather than folded into one, because they
count different things. The derived table counts what *currently* holds a reference and
releases on reversal; the index counts what *ever* succeeded and never releases. A single
mechanism that did both would have to be a derived table that both deletes and remembers,
which is the mutable slot ADR-0003 rejected.

Consequences, added to the list above:

- `REFERENCE_ALREADY_REVERSED` surfaces as `23505` on either `active_reversal_pkey` or
  `wager_transaction_one_successful_reversal_per_kind`. The adapter maps both to the same
  sentinel, and the domain refuses both first, from the reference view.
- The reference view must carry every processed reversal of a reference, flagged with
  whether it was itself reversed, and not only the row `active_reversal` holds. A view
  built from the derived table alone has forgotten the refund that was undone, and a
  domain reading it would let the second refund through to the index — where it is refused
  as an error rather than rejected as an outcome. The adapter reads the reversals from
  `wager_transaction` over `wager_transaction_resolved_reference_idx` and consults
  `active_reversal` per reversal for the one bit it does answer: whether the hold still
  stands.
- The concurrent case is decided the same way as before. Two refunds of one released bet
  racing each other both pass the domain, and the second blocks on the uncommitted index
  entry and receives `23505` when the first commits. One wins; the other sees it.
