# The active reversal is a derived table, not a unique index

The schema enforces "a reference receives at most one active reversal" with a
`wagering.active_reversal (reference_id PRIMARY KEY, reversal_id UNIQUE)` table, maintained
by a trigger on `wager_transaction`. There is deliberately **no** unique index on
`resolved_reference_id`.

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
