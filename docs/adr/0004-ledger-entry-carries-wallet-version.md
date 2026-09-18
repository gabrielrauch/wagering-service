# The wallet ledger entry carries the wallet version

`WalletLedgerEntry` records `walletVersion` — the version its change produced — in
addition to the fields the requirements enumerate.

## Why this needs recording

It is an addition to a field list that was set out explicitly, so a reader comparing the
two will want to know whether it was deliberate.

## Why

The ledger is specified as the financial source of truth: append-only, with the stored
balance always reconstructible from it. Reconstruction only needs the sum of credits less
debits, which is order-independent — which is why the entry could be specified without an
ordering and still be reconcilable.

But an ordering is worth having, and without the version there is none:

- `createdAt` ties. Two entries written in the same transaction share a timestamp, so it
  cannot order them.
- A version is not recoverable from a count of entries. A wallet opened with a balance has
  version 1 and one entry; opened at zero it has version 1 and **no** entries. After *n*
  changes the entry count is `version` or `version − 1` depending on how the wallet was
  born, so any code assuming the two track each other is wrong.

With it, `(walletId, walletVersion)` orders the ledger exactly and gives a second natural
unique key.

The version is also demonstrably available at the moment the entry is built: the specified
`WalletBalanceChanged` payload — describing the same moment, with otherwise identical data —
already includes `walletVersion`. `NewWalletBalanceChanged` is therefore a projection of a
ledger entry and nothing more, which is what keeps the audit trail and the published event
from ever disagreeing.

It is a projection, not a *total* one: it returns an error, and refuses an entry that was
never constructed. Every field of `WalletLedgerEntry` is unexported, but a composite literal
needs permission only to *name* a field and `WalletLedgerEntry{}` names none — so any package
can write one, and "this entry came from `NewWalletLedgerEntry`" has to be checked rather
than assumed. Announcing a balance change on a zero wallet, by a zero transaction, in no
direction is worse than an error, because a subscriber cannot tell it from a real one. Past
that single check there is nothing left to validate: anything that is not the zero value
came from the constructor, which proved its arithmetic then.

## Consequence

The persisted entry has one more `BIGINT` column than the requirements list. It exists so
that ordering and continuity checking are possible without changing the schema later.

> **Since the schema landed.** "Nothing reads it yet" no longer holds, and the column turned
> out to carry three separate jobs:
>
> - `UNIQUE (wallet_id, wallet_version)` is the stable pagination cursor. A creation
>   timestamp could not be one, because two entries can share it.
> - The `ledger_chain` trigger requires each entry to follow its predecessor's version and
>   balance. That is what makes ledger-to-balance agreement hold **by induction**, at O(1)
>   per write, instead of resumming the ledger.
> - `wallet_matches_ledger` compares the wallet against the latest entry by this column, so
>   the deferred check at commit is a single indexed lookup.
>
> The same index is also unreachable serially — the chain always demands the next version,
> and the next version is by definition free — so it is purely a concurrency guard.
