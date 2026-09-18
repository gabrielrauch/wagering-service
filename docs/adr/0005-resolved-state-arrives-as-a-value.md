# State the domain cannot look up arrives as a value

Where a decision depends on data the domain has no way to fetch, the caller resolves it
and passes it in:

- `Processor.Submit` takes a `*ReferenceView` — the transaction a reversal acts on, plus
  the reversals already pointing at it.
- `OpenWallet` takes an `existing *Wallet` — whatever the caller already holds for that
  `WalletKey`, or nil.

No repository interface is defined anywhere in the domain.

## Why this needs recording

`OpenWallet(in, existing, now)` looks odd. The obvious design is a `WalletFinder` port the
domain calls, and a reader will assume the parameter is a workaround rather than a choice.

## Why not a port

A port would make the rule look self-enforcing while not actually being so. Check-then-act
loses a race: two concurrent openings both see nil and both proceed. The real guarantee is
a unique constraint on `(player_id, currency)` in the layer that stores wallets, and adding
a port gives you two mechanisms where only one is authoritative — the weaker one being the
one that appears to be in charge.

It would also put a persistence-shaped collaborator in a package that must stay free of
persistence, and make every wallet test carry a stub for a lookup that no test cares about.

## What passing a value buys

The rule, its failure code and its tests live with the model. `WALLET_ALREADY_EXISTS` is
reachable and covered rather than declared and inert — before this it had no producer at
all, and the catalogue's own totality test certified it as well-formed, so the one test
that might have caught it instead vouched for it.

It also keeps one pattern rather than two. `ReferenceView` already established that the
caller resolves and the domain decides; wallet uniqueness had been left outside the domain
entirely, which was the same problem answered two different ways.

## Consequences

- The storage layer still needs unique constraints on `(player_id, currency)` for wallets
  and on `(wallet_id, transaction_id)` for ledger entries. Passing a value narrows the
  window; it does not close it.
- A caller that hands over a wallet under a different key gets `REFERENCE_MISMATCH`, not
  `WALLET_ALREADY_EXISTS`. The lookup and the request disagreeing is a caller defect, not a
  conflict worth reporting to anyone.

- The same principle governs the processor, and it decides the *shape* of the refusal as
  well as its code. `Submit` and `Continue` refuse a wallet belonging to another player
  before a transaction exists, rather than settling one against it: the command names a
  player, so choosing which wallet to load against that name is this service's work, and
  recording a rejection for our mistake would bind the provider's idempotency key to it for
  good — with no way for them to release it once the lookup here was fixed.

- The two report different codes, deliberately. `Submit` uses the correctable
  `INVALID_FIELD_FORMAT`, because `Code.Correctable()` means exactly "nothing was persisted
  and the key may be reused" and both halves are true — a definitive code would tell a
  caller mapping codes to responses that the key was spent, which is the outcome the check
  exists to prevent. `OpenWallet` keeps `REFERENCE_MISMATCH` because wallet creation carries
  no idempotency key at all, so the axis has nothing to say there.

- A caller that resolves state uniformly may hand over more than an operation asked for. A
  `ReferenceView` supplied for an operation that names no reference is ignored rather than
  treated as a fault: resolving references for every command must not turn a bet into a
  failure. Passing a value means the domain has to tolerate a caller being generous with it.
