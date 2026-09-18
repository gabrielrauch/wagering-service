# Events are numbered per aggregate, and published head-of-line

Every outbox row carries `aggregate_sequence`, contiguous per wallet and assigned by a
trigger that takes the next number from a counter row held per aggregate. Publishers claim
only the lowest unpublished sequence of each aggregate.

## Why this needs recording

The obvious outbox is a `BIGSERIAL` plus `FOR UPDATE SKIP LOCKED`, and it has a hole that
does not show up in testing. Transaction A takes sequence 6 and B takes 7; B commits first;
a publisher reads 7, sends it, and only later sees 6. The events reach the consumer
backwards.

SQS FIFO does not save you. It preserves the order messages are *sent* within a
`MessageGroupId` — so an out-of-order send is an out-of-order delivery.

A reader who sees the trigger serialise writers on a counter row will wonder why numbering
an event needs a lock at all. This is why.

## What we considered

**An xmin watermark.** Store `pg_current_xact_id()` and publish only rows below
`pg_snapshot_xmin(pg_current_snapshot())`. Gives global ordering with no extra locking, but
every wallet's events then wait behind the longest-running transaction anywhere in the
database — including an unrelated analytics query. Rejected: it couples latency to something
with no relationship to the events being published.

**Per-aggregate numbering with head-of-line claiming.** The ordering that actually matters
is per wallet: the wallet is the aggregate root, and it is the natural `MessageGroupId`.
Writers for one wallet queue on its row; writers for different wallets never meet. The claim
query excludes any row with an earlier unpublished sibling, so a wallet's second event is not
claimable until its first is published.

  > **Amended during implementation.** The serialisation point is a dedicated counter row in
  > `wagering.outbox_aggregate_sequence`, not the wallet row this ADR first named.
  >
  > `SELECT ... FROM wallet ... FOR UPDATE` turned out to be a lock *upgrade*. Every foreign
  > key into `wallet` already holds `FOR KEY SHARE`, and a balance change holds
  > `FOR NO KEY UPDATE`; `FOR UPDATE` may join neither. Two ordinary writers for one wallet
  > could therefore each end up waiting on the other and be broken apart by the deadlock
  > detector — the opposite of queueing. The counter row conflicts only with itself, so the
  > writers queue as intended, and `TestNumberingDoesNotWaitOnAMoneyMovement` pins that.
  >
  > The counter is also a *table* rather than `max(aggregate_sequence)` over the outbox, and
  > that is the more consequential half. The outbox is prunable — this schema prescribes
  > deleting published rows — and the numbering is not. A `max()` derivation has no memory of
  > the numbers it has already issued, so once the last row for a quiet wallet is pruned the
  > next event starts at 1 again, and `outbox_aggregate_sequence_key` cannot object because
  > the rows it would collide with are the ones that were deleted. A consumer tracking
  > last-seen-per-aggregate then reads the replay as a gap, or as something it has already
  > handled. `TestAggregateNumberingSurvivesRetention` pins it.
  >
  > Nothing else about the decision changed: numbering is still per aggregate, still
  > contiguous, and publishers still claim head-of-line only.

**Neither** — document the limit and move on. Rejected after the trade-off was put to the
point: an ordering guarantee that holds only when nothing is concurrent is not a guarantee.

## Consequences

- Head-of-line blocking per wallet, which is what FIFO means. One stuck event delays that
  wallet and nothing else.
- Contiguity is the gap-detection mechanism: a consumer can tell a gap from an ending, and
  refuse to process out of order rather than trusting the publisher.
- Writers for one wallet queue on that wallet's counter row. The wait is bounded by the
  transaction ahead of them, and writers for different wallets never meet.
- `SKIP LOCKED` still gives up *global* ordering across wallets. Nothing needs it.
