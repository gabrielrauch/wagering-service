# Wagering

Processes wagering operations submitted by game providers against player wallets, keeping every balance change audited and every submission idempotent.

## Language

**Player**:
A person who holds wallets and on whose behalf providers submit wagering operations.
_Avoid_: User, account, customer

**Wallet**:
A player's balance in a single currency. A player holds at most one wallet per currency.
_Avoid_: Account, balance, purse

**Balance**:
The amount of money currently held in a wallet. Never negative.

**Money**:
An amount paired with the currency it is denominated in. Amounts in different currencies are neither comparable nor addable.
_Avoid_: Amount, value, sum

**Currency**:
The unit a wallet and its movements are denominated in, named by an ISO 4217 code.

## Operations

**Wager Transaction**:
The durable record of one wagering operation against a wallet, from submission through to its final outcome.
_Avoid_: Movement, entry, event, request

**Operation**:
A wagering instruction submitted by a provider. Always external; internal wallet opening is not an operation.
_Avoid_: Command, request, message

**Opening**:
The internal wager transaction that records a wallet's starting balance. Produced only when a wallet is created with money in it.
_Avoid_: Initial deposit, seed, top-up

**Bet**:
A player staking money on a round. Debits the wallet.
_Avoid_: Stake, wager, debit

**Win**:
A payout to the player for a round. Credits the wallet.
_Avoid_: Payout, prize, credit

**Loss**:
The record that a round ended with no payout. Moves no money.
_Avoid_: Zero win, no-win

**Refund**:
Returning a bet's full stake to the player because the round did not stand.
_Avoid_: Cancellation, void, reimbursement

**Rollback**:
Undoing a wager transaction in full because it should never have taken effect.
_Avoid_: Reversal, undo, compensation, cancel

**Reversal**:
The category covering refunds and rollbacks — any operation whose purpose is to undo another wager transaction.

**Active Reversal**:
A reversal that has not itself been reversed. A wager transaction may have at most one at a time; when a reversal is undone, the transaction it pointed at becomes reversible again.

**Origin**:
Whether a wager transaction was submitted by a provider or raised internally by the system. Only internal transactions may be openings; only external ones carry provider fields.
_Avoid_: Source, channel, type

**Reference**:
The wager transaction that a reversal, or a win, points back at.
_Avoid_: Parent, original, source

## Provider integration

**Provider**:
The game operator that submits operations and to which outcomes are reported.
_Avoid_: Partner, vendor, client, integrator

**Round**:
One play of a game, grouping the operations that belong together.
_Avoid_: Session, hand, spin

**Game**:
The title a round belongs to.

**Idempotency Key**:
The provider-chosen identifier that binds repeated submissions to a single wager transaction.

**Payload Hash**:
A fingerprint of an operation's business fields, used to detect that one idempotency key has been reused for a different operation.

**Failure Code**:
The stable, documented reason an operation was not applied, or that stored state was found to be wrong. Distinguishes what a provider can correct from what is settled.
_Avoid_: Error code, reason, status

**Correctable Failure**:
A refusal caused by a malformed submission. Nothing is recorded, so the provider may repair the payload and submit it again under the same idempotency key.

**Definitive Failure**:
A refusal settled by a business rule. It is recorded and published, and binds the idempotency key to the payload that caused it.
_Avoid_: Permanent failure, hard failure

**Audit Failure**:
A finding that stored state is wrong, rather than a refusal of anything submitted. It is definitive — there is no payload to repair — but it settles no wager transaction, because no operation was in flight for it to settle.
_Avoid_: Corruption error, system failure

**Wait Budget**:
How long and how often an operation may wait for a reference before the wait is abandoned and the operation settled.
_Avoid_: Retry limit, timeout

## Audit

**Ledger**:
The append-only record of every balance change a wallet has ever undergone. The financial source of truth; corrections are made by adding entries, never by altering them.
_Avoid_: History, log, audit trail

**Wallet Ledger Entry**:
The immutable record of one balance change, stating the direction, the money moved, and the balance either side of it.
_Avoid_: Journal entry, transaction line, movement

**Movement**:
A balance change a wallet is asked to make: which entry and wager transaction it will be recorded under, the money to move, and when. A movement is the request; the Wallet Ledger Entry is the record that applying it produces. That is why an entry is never called a movement, and why the two are named apart even though one becomes the other.
_Avoid_: Transfer, adjustment, posting

**Reconciliation**:
Checking a wallet's stored balance against the balance its ledger implies. Reports disagreement; never corrects it.
_Avoid_: Audit, verification, balancing

**Direction**:
Whether a ledger entry took money out of a wallet or put money into it.

## Messaging

**Inbox**:
The record of which messages a consumer has already handled, so that a message delivered more than once is applied once. Kept per consumer, because two consumers may legitimately see the same message.
_Avoid_: Deduplication table, seen messages

**Outbox**:
The record of events that have happened and are waiting to be published, and of those recently published. An event is written here in the same breath as the change that caused it, so there is no moment at which one exists without the other. Unlike the Ledger it is not kept forever: once an event has been published it is eventually discarded, which is why an Aggregate Sequence has to outlive the events it numbers.
_Avoid_: Queue, publish log, event store

**Claim**:
A publisher's temporary hold on an event it is about to publish. A claim expires on its own, so work abandoned by a publisher that stopped is taken up by another rather than waiting forever.
_Avoid_: Lock, lease, reservation

**Aggregate Sequence**:
An event's position in the history of the wallet it concerns. Contiguous, so a consumer can tell a gap from an ending, and can refuse to act on events that arrived out of order. A position is never reused and the numbering never restarts, so it keeps its meaning for a consumer even after the events themselves have been discarded.
_Avoid_: Offset, index, event number
