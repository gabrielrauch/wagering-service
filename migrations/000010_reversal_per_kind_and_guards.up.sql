-- Three rules the schema was missing, and two catalogue descriptions that no
-- longer said what their codes mean. None of them changes a table's shape.

-- A reference never receives two successful reversals of one kind.
--
-- The brief asks for this in as many words, and active_reversal (000004) does
-- not give it. That table keeps at most one ACTIVE reversal per reference and
-- releases the hold when the reversal is itself reversed, which is what lets
-- BET → REFUND → ROLLBACK of the refund → ROLLBACK of the bet through — the
-- sequence ADR-0003 exists to permit. The release is also what let BET → REFUND
-- → ROLLBACK of the refund → REFUND through: the rollback freed the bet, the
-- second refund found the slot empty and took it, and the bet had been refunded
-- twice, each refund returning the stake. Two PROCESSED refunds of one bet.
--
-- This index counts by (reference, kind) and never releases. The second refund
-- is a duplicate key; the rollback of the same bet, being another kind, is not.
-- ADR-0007 rejected the literal reading of the brief — a unique index on the
-- reference alone — because it forbade that rollback. Keyed by kind as well, the
-- index forbids exactly the repeat and nothing else. See the amendments to
-- ADR-0003 and ADR-0007.
--
-- Partial on success: a rejected or still-waiting reversal returned nothing and
-- must not block the one that eventually takes effect. resolved_reference_id is
-- never NULL on a row this index covers, which
-- wager_transaction_processed_reversal_is_resolved (000004) guarantees.
--
-- The domain refuses this first, under REFERENCE_ALREADY_REVERSED, from a
-- reference view that carries every processed reversal and not only the one
-- holding the reference. The index is the backstop for a view built wrongly
-- and the serialisation point for two such reversals racing, exactly as
-- active_reversal_pkey is for the active rule; the adapter maps both to the
-- same sentinel.
CREATE UNIQUE INDEX wager_transaction_one_successful_reversal_per_kind
    ON wagering.wager_transaction (resolved_reference_id, kind)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

-- The event in the outbox is a snapshot of what happened, written once.
--
-- The application holds UPDATE on the whole table (000008) because publishing
-- IS an update: a claim, an attempt count, a schedule, a publication time. Until
-- now nothing kept that privilege off the event itself, and
-- `UPDATE wagering.outbox SET payload = '{}'` succeeded as wagering_app. The
-- brief asks that the payload be an immutable snapshot; this is where the
-- schema says so, for every role including the owner, the way ledger_append_only
-- does for the ledger.
--
-- What is pinned is everything that describes the event: its identity, its
-- aggregate and its place in that aggregate's order, its type and version, its
-- payload and when it occurred. What stays writable is everything that
-- describes publishing it. DELETE is deliberately not refused: retention prunes
-- published rows, and the ledger — not the outbox — is what is kept forever.
--
-- IS DISTINCT FROM over the row constructor, so that restating a column to the
-- value it already holds is not a change: `SET payload = payload` is harmless
-- and stays allowed.
CREATE FUNCTION wagering.outbox_guard() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
BEGIN
    IF (NEW.event_id, NEW.aggregate_type, NEW.aggregate_id, NEW.aggregate_sequence,
        NEW.event_type, NEW.event_version, NEW.payload, NEW.occurred_at)
       IS DISTINCT FROM
       (OLD.event_id, OLD.aggregate_type, OLD.aggregate_id, OLD.aggregate_sequence,
        OLD.event_type, OLD.event_version, OLD.payload, OLD.occurred_at) THEN
        RAISE EXCEPTION
            'an outbox event is a snapshot of what happened: the identity, aggregate, sequence, type, version, payload and time of % are fixed',
            OLD.event_id
            USING CONSTRAINT = 'outbox_payload_is_a_snapshot';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER outbox_guard
    BEFORE UPDATE ON wagering.outbox
    FOR EACH ROW EXECUTE FUNCTION wagering.outbox_guard();

-- A processed transaction is in its wallet's currency.
--
-- 000009 narrowed wager_transaction_wallet_fkey from (wallet_id, player_id,
-- currency) to (wallet_id, player_id) so that a REJECTED CURRENCY_MISMATCH row
-- — the provider named a wallet the player holds and paid in a currency it does
-- not — can be stored, because a rejection that cannot be recorded is not a
-- rejection. That dropped the guarantee the three-column key gave about every
-- other row too. This trigger restores it for the rows it matters for: nothing
-- PROCESSED sits in a currency its wallet is not denominated in. REJECTED,
-- FAILED, PENDING and PENDING_REFERENCE rows may carry another currency,
-- because that is the shape a mismatch has on its way to being rejected.
--
-- The ledger already refuses such an entry (wallet_ledger_entry_matches_the_
-- wallet_currency), so for a BET, WIN, REFUND or ROLLBACK this is stated twice.
-- The kinds that write no entry are the reason it is stated here at all: a
-- PROCESSED LOSS in the wrong currency touched no ledger and nothing refused it.
--
-- BEFORE INSERT OR UPDATE, gated on the status the row is reaching, so a parked
-- row that settles as PROCESSED is checked when it settles. The wallet is read
-- with no lock: its currency is immutable (wallet_currency_is_immutable), so
-- there is nothing a concurrent writer could change under this read.
CREATE FUNCTION wagering.wager_transaction_processed_in_wallet_currency() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
DECLARE
    wallet_currency wagering.currency_code;
BEGIN
    SELECT w.currency INTO wallet_currency
    FROM wagering.wallet w WHERE w.id = NEW.wallet_id;

    IF NEW.currency IS DISTINCT FROM wallet_currency THEN
        RAISE EXCEPTION
            'transaction % is processed in % but wallet % is denominated in %',
            NEW.id, NEW.currency, NEW.wallet_id, wallet_currency
            USING CONSTRAINT = 'wager_transaction_processed_in_wallet_currency';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER wager_transaction_processed_in_wallet_currency
    BEFORE INSERT OR UPDATE ON wagering.wager_transaction
    FOR EACH ROW
    WHEN (NEW.status = 'PROCESSED')
    EXECUTE FUNCTION wagering.wager_transaction_processed_in_wallet_currency();

-- Two descriptions restated to match what their codes now mean. The codes,
-- their classification and the settling subset are unchanged.
--
-- REFERENCE_ALREADY_REVERSED now answers for both rules above the line: the
-- active one and the per-kind one. UNSUPPORTED_CURRENCY has checked the code
-- against the currencies a fixed scale of two can hold since the amendment to
-- ADR-0001, not only its form.
UPDATE wagering.failure_code
SET description = 'A reference that already carries an active reversal, or already received a successful reversal of this kind; either would return the same money twice.'
WHERE code = 'REFERENCE_ALREADY_REVERSED';

UPDATE wagering.failure_code
SET description = 'A currency code that is not a supported ISO 4217 currency with two minor-unit digits, or not three uppercase ASCII letters.'
WHERE code = 'UNSUPPORTED_CURRENCY';
