-- Events waiting to be published, and the record that they were.
--
-- The aggregate is the wallet. All four domain events name one, the wallet is
-- the root of the financial aggregate, and it is the natural FIFO group for a
-- consumer — so aggregate_id doubles as the group key a queue orders within.
CREATE TABLE wagering.outbox (
    -- The event's identity, stable across every republication. A consumer
    -- deduplicating on it still sees one event however many times it was sent.
    event_id uuid NOT NULL,

    -- Insertion order across the whole table. Within one transaction it is the
    -- order the rows were written, which is what keeps WagerTransactionProcessed
    -- ahead of WalletBalanceChanged for an operation. Across transactions it is
    -- only a tiebreaker: aggregate_sequence below is what ordering rests on.
    sequence bigint GENERATED ALWAYS AS IDENTITY,

    aggregate_type text NOT NULL,
    aggregate_id   uuid NOT NULL,

    -- Contiguous per aggregate, assigned by the trigger below. Contiguity is
    -- the point: a consumer can tell a gap from an ending, so it can refuse to
    -- process out of order instead of trusting the publisher.
    aggregate_sequence bigint NOT NULL,

    event_type    text    NOT NULL,
    event_version integer NOT NULL,

    -- The payload as it was when the event happened. jsonb rather than bytes:
    -- money crosses this boundary as a string ("25.00"), so there is no number
    -- for jsonb's numeric handling to touch, and an operator can query it.
    payload jsonb NOT NULL,

    occurred_at timestamptz NOT NULL,

    attempts        integer     NOT NULL,
    next_attempt_at timestamptz NOT NULL,

    -- A claim that expires by wall clock, not by a connection staying open.
    -- Work abandoned by a crashed publisher comes back on its own, and an
    -- operator can see who holds what and until when.
    claimed_by       text,
    claimed_at       timestamptz,
    claim_expires_at timestamptz,

    published_at timestamptz,

    CONSTRAINT outbox_pkey PRIMARY KEY (event_id),
    CONSTRAINT outbox_sequence_key UNIQUE (sequence),

    CONSTRAINT outbox_aggregate_type_is_known CHECK (aggregate_type = 'WALLET'),
    CONSTRAINT outbox_aggregate_fkey FOREIGN KEY (aggregate_id)
        REFERENCES wagering.wallet (id),
    CONSTRAINT outbox_aggregate_sequence_starts_at_one CHECK (aggregate_sequence >= 1),
    CONSTRAINT outbox_aggregate_sequence_key UNIQUE (aggregate_id, aggregate_sequence),

    CONSTRAINT outbox_event_type_fkey FOREIGN KEY (event_type, event_version)
        REFERENCES wagering.event_type (event_type, event_version),

    CONSTRAINT outbox_payload_is_an_object CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT outbox_attempts_are_never_negative CHECK (attempts >= 0),
    CONSTRAINT outbox_claim_is_whole CHECK (
        num_nonnulls(claimed_by, claimed_at, claim_expires_at) IN (0, 3)
    ),
    CONSTRAINT outbox_claim_expires_after_it_is_taken CHECK (
        claim_expires_at IS NULL OR claim_expires_at > claimed_at
    ),
    CONSTRAINT outbox_published_after_it_occurred CHECK (
        published_at IS NULL OR published_at >= occurred_at
    )
);

-- How far each aggregate's numbering has got.
--
-- A counter rather than max() over the outbox, because the outbox is prunable
-- and the numbering is not. The retention this schema prescribes -- DELETE FROM
-- wagering.outbox WHERE published_at < ... -- eventually removes every row for a
-- quiet wallet, and a max() derivation has no memory of the numbers it already
-- issued: the next event starts at 1 again, and outbox_aggregate_sequence_key
-- cannot see the collision, because the rows it would collide with are gone. A
-- consumer tracking last-seen-per-aggregate then reads the replay as a gap, or
-- worse, as an event it has already handled.
--
-- Owned by the migration role, like active_reversal and for the same reason: the
-- trigger below runs as its definer, and the application holds nothing here.
CREATE TABLE wagering.outbox_aggregate_sequence (
    aggregate_id  uuid   NOT NULL,
    last_sequence bigint NOT NULL,

    CONSTRAINT outbox_aggregate_sequence_pkey PRIMARY KEY (aggregate_id),
    CONSTRAINT outbox_aggregate_sequence_last_starts_at_one CHECK (last_sequence >= 1)
);

ALTER TABLE wagering.outbox_aggregate_sequence OWNER TO wagering_migrator;

-- Numbers an event within its aggregate.
--
-- The counter row is the serialisation point. Two writers for one aggregate meet
-- on it: the second blocks inside ON CONFLICT until the first commits, then
-- re-reads and takes the next number, so they queue rather than both computing
-- the same one. Writers for different aggregates never touch the same row.
--
-- It is deliberately not the wallet row. Taking FOR UPDATE there was a lock
-- upgrade: every foreign key into wallet already holds FOR KEY SHARE and a
-- balance change holds FOR NO KEY UPDATE, neither of which FOR UPDATE may join.
-- Two ordinary writers for one wallet could therefore each end up waiting on the
-- other and be broken apart by the deadlock detector, which is the opposite of
-- queueing. The counter conflicts only with itself.
--
-- SECURITY DEFINER for the same reason as maintain_active_reversal: the counter
-- is derived state the application may not write. That is only possible because
-- this function no longer touches wallet, which the definer holds nothing on.
CREATE FUNCTION wagering.outbox_assign_sequence() RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = wagering, pg_temp
AS $$
BEGIN
    IF NEW.aggregate_sequence IS NOT NULL THEN
        RAISE EXCEPTION
            'the aggregate sequence is assigned by the database, not supplied (got % for aggregate %)',
            NEW.aggregate_sequence, NEW.aggregate_id
            USING CONSTRAINT = 'outbox_aggregate_sequence_is_assigned';
    END IF;

    INSERT INTO wagering.outbox_aggregate_sequence AS s (aggregate_id, last_sequence)
    VALUES (NEW.aggregate_id, 1)
    ON CONFLICT (aggregate_id) DO UPDATE SET last_sequence = s.last_sequence + 1
    RETURNING s.last_sequence INTO NEW.aggregate_sequence;

    RETURN NEW;
END
$$;

ALTER FUNCTION wagering.outbox_assign_sequence() OWNER TO wagering_migrator;
REVOKE ALL ON FUNCTION wagering.outbox_assign_sequence() FROM PUBLIC;

CREATE TRIGGER outbox_assign_sequence
    BEFORE INSERT ON wagering.outbox
    FOR EACH ROW EXECUTE FUNCTION wagering.outbox_assign_sequence();

-- Unpublished work that is due, in the order a publisher takes it.
CREATE INDEX outbox_due_idx
    ON wagering.outbox (next_attempt_at, sequence)
    WHERE published_at IS NULL;

-- The head-of-line test: is anything of this aggregate still unpublished and
-- earlier than the row being considered?
CREATE INDEX outbox_aggregate_pending_idx
    ON wagering.outbox (aggregate_id, aggregate_sequence)
    WHERE published_at IS NULL;
