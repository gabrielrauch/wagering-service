-- What has already been handled, so that a message delivered twice is applied
-- once.
--
-- The row and the domain changes it causes are written in one transaction:
-- there is no window in which a message is marked handled but its effects are
-- missing, or the other way round. Nothing here opens a transaction of its own.
CREATE TABLE wagering.inbox (
    consumer_name wagering.opaque_id NOT NULL,
    message_id    wagering.opaque_id NOT NULL,
    payload_hash  wagering.sha256_hex NOT NULL,
    received_at   timestamptz NOT NULL,
    completed_at  timestamptz,

    -- One message, once, per consumer. Two consumers may legitimately see the
    -- same message, so the consumer is half of the identity.
    --
    -- As a primary key rather than a unique constraint over a surrogate: this
    -- is the identity, and it is also the serialisation point. A duplicate
    -- arriving while the first is still in flight blocks here and then sees the
    -- winner, exactly as it does for a duplicate wager transaction.
    CONSTRAINT inbox_pkey PRIMARY KEY (consumer_name, message_id),

    CONSTRAINT inbox_completion_follows_receipt CHECK (
        completed_at IS NULL OR completed_at >= received_at
    )
);

-- Work that arrived and never finished, oldest first.
CREATE INDEX inbox_unfinished_idx
    ON wagering.inbox (received_at)
    WHERE completed_at IS NULL;
