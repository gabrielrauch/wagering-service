-- The durable record of one wagering operation against a wallet.
--
-- One table, not two. An opening and a provider submission differ only in
-- whether they carry a provider side, which the domain models as a nullable
-- struct on a single type; splitting them here would put the idempotency
-- constraints, the opening constraint and the reversal rule on opposite sides
-- of a join for no gain.
CREATE TABLE wagering.wager_transaction (
    id            uuid                   NOT NULL,
    wallet_id     uuid                   NOT NULL,
    player_id     wagering.opaque_id     NOT NULL,
    currency      wagering.currency_code NOT NULL,
    kind          text                   NOT NULL,
    status        text                   NOT NULL,
    amount_minor  wagering.minor_amount  NOT NULL,

    -- Derived, never stored twice. Origin is decided by the kind, so a column
    -- the application could set would be a second answer free to contradict the
    -- first.
    origin text GENERATED ALWAYS AS (
        CASE WHEN kind = 'OPENING' THEN 'INTERNAL' ELSE 'EXTERNAL' END
    ) STORED,

    -- The provider-owned side. All six together, or none of them.
    provider                wagering.opaque_id,
    external_transaction_id wagering.opaque_id,
    idempotency_key         wagering.opaque_id,
    payload_hash            wagering.sha256_hex,
    round_id                wagering.opaque_id,
    game_id                 wagering.opaque_id,

    reference_external_transaction_id wagering.opaque_id,
    resolved_reference_id             uuid,

    -- What settling produced: the balance reported back to the provider, or the
    -- code that refused the operation. Each is present exactly when its status
    -- was reached.
    result_balance_minor wagering.minor_amount,
    failure_code         text,

    -- The wait budget the domain owns, and the schedule the worker owns. They
    -- are different things: the deadline is when waiting stops for good, the
    -- next attempt is when to look again. RehydrateWagerTransaction reads the
    -- first two and ignores the third.
    reference_attempts        integer NOT NULL,
    reference_deadline        timestamptz,
    reference_next_attempt_at timestamptz,

    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,

    CONSTRAINT wager_transaction_pkey PRIMARY KEY (id),

    CONSTRAINT wager_transaction_kind_is_known CHECK (
        kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')
    ),
    CONSTRAINT wager_transaction_status_is_known CHECK (
        status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')
    ),

    -- The wallet, the player and the currency, together. See wallet_identity_key.
    CONSTRAINT wager_transaction_wallet_fkey FOREIGN KEY (wallet_id, player_id, currency)
        REFERENCES wagering.wallet (id, player_id, currency),

    -- validateOrigin, as one counting constraint: an internal transaction
    -- carries none of the six provider-owned fields, an external one carries
    -- all six. There is no arrangement in between.
    CONSTRAINT wager_transaction_origin_carries_its_fields CHECK (
        num_nonnulls(provider, external_transaction_id, idempotency_key, payload_hash, round_id, game_id)
        = CASE WHEN kind = 'OPENING' THEN 0 ELSE 6 END
    ),

    -- An opening is applied as part of creating the wallet. There is no moment
    -- at which one is awaiting anything, so no other status can ever have been
    -- stored for one.
    CONSTRAINT wager_transaction_opening_is_born_processed CHECK (
        kind <> 'OPENING' OR status = 'PROCESSED'
    ),

    -- Kind.checkAmount. A loss records that a round ended with no payout; every
    -- other kind exists because money moved.
    CONSTRAINT wager_transaction_amount_follows_kind CHECK (
        CASE WHEN kind = 'LOSS' THEN amount_minor = 0 ELSE amount_minor > 0 END
    ),

    -- Kind.checkReference. A reversal must name what it undoes, a win may name
    -- the bet it pays out on, and nothing else may name anything.
    CONSTRAINT wager_transaction_reference_follows_kind CHECK (
        CASE
            WHEN kind IN ('REFUND', 'ROLLBACK') THEN reference_external_transaction_id IS NOT NULL
            WHEN kind = 'WIN'                   THEN true
            ELSE reference_external_transaction_id IS NULL
        END
    ),
    CONSTRAINT wager_transaction_does_not_reference_itself CHECK (
        reference_external_transaction_id IS NULL
        OR reference_external_transaction_id <> external_transaction_id
    ),
    CONSTRAINT wager_transaction_resolves_only_what_it_names CHECK (
        resolved_reference_id IS NULL OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_transaction_resolved_reference_fkey FOREIGN KEY (resolved_reference_id)
        REFERENCES wagering.wager_transaction (id),

    -- A reversal that has completed names what it undid. Without this a REFUND
    -- or ROLLBACK could reach PROCESSED carrying only the provider's external
    -- name, and both triggers below are gated on the resolved internal id -- so
    -- the row would take no hold at all and the same bet could be returned
    -- again, and again. The invariant active_reversal exists for is only as
    -- strong as the guarantee that a settled reversal reaches it.
    CONSTRAINT wager_transaction_processed_reversal_is_resolved CHECK (
        NOT (status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))
        OR resolved_reference_id IS NOT NULL
    ),

    -- validateSettlement.
    CONSTRAINT wager_transaction_processed_reports_a_balance CHECK (
        (status = 'PROCESSED') = (result_balance_minor IS NOT NULL)
    ),
    CONSTRAINT wager_transaction_result_is_never_negative CHECK (
        result_balance_minor IS NULL OR result_balance_minor >= 0
    ),
    CONSTRAINT wager_transaction_rejected_names_a_code CHECK (
        (status = 'REJECTED') = (failure_code IS NOT NULL)
    ),
    -- checkSettlingCode, as a foreign key into the settling subset. A
    -- correctable code means nothing was persisted, and an audit code reports on
    -- stored state; neither can stand as the reason a transaction was refused.
    CONSTRAINT wager_transaction_settling_code_fkey FOREIGN KEY (failure_code)
        REFERENCES wagering.settling_failure_code (code),

    -- validateWaitBudget. The deadline is set by the first wait and every wait
    -- is counted, so the two appear together or not at all.
    CONSTRAINT wager_transaction_attempts_are_never_negative CHECK (reference_attempts >= 0),
    CONSTRAINT wager_transaction_deadline_accompanies_attempts CHECK (
        (reference_attempts > 0) = (reference_deadline IS NOT NULL)
    ),
    CONSTRAINT wager_transaction_waiting_has_waited CHECK (
        status <> 'PENDING_REFERENCE' OR reference_attempts > 0
    ),
    -- Only work that is still waiting is scheduled to be looked at again, which
    -- also keeps the worker's index down to exactly the rows it claims.
    CONSTRAINT wager_transaction_only_waiting_is_scheduled CHECK (
        (status = 'PENDING_REFERENCE') = (reference_next_attempt_at IS NOT NULL)
    ),

    CONSTRAINT wager_transaction_updated_at_follows_created_at CHECK (updated_at >= created_at),

    -- Persistent idempotency. The provider's own identifier for an operation is
    -- unique to that provider, and so is the key it submits under; a financial
    -- operation identified by the first pair therefore cannot be reapplied under
    -- another key.
    --
    -- This is also what serialises concurrent duplicates. The second INSERT
    -- blocks on the uncommitted duplicate key and, once the first commits,
    -- receives a unique violation and can read the winner. One row wins; the
    -- others observe it after commit.
    --
    -- An opening carries neither column, and NULLs are distinct in a unique
    -- index, so openings never collide here.
    CONSTRAINT wager_transaction_provider_external_key UNIQUE (provider, external_transaction_id),
    CONSTRAINT wager_transaction_provider_idempotency_key UNIQUE (provider, idempotency_key),

    -- The same pair with the row's own identity carried along, so that a
    -- reference can be named and resolved in one constraint. Still unique,
    -- because the pair already was.
    CONSTRAINT wager_transaction_provider_external_id_key
        UNIQUE (provider, external_transaction_id, id),

    -- The resolved reference is the transaction this row names, not merely some
    -- transaction. Without the third column tying them together, resolves_only_
    -- what_it_names asserts that *a* name was given and nothing more, and the
    -- hold active_reversal takes can land on a row belonging to another
    -- provider, another player or another wallet.
    --
    -- MATCH SIMPLE: with any of the three NULL the constraint is satisfied,
    -- which is what an OPENING (no provider, no reference) and an unresolved
    -- submission both need.
    CONSTRAINT wager_transaction_resolves_the_one_it_names
        FOREIGN KEY (provider, reference_external_transaction_id, resolved_reference_id)
        REFERENCES wagering.wager_transaction (provider, external_transaction_id, id),

    -- The foreign-key target that makes a ledger entry's wallet the wallet of
    -- the transaction that caused it. See 000005.
    CONSTRAINT wager_transaction_wallet_identity_key UNIQUE (id, wallet_id)
);

-- A wallet has at most one opening credit.
CREATE UNIQUE INDEX wager_transaction_one_opening_per_wallet
    ON wagering.wager_transaction (wallet_id)
    WHERE kind = 'OPENING';

-- "What points at this operation?" — building a reference view.
CREATE INDEX wager_transaction_reference_idx
    ON wagering.wager_transaction (provider, reference_external_transaction_id)
    WHERE reference_external_transaction_id IS NOT NULL;

-- The resolved side of the same question, and the index the self-referencing
-- foreign key needs.
CREATE INDEX wager_transaction_resolved_reference_idx
    ON wagering.wager_transaction (resolved_reference_id)
    WHERE resolved_reference_id IS NOT NULL;

-- Workers claiming due pending-reference work.
CREATE INDEX wager_transaction_due_idx
    ON wagering.wager_transaction (reference_next_attempt_at, id)
    WHERE status = 'PENDING_REFERENCE';

-- A wallet's operation history, newest first.
CREATE INDEX wager_transaction_wallet_history_idx
    ON wagering.wager_transaction (wallet_id, created_at DESC, id);

-- What a wager transaction may and may not do when it changes.
--
-- The wallet has wallet_guard and the ledger has ledger_append_only; this is the
-- same rule for the table that holds every provider identity, amount and kind.
-- Without it the two unique constraints above only ever constrain the current
-- tuple, and nothing pins the tuple: a settled operation can be rewritten into a
-- different one, and the idempotency key it was submitted under freed for a
-- replay of the very operation it was meant to make unrepeatable.
--
-- Settling is the only thing an update is for. What the operation IS arrived
-- with the submission and is now history.
CREATE FUNCTION wagering.wager_transaction_guard() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
BEGIN
    IF (NEW.id, NEW.wallet_id, NEW.player_id, NEW.currency, NEW.kind,
        NEW.amount_minor, NEW.created_at)
       IS DISTINCT FROM
       (OLD.id, OLD.wallet_id, OLD.player_id, OLD.currency, OLD.kind,
        OLD.amount_minor, OLD.created_at) THEN
        RAISE EXCEPTION
            'a wager transaction records one operation: the wallet, player, currency, kind, amount and creation time of % are fixed',
            OLD.id
            USING CONSTRAINT = 'wager_transaction_operation_is_immutable';
    END IF;

    IF (NEW.provider, NEW.external_transaction_id, NEW.idempotency_key,
        NEW.payload_hash, NEW.round_id, NEW.game_id,
        NEW.reference_external_transaction_id)
       IS DISTINCT FROM
       (OLD.provider, OLD.external_transaction_id, OLD.idempotency_key,
        OLD.payload_hash, OLD.round_id, OLD.game_id,
        OLD.reference_external_transaction_id) THEN
        RAISE EXCEPTION
            'the provider side of % is the submission as it arrived and cannot be rewritten',
            OLD.id
            USING CONSTRAINT = 'wager_transaction_provider_fields_are_immutable';
    END IF;

    -- Resolution is decided once, from a lookup that either found the reference
    -- or did not. ADR-0005: it arrives as a value, and a value does not change
    -- its mind.
    IF OLD.resolved_reference_id IS NOT NULL
        AND NEW.resolved_reference_id IS DISTINCT FROM OLD.resolved_reference_id THEN
        RAISE EXCEPTION
            'transaction % already resolved %, which cannot become %',
            OLD.id, OLD.resolved_reference_id, NEW.resolved_reference_id
            USING CONSTRAINT = 'wager_transaction_resolution_is_decided_once';
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status THEN
        -- Status.IsTerminal, and the transitions map, as a database rule.
        IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
            RAISE EXCEPTION
                'transaction % is %, which is terminal, and cannot become %',
                OLD.id, OLD.status, NEW.status
                USING CONSTRAINT = 'wager_transaction_terminal_status_is_final';
        END IF;
        IF NEW.status = 'PENDING' THEN
            RAISE EXCEPTION
                'transaction % has already left PENDING and does not return to it',
                OLD.id
                USING CONSTRAINT = 'wager_transaction_does_not_return_to_pending';
        END IF;
    END IF;

    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION
            'a wager transaction moves the clock forward: updated_at % does not follow %',
            NEW.updated_at, OLD.updated_at
            USING CONSTRAINT = 'wager_transaction_clock_moves_forward';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER wager_transaction_guard
    BEFORE UPDATE ON wagering.wager_transaction
    FOR EACH ROW EXECUTE FUNCTION wagering.wager_transaction_guard();

-- The reversal currently holding a reference.
--
-- The brief asks that a reference never receive two successful reversals, which
-- read literally is a unique index on resolved_reference_id. That index would
-- forbid a sequence the domain permits: ADR-0003 settles on at most one *active*
-- reversal, so rolling back a refund releases the bet it returned and the bet
-- becomes reversible again. A derived table makes the invariant a primary key
-- rather than a predicate, and survives the next reversal kind. See ADR-0007.
CREATE TABLE wagering.active_reversal (
    reference_id uuid NOT NULL,
    reversal_id  uuid NOT NULL,

    -- Which kind of reversal is holding it. The maintainer below needs this to
    -- tell a hold that can be released from one that cannot, and it cannot ask
    -- wager_transaction: the function runs as the role that owns this table and
    -- deliberately holds nothing on that one.
    reversal_kind text NOT NULL,

    -- One reference, one active reversal. This is REFERENCE_ALREADY_REVERSED.
    CONSTRAINT active_reversal_pkey PRIMARY KEY (reference_id),
    -- A reversal holds at most one thing.
    CONSTRAINT active_reversal_holds_one_reference UNIQUE (reversal_id),
    CONSTRAINT active_reversal_is_not_itself CHECK (reference_id <> reversal_id),
    CONSTRAINT active_reversal_kind_is_a_reversal CHECK (reversal_kind IN ('REFUND', 'ROLLBACK')),

    CONSTRAINT active_reversal_reference_fkey FOREIGN KEY (reference_id)
        REFERENCES wagering.wager_transaction (id) ON DELETE CASCADE,
    CONSTRAINT active_reversal_reversal_fkey FOREIGN KEY (reversal_id)
        REFERENCES wagering.wager_transaction (id) ON DELETE CASCADE
);

-- The derived table belongs to the migration role, because the trigger below
-- runs as that role and a SECURITY DEFINER function has only the definer's
-- privileges. Every other table is owned by whoever runs the migration; this one
-- has to be owned by the role its maintainer runs as, or the maintainer cannot
-- write it.
ALTER TABLE wagering.active_reversal OWNER TO wagering_migrator;

-- Maintains that table as reversals complete.
--
-- The delete is what releases a reference whose reversal is itself being
-- reversed: rolling back a refund removes the refund's hold on the bet. The
-- insert then records the new hold, and the primary key refuses a second one.
--
-- The delete is conditional, and that condition is the whole of ADR-0003. Keyed
-- on reversal_id alone it would release *any* hold, including a ROLLBACK's --
-- and a rollback applied straight to a bet is permanent, because nothing may
-- undo a rollback. An unconditional delete therefore hands back a bet the domain
-- holds forever, and the bet becomes reversible again.
--
-- SECURITY DEFINER because the application role holds only SELECT here. A table
-- the application can write to is not derived, and this trigger is the only
-- thing that may maintain it. search_path is pinned so that the definer's
-- privileges cannot be turned against it by a caller's own schema.
CREATE FUNCTION wagering.maintain_active_reversal() RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = wagering, pg_temp
AS $$
DECLARE
    held_kind text;
BEGIN
    -- What is being reversed, when it is itself a reversal that holds
    -- something -- and releasing it in the same statement, because undoing a
    -- refund releases the bet the refund returned. A bet, a win and a released
    -- reversal all match nothing and leave held_kind NULL.
    --
    -- The release happens before the rule below is checked rather than after,
    -- which costs one probe of this key instead of two. Nothing can observe the
    -- order: the RAISE aborts the transaction, and the DELETE goes back with it.
    DELETE FROM wagering.active_reversal
    WHERE reversal_id = NEW.resolved_reference_id
    RETURNING reversal_kind INTO held_kind;

    IF FOUND THEN
        -- Kind.CanReverse, as far as this table can see it: a refund returns a
        -- bet and nothing else, and no rollback undoes another rollback.
        IF NEW.kind = 'REFUND' OR held_kind = 'ROLLBACK' THEN
            RAISE EXCEPTION 'a % cannot reverse a %, which is not reversible',
                NEW.kind, held_kind
                USING CONSTRAINT = 'active_reversal_reference_is_reversible';
        END IF;
    END IF;

    INSERT INTO wagering.active_reversal (reference_id, reversal_id, reversal_kind)
    VALUES (NEW.resolved_reference_id, NEW.id, NEW.kind);

    RETURN NULL;
END
$$;

ALTER FUNCTION wagering.maintain_active_reversal() OWNER TO wagering_migrator;

-- A definer-rights function is created EXECUTE-to-PUBLIC, because that is what a
-- NULL proacl means. Left that way, any role may attach this one to a table of
-- its own -- a temporary table needs no privilege to create -- and drive its
-- DELETE and INSERT against active_reversal with a NEW row of its choosing,
-- which is exactly the write access the grants deny. Trigger-only means the
-- trigger this migration creates, and nothing else.
REVOKE ALL ON FUNCTION wagering.maintain_active_reversal() FROM PUBLIC;

-- Two triggers rather than one, because a WHEN clause cannot mention OLD on an
-- INSERT. The update trigger needs it: UPDATE OF status fires whenever the
-- column is assigned, even to the value it already held, and without the
-- comparison a harmless restatement would try to take the reference twice.
CREATE TRIGGER maintain_active_reversal_on_insert
    AFTER INSERT ON wagering.wager_transaction
    FOR EACH ROW
    WHEN (
        NEW.status = 'PROCESSED'
        AND NEW.kind IN ('REFUND', 'ROLLBACK')
        AND NEW.resolved_reference_id IS NOT NULL
    )
    EXECUTE FUNCTION wagering.maintain_active_reversal();

CREATE TRIGGER maintain_active_reversal_on_settle
    AFTER UPDATE OF status ON wagering.wager_transaction
    FOR EACH ROW
    WHEN (
        OLD.status IS DISTINCT FROM NEW.status
        AND NEW.status = 'PROCESSED'
        AND NEW.kind IN ('REFUND', 'ROLLBACK')
        AND NEW.resolved_reference_id IS NOT NULL
    )
    EXECUTE FUNCTION wagering.maintain_active_reversal();
