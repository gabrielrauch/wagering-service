-- A player's balance in a single currency, and the root of the financial
-- aggregate.
--
-- There is no separate currency-of-the-balance column: the wallet's currency is
-- the one its balance is denominated in, which a zero balance carries just as
-- well as a positive one, and a second column would be a copy free to drift
-- from the value it claims to describe. The one column below is therefore both
-- the denomination and half of the uniqueness key.
--
-- No column has a DEFAULT. Every timestamp and identifier arrives from the
-- caller, because the domain takes now as an argument and mints its own
-- identifiers; a database default would be a second clock, disagreeing.
CREATE TABLE wagering.wallet (
    id            uuid                   NOT NULL,
    player_id     wagering.opaque_id     NOT NULL,
    currency      wagering.currency_code NOT NULL,
    balance_minor wagering.minor_amount  NOT NULL,
    version       bigint                 NOT NULL,
    created_at    timestamptz            NOT NULL,
    updated_at    timestamptz            NOT NULL,

    CONSTRAINT wallet_pkey PRIMARY KEY (id),

    -- A wallet never holds less than nothing.
    CONSTRAINT wallet_balance_is_never_negative CHECK (balance_minor >= 0),

    -- The version starts at 1: a wallet that exists has stood at one balance,
    -- even if that balance is zero.
    CONSTRAINT wallet_version_starts_at_one CHECK (version >= 1),

    CONSTRAINT wallet_updated_at_follows_created_at CHECK (updated_at >= created_at),

    -- One wallet per player per currency. The domain refuses a second wallet
    -- when it is handed the one that already exists, but a check against a
    -- value cannot win a race; this is what actually decides it.
    CONSTRAINT wallet_player_currency_key UNIQUE (player_id, currency),

    -- Redundant with the primary key, and there so that other tables can point
    -- at all three columns at once. A wager transaction and a ledger entry each
    -- name a wallet, a player and a currency; with this as a foreign key target
    -- a row claiming a player or a currency the wallet does not have cannot be
    -- written at all. CURRENCY_MISMATCH stops being a rule to check and becomes
    -- a state with no representation.
    CONSTRAINT wallet_identity_key UNIQUE (id, player_id, currency)
);

-- What a wallet may and may not do when it changes.
--
-- Without this the version column is a number the application promises to
-- increment. With it, the version is an optimistic-concurrency token the
-- database will not let drift: a writer holding a stale version computes the
-- same next version as the writer that beat it, and cannot commit. That is the
-- lost update, detected rather than silently accepted.
CREATE FUNCTION wagering.wallet_guard() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id THEN
        RAISE EXCEPTION 'a wallet identifier is immutable: % cannot become %', OLD.id, NEW.id
            USING CONSTRAINT = 'wallet_identifier_is_immutable';
    END IF;
    IF NEW.player_id IS DISTINCT FROM OLD.player_id THEN
        RAISE EXCEPTION 'a wallet cannot change hands: % cannot become %', OLD.player_id, NEW.player_id
            USING CONSTRAINT = 'wallet_player_is_immutable';
    END IF;
    IF NEW.currency IS DISTINCT FROM OLD.currency THEN
        RAISE EXCEPTION 'a wallet cannot be redenominated: % cannot become %', OLD.currency, NEW.currency
            USING CONSTRAINT = 'wallet_currency_is_immutable';
    END IF;
    IF NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'a wallet was opened once: % cannot become %', OLD.created_at, NEW.created_at
            USING CONSTRAINT = 'wallet_opening_time_is_immutable';
    END IF;

    IF NEW.balance_minor IS DISTINCT FROM OLD.balance_minor THEN
        IF NEW.version IS DISTINCT FROM OLD.version + 1 THEN
            RAISE EXCEPTION
                'a balance change advances the wallet version by exactly one: % to % does not follow version %',
                OLD.balance_minor, NEW.balance_minor, OLD.version
                USING CONSTRAINT = 'wallet_version_advances_by_one';
        END IF;
        IF NEW.updated_at <= OLD.updated_at THEN
            RAISE EXCEPTION
                'a balance change moves the clock forward: updated_at % does not follow %',
                NEW.updated_at, OLD.updated_at
                USING CONSTRAINT = 'wallet_clock_moves_forward';
        END IF;
    ELSIF NEW.version IS DISTINCT FROM OLD.version THEN
        RAISE EXCEPTION
            'the wallet version advances only when the balance changes, but % became % at balance %',
            OLD.version, NEW.version, OLD.balance_minor
            USING CONSTRAINT = 'wallet_version_advances_only_with_the_balance';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER wallet_guard
    BEFORE UPDATE ON wagering.wallet
    FOR EACH ROW EXECUTE FUNCTION wagering.wallet_guard();
