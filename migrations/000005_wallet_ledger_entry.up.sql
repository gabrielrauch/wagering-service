-- The append-only record of every balance change a wallet has undergone.
--
-- An entry is never edited or deleted; a financial correction is made by adding
-- further entries. That is what makes the ledger the financial source of truth
-- rather than a log that happens to agree with one.
CREATE TABLE wagering.wallet_ledger_entry (
    id             uuid                   NOT NULL,
    wallet_id      uuid                   NOT NULL,
    transaction_id uuid                   NOT NULL,
    currency       wagering.currency_code NOT NULL,
    direction      text                   NOT NULL,
    amount_minor   wagering.minor_amount  NOT NULL,

    -- The movement with its sign applied. It turns reconstructing a balance
    -- into sum(signed_minor) over one covering index, and the arithmetic check
    -- below into an addition.
    signed_minor bigint GENERATED ALWAYS AS (
        CASE direction WHEN 'DEBIT' THEN -amount_minor ELSE amount_minor END
    ) STORED,

    balance_before_minor wagering.minor_amount NOT NULL,
    balance_after_minor  wagering.minor_amount NOT NULL,

    -- The wallet version this change produced. The brief's field list does not
    -- include it; without it a ledger has no ordering, because two entries can
    -- share a creation timestamp and a wallet's version is not recoverable from
    -- a count of entries — a wallet opened at zero has a version and no entry.
    -- See ADR-0004.
    wallet_version bigint NOT NULL,

    created_at timestamptz NOT NULL,

    CONSTRAINT wallet_ledger_entry_pkey PRIMARY KEY (id),

    CONSTRAINT wallet_ledger_entry_direction_is_known CHECK (direction IN ('DEBIT', 'CREDIT')),

    -- An entry exists because money moved; a zero-amount entry would be a
    -- movement that did not happen.
    CONSTRAINT wallet_ledger_entry_moves_something CHECK (amount_minor > 0),

    -- Balances are what the wallet held, and a wallet never holds less than
    -- nothing.
    CONSTRAINT wallet_ledger_entry_balances_are_never_negative CHECK (
        balance_before_minor >= 0 AND balance_after_minor >= 0
    ),

    -- balanceAfter = balanceBefore ± amount, stated once by reusing the sign
    -- ladder signed_minor already applies.
    CONSTRAINT wallet_ledger_entry_arithmetic_holds CHECK (
        balance_after_minor = balance_before_minor + signed_minor
    ),

    CONSTRAINT wallet_ledger_entry_version_starts_at_one CHECK (wallet_version >= 1),

    CONSTRAINT wallet_ledger_entry_wallet_fkey FOREIGN KEY (wallet_id)
        REFERENCES wagering.wallet (id),
    -- The transaction AND the wallet it belongs to, together. Validated apart,
    -- the two only say "this wallet exists" and "this transaction exists", so an
    -- entry could book one player's operation against another player's wallet --
    -- and every check downstream, reconcile_wallet included, would agree the
    -- result was sound. With the pair as the target, the misattribution has no
    -- representation. See wager_transaction_wallet_identity_key in 000004.
    CONSTRAINT wallet_ledger_entry_transaction_fkey FOREIGN KEY (transaction_id, wallet_id)
        REFERENCES wagering.wager_transaction (id, wallet_id),

    -- One transaction moves a wallet's balance once. transaction_id leads
    -- because uniqueness does not care about column order but lookup does:
    -- this way the constraint's own index also answers "which entry did this
    -- transaction produce?", and the ledger carries one index fewer on the
    -- append path. Wallet-leading access is served by the version key below.
    CONSTRAINT wallet_ledger_entry_transaction_key UNIQUE (transaction_id, wallet_id),

    -- The stable cursor for pagination, the proof that no two entries claim the
    -- same version, and — with signed_minor carried along — an index-only scan
    -- for credits less debits.
    CONSTRAINT wallet_ledger_entry_version_key UNIQUE (wallet_id, wallet_version)
        INCLUDE (signed_minor)
);

-- Every entry links to the one before it.
--
-- With this, agreement between a wallet and its whole ledger follows by
-- induction from the per-entry arithmetic above, so the wallet check below can
-- compare against the latest entry alone rather than resumming the ledger on
-- every write. The currency is checked here too, because this trigger is
-- already paying for the wallet lookup.
CREATE FUNCTION wagering.ledger_chain() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
DECLARE
    previous_version bigint;
    previous_balance wagering.minor_amount;
    wallet_currency  wagering.currency_code;
    opens_the_wallet boolean;
    first_version    bigint;
BEGIN
    SELECT w.currency INTO wallet_currency
    FROM wagering.wallet w WHERE w.id = NEW.wallet_id;

    IF NEW.currency IS DISTINCT FROM wallet_currency THEN
        RAISE EXCEPTION 'wallet % is denominated in % but the entry is in %',
            NEW.wallet_id, wallet_currency, NEW.currency
            USING CONSTRAINT = 'wallet_ledger_entry_matches_the_wallet_currency';
    END IF;

    SELECT e.wallet_version, e.balance_after_minor
        INTO previous_version, previous_balance
    FROM wagering.wallet_ledger_entry e
    WHERE e.wallet_id = NEW.wallet_id
    ORDER BY e.wallet_version DESC
    LIMIT 1;

    IF NOT FOUND THEN
        -- Which version the first entry records depends on how the wallet was
        -- opened, and there are two shapes.
        --
        -- Opened with money: the opening credit is part of creating the wallet,
        -- so the wallet is born at version 1 and its entry records version 1.
        -- OpenWallet credits from version 0 to land both there.
        --
        -- Opened at zero: there is no opening, because an OPENING must carry a
        -- positive amount -- so the wallet stands at version 1 with no entry
        -- behind it, and the first movement is its second version. Assuming 1
        -- here instead wedges every such wallet permanently: the entry wants
        -- version 1, wallet_guard wants the balance change to advance the
        -- version to 2, and wallet_matches_ledger wants the two to agree. No
        -- assignment satisfies all three, so a wallet opened empty could never
        -- take a bet or a win.
        SELECT t.kind = 'OPENING' INTO opens_the_wallet
        FROM wagering.wager_transaction t WHERE t.id = NEW.transaction_id;
        first_version := CASE WHEN opens_the_wallet THEN 1 ELSE 2 END;

        IF NEW.wallet_version <> first_version THEN
            RAISE EXCEPTION 'the first entry for wallet % records version %, not %',
                NEW.wallet_id, first_version, NEW.wallet_version
                USING CONSTRAINT = 'wallet_ledger_entry_first_records_the_opening_version';
        END IF;
        IF NEW.balance_before_minor <> 0 THEN
            RAISE EXCEPTION 'the first entry for wallet % starts from 0, not %',
                NEW.wallet_id, NEW.balance_before_minor
                USING CONSTRAINT = 'wallet_ledger_entry_first_starts_from_zero';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.wallet_version <> previous_version + 1 THEN
        RAISE EXCEPTION 'the ledger for wallet % is at version %, so the next entry is %, not %',
            NEW.wallet_id, previous_version, previous_version + 1, NEW.wallet_version
            USING CONSTRAINT = 'wallet_ledger_entry_follows_the_previous_version';
    END IF;
    IF NEW.balance_before_minor <> previous_balance THEN
        RAISE EXCEPTION 'the ledger for wallet % ends at %, but the next entry starts from %',
            NEW.wallet_id, previous_balance, NEW.balance_before_minor
            USING CONSTRAINT = 'wallet_ledger_entry_follows_the_previous_balance';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER ledger_chain
    BEFORE INSERT ON wagering.wallet_ledger_entry
    FOR EACH ROW EXECUTE FUNCTION wagering.ledger_chain();

-- The ledger refuses to be rewritten.
--
-- The privileges in 0008 say the application may not attempt this; this trigger
-- says nobody may, including the role that owns the schema. Breaking the glass
-- for a genuine repair means disabling it deliberately, which leaves a trace.
CREATE FUNCTION wagering.ledger_append_only() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
BEGIN
    RAISE EXCEPTION
        'the ledger is append-only: % on wallet_ledger_entry is refused, record a correcting entry instead',
        TG_OP
        USING CONSTRAINT = 'wallet_ledger_entry_is_append_only';
END
$$;

CREATE TRIGGER ledger_append_only
    BEFORE UPDATE OR DELETE ON wagering.wallet_ledger_entry
    FOR EACH ROW EXECUTE FUNCTION wagering.ledger_append_only();

CREATE TRIGGER ledger_no_truncate
    BEFORE TRUNCATE ON wagering.wallet_ledger_entry
    FOR EACH STATEMENT EXECUTE FUNCTION wagering.ledger_append_only();

-- A wallet and its ledger agree, checked when the transaction commits.
--
-- The rule is stated here once and armed from both sides below. It needs two
-- arming points because a constraint trigger is only ever fired by writes to
-- its own table: the wallet side never sees a transaction that appends an entry
-- and leaves the balance alone, and the ledger side never sees one that moves
-- the balance and records nothing. Before the second one existed the pairing
-- was one-directional -- money could be added to the ledger that the wallet
-- never received, the transaction committed, and reconcile_wallet then reported
-- a LEDGER_BALANCE_MISMATCH that nothing had refused.
--
-- Deferred because the wallet and the entry that explains it are written in one
-- transaction and neither order should be forced on the caller. What this buys
-- is that a balance change without a ledger entry is not something the database
-- declines to do -- it is something that cannot be committed.
--
-- Both rows are re-read rather than taken from NEW. By commit time a
-- transaction may have written the wallet or appended entries more than once,
-- and what has to agree is where the two ended up, not any step along the way.
--
-- A wallet with no entries is the shape a wallet opened at zero has: version 1,
-- balance 0, nothing to record.
CREATE FUNCTION wagering.assert_wallet_matches_ledger(target uuid) RETURNS void
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
DECLARE
    held_balance   wagering.minor_amount;
    held_version   bigint;
    ledger_balance wagering.minor_amount;
    ledger_version bigint;
BEGIN
    SELECT w.balance_minor, w.version INTO held_balance, held_version
    FROM wagering.wallet w WHERE w.id = target;

    IF NOT FOUND THEN
        -- Unreachable behind the foreign key, and stated anyway so that a
        -- missing wallet can never pass as NULL <> NULL.
        RAISE EXCEPTION 'ledger entry names wallet %, which does not exist', target
            USING CONSTRAINT = 'wallet_ledger_entry_names_an_existing_wallet';
    END IF;

    SELECT e.balance_after_minor, e.wallet_version
        INTO ledger_balance, ledger_version
    FROM wagering.wallet_ledger_entry e
    WHERE e.wallet_id = target
    ORDER BY e.wallet_version DESC
    LIMIT 1;

    IF NOT FOUND THEN
        IF held_balance <> 0 OR held_version <> 1 THEN
            RAISE EXCEPTION 'wallet % holds % at version % with no ledger behind it',
                target, held_balance, held_version
                USING CONSTRAINT = 'wallet_with_no_ledger_holds_nothing';
        END IF;
        RETURN;
    END IF;

    IF held_balance <> ledger_balance OR held_version <> ledger_version THEN
        RAISE EXCEPTION 'wallet % holds % at version %, but its ledger ends at % at version %',
            target, held_balance, held_version, ledger_balance, ledger_version
            USING CONSTRAINT = 'wallet_matches_its_ledger';
    END IF;
END
$$;

CREATE FUNCTION wagering.wallet_matches_ledger() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
BEGIN
    PERFORM wagering.assert_wallet_matches_ledger(NEW.id);
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER wallet_matches_ledger
    AFTER INSERT OR UPDATE ON wagering.wallet
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wagering.wallet_matches_ledger();

CREATE FUNCTION wagering.ledger_matches_wallet() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path = wagering, pg_temp
AS $$
BEGIN
    PERFORM wagering.assert_wallet_matches_ledger(NEW.wallet_id);
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER ledger_matches_wallet
    AFTER INSERT ON wagering.wallet_ledger_entry
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wagering.ledger_matches_wallet();

-- Reconciliation, as the domain defines it: does the stored balance equal the
-- ledger summed, credits less debits, including the opening?
--
-- It is a function and not a trigger on purpose. The chain above already keeps
-- the two in step at O(1) per write; this is the O(n) confirmation an operator
-- runs deliberately, and paying for it on every movement would make a busy
-- wallet quadratic. It reports; it never corrects.
CREATE FUNCTION wagering.reconcile_wallet(wallet_id uuid) RETURNS boolean
    LANGUAGE sql
    STABLE
    SET search_path = wagering, pg_temp
AS $$
    SELECT w.balance_minor = coalesce((
        SELECT sum(e.signed_minor)
        FROM wagering.wallet_ledger_entry e
        WHERE e.wallet_id = w.id
    ), 0)
    FROM wagering.wallet w
    WHERE w.id = reconcile_wallet.wallet_id;
$$;
