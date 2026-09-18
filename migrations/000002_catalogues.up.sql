-- The closed sets the domain owns, as tables rather than CHECK constraints.
--
-- Kinds, statuses and directions are small enough to be spelled inline where
-- they are used. Failure codes are not: they carry two independent
-- classifications, and an operator woken at 3am needs to know whether the code
-- in front of them means the provider may retry. A table answers that; a CHECK
-- constraint only refuses things.

-- Every declared failure code, with both axes the domain splits them along.
--
-- correctable: the submission was malformed, nothing was persisted, and the
-- same idempotency key remains free for a corrected resubmission.
--
-- audit: the code reports corruption found in stored state rather than the
-- outcome of a submission. Audit codes are definitive — there is nothing to
-- repair — but they settle no wager transaction, because no operation was in
-- flight for them to settle.
CREATE TABLE wagering.failure_code (
    code        text    NOT NULL,
    correctable boolean NOT NULL,
    audit       boolean NOT NULL,
    description text    NOT NULL,

    CONSTRAINT failure_code_pkey PRIMARY KEY (code),
    CONSTRAINT failure_code_is_screaming_snake CHECK (code ~ '^[A-Z]+(_[A-Z]+)*$'),
    CONSTRAINT failure_code_description_is_present CHECK (btrim(description) <> ''),
    -- An audit code is never correctable: there is no payload behind it to
    -- repair. Stated here so the seed cannot introduce a combination the domain
    -- has no meaning for.
    CONSTRAINT failure_code_audit_is_definitive CHECK (NOT (audit AND correctable))
);

-- The subset a rejected wager transaction may name.
--
-- It exists as its own table because that is what makes the rule declarative: a
-- transaction's failure_code is a foreign key into this table, so a correctable
-- or audit code cannot be stored as one. PostgreSQL has no partial foreign key,
-- and a CHECK constraint cannot consult another table, so the subset has to be
-- a relation to be referenced.
CREATE TABLE wagering.settling_failure_code (
    code text NOT NULL,

    CONSTRAINT settling_failure_code_pkey PRIMARY KEY (code),
    CONSTRAINT settling_failure_code_is_a_code FOREIGN KEY (code)
        REFERENCES wagering.failure_code (code) ON DELETE CASCADE
);

-- Every event the domain emits, at the schema version of its payload. A
-- transaction's outbox rows reference this, so an event type nobody declared
-- cannot be published.
CREATE TABLE wagering.event_type (
    event_type    text    NOT NULL,
    event_version integer NOT NULL,
    description   text    NOT NULL,

    CONSTRAINT event_type_pkey PRIMARY KEY (event_type, event_version),
    CONSTRAINT event_type_version_starts_at_one CHECK (event_version >= 1),
    CONSTRAINT event_type_description_is_present CHECK (btrim(description) <> '')
);

INSERT INTO wagering.failure_code (code, correctable, audit, description) VALUES
    ('UNINITIALIZED_VALUE',          true,  false, 'A domain value that was never built through its validating constructor, such as a zero-valued money or a nil identifier.'),
    ('INVALID_AMOUNT_FORMAT',        true,  false, 'An amount that is not a plain non-negative decimal: a sign, scientific notation, NaN, Infinity, a leading zero, whitespace, or a missing part.'),
    ('INVALID_AMOUNT_SCALE',         true,  false, 'A well-formed decimal carrying the wrong number of fraction digits, whether too few or too many.'),
    ('AMOUNT_OUT_OF_RANGE',          true,  false, 'An amount that cannot be represented, or arithmetic whose result would overflow the minor-unit representation.'),
    ('INVALID_AMOUNT_FOR_KIND',      true,  false, 'An amount that is well formed but forbidden for the kind: zero for a BET, WIN, REFUND or ROLLBACK, or non-zero for a LOSS.'),
    ('UNSUPPORTED_CURRENCY',         true,  false, 'A currency code that is not three uppercase ASCII letters.'),
    ('MISSING_REQUIRED_FIELD',       true,  false, 'A field the kind requires but that was absent or empty.'),
    ('INVALID_FIELD_FORMAT',         true,  false, 'A field that is present but malformed: surrounding whitespace, an over-long identifier, or an unknown enum value.'),
    ('UNSUPPORTED_TRANSACTION_KIND', true,  false, 'An OPENING submitted as an external operation. Openings are raised only by the system, when a wallet is created.'),
    ('REFERENCE_REQUIRED',           true,  false, 'A REFUND or ROLLBACK submitted without the reference it must reverse.'),
    ('REFERENCE_NOT_APPLICABLE',     true,  false, 'A reference supplied on a kind that cannot carry one.'),
    ('INVALID_STATE_TRANSITION',     true,  false, 'An attempt to move a wager transaction to a status it cannot reach, including any transition out of a terminal status.'),

    ('INSUFFICIENT_FUNDS',           false, false, 'A BET whose debit would take the wallet below zero.'),
    ('REVERSAL_INSUFFICIENT_FUNDS',  false, false, 'A reversal whose debit would take the wallet below zero. Distinct from INSUFFICIENT_FUNDS: money has already left the wallet and an operator needs to know.'),
    ('BALANCE_OUT_OF_RANGE',         false, false, 'A movement that cannot be applied because the resulting balance would not be representable.'),
    ('CURRENCY_MISMATCH',            false, false, 'Money whose currency differs from the wallet it would move, or arithmetic attempted across two currencies.'),
    ('REFERENCE_NOT_FOUND',          false, false, 'A reference that never arrived before the wait budget was exhausted.'),
    ('REFERENCE_NOT_PROCESSED',      false, false, 'A reference that exists but ended unsuccessfully, so there is nothing to reverse.'),
    ('REFERENCE_NOT_REVERSIBLE',     false, false, 'A reference whose kind cannot be reversed by the submitted kind.'),
    ('REFERENCE_ALREADY_REVERSED',   false, false, 'A reference that already carries an active reversal, which would otherwise return the same money twice.'),
    ('REFERENCE_MISMATCH',           false, false, 'A reference that disagrees with the operation on provider, player, wallet, currency or round.'),
    ('REVERSAL_AMOUNT_MISMATCH',     false, false, 'A reversal whose amount differs from the reference it reverses. Partial reversals do not exist.'),
    ('IDEMPOTENCY_PAYLOAD_CONFLICT', false, false, 'An idempotency key reused for a different set of business fields.'),
    ('WALLET_ALREADY_EXISTS',        false, false, 'A second wallet opened for a player and currency that already has one.'),

    ('LEDGER_BALANCE_MISMATCH',      false, true,  'A wallet whose stored balance does not equal its ledger summed, credits less debits, including the opening. A finding for an operator, not a refusal a provider can act on.');

-- The settling subset is derived rather than listed again. Writing the twelve
-- codes out a second time would be a second place for the classification to
-- live, and the two would eventually disagree.
INSERT INTO wagering.settling_failure_code (code)
SELECT code FROM wagering.failure_code WHERE NOT correctable AND NOT audit;

INSERT INTO wagering.event_type (event_type, event_version, description) VALUES
    ('WagerTransactionProcessed',        1, 'An operation completed successfully. Emitted for every processed kind, including a LOSS, which completes without moving money.'),
    ('WagerTransactionRejected',         1, 'A business rule settled an operation against it. Emitted only for definitive rejections; a malformed submission never becomes a transaction.'),
    ('WalletBalanceChanged',             1, 'A wallet balance changed effectively. Its payload is exactly a ledger entry''s, so the audit trail and its subscribers cannot disagree.'),
    ('WagerTransactionPendingReference', 1, 'An operation is waiting for a transaction it depends on.');
