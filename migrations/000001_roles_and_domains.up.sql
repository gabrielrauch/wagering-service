-- Roles and the value domains every table is built from.
--
-- The wagering schema itself is not created here. golang-migrate writes its
-- bookkeeping table into that schema before it runs anything, so the schema is
-- the namespace the migrations live in rather than something a migration makes
-- -- in the same way the database itself is. The migrator creates it.

-- Two NOLOGIN group roles. Deployment creates the real login users and grants
-- them membership, which keeps privileges in version control without putting
-- credentials there.
--
-- CREATE ROLE is attempted and the duplicate caught, rather than guarded by a
-- prior existence check. Roles are cluster-wide, so two deployments migrating
-- two different databases at the same moment would both see the role absent and
-- both try to create it; only the exception handler survives that race.
--
-- Both duplicate_object and unique_violation are caught, because the race has
-- two outcomes. A deployment arriving after the role is committed sees it in
-- the catalogue and raises duplicate_object; one arriving while the other is
-- still in flight blocks on pg_authid's unique index and, when that commits,
-- raises unique_violation instead. Catching only the first leaves the second
-- deployment failing on a role that now exists.
DO $$
BEGIN
    CREATE ROLE wagering_migrator NOLOGIN;
EXCEPTION
    WHEN duplicate_object OR unique_violation THEN NULL;
END
$$;

DO $$
BEGIN
    CREATE ROLE wagering_app NOLOGIN;
EXCEPTION
    WHEN duplicate_object OR unique_violation THEN NULL;
END
$$;

ALTER SCHEMA wagering OWNER TO wagering_migrator;
REVOKE ALL ON SCHEMA wagering FROM PUBLIC;

-- Value domains.
--
-- Each is a rule stated once and reused by every column that holds such a
-- value, so "a currency is three uppercase letters" cannot come to mean one
-- thing on a wallet and another on a ledger entry.

-- An ISO 4217 code, validated on form alone against no list of any kind. See
-- ADR-0001: money is held at a fixed scale of two, so an allowlist would have
-- to encode exponents it cannot honour.
CREATE DOMAIN wagering.currency_code AS text
    CONSTRAINT currency_code_is_three_uppercase_letters CHECK (VALUE ~ '^[A-Z]{3}$');

-- A provider-supplied identifier. This is parseOpaque as a database rule:
-- bounded in octets rather than characters, so that it matches Go's len and a
-- multi-byte identifier cannot slip past a character count; no control
-- characters; and refused rather than trimmed when it carries surrounding
-- whitespace, because trimming is a normalisation and a normalisation can merge
-- two identifiers a provider meant to keep apart.
CREATE DOMAIN wagering.opaque_id AS text
    CONSTRAINT opaque_id_is_bounded_and_untrimmed CHECK (
            octet_length(VALUE) BETWEEN 1 AND 128
        AND VALUE !~ '[[:cntrl:]]'
        AND VALUE ~ '^[^[:space:]]'
        AND VALUE ~ '[^[:space:]]$'
    );

-- A SHA-256 digest in the form the domain produces: lowercase hex, 64
-- characters. Uppercase is refused rather than folded, so one payload has one
-- spelling of its hash.
CREATE DOMAIN wagering.sha256_hex AS text
    CONSTRAINT sha256_hex_is_lowercase_hex CHECK (VALUE ~ '^[0-9a-f]{64}$');

-- Money in minor units at a fixed scale of two. The domain adds no constraint:
-- its whole job is to say, at every column that holds one, that this is a count
-- of minor units and not a count of anything else. Range and sign belong to the
-- table, because a balance, an amount and a signed movement each allow
-- something different.
CREATE DOMAIN wagering.minor_amount AS bigint;
