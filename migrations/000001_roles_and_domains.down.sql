-- Domains go before anything that might still hold them.
DROP DOMAIN IF EXISTS wagering.minor_amount;
DROP DOMAIN IF EXISTS wagering.sha256_hex;
DROP DOMAIN IF EXISTS wagering.opaque_id;
DROP DOMAIN IF EXISTS wagering.currency_code;

-- The schema outlives this migration: it still holds golang-migrate's
-- bookkeeping table, which has to survive a full revert for the next apply to
-- start from a known state. Ownership moves back to whoever is reverting, so
-- the namespace is left as this version found it.
ALTER SCHEMA wagering OWNER TO CURRENT_USER;
REVOKE ALL ON SCHEMA wagering FROM wagering_migrator;

-- The two roles are deliberately NOT dropped here, and neither is DROP OWNED BY
-- run against them.
--
-- Roles are cluster-wide; a migration is per-database. A revert therefore cannot
-- know whether another database on the same cluster is still using them, and
-- PostgreSQL will not let it find out: DROP OWNED BY reaches only the current
-- database, so DROP ROLE fails with 2BP01 the moment a sibling database still
-- holds a grant. That failure lands mid-migration, which leaves *this* database
-- recorded at version -1 and dirty -- unrecoverable without a manual force, on a
-- database whose objects are already gone.
--
-- DROP OWNED BY is also unrunnable by the role the deployment actually uses. It
-- requires the caller to hold the privileges of the role named, and the migrating
-- login is documented as a member of wagering_migrator and nothing else, so
-- DROP OWNED BY wagering_app raises 42501 before any of it is reached.
--
-- The up creates both roles idempotently, catching the duplicate, precisely so
-- that leaving them behind is safe: the next apply finds them and carries on.
-- Their lifetime belongs to deployment, alongside the login users and the
-- credentials that are already kept out of here. See ADR-0009.
