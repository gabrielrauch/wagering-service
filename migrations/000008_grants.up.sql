-- Who may do what.
--
-- Grants to the application come last and on their own, because they are the
-- part most likely to change without the tables under them changing. The roles
-- themselves belong to 0001, so reverting this version revokes without dropping
-- anything.
--
-- Privileges that constitute an object stay with the object instead: a SECURITY
-- DEFINER function's owner and its revoke from PUBLIC are part of its
-- definition, and deferring them to here would leave a window between the two
-- versions in which any role could attach one to a table of its own. Those live
-- in 0004 and 0007, beside the functions they belong to.
--
-- wagering_migrator owns the schema and needs nothing granted: it is the owner.
-- wagering_app is the service, and gets the least it can work with.

GRANT USAGE ON SCHEMA wagering TO wagering_app;

-- Explicit, though it was never granted: the service creates no objects.
-- Schema changes arrive through migrations, run as the other role.
REVOKE CREATE ON SCHEMA wagering FROM wagering_app;
REVOKE ALL ON ALL TABLES IN SCHEMA wagering FROM PUBLIC;

-- Balances change and transactions settle, but neither is ever removed.
GRANT SELECT, INSERT, UPDATE ON wagering.wallet            TO wagering_app;
GRANT SELECT, INSERT, UPDATE ON wagering.wager_transaction TO wagering_app;

-- The ledger is append-only, said a second time and earlier: the trigger
-- refuses the statement, this refuses the attempt. TRUNCATE is not granted
-- either, which is the hole a DELETE-only revoke would leave open.
GRANT SELECT, INSERT ON wagering.wallet_ledger_entry TO wagering_app;

-- Derived state. The trigger that maintains it runs as its definer; the service
-- only reads it.
GRANT SELECT ON wagering.active_reversal TO wagering_app;

-- Closed sets, changed by deployment and never at runtime.
GRANT SELECT ON wagering.failure_code          TO wagering_app;
GRANT SELECT ON wagering.settling_failure_code TO wagering_app;
GRANT SELECT ON wagering.event_type            TO wagering_app;

-- Message plumbing, including the DELETE that retention needs. Neither table is
-- financial: the ledger is what is kept forever.
GRANT SELECT, INSERT, UPDATE, DELETE ON wagering.inbox  TO wagering_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON wagering.outbox TO wagering_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA wagering TO wagering_app;

-- Reconciliation reports; it never corrects. Safe for the service to run.
GRANT EXECUTE ON FUNCTION wagering.reconcile_wallet(uuid) TO wagering_app;
