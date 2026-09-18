-- Revokes without dropping the roles: their lifetime belongs to 0001.
REVOKE EXECUTE ON FUNCTION wagering.reconcile_wallet(uuid) FROM wagering_app;
REVOKE USAGE ON ALL SEQUENCES IN SCHEMA wagering FROM wagering_app;
REVOKE ALL ON ALL TABLES IN SCHEMA wagering FROM wagering_app;
REVOKE USAGE ON SCHEMA wagering FROM wagering_app;
