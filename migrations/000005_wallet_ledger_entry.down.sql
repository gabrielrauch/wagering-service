DROP FUNCTION IF EXISTS wagering.reconcile_wallet(uuid);

DROP TRIGGER IF EXISTS ledger_matches_wallet ON wagering.wallet_ledger_entry;
DROP FUNCTION IF EXISTS wagering.ledger_matches_wallet();

DROP TRIGGER IF EXISTS wallet_matches_ledger ON wagering.wallet;
DROP FUNCTION IF EXISTS wagering.wallet_matches_ledger();

DROP FUNCTION IF EXISTS wagering.assert_wallet_matches_ledger(uuid);

DROP TRIGGER IF EXISTS ledger_no_truncate ON wagering.wallet_ledger_entry;
DROP TRIGGER IF EXISTS ledger_append_only ON wagering.wallet_ledger_entry;
DROP FUNCTION IF EXISTS wagering.ledger_append_only();

DROP TRIGGER IF EXISTS ledger_chain ON wagering.wallet_ledger_entry;
DROP FUNCTION IF EXISTS wagering.ledger_chain();

DROP TABLE IF EXISTS wagering.wallet_ledger_entry;
