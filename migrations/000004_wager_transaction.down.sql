DROP TRIGGER IF EXISTS maintain_active_reversal_on_settle ON wagering.wager_transaction;
DROP TRIGGER IF EXISTS maintain_active_reversal_on_insert ON wagering.wager_transaction;
DROP FUNCTION IF EXISTS wagering.maintain_active_reversal();
DROP TRIGGER IF EXISTS wager_transaction_guard ON wagering.wager_transaction;
DROP FUNCTION IF EXISTS wagering.wager_transaction_guard();
DROP TABLE IF EXISTS wagering.active_reversal;
DROP TABLE IF EXISTS wagering.wager_transaction;
