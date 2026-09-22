-- Removes the three rules and restores the two descriptions exactly as 000002
-- seeded them. Dropping the index is safe whatever has been recorded: it only
-- ever refused rows, so nothing that exists depends on it.
UPDATE wagering.failure_code
SET description = 'A currency code that is not three uppercase ASCII letters.'
WHERE code = 'UNSUPPORTED_CURRENCY';

UPDATE wagering.failure_code
SET description = 'A reference that already carries an active reversal, which would otherwise return the same money twice.'
WHERE code = 'REFERENCE_ALREADY_REVERSED';

DROP TRIGGER IF EXISTS wager_transaction_processed_in_wallet_currency ON wagering.wager_transaction;
DROP FUNCTION IF EXISTS wagering.wager_transaction_processed_in_wallet_currency();
DROP TRIGGER IF EXISTS outbox_guard ON wagering.outbox;
DROP FUNCTION IF EXISTS wagering.outbox_guard();
DROP INDEX IF EXISTS wagering.wager_transaction_one_successful_reversal_per_kind;
