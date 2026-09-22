-- Restores the three-column key of 000003 and 000004. It fails, deliberately,
-- if a REJECTED transaction in a currency its wallet does not hold has been
-- recorded since: such a row is exactly what the older key cannot represent.

ALTER TABLE wagering.wager_transaction
    DROP CONSTRAINT wager_transaction_wallet_fkey;

ALTER TABLE wagering.wallet
    ADD CONSTRAINT wallet_identity_key UNIQUE (id, player_id, currency);

ALTER TABLE wagering.wager_transaction
    ADD CONSTRAINT wager_transaction_wallet_fkey FOREIGN KEY (wallet_id, player_id, currency)
        REFERENCES wagering.wallet (id, player_id, currency);

ALTER TABLE wagering.wallet
    DROP CONSTRAINT wallet_player_key;
