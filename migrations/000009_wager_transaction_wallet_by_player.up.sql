-- The wallet a submission addresses is a member of the contract: a provider
-- names the wallet, and the service checks that it is the player's rather than
-- choosing one from the player and currency. A provider can therefore name a
-- wallet the player holds and pay in a currency it does not hold, and the
-- domain settles that as CURRENCY_MISMATCH — a REJECTED transaction carrying
-- the money the provider asked for, in the currency they asked for.
--
-- The three-column key made that row unrepresentable. It was written when the
-- wallet was resolved from the player and currency, so a currency the wallet
-- did not have could never reach storage; now it can, and it has to be stored
-- to be a rejection at all. The player stays pinned: a transaction still cannot
-- name a wallet another player holds. The currency is the domain's to settle,
-- and a PROCESSED transaction in the wrong currency is not something the domain
-- produces — it rejects before it moves anything.

ALTER TABLE wagering.wager_transaction
    DROP CONSTRAINT wager_transaction_wallet_fkey;

-- Redundant with the primary key, and there so that a wager transaction can
-- point at the wallet and the player at once.
ALTER TABLE wagering.wallet
    ADD CONSTRAINT wallet_player_key UNIQUE (id, player_id);

ALTER TABLE wagering.wager_transaction
    ADD CONSTRAINT wager_transaction_wallet_fkey FOREIGN KEY (wallet_id, player_id)
        REFERENCES wagering.wallet (id, player_id);

-- Nothing points at all three columns any more.
ALTER TABLE wagering.wallet
    DROP CONSTRAINT wallet_identity_key;
