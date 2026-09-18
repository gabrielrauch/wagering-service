DROP TRIGGER IF EXISTS outbox_assign_sequence ON wagering.outbox;
DROP FUNCTION IF EXISTS wagering.outbox_assign_sequence();
DROP TABLE IF EXISTS wagering.outbox_aggregate_sequence;
DROP TABLE IF EXISTS wagering.outbox;
