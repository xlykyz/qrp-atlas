-- 004_migrate_ingested_at_to_timestamptz.sql
-- Explicitly migrate legacy naive TIMESTAMP ingested_at to TIMESTAMPTZ.
-- Legacy ingested_at values were produced via datetime.now(timezone.utc) in Python
-- and stored as naive UTC timestamps. Converting them with timezone('UTC', ingested_at)
-- guarantees that the actual UTC moment is preserved regardless of the session TimeZone setting.

ALTER TABLE stock_collection ALTER COLUMN ingested_at TYPE TIMESTAMPTZ USING timezone('UTC', ingested_at);
ALTER TABLE theme ALTER COLUMN ingested_at TYPE TIMESTAMPTZ USING timezone('UTC', ingested_at);
ALTER TABLE theme_membership_history ALTER COLUMN ingested_at TYPE TIMESTAMPTZ USING timezone('UTC', ingested_at);
