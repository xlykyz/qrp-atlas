-- 004_migrate_ingested_at_to_timestamptz.sql
-- Explicitly migrate legacy naive TIMESTAMP ingested_at to TIMESTAMPTZ.
-- This migration is EXPLICIT and NON-AUTOMATIC.
-- The DBA/caller must verify that legacy naive ingested_at timestamps in the target database
-- were generated in UTC before applying timezone('UTC', ingested_at). If the target database
-- used a different source timezone (e.g., Asia/Shanghai), substitute that source timezone accordingly.

ALTER TABLE stock_collection ALTER COLUMN ingested_at TYPE TIMESTAMPTZ USING timezone('UTC', ingested_at);
ALTER TABLE theme ALTER COLUMN ingested_at TYPE TIMESTAMPTZ USING timezone('UTC', ingested_at);
ALTER TABLE theme_membership_history ALTER COLUMN ingested_at TYPE TIMESTAMPTZ USING timezone('UTC', ingested_at);
