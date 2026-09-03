-- 006_theme_m5_observation.sql
-- Task04-B2: daily Theme popularity facts mapped from complete B1 snapshots.

CREATE TABLE IF NOT EXISTS theme_m5_observation (
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    theme_member_count BIGINT NOT NULL,
    theme_hot_stock_count BIGINT NOT NULL,
    theme_hot_stock_ratio DOUBLE,
    theme_hot_list_appearance_count BIGINT NOT NULL,
    theme_hot_source_count BIGINT NOT NULL,
    calculation_version VARCHAR NOT NULL,
    production_run_id VARCHAR,
    input_snapshot_id VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (theme_id, trade_date)
);
