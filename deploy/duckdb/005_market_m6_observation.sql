-- 005_market_m6_observation.sql
-- Task04-C: Daily Market Sentiment (M6) complete facts observation schema.

CREATE TABLE IF NOT EXISTS market_m6_observation (
    trade_date DATE NOT NULL,
    market_scope VARCHAR NOT NULL,
    limit_up_count BIGINT NOT NULL,
    limit_down_count BIGINT NOT NULL,
    consecutive_limit_up_count BIGINT NOT NULL,
    max_consecutive_limit_up_height BIGINT NOT NULL,
    pre_limit_up_premium DOUBLE,
    calculation_version VARCHAR NOT NULL,
    production_run_id VARCHAR,
    input_snapshot_id VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, market_scope)
);
