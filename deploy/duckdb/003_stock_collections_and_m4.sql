-- 003_stock_collections_and_m4.sql
-- StockCollection, Theme, PIT Membership, Theme Custom Index, Trend/Episode, and M4 Observation schemas.

CREATE TABLE IF NOT EXISTS stock_collection (
    collection_id VARCHAR NOT NULL,
    collection_type VARCHAR NOT NULL,
    collection_scope VARCHAR NOT NULL,
    namespace VARCHAR NOT NULL,
    source_key VARCHAR NOT NULL,
    canonical_name VARCHAR NOT NULL,
    membership_model VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    available_trade_date DATE NOT NULL,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR,
    revision_id VARCHAR NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (collection_id, revision_id)
);

CREATE TABLE IF NOT EXISTS theme (
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    theme_name VARCHAR NOT NULL,
    namespace VARCHAR NOT NULL,
    source_key VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    available_trade_date DATE NOT NULL,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR,
    revision_id VARCHAR NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (theme_id, revision_id)
);

CREATE TABLE IF NOT EXISTS theme_membership_history (
    membership_id VARCHAR NOT NULL,
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    asset_id VARCHAR NOT NULL,
    weight DOUBLE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    available_trade_date DATE NOT NULL,
    source VARCHAR NOT NULL,
    source_record_id VARCHAR,
    revision_id VARCHAR NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (membership_id, revision_id)
);

CREATE TABLE IF NOT EXISTS theme_custom_index_daily (
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    theme_daily_return DOUBLE,
    index_level DOUBLE,
    base_level DOUBLE NOT NULL,
    effective_member_count BIGINT NOT NULL,
    total_member_count BIGINT NOT NULL,
    calculation_version VARCHAR NOT NULL,
    production_run_id VARCHAR,
    input_snapshot_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (theme_id, trade_date)
);

CREATE TABLE IF NOT EXISTS theme_custom_index_state (
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    close DOUBLE,
    ma5 DOUBLE,
    ma10 DOUBLE,
    trend_state VARCHAR,
    previous_trend_state VARCHAR,
    custom_index_trend_run_days BIGINT NOT NULL,
    is_above_or_equal_ma5 BOOLEAN,
    state_changed BOOLEAN,
    rule_version VARCHAR NOT NULL,
    production_run_id VARCHAR,
    input_snapshot_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (theme_id, trade_date)
);

CREATE TABLE IF NOT EXISTS theme_custom_index_episode (
    episode_id VARCHAR NOT NULL,
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    episode_no BIGINT NOT NULL,
    episode_start_date DATE NOT NULL,
    episode_confirmed_date DATE NOT NULL,
    episode_end_date DATE,
    ma5_reentry_count BIGINT NOT NULL,
    episode_return DOUBLE,
    rule_version VARCHAR NOT NULL,
    production_run_id VARCHAR,
    input_snapshot_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (episode_id)
);

CREATE TABLE IF NOT EXISTS theme_m4_observation (
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    theme_daily_return DOUBLE,
    theme_limit_up_count BIGINT NOT NULL,
    theme_return_rank BIGINT,
    effective_member_count BIGINT NOT NULL,
    total_member_count BIGINT NOT NULL,
    comparison_universe_size BIGINT NOT NULL,
    comparison_universe_version VARCHAR NOT NULL,
    custom_index_trend_state VARCHAR,
    custom_index_trend_run_days BIGINT,
    custom_index_episode_id VARCHAR,
    qualification_status VARCHAR NOT NULL,
    calculation_version VARCHAR NOT NULL,
    production_run_id VARCHAR,
    input_snapshot_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (theme_id, trade_date)
);
