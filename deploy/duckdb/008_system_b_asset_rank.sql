-- Task06-A asset-relative ranking result, component audit, and replayable
-- popularity source/date availability facts.

CREATE TABLE IF NOT EXISTS system_b_asset_rank_snapshot (
    trade_date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    m1_score DOUBLE,
    m1_rank DOUBLE,
    m1_status VARCHAR NOT NULL,
    m1_universe_size INTEGER NOT NULL,
    m2_score DOUBLE,
    m2_rank DOUBLE,
    m2_status VARCHAR NOT NULL,
    m2_universe_size INTEGER NOT NULL,
    m3_score DOUBLE,
    m3_rank DOUBLE,
    m3_status VARCHAR NOT NULL,
    m3_universe_size INTEGER NOT NULL,
    m1_raw DOUBLE,
    m2_raw DOUBLE,
    m3_raw DOUBLE,
    input_provenance VARCHAR NOT NULL,
    diagnostics VARCHAR NOT NULL,
    evidence VARCHAR NOT NULL,
    production_run_id VARCHAR NOT NULL,
    calculation_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (trade_date, ticker)
);

CREATE TABLE IF NOT EXISTS system_b_asset_rank_component_audit (
    trade_date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    dimension VARCHAR NOT NULL,
    component VARCHAR NOT NULL,
    raw_value DOUBLE,
    direction VARCHAR NOT NULL,
    raw_rank DOUBLE,
    normalized_rank_score DOUBLE,
    dimension_raw DOUBLE,
    final_dimension_rank DOUBLE,
    final_dimension_score DOUBLE,
    universe_size INTEGER NOT NULL,
    tie_count INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    calculation_version VARCHAR NOT NULL,
    source_provenance VARCHAR NOT NULL,
    metadata_json VARCHAR NOT NULL,
    production_run_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (trade_date, ticker, dimension, component)
);

CREATE TABLE IF NOT EXISTS popularity_source_availability (
    trade_date DATE NOT NULL,
    source VARCHAR NOT NULL,
    source_status VARCHAR NOT NULL,
    valid_snapshot_count INTEGER NOT NULL,
    snapshot_seqs VARCHAR NOT NULL,
    input_version VARCHAR NOT NULL,
    source_provenance VARCHAR NOT NULL,
    source_pipeline_run_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (trade_date, source)
);
