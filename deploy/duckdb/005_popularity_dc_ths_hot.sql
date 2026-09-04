-- 005_popularity_dc_ths_hot.sql
-- M5 Popularity data foundation: Eastmoney A-share popularity rank (dc_hot) and THS hot stock rank (ths_hot).

CREATE TABLE IF NOT EXISTS dc_hot (
    trade_date DATE NOT NULL,
    source VARCHAR NOT NULL,
    list_name VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    name VARCHAR,
    rank_position INTEGER NOT NULL,
    pct_change DOUBLE,
    current_price DOUBLE,
    source_rank_time VARCHAR NOT NULL,
    snapshot_seq INTEGER NOT NULL,
    snapshot_started_at VARCHAR NOT NULL,
    snapshot_completed_at VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, snapshot_seq, rank_position)
);

CREATE TABLE IF NOT EXISTS ths_hot (
    trade_date DATE NOT NULL,
    source VARCHAR NOT NULL,
    list_name VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    name VARCHAR,
    rank_position INTEGER NOT NULL,
    pct_change DOUBLE,
    current_price DOUBLE,
    hot DOUBLE,
    concept VARCHAR,
    rank_reason VARCHAR,
    source_rank_time VARCHAR NOT NULL,
    snapshot_seq INTEGER NOT NULL,
    snapshot_started_at VARCHAR NOT NULL,
    snapshot_completed_at VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, snapshot_seq, rank_position)
);
