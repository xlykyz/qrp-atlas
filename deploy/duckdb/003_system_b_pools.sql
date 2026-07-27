CREATE TABLE IF NOT EXISTS system_b_pool_membership_daily (
  trade_date DATE NOT NULL,
  asset_id VARCHAR NOT NULL,
  pool_type VARCHAR NOT NULL,
  membership_state VARCHAR NOT NULL,
  pool_cycle_no INTEGER NOT NULL,
  entry_date DATE NOT NULL,
  exit_date DATE,
  entry_reason VARCHAR,
  exit_reason VARCHAR,
  episode_id VARCHAR,
  metrics_json VARCHAR NOT NULL,
  completed_run_id VARCHAR NOT NULL,
  rule_version VARCHAR NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (trade_date, asset_id, pool_type)
);

CREATE TABLE IF NOT EXISTS system_b_pool_run (
  trade_date DATE NOT NULL,
  pool_type VARCHAR NOT NULL,
  status VARCHAR NOT NULL,
  completed_run_id VARCHAR NOT NULL,
  input_snapshot_id VARCHAR NOT NULL,
  asset_count INTEGER NOT NULL,
  membership_row_count INTEGER NOT NULL,
  metrics VARCHAR NOT NULL,
  created_at TIMESTAMP NOT NULL,
  pool_completed_at TIMESTAMP,
  PRIMARY KEY (trade_date, pool_type)
);
