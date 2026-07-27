CREATE TABLE IF NOT EXISTS system_b_episode (
  episode_id VARCHAR PRIMARY KEY, asset_id VARCHAR NOT NULL, episode_no INTEGER NOT NULL,
  episode_start_date DATE NOT NULL, episode_confirmed_date DATE NOT NULL, episode_end_date DATE,
  ma5_reentry_count INTEGER NOT NULL, created_run_id VARCHAR NOT NULL,
  rule_version VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS system_b_episode_observation (
  trade_date DATE NOT NULL, asset_id VARCHAR NOT NULL, episode_id VARCHAR NOT NULL,
  days_since_start INTEGER NOT NULL, days_since_confirmed INTEGER NOT NULL,
  close DOUBLE NOT NULL, ma5 DOUBLE NOT NULL, ma10 DOUBLE NOT NULL,
  trend_state VARCHAR NOT NULL, previous_trend_state VARCHAR, state_transition VARCHAR,
  episode_return DOUBLE NOT NULL, peak_return DOUBLE NOT NULL, drawdown_from_peak DOUBLE NOT NULL,
  ma5_reentry_count INTEGER NOT NULL, is_episode_confirmed BOOLEAN NOT NULL,
  is_episode_end BOOLEAN NOT NULL, created_run_id VARCHAR NOT NULL,
  rule_version VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (trade_date, asset_id, rule_version)
);
