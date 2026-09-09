-- Task09 System B decision facts, strategy result/target, and closeout schemas.

CREATE TABLE IF NOT EXISTS system_b_decision_facts_daily (
    trade_date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    comparison_score DOUBLE,
    comparison_score_status VARCHAR NOT NULL,
    entry_eligible BOOLEAN,
    entry_eligibility_status VARCHAR NOT NULL,
    system_b_exit_triggered BOOLEAN,
    exit_status VARCHAR NOT NULL,
    severe_abnormal_supervision_status VARCHAR NOT NULL,
    candidate_membership BOOLEAN,
    candidate_membership_status VARCHAR NOT NULL,
    score_calculation_version VARCHAR,
    rule_version_set_id VARCHAR NOT NULL,
    parameter_set_id VARCHAR NOT NULL,
    input_snapshot_id VARCHAR NOT NULL,
    producer_version VARCHAR NOT NULL,
    provenance_json JSON NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (trade_date, ticker, producer_version, input_snapshot_id)
);

CREATE TABLE IF NOT EXISTS system_b_strategy_result (
    strategy_run_id VARCHAR PRIMARY KEY,
    trade_date DATE NOT NULL,
    strategy_code VARCHAR NOT NULL,
    strategy_version VARCHAR NOT NULL,
    rule_version_set_id VARCHAR NOT NULL,
    parameter_set_id VARCHAR NOT NULL,
    input_snapshot_id VARCHAR NOT NULL,
    input_provenance_json JSON NOT NULL,
    parameters_json JSON NOT NULL,
    authorization_json JSON NOT NULL,
    result_json JSON NOT NULL,
    result_digest VARCHAR NOT NULL,
    result_status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS system_b_strategy_target (
    strategy_run_id VARCHAR NOT NULL,
    target_identity VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    strategy_code VARCHAR NOT NULL,
    strategy_version VARCHAR NOT NULL,
    target_kind VARCHAR NOT NULL,
    target_digest VARCHAR NOT NULL,
    canonical_target_json JSON NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (strategy_run_id, target_identity)
);

CREATE TABLE IF NOT EXISTS system_b_strategy_closeout (
    closeout_identity VARCHAR PRIMARY KEY,
    strategy_run_id VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    strategy_code VARCHAR NOT NULL,
    strategy_version VARCHAR NOT NULL,
    result_digest VARCHAR NOT NULL,
    target_identity VARCHAR NOT NULL,
    target_digest VARCHAR NOT NULL,
    rule_version_set_id VARCHAR NOT NULL,
    parameter_set_id VARCHAR NOT NULL,
    input_snapshot_id VARCHAR NOT NULL,
    completion_identity VARCHAR NOT NULL,
    completion_status VARCHAR NOT NULL,
    completed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
);
