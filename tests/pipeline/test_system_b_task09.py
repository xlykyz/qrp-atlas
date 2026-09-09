"""Offline acceptance tests for the Task09 System B daily framework."""

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.contracts import MARKET_PHASE
from qrp_atlas.backtest.harness.strategy_driver import run_system_b_day_by_day_replay
from qrp_atlas.backtest.models import CostRule
from qrp_atlas.backtest.portfolio.models import PortfolioBacktestConfig
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.system_b_task09 import (
    canonical_target_json,
    closeout_strategy_daily,
    load_persisted_decision_facts,
    normalize_decision_facts,
    persist_decision_facts,
    resolve_task09_target_date,
    run_task09_daily,
    target_digest,
)
from qrp_atlas.strategies.models import StrategyPortfolioTarget, StrategyPortfolioTargetPosition


def _target(trade_date: str, positions=()):
    return StrategyPortfolioTarget(trade_date, "system_b_portfolio", "1.0.0", tuple(positions))


def test_frozen_target_digest_fixtures_are_stable():
    fixtures = (
        (_target("2024-01-10"), "9bb5e39176a402d3eb1e90b4cc6fff20a4ecd637cd6e83c8b1ffd9d9e8f93a28"),
        (_target("2024-01-10", (StrategyPortfolioTargetPosition("H1", 0.25, "ADD_ENTRY_SELECTED"), StrategyPortfolioTargetPosition("N1", 0.125, "NEW_ENTRY_SELECTED"))), "207bca336ac555e2a00f1a40e5249f09e476ff6b197714dfdfa788a640ef055b"),
        (_target("2024-01-10", (StrategyPortfolioTargetPosition("B", 0.1250000000006, "HOLD"), StrategyPortfolioTargetPosition("A", 0.1250000000004, "HOLD"))), "6b8b2281011ac4095ac50a7727b72f79cc4e76c83e3d742aa661ac33e66f0a82"),
    )
    for target, expected in fixtures:
        assert target_digest(target) == expected
    assert canonical_target_json(fixtures[2][0]).index('"A"') < canonical_target_json(fixtures[2][0]).index('"B"')
    with pytest.raises(ValueError, match="TASK09_TARGET_WEIGHT_INVALID"):
        target_digest(_target("2024-01-10", (StrategyPortfolioTargetPosition("A", float("nan")),)))
    with pytest.raises(ValueError, match="TASK09_TARGET_ASSET_ID_INVALID"):
        target_digest(_target("2024-01-10", (StrategyPortfolioTargetPosition(" ", 0.1),)))
    with pytest.raises(ValueError, match="TASK09_TARGET_ASSET_ID_DUPLICATE"):
        target_digest(_target("2024-01-10", (StrategyPortfolioTargetPosition("A", 0.1), StrategyPortfolioTargetPosition("A", 0.2))))


def test_target_date_policy_is_strict_and_shanghai_based():
    assert resolve_task09_target_date(datetime(2024, 1, 9, 16, 30, tzinfo=timezone.utc)) == date(2024, 1, 10)
    assert resolve_task09_target_date(datetime(2024, 1, 9, 16, 30, tzinfo=timezone.utc), date(2024, 2, 3)) == date(2024, 2, 3)
    with pytest.raises(ValueError, match="TASK09_TARGET_DATE_INVALID"):
        resolve_task09_target_date(datetime.now(timezone.utc), datetime(2024, 2, 3))
    with pytest.raises(ValueError, match="TASK09_SCHEDULE_TIMEZONE_MISSING"):
        resolve_task09_target_date(datetime(2024, 1, 10))


def test_unavailable_facts_are_explicit_and_fail_closed():
    rows = normalize_decision_facts(date(2024, 1, 10), [{"ticker": "A"}], provenance={"score_version": "score-v0"})
    assert rows[0]["comparison_score_status"] == "UNAVAILABLE"
    assert rows[0]["entry_eligibility_status"] == "UNAVAILABLE"
    assert rows[0]["candidate_membership_status"] == "UNAVAILABLE"
    assert rows[0]["rule_version_set_id"] == "UNAVAILABLE"
    assert rows[0]["parameter_set_id"] == "UNAVAILABLE"


def test_daily_result_target_closeout_are_atomic_and_idempotent(tmp_path: Path):
    db = tmp_path / "task09.duckdb"
    facts = [{"ticker": "A", "comparison_score": 60.0, "entry_eligible": True, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"}]
    kwargs = dict(trade_date=date(2024, 1, 10), facts=facts, holdings=[], authorization_input={"phase": "B", "V_triggered": False}, invocation_id="acceptance-1", duckdb_path=db, candidate_asset_ids=["A"], rule_version_set_id="rules-v1", parameter_set_id="params-v1", input_snapshot_id="snap-v1", comparison_score_provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snap-v1"})
    first = run_task09_daily(**kwargs)
    second = run_task09_daily(**kwargs)
    assert first["strategy_run_id"] == second["strategy_run_id"]
    assert first["target_identity"] == second["target_identity"]
    completion = closeout_strategy_daily(duckdb_path=db, strategy_run_id=first["strategy_run_id"], trade_date=date(2024, 1, 10), result_digest=first["result_digest"], target_identity=first["target_identity"])
    assert closeout_strategy_daily(duckdb_path=db, strategy_run_id=first["strategy_run_id"], trade_date=date(2024, 1, 10), result_digest=first["result_digest"], target_identity=first["target_identity"]) == completion
    with duckdb.connect(str(db), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM system_b_strategy_result").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM system_b_strategy_target").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM system_b_strategy_closeout").fetchone()[0] == 1


def test_business_identity_ignores_runtime_invocation_id(tmp_path: Path):
    kwargs = dict(
        trade_date=date(2024, 1, 10),
        facts=[{"ticker": "A", "comparison_score": 60.0, "entry_eligible": True, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"}],
        holdings=[], authorization_input={"phase": "B", "V_triggered": False}, duckdb_path=tmp_path / "identity.duckdb",
        candidate_asset_ids=["A"], rule_version_set_id="rules-v1", parameter_set_id="params-v1", input_snapshot_id="snap-v1",
        comparison_score_provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snap-v1"},
    )
    first = run_task09_daily(**kwargs, invocation_id="runtime-1")
    second = run_task09_daily(**kwargs, invocation_id="runtime-2")
    assert first["strategy_run_id"] == second["strategy_run_id"]
    assert first["result_digest"] == second["result_digest"]


def test_persisted_facts_require_one_snapshot_and_preserve_unavailable(tmp_path: Path):
    db = tmp_path / "facts.duckdb"
    unavailable = normalize_decision_facts(
        date(2024, 1, 10), [{"ticker": "A"}],
        provenance={"score_version": "score-v0.1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snapshot-1"},
    )
    with duckdb.connect(str(db)) as connection:
        assert persist_decision_facts(connection, unavailable) == 1
    rows, provenance = load_persisted_decision_facts(db, date(2024, 1, 10))
    assert rows[0]["comparison_score_status"] == "UNAVAILABLE"
    assert provenance["input_snapshot_id"] == "snapshot-1"

    other_snapshot = normalize_decision_facts(
        date(2024, 1, 10), [{"ticker": "B", "comparison_score": 1.0}],
        provenance={"score_version": "score-v0.1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snapshot-2"},
    )
    with duckdb.connect(str(db)) as connection:
        persist_decision_facts(connection, other_snapshot)
    with pytest.raises(ValueError, match="TASK09_FACTS_PROVENANCE_MISMATCH"):
        load_persisted_decision_facts(db, date(2024, 1, 10))


def test_decision_facts_batch_write_rolls_back_all_rows(tmp_path: Path):
    db = tmp_path / "facts-rollback.duckdb"
    rows = normalize_decision_facts(
        date(2024, 1, 10), [{"ticker": "A"}, {"ticker": "B"}],
        provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snapshot-1"},
    )
    invalid = dict(rows[1])
    invalid["ticker"] = None
    with duckdb.connect(str(db)) as connection:
        with pytest.raises(duckdb.ConstraintException):
            persist_decision_facts(connection, (rows[0], invalid))
        assert connection.execute("SELECT COUNT(*) FROM system_b_decision_facts_daily").fetchone()[0] == 0


def test_strategy_rejects_fact_provenance_assertion_mismatch(tmp_path: Path):
    with pytest.raises(ValueError, match="TASK09_FACTS_PROVENANCE_MISMATCH"):
        run_task09_daily(
            trade_date=date(2024, 1, 10), facts=[{"ticker": "A", "comparison_score": 60.0}], holdings=[], authorization_input={},
            invocation_id="runtime", duckdb_path=tmp_path / "mismatch.duckdb", rule_version_set_id="wrong-rules",
            parameter_set_id="params-v1", input_snapshot_id="snap-v1",
            comparison_score_provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snap-v1"},
        )


def test_missing_authorization_is_unavailable_and_blocks_new_positions(tmp_path: Path):
    result = run_task09_daily(
        trade_date=date(2024, 1, 10),
        facts=[{"ticker": "A", "comparison_score": 60.0, "entry_eligible": True, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"}],
        holdings=[], authorization_input={}, invocation_id="runtime", duckdb_path=tmp_path / "authorization.duckdb",
        candidate_asset_ids=["A"], rule_version_set_id="rules-v1", parameter_set_id="params-v1", input_snapshot_id="snap-v1",
        comparison_score_provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snap-v1"},
    )
    assert result["target"].positions == ()
    with duckdb.connect(str(tmp_path / "authorization.duckdb"), read_only=True) as connection:
        assert '"status":"UNAVAILABLE"' in connection.execute("SELECT authorization_json FROM system_b_strategy_result").fetchone()[0]


def test_closeout_resolves_unique_dated_record_and_rolls_back_mismatch(tmp_path: Path):
    db = tmp_path / "closeout.duckdb"
    result = run_task09_daily(
        trade_date=date(2024, 1, 10), facts=[{"ticker": "A", "comparison_score": 60.0, "entry_eligible": True, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"}],
        holdings=[], authorization_input={"phase": "B", "V_triggered": False}, invocation_id="runtime", duckdb_path=db,
        candidate_asset_ids=["A"], rule_version_set_id="rules-v1", parameter_set_id="params-v1", input_snapshot_id="snap-v1",
        comparison_score_provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snap-v1"},
    )
    with pytest.raises(ValueError, match="TASK09_CLOSEOUT_RESULT_MISMATCH"):
        closeout_strategy_daily(duckdb_path=db, trade_date=date(2024, 1, 10), result_digest="wrong")
    with duckdb.connect(str(db), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM system_b_strategy_closeout").fetchone()[0] == 0
    assert closeout_strategy_daily(duckdb_path=db, trade_date=date(2024, 1, 10))
    assert closeout_strategy_daily(duckdb_path=db, trade_date=date(2024, 1, 10), strategy_run_id=result["strategy_run_id"])


def test_non_trading_day_is_explicit_no_op(tmp_path: Path):
    result = run_task09_daily(
        trade_date=date(2024, 2, 3), facts=[], holdings=[], authorization_input={},
        invocation_id="non-trading-1", duckdb_path=tmp_path / "non-trading.duckdb",
        rule_version_set_id="rules-v1", parameter_set_id="params-v1", input_snapshot_id="snap-v1",
        trading_day=False,
    )
    assert result["status"] == "NO_OP"
    assert result["target"].positions == ()
    closeout = closeout_strategy_daily(
        duckdb_path=tmp_path / "non-trading.duckdb", strategy_run_id=result["strategy_run_id"],
        trade_date=date(2024, 2, 3), result_digest=result["result_digest"], target_identity=result["target_identity"],
    )
    with duckdb.connect(str(tmp_path / "non-trading.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT target_kind FROM system_b_strategy_target").fetchone()[0] == "NO_OP"
        assert connection.execute("SELECT result_status FROM system_b_strategy_result").fetchone()[0] == "NO_OP"
        assert connection.execute("SELECT completion_status, closeout_identity FROM system_b_strategy_closeout").fetchone() == ("NO_OP", closeout)


def test_authorization_defaults_to_formal_market_phase_fact(tmp_path: Path):
    db = tmp_path / "market-phase.duckdb"
    with duckdb.connect(str(db)) as connection:
        connection.execute(MARKET_PHASE.duckdb_create_sql())
        connection.execute(
            "INSERT INTO market_phase (trade_date, phase, V_triggered) VALUES (?, ?, ?)",
            [date(2024, 1, 10), "B", False],
        )
    result = run_task09_daily(
        trade_date=date(2024, 1, 10),
        facts=[{"ticker": "A", "comparison_score": 60.0, "entry_eligible": True, "system_b_exit_triggered": False}],
        holdings=[], authorization_input={}, invocation_id="market-phase-1", duckdb_path=db,
        candidate_asset_ids=["A"], rule_version_set_id="rules-v1", parameter_set_id="params-v1", input_snapshot_id="snap-v1",
        comparison_score_provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "snap-v1"},
    )
    assert result["status"] == "SUCCESS"
    with duckdb.connect(str(db), read_only=True) as connection:
        authorization = connection.execute("SELECT authorization_json FROM system_b_strategy_result").fetchone()[0]
        assert "PHASE_B_AUTHORIZED" in authorization


def test_task09_contracts_are_registered_and_valid():
    registry = default_registry()
    assert {"system_b_decision_facts_daily", "system_b_strategy_daily", "system_b_daily_closeout"} <= {item.pipeline_id for item in registry.all()}


def test_task08_replay_and_task09_adapter_have_target_parity(tmp_path: Path):
    facts = pd.DataFrame([
        {"trade_date": "2024-01-09", "ticker": "H1", "comparison_score": 60.0, "entry_eligible": True, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"},
        {"trade_date": "2024-01-09", "ticker": "N1", "comparison_score": 10.0, "entry_eligible": False, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"},
        {"trade_date": "2024-01-09", "ticker": "N2", "comparison_score": 5.0, "entry_eligible": False, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"},
        {"trade_date": "2024-01-10", "ticker": "H1", "comparison_score": 40.0, "entry_eligible": True, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"},
        {"trade_date": "2024-01-10", "ticker": "N1", "comparison_score": 60.0, "entry_eligible": True, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"},
        {"trade_date": "2024-01-10", "ticker": "N2", "comparison_score": 20.0, "entry_eligible": False, "system_b_exit_triggered": False, "severe_abnormal_supervision_status": "INACTIVE"},
    ])
    prices = pd.DataFrame([
        {"trade_date": day, "asset_id": asset, "asset_name": asset, "asset_type": "stock", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0}
        for day in ("2024-01-09", "2024-01-10") for asset in ("H1", "N1", "N2")
    ])
    config = PortfolioBacktestConfig("task09-parity", 100000.0, 6, 0.25, CostRule(0.0003, 0.0005, 10.0))
    provenance = {"score_calculation_version": "score-v1", "rule_version_set_id": "rules-v1", "parameter_set_id": "params-v1", "input_snapshot_id": "fixture-task09-v1"}
    replay = run_system_b_day_by_day_replay(
        facts, prices, config,
        runtime_context={"authorization": True, "candidate_asset_ids": frozenset({"H1", "N1", "N2"}), "comparison_score_provenance": provenance},
    )
    replay_target = replay.portfolio_targets[-1]
    daily = run_task09_daily(
        trade_date=date(2024, 1, 10), facts=facts[facts.trade_date == "2024-01-10"].to_dict("records"),
        holdings=[{"asset_id": "H1", "current_weight": 0.125, "entry_count": 1}],
        authorization_input={"phase": "B", "V_triggered": False}, invocation_id="task09-parity-v1", duckdb_path=tmp_path / "parity.duckdb",
        candidate_asset_ids=["N1", "N2"], rule_version_set_id="rules-v1", parameter_set_id="params-v1", input_snapshot_id="fixture-task09-v1",
        comparison_score_provenance={"score_version": "score-v1", "rule_version": "rules-v1", "parameter_version": "params-v1", "input_snapshot_id": "fixture-task09-v1"},
    )
    assert canonical_target_json(replay_target) == canonical_target_json(daily["target"])
    assert target_digest(replay_target) == daily["target_identity"] == "5b89d2bd8efffca32a97f4d2919246cbdfa4f48258a79881ffc248cd753ac92d"
