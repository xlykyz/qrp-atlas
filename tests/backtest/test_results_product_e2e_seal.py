"""07-C: classic / cross-section / event end-to-end product results seal tests."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.product import (
    CreateBacktestTaskRequest,
    execute_validated_task,
    replay_product_run,
    validate_create_request,
)
from qrp_atlas.backtest.product.schemas import (
    BacktestCostConfigDTO,
    BacktestExecutionConfigDTO,
    BacktestPositionConfigDTO,
)
from qrp_atlas.backtest.results.loader import BacktestRunsLoader


def _insert_df(con: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame) -> None:
    con.register("_tmp_df", frame)
    con.execute(f"INSERT INTO {table} SELECT * FROM _tmp_df")
    con.unregister("_tmp_df")


def _make_classic_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "classic_product.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE daily_market_snapshot (
            trade_date DATE,
            ticker VARCHAR,
            name VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            turnover DOUBLE,
            market_cap DOUBLE,
            float_cap DOUBLE,
            is_st BOOLEAN,
            is_limit_up BOOLEAN,
            is_limit_down BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE suspend_d (
            trade_date DATE,
            ticker VARCHAR,
            suspend_timing VARCHAR,
            suspend_type VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE index_daily (
            trade_date DATE,
            index_code VARCHAR,
            index_name VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE
        )
        """
    )
    dates = pd.bdate_range("2024-01-02", periods=60)
    rows = []
    idx_rows = []
    for i, d in enumerate(dates):
        close = 10 + i * 0.12 + ((-1) ** i) * 0.15
        rows.append(
            (
                d.date().isoformat(),
                "000001.SZ",
                "000001.SZ",
                close - 0.05,
                close + 0.2,
                close - 0.2,
                close,
                1_000_000.0,
                1_000_000.0 * close,
                0.01,
                1e10 + i * 1e8,
                5e9,
                False,
                False,
                False,
            )
        )
        idx_close = 1000 + i * 1.5
        idx_rows.append(
            (
                d.date().isoformat(),
                "000300.SH",
                "CSI300",
                idx_close - 1,
                idx_close + 1,
                idx_close - 1,
                idx_close,
                1e9,
                1e12,
            )
        )
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.executemany(
        "INSERT INTO index_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        idx_rows,
    )
    con.close()
    return db_path


def _make_cs_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "cs_product_seal.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE daily_market_snapshot (
            trade_date DATE,
            ticker VARCHAR,
            name VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            turnover DOUBLE,
            market_cap DOUBLE,
            float_cap DOUBLE,
            is_st BOOLEAN,
            is_limit_up BOOLEAN,
            is_limit_down BOOLEAN
        )
        """
    )
    con.execute(
        """
        CREATE TABLE suspend_d (
            trade_date DATE,
            ticker VARCHAR,
            suspend_timing VARCHAR,
            suspend_type VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE index_component_history (
            index_code VARCHAR,
            asset_id VARCHAR,
            snapshot_date DATE,
            weight DOUBLE,
            effective_from DATE,
            effective_to DATE,
            available_trade_date DATE,
            source VARCHAR,
            source_record_id VARCHAR,
            revision_id VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE industry_membership_history (
            asset_id VARCHAR,
            classification_system VARCHAR,
            industry_level INTEGER,
            industry_code VARCHAR,
            industry_name VARCHAR,
            effective_from DATE,
            effective_to DATE,
            available_trade_date DATE,
            source VARCHAR,
            source_record_id VARCHAR,
            revision_id VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    dates = pd.bdate_range("2024-01-02", periods=50)
    tickers = ["AAA.SZ", "BBB.SZ", "CCC.SZ", "DDD.SZ", "EEE.SZ"]
    industries = {
        "AAA.SZ": ("I1", "Bank"),
        "BBB.SZ": ("I1", "Bank"),
        "CCC.SZ": ("I2", "Tech"),
        "DDD.SZ": ("I2", "Tech"),
        "EEE.SZ": ("I3", "Consumer"),
    }
    rows = []
    for i, d in enumerate(dates):
        for j, ticker in enumerate(tickers):
            close = 10 + i * (0.2 + j * 0.05) + j
            mcap = (j + 1) * 1e10 + i * 1e8
            rows.append(
                (
                    d.date().isoformat(),
                    ticker,
                    ticker,
                    close - 0.05,
                    close + 0.15,
                    close - 0.15,
                    close,
                    1_000_000.0,
                    1_000_000.0 * close,
                    0.01,
                    mcap,
                    mcap * 0.5,
                    False,
                    False,
                    False,
                )
            )
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    index_rows = []
    rid = 0

    def add_index(**kwargs):
        nonlocal rid
        rid += 1
        index_rows.append(
            {
                "index_code": kwargs["index_code"],
                "asset_id": kwargs["asset_id"],
                "snapshot_date": kwargs["snapshot_date"],
                "weight": kwargs["weight"],
                "effective_from": kwargs["snapshot_date"],
                "effective_to": None,
                "available_trade_date": kwargs["available_trade_date"],
                "source": "test",
                "source_record_id": f"src-{rid}",
                "revision_id": f"rev-{rid}",
                "ingested_at": kwargs["ingested_at"],
            }
        )

    for asset, w in [("AAA.SZ", 0.3), ("BBB.SZ", 0.3), ("CCC.SZ", 0.4)]:
        add_index(
            index_code="000300.SH",
            asset_id=asset,
            snapshot_date=date(2024, 1, 2),
            weight=w,
            available_trade_date=date(2024, 1, 2),
            ingested_at=datetime(2024, 1, 2, 8, 0, 0),
        )
    for asset, w in [("BBB.SZ", 0.3), ("CCC.SZ", 0.3), ("DDD.SZ", 0.4)]:
        add_index(
            index_code="000300.SH",
            asset_id=asset,
            snapshot_date=date(2024, 2, 1),
            weight=w,
            available_trade_date=date(2024, 2, 1),
            ingested_at=datetime(2024, 2, 1, 8, 0, 0),
        )
    _insert_df(con, "index_component_history", pd.DataFrame(index_rows))

    ind_rows = []
    for asset, (code, name) in industries.items():
        ind_rows.append(
            {
                "asset_id": asset,
                "classification_system": "sw2021",
                "industry_level": 1,
                "industry_code": code,
                "industry_name": name,
                "effective_from": date(2023, 1, 1),
                "effective_to": None,
                "available_trade_date": date(2023, 1, 1),
                "source": "test",
                "source_record_id": f"ind-{asset}",
                "revision_id": f"indrev-{asset}",
                "ingested_at": datetime(2023, 1, 1, 8, 0, 0),
            }
        )
    _insert_df(con, "industry_membership_history", pd.DataFrame(ind_rows))
    con.close()
    return db_path


def _make_event_db(tmp_path: Path) -> Path:
    # Reuse the existing event fixture shape from product event tests (minimal).
    from tests.backtest.test_product_event_loop import _make_event_db as _event_db

    return _event_db(tmp_path)


def _assert_common_package(run_dir: Path) -> dict:
    required = [
        "run_meta.json",
        "summary.json",
        "equity.json",
        "trades.json",
        "orders.json",
        "fills.json",
        "targets.json",
        "snapshots.json",
        "config.json",
        "daily_returns.json",
        "rolling_performance.json",
        "costs.json",
        "diagnostics.json",
        "benchmark.json",
        "exposures.json",
        "reproducibility.json",
    ]
    for name in required:
        assert (run_dir / name).exists(), name
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    repro = json.loads((run_dir / "reproducibility.json").read_text(encoding="utf-8"))
    assert repro.get("locked_to_run_snapshot") is True
    assert isinstance(repro.get("snapshot_hash"), str) and len(repro["snapshot_hash"]) == 64
    universe = repro.get("universe") or {}
    assert "resolved_assets" in universe
    assert "data_fingerprints" in repro
    assert (repro.get("replay") or {}).get("supported") is True
    return {"summary": summary, "repro": repro}


def _classic_request() -> CreateBacktestTaskRequest:
    return validate_create_request(
        CreateBacktestTaskRequest(
            name="classic seal",
            strategy_code="dual_sma_trend",
            strategy_version="1.0.0",
            strategy_params={"fast_window": 3, "slow_window": 8},
            start_date="2024-01-08",
            end_date="2024-03-15",
            universe_mode="tickers",
            tickers=["000001.SZ"],
            benchmark_id="000300.SH",
            execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
            cost=BacktestCostConfigDTO(),
            position=BacktestPositionConfigDTO(initial_cash=1_000_000, max_positions=5),
        )
    )


def test_classic_product_results_seal(tmp_path: Path):
    db_path = _make_classic_db(tmp_path)
    request = _classic_request()
    run_id, run_dir = execute_validated_task(
        request,
        run_id="classic_seal",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    pkg = _assert_common_package(run_dir)
    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    closed = [t for t in trades if t.get("status") == "closed"]
    if closed:
        assert any(t.get("mae_pct") is not None for t in closed)
        assert any(t.get("mfe_pct") is not None for t in closed)
    bench = json.loads((run_dir / "benchmark.json").read_text(encoding="utf-8"))
    assert bench.get("benchmark_id") == "000300.SH"
    # continuous fixture => full-range relative available
    assert bench["summary"].get("full_range_excess_available") is True
    assert "relative_return" in bench["summary"]
    assert "excess_percentage_point" in bench["summary"]
    points = bench["points"]
    assert any(p.get("daily_active_return") is not None for p in points)

    exp = json.loads((run_dir / "exposures.json").read_text(encoding="utf-8"))
    assert "position_concentration" in exp
    # non-CS: industry/mcap not claimed sealed
    assert exp.get("industry_available") is False
    assert exp.get("market_cap_available") is False

    # replay re-execution matches business outcomes
    replay = replay_product_run(
        run_id,
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        new_run_id="classic_seal_replay",
    )
    assert replay["match"]["all_business"] is True
    assert replay["match"]["strategy_definition_match"] is True
    assert replay["match"]["resolved_universe_match"] is True
    assert replay["match"]["data_fingerprints_match"] is True
    assert replay["match"]["execution_targets_match"] is True
    assert replay["match"]["equity"] is True
    assert replay["match"]["fills"] is True
    assert replay["match"]["trades"] is True
    assert pkg["repro"]["universe"]["resolved_assets"]


def test_replay_detects_current_price_data_change(tmp_path: Path):
    db_path = _make_classic_db(tmp_path)
    runs_dir = tmp_path / "runs"
    run_id, _ = execute_validated_task(
        _classic_request(),
        run_id="classic_data_source",
        runs_dir=runs_dir,
        db_path=db_path,
    )
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        UPDATE daily_market_snapshot
        SET close = close + 3.0
        WHERE ticker = '000001.SZ' AND trade_date = DATE '2024-02-01'
        """
    )
    con.close()

    replay = replay_product_run(
        run_id,
        runs_dir=runs_dir,
        db_path=db_path,
        new_run_id="classic_data_replay",
    )

    assert replay["match"]["data_fingerprints_match"] is False
    assert replay["match"]["all_business"] is False


def test_replay_detects_order_fill_and_target_artifact_mismatch(tmp_path: Path):
    db_path = _make_classic_db(tmp_path)
    runs_dir = tmp_path / "runs"
    run_id, run_dir = execute_validated_task(
        _classic_request(),
        run_id="classic_artifact_source",
        runs_dir=runs_dir,
        db_path=db_path,
    )

    orders = json.loads((run_dir / "orders.json").read_text(encoding="utf-8"))
    fills = json.loads((run_dir / "fills.json").read_text(encoding="utf-8"))
    targets = json.loads((run_dir / "targets.json").read_text(encoding="utf-8"))
    assert orders and fills and targets
    orders[0]["status"] = "REJECTED"
    orders[0]["reason"] = "TEST_MISMATCH"
    fills[0]["execution_price"] = float(fills[0]["execution_price"]) + 1.0
    targets[0]["target_weight"] = float(targets[0]["target_weight"]) / 2.0
    (run_dir / "orders.json").write_text(json.dumps(orders), encoding="utf-8")
    (run_dir / "fills.json").write_text(json.dumps(fills), encoding="utf-8")
    (run_dir / "targets.json").write_text(json.dumps(targets), encoding="utf-8")

    replay = replay_product_run(
        run_id,
        runs_dir=runs_dir,
        db_path=db_path,
        new_run_id="classic_artifact_replay",
    )

    assert replay["match"]["orders_match"] is False
    assert replay["match"]["fills_match"] is False
    assert replay["match"]["execution_targets_match"] is False
    assert replay["match"]["all_business"] is False


def test_cross_section_product_results_seal_mae_mfe_and_exposures(tmp_path: Path):
    db_path = _make_cs_db(tmp_path)
    request = validate_create_request(
        CreateBacktestTaskRequest(
            name="cs seal",
            strategy_code="cross_sectional_momentum_long_only",
            strategy_version="1.0.0",
            strategy_params={
                "top_n": 2,
                "momentum_lookback": 3,
                "rebalance_frequency": "weekly",
                "cash_buffer": 0.0,
                "ascending": False,
            },
            start_date="2024-01-15",
            end_date="2024-02-28",
            universe_mode="index_components",
            index_code="000300.SH",
            execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
            cost=BacktestCostConfigDTO(
                commission_rate=0.00025, stamp_tax_rate=0.0005, slippage_bps=0
            ),
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000, max_positions=2, max_weight_per_symbol=0.5
            ),
        )
    )
    run_id, run_dir = execute_validated_task(
        request,
        run_id="cs_seal",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    _assert_common_package(run_dir)
    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    closed = [t for t in trades if t.get("status") == "closed"]
    assert closed, "CS seal fixture must produce closed trades"
    assert any(t.get("mae_pct") is not None for t in closed), closed
    assert any(t.get("mfe_pct") is not None for t in closed), closed
    for t in closed:
        if t.get("mae_pct") is not None:
            assert isinstance(t["mae_pct"], (int, float))
        if t.get("mfe_pct") is not None:
            assert isinstance(t["mfe_pct"], (int, float))

    exp = json.loads((run_dir / "exposures.json").read_text(encoding="utf-8"))
    assert "position_concentration" in exp
    # market cap from daily size fields should be available
    assert exp.get("market_cap_available") is True
    assert exp.get("market_cap"), exp
    # industry from membership history
    assert exp.get("industry_available") is True
    assert exp.get("industry"), exp
    # concentration must not be stuffed into market_cap rows
    for row in exp["market_cap"]:
        assert "max_weight" not in row
        assert "position_count" not in row
        assert "weighted_log_market_cap" in row

    repro = json.loads((run_dir / "reproducibility.json").read_text(encoding="utf-8"))
    assert repro["universe"]["resolved_asset_count"] >= 1
    assert repro["data_fingerprints"].get("prices") is not None

    replay = replay_product_run(
        run_id,
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        new_run_id="cs_seal_replay",
    )
    assert replay["match"]["all_business"] is True


def test_event_product_results_seal(tmp_path: Path):
    from qrp_atlas.strategies import get_strategy

    db_path = _make_event_db(tmp_path)
    strategy = get_strategy("event_drift_basic")
    request = validate_create_request(
        CreateBacktestTaskRequest(
            name="event seal",
            strategy_code="event_drift_basic",
            strategy_version=strategy.definition.version,
            strategy_params={"hold_days": 3, "min_profit_change_midpoint": 0.0},
            start_date="2024-03-18",
            end_date="2024-04-05",
            universe_mode="tickers",
            tickers=["000001.SZ", "600519.SH", "300750.SZ"],
            execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
            cost=BacktestCostConfigDTO(
                commission_rate=0.00025, stamp_tax_rate=0.0005, slippage_bps=5
            ),
            position=BacktestPositionConfigDTO(
                initial_cash=1_000_000, max_positions=5, max_weight_per_symbol=0.5
            ),
        )
    )
    run_id, run_dir = execute_validated_task(
        request,
        run_id="event_seal",
        runs_dir=tmp_path / "runs",
        db_path=db_path,
    )
    _assert_common_package(run_dir)
    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    # event path may have closed or open; if closed, MAE/MFE should exist
    closed = [t for t in trades if t.get("status") == "closed"]
    if closed:
        assert any(t.get("mae_pct") is not None or t.get("mfe_pct") is not None for t in closed)
    repro = json.loads((run_dir / "reproducibility.json").read_text(encoding="utf-8"))
    assert "event" in (repro.get("pit") or {})
    replay = replay_product_run(
        run_id,
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        new_run_id="event_seal_replay",
    )
    assert replay["match"]["summary_business_fields"] is True
    assert replay["match"]["equity"] is True
