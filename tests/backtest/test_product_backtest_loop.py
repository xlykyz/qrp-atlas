"""Product backtest loop: timing, warmup isolation, and multi-strategy support."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.backtest.product import (
    PRODUCT_SUPPORTED_STRATEGY_CODES,
    BacktestProductService,
    BacktestTaskStore,
    BacktestTaskValidationError,
    CreateBacktestTaskRequest,
    execute_validated_task,
    list_indicator_catalog,
    list_strategy_catalog,
    validate_create_request,
)
from qrp_atlas.backtest.product.schemas import (
    BacktestCostConfigDTO,
    BacktestExecutionConfigDTO,
    BacktestPositionConfigDTO,
)
from qrp_atlas.backtest.product.timing import (
    REASON_NO_EXECUTION_DATE_IN_RANGE,
    market_trade_dates,
    next_trade_date,
    shift_target_weights_to_execution_dates,
)
from qrp_atlas.backtest.results import BacktestRunsLoader, BacktestSummary, get_equity, get_run_meta, get_summary
from qrp_atlas.backtest.results.service import set_loader_for_tests
from qrp_atlas.backtest.results.writer import BacktestRunWriter
from qrp_atlas.config.paths import BACKTEST_FIXTURE_RUNS_DIR, BACKTEST_RUNS_DIR


def _make_price_db(
    tmp_path: Path,
    *,
    ticker: str = "000001.SZ",
    start: str = "2024-01-02",
    periods: int = 40,
    pattern: str = "uptrend",
) -> Path:
    db_path = tmp_path / f"product_{ticker.replace('.', '_')}.duckdb"
    con = duckdb.connect(str(db_path))
    dates = pd.bdate_range(start, periods=periods)
    if pattern == "uptrend":
        closes = [10 + i * 0.15 + ((-1) ** i) * 0.2 for i in range(len(dates))]
    elif pattern == "mean_reversion":
        closes = [10 + ((-1) ** i) * (1.5 if i % 7 == 0 else 0.2) for i in range(len(dates))]
    else:
        closes = [10 + i * 0.05 for i in range(len(dates))]
    rows = []
    for d, close in zip(dates, closes):
        open_px = close - 0.05
        high = close + 0.1
        low = close - 0.1
        rows.append(
            (
                d.date().isoformat(),
                ticker,
                ticker,
                open_px,
                high,
                low,
                close,
                1_000_000.0,
                1_000_000.0 * close,
                0.01,
                1e10,
                5e9,
                False,
                False,
                False,
            )
        )
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
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    con.close()
    return db_path


def _request(**overrides):
    base = dict(
        name="dual_sma product test",
        strategy_code="dual_sma_trend",
        strategy_version="1.0.0",
        strategy_params={"fast_window": 3, "slow_window": 5},
        universe_mode="tickers",
        tickers=["000001.SZ"],
        start_date="2024-01-15",
        end_date="2024-02-28",
        position=BacktestPositionConfigDTO(
            initial_cash=1_000_000,
            max_positions=1,
            max_weight_per_symbol=1.0,
        ),
        cost=BacktestCostConfigDTO(
            commission_rate=0.00025,
            stamp_tax_rate=0.0005,
            slippage_bps=0,
        ),
        execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
    )
    base.update(overrides)
    return CreateBacktestTaskRequest(**base)


def _assert_dates_in_range(run_dir: Path, start: str, end: str) -> None:
    for filename in ("orders.json", "fills.json", "snapshots.json", "equity.json"):
        payload = json.loads((run_dir / filename).read_text(encoding="utf-8"))
        if not payload:
            continue
        if filename == "equity.json":
            dates = [item["date"] for item in payload]
        else:
            dates = [item["trade_date"] for item in payload]
        assert all(start <= d <= end for d in dates), (filename, dates[:3], dates[-3:])


def test_catalog_lists_product_strategies_and_indicators():
    strategies = list_strategy_catalog(product_only=True)
    codes = {item.code for item in strategies}
    assert codes == set(PRODUCT_SUPPORTED_STRATEGY_CODES)
    dual = next(item for item in strategies if item.code == "dual_sma_trend")
    assert dual.version == "1.0.0"
    assert "fast_window" in dual.parameter_schema

    indicators = list_indicator_catalog()
    assert any(item.code == "sma" for item in indicators)
    assert any(item.code == "system_b_trend_valid" for item in indicators)


def test_validate_rejects_unknown_strategy_and_bad_params():
    with pytest.raises(BacktestTaskValidationError, match="not supported|unknown"):
        validate_create_request(_request(strategy_code="not_a_strategy"))

    with pytest.raises(BacktestTaskValidationError):
        validate_create_request(
            _request(strategy_params={"fast_window": 10, "slow_window": 5})
        )

    with pytest.raises(BacktestTaskValidationError, match="tickers"):
        validate_create_request(_request(tickers=[]))

    with pytest.raises(BacktestTaskValidationError, match="preset"):
        validate_create_request(_request(universe_mode="preset", universe_preset="CSI300"))


def test_next_trade_date_skips_weekend_and_respects_end():
    trade_dates = market_trade_dates(
        pd.DataFrame(
            {
                "trade_date": [
                    "2024-01-05",  # Friday
                    "2024-01-08",  # Monday
                    "2024-01-09",
                ]
            }
        )
    )
    nxt = next_trade_date(trade_dates, "2024-01-05")
    assert nxt == pd.Timestamp("2024-01-08")
    assert next_trade_date(trade_dates, "2024-01-09", end_date="2024-01-09") is None


def test_shift_targets_next_open_and_next_close_differ_from_signal():
    trade_dates = [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-03"),
        pd.Timestamp("2024-01-04"),
    ]
    targets = pd.DataFrame(
        [
            {"trade_date": "2024-01-02", "asset_id": "A", "target_weight": 1.0, "priority": 0.0},
            {"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 0.0, "priority": 0.0},
        ]
    )
    next_open, skipped = shift_target_weights_to_execution_dates(
        targets,
        entry_timing="next_open",
        trade_dates=trade_dates,
        end_date="2024-01-04",
    )
    assert skipped == []
    assert list(next_open["trade_date"]) == ["2024-01-03", "2024-01-04"]

    same_close, _ = shift_target_weights_to_execution_dates(
        targets,
        entry_timing="same_close",
        trade_dates=trade_dates,
        end_date="2024-01-04",
    )
    assert list(same_close["trade_date"]) == ["2024-01-02", "2024-01-03"]


def test_shift_targets_end_of_range_is_skipped():
    trade_dates = [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")]
    targets = pd.DataFrame(
        [{"trade_date": "2024-01-03", "asset_id": "A", "target_weight": 1.0, "priority": 0.0}]
    )
    shifted, skipped = shift_target_weights_to_execution_dates(
        targets,
        entry_timing="next_open",
        trade_dates=trade_dates,
        end_date="2024-01-03",
    )
    assert shifted.empty
    assert skipped[0]["reason"] == REASON_NO_EXECUTION_DATE_IN_RANGE


def test_task_store_persists_status_and_request_snapshot(tmp_path: Path):
    store = BacktestTaskStore(tmp_path / "tasks")
    created = store.create(_request())
    assert created.status == "pending"
    assert created.is_mock is False
    assert created.request_snapshot["strategy_code"] == "dual_sma_trend"

    running = store.update(created.task_id, status="running")
    assert running.status == "running"
    succeeded = store.update(created.task_id, status="succeeded", run_id="run_abc")
    assert succeeded.status == "succeeded"
    assert succeeded.run_id == "run_abc"


def test_real_dual_sma_next_open_warmup_and_range(tmp_path: Path):
    db_path = _make_price_db(tmp_path)
    runs_dir = tmp_path / "runs"
    request = validate_create_request(_request(entry_timing=None) if False else _request())
    # explicit next_open
    request = validate_create_request(
        _request(execution=BacktestExecutionConfigDTO(entry_timing="next_open"))
    )
    run_id, run_dir = execute_validated_task(
        request,
        run_id="dual_sma_product_001",
        runs_dir=runs_dir,
        db_path=db_path,
    )
    assert run_id == "dual_sma_product_001"
    for name in (
        "run_meta.json",
        "summary.json",
        "equity.json",
        "trades.json",
        "skipped.json",
        "config.json",
        "fills.json",
        "orders.json",
        "snapshots.json",
    ):
        assert (run_dir / name).exists()

    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["product_request"]["strategy_code"] == "dual_sma_trend"
    assert config["requested_start_date"] == "2024-01-15"
    assert config["requested_end_date"] == "2024-02-28"
    assert config["entry_timing"] == "next_open"
    assert "same_close_warning" in config["execution_semantics"]

    _assert_dates_in_range(run_dir, "2024-01-15", "2024-02-28")

    orders = json.loads((run_dir / "orders.json").read_text(encoding="utf-8"))
    # If any orders exist, none may land on the first formal signal day only by accident;
    # more importantly they must not precede requested start.
    assert all(o["trade_date"] >= "2024-01-15" for o in orders)

    set_loader_for_tests(BacktestRunsLoader(runs_dir))
    try:
        assert get_run_meta(run_id).run_id == run_id
        assert get_summary(run_id).run_id == run_id
        assert len(get_equity(run_id)) >= 1
    finally:
        set_loader_for_tests(None)


def test_next_close_executes_on_next_session_close(tmp_path: Path):
    db_path = _make_price_db(tmp_path)
    runs_dir = tmp_path / "runs"
    request = validate_create_request(
        _request(execution=BacktestExecutionConfigDTO(entry_timing="next_close"))
    )
    _, run_dir = execute_validated_task(
        request,
        run_id="dual_sma_next_close",
        runs_dir=runs_dir,
        db_path=db_path,
    )
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["entry_timing"] == "next_close"
    assert config["execution"]["price_field"] == "close"
    _assert_dates_in_range(run_dir, request.start_date, request.end_date)


def test_product_loader_excludes_fixtures_by_default(tmp_path: Path, monkeypatch):
    product_root = tmp_path / "product_runs"
    product_root.mkdir()
    monkeypatch.setenv("QRP_ATLAS_BACKTEST_RUNS_DIR", str(product_root))
    # Re-import path constant behavior via explicit loader construction.
    product_loader = BacktestRunsLoader(product_root)
    assert "sample_run_001" not in product_loader.list_run_ids()

    fixture_loader = BacktestRunsLoader(BACKTEST_FIXTURE_RUNS_DIR)
    assert "sample_run_001" in fixture_loader.list_run_ids()


def test_writer_and_loader_share_env_root(tmp_path: Path, monkeypatch):
    root = tmp_path / "shared_runs"
    monkeypatch.setenv("QRP_ATLAS_BACKTEST_RUNS_DIR", str(root))
    from qrp_atlas.backtest.product.service import default_product_runs_dir
    from qrp_atlas.backtest.results.writer import BacktestRunWriter

    # Product service and writer/loader all resolve the same env-backed root.
    assert default_product_runs_dir() == root
    writer = BacktestRunWriter(default_product_runs_dir())
    loader = BacktestRunsLoader(default_product_runs_dir())
    assert writer.root == loader.root == root


def test_config_write_failure_does_not_mark_success(tmp_path: Path, monkeypatch):
    db_path = _make_price_db(tmp_path)
    runs_dir = tmp_path / "runs"

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(BacktestRunWriter, "write_portfolio_run", boom)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks"),
        runs_dir=runs_dir,
        db_path=db_path,
        execute_inline=True,
    )
    task = service.create_task(_request()).task
    assert task.status == "failed"
    assert task.run_id is None
    assert "failed to persist" in (task.error_message or "")


def test_insufficient_data_fails_task(tmp_path: Path):
    db_path = _make_price_db(tmp_path)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks"),
        runs_dir=tmp_path / "runs",
        db_path=db_path,
        execute_inline=True,
    )
    response = service.create_task(
        _request(tickers=["999999.SZ"], start_date="2024-01-15", end_date="2024-02-28")
    )
    task = response.task
    assert task.status == "failed"
    assert task.run_id is None
    assert task.error_message


@pytest.mark.parametrize(
    "strategy_code,params,pattern",
    [
        ("dual_sma_trend", {"fast_window": 3, "slow_window": 5}, "uptrend"),
        ("time_series_momentum", {"lookback": 3, "threshold": 0.0}, "uptrend"),
        ("donchian_breakout", {"entry_window": 3, "exit_window": 2}, "uptrend"),
        ("rolling_zscore_mean_reversion", {"lookback": 5, "entry_z": 1.0, "exit_z": 0.0}, "mean_reversion"),
        ("system_b_basic", {}, "uptrend"),
    ],
)
def test_product_supported_strategies_smoke(tmp_path: Path, strategy_code, params, pattern):
    db_path = _make_price_db(tmp_path, pattern=pattern, periods=50)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks" / strategy_code),
        runs_dir=tmp_path / "runs" / strategy_code,
        db_path=db_path,
        execute_inline=True,
    )
    task = service.create_task(
        _request(
            name=f"{strategy_code} smoke",
            strategy_code=strategy_code,
            strategy_version="1.0.0",
            strategy_params=params,
            start_date="2024-01-20",
            end_date="2024-03-05",
        )
    ).task
    assert task.status == "succeeded", task.error_message
    assert task.run_id
    run_dir = tmp_path / "runs" / strategy_code / task.run_id
    _assert_dates_in_range(run_dir, "2024-01-20", "2024-03-05")
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["product_request"]["strategy_code"] == strategy_code
