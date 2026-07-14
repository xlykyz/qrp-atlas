"""07-B2: event_drift_basic product loop tests."""

from __future__ import annotations

import json
from datetime import date, datetime
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
    list_strategy_catalog,
    validate_create_request,
)
from qrp_atlas.backtest.product.schemas import (
    BacktestCostConfigDTO,
    BacktestExecutionConfigDTO,
    BacktestPositionConfigDTO,
)
from qrp_atlas.backtest.results.loader import BacktestRunsLoader
from qrp_atlas.strategies import get_strategy


def _bdates(start: str, n: int) -> list[str]:
    days = list(pd.bdate_range(start, periods=n))
    return [d.date().isoformat() for d in days]


def _make_event_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "event_product.duckdb"
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
        CREATE TABLE earnings_forecast_event (
            ticker VARCHAR,
            event_type VARCHAR,
            event_series_id VARCHAR,
            report_period DATE,
            announcement_date DATE,
            first_announcement_date DATE,
            published_at TIMESTAMP,
            time_precision VARCHAR,
            available_trade_date DATE,
            forecast_type VARCHAR,
            profit_change_min DOUBLE,
            profit_change_max DOUBLE,
            net_profit_min DOUBLE,
            net_profit_max DOUBLE,
            last_parent_net DOUBLE,
            summary VARCHAR,
            change_reason VARCHAR,
            source VARCHAR,
            source_record_id VARCHAR,
            revision_id VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )

    trade_days = _bdates("2024-03-01", 40)
    tickers = ["000001.SZ", "600519.SH", "300750.SZ"]
    price_rows = []
    for i, d in enumerate(trade_days):
        for j, ticker in enumerate(tickers):
            close = 10 + i * 0.2 + j
            open_px = close - 0.05
            price_rows.append(
                (
                    d,
                    ticker,
                    ticker,
                    open_px,
                    close + 0.1,
                    close - 0.1,
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
    con.executemany(
        "INSERT INTO daily_market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        price_rows,
    )

    # Friday announcement 2024-03-15 -> next open Monday 2024-03-18
    # Same-calendar-day announcement must not map to same available day.
    event_rows = [
        {
            "ticker": "000001.SZ",
            "event_type": "earnings_forecast",
            "event_series_id": "s1",
            "report_period": date(2023, 12, 31),
            "announcement_date": date(2024, 3, 15),
            "first_announcement_date": date(2024, 3, 15),
            "published_at": datetime(2024, 3, 15, 18, 0, 0),
            "time_precision": "date",
            "available_trade_date": date(2024, 3, 18),
            "forecast_type": "预增",
            "profit_change_min": 20.0,
            "profit_change_max": 40.0,
            "net_profit_min": 100.0,
            "net_profit_max": 120.0,
            "last_parent_net": None,
            "summary": None,
            "change_reason": None,
            "source": "test",
            "source_record_id": "src-s1-v1",
            "revision_id": "rev-s1-v1",
            "ingested_at": datetime(2024, 3, 15, 20, 0, 0),
        },
        # technical revision later; later as_of may see it, earlier product end should still be consistent via as_of
        {
            "ticker": "000001.SZ",
            "event_type": "earnings_forecast",
            "event_series_id": "s1",
            "report_period": date(2023, 12, 31),
            "announcement_date": date(2024, 3, 15),
            "first_announcement_date": date(2024, 3, 15),
            "published_at": datetime(2024, 3, 15, 18, 0, 0),
            "time_precision": "date",
            "available_trade_date": date(2024, 3, 18),
            "forecast_type": "预增",
            "profit_change_min": 25.0,
            "profit_change_max": 45.0,
            "net_profit_min": 100.0,
            "net_profit_max": 120.0,
            "last_parent_net": None,
            "summary": "tech revision",
            "change_reason": None,
            "source": "test",
            "source_record_id": "src-s1-v1",
            "revision_id": "rev-s1-v2",
            "ingested_at": datetime(2024, 4, 1, 12, 0, 0),
        },
        # future event relative to early windows
        {
            "ticker": "600519.SH",
            "event_type": "earnings_forecast",
            "event_series_id": "s2",
            "report_period": date(2024, 3, 31),
            "announcement_date": date(2024, 4, 10),
            "first_announcement_date": date(2024, 4, 10),
            "published_at": datetime(2024, 4, 10, 18, 0, 0),
            "time_precision": "date",
            "available_trade_date": date(2024, 4, 11),
            "forecast_type": "预增",
            "profit_change_min": 50.0,
            "profit_change_max": 80.0,
            "net_profit_min": 200.0,
            "net_profit_max": 250.0,
            "last_parent_net": None,
            "summary": None,
            "change_reason": None,
            "source": "test",
            "source_record_id": "src-s2",
            "revision_id": "rev-s2",
            "ingested_at": datetime(2024, 4, 10, 20, 0, 0),
        },
        # negative forecast should not enter under min midpoint 0
        {
            "ticker": "300750.SZ",
            "event_type": "earnings_forecast",
            "event_series_id": "s3",
            "report_period": date(2023, 12, 31),
            "announcement_date": date(2024, 3, 15),
            "first_announcement_date": date(2024, 3, 15),
            "published_at": datetime(2024, 3, 15, 18, 0, 0),
            "time_precision": "date",
            "available_trade_date": date(2024, 3, 18),
            "forecast_type": "预减",
            "profit_change_min": -40.0,
            "profit_change_max": -10.0,
            "net_profit_min": -20.0,
            "net_profit_max": -10.0,
            "last_parent_net": None,
            "summary": None,
            "change_reason": None,
            "source": "test",
            "source_record_id": "src-s3",
            "revision_id": "rev-s3",
            "ingested_at": datetime(2024, 3, 15, 20, 0, 0),
        },
    ]
    for row in event_rows:
        con.execute(
            """
            INSERT INTO earnings_forecast_event VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                row["ticker"],
                row["event_type"],
                row["event_series_id"],
                row["report_period"],
                row["announcement_date"],
                row["first_announcement_date"],
                row["published_at"],
                row["time_precision"],
                row["available_trade_date"],
                row["forecast_type"],
                row["profit_change_min"],
                row["profit_change_max"],
                row["net_profit_min"],
                row["net_profit_max"],
                row["last_parent_net"],
                row["summary"],
                row["change_reason"],
                row["source"],
                row["source_record_id"],
                row["revision_id"],
                row["ingested_at"],
            ],
        )
    con.close()
    return db_path


def _request(**overrides) -> CreateBacktestTaskRequest:
    strategy = get_strategy("event_drift_basic")
    base = dict(
        name="event-product",
        strategy_code="event_drift_basic",
        strategy_version=strategy.definition.version,
        strategy_params={"hold_days": 3, "min_profit_change_midpoint": 0.0},
        universe_mode="tickers",
        tickers=["000001.SZ", "600519.SH", "300750.SZ"],
        start_date="2024-03-18",
        end_date="2024-04-05",
        position=BacktestPositionConfigDTO(
            initial_cash=1_000_000, max_positions=5, max_weight_per_symbol=0.5
        ),
        cost=BacktestCostConfigDTO(
            commission_rate=0.00025, stamp_tax_rate=0.0005, slippage_bps=5
        ),
        execution=BacktestExecutionConfigDTO(entry_timing="next_open"),
    )
    base.update(overrides)
    return CreateBacktestTaskRequest(**base)


def test_catalog_includes_event_strategy_metadata():
    assert "event_drift_basic" in PRODUCT_SUPPORTED_STRATEGY_CODES
    items = list_strategy_catalog(product_only=True)
    codes = {item.code for item in items}
    assert "event_drift_basic" in codes
    event = next(item for item in items if item.code == "event_drift_basic")
    assert event.product_supported is True
    assert event.supported_entry_timings == ["next_open"]
    assert event.supported_universe_modes == ["tickers"]
    assert "hold_days" in event.parameter_schema
    assert "min_profit_change_midpoint" in event.parameter_schema


def test_validate_rejects_non_next_open_and_bad_universe():
    with pytest.raises(BacktestTaskValidationError, match="entry_timing"):
        validate_create_request(
            _request(execution=BacktestExecutionConfigDTO(entry_timing="same_close"))
        )
    with pytest.raises(BacktestTaskValidationError, match="universe_mode"):
        validate_create_request(_request(universe_mode="index_components", index_code="000300.SH", tickers=None))


def test_weekend_announcement_maps_to_next_open_no_second_shift(tmp_path: Path):
    db = _make_event_db(tmp_path)
    run_id, run_dir = execute_validated_task(
        _request(start_date="2024-03-18", end_date="2024-03-29"),
        runs_dir=tmp_path / "runs",
        db_path=db,
        run_id="evt_weekend",
    )
    assert run_id == "evt_weekend"
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    event_meta = config["event"]
    assert event_meta["product_timing_shift"] is False
    assert event_meta["time_semantics"]["product_recomputes_available_trade_date"] is False
    assert event_meta["selected_event_rows"] >= 1

    trades = json.loads((run_dir / "trades.json").read_text(encoding="utf-8"))
    # Entry must be available_trade_date 2024-03-18, not announcement day and not +1 again.
    entry_dates = {
        t.get("entry_date") or t.get("open_date") or t.get("trade_date")
        for t in trades
        if (t.get("side") in {None, "buy", "BUY", "long"} or "entry" in t)
    }
    # fallback: inspect fills
    fills = json.loads((run_dir / "fills.json").read_text(encoding="utf-8"))
    buy_dates = sorted({f["trade_date"] for f in fills if str(f.get("side", "")).lower() in {"buy", "long"}})
    assert "2024-03-18" in buy_dates
    assert "2024-03-15" not in buy_dates
    # no double shift to 2024-03-19 for first entry
    # (next open after 03-18 would be 03-19)
    # first buy should be 03-18
    assert buy_dates[0] == "2024-03-18"


def test_same_day_announcement_cannot_trade(tmp_path: Path):
    db = _make_event_db(tmp_path)
    # Inject a bad same-day available row and ensure product rejects it.
    con = duckdb.connect(str(db))
    con.execute(
        """
        INSERT INTO earnings_forecast_event VALUES (
          '000001.SZ','earnings_forecast','s-same','2023-12-31','2024-03-18','2024-03-18',
          '2024-03-18 09:00:00','date','2024-03-18','预增',30,40,1,2,NULL,NULL,NULL,
          'test','src-same','rev-same','2024-03-18 10:00:00'
        )
        """
    )
    con.close()
    _, run_dir = execute_validated_task(
        _request(start_date="2024-03-18", end_date="2024-03-22", tickers=["000001.SZ"]),
        runs_dir=tmp_path / "runs",
        db_path=db,
        run_id="evt_same_day",
    )
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["event"]["rejected_same_day_or_not_after_announcement"] >= 1


def test_future_event_not_leaked(tmp_path: Path):
    db = _make_event_db(tmp_path)
    _, run_dir = execute_validated_task(
        _request(
            start_date="2024-03-18",
            end_date="2024-03-29",
            tickers=["000001.SZ", "600519.SH"],
        ),
        runs_dir=tmp_path / "runs",
        db_path=db,
        run_id="evt_no_future",
    )
    fills = json.loads((run_dir / "fills.json").read_text(encoding="utf-8"))
    assets = {f["asset_id"] for f in fills}
    assert "600519.SH" not in assets
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert "600519.SH" not in config["event"].get("event_tickers", [])


def test_empty_events_cash_only(tmp_path: Path):
    db = _make_event_db(tmp_path)
    _, run_dir = execute_validated_task(
        _request(
            start_date="2024-03-01",
            end_date="2024-03-08",
            tickers=["000001.SZ"],
        ),
        runs_dir=tmp_path / "runs",
        db_path=db,
        run_id="evt_empty",
    )
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    assert config["event"]["empty_events"] is True
    assert config["event"]["cash_only_result"] is True
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert abs(float(summary.get("total_return", summary.get("total_return_pct", 0))) ) < 1e-12 or float(summary.get("final_equity", 0)) == 1_000_000


def test_event_task_end_to_end_and_reload(tmp_path: Path):
    db = _make_event_db(tmp_path)
    service = BacktestProductService(
        task_store=BacktestTaskStore(tmp_path / "tasks"),
        runs_dir=tmp_path / "runs",
        db_path=db,
        execute_inline=True,
    )
    task = service.create_task(_request()).task
    assert task.status == "succeeded"
    assert task.run_id
    run_dir = tmp_path / "runs" / task.run_id
    assert (run_dir / "config.json").exists()
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "equity.json").exists()

    loader = BacktestRunsLoader(tmp_path / "runs")
    assert task.run_id in loader.list_run_ids()
    summary = loader.load_summary(task.run_id)
    assert summary is not None
    config = loader.load_config(task.run_id)
    assert config["event"]["event_type"] == "earnings_forecast"


def test_classic_and_cs_still_in_catalog():
    codes = {item.code for item in list_strategy_catalog(product_only=True)}
    assert "dual_sma_trend" in codes
    assert "cross_sectional_momentum_long_only" in codes
    assert "event_drift_basic" in codes
