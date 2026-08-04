from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.limit_step_contracts import LIMIT_STEP_INGEST
from qrp_atlas.pipeline.stk_high_shock_contracts import STK_HIGH_SHOCK_INGEST
from qrp_atlas.pipeline.testing import ContractTestHarness
from qrp_atlas.pipeline.ths_daily_contracts import THS_DAILY_INGEST
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.contracts import ResultStatus


TARGET = date(2026, 7, 29)


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
            "TUSHARE_TOKEN": "test-token-only",
            "QRP_RUNTIME_ENV": "test",
        },
        project_root=tmp_path / "repo",
    )


def _initialise_database(settings: AppSettings) -> None:
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        init_database(connection)
    finally:
        connection.close()


class _FakeTushare:
    def __init__(self) -> None:
        self.limit_step_calls: list[dict[str, str]] = []
        self.ths_daily_calls: list[dict[str, str]] = []
        self.stk_high_shock_calls: list[dict[str, str]] = []

    def limit_step(self, **kwargs: str) -> pd.DataFrame:
        self.limit_step_calls.append(kwargs)
        return pd.DataFrame(
            {
                "ts_code": ["000833.SZ"],
                "name": ["粤桂股份"],
                "trade_date": [kwargs["trade_date"]],
                "nums": [11],
            }
        )

    def ths_daily(self, **kwargs: str) -> pd.DataFrame:
        self.ths_daily_calls.append(kwargs)
        value = int(kwargs["trade_date"][-2:])
        return pd.DataFrame(
            {
                "ts_code": ["865001.TI"],
                "trade_date": [kwargs["trade_date"]],
                "close": [1664.753 + value],
                "open": [1660.706],
                "high": [1671.229],
                "low": [1649.420],
                "pre_close": [1655.407],
                "avg_price": [1662.0],
                "change": [9.346],
                "pct_change": [0.5646],
                "vol": [13224.26],
                "turnover_rate": [0.2],
                "total_mv": [1000000.0],
                "float_mv": [800000.0],
            }
        )

    def stk_high_shock(self, **kwargs: str) -> pd.DataFrame:
        self.stk_high_shock_calls.append(kwargs)
        return pd.DataFrame(
            {
                "ts_code": ["301373.SZ"],
                "trade_date": [kwargs["trade_date"]],
                "name": ["凌玮科技"],
                "trade_market": ["创业板"],
                "reason": ["连续10个交易日内收盘价格涨幅偏离值累计达100%"],
                "period": ["2026-03-10-2026-03-24"],
            }
        )


def test_new_contracts_are_registered_and_pass_formal_validation() -> None:
    contracts = (LIMIT_STEP_INGEST, THS_DAILY_INGEST, STK_HIGH_SHOCK_INGEST)
    validate_contracts(contracts)
    registered = {item.pipeline_id for item in default_registry().all()}
    assert {item.pipeline_id for item in contracts} <= registered
    assert all(item.resource_locks == ("quant_db_writer",) for item in contracts)
    assert all(item.outputs[0].physical_resource == "quant_db" for item in contracts)


def test_limit_step_replaces_canonical_target_rows_idempotently(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakeTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.limit_step_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    first = ContractTestHarness(LIMIT_STEP_INGEST, settings).run(trade_date=TARGET)
    second = ContractTestHarness(LIMIT_STEP_INGEST, settings).run(trade_date=TARGET)

    assert first.status is second.status is ResultStatus.SUCCESS
    assert len(client.limit_step_calls) == 2
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT ticker, trade_date, consecutive_boards FROM limit_step"
        ).fetchall() == [("000833.SZ", TARGET, 11)]
    finally:
        connection.close()


def test_ths_daily_range_requests_each_date_and_normalizes_market_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakeTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.ths_daily_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(THS_DAILY_INGEST, settings).run(
        parameter_overrides={
            "start_date": "2026-07-29",
            "end_date": "2026-07-30",
        }
    )

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.api_requests == 2
    assert [call["trade_date"] for call in client.ths_daily_calls] == ["20260729", "20260730"]
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT index_code, trade_date, volume, float_mv FROM ths_daily ORDER BY trade_date"
        ).fetchall() == [
            ("865001.TI", date(2026, 7, 29), 13224.26, 800000.0),
            ("865001.TI", date(2026, 7, 30), 13224.26, 800000.0),
        ]
    finally:
        connection.close()


def test_stk_high_shock_preserves_distinct_reason_period_events(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakeTushare()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stk_high_shock_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STK_HIGH_SHOCK_INGEST, settings).run(trade_date=TARGET)

    assert result.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT ticker, trade_market, reason, period FROM stk_high_shock"
        ).fetchone() == (
            "301373.SZ",
            "创业板",
            "连续10个交易日内收盘价格涨幅偏离值累计达100%",
            "2026-03-10-2026-03-24",
        )
    finally:
        connection.close()
