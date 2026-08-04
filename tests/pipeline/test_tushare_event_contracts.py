from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import (
    ContractError,
    ResultStatus,
    parse_parameter_overrides,
)
from qrp_atlas.pipeline.limit_step_contracts import LIMIT_STEP_INGEST
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.stk_high_shock_contracts import STK_HIGH_SHOCK_INGEST
from qrp_atlas.pipeline.testing import ContractTestHarness
from qrp_atlas.pipeline.ths_daily_contracts import THS_DAILY_INGEST

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
    def __init__(self, responses: dict[tuple[str, str], object] | None = None) -> None:
        self.limit_step_calls: list[dict[str, str]] = []
        self.ths_daily_calls: list[dict[str, str]] = []
        self.stk_high_shock_calls: list[dict[str, str]] = []
        self.responses = responses or {}

    def _override(self, endpoint: str, kwargs: dict[str, str], default: pd.DataFrame | None):
        key = (endpoint, kwargs["trade_date"])
        if key not in self.responses:
            return default
        response = self.responses[key]
        if isinstance(response, pd.DataFrame):
            return response.copy()
        return response

    def limit_step(self, **kwargs: str) -> pd.DataFrame:
        self.limit_step_calls.append(kwargs)
        default = pd.DataFrame(
            {
                "ts_code": ["000833.SZ"],
                "name": ["粤桂股份"],
                "trade_date": [kwargs["trade_date"]],
                "nums": [11],
            }
        )
        return self._override("limit_step", kwargs, default)

    def ths_daily(self, **kwargs: str) -> pd.DataFrame:
        self.ths_daily_calls.append(kwargs)
        value = int(kwargs["trade_date"][-2:])
        default = pd.DataFrame(
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
        return self._override("ths_daily", kwargs, default)

    def stk_high_shock(self, **kwargs: str) -> pd.DataFrame:
        self.stk_high_shock_calls.append(kwargs)
        default = pd.DataFrame(
            {
                "ts_code": ["301373.SZ"],
                "trade_date": [kwargs["trade_date"]],
                "name": ["凌玮科技"],
                "trade_market": ["创业板"],
                "reason": ["连续10个交易日内收盘价格涨幅偏离值累计达100%"],
                "period": ["2026-03-10-2026-03-24"],
            }
        )
        return self._override("stk_high_shock", kwargs, default)


def _date_key(value: date) -> str:
    return value.strftime("%Y%m%d")


def _seed_limit_step(settings: AppSettings, rows: list[tuple[date, str, str, int]]) -> None:
    connection = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        connection.executemany(
            "INSERT INTO limit_step (trade_date, ticker, name, consecutive_boards) VALUES (?, ?, ?, ?)",
            rows,
        )
    finally:
        connection.close()


def _read_limit_step(settings: AppSettings) -> list[tuple[object, ...]]:
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        return connection.execute(
            "SELECT trade_date, ticker, name, consecutive_boards FROM limit_step ORDER BY trade_date, ticker"
        ).fetchall()
    finally:
        connection.close()


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
    assert client.limit_step_calls == [{"trade_date": _date_key(TARGET)}] * 2
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
    assert all(set(call) == {"trade_date"} for call in client.ths_daily_calls)
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
    assert client.stk_high_shock_calls == [{"trade_date": _date_key(TARGET)}]
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


def test_provider_none_fails_before_deleting_existing_snapshot(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    _seed_limit_step(settings, [(TARGET, "000833.SZ", "旧记录", 11)])
    client = _FakeTushare({("limit_step", _date_key(TARGET)): None})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.limit_step_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(LIMIT_STEP_INGEST, settings).run(trade_date=TARGET)

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "LIMIT_STEP_API_PARTIAL"
    assert "returned None instead of a DataFrame" in result.diagnostics[0].detail[
        "contract_error_detail"
    ]
    assert _read_limit_step(settings) == [(TARGET, "000833.SZ", "旧记录", 11)]


def test_empty_dataframe_with_existing_snapshot_fails_closed(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    _seed_limit_step(settings, [(TARGET, "000833.SZ", "旧记录", 11)])
    client = _FakeTushare({("limit_step", _date_key(TARGET)): pd.DataFrame()})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.limit_step_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(LIMIT_STEP_INGEST, settings).run(trade_date=TARGET)

    assert result.status is ResultStatus.FAILED
    assert result.diagnostics[0].code == "LIMIT_STEP_API_PARTIAL"
    assert _read_limit_step(settings) == [(TARGET, "000833.SZ", "旧记录", 11)]


def test_empty_dataframe_with_empty_target_succeeds_without_delete(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    client = _FakeTushare({("limit_step", _date_key(TARGET)): pd.DataFrame()})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.limit_step_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(LIMIT_STEP_INGEST, settings).run(trade_date=TARGET)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_written == 0
    assert _read_limit_step(settings) == []


def test_multiday_empty_response_conflict_preserves_every_existing_date(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    second_date = TARGET.replace(day=30)
    old_rows = [
        (TARGET, "000001.SZ", "第一天旧记录", 3),
        (second_date, "000002.SZ", "第二天旧记录", 4),
    ]
    _seed_limit_step(settings, old_rows)
    client = _FakeTushare({("limit_step", _date_key(second_date)): pd.DataFrame()})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.limit_step_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(LIMIT_STEP_INGEST, settings).run(
        parameter_overrides={
            "start_date": TARGET.isoformat(),
            "end_date": second_date.isoformat(),
        }
    )

    assert result.status is ResultStatus.FAILED
    assert _read_limit_step(settings) == old_rows


def test_snapshot_contracts_reject_local_provider_filters() -> None:
    assert [item.name for item in LIMIT_STEP_INGEST.parameters] == ["start_date", "end_date"]
    assert [item.name for item in THS_DAILY_INGEST.parameters] == ["start_date", "end_date"]
    assert [item.name for item in STK_HIGH_SHOCK_INGEST.parameters] == ["start_date", "end_date"]


@pytest.mark.parametrize(
    ("contract", "parameter"),
    (
        (LIMIT_STEP_INGEST, "ts_code"),
        (LIMIT_STEP_INGEST, "nums"),
        (THS_DAILY_INGEST, "ts_code"),
        (STK_HIGH_SHOCK_INGEST, "ts_code"),
    ),
)
def test_removed_local_provider_filters_are_rejected(contract, parameter: str) -> None:
    with pytest.raises(ContractError, match="UNKNOWN_PARAMETER"):
        parse_parameter_overrides(contract, {parameter: "local-filter"})
