"""Offline acceptance tests for the formal ETF data Contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import ETF_ADJ_FACTOR, ETF_DAILY, init_database
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline.contracts import PipelineContract, ResultStatus
from qrp_atlas.pipeline.etf_adj_factor_contracts import ETF_ADJ_FACTOR_UPDATE
from qrp_atlas.pipeline.etf_daily_contracts import ETF_DAILY_UPDATE
from qrp_atlas.pipeline.etf_support import fetch_fund_adj_pages
from qrp_atlas.pipeline.testing import ContractTestHarness


TARGET = date(2026, 7, 29)
PREVIOUS = date(2026, 7, 28)


class FakeTushare:
    def __init__(self) -> None:
        self.daily_frame = pd.DataFrame(
            {
                "ts_code": ["510330.SH", "159915.SZ"],
                "trade_date": ["20260729", "20260729"],
                "open": [4.008, 2.5],
                "high": [4.024, 2.6],
                "low": [3.996, 2.4],
                "close": [4.017, 2.55],
                "pre_close": [4.0, 2.45],
                "change": [0.017, 0.1],
                "pct_chg": [0.425, 4.0816],
                "vol": [382896.0, 1000.0],
                "amount": [153574.446, 2500.0],
            }
        )
        self.adj_frame = pd.DataFrame(
            {
                "ts_code": ["510330.SH", "159915.SZ"],
                "trade_date": ["20260729", "20260729"],
                "adj_factor": [1.0, 1.25],
            }
        )
        self.adj_calls: list[tuple[str, str]] = []

    def fund_daily(self, **_kwargs) -> pd.DataFrame:
        return self.daily_frame.copy()

    def fund_adj(self, *, offset: str, limit: str, **_kwargs) -> pd.DataFrame:
        self.adj_calls.append((offset, limit))
        start = int(offset)
        return self.adj_frame.iloc[start : start + int(limit)].copy()


def settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
            "TUSHARE_TOKEN": "test-token-only",
            "QRP_RUNTIME_ENV": "test",
        },
        project_root=tmp_path / "repo",
    )


def initialise_database(item: AppSettings) -> None:
    item.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        init_database(connection)
        connection.executemany(
            "INSERT INTO trading_calendar (trade_date, is_open, year, month, quarter) VALUES (?, ?, ?, ?, ?)",
            [
                (PREVIOUS, True, PREVIOUS.year, PREVIOUS.month, 3),
                (TARGET, True, TARGET.year, TARGET.month, 3),
            ],
        )
    finally:
        connection.close()


def run(contract: PipelineContract, item: AppSettings, *, dependencies=()):
    return ContractTestHarness(
        contract,
        item,
        scheduled_for=datetime(2026, 7, 29, 8, 30, tzinfo=UTC),
        dependency_contracts=tuple(dependencies),
    ).run(trade_date=TARGET)


def test_etf_tables_are_part_of_the_main_database_schema(tmp_path: Path) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert {ETF_DAILY.name, ETF_ADJ_FACTOR.name} <= tables
        assert ETF_DAILY.primary_key == ("trade_date", "ticker")
        assert ETF_ADJ_FACTOR.primary_key == ("ticker", "trade_date")
    finally:
        connection.close()


def test_etf_daily_normalizes_units_and_replaces_target_idempotently(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.etf_daily_contracts.get_tushare_pro", lambda **_kwargs: client)

    first = run(ETF_DAILY_UPDATE, item)
    second = run(ETF_DAILY_UPDATE, item)

    assert first.status is second.status is ResultStatus.SUCCESS
    assert first.metrics.rows_written == second.metrics.rows_written == 2
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT volume, amount FROM etf_daily WHERE trade_date = ? AND ticker = ?",
            [TARGET, "510330.SH"],
        ).fetchone() == (38289600, pytest.approx(153574446.0))
        assert connection.execute("SELECT COUNT(*) FROM etf_daily WHERE trade_date = ?", [TARGET]).fetchone()[0] == 2
    finally:
        connection.close()


def test_etf_adj_factor_requires_daily_coverage_and_writes_full_factors(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.etf_daily_contracts.get_tushare_pro", lambda **_kwargs: client)
    monkeypatch.setattr("qrp_atlas.pipeline.etf_adj_factor_contracts.get_tushare_pro", lambda **_kwargs: client)

    daily = run(ETF_DAILY_UPDATE, item)
    factors = run(ETF_ADJ_FACTOR_UPDATE, item, dependencies=(ETF_DAILY_UPDATE,))

    assert daily.status is factors.status is ResultStatus.SUCCESS
    assert factors.metrics.api_requests == 1
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT ticker, adj_factor FROM etf_adj_factor WHERE trade_date = ? ORDER BY ticker",
            [TARGET],
        ).fetchall() == [("159915.SZ", 1.25), ("510330.SH", 1.0)]
    finally:
        connection.close()


def test_etf_adj_factor_follows_the_2000_row_pagination_boundary() -> None:
    rows = 2001
    frame = pd.DataFrame(
        {
            "ts_code": [f"{500000 + index:06d}.SH" for index in range(rows)],
            "trade_date": ["20260729"] * rows,
            "adj_factor": [1.0] * rows,
        }
    )

    class PagedClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def fund_adj(self, *, offset: str, limit: str, **_kwargs) -> pd.DataFrame:
            self.calls.append((offset, limit))
            start = int(offset)
            return frame.iloc[start : start + int(limit)].copy()

    client = PagedClient()
    actual, requests = fetch_fund_adj_pages(client, TARGET, ExecutionControl())

    assert len(actual) == rows
    assert requests == 2
    assert client.calls == [("0", "2000"), ("2000", "2000")]


def test_etf_adj_factor_rejects_missing_code_before_replacing_target(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.etf_daily_contracts.get_tushare_pro", lambda **_kwargs: client)
    monkeypatch.setattr("qrp_atlas.pipeline.etf_adj_factor_contracts.get_tushare_pro", lambda **_kwargs: client)
    run(ETF_DAILY_UPDATE, item)

    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            "INSERT INTO etf_adj_factor (ticker, trade_date, adj_factor) VALUES (?, ?, ?)",
            ["510330.SH", TARGET, 9.9],
        )
    finally:
        connection.close()
    client.adj_frame = client.adj_frame.iloc[:1].copy()

    result = run(ETF_ADJ_FACTOR_UPDATE, item, dependencies=(ETF_DAILY_UPDATE,))

    assert result.status is ResultStatus.FAILED
    assert {item.code for item in result.diagnostics} & {"ETF_ADJ_FACTOR_API_PARTIAL"}
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT adj_factor FROM etf_adj_factor WHERE ticker = ? AND trade_date = ?",
            ["510330.SH", TARGET],
        ).fetchone()[0] == 9.9
    finally:
        connection.close()
