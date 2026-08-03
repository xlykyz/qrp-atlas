from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.pipeline.contracts import ResultStatus
from qrp_atlas.pipeline.index_basic_contracts import INDEX_BASIC_UPDATE
from qrp_atlas.pipeline.testing import ContractTestHarness


TARGET = date(2026, 7, 29)


class FakeIndexBasic:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.frames = {
            "SSE": index_basic_frame("000001.SH", "SSE", "上证综指"),
            "SZSE": index_basic_frame("399001.SZ", "SZSE", "深证成指"),
        }

    def index_basic(self, *, market: str) -> pd.DataFrame:
        self.calls.append(market)
        return self.frames.get(market, pd.DataFrame()).copy()


def index_basic_frame(index_code: str, market: str, name: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": [index_code],
            "name": [name],
            "fullname": [f"{name}全收益指数"],
            "market": [market],
            "publisher": [market],
            "index_type": ["综合指数"],
            "category": ["综合指数"],
            "base_date": ["19901219"],
            "base_point": [100.0],
            "list_date": ["19910715"],
            "weight_rule": ["市值加权"],
            "desc": [f"{name}描述"],
            "exp_date": [None],
        }
    )


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
    finally:
        connection.close()


def diagnostics(result) -> set[str]:
    return {diagnostic.code for diagnostic in result.diagnostics}


def test_index_basic_syncs_requested_markets_and_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeIndexBasic()
    monkeypatch.setattr(
        "qrp_atlas.pipeline.index_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(INDEX_BASIC_UPDATE, item).run(
        trade_date=TARGET,
        parameter_overrides={"markets": "SSE,SZSE"},
    )

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.api_requests == 2
    assert client.calls == ["SSE", "SZSE"]
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM index_basic").fetchone()[0] == 2
        assert connection.execute(
            "SELECT index_code, base_date, list_date FROM index_basic ORDER BY index_code"
        ).fetchall() == [
            ("000001.SH", date(1990, 12, 19), date(1991, 7, 15)),
            ("399001.SZ", date(1990, 12, 19), date(1991, 7, 15)),
        ]
    finally:
        connection.close()

    repeated = ContractTestHarness(INDEX_BASIC_UPDATE, item).run(
        trade_date=TARGET,
        parameter_overrides={"markets": "SSE,SZSE"},
    )
    assert repeated.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM index_basic").fetchone()[0] == 2
    finally:
        connection.close()


def test_index_basic_rejects_conflicting_duplicate_without_writing(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeIndexBasic()
    client.frames["SSE"] = pd.concat(
        [
            client.frames["SSE"],
            client.frames["SSE"].assign(name="冲突定义"),
        ],
        ignore_index=True,
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.index_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(INDEX_BASIC_UPDATE, item).run(
        trade_date=TARGET,
        parameter_overrides={"markets": "SSE"},
    )

    assert result.status is ResultStatus.FAILED
    assert "INDEX_BASIC_CONFLICTING_DUPLICATE" in diagnostics(result)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM index_basic").fetchone()[0] == 0
    finally:
        connection.close()
