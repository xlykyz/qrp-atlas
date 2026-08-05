from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import (
    STOCK_INFO,
    STOCK_INFO_HISTORICAL_IDENTITY,
    TICKER,
    TUSHARE_STOCK_BASIC,
    init_database,
)
from qrp_atlas.pipeline.contracts import ResultStatus
from qrp_atlas.pipeline.stock_basic_contracts import (
    STOCK_BASIC_FIELDS,
    STOCK_BASIC_QUERY_PARTITIONS,
    STOCK_BASIC_UPDATE,
)
from qrp_atlas.pipeline.testing import ContractTestHarness


TARGET = date(2026, 7, 29)


class FakeStockBasic:
    def __init__(self, frames: dict[tuple[str, str], pd.DataFrame]) -> None:
        self.frames = frames
        self.calls: list[tuple[str, str, str]] = []

    def stock_basic(self, *, exchange: str, list_status: str, fields: str) -> pd.DataFrame:
        self.calls.append((list_status, exchange, fields))
        return self.frames.get((list_status, exchange), pd.DataFrame()).copy()


def stock_frame(
    ts_code: str,
    *,
    list_status: str,
    exchange: str,
    name: str,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": [ts_code],
            "symbol": [ts_code.split(".")[0]],
            "name": [name],
            "area": ["深圳"],
            "industry": ["银行"],
            "fullname": [f"{name}股份有限公司"],
            "enname": ["Example Bank Co., Ltd."],
            "cnspell": ["LPYH"],
            "market": ["主板"],
            "exchange": [exchange],
            "curr_type": ["CNY"],
            "list_status": [list_status],
            "list_date": ["19910403"],
            "delist_date": [None],
            "is_hs": ["H"],
            "act_name": ["实际控制人"],
            "act_ent_type": ["国有企业"],
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


def test_stock_basic_writes_full_current_snapshot_and_compatibility_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeStockBasic(
        {
            ("L", "SSE"): stock_frame(
                "600000.SH", list_status="L", exchange="SSE", name="浦发银行"
            ),
            ("D", "SZSE"): stock_frame(
                "000001.SZ", list_status="D", exchange="SZSE", name="退市样本"
            ),
        }
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.api_requests == len(STOCK_BASIC_QUERY_PARTITIONS)
    assert result.metrics.rows_read == 2
    assert result.metrics.rows_written == 2
    assert len(client.calls) == len(STOCK_BASIC_QUERY_PARTITIONS)
    assert all(call[2] == STOCK_BASIC_FIELDS for call in client.calls)

    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        columns = set(connection.execute("DESCRIBE stock_info").fetchdf()["column_name"])
        assert set(STOCK_INFO.column_names()) <= columns
        rows = connection.execute(
            """
            SELECT ts_code, symbol, name, list_status, ticker, is_active
            FROM stock_info
            ORDER BY ts_code
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        ("000001.SZ", "000001", "退市样本", "D", "000001.SZ", False),
        ("600000.SH", "600000", "浦发银行", "L", "600000.SH", True),
    ]


def test_stock_basic_replaces_stale_current_rows_on_repeat_run(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeStockBasic(
        {
            ("L", "SSE"): stock_frame(
                "600000.SH", list_status="L", exchange="SSE", name="浦发银行"
            ),
            ("L", "SZSE"): stock_frame(
                "000001.SZ", list_status="L", exchange="SZSE", name="平安银行"
            ),
        }
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    first = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)
    assert first.status is ResultStatus.SUCCESS

    client.frames = {
        ("L", "SSE"): stock_frame(
            "600000.SH", list_status="L", exchange="SSE", name="浦发银行更新"
        ),
    }
    second = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)
    assert second.status is ResultStatus.SUCCESS

    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        rows = connection.execute("SELECT ticker, name FROM stock_info").fetchall()
    finally:
        connection.close()
    assert rows == [("600000.SH", "浦发银行更新")]


def test_stock_basic_upgrades_legacy_eight_column_table_in_write_transaction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    item.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE stock_info (
                ticker VARCHAR PRIMARY KEY,
                name VARCHAR,
                exchange VARCHAR,
                market VARCHAR,
                list_date DATE,
                delist_date DATE,
                is_active BOOLEAN,
                updated_at TIMESTAMP
            )
            """
        )
        connection.execute(
            "INSERT INTO stock_info VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ["000001.SZ", "旧名称", "SZSE", "主板", date(1991, 4, 3), None, True, None],
        )
    finally:
        connection.close()

    client = FakeStockBasic(
        {
            ("L", "SZSE"): stock_frame(
                "000001.SZ", list_status="L", exchange="SZSE", name="平安银行"
            ),
        }
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert result.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        columns = set(connection.execute("DESCRIBE stock_info").fetchdf()["column_name"])
        row = connection.execute(
            "SELECT ts_code, ticker, name, list_status, is_active FROM stock_info"
        ).fetchone()
    finally:
        connection.close()
    assert set(STOCK_INFO.column_names()) <= columns
    assert row == ("000001.SZ", "000001.SZ", "平安银行", "L", True)


def test_stock_basic_rejects_invalid_provider_data_without_replacing_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            "INSERT INTO stock_info (ticker, name, is_active) VALUES (?, ?, ?)",
            ["600000.SH", "旧快照", True],
        )
    finally:
        connection.close()

    invalid = stock_frame(
        "NOT-A-STOCK", list_status="L", exchange="SSE", name="坏数据"
    )
    client = FakeStockBasic({("L", "SSE"): invalid})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert result.status is ResultStatus.FAILED
    assert any(item.code == "STOCK_BASIC_IDENTITY_UNSUPPORTED" for item in result.diagnostics)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT ticker, name FROM stock_info").fetchall() == [
            ("600000.SH", "旧快照")
        ]
    finally:
        connection.close()


def test_stock_basic_preserves_historical_special_identity_and_nullable_market(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    historical = stock_frame(
        "T600018.SH",
        list_status="D",
        exchange="SSE",
        name="上港集箱(退)",
    )
    historical["symbol"] = ["T600018"]
    historical["market"] = [None]
    client = FakeStockBasic(
        {
            ("L", "SSE"): stock_frame(
                "600000.SH",
                list_status="L",
                exchange="SSE",
                name="浦发银行",
            ),
            ("D", "SSE"): historical,
        }
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_read == 2
    assert result.metrics.rows_written == 2
    standard_output = next(item for item in result.outputs if item.output_id == STOCK_INFO.name)
    historical_output = next(
        item
        for item in result.outputs
        if item.output_id == STOCK_INFO_HISTORICAL_IDENTITY.name
    )
    assert standard_output.rows_written == 1
    assert historical_output.rows_written == 1
    assert historical_output.detail["provider_identity_samples"] == ["T600018.SH"]

    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        standard_rows = connection.execute(
            "SELECT ts_code, ticker, market FROM stock_info ORDER BY ticker"
        ).fetchall()
        historical_rows = connection.execute(
            """
            SELECT provider, ts_code, name, exchange, list_status, market,
                   standard_ticker, identity_type, isolation_reason, snapshot_date
            FROM stock_info_historical_identity
            """
        ).fetchall()
    finally:
        connection.close()

    assert standard_rows == [("600000.SH", "600000.SH", "主板")]
    assert historical_rows == [
        (
            "tushare",
            "T600018.SH",
            "上港集箱(退)",
            "SSE",
            "D",
            None,
            None,
            "provider_historical_id",
            "non_standard_provider_identity",
            TARGET,
        )
    ]
    assert "600018.SH" not in {row[1] for row in historical_rows}


@pytest.mark.parametrize("missing_field", ["ts_code", "exchange", "list_status"])
def test_stock_basic_fails_closed_when_identity_field_is_missing(
    tmp_path: Path,
    monkeypatch,
    missing_field: str,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    invalid = stock_frame(
        "600000.SH",
        list_status="L",
        exchange="SSE",
        name="缺失身份字段",
    )
    invalid[missing_field] = [None]
    client = FakeStockBasic({("L", "SSE"): invalid})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert result.status is ResultStatus.FAILED
    assert any(item.code == "STOCK_BASIC_IDENTITY_MISSING" for item in result.diagnostics)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM stock_info").fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM stock_info_historical_identity"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_stock_basic_rejects_conflicting_duplicate_identity_without_writes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            "INSERT INTO stock_info (ticker, name, is_active) VALUES (?, ?, ?)",
            ["600000.SH", "旧快照", True],
        )
    finally:
        connection.close()
    first = stock_frame(
        "600000.SH",
        list_status="L",
        exchange="SSE",
        name="浦发银行",
    )
    second = stock_frame(
        "600000.SH",
        list_status="D",
        exchange="SSE",
        name="冲突历史名称",
    )
    client = FakeStockBasic({("L", "SSE"): first, ("D", "SSE"): second})
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert result.status is ResultStatus.FAILED
    assert any(item.code == "STOCK_BASIC_CONFLICTING_DUPLICATE" for item in result.diagnostics)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT ticker, name FROM stock_info").fetchall() == [
            ("600000.SH", "旧快照")
        ]
    finally:
        connection.close()


def test_stock_basic_special_identity_rerun_is_idempotent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    historical = stock_frame(
        "T600018.SH",
        list_status="D",
        exchange="SSE",
        name="上港集箱(退)",
    )
    historical["market"] = [None]
    client = FakeStockBasic(
        {
            ("L", "SSE"): stock_frame(
                "600000.SH",
                list_status="L",
                exchange="SSE",
                name="浦发银行",
            ),
            ("D", "SSE"): historical,
        }
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    first = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)
    second = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert first.status is ResultStatus.SUCCESS
    assert second.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM stock_info").fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM stock_info_historical_identity"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT provider, ts_code, market, standard_ticker FROM stock_info_historical_identity"
        ).fetchall() == [("tushare", "T600018.SH", None, None)]
    finally:
        connection.close()


def test_stock_basic_transaction_failure_preserves_both_previous_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            "INSERT INTO stock_info (ticker, name, is_active) VALUES (?, ?, ?)",
            ["600000.SH", "旧标准快照", True],
        )
        connection.execute(f"DROP TABLE {STOCK_INFO_HISTORICAL_IDENTITY.name}")
        definitions = [f"{column.name} {column.dtype}" for column in STOCK_INFO_HISTORICAL_IDENTITY.columns]
        definitions.append("CHECK (ts_code <> 'T600018.SH')")
        definitions.append(
            f"PRIMARY KEY ({', '.join(STOCK_INFO_HISTORICAL_IDENTITY.primary_key)})"
        )
        connection.execute(
            f"CREATE TABLE {STOCK_INFO_HISTORICAL_IDENTITY.name} ({', '.join(definitions)})"
        )
        connection.execute(
            f"INSERT INTO {STOCK_INFO_HISTORICAL_IDENTITY.name} (provider, ts_code) VALUES (?, ?)",
            ["tushare", "T999999.SH"],
        )
    finally:
        connection.close()

    historical = stock_frame(
        "T600018.SH",
        list_status="D",
        exchange="SSE",
        name="上港集箱(退)",
    )
    historical["market"] = [None]
    client = FakeStockBasic(
        {
            ("L", "SSE"): stock_frame(
                "600001.SH",
                list_status="L",
                exchange="SSE",
                name="新标准快照",
            ),
            ("D", "SSE"): historical,
        }
    )
    monkeypatch.setattr(
        "qrp_atlas.pipeline.stock_basic_contracts.get_tushare_pro",
        lambda **_kwargs: client,
    )

    result = ContractTestHarness(STOCK_BASIC_UPDATE, item).run(trade_date=TARGET)

    assert result.status is ResultStatus.FAILED
    assert any(item.code == "STOCK_BASIC_WRITE_FAILED" for item in result.diagnostics)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT ticker, name FROM stock_info").fetchall() == [
            ("600000.SH", "旧标准快照")
        ]
        assert connection.execute(
            f"SELECT provider, ts_code FROM {STOCK_INFO_HISTORICAL_IDENTITY.name}"
        ).fetchall() == [("tushare", "T999999.SH")]
    finally:
        connection.close()


def test_stock_basic_contract_declares_provider_fields_and_current_snapshot_semantics() -> None:
    assert STOCK_BASIC_UPDATE.pipeline_id == "stock_basic_update"
    assert STOCK_BASIC_UPDATE.manual_execution_allowed is True
    assert STOCK_BASIC_UPDATE.outputs[0].object_name == "stock_info"
    assert STOCK_BASIC_UPDATE.outputs[0].write_mode.value == "FULL_REBUILD"
    assert STOCK_BASIC_UPDATE.inputs[0].required_fields == tuple(TUSHARE_STOCK_BASIC.values())
    assert set(STOCK_BASIC_UPDATE.outputs[0].unique_key) == {TICKER}
    assert STOCK_BASIC_UPDATE.outputs[1].object_name == STOCK_INFO_HISTORICAL_IDENTITY.name
    assert STOCK_BASIC_UPDATE.outputs[1].allow_empty is True
    assert STOCK_BASIC_UPDATE.outputs[1].unique_key == ("provider", "ts_code")
