"""Offline acceptance tests for the six formal daily market-data Pipelines."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import CheckResult, CompletionContract, PipelineInvocation, ResultStatus
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.market_data_contracts import (
    ADJ_FACTOR_DAILY,
    DAILY_BASIC_UPDATE,
    INDEX_DAILY_UPDATE,
    MARKET_DAILY_UPDATE,
    MARKET_DATA_CONTRACTS,
    MARKET_TARGET_DATE_POLICY,
    SUSPEND_D_INGEST,
    ZT_DT_POOL_DAILY,
)
from qrp_atlas.pipeline.cninfo_contracts import CNINFO_CONTRACTS
from qrp_atlas.pipeline.irm_qa_contracts import IRM_CONTRACTS
from qrp_atlas.pipeline.membership_contracts import MEMBERSHIP_CONTRACTS
from qrp_atlas.pipeline.pit_fundamentals_contracts import PIT_FUNDAMENTALS_CONTRACTS
from qrp_atlas.pipeline.research_report_contracts import RESEARCH_REPORT_CONTRACTS
from qrp_atlas.pipeline.research_industry_contracts import RESEARCH_INDUSTRY_CONTRACTS
from qrp_atlas.pipeline.job_adapter import ContractDeploymentSelection, contract_runtime_definition
from qrp_atlas.jobs_cli import main as pipeline_cli
from qrp_atlas.orchestration.store import JobRuntimeStore
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.testing import ContractTestHarness, assert_contract_result_matches_context


TARGET = date(2026, 7, 29)
PREVIOUS = date(2026, 7, 28)


class FakeTushare:
    def __init__(self) -> None:
        self.daily_frame = market_frame()
        self.daily_basic_frame = daily_basic_frame()
        self.adj_factor_frame = adj_factor_frame()
        self.suspend_frame = suspend_frame()

    def daily(self, **_kwargs) -> pd.DataFrame:
        return self.daily_frame.copy()

    def daily_basic(self, **_kwargs) -> pd.DataFrame:
        return self.daily_basic_frame.copy()

    def adj_factor(self, **_kwargs) -> pd.DataFrame:
        return self.adj_factor_frame.copy()

    def suspend_d(self, **_kwargs) -> pd.DataFrame:
        return self.suspend_frame.copy()


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


def initialise_database(item: AppSettings, *, include_target: bool = True) -> None:
    item.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        init_database(connection)
        rows = [(PREVIOUS, True), (TARGET, True), (date(2026, 7, 30), True), (date(2026, 8, 1), False)]
        if not include_target:
            rows = [(PREVIOUS, True), (date(2026, 7, 30), True)]
        connection.executemany(
            "INSERT INTO trading_calendar (trade_date, is_open, year, month, quarter) VALUES (?, ?, ?, ?, ?)",
            [(value, is_open, value.year, value.month, (value.month - 1) // 3 + 1) for value, is_open in rows],
        )
    finally:
        connection.close()


def market_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "trade_date": ["20260729", "20260729"],
            "open": [10.0, 8.0],
            "high": [11.0, 9.0],
            "low": [9.9, 7.9],
            "close": [10.5, 8.5],
            "pre_close": [10.0, 8.0],
            "pct_chg": [5.0, 6.25],
            "vol": [1000, 2000],
            "amount": [10000, 16000],
        }
    )


def daily_basic_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "trade_date": ["20260729", "20260729"],
            "close": [10.5, 8.5],
            "pe": [12.0, 8.0],
            "total_share": [100.0, 200.0],
        }
    )


def adj_factor_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "trade_date": ["20260729", "20260729"],
            "adj_factor": [1.1, 2.2],
        }
    )


def suspend_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20260729"],
            "suspend_timing": ["09:30"],
            "suspend_type": ["S"],
        }
    )


def index_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-07-28", "2026-07-29"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [1000, 1200],
        }
    )


def seed_market_target(item: AppSettings) -> None:
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.executemany(
            """
            INSERT INTO daily_market_snapshot (trade_date, ticker, close)
            VALUES (?, ?, ?)
            """,
            [(TARGET, "000001.SZ", 10.5), (TARGET, "600000.SH", 8.5)],
        )
    finally:
        connection.close()


def run(contract, item: AppSettings, *, dependencies=()):
    available = tuple(dependencies)
    if "market_daily_update" in contract.dependencies and MARKET_DAILY_UPDATE not in available:
        available = (*available, MARKET_DAILY_UPDATE)
    return ContractTestHarness(contract, item, dependency_contracts=available).run(trade_date=TARGET)


def run_suspend_with_control(item: AppSettings, control: ExecutionControl):
    return execute_pipeline_contract(
        SUSPEND_D_INGEST,
        PipelineInvocation(
            run_id="suspend-d-execution-control-test",
            pipeline_id=SUSPEND_D_INGEST.pipeline_id,
            scheduled_for=datetime(2026, 7, 29, 8, 30, tzinfo=UTC),
            attempt=1,
            settings=item,
            trade_date_override=TARGET,
            audit_context={"test": "true"},
            execution_control=control,
        ),
    )


def diagnostics(result) -> set[str]:
    return {diagnostic.code for diagnostic in result.diagnostics}


class _EastmoneyResponse:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def _eastmoney_payload(*, records: list[dict], total: int, response_date: date = TARGET) -> dict:
    return {
        "rc": 0,
        "data": {
            "date": response_date.strftime("%Y%m%d"),
            "total": total,
            "pool": records,
        },
    }


def _eastmoney_record(value: int) -> dict:
    return {"c": f"{value:06d}", "n": f"Sample {value}", "p": 10000, "zdp": 10.0}


def _eastmoney_urlopen(responses: dict[tuple[str, int], dict | Exception]):
    calls: list[tuple[str, int]] = []

    def open_url(request, *, timeout: int):
        assert timeout == 15
        parsed = urlparse(request.full_url)
        key = (parsed.path.rsplit("/", 1)[-1], int(parse_qs(parsed.query)["Pageindex"][0]))
        calls.append(key)
        outcome = responses[key]
        if isinstance(outcome, Exception):
            raise outcome
        return _EastmoneyResponse(outcome)

    return calls, open_url


def test_market_data_contracts_are_registered_with_one_quant_writer_lock() -> None:
    validate_contracts(MARKET_DATA_CONTRACTS)
    registered = default_registry().all()
    validate_contracts(registered)
    assert {contract.pipeline_id for contract in MARKET_DATA_CONTRACTS} == {
        "market_daily_update",
        "adj_factor_daily",
        "daily_basic_update",
        "index_daily_update",
        "zt_dt_pool_daily",
        "suspend_d_ingest",
    }
    assert {contract.pipeline_id for contract in registered} == {
        *(contract.pipeline_id for contract in MARKET_DATA_CONTRACTS),
        *(contract.pipeline_id for contract in CNINFO_CONTRACTS),
        *(contract.pipeline_id for contract in IRM_CONTRACTS),
        *(contract.pipeline_id for contract in MEMBERSHIP_CONTRACTS),
        *(contract.pipeline_id for contract in PIT_FUNDAMENTALS_CONTRACTS),
        *(contract.pipeline_id for contract in RESEARCH_REPORT_CONTRACTS),
        *(contract.pipeline_id for contract in RESEARCH_INDUSTRY_CONTRACTS),
    }
    assert all(contract.resource_locks == ("quant_db_writer",) for contract in MARKET_DATA_CONTRACTS)
    assert MARKET_DAILY_UPDATE.dependencies == ()
    assert all("stock_info" not in item.source and "suspend_d" not in item.source for item in MARKET_DAILY_UPDATE.inputs)
    assert ADJ_FACTOR_DAILY.dependencies == ("market_daily_update",)
    assert DAILY_BASIC_UPDATE.dependencies == ("market_daily_update",)


def test_target_policy_uses_calendar_close_time_weekends_and_explicit_dates(tmp_path: Path) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    after_close = PipelineInvocation(
        run_id="after-close",
        pipeline_id="market_daily_update",
        scheduled_for=datetime(2026, 7, 29, 8, 30, tzinfo=UTC),  # 16:30 Shanghai
        attempt=1,
        settings=item,
    )
    before_close = replace(after_close, scheduled_for=datetime(2026, 7, 29, 6, 30, tzinfo=UTC))
    weekend = replace(after_close, scheduled_for=datetime(2026, 8, 1, 9, 0, tzinfo=UTC))

    assert MARKET_TARGET_DATE_POLICY.resolver(after_close).target_date == TARGET
    assert MARKET_TARGET_DATE_POLICY.resolver(before_close).target_date == PREVIOUS
    assert MARKET_TARGET_DATE_POLICY.resolver(weekend).target_date == date(2026, 7, 30)
    assert MARKET_TARGET_DATE_POLICY.validate_explicit_date(TARGET, after_close)
    assert not MARKET_TARGET_DATE_POLICY.validate_explicit_date(date(2026, 8, 1), after_close)


def test_market_daily_real_executor_writes_completion_and_metrics(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    result = run(MARKET_DAILY_UPDATE, item)

    assert_contract_result_matches_context(result, MARKET_DAILY_UPDATE)
    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_read == 2
    assert result.metrics.rows_written == 2
    assert result.metrics.api_requests == 1
    assert result.outputs[0].completed
    assert (item.paths.raw_dir / "daily_snapshot" / "2026" / "2026-07-29_Astock_tushare.csv").is_file()
    assert (item.paths.canonical_dir / "daily_market_snapshot" / "2026-07-29.csv").is_file()

    repeated = run(MARKET_DAILY_UPDATE, item)
    assert repeated.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM daily_market_snapshot WHERE trade_date = ?", [TARGET]).fetchone()[0] == 2
    finally:
        connection.close()


def test_market_daily_historical_replay_ignores_future_rows_and_only_enriches_from_prior_history(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    future = date(2026, 7, 30)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, close) VALUES (?, ?, ?)",
            [PREVIOUS, "000001.SZ", 9.0],
        )
        connection.execute(
            "INSERT INTO daily_market_snapshot (trade_date, ticker, close) VALUES (?, ?, ?)",
            [future, "000001.SZ", 99.0],
        )
    finally:
        connection.close()
    client = FakeTushare()
    client.daily_frame.loc[client.daily_frame["ts_code"] == "000001.SZ", "pre_close"] = None
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    replay = run(MARKET_DAILY_UPDATE, item)

    assert replay.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT pre_close FROM daily_market_snapshot WHERE trade_date = ? AND ticker = ?",
            [TARGET, "000001.SZ"],
        ).fetchone()[0] == 9.0
        assert connection.execute(
            "SELECT close FROM daily_market_snapshot WHERE trade_date = ? AND ticker = ?",
            [future, "000001.SZ"],
        ).fetchone()[0] == 99.0
    finally:
        connection.close()


def test_daily_basic_replaces_target_without_duplicate_or_historical_loss(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    seed_market_target(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute("INSERT INTO daily_basic (trade_date, ticker, close) VALUES (?, ?, ?)", [PREVIOUS, "000001.SZ", 9.0])
        connection.execute("INSERT INTO daily_basic (trade_date, ticker, close) VALUES (?, ?, ?)", [TARGET, "000001.SZ", 1.0])
    finally:
        connection.close()

    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    first = run(DAILY_BASIC_UPDATE, item)
    second = run(DAILY_BASIC_UPDATE, item)

    assert first.status is second.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM daily_basic WHERE trade_date = ?", [TARGET]).fetchone()[0] == 2
        assert connection.execute("SELECT close FROM daily_basic WHERE trade_date = ? AND ticker = '000001.SZ'", [PREVIOUS]).fetchone()[0] == 9.0
    finally:
        connection.close()


def test_daily_basic_rejects_a_single_missing_market_ticker_before_writing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    seed_market_target(item)
    client = FakeTushare()
    client.daily_basic_frame = client.daily_basic_frame.iloc[:1].copy()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    import qrp_atlas.pipeline.market_data_contracts as subject

    write_calls: list[str] = []
    monkeypatch.setattr(subject, "_replace_target_date", lambda *_args, **_kwargs: write_calls.append("replace"))

    result = run(DAILY_BASIC_UPDATE, item)

    assert result.status is ResultStatus.FAILED
    assert "DAILY_BASIC_API_PARTIAL" in diagnostics(result)
    assert not write_calls


def test_market_daily_does_not_infer_coverage_from_stock_info_or_suspend_d(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute("DROP TABLE stock_info")
        connection.execute("DROP TABLE suspend_d")
    finally:
        connection.close()
    client = FakeTushare()
    client.daily_frame = client.daily_frame.iloc[:1].copy()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    result = run(MARKET_DAILY_UPDATE, item)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_written == 1
    assert "MARKET_DAILY_API_PARTIAL" not in diagnostics(result)


def test_adj_factor_requires_complete_market_universe_and_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    seed_market_target(item)
    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    first = run(ADJ_FACTOR_DAILY, item, dependencies=(MARKET_DAILY_UPDATE,))
    second = run(ADJ_FACTOR_DAILY, item, dependencies=(MARKET_DAILY_UPDATE,))

    assert first.status is second.status is ResultStatus.SUCCESS
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM adj_factor_changes WHERE trade_date = ?", [TARGET]).fetchone()[0] == 2
    finally:
        connection.close()

    client.adj_factor_frame = client.adj_factor_frame.iloc[:1]
    partial = run(ADJ_FACTOR_DAILY, item, dependencies=(MARKET_DAILY_UPDATE,))
    assert partial.status is ResultStatus.FAILED
    assert "ADJ_FACTOR_API_PARTIAL" in diagnostics(partial)


def test_adj_factor_replay_replaces_the_complete_target_change_set_and_preserves_other_dates(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    seed_market_target(item)
    future = date(2026, 7, 30)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.executemany(
            "INSERT INTO adj_factor_changes (trade_date, ticker, adj_factor) VALUES (?, ?, ?)",
            [
                (PREVIOUS, "000001.SZ", 1.0),
                (PREVIOUS, "600000.SH", 2.0),
                (future, "000001.SZ", 9.9),
            ],
        )
    finally:
        connection.close()
    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    first = run(ADJ_FACTOR_DAILY, item, dependencies=(MARKET_DAILY_UPDATE,))
    assert first.status is ResultStatus.SUCCESS
    client.adj_factor_frame = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "trade_date": [TARGET.strftime("%Y%m%d")] * 2,
            "adj_factor": [1.0, 2.0],
        }
    )

    corrected = run(ADJ_FACTOR_DAILY, item, dependencies=(MARKET_DAILY_UPDATE,))

    assert corrected.status is ResultStatus.SUCCESS
    assert corrected.metrics.rows_written == 0
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM adj_factor_changes WHERE trade_date = ?", [TARGET]).fetchone()[0] == 0
        assert connection.execute(
            "SELECT adj_factor FROM adj_factor_changes WHERE trade_date = ? AND ticker = ?",
            [future, "000001.SZ"],
        ).fetchone()[0] == 9.9
    finally:
        connection.close()


def test_adj_factor_blocks_when_market_target_is_not_complete(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: FakeTushare())

    result = run(ADJ_FACTOR_DAILY, item, dependencies=(MARKET_DAILY_UPDATE,))

    assert result.status is ResultStatus.FAILED
    assert "MARKET_DAILY_INPUT_STALE" in diagnostics(result)


def test_index_daily_requires_all_series_and_upserts_exact_target(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    monkeypatch.setattr(
        "qrp_atlas.pipeline.market_data_contracts.ak.stock_zh_index_daily",
        lambda **_kwargs: index_frame(),
    )

    result = run(INDEX_DAILY_UPDATE, item)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.api_requests == 4
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM index_daily WHERE trade_date = ?", [TARGET]).fetchone()[0] == 4
    finally:
        connection.close()

    repeated = run(INDEX_DAILY_UPDATE, item)
    assert repeated.status is ResultStatus.SUCCESS

    monkeypatch.setattr(
        "qrp_atlas.pipeline.market_data_contracts.ak.stock_zh_index_daily",
        lambda **_kwargs: index_frame().iloc[:1],
    )
    failed = run(INDEX_DAILY_UPDATE, item)
    assert failed.status is ResultStatus.FAILED
    assert "INDEX_DAILY_API_PARTIAL" in diagnostics(failed)


def test_zt_dt_pool_is_atomic_and_allows_explicit_empty_snapshots(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)

    def pool(endpoint: str, _target: date, *, sort: str):
        del sort
        if endpoint == "getTopicZTPool":
            return ([{"c": "000001", "n": "Sample", "p": 10000, "zdp": 10.0, "fbt": 93000}], 1)
        return ([{"c": "600000", "n": "Other", "p": 8000, "zdp": -10.0, "lbt": 145900}], 1)

    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts._fetch_eastmoney_pool", pool)
    first = run(ZT_DT_POOL_DAILY, item)
    assert first.status is ResultStatus.SUCCESS
    assert first.metrics.rows_written == 2

    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts._fetch_eastmoney_pool", lambda *_args, **_kwargs: ([], 1))
    empty = run(ZT_DT_POOL_DAILY, item)
    assert empty.status is ResultStatus.SUCCESS
    assert [output.rows_written for output in empty.outputs] == [0, 0]
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM zt_pool WHERE trade_date = ?", [TARGET]).fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM dt_pool WHERE trade_date = ?", [TARGET]).fetchone()[0] == 0
    finally:
        connection.close()


def test_eastmoney_pool_fetches_all_pages_when_the_reported_total_exceeds_200(monkeypatch) -> None:
    import qrp_atlas.pipeline.market_data_contracts as subject

    records = [_eastmoney_record(value) for value in range(201)]
    calls, open_url = _eastmoney_urlopen(
        {
            ("getTopicZTPool", 0): _eastmoney_payload(records=records[:200], total=201),
            ("getTopicZTPool", 1): _eastmoney_payload(records=records[200:], total=201),
        }
    )
    monkeypatch.setattr(subject.urllib.request, "urlopen", open_url)

    actual, requests = subject._fetch_eastmoney_pool("getTopicZTPool", TARGET, sort="fbt:asc")

    assert len(actual) == 201
    assert requests == 2
    assert calls == [("getTopicZTPool", 0), ("getTopicZTPool", 1)]


@pytest.mark.parametrize(
    "responses",
    (
        {
            ("getTopicZTPool", 0): _eastmoney_payload(records=[_eastmoney_record(1)], total=1, response_date=PREVIOUS),
        },
        {
            ("getTopicZTPool", 0): _eastmoney_payload(records=[_eastmoney_record(value) for value in range(200)], total=201),
            ("getTopicZTPool", 1): _eastmoney_payload(records=[_eastmoney_record(200), _eastmoney_record(201)], total=201),
        },
        {
            ("getTopicZTPool", 0): _eastmoney_payload(records=[_eastmoney_record(value) for value in range(200)], total=201),
            ("getTopicZTPool", 1): RuntimeError("second page unavailable"),
        },
    ),
    ids=("wrong-date", "inconsistent-total", "second-page-failure"),
)
def test_eastmoney_invalid_or_incomplete_pool_response_preserves_both_existing_snapshots(
    tmp_path: Path,
    monkeypatch,
    responses,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute("INSERT INTO zt_pool (trade_date, ticker) VALUES (?, ?)", [TARGET, "000001"])
        connection.execute("INSERT INTO dt_pool (trade_date, ticker) VALUES (?, ?)", [TARGET, "600000"])
    finally:
        connection.close()
    import qrp_atlas.pipeline.market_data_contracts as subject

    _calls, open_url = _eastmoney_urlopen(responses)
    monkeypatch.setattr(subject.urllib.request, "urlopen", open_url)

    result = run(ZT_DT_POOL_DAILY, item)

    assert result.status is ResultStatus.FAILED
    assert "ZT_DT_POOL_API_PARTIAL" in diagnostics(result) or "ZT_DT_POOL_API_FAILED" in diagnostics(result)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM zt_pool WHERE trade_date = ?", [TARGET]).fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM dt_pool WHERE trade_date = ?", [TARGET]).fetchone()[0] == 1
    finally:
        connection.close()


def test_eastmoney_explicit_zero_totals_are_the_only_valid_empty_snapshots(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute("INSERT INTO zt_pool (trade_date, ticker) VALUES (?, ?)", [TARGET, "000001"])
        connection.execute("INSERT INTO dt_pool (trade_date, ticker) VALUES (?, ?)", [TARGET, "600000"])
    finally:
        connection.close()
    import qrp_atlas.pipeline.market_data_contracts as subject

    _calls, open_url = _eastmoney_urlopen(
        {
            ("getTopicZTPool", 0): _eastmoney_payload(records=[], total=0),
            ("getTopicDTPool", 0): _eastmoney_payload(records=[], total=0),
        }
    )
    monkeypatch.setattr(subject.urllib.request, "urlopen", open_url)

    result = run(ZT_DT_POOL_DAILY, item)

    assert result.status is ResultStatus.SUCCESS
    assert [output.rows_written for output in result.outputs] == [0, 0]
    assert [output.detail["empty_snapshot"] for output in result.outputs] == [True, True]


def test_zt_dt_pool_rejects_one_api_failure_without_replacing_either_table(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute("INSERT INTO zt_pool (trade_date, ticker) VALUES (?, ?)", [TARGET, "000001"])
        connection.execute("INSERT INTO dt_pool (trade_date, ticker) VALUES (?, ?)", [TARGET, "600000"])
    finally:
        connection.close()

    def pool(endpoint: str, _target: date, *, sort: str):
        del sort
        if endpoint == "getTopicZTPool":
            return ([], 1)
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts._fetch_eastmoney_pool", pool)
    result = run(ZT_DT_POOL_DAILY, item)
    assert result.status is ResultStatus.FAILED
    assert "ZT_DT_POOL_API_FAILED" in diagnostics(result)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM zt_pool WHERE trade_date = ?", [TARGET]).fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM dt_pool WHERE trade_date = ?", [TARGET]).fetchone()[0] == 1
    finally:
        connection.close()


def test_suspend_d_empty_is_an_explicit_complete_snapshot(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            "INSERT INTO suspend_d (trade_date, ticker, suspend_type) VALUES (?, ?, ?)",
            [TARGET, "000001.SZ", "S"],
        )
    finally:
        connection.close()

    client = FakeTushare()
    client.suspend_frame = pd.DataFrame()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    result = run(SUSPEND_D_INGEST, item)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_written == 0
    assert result.outputs[0].detail["empty_snapshot"] is True
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM suspend_d WHERE trade_date = ?", [TARGET]).fetchone()[0] == 0
    finally:
        connection.close()


def test_suspend_d_passes_the_invocation_execution_control_to_tushare(
    tmp_path: Path,
    monkeypatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeTushare()
    control = ExecutionControl()
    observed: list[ExecutionControl | None] = []

    def get_client(**kwargs):
        observed.append(kwargs.get("execution_control"))
        return client

    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", get_client)

    result = run_suspend_with_control(item, control)

    assert result.status is ResultStatus.SUCCESS
    assert observed == [control]


@pytest.mark.parametrize("stop_mode", ("cancel", "deadline"))
def test_suspend_d_stops_before_client_creation_when_control_is_not_active(
    tmp_path: Path,
    monkeypatch,
    stop_mode: str,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    control = ExecutionControl()
    if stop_mode == "cancel":
        control.cancel("test cancellation")
    else:
        control.deadline = datetime.now(UTC) - timedelta(seconds=1)
    client_calls: list[dict] = []

    def get_client(**kwargs):
        client_calls.append(kwargs)
        return FakeTushare()

    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", get_client)

    result = run_suspend_with_control(item, control)

    assert result.status is ResultStatus.FAILED
    assert not client_calls
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM suspend_d WHERE trade_date = ?", [TARGET]).fetchone()[0] == 0
    finally:
        connection.close()


@pytest.mark.parametrize("stop_mode", ("cancel", "deadline"))
def test_suspend_d_checks_control_after_response_before_duckdb_write(
    tmp_path: Path,
    monkeypatch,
    stop_mode: str,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute(
            "INSERT INTO suspend_d (trade_date, ticker, suspend_type) VALUES (?, ?, ?)",
            [TARGET, "000001.SZ", "S"],
        )
    finally:
        connection.close()

    control = ExecutionControl()
    client = FakeTushare()

    def suspend_d(**_kwargs):
        if stop_mode == "cancel":
            control.cancel("provider response cancellation")
        else:
            control.deadline = datetime.now(UTC) - timedelta(seconds=1)
        return client.suspend_frame.copy()

    client.suspend_d = suspend_d
    monkeypatch.setattr(
        "qrp_atlas.pipeline.market_data_contracts.get_tushare_pro",
        lambda **kwargs: client,
    )
    import qrp_atlas.pipeline.market_data_contracts as subject

    write_calls: list[object] = []
    original_replace = subject._replace_target_date

    def tracked_replace(*args, **kwargs):
        write_calls.append(object())
        return original_replace(*args, **kwargs)

    monkeypatch.setattr(subject, "_replace_target_date", tracked_replace)

    result = run_suspend_with_control(item, control)

    assert result.status is ResultStatus.FAILED
    assert not write_calls
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM suspend_d WHERE trade_date = ?", [TARGET]).fetchone()[0] == 1
    finally:
        connection.close()


def test_input_missing_stale_and_api_failures_return_stable_codes(tmp_path: Path, monkeypatch) -> None:
    missing = settings(tmp_path / "missing")
    missing.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(missing.paths.duckdb_path))
    try:
        connection.execute("CREATE TABLE trading_calendar (trade_date DATE PRIMARY KEY, is_open BOOLEAN)")
        connection.execute("INSERT INTO trading_calendar VALUES (?, TRUE)", [TARGET])
    finally:
        connection.close()
    missing_result = run(MARKET_DAILY_UPDATE, missing)
    assert missing_result.status is ResultStatus.FAILED
    assert "MARKET_HISTORY_STRUCTURE_MISSING" in diagnostics(missing_result)

    stale = settings(tmp_path / "stale")
    initialise_database(stale, include_target=False)
    stale_result = ContractTestHarness(
        DAILY_BASIC_UPDATE,
        stale,
        scheduled_for=datetime(2026, 7, 29, 8, 30, tzinfo=UTC),
        dependency_contracts=(MARKET_DAILY_UPDATE,),
    ).run()
    assert stale_result.status is ResultStatus.FAILED
    assert "TRADING_CALENDAR_STALE" in diagnostics(stale_result)

    item = settings(tmp_path / "api")
    initialise_database(item)
    seed_market_target(item)
    client = FakeTushare()
    client.daily_basic_frame = pd.DataFrame()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)
    api_result = run(DAILY_BASIC_UPDATE, item)
    assert api_result.status is ResultStatus.FAILED
    assert "DAILY_BASIC_API_EMPTY" in diagnostics(api_result)

    invalid_override = ContractTestHarness(
        DAILY_BASIC_UPDATE,
        item,
        dependency_contracts=(MARKET_DAILY_UPDATE,),
    ).run(trade_date=date(2026, 8, 1))
    assert invalid_override.status is ResultStatus.FAILED
    assert "TARGET_DATE_OVERRIDE_INVALID" in diagnostics(invalid_override)
    parameter_error = ContractTestHarness(
        DAILY_BASIC_UPDATE,
        item,
        dependency_contracts=(MARKET_DAILY_UPDATE,),
    ).run(
        trade_date=TARGET,
        parameter_overrides={"unexpected": "value"},
    )
    assert parameter_error.status is ResultStatus.FAILED
    assert "UNKNOWN_PARAMETER" in diagnostics(parameter_error)


def test_completion_failure_and_transaction_interruption_do_not_report_success(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    seed_market_target(item)
    client = FakeTushare()
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute("INSERT INTO daily_basic (trade_date, ticker, close) VALUES (?, ?, ?)", [TARGET, "000001.SZ", 1.0])
    finally:
        connection.close()

    import qrp_atlas.pipeline.market_data_contracts as subject

    original_insert = subject._insert_frame
    monkeypatch.setattr(subject, "_insert_frame", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupted")))
    interrupted = run(DAILY_BASIC_UPDATE, item)
    assert interrupted.status is ResultStatus.FAILED
    assert "DAILY_BASIC_WRITE_FAILED" in diagnostics(interrupted)
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute("SELECT close FROM daily_basic WHERE trade_date = ? AND ticker = '000001.SZ'", [TARGET]).fetchone()[0] == 1.0
    finally:
        connection.close()

    monkeypatch.setattr(subject, "_insert_frame", original_insert)
    output = DAILY_BASIC_UPDATE.outputs[0]
    failing_completion = replace(
        output,
        completion=CompletionContract(
            marker=output.completion.marker,
            error_code="DAILY_BASIC_COMPLETION_MISSING",
            checker=lambda _context: CheckResult.failure("forced_completion", "DAILY_BASIC_COMPLETION_MISSING", "forced"),
        ),
    )
    failing_contract = replace(DAILY_BASIC_UPDATE, outputs=(failing_completion,))
    completion_failed = run(failing_contract, item)
    assert completion_failed.status is ResultStatus.FAILED
    assert "DAILY_BASIC_COMPLETION_MISSING" in diagnostics(completion_failed)


def test_runtime_definition_uses_existing_claim_lock_retry_and_structured_result_path() -> None:
    definitions = {
        contract.pipeline_id: contract_runtime_definition(
            contract,
            ContractDeploymentSelection(contract.pipeline_id, True, "15 16 * * 1-5"),
        )
        for contract in MARKET_DATA_CONTRACTS
    }
    assert definitions["adj_factor_daily"].dependencies == ("market_daily_update",)
    assert all(definition.resource_locks == ("quant_db_writer",) for definition in definitions.values())
    assert all(definition.requires_structured_result for definition in definitions.values())
    assert all(definition.in_process_executor is not None for definition in definitions.values())
    assert all(definition.max_retries == 1 and definition.overlap_policy.value == "FORBID" for definition in definitions.values())


def test_formal_cli_uses_existing_runner_claim_and_persists_a_structured_failure(tmp_path: Path) -> None:
    item = settings(tmp_path)
    item.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        connection.execute("CREATE TABLE trading_calendar (trade_date DATE PRIMARY KEY, is_open BOOLEAN)")
        connection.execute("INSERT INTO trading_calendar VALUES (?, TRUE)", [TARGET])
    finally:
        connection.close()
    env_file = tmp_path / "pipeline.env"
    env_file.write_text(
        "\n".join(
            (
                f"QRP_HOME={item.paths.home}",
                f"QRP_DATA_DIR={item.paths.data_dir}",
                "TUSHARE_TOKEN=test-token-only",
                "QRP_RUNTIME_ENV=test",
            )
        ),
        encoding="utf-8",
    )
    runtime_dir = tmp_path / "runtime"

    code = pipeline_cli(
        [
            "--env-file",
            str(env_file),
            "--runtime-dir",
            str(runtime_dir),
            "run",
            "market_daily_update",
            "--trade-date",
            TARGET.isoformat(),
        ]
    )

    assert code == 1
    store = JobRuntimeStore(runtime_dir / "job_runtime.sqlite3")
    runs = store.list_runs(job_id="market_daily_update")
    assert len(runs) == 1
    assert runs[0].status.value == "FAILED"
    result = store.get_result(runs[0].run_id)
    assert result is not None
    assert result["status"] == "FAILED"
    assert any(item["code"] == "MARKET_HISTORY_STRUCTURE_MISSING" for item in result["diagnostics"])
    assert runs[0].stdout_path is None
    assert runs[0].stderr_path is None
    assert not (runtime_dir / "logs").exists()


def _large_market_frame(rows: int = 5_000) -> pd.DataFrame:
    tickers = [f"{value:06d}.SZ" for value in range(rows)]
    return pd.DataFrame(
        {
            "ts_code": tickers,
            "trade_date": [TARGET.strftime("%Y%m%d")] * rows,
            "open": [10.0] * rows,
            "high": [11.0] * rows,
            "low": [9.0] * rows,
            "close": [10.5] * rows,
            "pre_close": [10.0] * rows,
            "pct_chg": [5.0] * rows,
            "vol": [1000] * rows,
            "amount": [10000] * rows,
        }
    )


def _large_daily_basic_frame(rows: int = 5_000) -> pd.DataFrame:
    tickers = [f"{value:06d}.SZ" for value in range(rows)]
    return pd.DataFrame(
        {
            "ts_code": tickers,
            "trade_date": [TARGET.strftime("%Y%m%d")] * rows,
            "close": [10.5] * rows,
            "pe": [12.0] * rows,
            "total_share": [100.0] * rows,
        }
    )


def test_equivalent_daily_scale_benchmark_records_metrics_for_all_six_contracts(tmp_path: Path, monkeypatch) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    client = FakeTushare()
    client.daily_frame = _large_market_frame()
    client.daily_basic_frame = _large_daily_basic_frame()
    client.adj_factor_frame = pd.DataFrame(
        {
            "ts_code": client.daily_frame["ts_code"],
            "trade_date": [TARGET.strftime("%Y%m%d")] * len(client.daily_frame),
            "adj_factor": [1.0] * len(client.daily_frame),
        }
    )
    client.suspend_frame = pd.DataFrame(
        {
            "ts_code": client.daily_frame["ts_code"],
            "trade_date": [TARGET.strftime("%Y%m%d")] * len(client.daily_frame),
            "suspend_timing": ["09:30"] * len(client.daily_frame),
            "suspend_type": ["S"] * len(client.daily_frame),
        }
    )
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)
    monkeypatch.setattr(
        "qrp_atlas.pipeline.market_data_contracts.ak.stock_zh_index_daily",
        lambda **_kwargs: index_frame(),
    )
    pool_records = [{"c": f"{value:06d}", "n": "Sample", "p": 10000, "zdp": 10.0} for value in range(200)]
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts._fetch_eastmoney_pool", lambda *_args, **_kwargs: (pool_records, 1))

    market = run(MARKET_DAILY_UPDATE, item)
    adj = run(ADJ_FACTOR_DAILY, item, dependencies=(MARKET_DAILY_UPDATE,))
    basic = run(DAILY_BASIC_UPDATE, item)
    index = run(INDEX_DAILY_UPDATE, item)
    pools = run(ZT_DT_POOL_DAILY, item)
    suspend = run(SUSPEND_D_INGEST, item)

    results = (market, adj, basic, index, pools, suspend)
    assert all(result.status is ResultStatus.SUCCESS for result in results)
    assert all(result.performance.within_normal_budget for result in results)
    assert market.metrics.rows_read == basic.metrics.rows_read == adj.metrics.rows_read == suspend.metrics.rows_read == 5_000
    assert pools.metrics.rows_read == 400
    assert all(result.metrics.database_write_seconds >= 0 for result in results)
    print(
        "market-data-benchmark-seconds "
        + " ".join(f"{result.pipeline_id}={result.duration_seconds:.3f}" for result in results)
    )


def test_eastmoney_pool_accepts_qdate_and_tc_fields(monkeypatch) -> None:
    """真实东财响应使用 qdate/tc 字段；date/total 兼容保持。"""
    import qrp_atlas.pipeline.market_data_contracts as subject

    records = [_eastmoney_record(value) for value in range(3)]
    calls, open_url = _eastmoney_urlopen(
        {
            ("getTopicZTPool", 0): {
                "rc": 0,
                "data": {"qdate": int(TARGET.strftime("%Y%m%d")), "tc": 3, "pool": records},
            },
        }
    )
    monkeypatch.setattr(subject.urllib.request, "urlopen", open_url)

    actual, total = subject._fetch_eastmoney_pool_page(
        "getTopicZTPool", TARGET, sort="fbt:asc", page_index=0
    )

    assert len(actual) == 3
    assert total == 3
    assert calls == [("getTopicZTPool", 0)]


def test_daily_basic_coverage_excludes_suspended_enriched_tickers(tmp_path: Path, monkeypatch) -> None:
    """snapshot 中的停牌补全股（Tushare daily 无当日行）不要求 daily_basic 覆盖。"""
    item = settings(tmp_path)
    initialise_database(item)
    # snapshot 预置 3 个 ticker：2 个当日交易 + 1 个补全停牌股
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        for ticker in ("000001.SZ", "600000.SH", "300001.SZ"):
            connection.execute(
                "INSERT INTO daily_market_snapshot (trade_date, ticker, close) VALUES (?, ?, ?)",
                [TARGET, ticker, 10.0],
            )
    finally:
        connection.close()

    client = FakeTushare()
    # daily 当日仅 2 个交易；daily_basic 覆盖这 2 个
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    result = run(DAILY_BASIC_UPDATE, item)

    assert result.status is ResultStatus.SUCCESS
    assert result.outputs[0].detail["expected_tickers"] == 2
    assert result.outputs[0].detail["suspended_excluded"] == 1
    assert result.metrics.api_requests == 2


def test_daily_basic_coverage_still_fails_closed_when_traded_ticker_missing(tmp_path: Path, monkeypatch) -> None:
    """当日交易股缺失仍必须 PARTIAL（fail-closed 保持）。"""
    item = settings(tmp_path)
    initialise_database(item)
    connection = duckdb.connect(str(item.paths.duckdb_path))
    try:
        for ticker in ("000001.SZ", "600000.SH"):
            connection.execute(
                "INSERT INTO daily_market_snapshot (trade_date, ticker, close) VALUES (?, ?, ?)",
                [TARGET, ticker, 10.0],
            )
    finally:
        connection.close()

    client = FakeTushare()
    # daily_basic 只返回 1 个 traded ticker
    client.daily_basic_frame = client.daily_basic_frame.iloc[[0]]
    monkeypatch.setattr("qrp_atlas.pipeline.market_data_contracts.get_tushare_pro", lambda **_kwargs: client)

    result = run(DAILY_BASIC_UPDATE, item)

    assert result.status is ResultStatus.FAILED
    assert any(d.code == "DAILY_BASIC_API_PARTIAL" for d in result.diagnostics)
