"""Contract-level acceptance for the PIT/fundamentals/forecast work package."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline.fundamentals.fetch import (
    FinancialFetchError,
    fetch_financial_by_ann_date,
    fetch_financial_by_tickers,
)
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, ResultStatus
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.pit_backfill.raw_io import save_parquet
from qrp_atlas.pipeline.pit_backfill.safety import load_backup_marker
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline import pit_fundamentals_contracts as subject


SCHEDULED_FOR = datetime(2024, 1, 2, tzinfo=UTC)


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
            "QRP_RUNTIME_ENV": "test",
            "TUSHARE_TOKEN": "test-token",
        },
        project_root=tmp_path / "repo",
    )


def _initialise_database(settings: AppSettings) -> None:
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.paths.duckdb_path))
    try:
        init_database(connection)
        connection.executemany(
            "INSERT INTO trading_calendar (trade_date, is_open, year, quarter, month) VALUES (?, ?, ?, ?, ?)",
            [
                (date(2024, 1, 2), True, 2024, 1, 1),
                (date(2024, 3, 18), True, 2024, 1, 3),
                (date(2024, 3, 19), True, 2024, 1, 3),
                (date(2024, 4, 1), True, 2024, 2, 4),
            ],
        )
    finally:
        connection.close()


def _financial_row(period: str = "20231231", *, basic_eps: float = 1.2) -> dict:
    return {
        "ts_code": "000001.SZ",
        "ann_date": "20240315",
        "f_ann_date": "20240315",
        "end_date": period,
        "report_type": "1",
        "comp_type": "1",
        "end_type": "4",
        "update_flag": "0",
        "basic_eps": basic_eps,
    }


def _forecast_row(*, summary: str = "growth", p_change_min: float = 10.0, end_date: str = "20231231") -> dict:
    return {
        "ts_code": "000001.SZ",
        "ann_date": "20240315",
        "end_date": end_date,
        "type": "预增",
        "p_change_min": p_change_min,
        "p_change_max": 20.0,
        "net_profit_min": 1000.0,
        "net_profit_max": 1200.0,
        "last_parent_net": 900.0,
        "first_ann_date": "20240315",
        "summary": summary,
        "change_reason": "主营增长",
    }


def _run(contract, settings: AppSettings, parameters: dict[str, object], *, run_id: str, control=None):
    return execute_pipeline_contract(
        contract,
        PipelineInvocation(
            run_id=run_id,
            pipeline_id=contract.pipeline_id,
            scheduled_for=SCHEDULED_FOR,
            attempt=1,
            settings=settings,
            parameter_overrides=parameters,
            execution_control=control or ExecutionControl(),
            audit_context={"test": "true"},
        ),
    )


def _count(settings: AppSettings, table: str) -> int:
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        connection.close()


def test_work_package_contracts_are_registered_manual_and_valid() -> None:
    validate_contracts(subject.PIT_FUNDAMENTALS_CONTRACTS)
    registry = default_registry()
    assert {item.pipeline_id for item in subject.PIT_FUNDAMENTALS_CONTRACTS} <= {
        item.pipeline_id for item in registry.all()
    }
    assert all(item.manual_execution_allowed for item in subject.PIT_FUNDAMENTALS_CONTRACTS)
    assert all(item.resource_locks == ("quant_db_writer",) for item in subject.PIT_FUNDAMENTALS_CONTRACTS)
    assert all(item.resource_reads == ("duckdb://quant_db#trading_calendar",) for item in subject.PIT_FUNDAMENTALS_CONTRACTS)
    assert [item.name for item in subject.PIT_BACKFILL.parameters][:4] == [
        "run_tag",
        "mode",
        "datasets",
        "stages",
    ]
    assert subject.PIT_BACKFILL.target_date_policy.non_trading_day_policy.value == "ALLOW_CALENDAR_DATE"


def test_fundamentals_repeat_and_changed_revision_are_transactional(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    current = {"row": _financial_row()}

    def provider(*_args, **_kwargs):
        return pd.DataFrame([current["row"]])

    monkeypatch.setattr(subject, "fetch_financial", provider)
    parameters = {"mode": "period", "tables": "income_statement", "periods": "20231231"}
    first = _run(subject.FUNDAMENTALS_INGEST, settings, parameters, run_id="fund-1")
    repeat = _run(subject.FUNDAMENTALS_INGEST, settings, parameters, run_id="fund-2")
    current["row"] = _financial_row(basic_eps=1.3)
    changed = _run(subject.FUNDAMENTALS_INGEST, settings, parameters, run_id="fund-3")

    assert first.status is ResultStatus.SUCCESS
    assert first.metrics.rows_written == 1
    assert repeat.metrics.rows_written == 0
    assert changed.metrics.rows_written == 1
    assert _count(settings, "income_statement") == 2


def test_fundamentals_failure_before_write_leaves_all_selected_tables_empty(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    monkeypatch.setattr(subject, "fetch_financial", lambda *_args, **_kwargs: pd.DataFrame([_financial_row()]))
    original_clean = subject.clean_financial

    def fail_second(frame, table, **kwargs):
        if table == "balance_sheet":
            raise ValueError("synthetic clean failure")
        return original_clean(frame, table, **kwargs)

    monkeypatch.setattr(subject, "clean_financial", fail_second)
    result = _run(
        subject.FUNDAMENTALS_INGEST,
        settings,
        {"mode": "period", "tables": "income_statement,balance_sheet", "periods": "20231231"},
        run_id="fund-fail",
    )
    assert result.status is ResultStatus.FAILED
    assert _count(settings, "income_statement") == 0
    assert _count(settings, "balance_sheet") == 0


def test_fundamentals_ticker_date_scope_uses_provider_announcement_date(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    def provider(*_args, **_kwargs):
        return pd.DataFrame([_financial_row(period="20231231") | {"f_ann_date": "20240320"}])

    monkeypatch.setattr(subject, "fetch_financial", provider)
    result = _run(
        subject.FUNDAMENTALS_INGEST,
        settings,
        {
            "mode": "ticker",
            "tables": "income_statement,balance_sheet,cashflow_statement",
            "tickers": "000001.SZ",
            "start_date": "20240301",
            "end_date": "20240331",
        },
        run_id="fund-date-scope",
    )

    assert result.status is ResultStatus.SUCCESS
    assert _count(settings, "income_statement") == 1
    assert _count(settings, "balance_sheet") == 1
    assert _count(settings, "cashflow_statement") == 1


def test_fundamentals_ann_date_mode_defaults_to_three_calendar_dates(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    calls: list[dict[str, object]] = []
    row = _financial_row(period="20231231") | {
        "ann_date": "20240102",
        "f_ann_date": "20240102",
    }

    def provider(*_args, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame([row])

    monkeypatch.setattr(subject, "fetch_financial", provider)
    result = _run(
        subject.FUNDAMENTALS_INGEST,
        settings,
        {"mode": "ann_date", "tables": "income_statement"},
        run_id="fund-ann-window",
    )

    assert result.status is ResultStatus.SUCCESS
    assert calls[0]["mode"] == "ann_date"
    assert calls[0]["ann_dates"] == ("20240101", "20240102", "20240103")
    assert result.outputs[0].detail["ann_dates"] == ["20240101", "20240102", "20240103"]
    assert _count(settings, "income_statement") == 1


@pytest.mark.parametrize("table", ["income_statement", "balance_sheet", "cashflow_statement"])
def test_fundamentals_ticker_announcement_date_out_of_range_fails_closed(tmp_path, monkeypatch, table) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    row = _financial_row(period="20240331") | {
        "ann_date": "20240229",
        "f_ann_date": "20240315",
    }
    monkeypatch.setattr(subject, "fetch_financial", lambda *_args, **_kwargs: pd.DataFrame([row]))

    result = _run(
        subject.FUNDAMENTALS_INGEST,
        settings,
        {
            "mode": "ticker",
            "tables": table,
            "tickers": "000001.SZ",
            "start_date": "20240301",
            "end_date": "20240331",
        },
        run_id=f"fund-ann-date-out-{table}",
    )

    assert result.status is ResultStatus.FAILED
    assert "FUNDAMENTALS_SCOPE_MISMATCH" in {item.code for item in result.diagnostics}
    assert _count(settings, table) == 0


def test_financial_indicator_ticker_scope_uses_report_period_and_rejects_limit(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    current = {"frame": pd.DataFrame([_financial_row(period="20231231")])}

    monkeypatch.setattr(subject, "fetch_financial", lambda *_args, **_kwargs: current["frame"].copy())
    out_of_range = _run(
        subject.FUNDAMENTALS_INGEST,
        settings,
        {
            "mode": "ticker",
            "tables": "financial_indicator",
            "tickers": "000001.SZ",
            "start_date": "20240301",
            "end_date": "20240331",
        },
        run_id="indicator-date-scope",
    )
    assert out_of_range.status is ResultStatus.FAILED
    assert "FUNDAMENTALS_SCOPE_MISMATCH" in {item.code for item in out_of_range.diagnostics}
    assert _count(settings, "financial_indicator") == 0

    current["frame"] = pd.DataFrame([_financial_row(period="20240331") for _ in range(100)])
    page_limited = _run(
        subject.FUNDAMENTALS_INGEST,
        settings,
        {
            "mode": "ticker",
            "tables": "financial_indicator",
            "tickers": "000001.SZ",
            "start_date": "20240301",
            "end_date": "20240331",
        },
        run_id="indicator-page-limit",
    )
    assert page_limited.status is ResultStatus.FAILED
    assert "FUNDAMENTALS_PAGE_LIMIT_REACHED" in {item.code for item in page_limited.diagnostics}
    assert _count(settings, "financial_indicator") == 0


def test_financial_indicator_fetch_rejects_exact_provider_page_limit() -> None:
    class FakePro:
        def fina_indicator(self, ts_code, **_kwargs):
            return pd.DataFrame([_financial_row(period="20240331") for _ in range(100)])

    with pytest.raises(FinancialFetchError) as caught:
        fetch_financial_by_tickers(
            "financial_indicator",
            ["000001.SZ"],
            start_date="20240301",
            end_date="20240331",
            client=FakePro(),
        )
    assert caught.value.code == "FUNDAMENTALS_PAGE_LIMIT_REACHED"


def test_financial_ann_date_fetch_uses_vip_announcement_parameter() -> None:
    class FakePro:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def income_vip(self, **kwargs):
            self.calls.append(kwargs)
            return pd.DataFrame([_financial_row()])

    client = FakePro()
    result = fetch_financial_by_ann_date("income_statement", "2024-03-15", client=client)

    assert client.calls == [{"ann_date": "20240315"}]
    assert len(result) == 1


def test_earnings_forecast_preserves_disclosure_and_technical_revisions(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    current = {"frame": pd.DataFrame([_forecast_row()])}
    report_calls = []

    def provider(*_args, report, **_kwargs):
        report.api_requests += 1
        report.batches += 1
        report.rows_read += len(current["frame"])
        report_calls.append(report)
        return current["frame"].copy()

    monkeypatch.setattr(subject, "fetch_earnings_forecast", provider)
    parameters = {"mode": "period", "periods": "20231231"}
    first = _run(subject.EARNINGS_FORECAST_INGEST, settings, parameters, run_id="earn-1")
    repeat = _run(subject.EARNINGS_FORECAST_INGEST, settings, parameters, run_id="earn-2")
    current["frame"] = pd.DataFrame([_forecast_row(summary="revised", p_change_min=12.0)])
    revised = _run(subject.EARNINGS_FORECAST_INGEST, settings, parameters, run_id="earn-3")

    assert first.status is ResultStatus.SUCCESS
    assert repeat.metrics.rows_written == 0
    assert revised.metrics.rows_written == 1
    assert revised.metrics.api_requests == 1
    assert _count(settings, "earnings_forecast_event") == 2
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT COUNT(DISTINCT source_record_id), COUNT(DISTINCT revision_id) FROM earnings_forecast_event"
        ).fetchone() == (1, 2)
    finally:
        connection.close()
    assert first.outputs[0].detail["ingestion_run_id"] == "earn-1"
    assert report_calls


def test_earnings_ann_date_mode_defaults_to_three_calendar_dates(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    calls: list[dict[str, object]] = []
    row = _forecast_row() | {"ann_date": "20240102", "first_ann_date": "20240102"}

    def provider(*_args, report, **kwargs):
        calls.append(kwargs)
        report.api_requests += 1
        report.batches += 1
        report.rows_read += 1
        return pd.DataFrame([row])

    monkeypatch.setattr(subject, "fetch_earnings_forecast", provider)
    result = _run(
        subject.EARNINGS_FORECAST_INGEST,
        settings,
        {"mode": "ann_date"},
        run_id="earn-ann-window",
    )

    assert result.status is ResultStatus.SUCCESS
    assert calls[0]["mode"] == "ann_date"
    assert calls[0]["ann_dates"] == ("20240101", "20240102", "20240103")
    assert result.outputs[0].detail["empty_scope_result"] is False
    assert _count(settings, "earnings_forecast_event") == 1


def test_earnings_raw_recovery_missing_corrupt_and_identity_fail_closed(tmp_path) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw_path = tmp_path / "recovery.parquet"
    missing = _run(
        subject.EARNINGS_FORECAST_INGEST,
        settings,
        {"mode": "from_raw", "raw_path": str(tmp_path / "missing.parquet"), "periods": "20231231"},
        run_id="raw-missing",
    )
    assert missing.status is ResultStatus.FAILED
    assert {item.code for item in missing.diagnostics} == {"EARNINGS_RAW_MISSING"}

    raw_path.write_bytes(b"not parquet")
    corrupt = _run(
        subject.EARNINGS_FORECAST_INGEST,
        settings,
        {"mode": "from_raw", "raw_path": str(raw_path), "periods": "20231231"},
        run_id="raw-corrupt",
    )
    assert corrupt.status is ResultStatus.FAILED
    assert {item.code for item in corrupt.diagnostics} == {"EARNINGS_RAW_CORRUPT"}

    save_parquet(pd.DataFrame([_forecast_row(end_date="20240630")]), raw_path)
    mismatch = _run(
        subject.EARNINGS_FORECAST_INGEST,
        settings,
        {"mode": "from_raw", "raw_path": str(raw_path), "periods": "20231231"},
        run_id="raw-mismatch",
    )
    assert mismatch.status is ResultStatus.FAILED
    assert {item.code for item in mismatch.diagnostics} == {"EARNINGS_RAW_IDENTITY_MISMATCH"}
    assert _count(settings, "earnings_forecast_event") == 0


def test_cancellation_before_provider_is_reported_and_does_not_write(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    called = False

    def provider(*_args, **_kwargs):
        nonlocal called
        called = True
        return pd.DataFrame([_financial_row()])

    monkeypatch.setattr(subject, "fetch_financial", provider)
    control = ExecutionControl()
    control.cancel("operator cancellation")
    result = _run(
        subject.FUNDAMENTALS_INGEST,
        settings,
        {"mode": "period", "tables": "income_statement", "periods": "20231231"},
        run_id="cancelled",
        control=control,
    )
    assert result.status is ResultStatus.FAILED
    assert "EXECUTION_CANCELLED" in {item.code for item in result.diagnostics}
    assert called is False
    assert _count(settings, "income_statement") == 0


def test_pit_backfill_uses_explicit_scope_and_offline_corruption_fails_closed(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    class FakePro:
        def income_vip(self, period):
            return pd.DataFrame([_financial_row(period)])

    monkeypatch.setattr("qrp_atlas.config.get_tushare_pro", lambda **_kwargs: FakePro())
    parameters = {
        "run_tag": "contract-test",
        "datasets": "fundamentals",
        "financial_tables": "income_statement",
        "financial_periods": "20231231",
    }
    first = _run(subject.PIT_BACKFILL, settings, parameters, run_id="pit-1")
    assert first.status is ResultStatus.SUCCESS
    assert first.metrics.batches == 1
    raw_path = settings.paths.raw_dir / "pit_backfill" / "contract-test" / "fundamentals__income_statement__20231231.parquet"
    raw_path.write_bytes(b"corrupt")
    retry = _run(
        subject.PIT_BACKFILL,
        settings,
        {**parameters, "resume": True, "offline_only": True},
        run_id="pit-2",
    )
    assert retry.status is ResultStatus.FAILED
    assert "PIT_BACKFILL_FAILED" in {item.code for item in retry.diagnostics}
    assert _count(settings, "income_statement") == 1


def test_pit_backfill_formal_staged_load_creates_backup_and_is_idempotent(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    class FakePro:
        def income_vip(self, period):
            return pd.DataFrame([_financial_row(period)])

    monkeypatch.setattr("qrp_atlas.config.get_tushare_pro", lambda **_kwargs: FakePro())
    parameters = {
        "run_tag": "staged-load",
        "datasets": "fundamentals",
        "financial_tables": "income_statement",
        "financial_periods": "20231231",
        "stages": "fetch,clean",
    }
    first = _run(subject.PIT_BACKFILL, settings, parameters, run_id="staged-fetch-clean")
    assert first.status is ResultStatus.SUCCESS
    assert _count(settings, "income_statement") == 0
    state_dir = settings.paths.state_dir / "pit_backfill" / "staged-load"
    assert load_backup_marker(state_dir) is None

    load_parameters = {**parameters, "stages": "load", "resume": True, "offline_only": True}
    loaded = _run(subject.PIT_BACKFILL, settings, load_parameters, run_id="staged-load-1")
    assert loaded.status is ResultStatus.SUCCESS
    assert loaded.metrics.rows_written == 1
    marker = load_backup_marker(state_dir)
    assert marker is not None
    assert marker["tag"] == "staged-load"

    repeated = _run(subject.PIT_BACKFILL, settings, load_parameters, run_id="staged-load-2")
    assert repeated.status is ResultStatus.SUCCESS
    assert repeated.metrics.rows_written == 0
    assert _count(settings, "income_statement") == 1


def test_pit_backfill_load_only_rejects_corrupt_cleaned_artifact_without_write(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    class FakePro:
        def income_vip(self, period):
            return pd.DataFrame([_financial_row(period)])

    monkeypatch.setattr("qrp_atlas.config.get_tushare_pro", lambda **_kwargs: FakePro())
    parameters = {
        "run_tag": "staged-corrupt",
        "datasets": "fundamentals",
        "financial_tables": "income_statement",
        "financial_periods": "20231231",
        "stages": "fetch,clean",
    }
    first = _run(subject.PIT_BACKFILL, settings, parameters, run_id="corrupt-fetch-clean")
    assert first.status is ResultStatus.SUCCESS
    cleaned_path = settings.paths.canonical_dir / "pit_backfill" / "staged-corrupt" / "fundamentals__income_statement__20231231.parquet"
    cleaned_path.write_bytes(b"corrupt cleaned artifact")

    failed = _run(
        subject.PIT_BACKFILL,
        settings,
        {**parameters, "stages": "load", "resume": True, "offline_only": True},
        run_id="corrupt-load",
    )
    assert failed.status is ResultStatus.FAILED
    assert "PIT_BACKFILL_FAILED" in {item.code for item in failed.diagnostics}
    assert _count(settings, "income_statement") == 0


def test_performance_result_and_all_registered_contracts_are_valid(tmp_path) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    validate_contracts(default_registry().all())
    raw_path = tmp_path / "empty-forecast.parquet"
    save_parquet(pd.DataFrame(), raw_path)
    result = _run(
        subject.EARNINGS_FORECAST_INGEST,
        settings,
        {"mode": "from_raw", "raw_path": str(raw_path)},
        run_id="perf-empty",
    )
    assert result.status is ResultStatus.SUCCESS
    assert result.performance.duration_seconds >= 0
