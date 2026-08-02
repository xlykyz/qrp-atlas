"""Offline acceptance tests for industry and index membership Contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError
from qrp_atlas.pipeline import membership_contracts as subject
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, ResultStatus, parse_parameter_overrides
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.industry_membership.fetch import (
    IndustryMembershipFetchReport,
    fetch_industry_membership_with_report,
)
from qrp_atlas.pipeline.index_component.fetch import IndexComponentFetchReport
from qrp_atlas.pipeline.registry import default_registry


SCHEDULED_FOR = datetime(2024, 1, 2, 0, 0, tzinfo=UTC)


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
            "INSERT INTO trading_calendar VALUES (?, ?, ?, ?, ?)",
            [
                (date(2024, 1, 2), True, 2024, 1, 1),
                (date(2024, 1, 3), True, 2024, 1, 1),
                (date(2024, 1, 16), True, 2024, 1, 1),
                (date(2024, 2, 1), True, 2024, 2, 1),
            ],
        )
    finally:
        connection.close()


def _industry_frame(*, asset: str = "300750.SZ", l1_name: str = "Power") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": asset,
                "l1_code": "801730.SI",
                "l1_name": l1_name,
                "l2_code": "801737.SI",
                "l2_name": "Battery",
                "l3_code": "857371.SI",
                "l3_name": "Lithium",
                "in_date": "20240101",
                "out_date": None,
            }
        ]
    )


def _index_frame(*, weight: float = 5.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "index_code": "000300.SH",
                "con_code": "600519.SH",
                "trade_date": "20240101",
                "weight": weight,
            },
            {
                "index_code": "000300.SH",
                "con_code": "600519.SH",
                "trade_date": "20240102",
                "weight": weight + 1,
            },
        ]
    )


def _run(
    contract,
    settings: AppSettings,
    parameters: dict[str, object],
    *,
    control: ExecutionControl | None = None,
    trade_date_override: date | None = None,
):
    return execute_pipeline_contract(
        contract,
        PipelineInvocation(
            run_id=f"{contract.pipeline_id}-test",
            pipeline_id=contract.pipeline_id,
            scheduled_for=SCHEDULED_FOR,
            attempt=1,
            settings=settings,
            parameter_overrides=parameters,
            trade_date_override=trade_date_override,
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


def _diagnostics(result) -> set[str]:
    return {item.code for item in result.diagnostics}


def test_membership_contracts_are_registered_described_and_manual() -> None:
    validate_contracts(subject.MEMBERSHIP_CONTRACTS)
    registered = default_registry().all()
    assert {item.pipeline_id for item in subject.MEMBERSHIP_CONTRACTS} <= {
        item.pipeline_id for item in registered
    }
    assert {item.pipeline_id for item in subject.MEMBERSHIP_CONTRACTS} == {
        "industry_membership_ingest",
        "index_component_ingest",
    }
    assert all(item.manual_execution_allowed for item in subject.MEMBERSHIP_CONTRACTS)
    assert all(item.resource_locks == ("quant_db_writer",) for item in subject.MEMBERSHIP_CONTRACTS)
    assert all(item.resource_reads == ("duckdb://quant_db#trading_calendar",) for item in subject.MEMBERSHIP_CONTRACTS)
    assert subject.INDUSTRY_MEMBERSHIP_INGEST.describe()["parameters"][0]["default"] == ""
    assert [item.name for item in subject.INDEX_COMPONENT_INGEST.parameters] == [
        "index_codes",
        "start_date",
        "end_date",
    ]


def test_parameter_contracts_parse_real_scopes_and_reject_unknown_values() -> None:
    industry = parse_parameter_overrides(
        subject.INDUSTRY_MEMBERSHIP_INGEST,
        {"tickers": "300750.SZ,600519.SH", "is_new": "Y"},
    )
    assert industry["tickers"] == "300750.SZ,600519.SH"
    assert industry["l1_code"] == ""
    index = parse_parameter_overrides(
        subject.INDEX_COMPONENT_INGEST,
        {
            "index_codes": "000300.SH,000905.SH",
            "start_date": "2024-01-01",
            "end_date": "2024-03-31",
        },
    )
    assert index["start_date"] == date(2024, 1, 1)
    with pytest.raises(ValueError, match="UNKNOWN_PARAMETER"):
        parse_parameter_overrides(
            subject.INDEX_COMPONENT_INGEST,
            {
                "index_codes": "000300.SH",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "unexpected": "value",
            },
        )


def test_industry_ticker_scope_is_transactional_and_idempotent(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    observed_controls: list[ExecutionControl] = []

    def provider(*, execution_control, **kwargs):
        observed_controls.append(execution_control)
        assert kwargs["tickers"] == ("300750.SZ",)
        return _industry_frame(), IndustryMembershipFetchReport(
            api_requests=1,
            batches=1,
            rows_read=1,
        )

    monkeypatch.setattr(subject, "fetch_industry_membership_with_report", provider)
    first = _run(subject.INDUSTRY_MEMBERSHIP_INGEST, settings, {"tickers": "300750.SZ"})
    second = _run(subject.INDUSTRY_MEMBERSHIP_INGEST, settings, {"tickers": "300750.SZ"})

    assert first.status is ResultStatus.SUCCESS
    assert first.metrics.rows_read == 1
    assert first.metrics.rows_written == 3
    assert first.metrics.assets_processed == 1
    assert first.metrics.api_requests == 1
    assert first.metrics.batches == 1
    assert first.outputs[0].detail["industry_count"] == 3
    assert second.status is ResultStatus.SUCCESS
    assert second.metrics.rows_written == 0
    assert _count(settings, "industry_membership_history") == 3
    assert len(observed_controls) == 2


def test_industry_code_scope_and_empty_result_are_explicit(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    calls: list[dict] = []

    def provider(*, execution_control, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame(), IndustryMembershipFetchReport(api_requests=1, batches=1, rows_read=0)

    monkeypatch.setattr(subject, "fetch_industry_membership_with_report", provider)
    result = _run(
        subject.INDUSTRY_MEMBERSHIP_INGEST,
        settings,
        {"l1_code": "801730.SI", "is_new": "N"},
    )
    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_read == 0
    assert result.metrics.rows_written == 0
    assert result.outputs[0].detail["empty_scope_result"] is True
    assert calls == [
        {
            "tickers": None,
            "l1_code": "801730.SI",
            "l2_code": None,
            "l3_code": None,
            "is_new": "N",
        }
    ]
    assert _count(settings, "industry_membership_history") == 0


def test_industry_revision_change_is_retained_as_a_new_pit_revision(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    current = {"frame": _industry_frame()}

    def provider(**_kwargs):
        return current["frame"], IndustryMembershipFetchReport(api_requests=1, batches=1, rows_read=1)

    monkeypatch.setattr(subject, "fetch_industry_membership_with_report", provider)
    first = _run(subject.INDUSTRY_MEMBERSHIP_INGEST, settings, {"tickers": "300750.SZ"})
    current["frame"] = _industry_frame(l1_name="Power revised")
    second = _run(subject.INDUSTRY_MEMBERSHIP_INGEST, settings, {"tickers": "300750.SZ"})
    assert first.metrics.rows_written == 3
    assert second.metrics.rows_written == 1
    assert _count(settings, "industry_membership_history") == 4


def test_index_multi_scope_range_deduplicates_provider_rows(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = pd.concat(
        [
            _index_frame(),
            _index_frame(weight=2.0).assign(index_code="000905.SH"),
        ],
        ignore_index=True,
    )
    raw = pd.concat([raw, raw.iloc[[0]]], ignore_index=True)

    def provider(*args, execution_control, **kwargs):
        assert args == (("000300.SH", "000905.SH"),)
        assert kwargs == {"start_date": "20240101", "end_date": "20240131"}
        return raw, IndexComponentFetchReport(api_requests=2, batches=2, rows_read=len(raw))

    monkeypatch.setattr(subject, "fetch_index_weights_with_report", provider)
    result = _run(
        subject.INDEX_COMPONENT_INGEST,
        settings,
        {
            "index_codes": "000300.SH,000905.SH",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
        },
    )
    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_read == 5
    assert result.metrics.rows_written == 4
    assert result.metrics.assets_processed == 1
    assert result.metrics.dates_processed == 2
    assert result.metrics.api_requests == 2
    assert result.metrics.batches == 2
    assert result.outputs[0].detail["index_count"] == 2
    assert _count(settings, "index_component_history") == 4


def test_index_repeat_and_weight_revision_are_idempotent_by_revision_id(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    current = {"frame": _index_frame()}

    def provider(*_args, **_kwargs):
        return current["frame"], IndexComponentFetchReport(api_requests=1, batches=1, rows_read=2)

    monkeypatch.setattr(subject, "fetch_index_weights_with_report", provider)
    params = {
        "index_codes": "000300.SH",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
    }
    first = _run(subject.INDEX_COMPONENT_INGEST, settings, params)
    repeat = _run(subject.INDEX_COMPONENT_INGEST, settings, params)
    current["frame"] = _index_frame(weight=6.0)
    revised = _run(subject.INDEX_COMPONENT_INGEST, settings, params)
    assert first.metrics.rows_written == 2
    assert repeat.metrics.rows_written == 0
    assert revised.metrics.rows_written == 2
    assert _count(settings, "index_component_history") == 4


def test_invalid_scope_or_date_range_fails_before_provider(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    calls = 0

    def provider(**_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("provider should not be called")

    monkeypatch.setattr(subject, "fetch_index_weights_with_report", provider)
    result = _run(
        subject.INDEX_COMPONENT_INGEST,
        settings,
        {"index_codes": "000300.SH", "start_date": "2024-02-01", "end_date": "2024-01-01"},
    )
    assert result.status is ResultStatus.FAILED
    assert "INVALID_DATE_RANGE" in _diagnostics(result)
    assert calls == 0

    monkeypatch.setattr(subject, "fetch_industry_membership_with_report", provider)
    result = _run(subject.INDUSTRY_MEMBERSHIP_INGEST, settings, {})
    assert result.status is ResultStatus.FAILED
    assert calls == 0


def test_provider_missing_field_or_out_of_range_row_fails_closed(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    missing = _index_frame().drop(columns=["weight"])
    monkeypatch.setattr(
        subject,
        "fetch_index_weights_with_report",
        lambda *_args, **_kwargs: (missing, IndexComponentFetchReport(api_requests=1, batches=1, rows_read=2)),
    )
    params = {"index_codes": "000300.SH", "start_date": "2024-01-01", "end_date": "2024-01-31"}
    result = _run(subject.INDEX_COMPONENT_INGEST, settings, params)
    assert result.status is ResultStatus.FAILED
    assert "INDEX_PROVIDER_SCHEMA_MISSING" in _diagnostics(result)
    assert _count(settings, "index_component_history") == 0

    out_of_range = _index_frame().copy()
    out_of_range.loc[0, "trade_date"] = "20240201"
    monkeypatch.setattr(
        subject,
        "fetch_index_weights_with_report",
        lambda *_args, **_kwargs: (out_of_range, IndexComponentFetchReport(api_requests=1, batches=1, rows_read=2)),
    )
    result = _run(subject.INDEX_COMPONENT_INGEST, settings, params)
    assert result.status is ResultStatus.FAILED
    assert "INDEX_PROVIDER_SCOPE_MISMATCH" in _diagnostics(result)
    assert _count(settings, "index_component_history") == 0


def test_network_failure_does_not_commit_previous_scope_rows(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)

    def provider(**_kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(subject, "fetch_industry_membership_with_report", provider)
    result = _run(
        subject.INDUSTRY_MEMBERSHIP_INGEST,
        settings,
        {"tickers": "300750.SZ,600519.SH"},
    )
    assert result.status is ResultStatus.FAILED
    assert "INDUSTRY_PROVIDER_REQUEST_FAILED" in _diagnostics(result)
    assert _count(settings, "industry_membership_history") == 0


def test_cancel_after_provider_response_does_not_start_write(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    control = ExecutionControl()
    write_calls = 0

    def provider(*, execution_control, **_kwargs):
        assert execution_control is control
        execution_control.cancel("provider response completed")
        return _industry_frame(), IndustryMembershipFetchReport(api_requests=1, batches=1, rows_read=1)

    def append_spy(*_args, **_kwargs):
        nonlocal write_calls
        write_calls += 1
        raise AssertionError("write must not start after cancellation")

    monkeypatch.setattr(subject, "fetch_industry_membership_with_report", provider)
    monkeypatch.setattr(subject, "_append_rows", append_spy)
    result = _run(
        subject.INDUSTRY_MEMBERSHIP_INGEST,
        settings,
        {"tickers": "300750.SZ"},
        control=control,
    )
    assert result.status is ResultStatus.FAILED
    assert write_calls == 0
    assert _count(settings, "industry_membership_history") == 0


def test_deadline_before_provider_does_not_request_or_write(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    calls = 0

    def provider(**_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("provider should not be called after deadline")

    monkeypatch.setattr(subject, "fetch_index_weights_with_report", provider)
    control = ExecutionControl(deadline=datetime.now(UTC) - timedelta(seconds=1))
    result = _run(
        subject.INDEX_COMPONENT_INGEST,
        settings,
        {"index_codes": "000300.SH", "start_date": "2024-01-01", "end_date": "2024-01-31"},
        control=control,
    )
    assert result.status is ResultStatus.FAILED
    assert calls == 0
    assert _count(settings, "index_component_history") == 0


def test_control_is_checked_before_next_industry_scope() -> None:
    calls: list[dict] = []

    class Client:
        def index_member_all(self, **kwargs):
            calls.append(kwargs)
            control.cancel("stop before next scope")
            return _industry_frame()

    control = ExecutionControl()
    with pytest.raises(ExecutionControlError, match="stop before next scope"):
        fetch_industry_membership_with_report(
            tickers=("300750.SZ", "600519.SH"),
            client=Client(),
            execution_control=control,
        )
    assert calls == [{"ts_code": "300750.SZ"}]
