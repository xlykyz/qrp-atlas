"""Offline acceptance tests for the formal CNINFO Pipeline contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline import cninfo_contracts as subject
from qrp_atlas.pipeline.cninfo import fetch as fetch_module
from qrp_atlas.pipeline.cninfo.fetch import EastmoneyFetchReport
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, ResultStatus
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import default_registry


TARGET = date(2026, 7, 29)
PREVIOUS = date(2026, 7, 28)
SCHEDULED_FOR = datetime(2026, 7, 29, 0, 0, tzinfo=UTC)


def settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
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
            """
            INSERT INTO trading_calendar (trade_date, is_open, year, month, quarter)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (PREVIOUS, True, PREVIOUS.year, PREVIOUS.month, 3),
                (TARGET, True, TARGET.year, TARGET.month, 3),
            ],
        )
    finally:
        connection.close()


def record(target_date: date, *, secu_code: str = "000001.SZ") -> dict[str, str]:
    return {
        "SECUCODE": secu_code,
        "SECURITY_NAME_ABBR": "Sample Security",
        "NOTICE_DATE": f"{target_date.isoformat()} 00:00:00",
        "RECEIVE_START_DATE": f"{target_date.isoformat()} 09:30:00",
        "RECEIVE_WAY_EXPLAIN": "on-site visit",
        "RECEIVE_PLACE": "company meeting room",
        "RECEPTIONIST": "Sample Contact",
        "CONTENT": "Sample research visit content",
        "URL": "https://example.invalid/cninfo/sample",
    }


def report(target_date: date, records: tuple[dict, ...] = (), *, complete: bool = True) -> EastmoneyFetchReport:
    return EastmoneyFetchReport(
        date_str=target_date.isoformat(),
        records=records,
        pages_fetched=1 if complete else 1,
        requests=1,
        retries=0,
        failed_pages=() if complete else (2,),
        complete=complete,
        last_error=None if complete else "TimeoutError",
    )


def run(
    contract,
    item: AppSettings,
    monkeypatch: pytest.MonkeyPatch,
    provider,
    *,
    control: ExecutionControl | None = None,
    trade_date_override: date | None = None,
):
    monkeypatch.setattr(subject, "fetch_from_eastmoney_report", provider)
    execution_control = control or ExecutionControl()
    return execute_pipeline_contract(
        contract,
        PipelineInvocation(
            run_id=f"{contract.pipeline_id}-test",
            pipeline_id=contract.pipeline_id,
            scheduled_for=SCHEDULED_FOR,
            attempt=1,
            settings=item,
            trade_date_override=trade_date_override,
            execution_control=execution_control,
        ),
    )


def row_count(item: AppSettings) -> int:
    connection = duckdb.connect(str(item.paths.duckdb_path), read_only=True)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM cninfo_research_visits").fetchone()[0])
    finally:
        connection.close()


def diagnostic_codes(result) -> set[str]:
    return {diagnostic.code for diagnostic in result.diagnostics}


def test_cninfo_contracts_are_registered_described_and_locked() -> None:
    validate_contracts(subject.CNINFO_CONTRACTS)
    registered = default_registry().all()
    assert {item.pipeline_id for item in subject.CNINFO_CONTRACTS} <= {
        item.pipeline_id for item in registered
    }
    assert {item.pipeline_id for item in subject.CNINFO_CONTRACTS} == {
        "cninfo_main_update",
        "cninfo_incremental_noon",
        "cninfo_incremental_afternoon",
    }
    assert all(item.resource_locks == ("quant_db_writer",) for item in subject.CNINFO_CONTRACTS)
    assert subject.CNINFO_MAIN_UPDATE.describe()["target_date_policy"]["policy_id"] == (
        "cninfo_main_previous_through_scheduled_date"
    )
    assert subject.CNINFO_INCREMENTAL_NOON.describe()["target_date_policy"]["policy_id"] == (
        "cninfo_incremental_scheduled_local_date"
    )


def test_main_resolves_previous_open_date_through_scheduled_date_and_incremental_is_one_date(
    tmp_path: Path,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    invocation = PipelineInvocation(
        run_id="date-policy-test",
        pipeline_id=subject.CNINFO_MAIN_UPDATE.pipeline_id,
        scheduled_for=SCHEDULED_FOR,
        attempt=1,
        settings=item,
    )
    assert subject.CNINFO_MAIN_TARGET_DATE_POLICY.resolver(invocation).as_dict() == {
        "target_date": None,
        "start_date": PREVIOUS.isoformat(),
        "end_date": TARGET.isoformat(),
    }
    incremental = subject.CNINFO_INCREMENTAL_TARGET_DATE_POLICY.resolver(invocation)
    assert incremental.target_date == TARGET
    assert subject.CNINFO_MAIN_TARGET_DATE_POLICY.validate_explicit_date(TARGET, invocation)
    assert not subject.CNINFO_MAIN_TARGET_DATE_POLICY.validate_explicit_date(date(2026, 8, 1), invocation)
    assert subject.CNINFO_INCREMENTAL_TARGET_DATE_POLICY.validate_explicit_date(date(2026, 8, 1), invocation)


def test_main_fetches_both_dates_and_uses_the_same_execution_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    calls: list[tuple[date, ExecutionControl]] = []

    def provider(date_text: str, *, execution_control: ExecutionControl):
        target_date = date.fromisoformat(date_text)
        calls.append((target_date, execution_control))
        return report(target_date, (record(target_date),))

    control = ExecutionControl()
    result = run(subject.CNINFO_MAIN_UPDATE, item, monkeypatch, provider, control=control)

    assert result.status is ResultStatus.SUCCESS
    assert [item[0] for item in calls] == [PREVIOUS, TARGET]
    assert all(item[1] is control for item in calls)
    assert result.metrics.rows_read == 2
    assert result.metrics.rows_written == 2
    assert row_count(item) == 2


def test_incremental_empty_snapshot_is_success_and_writes_no_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)

    def provider(date_text: str, *, execution_control: ExecutionControl):
        execution_control.check()
        return report(date.fromisoformat(date_text))

    result = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_read == 0
    assert result.metrics.rows_written == 0
    assert result.outputs[0].detail["empty_snapshot"] is True
    assert row_count(item) == 0


def test_duplicate_provider_rows_and_same_day_rerun_are_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    duplicate = record(TARGET)

    def provider(date_text: str, *, execution_control: ExecutionControl):
        execution_control.check()
        return report(date.fromisoformat(date_text), (duplicate.copy(), duplicate.copy()))

    first = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider)
    second = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider)

    assert first.status is ResultStatus.SUCCESS
    assert first.metrics.rows_read == 2
    assert first.metrics.rows_written == 1
    assert second.status is ResultStatus.SUCCESS
    assert second.metrics.rows_written == 0
    assert row_count(item) == 1


@pytest.mark.parametrize(
    ("case", "expected_code"),
    (
        ("missing", "CNINFO_PROVIDER_SCHEMA_MISSING"),
        ("wrong_date", "CNINFO_PROVIDER_WRONG_DATE"),
        ("partial", "CNINFO_PROVIDER_PARTIAL"),
        ("exception", "CNINFO_PROVIDER_ERROR"),
    ),
)
def test_provider_validation_failures_do_not_open_a_write_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_code: str,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    calls = 0

    def provider(date_text: str, *, execution_control: ExecutionControl):
        nonlocal calls
        calls += 1
        target_date = date.fromisoformat(date_text)
        execution_control.check()
        if case == "missing":
            value = record(target_date)
            del value["CONTENT"]
            return report(target_date, (value,))
        if case == "wrong_date":
            return report(target_date, (record(PREVIOUS),))
        if case == "partial":
            return report(target_date, (record(target_date),), complete=False)
        raise RuntimeError("provider unavailable")

    result = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider)

    assert result.status is ResultStatus.FAILED
    assert expected_code in diagnostic_codes(result)
    assert calls == 1
    assert row_count(item) == 0


def test_transaction_failure_rolls_back_rows_inserted_before_the_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)

    def provider(date_text: str, *, execution_control: ExecutionControl):
        target_date = date.fromisoformat(date_text)
        execution_control.check()
        return report(target_date, (record(target_date),))

    real_insert = subject._insert_cninfo_rows

    def insert_then_fail(connection, rows):
        real_insert(connection, rows)
        raise RuntimeError("transaction failure after insert")

    monkeypatch.setattr(subject, "_insert_cninfo_rows", insert_then_fail)
    result = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider)

    assert result.status is ResultStatus.FAILED
    assert "CNINFO_WRITE_FAILED" in diagnostic_codes(result)
    assert row_count(item) == 0


def test_cancelled_before_provider_does_not_request_or_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    calls = 0

    def provider(date_text: str, *, execution_control: ExecutionControl):
        nonlocal calls
        calls += 1
        return report(date.fromisoformat(date_text), (record(TARGET),))

    control = ExecutionControl()
    control.cancel("test cancellation before provider")
    result = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider, control=control)

    assert result.status is ResultStatus.FAILED
    assert calls == 0
    assert row_count(item) == 0


def test_deadline_before_provider_does_not_request_or_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    calls = 0

    def provider(date_text: str, *, execution_control: ExecutionControl):
        nonlocal calls
        calls += 1
        return report(date.fromisoformat(date_text), (record(TARGET),))

    control = ExecutionControl(deadline=datetime.now(UTC) - timedelta(seconds=1))
    result = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider, control=control)

    assert result.status is ResultStatus.FAILED
    assert calls == 0
    assert row_count(item) == 0


def test_cancelled_after_provider_response_does_not_enter_write_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = settings(tmp_path)
    initialise_database(item)
    control = ExecutionControl()
    write_calls = 0

    def provider(date_text: str, *, execution_control: ExecutionControl):
        assert execution_control is control
        execution_control.cancel("response completed after cancellation")
        return report(date.fromisoformat(date_text), (record(TARGET),))

    real_append = subject._append_cninfo_rows

    def append_spy(context, rows):
        nonlocal write_calls
        write_calls += 1
        return real_append(context, rows)

    monkeypatch.setattr(subject, "_append_cninfo_rows", append_spy)
    result = run(subject.CNINFO_INCREMENTAL_NOON, item, monkeypatch, provider, control=control)

    assert result.status is ResultStatus.FAILED
    assert write_calls == 0
    assert row_count(item) == 0


class _FakeResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def test_eastmoney_fetch_report_requires_a_complete_short_page(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[int, float]] = []

    def open_url(request, *, timeout):
        page = int(parse_qs(urlparse(request.full_url).query)["pageNumber"][0])
        calls.append((page, timeout))
        if page == 1:
            rows = [{"SECUCODE": f"000{index:03d}.SZ"} for index in range(50)]
        else:
            rows = [{"SECUCODE": "000050.SZ"}]
        return _FakeResponse({"success": True, "result": {"data": rows}})

    monkeypatch.setattr(fetch_module.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(fetch_module, "eastmoney_sleep_interval", lambda: 0.0)
    target_report = fetch_module.fetch_from_eastmoney_report(TARGET.isoformat())

    assert target_report.complete is True
    assert target_report.pages_fetched == 2
    assert target_report.requests == 2
    assert len(target_report.records) == 51
    assert calls == [(1, 15.0), (2, 15.0)]


def test_eastmoney_fetch_report_marks_failed_pages_as_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    def open_url(request, *, timeout):
        page = int(parse_qs(urlparse(request.full_url).query)["pageNumber"][0])
        if page == 1:
            rows = [{"SECUCODE": f"000{index:03d}.SZ"} for index in range(50)]
            return _FakeResponse({"success": True, "result": {"data": rows}})
        raise TimeoutError("provider timeout")

    monkeypatch.setattr(fetch_module.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(fetch_module, "_controlled_wait", lambda *_args: None)
    target_report = fetch_module.fetch_from_eastmoney_report(TARGET.isoformat())

    assert target_report.complete is False
    assert target_report.pages_fetched == 1
    assert target_report.failed_pages == (2, 3, 4, 5, 6)
    assert target_report.requests == 16
