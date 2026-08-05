"""Formal Contract tests for the P5W latest-reply ingestion pipeline."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database, init_irm_database
from qrp_atlas.orchestration.execution_control import ExecutionControl, ExecutionControlError
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import ContractError, ResultStatus
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.testing import ContractTestHarness


fetch_module = importlib.import_module("qrp_atlas.pipeline.irm_qa.fetch")
contract_module = importlib.import_module("qrp_atlas.pipeline.irm_qa_contracts")


def _raw_row(index: int, *, reply_time: str | None = None) -> dict[str, str]:
    return {
        "companyShortname": "盛航股份",
        "companyCode": "001205",
        "nickname": "投资者",
        "content": f"问题 {index}",
        "replyContent": f"回复 {index}",
        "replyerTimeStr": reply_time or f"2026-07-10 23:43:{index:02d}",
        "questionerTimeStr": "2026-07-10 22:00:00",
        "pid": f"PID{index:03d}",
    }


def _settings(tmp_path: Path) -> AppSettings:
    settings = AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
        },
        project_root=tmp_path / "repo",
    )
    settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.paths.duckdb_path)) as connection:
        init_database(connection)
    settings.paths.irm_qa_duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.paths.irm_qa_duckdb_path)) as connection:
        init_irm_database(connection)
    return settings


def _install_pages(monkeypatch, pages: dict[int, object]) -> list[int]:
    calls: list[int] = []

    def fake_post(page: int, **_kwargs):
        calls.append(page)
        response = pages[page]
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(fetch_module, "_post_page", fake_post)
    monkeypatch.setattr(fetch_module, "p5w_sleep_interval", lambda: 0.0)
    monkeypatch.setattr(fetch_module, "P5W_RETRY_BACKOFF_BASE_SECONDS", 0.0)
    return calls


def _run_contract(settings: AppSettings):
    return ContractTestHarness(
        contract_module.IRM_QA_INCREMENTAL,
        settings,
        scheduled_for=datetime(2026, 7, 10, 12, 0, tzinfo=UTC),
    ).run()


def _count_rows(settings: AppSettings) -> int:
    with duckdb.connect(
        str(settings.paths.irm_qa_duckdb_path), read_only=True
    ) as connection:
        return int(connection.execute("SELECT COUNT(*) FROM irm_interaction_qa").fetchone()[0])


def _diagnostic_codes(result) -> set[str]:
    return {diagnostic.code for diagnostic in result.diagnostics}


def test_irm_contract_is_registered_and_describes_latest_feed() -> None:
    registry = default_registry()
    contracts = validate_contracts(registry.all())
    assert len(contracts) == 28
    contract = registry.get("irm_qa_incremental")
    description = contract.describe()
    assert description["pipeline_id"] == "irm_qa_incremental"
    assert description["execution"]["overlap_policy"] == "FORBID"
    assert description["resource_locks"] == ["irm_qa_writer"]
    assert description["outputs"][0]["unique_key"] == ["pid"]
    assert description["outputs"][0]["physical_resource"] == "irm_qa_db"
    assert description["outputs"][0]["location"] == "settings.paths.irm_qa_duckdb_path"
    assert description["inputs"][0]["target_date_semantics"].startswith("latest provider feed")


def test_irm_contract_does_not_use_quant_db_writer() -> None:
    contract = default_registry().get("irm_qa_incremental")
    assert "quant_db_writer" not in contract.resource_locks
    assert "quant_db" not in {
        output.physical_resource for output in contract.outputs
    }
    assert all(
        output.location != "settings.paths.duckdb_path" for output in contract.outputs
    )


def test_contract_scans_multiple_pages_and_reports_actual_metrics(tmp_path, monkeypatch) -> None:
    page_one = {"success": True, "rows": [_raw_row(index) for index in range(10)]}
    page_two = {"success": True, "rows": [_raw_row(index) for index in range(10, 12)]}
    calls = _install_pages(monkeypatch, {1: page_one, 2: page_two})
    settings = _settings(tmp_path)

    result = _run_contract(settings)

    assert result.status is ResultStatus.SUCCESS
    assert calls == [1, 2]
    assert result.metrics.api_requests == 2
    assert result.metrics.batches == 2
    assert result.metrics.rows_read == 12
    assert result.metrics.rows_written == 12
    assert result.outputs[0].detail["rows_deduplicated"] == 12
    assert result.outputs[0].detail["stop_reason"] == "short_page"
    assert _count_rows(settings) == 12


def test_empty_latest_feed_is_successful_zero_write(tmp_path, monkeypatch) -> None:
    _install_pages(monkeypatch, {1: {"success": True, "rows": []}})
    settings = _settings(tmp_path)

    result = _run_contract(settings)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_read == 0
    assert result.metrics.rows_written == 0
    assert result.outputs[0].rows_written == 0
    assert result.outputs[0].detail["stop_reason"] == "empty_page"
    assert _count_rows(settings) == 0


def test_full_repeated_page_is_a_proven_termination_boundary(tmp_path, monkeypatch) -> None:
    page = {"success": True, "rows": [_raw_row(index) for index in range(10)]}
    calls = _install_pages(monkeypatch, {1: page, 2: page})
    settings = _settings(tmp_path)

    result = _run_contract(settings)

    assert result.status is ResultStatus.SUCCESS
    assert calls == [1, 2]
    assert result.metrics.rows_read == 20
    assert result.metrics.rows_written == 10
    assert result.outputs[0].detail["rows_deduplicated"] == 10
    assert result.outputs[0].detail["stop_reason"] == "full_page_overlap"
    assert _count_rows(settings) == 10


def test_repeated_scan_is_pid_idempotent_and_reports_zero_insertions(tmp_path, monkeypatch) -> None:
    page = {"success": True, "rows": [_raw_row(1)]}
    _install_pages(monkeypatch, {1: page})
    settings = _settings(tmp_path)

    first = _run_contract(settings)
    second = _run_contract(settings)

    assert first.metrics.rows_written == 1
    assert second.status is ResultStatus.SUCCESS
    assert second.metrics.rows_read == 1
    assert second.metrics.rows_written == 0
    assert second.outputs[0].rows_written == 0
    assert _count_rows(settings) == 1


def test_partial_page_overlap_fails_closed_without_writing(tmp_path, monkeypatch) -> None:
    page_one = {"success": True, "rows": [_raw_row(index) for index in range(10)]}
    page_two = {"success": True, "rows": [_raw_row(index) for index in range(9, 19)]}
    calls = _install_pages(monkeypatch, {1: page_one, 2: page_two})
    settings = _settings(tmp_path)

    result = _run_contract(settings)

    assert result.status is ResultStatus.FAILED
    assert "IRM_PROVIDER_PARTIAL_PAGE_OVERLAP" in _diagnostic_codes(result)
    assert calls == [1, 2]
    assert _count_rows(settings) == 0


def test_missing_provider_field_fails_closed(tmp_path, monkeypatch) -> None:
    row = _raw_row(1)
    row.pop("replyContent")
    calls = _install_pages(monkeypatch, {1: {"success": True, "rows": [row]}})
    settings = _settings(tmp_path)

    result = _run_contract(settings)

    assert result.status is ResultStatus.FAILED
    assert "IRM_PROVIDER_SCHEMA_MISSING" in _diagnostic_codes(result)
    assert calls == [1]
    assert _count_rows(settings) == 0


def test_provider_failure_retries_then_fails_closed(tmp_path, monkeypatch) -> None:
    page_one = {"success": True, "rows": [_raw_row(index) for index in range(10)]}
    calls = _install_pages(monkeypatch, {1: page_one, 2: OSError("network down")})
    settings = _settings(tmp_path)

    result = _run_contract(settings)

    assert result.status is ResultStatus.FAILED
    assert "IRM_PROVIDER_REQUEST_FAILED" in _diagnostic_codes(result)
    assert calls == [1, 2, 2, 2]
    assert _count_rows(settings) == 0


def test_page_limit_without_boundary_fails_closed(monkeypatch) -> None:
    page = {"success": True, "rows": [_raw_row(index) for index in range(10)]}
    calls = _install_pages(monkeypatch, {1: page})

    with pytest.raises(ContractError, match="IRM_PROVIDER_PAGE_LIMIT") as exc_info:
        fetch_module.fetch_interaction_qa_with_report(max_pages=1)

    assert exc_info.value.code == "IRM_PROVIDER_PAGE_LIMIT"
    assert calls == [1]


def test_execution_control_is_checked_before_next_page(monkeypatch) -> None:
    control = ExecutionControl()
    page = {"success": True, "rows": [_raw_row(index) for index in range(10)]}
    calls: list[int] = []

    def fake_post(page_number: int, **_kwargs):
        calls.append(page_number)
        control.cancel("test cancellation")
        return page

    monkeypatch.setattr(fetch_module, "_post_page", fake_post)

    with pytest.raises(ExecutionControlError, match="test cancellation"):
        fetch_module.fetch_interaction_qa_with_report(
            execution_control=control,
            max_pages=2,
        )

    assert calls == [1]


def test_cancelled_after_provider_response_does_not_enter_write_transaction(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    write_calls = 0
    seen_controls = []

    def fake_fetch(*, execution_control, **_kwargs):
        seen_controls.append(execution_control)
        execution_control.cancel("provider response completed")
        return [_raw_row(1)], fetch_module.InteractionQAFetchReport(
            api_requests=1,
            pages_fetched=1,
            rows_read=1,
            unique_rows=1,
            stop_reason="short_page",
        )

    def append_spy(*_args, **_kwargs):
        nonlocal write_calls
        write_calls += 1
        raise AssertionError("write transaction must not start after cancellation")

    monkeypatch.setattr(contract_module, "fetch_interaction_qa_with_report", fake_fetch)
    monkeypatch.setattr(contract_module, "append_interaction_qa", append_spy)

    result = _run_contract(settings)

    assert result.status is ResultStatus.FAILED
    assert len(seen_controls) == 1
    assert write_calls == 0
    assert _count_rows(settings) == 0


def test_explicit_historical_date_is_rejected_for_latest_feed(tmp_path, monkeypatch) -> None:
    calls = _install_pages(monkeypatch, {1: {"success": True, "rows": []}})
    settings = _settings(tmp_path)

    result = ContractTestHarness(
        contract_module.IRM_QA_INCREMENTAL,
        settings,
        scheduled_for=datetime(2026, 7, 10, 12, 0, tzinfo=UTC),
    ).run(trade_date=datetime(2026, 7, 9, tzinfo=UTC).date())

    assert result.status is ResultStatus.FAILED
    assert "TARGET_DATE_OVERRIDE_INVALID" in _diagnostic_codes(result)
    assert calls == []


def test_write_failure_is_failed_and_does_not_report_success(tmp_path, monkeypatch) -> None:
    _install_pages(monkeypatch, {1: {"success": True, "rows": [_raw_row(1)]}})
    settings = _settings(tmp_path)

    def fail_append(*_args, **_kwargs):
        raise RuntimeError("write unavailable")

    monkeypatch.setattr(contract_module, "append_interaction_qa", fail_append)
    result = _run_contract(settings)

    assert result.status is ResultStatus.FAILED
    assert "IRM_WRITE_FAILED" in _diagnostic_codes(result)
    assert _count_rows(settings) == 0


def _main_db_tables(settings: AppSettings) -> set[str]:
    with duckdb.connect(str(settings.paths.duckdb_path), read_only=True) as connection:
        return {row[0] for row in connection.execute("SHOW TABLES").fetchall()}


def test_writes_happen_only_in_dedicated_database(tmp_path, monkeypatch) -> None:
    page = {"success": True, "rows": [_raw_row(index) for index in range(3)]}
    _install_pages(monkeypatch, {1: page})
    settings = _settings(tmp_path)
    main_tables_before = _main_db_tables(settings)

    result = _run_contract(settings)

    assert result.status is ResultStatus.SUCCESS
    assert result.metrics.rows_written == 3
    # 独立库拿到数据
    assert _count_rows(settings) == 3
    # 主库没有被创建 IRM 表、也没有其他表集合变化
    assert _main_db_tables(settings) == main_tables_before
    assert "irm_interaction_qa" not in _main_db_tables(settings)


def test_completion_and_quality_use_dedicated_database(tmp_path, monkeypatch) -> None:
    """主库完全没有 irm_interaction_qa 表时，completion 与 quality check 仍成功，
    证明二者查询的是独立库而非主库。"""
    page = {"success": True, "rows": [_raw_row(1)]}
    _install_pages(monkeypatch, {1: page})
    settings = _settings(tmp_path)
    assert "irm_interaction_qa" not in _main_db_tables(settings)

    result = _run_contract(settings)

    assert result.status is ResultStatus.SUCCESS
    assert result.outputs[0].completed is True
    assert all(check.passed for check in result.completion_checks)
    assert any(
        check.check_id == "irm_unique_key_quality" and check.passed
        for check in result.completion_checks
    )


def test_transaction_failure_leaves_no_partial_rows(tmp_path, monkeypatch) -> None:
    """写入事务失败必须完整回滚，独立库不残留半成品。"""
    _install_pages(monkeypatch, {1: {"success": True, "rows": [_raw_row(1)]}})
    settings = _settings(tmp_path)

    def fail_commit(*_args, **_kwargs):
        raise RuntimeError("commit unavailable")

    real_append = contract_module.append_interaction_qa
    calls: list[str] = []

    def append_with_failing_commit(*args, **kwargs):
        result = real_append(*args, **kwargs)
        calls.append("appended")
        fail_commit()
        return result

    monkeypatch.setattr(contract_module, "append_interaction_qa", append_with_failing_commit)
    result = _run_contract(settings)

    assert result.status is ResultStatus.FAILED
    assert calls == ["appended"]
    assert _count_rows(settings) == 0
