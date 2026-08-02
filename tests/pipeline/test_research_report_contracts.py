"""Contract-level acceptance for the stock research-report work package."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, ResultStatus
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.research_report.fetch import ResearchReportFetchReport
from qrp_atlas.pipeline.research_report.fetch_detail import ResearchReportDetailReport
from qrp_atlas.pipeline.research_report.download_pdf import build_pdf_path
from qrp_atlas.pipeline import research_report_contracts as subject


SCHEDULED_FOR = datetime(2024, 1, 2, 3, 0, tzinfo=UTC)


def _settings(tmp_path: Path) -> AppSettings:
    return AppSettings.load(
        environ={
            "QRP_HOME": str(tmp_path / "home"),
            "QRP_DATA_DIR": str(tmp_path / "data"),
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


def _record(
    info_code: str = "R-001",
    *,
    publish_date: str = "2024-01-02",
    attach_url: str = "",
) -> dict[str, object]:
    return {
        "infoCode": info_code,
        "title": "A stock research report",
        "stockCode": "000001",
        "stockName": "Test Stock",
        "publishDate": publish_date,
        "market": "SZ",
        "column": "个股研报",
        "reportType": 1,
        "encodeUrl": f"encoded-{info_code}",
        "author": [],
        "authorID": [],
        "noticeContent": "report body",
        "attachUrl": attach_url,
    }


def _list_report(records: list[dict[str, object]], *, complete: bool = True) -> ResearchReportFetchReport:
    return ResearchReportFetchReport(
        records=tuple(records),
        pages_fetched=1,
        api_requests=1,
        retries=0,
        failed_pages=() if complete else (2,),
        complete=complete,
        stop_reason="short_page" if complete else "page_2_failed:TimeoutError",
    )


def _detail_report(records: list[dict[str, object]], *, complete: bool = True) -> ResearchReportDetailReport:
    enriched = []
    for record in records:
        item = dict(record)
        item["noticeContent"] = "report body"
        item["attachUrl"] = item.get("attachUrl", "")
        enriched.append(item)
    return ResearchReportDetailReport(
        records=tuple(enriched),
        requests=len(records),
        retries=0,
        failed_indices=() if complete else (0,),
        complete=complete,
    )


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


def _count(settings: AppSettings) -> int:
    connection = duckdb.connect(str(settings.paths.duckdb_path), read_only=True)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM research_report_stock").fetchone()[0])
    finally:
        connection.close()


def test_work_package_contract_is_registered_and_valid() -> None:
    validate_contracts(subject.RESEARCH_REPORT_CONTRACTS)
    registry = default_registry()
    assert subject.RESEARCH_STOCK_REPORT_INGEST.pipeline_id in {item.pipeline_id for item in registry.all()}
    assert subject.RESEARCH_STOCK_REPORT_INGEST.resource_locks == ("quant_db_writer",)
    assert subject.RESEARCH_STOCK_REPORT_INGEST.manual_execution_allowed
    assert {item.pipeline_id for item in subject.RESEARCH_REPORT_CONTRACTS} == {
        "research_stock_report_ingest"
    }
    assert {item.name for item in subject.RESEARCH_STOCK_REPORT_INGEST.parameters} == {
        "start_date",
        "end_date",
        "incremental",
    }


def test_explicit_range_writes_database_and_csv_outputs(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record()]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))

    result = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-01", "end_date": "2024-01-03"},
        run_id="range-success",
    )

    assert result.status is ResultStatus.SUCCESS
    assert result.target_window.start_date.isoformat() == "2024-01-01"
    assert result.target_window.end_date.isoformat() == "2024-01-03"
    assert _count(settings) == 1
    assert (settings.paths.raw_dir / "research_report/2024-01-01_2024-01-03.csv").is_file()
    assert (settings.paths.canonical_dir / "research_report/2024-01-01_2024-01-03.csv").is_file()
    assert result.metrics.rows_written == sum(item.rows_written for item in result.outputs)
    assert {item.output_id for item in result.outputs} == {
        "research_report_stock",
        "research_report_stock_raw_csv",
        "research_report_stock_canonical_csv",
        "research_report_stock_pdf",
    }
    assert all(item.passed for item in result.completion_checks)


def test_empty_provider_range_is_a_noop_without_files(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report([]))

    result = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-01", "end_date": "2024-01-03"},
        run_id="range-empty",
    )

    assert result.status is ResultStatus.NOOP
    assert result.noop_reason == "NO_REPORTS_IN_TARGET_RANGE"
    assert result.outputs == ()
    assert not (settings.paths.raw_dir / "research_report/2024-01-01_2024-01-03.csv").exists()


def test_partial_list_or_detail_response_fails_before_any_output(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record()]
    monkeypatch.setattr(
        subject,
        "fetch_report_list_with_report",
        lambda *args, **kwargs: _list_report(raw, complete=False),
    )
    partial = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="partial-list",
    )
    assert partial.status is ResultStatus.FAILED
    assert "RESEARCH_REPORT_PROVIDER_PARTIAL" in {item.code for item in partial.diagnostics}
    assert _count(settings) == 0

    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(
        subject,
        "fetch_report_detail_with_report",
        lambda records, **kwargs: _detail_report(list(records), complete=False),
    )
    partial_detail = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="partial-detail",
    )
    assert partial_detail.status is ResultStatus.FAILED
    assert "RESEARCH_REPORT_DETAIL_PARTIAL" in {item.code for item in partial_detail.diagnostics}
    assert _count(settings) == 0


def test_pdf_is_staged_and_reused_on_repeat(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record(attach_url="https://example.test/report.pdf")]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))
    download_calls: list[str] = []

    def download(url, destination, control):
        download_calls.append(url)
        control.check()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"%PDF-1.7\n" + b"x" * 1200)
        control.check()

    monkeypatch.setattr(subject, "_download_pdf_to_stage", download)
    parameters = {"start_date": "2024-01-02", "end_date": "2024-01-02"}
    invalid_existing = build_pdf_path(
        publish_date=datetime(2024, 1, 2).date(),
        title="A stock research report",
        stock_name="Test Stock",
        stock_code="000001",
        base_dir=settings.paths.research_pdfs_dir / "research_report",
    )
    invalid_existing.parent.mkdir(parents=True, exist_ok=True)
    invalid_existing.write_bytes(b"<html>" + b"blocked" * 200)
    first = _run(subject.RESEARCH_STOCK_REPORT_INGEST, settings, parameters, run_id="pdf-first")
    assert first.status is ResultStatus.SUCCESS
    assert download_calls == ["https://example.test/report.pdf"]
    pdf_output = next(item for item in first.outputs if item.output_id == "research_report_stock_pdf")
    assert pdf_output.detail["downloaded_files"] == 1
    pdf_path = invalid_existing
    assert pdf_path.is_file()
    assert pdf_path.read_bytes().startswith(b"%PDF-")

    second = _run(subject.RESEARCH_STOCK_REPORT_INGEST, settings, parameters, run_id="pdf-second")
    assert second.status is ResultStatus.SUCCESS
    assert download_calls == ["https://example.test/report.pdf"]
    second_pdf_output = next(item for item in second.outputs if item.output_id == "research_report_stock_pdf")
    assert second_pdf_output.detail["reused_files"] == 1


def test_html_pdf_response_fails_closed_before_database_commit(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    monkeypatch.setattr(subject, "PDF_MAX_RETRIES", 0)

    class HtmlResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self):
            return b"<html><body>blocked</body></html>" + b"x" * 1200

    monkeypatch.setattr(subject.urllib.request, "urlopen", lambda *args, **kwargs: HtmlResponse())
    destination = settings.paths.tmp_dir / "blocked.pdf"

    with pytest.raises(subject.ContractError, match="RESEARCH_REPORT_PDF_PROVIDER_FAILED"):
        subject._download_pdf_to_stage("https://example.test/report.pdf", destination, ExecutionControl())

    assert not destination.exists()


def test_write_failure_rolls_back_and_does_not_promote_files(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record()]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))
    monkeypatch.setattr(subject, "load_report", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("db fail")))

    result = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="write-failure",
    )

    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_REPORT_WRITE_FAILED" in {item.code for item in result.diagnostics}
    assert _count(settings) == 0
    assert not (settings.paths.raw_dir / "research_report/2024-01-02_2024-01-02.csv").exists()


def test_pdf_failure_happens_before_database_commit(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record(attach_url="https://example.test/report.pdf")]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))

    def fail_download(url, destination, control):
        raise subject.ContractError("RESEARCH_REPORT_PDF_PROVIDER_FAILED", "synthetic provider failure")

    monkeypatch.setattr(subject, "_download_pdf_to_stage", fail_download)
    result = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="pdf-failure",
    )
    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_REPORT_PDF_PROVIDER_FAILED" in {item.code for item in result.diagnostics}
    assert _count(settings) == 0
    assert not (settings.paths.raw_dir / "research_report/2024-01-02_2024-01-02.csv").exists()


def test_file_promotion_failure_is_reported_after_idempotent_database_commit(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record()]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))
    original_replace = subject.os.replace

    def fail_csv_promotion(source, destination):
        if str(destination).endswith(".csv"):
            raise OSError("synthetic file commit failure")
        return original_replace(source, destination)

    monkeypatch.setattr(subject.os, "replace", fail_csv_promotion)
    result = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="file-commit-failure",
    )
    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_REPORT_FILE_COMMIT_FAILED" in {item.code for item in result.diagnostics}
    assert _count(settings) == 1


def test_cancellation_before_provider_and_during_detail_stops_without_write(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    control = ExecutionControl()
    control.cancel("test cancellation")
    provider_calls: list[str] = []

    def should_not_fetch(*args, **kwargs):
        provider_calls.append("list")
        raise AssertionError("provider must not be called after cancellation")

    monkeypatch.setattr(subject, "fetch_report_list_with_report", should_not_fetch)
    before = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="cancel-before",
        control=control,
    )
    assert before.status is ResultStatus.FAILED
    assert "EXECUTION_CANCELLED" in {item.code for item in before.diagnostics}
    assert provider_calls == []

    raw = [_record()]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))

    def cancel_in_detail(records, **kwargs):
        kwargs["execution_control"].cancel("during detail")
        kwargs["execution_control"].check()
        raise AssertionError("cancelled detail must not continue")

    monkeypatch.setattr(subject, "fetch_report_detail_with_report", cancel_in_detail)
    during = _run(
        subject.RESEARCH_STOCK_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="cancel-detail",
    )
    assert during.status is ResultStatus.FAILED
    assert "EXECUTION_CANCELLED" in {item.code for item in during.diagnostics}
    assert _count(settings) == 0


@pytest.mark.parametrize(
    "parameters",
    [
        {"start_date": "2024-01-03"},
        {"start_date": "2024-01-03", "end_date": "2024-01-02"},
    ],
)
def test_invalid_date_range_fails_before_provider(tmp_path, monkeypatch, parameters) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    calls: list[str] = []

    def provider(*args, **kwargs):
        calls.append("called")
        return _list_report([])

    monkeypatch.setattr(subject, "fetch_report_list_with_report", provider)
    result = _run(subject.RESEARCH_STOCK_REPORT_INGEST, settings, parameters, run_id="invalid-range")
    assert result.status is ResultStatus.FAILED
    assert "INVALID_DATE_RANGE" in {item.code for item in result.diagnostics}
    assert calls == []
