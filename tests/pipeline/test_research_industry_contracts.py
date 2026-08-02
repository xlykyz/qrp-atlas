"""Contract-level acceptance for the industry research-report work package."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.contracts import init_database
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline import research_industry_contracts as subject
from qrp_atlas.pipeline.contract_validation import validate_contracts
from qrp_atlas.pipeline.contracts import PipelineInvocation, ResultStatus
from qrp_atlas.pipeline.execution import execute_pipeline_contract
from qrp_atlas.pipeline.registry import default_registry
from qrp_atlas.pipeline.research_industry.download_pdf import build_pdf_path
from qrp_atlas.pipeline.research_industry.fetch import IndustryReportFetchReport
from qrp_atlas.pipeline.research_industry.fetch_detail import IndustryReportDetailReport


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
    info_code: str = "I-001",
    *,
    publish_date: str = "2024-01-02",
    industry_code: str = "L1-01",
    industry_name: str = "Technology",
    attach_url: str = "",
) -> dict[str, object]:
    return {
        "infoCode": info_code,
        "title": "An industry research report",
        "publishDate": publish_date,
        "market": "",
        "column": "行业研报",
        "reportType": 1,
        "industryCode": industry_code,
        "industryName": industry_name,
        "indvInduCode": industry_code,
        "indvInduName": industry_name,
        "author": [],
        "authorID": [],
        "noticeContent": "report body",
        "attachUrl": attach_url,
    }


def _list_report(records: list[dict[str, object]], *, complete: bool = True) -> IndustryReportFetchReport:
    return IndustryReportFetchReport(
        records=tuple(records),
        pages_fetched=1,
        api_requests=1,
        retries=0,
        failed_pages=() if complete else (2,),
        complete=complete,
        stop_reason="short_page" if complete else "page_2_failed:TimeoutError",
    )


def _detail_report(records: list[dict[str, object]], *, complete: bool = True) -> IndustryReportDetailReport:
    enriched = []
    for record in records:
        item = dict(record)
        item["noticeContent"] = "report body"
        item["attachUrl"] = item.get("attachUrl", "")
        enriched.append(item)
    return IndustryReportDetailReport(
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
        return int(connection.execute("SELECT COUNT(*) FROM research_report_industry").fetchone()[0])
    finally:
        connection.close()


def test_work_package_contract_is_registered_and_describes_non_aggregate_semantics() -> None:
    validate_contracts(subject.RESEARCH_INDUSTRY_CONTRACTS)
    registry = default_registry()
    contract = registry.get("research_industry_report_ingest")
    assert {item.pipeline_id for item in subject.RESEARCH_INDUSTRY_CONTRACTS} == {
        "research_industry_report_ingest"
    }
    assert contract.resource_locks == ("quant_db_writer",)
    assert contract.resource_reads == ()
    assert contract.manual_execution_allowed
    assert {item.name for item in contract.parameters} == {"start_date", "end_date", "incremental"}
    assert "does not aggregate members or prices" in contract.description


def test_explicit_range_writes_database_and_formal_files(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record()]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))

    result = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-01", "end_date": "2024-01-03"},
        run_id="industry-success",
    )

    assert result.status is ResultStatus.SUCCESS
    assert result.target_window.start_date.isoformat() == "2024-01-01"
    assert result.target_window.end_date.isoformat() == "2024-01-03"
    assert _count(settings) == 1
    assert (settings.paths.raw_dir / "research_industry/2024-01-01_2024-01-03.csv").is_file()
    assert (settings.paths.canonical_dir / "research_industry/2024-01-01_2024-01-03.csv").is_file()
    database_output = next(item for item in result.outputs if item.output_id == "research_report_industry")
    assert database_output.detail["industry_count"] == 1
    assert database_output.detail["member_count"] == 0
    assert result.metrics.assets_processed == 1
    assert result.metrics.rows_written == sum(item.rows_written for item in result.outputs)
    assert all(item.passed for item in result.completion_checks)


def test_representative_fifty_report_range_reports_industry_metrics(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [
        _record(
            f"I-{index:03d}",
            industry_code=f"L1-{index % 5:02d}",
            industry_name=f"Industry {index % 5}",
        )
        for index in range(50)
    ]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))

    result = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-fifty",
    )

    assert result.status is ResultStatus.SUCCESS
    assert _count(settings) == 50
    assert result.metrics.rows_read == 50
    assert result.metrics.assets_processed == 5
    assert result.metrics.api_requests == 51


def test_empty_provider_range_is_noop_without_files(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report([]))

    result = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-01", "end_date": "2024-01-03"},
        run_id="industry-empty",
    )

    assert result.status is ResultStatus.NOOP
    assert result.noop_reason == "NO_INDUSTRY_REPORTS_IN_TARGET_RANGE"
    assert result.outputs == ()
    assert not (settings.paths.raw_dir / "research_industry/2024-01-01_2024-01-03.csv").exists()


def test_provider_configuration_failure_happens_before_fetch(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    calls: list[str] = []
    monkeypatch.setattr(subject, "REPORT_API_URL", "")
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: calls.append("called"))

    result = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-config-failure",
    )

    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_INDUSTRY_PROVIDER_CONFIGURATION_MISSING" in {item.code for item in result.diagnostics}
    assert calls == []


def test_missing_classification_is_rejected_before_any_output(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record(industry_code="", industry_name="")]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))

    result = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-no-classification",
    )

    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_INDUSTRY_CLASSIFICATION_MISSING" in {item.code for item in result.diagnostics}
    assert _count(settings) == 0
    assert not (settings.paths.raw_dir / "research_industry/2024-01-02_2024-01-02.csv").exists()


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
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-partial-list",
    )
    assert partial.status is ResultStatus.FAILED
    assert "RESEARCH_INDUSTRY_PROVIDER_PARTIAL" in {item.code for item in partial.diagnostics}
    assert _count(settings) == 0

    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(
        subject,
        "fetch_report_detail_with_report",
        lambda records, **kwargs: _detail_report(list(records), complete=False),
    )
    partial_detail = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-partial-detail",
    )
    assert partial_detail.status is ResultStatus.FAILED
    assert "RESEARCH_INDUSTRY_DETAIL_PARTIAL" in {item.code for item in partial_detail.diagnostics}
    assert _count(settings) == 0


def test_future_record_is_rejected_before_output(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record(publish_date="2024-01-03")]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))

    result = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-future-record",
    )

    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_INDUSTRY_PROVIDER_WRONG_DATE" in {item.code for item in result.diagnostics}
    assert _count(settings) == 0


def test_pdf_is_staged_invalid_existing_is_replaced_and_valid_repeat_reused(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record(attach_url="https://example.test/industry-report.pdf")]
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
    pdf_path = build_pdf_path(
        publish_date=datetime(2024, 1, 2).date(),
        title="An industry research report",
        industry_name="Technology",
        base_dir=settings.paths.research_pdfs_dir / "research_industry",
    )
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path.write_bytes(b"<html>" + b"blocked" * 200)
    parameters = {"start_date": "2024-01-02", "end_date": "2024-01-02"}

    first = _run(subject.RESEARCH_INDUSTRY_REPORT_INGEST, settings, parameters, run_id="industry-pdf-first")
    assert first.status is ResultStatus.SUCCESS
    assert download_calls == ["https://example.test/industry-report.pdf"]
    assert pdf_path.read_bytes().startswith(b"%PDF-")

    second = _run(subject.RESEARCH_INDUSTRY_REPORT_INGEST, settings, parameters, run_id="industry-pdf-second")
    assert second.status is ResultStatus.SUCCESS
    assert download_calls == ["https://example.test/industry-report.pdf"]
    pdf_output = next(item for item in second.outputs if item.output_id == "research_report_industry_pdf")
    assert pdf_output.detail["reused_files"] == 1


def test_html_pdf_response_fails_closed_without_writing(tmp_path, monkeypatch) -> None:
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
    with pytest.raises(subject.ContractError, match="RESEARCH_INDUSTRY_PDF_PROVIDER_FAILED"):
        subject._download_pdf_to_stage("https://example.test/industry-report.pdf", destination, ExecutionControl())
    assert not destination.exists()


def test_write_failure_rolls_back_and_does_not_promote_files(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    _initialise_database(settings)
    raw = [_record()]
    monkeypatch.setattr(subject, "fetch_report_list_with_report", lambda *args, **kwargs: _list_report(raw))
    monkeypatch.setattr(subject, "fetch_report_detail_with_report", lambda records, **kwargs: _detail_report(list(records)))
    monkeypatch.setattr(subject, "load_report", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("db fail")))

    result = _run(
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-write-failure",
    )

    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_INDUSTRY_WRITE_FAILED" in {item.code for item in result.diagnostics}
    assert _count(settings) == 0
    assert not (settings.paths.raw_dir / "research_industry/2024-01-02_2024-01-02.csv").exists()


def test_file_promotion_failure_is_reported_after_database_commit(tmp_path, monkeypatch) -> None:
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
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-file-commit-failure",
    )
    assert result.status is ResultStatus.FAILED
    assert "RESEARCH_INDUSTRY_FILE_COMMIT_FAILED" in {item.code for item in result.diagnostics}
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
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-cancel-before",
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
        subject.RESEARCH_INDUSTRY_REPORT_INGEST,
        settings,
        {"start_date": "2024-01-02", "end_date": "2024-01-02"},
        run_id="industry-cancel-detail",
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
    result = _run(subject.RESEARCH_INDUSTRY_REPORT_INGEST, settings, parameters, run_id="industry-invalid-range")
    assert result.status is ResultStatus.FAILED
    assert "INVALID_DATE_RANGE" in {item.code for item in result.diagnostics}
    assert calls == []
