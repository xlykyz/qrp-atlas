"""Formal Contract for deterministic stock research-report production.

The two historical stock-research Jobs used the same list, detail, database,
CSV, and PDF workflow. Their trigger times remain deployment history; this
module exposes one business Contract with an explicit date range.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
import os
from pathlib import Path
import re
import shutil
import tempfile
import threading
from time import monotonic
from typing import Any
from urllib.parse import urlsplit
import urllib.request
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import RESEARCH_REPORT_STOCK, align_to_schema, quick_validate
from qrp_atlas.orchestration.execution_control import ExecutionControlError
from qrp_atlas.orchestration.models import OverlapPolicy

from .contracts import (
    BusinessExecution,
    CheckResult,
    CompletionContract,
    ContractError,
    ExecutionPolicy,
    FreshnessContract,
    IdempotencyContract,
    InputContract,
    InputKind,
    NonTradingDayPolicy,
    OutputContract,
    OutputResult,
    ParameterContract,
    ParameterType,
    PerformanceBudget,
    PipelineContract,
    PipelineInvocation,
    PipelineKind,
    PipelineMetrics,
    PipelineRunContext,
    PipelineDiagnostic,
    DiagnosticLevel,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .registry import register_pipeline
from .research_report.clean import clean_report, write_canonical_csv, write_raw_csv
from .research_report.config import (
    DETAIL_HEADERS,
    DETAIL_URL_TEMPLATE,
    REPORT_API_URL,
    REPORT_HEADERS,
    REPORT_PAGE_SIZE,
    sleep_interval,
)
from .research_report.download_pdf import build_pdf_path
from .research_report.fetch import (
    ResearchReportFetchReport,
    fetch_report_list_with_report,
)
from .research_report.fetch_detail import (
    ResearchReportDetailReport,
    fetch_report_detail_with_report,
)
from .research_report.load import load_report


REPORT_TIMEZONE = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
QUANT_DB_LOCATION = "settings.paths.duckdb_path"
RAW_LOCATION = "settings.paths.raw_dir/research_report"
CANONICAL_LOCATION = "settings.paths.canonical_dir/research_report"
PDF_LOCATION = "settings.paths.research_pdfs_dir/research_report"
REPORT_TABLE = RESEARCH_REPORT_STOCK.name
PDF_TIMEOUT_SECONDS = 30.0
PDF_MIN_BYTES = 1000
PDF_MAX_RETRIES = 2
STOCK_CODE_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")

REPORT_REQUIRED_PROVIDER_FIELDS: tuple[str, ...] = (
    "infoCode",
    "title",
    "stockCode",
    "stockName",
    "publishDate",
    "encodeUrl",
)
REPORT_DETAIL_PROVIDER_FIELDS: tuple[str, ...] = REPORT_REQUIRED_PROVIDER_FIELDS + (
    "noticeContent",
    "attachUrl",
)


@dataclass(frozen=True, slots=True)
class _FileLayout:
    stage_root: Path
    staged_raw: Path
    staged_canonical: Path
    staged_pdf_root: Path
    final_raw: Path
    final_canonical: Path
    final_pdf_root: Path


@dataclass(frozen=True, slots=True)
class _PdfStageReport:
    expected_files: int
    downloaded_files: int
    reused_files: int
    records_without_url: int
    requests: int
    retries: int


def _check_context(context: PipelineRunContext) -> None:
    try:
        context.execution_control.check()
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc


def _check_invocation(invocation: PipelineInvocation) -> None:
    try:
        invocation.execution_control.check()
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _parse_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text:
        raise ContractError("RESEARCH_REPORT_DATE_INVALID", field_name)
    for candidate in (text[:10], text):
        try:
            return date.fromisoformat(candidate)
        except ValueError:
            pass
    for fmt in ("%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise ContractError("RESEARCH_REPORT_DATE_INVALID", field_name)


def _parameter_date(value: object, name: str) -> date:
    try:
        return _parse_date(value, name)
    except ContractError as exc:
        raise ContractError("INVALID_DATE_RANGE", name) from exc


def _scheduled_local_date(invocation: PipelineInvocation) -> date:
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return invocation.scheduled_for.astimezone(REPORT_TIMEZONE).date()


def _target_window_resolver(invocation: PipelineInvocation) -> TargetWindow:
    _check_invocation(invocation)
    start_value = invocation.parameter_overrides.get("start_date")
    end_value = invocation.parameter_overrides.get("end_date")
    if start_value is None and end_value is None:
        target_date = invocation.trade_date_override or _scheduled_local_date(invocation)
        return TargetWindow.for_date(target_date)
    if start_value is None or end_value is None:
        raise ContractError("INVALID_DATE_RANGE", "start_date/end_date must be provided together")
    start_date = _parameter_date(start_value, "start_date")
    end_date = _parameter_date(end_value, "end_date")
    if start_date > end_date:
        raise ContractError("INVALID_DATE_RANGE", "start_date must not be after end_date")
    return TargetWindow(start_date=start_date, end_date=end_date)


def _explicit_date_validator(target_date: date, invocation: PipelineInvocation) -> bool:
    _check_invocation(invocation)
    return isinstance(target_date, date) and not any(
        invocation.parameter_overrides.get(name) is not None
        for name in ("start_date", "end_date")
    )


def _target_bounds(context: PipelineRunContext) -> tuple[date, date]:
    _check_context(context)
    window = context.target_window
    if window.target_date is not None:
        return window.target_date, window.target_date
    if window.start_date is None or window.end_date is None:
        raise ContractError("INVALID_TARGET_WINDOW")
    return window.start_date, window.end_date


def _target_date_count(context: PipelineRunContext) -> int:
    start_date, end_date = _target_bounds(context)
    return (end_date - start_date).days + 1


def _provider_configuration(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    valid = (
        REPORT_API_URL.startswith(("http://", "https://"))
        and DETAIL_URL_TEMPLATE.startswith(("http://", "https://"))
        and REPORT_PAGE_SIZE > 0
        and bool(REPORT_HEADERS)
        and bool(DETAIL_HEADERS)
        and callable(sleep_interval)
    )
    if not valid:
        return CheckResult.failure(
            "research_report_provider_configuration",
            "RESEARCH_REPORT_PROVIDER_CONFIGURATION_MISSING",
            "Eastmoney stock research-report configuration is incomplete",
        )
    return CheckResult.success(
        "research_report_provider_configuration",
        list_source=REPORT_API_URL,
        detail_source="Eastmoney stock report detail page",
        page_size=REPORT_PAGE_SIZE,
    )


def _provider_freshness(context: PipelineRunContext, check_id: str) -> CheckResult:
    start_date, end_date = _target_bounds(context)
    return CheckResult.success(
        check_id,
        target_start=start_date.isoformat(),
        target_end=end_date.isoformat(),
        date_rule="provider publishDate must fall inside the resolved inclusive range",
        partial_responses="fail closed before any output commit",
    )


def _list_freshness(context: PipelineRunContext) -> CheckResult:
    return _provider_freshness(context, "research_report_list_freshness")


def _detail_freshness(context: PipelineRunContext) -> CheckResult:
    return _provider_freshness(context, "research_report_detail_freshness")


def _pdf_freshness(context: PipelineRunContext) -> CheckResult:
    return _provider_freshness(context, "research_report_pdf_freshness")


def _validate_list_records(
    records: Sequence[Mapping[str, Any]],
    start_date: date,
    end_date: date,
) -> None:
    seen: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ContractError("RESEARCH_REPORT_PROVIDER_SCHEMA_INVALID", f"record {index} is not an object")
        missing = [field for field in REPORT_REQUIRED_PROVIDER_FIELDS if field not in record]
        if missing:
            raise ContractError(
                "RESEARCH_REPORT_PROVIDER_SCHEMA_INVALID",
                f"record {index} missing {','.join(missing)}",
            )
        info_code = _text(record.get("infoCode"))
        if not info_code:
            raise ContractError("RESEARCH_REPORT_PROVIDER_SCHEMA_INVALID", f"record {index} has empty infoCode")
        if info_code in seen:
            raise ContractError("RESEARCH_REPORT_DUPLICATE_KEY", info_code)
        seen.add(info_code)
        publish_date = _parse_date(record.get("publishDate"), f"record[{index}].publishDate")
        if not start_date <= publish_date <= end_date:
            raise ContractError(
                "RESEARCH_REPORT_PROVIDER_WRONG_DATE",
                f"record {index} publishDate {publish_date.isoformat()} is outside target range",
            )
        if not _text(record.get("title")) or not _text(record.get("stockCode")):
            raise ContractError(
                "RESEARCH_REPORT_PROVIDER_SCHEMA_INVALID",
                f"record {index} requires title and stockCode",
            )


def _prepare_rows(cleaned: Sequence[Mapping[str, Any]], start_date: date, end_date: date) -> pd.DataFrame:
    if not cleaned:
        raise ContractError("RESEARCH_REPORT_CLEAN_EMPTY")
    try:
        frame = pd.DataFrame(cleaned)
        frame = align_to_schema(
            frame,
            REPORT_TABLE,
            fill_missing_optional=True,
            drop_extra=True,
        )
        frame = quick_validate(frame, REPORT_TABLE, allow_extra=False)
    except ContractError:
        raise
    except Exception as exc:  # noqa: BLE001 - schema failures use a stable contract code.
        raise ContractError("RESEARCH_REPORT_CLEAN_SCHEMA_INVALID", type(exc).__name__) from exc

    required = ("info_code", "title", "stock_code", "publish_date")
    for field_name in required:
        if frame[field_name].isna().any() or frame[field_name].astype(str).str.strip().eq("").any():
            raise ContractError("RESEARCH_REPORT_CLEAN_SCHEMA_INVALID", field_name)
    if frame["info_code"].duplicated().any():
        raise ContractError("RESEARCH_REPORT_DUPLICATE_KEY")
    publish_dates = pd.to_datetime(frame["publish_date"], errors="coerce").dt.date
    if publish_dates.isna().any():
        raise ContractError("RESEARCH_REPORT_DATE_INVALID", "publish_date")
    if ((publish_dates < start_date) | (publish_dates > end_date)).any():
        raise ContractError("RESEARCH_REPORT_SCOPE_MISMATCH", "publish_date")
    return frame


def _safe_run_suffix(run_id: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("._")
    return value[:80] or "run"


def _file_layout(context: PipelineRunContext) -> _FileLayout:
    start_date, end_date = _target_bounds(context)
    date_tag = f"{start_date.isoformat()}_{end_date.isoformat()}"
    settings = context.settings
    settings.paths.tmp_dir.mkdir(parents=True, exist_ok=True)
    stage_root = Path(
        tempfile.mkdtemp(
            prefix=f"research_report_{_safe_run_suffix(context.run_id)}_",
            dir=str(settings.paths.tmp_dir),
        )
    )
    return _FileLayout(
        stage_root=stage_root,
        staged_raw=stage_root / "raw" / f"{date_tag}.csv",
        staged_canonical=stage_root / "canonical" / f"{date_tag}.csv",
        staged_pdf_root=stage_root / "pdfs",
        final_raw=settings.paths.raw_dir / "research_report" / f"{date_tag}.csv",
        final_canonical=settings.paths.canonical_dir / "research_report" / f"{date_tag}.csv",
        final_pdf_root=settings.paths.research_pdfs_dir / "research_report",
    )


def _stage_csv_outputs(
    raw_records: list[dict[str, Any]],
    cleaned_records: list[dict[str, Any]],
    layout: _FileLayout,
    control,
) -> None:
    control.check()
    write_raw_csv(raw_records, layout.staged_raw)
    control.check()
    write_canonical_csv(cleaned_records, layout.staged_canonical)
    control.check()


def _valid_pdf(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size >= PDF_MIN_BYTES
    except OSError:
        return False


def _download_pdf_to_stage(url: str, destination: Path, control) -> tuple[int, int]:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ContractError("RESEARCH_REPORT_PDF_URL_INVALID")
    last_error: Exception | None = None
    requests = 0
    retries = 0
    for retry_index in range(PDF_MAX_RETRIES + 1):
        control.check()
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Referer": "https://data.eastmoney.com/",
                },
            )
            timeout = control.bounded_timeout(PDF_TIMEOUT_SECONDS)
            if timeout is not None and timeout <= 0:
                control.check()
                raise TimeoutError("research report PDF deadline elapsed")
            requests += 1
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = getattr(response, "status", None)
                if status is not None and status != 200:
                    raise RuntimeError(f"Eastmoney PDF returned HTTP {status}")
                content = response.read()
            control.check()
            if len(content) < PDF_MIN_BYTES:
                raise ValueError("research report PDF is too small")
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
            temporary.write_bytes(content)
            control.check()
            os.replace(temporary, destination)
            control.check()
            return requests, retries
        except ExecutionControlError:
            raise
        except ContractError:
            raise
        except Exception as exc:  # noqa: BLE001 - retry bounded provider failures.
            last_error = exc
            if retry_index < PDF_MAX_RETRIES:
                retries += 1
                control.wait(threading.Event(), float(3**retry_index))
    raise ContractError(
        "RESEARCH_REPORT_PDF_PROVIDER_FAILED",
        type(last_error).__name__ if last_error is not None else "unknown",
    )


def _stage_pdf_outputs(
    cleaned_records: Sequence[Mapping[str, Any]],
    layout: _FileLayout,
    control,
) -> _PdfStageReport:
    expected_files = 0
    downloaded_files = 0
    reused_files = 0
    records_without_url = 0
    requests = 0
    retries = 0
    seen_paths: dict[Path, str] = {}

    for index, record in enumerate(cleaned_records):
        control.check()
        attach_url = _text(record.get("attach_url"))
        if not attach_url:
            records_without_url += 1
            continue
        publish_date = _parse_date(record.get("publish_date"), f"record[{index}].publish_date")
        stock_code = _text(record.get("stock_code"))
        if not STOCK_CODE_PATTERN.fullmatch(stock_code):
            raise ContractError("RESEARCH_REPORT_PDF_PATH_INVALID", f"record {index} stock_code")
        final_path = build_pdf_path(
            publish_date=publish_date,
            title=_text(record.get("title")),
            stock_name=_text(record.get("stock_name")),
            stock_code=stock_code,
            base_dir=layout.final_pdf_root,
        )
        try:
            relative_path = final_path.relative_to(layout.final_pdf_root)
        except ValueError as exc:
            raise ContractError("RESEARCH_REPORT_PDF_PATH_INVALID") from exc
        previous = seen_paths.get(relative_path)
        info_code = _text(record.get("info_code"))
        if previous is not None and previous != info_code:
            raise ContractError("RESEARCH_REPORT_PDF_PATH_COLLISION", str(relative_path))
        seen_paths[relative_path] = info_code
        expected_files += 1
        if _valid_pdf(final_path):
            reused_files += 1
            continue
        stage_path = layout.staged_pdf_root / relative_path
        download_stats = _download_pdf_to_stage(attach_url, stage_path, control)
        if download_stats is None:
            download_stats = (1, 0)
        requests += download_stats[0]
        downloaded_files += 1
        retries += download_stats[1]

    return _PdfStageReport(
        expected_files=expected_files,
        downloaded_files=downloaded_files,
        reused_files=reused_files,
        records_without_url=records_without_url,
        requests=requests,
        retries=retries,
    )


def _promote_files(layout: _FileLayout, control) -> None:
    for staged, final in (
        (layout.staged_raw, layout.final_raw),
        (layout.staged_canonical, layout.final_canonical),
    ):
        control.check()
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, final)
    if layout.staged_pdf_root.exists():
        for staged in sorted(path for path in layout.staged_pdf_root.rglob("*") if path.is_file()):
            control.check()
            relative_path = staged.relative_to(layout.staged_pdf_root)
            final = layout.final_pdf_root / relative_path
            final.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, final)
    control.check()


def _ensure_database_table(connection: duckdb.DuckDBPyConnection) -> None:
    present = connection.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [REPORT_TABLE],
    ).fetchone()
    if present is None:
        raise ContractError("RESEARCH_REPORT_SCHEMA_MISSING", REPORT_TABLE)


def _load_transaction(
    context: PipelineRunContext,
    cleaned_records: list[dict[str, Any]],
    *,
    incremental: bool,
) -> tuple[int, float, int]:
    started = monotonic()
    connection: duckdb.DuckDBPyConnection | None = None
    transaction_open = False
    try:
        _check_context(context)
        if getattr(context.settings.database, "read_only", False):
            raise ContractError("RESEARCH_REPORT_DATABASE_READ_ONLY")
        connection = duckdb.connect(str(context.settings.paths.duckdb_path))
        _ensure_database_table(connection)
        _check_context(context)
        connection.execute("BEGIN TRANSACTION")
        transaction_open = True
        _check_context(context)
        inserted = load_report(connection, cleaned_records, incremental=incremental)
        _check_context(context)
        connection.execute("COMMIT")
        transaction_open = False
        _check_context(context)
        rows_in_target = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {REPORT_TABLE} WHERE publish_date >= ? AND publish_date <= ?",
                [
                    _target_bounds(context)[0],
                    _target_bounds(context)[1],
                ],
            ).fetchone()[0]
        )
        return inserted, monotonic() - started, rows_in_target
    except ExecutionControlError:
        if connection is not None and transaction_open:
            connection.execute("ROLLBACK")
        raise
    except ContractError:
        if connection is not None and transaction_open:
            connection.execute("ROLLBACK")
        raise
    except Exception as exc:  # noqa: BLE001 - all write failures are stable contract failures.
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise ContractError("RESEARCH_REPORT_WRITE_FAILED", type(exc).__name__) from exc
    finally:
        if connection is not None:
            connection.close()


def _execute_research_report(context: PipelineRunContext) -> BusinessExecution:
    started = monotonic()
    start_date, end_date = _target_bounds(context)
    list_report: ResearchReportFetchReport
    detail_report: ResearchReportDetailReport
    layout: _FileLayout | None = None
    try:
        _check_context(context)
        list_report = fetch_report_list_with_report(
            start_date.isoformat(),
            end_date.isoformat(),
            execution_control=context.execution_control,
        )
        _check_context(context)
        if not list_report.complete:
            raise ContractError(
                "RESEARCH_REPORT_PROVIDER_PARTIAL",
                f"{list_report.stop_reason}; failed_pages={list_report.failed_pages}",
            )
        raw_records = [dict(record) for record in list_report.records]
        _validate_list_records(raw_records, start_date, end_date)
        if not raw_records:
            return BusinessExecution.noop(
                "NO_REPORTS_IN_TARGET_RANGE",
                metrics=PipelineMetrics(
                    rows_read=0,
                    rows_written=0,
                    dates_processed=(end_date - start_date).days + 1,
                    api_requests=list_report.api_requests,
                    batches=list_report.pages_fetched,
                    retries=list_report.retries,
                    stage_durations_seconds={"list_fetch": max(0.0, monotonic() - started)},
                ),
            )

        detail_report = fetch_report_detail_with_report(
            raw_records,
            execution_control=context.execution_control,
        )
        _check_context(context)
        if not detail_report.complete or len(detail_report.records) != len(raw_records):
            raise ContractError(
                "RESEARCH_REPORT_DETAIL_PARTIAL",
                f"failed_indices={detail_report.failed_indices}",
            )
        detailed_records = [dict(record) for record in detail_report.records]
        try:
            cleaned_records = clean_report(detailed_records)
        except Exception as exc:  # noqa: BLE001 - cleaner failures are business failures.
            raise ContractError("RESEARCH_REPORT_CLEAN_FAILED", type(exc).__name__) from exc
        _check_context(context)
        prepared = _prepare_rows(cleaned_records, start_date, end_date)
        cleaned_records = prepared.to_dict(orient="records")
        layout = _file_layout(context)
        try:
            _stage_csv_outputs(detailed_records, cleaned_records, layout, context.execution_control)
        except ExecutionControlError:
            raise
        except ContractError:
            raise
        except Exception as exc:  # noqa: BLE001 - staged file failures precede the DB transaction.
            raise ContractError("RESEARCH_REPORT_FILE_STAGE_FAILED", type(exc).__name__) from exc
        pdf_report = _stage_pdf_outputs(cleaned_records, layout, context.execution_control)
        _check_context(context)
        incremental = bool(context.parameter_overrides.get("incremental", True))
        inserted, write_seconds, rows_in_target = _load_transaction(
            context,
            cleaned_records,
            incremental=incremental,
        )
        _check_context(context)
        try:
            _promote_files(layout, context.execution_control)
        except ExecutionControlError:
            raise
        except ContractError:
            raise
        except Exception as exc:  # noqa: BLE001 - DB is committed; retry remains idempotent.
            raise ContractError("RESEARCH_REPORT_FILE_COMMIT_FAILED", type(exc).__name__) from exc
        _check_context(context)
        output_units = inserted + len(cleaned_records) + len(cleaned_records) + pdf_report.expected_files
        diagnostics: tuple[PipelineDiagnostic, ...] = ()
        if pdf_report.records_without_url:
            diagnostics = (
                PipelineDiagnostic(
                    code="RESEARCH_REPORT_PDF_URL_MISSING",
                    level=DiagnosticLevel.WARNING,
                    message="some reports have no PDF attachment URL",
                    detail={"records_without_url": pdf_report.records_without_url},
                ),
            )
        return BusinessExecution.success(
            metrics=PipelineMetrics(
                rows_read=len(raw_records),
                rows_written=output_units,
                assets_processed=int(prepared["stock_code"].nunique()),
                dates_processed=(end_date - start_date).days + 1,
                database_write_seconds=write_seconds,
                stage_durations_seconds={
                    "provider_and_clean": max(0.0, monotonic() - started - write_seconds),
                    "database_write": write_seconds,
                },
                api_requests=list_report.api_requests + detail_report.requests + pdf_report.requests,
                batches=list_report.pages_fetched,
                retries=list_report.retries + detail_report.retries + pdf_report.retries,
            ),
            outputs=(
                OutputResult(
                    output_id=REPORT_TABLE,
                    rows_written=inserted,
                    location=QUANT_DB_LOCATION,
                    completed=True,
                    detail={
                        "target_rows_after_commit": rows_in_target,
                        "rows_received": len(raw_records),
                        "incremental": incremental,
                        "unique_key": list(RESEARCH_REPORT_STOCK.primary_key),
                    },
                ),
                OutputResult(
                    output_id="research_report_stock_raw_csv",
                    rows_written=len(raw_records),
                    location=RAW_LOCATION,
                    completed=True,
                    detail={"path": str(layout.final_raw), "row_count": len(raw_records)},
                ),
                OutputResult(
                    output_id="research_report_stock_canonical_csv",
                    rows_written=len(cleaned_records),
                    location=CANONICAL_LOCATION,
                    completed=True,
                    detail={"path": str(layout.final_canonical), "row_count": len(cleaned_records)},
                ),
                OutputResult(
                    output_id="research_report_stock_pdf",
                    rows_written=pdf_report.expected_files,
                    location=PDF_LOCATION,
                    completed=True,
                    detail={
                        "expected_files": pdf_report.expected_files,
                        "downloaded_files": pdf_report.downloaded_files,
                        "reused_files": pdf_report.reused_files,
                        "records_without_url": pdf_report.records_without_url,
                    },
                ),
            ),
            diagnostics=diagnostics,
        )
    finally:
        if layout is not None:
            shutil.rmtree(layout.stage_root, ignore_errors=True)


def _db_completion(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    start_date, end_date = _target_bounds(context)
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            total = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {REPORT_TABLE} WHERE publish_date >= ? AND publish_date <= ?",
                    [start_date, end_date],
                ).fetchone()[0]
            )
            _check_context(context)
        finally:
            connection.close()
    except ExecutionControlError:
        raise
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "research_report_database_completion",
            "RESEARCH_REPORT_COMPLETION_MISSING",
            "research_report_stock is not queryable after commit",
            exception=type(exc).__name__,
        )
    if total <= 0:
        return CheckResult.failure(
            "research_report_database_completion",
            "RESEARCH_REPORT_COMPLETION_MISSING",
            "target range has no committed research report rows",
            rows=total,
        )
    return CheckResult.success(
        "research_report_database_completion",
        table=REPORT_TABLE,
        target_start=start_date.isoformat(),
        target_end=end_date.isoformat(),
        rows=total,
    )


def _csv_rows(path: Path, required_key: str) -> tuple[int, set[str]]:
    if not path.is_file():
        raise FileNotFoundError(path)
    import csv

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if required_key not in (reader.fieldnames or []):
            raise ValueError(f"CSV missing {required_key}")
        values = [str(row.get(required_key) or "").strip() for row in reader]
    return len(values), set(values)


def _raw_csv_completion(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    layout = _file_layout_for_check(context)
    try:
        rows, keys = _csv_rows(layout.final_raw, "infoCode")
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "research_report_raw_csv_completion",
            "RESEARCH_REPORT_FILE_COMPLETION_MISSING",
            "raw research-report CSV is missing or invalid",
            exception=type(exc).__name__,
        )
    if rows <= 0 or "" in keys:
        return CheckResult.failure(
            "research_report_raw_csv_completion",
            "RESEARCH_REPORT_FILE_COMPLETION_MISSING",
            "raw research-report CSV has no valid rows",
            rows=rows,
        )
    return CheckResult.success("research_report_raw_csv_completion", path=str(layout.final_raw), rows=rows)


def _canonical_csv_completion(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    layout = _file_layout_for_check(context)
    try:
        rows, keys = _csv_rows(layout.final_canonical, "info_code")
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "research_report_canonical_csv_completion",
            "RESEARCH_REPORT_FILE_COMPLETION_MISSING",
            "canonical research-report CSV is missing or invalid",
            exception=type(exc).__name__,
        )
    if rows <= 0 or "" in keys:
        return CheckResult.failure(
            "research_report_canonical_csv_completion",
            "RESEARCH_REPORT_FILE_COMPLETION_MISSING",
            "canonical research-report CSV has no valid rows",
            rows=rows,
        )
    return CheckResult.success(
        "research_report_canonical_csv_completion",
        path=str(layout.final_canonical),
        rows=rows,
    )


def _file_layout_for_check(context: PipelineRunContext) -> _FileLayout:
    start_date, end_date = _target_bounds(context)
    date_tag = f"{start_date.isoformat()}_{end_date.isoformat()}"
    settings = context.settings
    return _FileLayout(
        stage_root=Path("."),
        staged_raw=Path("."),
        staged_canonical=Path("."),
        staged_pdf_root=Path("."),
        final_raw=settings.paths.raw_dir / "research_report" / f"{date_tag}.csv",
        final_canonical=settings.paths.canonical_dir / "research_report" / f"{date_tag}.csv",
        final_pdf_root=settings.paths.research_pdfs_dir / "research_report",
    )


def _pdf_quality(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    layout = _file_layout_for_check(context)
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            rows = connection.execute(
                f"""
                SELECT info_code, title, stock_name, stock_code, attach_url, publish_date
                FROM {REPORT_TABLE}
                WHERE publish_date >= ? AND publish_date <= ?
                  AND attach_url IS NOT NULL AND attach_url != ''
                """,
                list(_target_bounds(context)),
            ).fetchall()
        finally:
            connection.close()
        missing: list[str] = []
        for info_code, title, stock_name, stock_code, _attach_url, publish_date in rows:
            _check_context(context)
            path = build_pdf_path(
                publish_date=publish_date,
                title=_text(title),
                stock_name=_text(stock_name),
                stock_code=_text(stock_code),
                base_dir=layout.final_pdf_root,
            )
            if not _valid_pdf(path):
                missing.append(str(info_code))
        if missing:
            return CheckResult.failure(
                "research_report_pdf_quality",
                "RESEARCH_REPORT_PDF_MISSING",
                "one or more attachment PDFs are missing or incomplete",
                missing_count=len(missing),
            )
        return CheckResult.success(
            "research_report_pdf_quality",
            expected_files=len(rows),
            marker="every persisted attachment URL has a valid target PDF",
        )
    except ExecutionControlError:
        raise
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "research_report_pdf_quality",
            "RESEARCH_REPORT_PDF_CHECK_FAILED",
            "PDF output quality query failed",
            exception=type(exc).__name__,
        )


def _db_quality(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    try:
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            duplicate = connection.execute(
                f"""
                SELECT info_code
                FROM {REPORT_TABLE}
                WHERE publish_date >= ? AND publish_date <= ?
                GROUP BY info_code
                HAVING COUNT(*) > 1
                LIMIT 1
                """,
                list(_target_bounds(context)),
            ).fetchone()
        finally:
            connection.close()
    except ExecutionControlError:
        raise
    except Exception as exc:  # noqa: BLE001
        return CheckResult.failure(
            "research_report_database_quality",
            "RESEARCH_REPORT_QUALITY_CHECK_FAILED",
            "research-report unique-key quality query failed",
            exception=type(exc).__name__,
        )
    if duplicate is not None:
        return CheckResult.failure(
            "research_report_database_quality",
            "RESEARCH_REPORT_DUPLICATE_KEY",
            "research_report_stock contains duplicate info_code values",
        )
    return CheckResult.success(
        "research_report_database_quality",
        unique_key=list(RESEARCH_REPORT_STOCK.primary_key),
    )


def _csv_quality(path_selector: str, key: str, check_id: str) -> Any:
    def check(context: PipelineRunContext) -> CheckResult:
        _check_context(context)
        layout = _file_layout_for_check(context)
        path = layout.final_raw if path_selector == "raw" else layout.final_canonical
        try:
            rows, keys = _csv_rows(path, key)
        except Exception as exc:  # noqa: BLE001
            return CheckResult.failure(
                check_id,
                "RESEARCH_REPORT_FILE_QUALITY_FAILED",
                "research-report CSV quality check failed",
                exception=type(exc).__name__,
            )
        if rows != len(keys) or "" in keys:
            return CheckResult.failure(
                check_id,
                "RESEARCH_REPORT_DUPLICATE_KEY",
                "research-report CSV contains duplicate or empty info_code values",
                rows=rows,
                unique_keys=len(keys),
            )
        return CheckResult.success(check_id, path=str(path), rows=rows, unique_keys=len(keys))

    return check


def _output_contracts() -> tuple[OutputContract, ...]:
    return (
        OutputContract(
            output_id=REPORT_TABLE,
            physical_resource=QUANT_DB_RESOURCE,
            location=QUANT_DB_LOCATION,
            object_name=REPORT_TABLE,
            unique_key=RESEARCH_REPORT_STOCK.primary_key,
            write_mode=WriteMode.UPSERT,
            target_date_semantics="publish_date is inside the resolved inclusive date range",
            completion=CompletionContract(
                marker="research_report_stock rows are queryable after the database transaction commits",
                error_code="RESEARCH_REPORT_COMPLETION_MISSING",
                checker=_db_completion,
            ),
            quality_checks=(_db_quality,),
            allow_empty=True,
        ),
        OutputContract(
            output_id="research_report_stock_raw_csv",
            physical_resource="research_report_files",
            location=RAW_LOCATION,
            object_name="{start_date}_{end_date}.csv",
            unique_key=("target_range",),
            write_mode=WriteMode.REPLACE_TARGET_RANGE,
            target_date_semantics="one raw CSV for the resolved inclusive date range",
            completion=CompletionContract(
                marker="raw CSV exists and contains the target records",
                error_code="RESEARCH_REPORT_FILE_COMPLETION_MISSING",
                checker=_raw_csv_completion,
            ),
            quality_checks=(_csv_quality("raw", "infoCode", "research_report_raw_csv_quality"),),
            allow_empty=True,
        ),
        OutputContract(
            output_id="research_report_stock_canonical_csv",
            physical_resource="research_report_files",
            location=CANONICAL_LOCATION,
            object_name="{start_date}_{end_date}.csv",
            unique_key=("target_range",),
            write_mode=WriteMode.REPLACE_TARGET_RANGE,
            target_date_semantics="one canonical CSV for the resolved inclusive date range",
            completion=CompletionContract(
                marker="canonical CSV exists and contains the target records",
                error_code="RESEARCH_REPORT_FILE_COMPLETION_MISSING",
                checker=_canonical_csv_completion,
            ),
            quality_checks=(_csv_quality("canonical", "info_code", "research_report_canonical_csv_quality"),),
            allow_empty=True,
        ),
        OutputContract(
            output_id="research_report_stock_pdf",
            physical_resource="research_report_files",
            location=PDF_LOCATION,
            object_name="publish-date partitioned PDF files",
            unique_key=("info_code",),
            write_mode=WriteMode.UPSERT,
            target_date_semantics="one PDF for each persisted report attachment URL when available",
            completion=CompletionContract(
                marker="all persisted attachment URLs have valid PDF files",
                error_code="RESEARCH_REPORT_PDF_MISSING",
                checker=_pdf_quality,
            ),
            quality_checks=(_pdf_quality,),
            allow_empty=True,
        ),
    )


def _input_contracts() -> tuple[InputContract, ...]:
    return (
        InputContract(
            input_id="external_eastmoney_research_list",
            kind=InputKind.EXTERNAL_API,
            source=REPORT_API_URL,
            required_fields=REPORT_REQUIRED_PROVIDER_FIELDS,
            target_date_semantics="publishDate must be inside the explicit resolved inclusive range",
            missing_error_code="RESEARCH_REPORT_PROVIDER_CONFIGURATION_MISSING",
            structure_check=_provider_configuration,
            freshness=FreshnessContract(
                check_id="research_report_list_freshness",
                target_date_semantics="complete list pagination is evaluated for the requested range",
                maximum_lag_trading_days=0,
                non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                error_code="RESEARCH_REPORT_PROVIDER_STALE",
                checker=_list_freshness,
            ),
        ),
        InputContract(
            input_id="external_eastmoney_research_detail",
            kind=InputKind.EXTERNAL_API,
            source="Eastmoney stock research-report detail endpoint",
            required_fields=REPORT_DETAIL_PROVIDER_FIELDS,
            target_date_semantics="detail enrichment must succeed for every list record",
            missing_error_code="RESEARCH_REPORT_PROVIDER_CONFIGURATION_MISSING",
            structure_check=_provider_configuration,
            freshness=FreshnessContract(
                check_id="research_report_detail_freshness",
                target_date_semantics="detail pages correspond to the list records in the resolved range",
                maximum_lag_trading_days=0,
                non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                error_code="RESEARCH_REPORT_PROVIDER_STALE",
                checker=_detail_freshness,
            ),
        ),
        InputContract(
            input_id="external_eastmoney_research_pdf",
            kind=InputKind.EXTERNAL_API,
            source="attachUrl returned by Eastmoney stock research-report detail pages",
            required_fields=("attachUrl",),
            target_date_semantics="each available attachment URL is downloaded before database commit",
            missing_error_code="RESEARCH_REPORT_PROVIDER_CONFIGURATION_MISSING",
            structure_check=_provider_configuration,
            freshness=FreshnessContract(
                check_id="research_report_pdf_freshness",
                target_date_semantics="PDF content is validated before it is promoted to the target directory",
                maximum_lag_trading_days=0,
                non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                error_code="RESEARCH_REPORT_PROVIDER_STALE",
                checker=_pdf_freshness,
            ),
        ),
    )


RESEARCH_REPORT_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="research_stock_report_explicit_date_range",
    description=(
        "Use explicit start_date/end_date parameters for a natural-date range; "
        "when omitted, use one Asia/Shanghai scheduled date."
    ),
    trading_calendar_id="none",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_target_window_resolver,
    validate_explicit_date=_explicit_date_validator,
)


RESEARCH_STOCK_REPORT_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="research_stock_report_ingest",
        name="Stock research report ingestion",
        description=(
            "Fetches a complete Eastmoney stock research-report list and detail set for an explicit date range, "
            "stages raw/canonical CSV and PDF outputs, then transactionally upserts research_report_stock."
        ),
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_research_report,
        target_date_policy=RESEARCH_REPORT_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract(
                name="start_date",
                parameter_type=ParameterType.DATE,
                description="Inclusive natural start date for report publication.",
                default=None,
            ),
            ParameterContract(
                name="end_date",
                parameter_type=ParameterType.DATE,
                description="Inclusive natural end date for report publication.",
                default=None,
            ),
            ParameterContract(
                name="incremental",
                parameter_type=ParameterType.BOOLEAN,
                description="Ignore existing info_code rows when true; replace them when false.",
                default=True,
            ),
        ),
        inputs=_input_contracts(),
        outputs=_output_contracts(),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(),
        idempotency=IdempotencyContract(
            idempotency_key="(target_start, target_end, info_code)",
            repeat_run_semantics=(
                "the same range may be fetched repeatedly; database primary keys prevent duplicate rows, "
                "CSV targets are atomically replaced, and existing valid PDFs are reused"
            ),
            existing_target_handling=(
                "incremental mode retains an existing info_code; non-incremental mode replaces its current row"
            ),
            failure_recovery=(
                "provider/detail/PDF failures occur before the database transaction; database failure rolls back; "
                "a file promotion failure is retryable after the committed database upsert"
            ),
            uses_staging=True,
            atomic_replace_boundary=(
                "raw/canonical CSV and downloaded PDFs are written under a run-scoped staging directory and "
                "promoted after the database transaction commits"
            ),
        ),
        transaction=TransactionContract(
            mode=TransactionMode.STAGING_ATOMIC_REPLACE,
            boundary="one prepared research_report_stock upsert transaction plus staged file promotion",
            failure_visibility=(
                "no database rows are visible on provider, validation, PDF, or transaction failure; "
                "if final file promotion fails after commit, retry reuses the idempotent database rows"
            ),
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=600.0,
            warning_threshold_seconds=420.0,
            hard_timeout_seconds=900,
            benchmark_scope=(
                "mocked 50-report range with complete list pagination, 50 detail pages, 50 attachment checks, "
                "two CSV files, and one temporary-DuckDB transaction"
            ),
            baseline_source=(
                "tests/pipeline/test_research_report_contracts.py representative mocked 50-report benchmark; "
                "docs/QRP产品蓝图v1.1/09_Pipeline现状事实与迁移边界.md research-stock-0700/1900 facts; "
                "src/qrp_atlas/pipeline/research_report/config.py request settings"
            ),
        ),
        manual_execution_allowed=True,
    )
)


RESEARCH_REPORT_CONTRACTS: tuple[PipelineContract, ...] = (RESEARCH_STOCK_REPORT_INGEST,)


__all__ = [
    "RESEARCH_REPORT_CONTRACTS",
    "RESEARCH_REPORT_TARGET_DATE_POLICY",
    "RESEARCH_STOCK_REPORT_INGEST",
    "REPORT_REQUIRED_PROVIDER_FIELDS",
]
