"""Formal manual Contracts for PIT, fundamentals, and earnings forecasts.

The module adapts the existing fetch/clean/load implementations.  It does not
turn the historical dated service examples into schedules or introduce a new
revision model: all database writes remain append-only by ``revision_id``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    EARNINGS_FORECAST_EVENT,
    FINANCIAL_INDICATOR,
    INCOME_STATEMENT,
    BALANCE_SHEET,
    CASHFLOW_STATEMENT,
    INDUSTRY_MEMBERSHIP_HISTORY,
    INDEX_COMPONENT_HISTORY,
    REVISION_ID,
    TRADING_CALENDAR,
    align_to_schema,
    get_table,
    init_database,
    quick_validate,
)
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
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .earnings_forecast.clean import clean_earnings_forecast
from .earnings_forecast.fetch import (
    CORE_RAW_FIELDS as FORECAST_CORE_FIELDS,
    ForecastFetchReport,
    fetch_earnings_forecast,
)
from .fundamentals.clean import clean_financial
from .fundamentals.fetch import FinancialFetchReport, fetch_financial
from .pit_backfill.batches import DEFAULT_INDEX_CODES
from .pit_backfill.manifest import (
    BatchRecord,
    ManifestStore,
    STATUS_EMPTY,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    utc_now_iso,
)
from .pit_backfill.raw_io import (
    CorruptParquetError,
    cleaned_file_path,
    load_parquet,
    raw_file_path,
    save_parquet,
)
from .pit_backfill.runner import BackfillConfig, PitBackfillRunner
from .pit_utils import NextTradeDateResolver, append_only_insert, to_date
from .registry import register_pipeline


PIT_TIMEZONE = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
TRADING_CALENDAR_READ = "duckdb://quant_db#trading_calendar"
QUANT_DB_LOCATION = "settings.paths.duckdb_path"
CODE_PATTERN = re.compile(r"^[0-9]{6}\.[A-Z]{2}$")
TAG_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

FINANCIAL_TABLES = (
    INCOME_STATEMENT.name,
    BALANCE_SHEET.name,
    CASHFLOW_STATEMENT.name,
    FINANCIAL_INDICATOR.name,
)
PIT_TABLES = FINANCIAL_TABLES + (
    INDUSTRY_MEMBERSHIP_HISTORY.name,
    INDEX_COMPONENT_HISTORY.name,
)
EARNINGS_TABLE = EARNINGS_FORECAST_EVENT.name
PIT_DATASETS = ("fundamentals", "industry", "index")
PIT_STAGES = ("fetch", "clean", "load")


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


def _scheduled_observation(invocation: PipelineInvocation) -> TargetWindow:
    _check_invocation(invocation)
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(PIT_TIMEZONE).date())


def _reject_explicit_target_date(target_date: date, invocation: PipelineInvocation) -> bool:
    _check_invocation(invocation)
    return False


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _date_value(value: object, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value).replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ContractError("INVALID_SCOPE", field_name)
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError as exc:
        raise ContractError("INVALID_SCOPE", field_name) from exc


def _date_list(value: object, field_name: str) -> tuple[str, ...]:
    raw = _text(value)
    if not raw:
        return ()
    result: list[str] = []
    for item in raw.split(","):
        parsed = _date_value(item, field_name)
        compact = parsed.strftime("%Y%m%d")
        if compact not in result:
            result.append(compact)
    return tuple(result)


def _code_list(value: object, field_name: str, *, required: bool = False) -> tuple[str, ...]:
    raw = _text(value)
    if not raw:
        if required:
            raise ContractError("REQUIRED_PARAMETER_MISSING", field_name)
        return ()
    result: list[str] = []
    for item in raw.split(","):
        code = _text(item).upper()
        if not CODE_PATTERN.fullmatch(code):
            raise ContractError("INVALID_SCOPE", field_name)
        if code not in result:
            result.append(code)
    return tuple(result)


def _tag(value: object, field_name: str) -> str:
    text = _text(value)
    if not TAG_PATTERN.fullmatch(text) or ".." in text:
        raise ContractError("INVALID_SCOPE", field_name)
    return text


def _split(value: object) -> tuple[str, ...]:
    return tuple(item.strip() for item in _text(value).split(",") if item.strip())


def _tables(value: object) -> tuple[str, ...]:
    raw = _text(value).lower()
    if raw in {"", "all"}:
        return FINANCIAL_TABLES
    result: list[str] = []
    for item in raw.split(","):
        table = item.strip()
        if table not in FINANCIAL_TABLES:
            raise ContractError("INVALID_SCOPE", "tables")
        if table not in result:
            result.append(table)
    if not result:
        raise ContractError("INVALID_SCOPE", "tables")
    return tuple(result)


def _pit_scope(context: PipelineRunContext) -> dict[str, Any]:
    _check_context(context)
    values = context.parameter_overrides
    mode = _text(values.get("mode", "full")).lower()
    if mode not in {"full", "plan-only"}:
        raise ContractError("INVALID_PARAMETER", "mode")
    datasets = tuple(item for item in _split(values.get("datasets", "fundamentals")) if item)
    if not datasets or any(item not in PIT_DATASETS for item in datasets):
        raise ContractError("INVALID_SCOPE", "datasets")
    stages = tuple(item for item in _split(values.get("stages", ",".join(PIT_STAGES))) if item)
    if not stages or any(item not in PIT_STAGES for item in stages):
        raise ContractError("INVALID_SCOPE", "stages")
    stages = tuple(item for item in PIT_STAGES if item in stages)
    run_tag = _tag(values.get("run_tag"), "run_tag")
    financial_tables = _tables(values.get("financial_tables", "all"))
    periods = _date_list(values.get("financial_periods", ""), "financial_periods")
    financial_start = values.get("financial_start_date")
    financial_end = values.get("financial_end_date")
    if financial_start is not None or financial_end is not None:
        if financial_start is None or financial_end is None:
            raise ContractError("INVALID_DATE_RANGE", "financial_start_date/end_date")
        financial_start = _date_value(financial_start, "financial_start_date")
        financial_end = _date_value(financial_end, "financial_end_date")
        if financial_start > financial_end:
            raise ContractError("INVALID_DATE_RANGE", "financial_start_date")
    if "fundamentals" in datasets and not periods and financial_start is None:
        raise ContractError("FUNDAMENTALS_SCOPE_REQUIRED", "financial_periods or financial date range")

    l1_codes = _code_list(values.get("l1_codes", ""), "l1_codes", required="industry" in datasets)
    index_codes = _code_list(values.get("index_codes", ""), "index_codes", required="index" in datasets)
    index_start = values.get("index_start_date")
    index_end = values.get("index_end_date")
    if "index" in datasets:
        if index_start is None or index_end is None:
            raise ContractError("REQUIRED_PARAMETER_MISSING", "index_start_date/index_end_date")
        index_start = _date_value(index_start, "index_start_date")
        index_end = _date_value(index_end, "index_end_date")
        if index_start > index_end:
            raise ContractError("INVALID_DATE_RANGE", "index_start_date")
    max_batches = int(values.get("max_batches", 0) or 0)
    if max_batches < 0:
        raise ContractError("INVALID_PARAMETER", "max_batches")
    return {
        "mode": mode,
        "datasets": datasets,
        "stages": stages,
        "run_tag": run_tag,
        "resume": bool(values.get("resume", False)),
        "offline_only": bool(values.get("offline_only", False)),
        "max_batches": max_batches or None,
        "financial_tables": financial_tables,
        "financial_periods": periods or None,
        "financial_start": financial_start,
        "financial_end": financial_end,
        "l1_codes": l1_codes,
        "index_codes": index_codes,
        "index_start": index_start,
        "index_end": index_end,
    }


def _fundamentals_scope(context: PipelineRunContext) -> dict[str, Any]:
    _check_context(context)
    values = context.parameter_overrides
    mode = _text(values.get("mode", "period")).lower()
    if mode not in {"period", "ticker"}:
        raise ContractError("INVALID_PARAMETER", "mode")
    tables = _tables(values.get("tables", "all"))
    periods = _date_list(values.get("periods", ""), "periods")
    tickers = _code_list(values.get("tickers", ""), "tickers")
    if mode == "period" and not periods:
        raise ContractError("FUNDAMENTALS_PERIODS_REQUIRED", "periods")
    if mode == "ticker" and not tickers:
        raise ContractError("FUNDAMENTALS_TICKERS_REQUIRED", "tickers")
    start_value = values.get("start_date")
    end_value = values.get("end_date")
    if start_value is not None or end_value is not None:
        if start_value is None or end_value is None:
            raise ContractError("INVALID_DATE_RANGE", "start_date/end_date")
        start_date = _date_value(start_value, "start_date")
        end_date = _date_value(end_value, "end_date")
        if start_date > end_date:
            raise ContractError("INVALID_DATE_RANGE", "start_date")
    else:
        start_date = end_date = None
    return {
        "mode": mode,
        "tables": tables,
        "periods": periods,
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date,
    }


def _earnings_scope(context: PipelineRunContext) -> dict[str, Any]:
    _check_context(context)
    values = context.parameter_overrides
    mode = _text(values.get("mode", "period")).lower()
    if mode not in {"period", "ticker", "ann_date", "from_raw"}:
        raise ContractError("INVALID_PARAMETER", "mode")
    periods = _date_list(values.get("periods", ""), "periods")
    tickers = _code_list(values.get("tickers", ""), "tickers")
    ann_dates = _date_list(values.get("ann_dates", ""), "ann_dates")
    if mode == "period" and not periods:
        raise ContractError("EARNINGS_PERIODS_REQUIRED", "periods")
    if mode == "ticker" and not tickers:
        raise ContractError("EARNINGS_TICKERS_REQUIRED", "tickers")
    if mode == "ann_date" and not ann_dates:
        raise ContractError("EARNINGS_ANN_DATES_REQUIRED", "ann_dates")
    if mode == "from_raw" and not _text(values.get("raw_path", "")):
        raise ContractError("EARNINGS_RAW_PATH_REQUIRED", "raw_path")
    start_value = values.get("start_date")
    end_value = values.get("end_date")
    if start_value is not None or end_value is not None:
        if start_value is None or end_value is None:
            raise ContractError("INVALID_DATE_RANGE", "start_date/end_date")
        start_date = _date_value(start_value, "start_date")
        end_date = _date_value(end_value, "end_date")
        if start_date > end_date:
            raise ContractError("INVALID_DATE_RANGE", "start_date")
    else:
        start_date = end_date = None
    run_tag = _tag(values.get("run_tag", "earnings_forecast"), "run_tag")
    return {
        "mode": mode,
        "periods": periods,
        "tickers": tickers,
        "ann_dates": ann_dates,
        "start_date": start_date,
        "end_date": end_date,
        "raw_path": Path(_text(values.get("raw_path", ""))) if _text(values.get("raw_path", "")) else None,
        "run_tag": run_tag,
    }


def _connect(context: PipelineRunContext, *, read_only: bool) -> duckdb.DuckDBPyConnection:
    _check_context(context)
    return duckdb.connect(str(context.settings.paths.duckdb_path), read_only=read_only)


def _table_structure(
    context: PipelineRunContext,
    table_name: str,
    required_fields: tuple[str, ...],
    check_id: str,
    error_code: str,
) -> CheckResult:
    _check_context(context)
    try:
        con = _connect(context, read_only=True)
        try:
            columns = set(con.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"])
        finally:
            con.close()
    except ContractError:
        raise
    except Exception as exc:
        return CheckResult.failure(check_id, error_code, f"table {table_name} is unavailable", exception=type(exc).__name__)
    missing = sorted(set(required_fields) - columns)
    if missing:
        return CheckResult.failure(check_id, error_code, f"table {table_name} is missing fields", missing_fields=missing)
    return CheckResult.success(check_id, table=table_name, fields=sorted(columns))


def _calendar_structure(context: PipelineRunContext) -> CheckResult:
    return _table_structure(
        context,
        TRADING_CALENDAR.name,
        ("trade_date", "is_open"),
        "pit_trading_calendar_structure",
        "TRADING_CALENDAR_STRUCTURE_MISSING",
    )


def _calendar_freshness(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    try:
        con = _connect(context, read_only=True)
        try:
            count = int(
                con.execute(
                    "SELECT COUNT(*) FROM trading_calendar WHERE COALESCE(is_open, TRUE)"
                ).fetchone()[0]
            )
        finally:
            con.close()
    except ContractError:
        raise
    except Exception as exc:
        return CheckResult.failure(
            "pit_trading_calendar_freshness",
            "TRADING_CALENDAR_UNAVAILABLE",
            "trading calendar could not be read",
            exception=type(exc).__name__,
        )
    if count <= 0:
        return CheckResult.failure(
            "pit_trading_calendar_freshness",
            "TRADING_CALENDAR_UNAVAILABLE",
            "trading calendar has no open dates",
        )
    return CheckResult.success("pit_trading_calendar_freshness", open_dates=count)


def _calendar_input() -> InputContract:
    return InputContract(
        input_id="trading_calendar",
        kind=InputKind.TABLE,
        source="quant_db.trading_calendar",
        required_fields=("trade_date", "is_open"),
        target_date_semantics="read-only calendar used for strict next-open available_trade_date mapping",
        missing_error_code="TRADING_CALENDAR_STRUCTURE_MISSING",
        structure_check=_calendar_structure,
        freshness=FreshnessContract(
            check_id="pit_trading_calendar_freshness",
            target_date_semantics="at least one open date must be available",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code="TRADING_CALENDAR_UNAVAILABLE",
            checker=_calendar_freshness,
        ),
    )


def _provider_configuration(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    values = context.parameter_overrides
    if bool(values.get("offline_only", False)) or _text(values.get("mode")) in {"plan-only", "from_raw"}:
        return CheckResult.success("tushare_configuration", configured=False, skipped=True)
    if not context.settings.external_services.tushare_token:
        return CheckResult.failure(
            "tushare_configuration",
            "TUSHARE_CONFIGURATION_MISSING",
            "TUSHARE_TOKEN must be configured by the approved environment",
        )
    return CheckResult.success("tushare_configuration", configured=True, provider="tushare")


def _pit_provider_freshness(context: PipelineRunContext) -> CheckResult:
    scope = _pit_scope(context)
    return CheckResult.success(
        "pit_provider_scope",
        datasets=list(scope["datasets"]),
        financial_periods=list(scope["financial_periods"] or ()),
        l1_codes=list(scope["l1_codes"]),
        index_codes=list(scope["index_codes"]),
        completeness_boundary="each explicit batch has a manifest, atomic parquet artifact, and append-only revision load",
    )


def _fundamentals_provider_freshness(context: PipelineRunContext) -> CheckResult:
    scope = _fundamentals_scope(context)
    return CheckResult.success(
        "fundamentals_provider_scope",
        mode=scope["mode"],
        tables=list(scope["tables"]),
        periods=list(scope["periods"]),
        tickers=list(scope["tickers"]),
        announcement_semantics="f_ann_date when present, otherwise ann_date; available_trade_date is strict next open date",
    )


def _earnings_provider_freshness(context: PipelineRunContext) -> CheckResult:
    scope = _earnings_scope(context)
    return CheckResult.success(
        "earnings_provider_scope",
        mode=scope["mode"],
        periods=list(scope["periods"]),
        tickers=list(scope["tickers"]),
        ann_dates=list(scope["ann_dates"]),
        source_identity="source_record_id identifies one disclosure; revision_id retains technical content revisions",
    )


def _earnings_file_structure(context: PipelineRunContext) -> CheckResult:
    scope = _earnings_scope(context)
    if scope["mode"] != "from_raw":
        return CheckResult.success("earnings_raw_file_structure", skipped=True)
    path = scope["raw_path"]
    if path is None or not path.is_file():
        return CheckResult.failure("earnings_raw_file_structure", "EARNINGS_RAW_MISSING", "raw parquet is missing")
    try:
        frame = load_parquet(path)
    except FileNotFoundError:
        return CheckResult.failure("earnings_raw_file_structure", "EARNINGS_RAW_MISSING", "raw parquet is missing")
    except CorruptParquetError as exc:
        return CheckResult.failure("earnings_raw_file_structure", "EARNINGS_RAW_CORRUPT", exc.reason)
    except Exception as exc:
        return CheckResult.failure(
            "earnings_raw_file_structure",
            "EARNINGS_RAW_CORRUPT",
            "raw parquet cannot be decoded",
            exception=type(exc).__name__,
        )
    try:
        _validate_forecast_frame(frame, scope, "EARNINGS_RAW_IDENTITY_MISMATCH")
    except ContractError as exc:
        return CheckResult.failure("earnings_raw_file_structure", exc.code, exc.detail or "raw scope mismatch")
    return CheckResult.success("earnings_raw_file_structure", path=str(path), rows=len(frame))


def _earnings_file_freshness(context: PipelineRunContext) -> CheckResult:
    scope = _earnings_scope(context)
    if scope["mode"] != "from_raw":
        return CheckResult.success("earnings_raw_file_freshness", skipped=True)
    return CheckResult.success(
        "earnings_raw_file_freshness",
        source_date_semantics="raw identity is checked against the declared mode scope",
        path=str(scope["raw_path"]),
    )


def _provider_input(*, input_id: str, source: str, fields: tuple[str, ...], freshness_checker, freshness_id: str, freshness_error_code: str) -> InputContract:
    return InputContract(
        input_id=input_id,
        kind=InputKind.EXTERNAL_API,
        source=source,
        required_fields=fields,
        target_date_semantics="explicit parameter scope is checked before provider rows reach the database",
        missing_error_code="TUSHARE_CONFIGURATION_MISSING",
        structure_check=_provider_configuration,
        freshness=FreshnessContract(
            check_id=freshness_id,
            target_date_semantics="provider response must remain inside the declared report-period/ticker/date scope",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code=freshness_error_code,
            checker=freshness_checker,
        ),
    )


def _raw_input() -> InputContract:
    return InputContract(
        input_id="earnings_raw_file",
        kind=InputKind.FILE,
        source="parameter:raw_path",
        required_fields=tuple(FORECAST_CORE_FIELDS),
        target_date_semantics="from_raw identity is checked against the declared mode scope",
        missing_error_code="EARNINGS_RAW_MISSING",
        structure_check=_earnings_file_structure,
        freshness=FreshnessContract(
            check_id="earnings_raw_file_freshness",
            target_date_semantics="raw file is an explicit recovery artifact, never an implicit production default",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code="EARNINGS_RAW_STALE",
            checker=_earnings_file_freshness,
        ),
    )


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _validate_financial_frame(frame: object, scope: Mapping[str, Any], table: str) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise ContractError("FUNDAMENTALS_PROVIDER_RESPONSE_INVALID", table)
    if frame.empty:
        return frame
    required = {"ts_code", "end_date"}
    missing = sorted(required - set(frame.columns))
    if "ann_date" not in frame.columns and "f_ann_date" not in frame.columns:
        missing.append("ann_date|f_ann_date")
    if missing:
        raise ContractError("FUNDAMENTALS_PROVIDER_SCHEMA_MISSING", ",".join(missing))
    for row_number, (_, row) in enumerate(frame.iterrows()):
        ticker = _text(row.get("ts_code")).upper()
        if not CODE_PATTERN.fullmatch(ticker):
            raise ContractError("FUNDAMENTALS_PROVIDER_SCHEMA_INVALID", f"row {row_number} ts_code")
        if scope["mode"] == "ticker" and ticker not in set(scope["tickers"]):
            raise ContractError("FUNDAMENTALS_SCOPE_MISMATCH", f"row {row_number} ts_code")
        try:
            period = _date_value(row.get("end_date"), "end_date").strftime("%Y%m%d")
        except ContractError as exc:
            raise ContractError("FUNDAMENTALS_PROVIDER_DATE_INVALID", f"row {row_number}") from exc
        if scope["periods"] and period not in set(scope["periods"]):
            raise ContractError("FUNDAMENTALS_SCOPE_MISMATCH", f"row {row_number} end_date")
        announcement = row.get("ann_date")
        if _is_missing(announcement) and "f_ann_date" in frame.columns:
            announcement = row.get("f_ann_date")
        if _is_missing(announcement):
            raise ContractError("FUNDAMENTALS_PROVIDER_DATE_INVALID", f"row {row_number} ann_date")
    return frame


def _validate_forecast_frame(frame: object, scope: Mapping[str, Any], error_code: str) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise ContractError("EARNINGS_PROVIDER_RESPONSE_INVALID", "response must be a DataFrame")
    if frame.empty:
        return frame
    missing = sorted(set(FORECAST_CORE_FIELDS) - set(frame.columns))
    if missing:
        raise ContractError("EARNINGS_PROVIDER_SCHEMA_MISSING", ",".join(missing))
    periods = set(scope["periods"])
    tickers = set(scope["tickers"])
    ann_dates = set(scope["ann_dates"])
    for row_number, (_, row) in enumerate(frame.iterrows()):
        ticker = _text(row.get("ts_code")).upper()
        if not CODE_PATTERN.fullmatch(ticker) or (tickers and ticker not in tickers):
            raise ContractError(error_code, f"row {row_number} ts_code")
        if any(_is_missing(row.get(field)) for field in FORECAST_CORE_FIELDS):
            raise ContractError(error_code, f"row {row_number} core field")
        try:
            period = _date_value(row.get("end_date"), "end_date").strftime("%Y%m%d")
            ann_date = _date_value(row.get("ann_date"), "ann_date").strftime("%Y%m%d")
        except ContractError as exc:
            raise ContractError("EARNINGS_PROVIDER_DATE_INVALID", f"row {row_number}") from exc
        if periods and period not in periods:
            raise ContractError(error_code, f"row {row_number} end_date")
        if ann_dates and ann_date not in ann_dates:
            raise ContractError(error_code, f"row {row_number} ann_date")
        if scope["start_date"] and not (scope["start_date"].strftime("%Y%m%d") <= ann_date <= scope["end_date"].strftime("%Y%m%d")):
            raise ContractError(error_code, f"row {row_number} announcement range")
    return frame


def _append_frames(context: PipelineRunContext, frames: Mapping[str, pd.DataFrame], error_code: str) -> tuple[dict[str, int], float]:
    _check_context(context)
    path = Path(context.settings.paths.duckdb_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con: duckdb.DuckDBPyConnection | None = None
    transaction_open = False
    started = monotonic()
    inserted: dict[str, int] = {table: 0 for table in frames}
    try:
        con = duckdb.connect(str(path))
        init_database(con)
        _check_context(context)
        con.execute("BEGIN TRANSACTION")
        transaction_open = True
        for table, frame in frames.items():
            _check_context(context)
            if frame is None or frame.empty:
                continue
            prepared = align_to_schema(frame, table, fill_missing_optional=True, drop_extra=True)
            prepared = quick_validate(prepared, table, allow_extra=False)
            inserted[table] = append_only_insert(
                con,
                table,
                prepared,
                id_column=REVISION_ID,
                execution_control=context.execution_control,
            )
        _check_context(context)
        con.execute("COMMIT")
        transaction_open = False
        _check_context(context)
        return inserted, max(0.0, monotonic() - started)
    except ExecutionControlError:
        if con is not None and transaction_open:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
        raise
    except ContractError:
        if con is not None and transaction_open:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
        raise
    except Exception as exc:
        if con is not None and transaction_open:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
        raise ContractError(error_code, type(exc).__name__) from exc
    finally:
        if con is not None:
            con.close()


def _table_completion(table: str, check_id: str, error_code: str):
    def checker(context: PipelineRunContext) -> CheckResult:
        _check_context(context)
        try:
            con = _connect(context, read_only=True)
            try:
                total = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            finally:
                con.close()
        except ContractError:
            raise
        except Exception as exc:
            return CheckResult.failure(check_id, error_code, "output table is not queryable after commit", exception=type(exc).__name__)
        _check_context(context)
        return CheckResult.success(check_id, table=table, total_rows=total, marker="committed append is queryable")

    checker.__name__ = f"{check_id}_checker"
    return checker


def _revision_quality(table: str, check_id: str, error_code: str):
    def checker(context: PipelineRunContext) -> CheckResult:
        _check_context(context)
        try:
            con = _connect(context, read_only=True)
            try:
                duplicate = con.execute(
                    f"SELECT {REVISION_ID} FROM {table} GROUP BY {REVISION_ID} HAVING COUNT(*) > 1 LIMIT 1"
                ).fetchone()
            finally:
                con.close()
        except ContractError:
            raise
        except Exception as exc:
            return CheckResult.failure(check_id, error_code, "revision uniqueness query failed", exception=type(exc).__name__)
        _check_context(context)
        if duplicate is not None:
            return CheckResult.failure(check_id, error_code, "duplicate revision_id exists")
        return CheckResult.success(check_id, table=table, unique_key=[REVISION_ID])

    checker.__name__ = f"{check_id}_checker"
    return checker


def _artifact_bytes(*paths: Path) -> int:
    total = 0
    for root in paths:
        if not root.exists():
            continue
        if root.is_file():
            total += root.stat().st_size
            continue
        for item in root.rglob("*"):
            if item.is_file():
                try:
                    total += item.stat().st_size
                except OSError:
                    pass
    return total


def _query_table_counts(context: PipelineRunContext, tables: Sequence[str]) -> tuple[int, int]:
    assets = 0
    periods = 0
    try:
        con = _connect(context, read_only=True)
        try:
            for table in tables:
                _check_context(context)
                if table in FINANCIAL_TABLES or table == EARNINGS_TABLE:
                    assets += int(con.execute(f"SELECT COUNT(DISTINCT ticker) FROM {table}").fetchone()[0])
                    periods += int(con.execute(f"SELECT COUNT(DISTINCT report_period) FROM {table}").fetchone()[0])
                elif table == INDUSTRY_MEMBERSHIP_HISTORY.name:
                    assets += int(con.execute(f"SELECT COUNT(DISTINCT asset_id) FROM {table}").fetchone()[0])
                    periods += int(con.execute(f"SELECT COUNT(DISTINCT effective_from) FROM {table}").fetchone()[0])
                elif table == INDEX_COMPONENT_HISTORY.name:
                    assets += int(con.execute(f"SELECT COUNT(DISTINCT asset_id) FROM {table}").fetchone()[0])
                    periods += int(con.execute(f"SELECT COUNT(DISTINCT snapshot_date) FROM {table}").fetchone()[0])
        finally:
            con.close()
    except Exception:
        return 0, 0
    return assets, periods


def _pit_execute(context: PipelineRunContext) -> BusinessExecution:
    started = monotonic()
    scope = _pit_scope(context)
    raw_root = Path(context.settings.paths.raw_dir) / "pit_backfill" / scope["run_tag"]
    clean_root = Path(context.settings.paths.canonical_dir) / "pit_backfill" / scope["run_tag"]
    state_root = Path(context.settings.paths.state_dir) / "pit_backfill" / scope["run_tag"]
    config = BackfillConfig(
        run_tag=scope["run_tag"],
        mode=scope["mode"],
        datasets=scope["datasets"],
        stages=scope["stages"],
        resume=scope["resume"],
        offline_only=scope["offline_only"],
        max_batches=scope["max_batches"],
        financial_tables=scope["financial_tables"],
        financial_periods=scope["financial_periods"],
        financial_start=scope["financial_start"],
        financial_end=scope["financial_end"],
        l1_codes=scope["l1_codes"] or None,
        index_codes=scope["index_codes"] or DEFAULT_INDEX_CODES,
        index_start=scope["index_start"],
        index_end=scope["index_end"],
        db_path=context.settings.paths.duckdb_path,
        raw_dir=raw_root,
        cleaned_dir=clean_root,
        state_dir=state_root,
        log_path=Path(context.settings.paths.log_dir) / f"pit_backfill_{scope['run_tag']}.log",
        settings=context.settings,
        execution_control=context.execution_control,
        skip_preflight=True,
        create_backup=False,
        lock_path=state_root / "quant.db.write.lock",
        strict_scope=True,
    )
    _check_context(context)
    try:
        report = PitBackfillRunner(config).run()
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("PIT_BACKFILL_FAILED", type(exc).__name__) from exc
    _check_context(context)
    if not report.get("ok", False):
        raise ContractError("PIT_BACKFILL_FAILED", "manifest contains failed batches")
    results = report.get("results", ())
    inserted_by_table = {table: 0 for table in PIT_TABLES}
    for item in results:
        dataset = item.get("batch_id", "").split(":", 1)[0]
        if dataset == "fundamentals" and item.get("batch_id", "").count(":") >= 2:
            table = item["batch_id"].split(":", 2)[1]
        elif dataset == "industry":
            table = INDUSTRY_MEMBERSHIP_HISTORY.name
        elif dataset == "index":
            table = INDEX_COMPONENT_HISTORY.name
        else:
            continue
        if table in inserted_by_table:
            inserted_by_table[table] += int(item.get("inserted", 0) or 0)
    selected_tables = set()
    if "fundamentals" in scope["datasets"]:
        selected_tables.update(scope["financial_tables"])
    if "industry" in scope["datasets"]:
        selected_tables.add(INDUSTRY_MEMBERSHIP_HISTORY.name)
    if "index" in scope["datasets"]:
        selected_tables.add(INDEX_COMPONENT_HISTORY.name)
    outputs = tuple(
        OutputResult(
            output_id=table,
            rows_written=inserted_by_table[table],
            location=QUANT_DB_LOCATION,
            completed=True,
            detail={
                "selected": table in selected_tables,
                "run_tag": scope["run_tag"],
                "manifest_path": report.get("paths", {}).get("manifest_path"),
                "plan_path": report.get("paths", {}).get("plan_path"),
                "recovery": "resume uses the manifest and atomic raw/cleaned parquet; corrupt artifacts are quarantined",
                "revision_semantics": "append-only by revision_id; existing revisions are retained",
            },
        )
        for table in PIT_TABLES
    )
    rows_read = int(report.get("totals", {}).get("fetched_rows", 0) or 0)
    rows_written = sum(item.rows_written for item in outputs)
    assets, periods = _query_table_counts(context, tuple(selected_tables))
    elapsed = max(0.0, monotonic() - started)
    db_seconds = float(report.get("totals", {}).get("database_write_seconds", 0.0) or 0.0)
    paths = report.get("paths", {})
    temporary_bytes = _artifact_bytes(
        Path(paths["raw_dir"]) if paths.get("raw_dir") else raw_root,
        Path(paths["cleaned_dir"]) if paths.get("cleaned_dir") else clean_root,
        Path(paths["state_dir"]) if paths.get("state_dir") else state_root,
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=rows_read,
            rows_written=rows_written,
            assets_processed=assets,
            dates_processed=periods,
            database_write_seconds=db_seconds,
            stage_durations_seconds={"pit_backfill": elapsed},
            api_requests=int(report.get("request_count", 0) or 0),
            batches=int(report.get("totals", {}).get("processed", 0) or 0),
            temporary_disk_bytes=temporary_bytes,
        ),
        outputs=outputs,
    )


def _execute_fundamentals(context: PipelineRunContext) -> BusinessExecution:
    started = monotonic()
    scope = _fundamentals_scope(context)
    resolver = NextTradeDateResolver(db_path=str(context.settings.paths.duckdb_path))
    report = FinancialFetchReport()
    raw_rows: dict[str, int] = {}
    prepared: dict[str, pd.DataFrame] = {}
    for table in scope["tables"]:
        _check_context(context)
        try:
            raw = fetch_financial(
                table,
                mode=scope["mode"],
                periods=scope["periods"] or None,
                tickers=scope["tickers"] or None,
                start_date=scope["start_date"].strftime("%Y%m%d") if scope["start_date"] else None,
                end_date=scope["end_date"].strftime("%Y%m%d") if scope["end_date"] else None,
                settings=context.settings,
                execution_control=context.execution_control,
                report=report,
            )
            raw = raw if raw is not None else pd.DataFrame()
            _validate_financial_frame(raw, scope, table)
            raw_rows[table] = len(raw)
            cleaned = clean_financial(
                raw,
                table,
                trade_date_resolver=resolver,
                execution_control=context.execution_control,
            )
            prepared[table] = align_to_schema(cleaned, table, fill_missing_optional=True, drop_extra=True) if not cleaned.empty else pd.DataFrame()
        except ExecutionControlError as exc:
            raise ContractError(exc.code, exc.detail) from exc
        except ContractError:
            raise
        except Exception as exc:
            raise ContractError("FUNDAMENTALS_INGEST_FAILED", f"{table}:{type(exc).__name__}") from exc
    _check_context(context)
    inserted, write_seconds = _append_frames(context, prepared, "FUNDAMENTALS_WRITE_FAILED")
    _check_context(context)
    outputs = tuple(
        OutputResult(
            output_id=table,
            rows_written=inserted.get(table, 0),
            location=QUANT_DB_LOCATION,
            completed=True,
            detail={
                "selected": table in scope["tables"],
                "rows_received": raw_rows.get(table, 0),
                "rows_cleaned": len(prepared.get(table, pd.DataFrame())),
                "rows_inserted": inserted.get(table, 0),
                "periods": list(scope["periods"]),
                "announcement_semantics": "f_ann_date preferred over ann_date; available_trade_date is strict next open",
                "revision_semantics": "same normalized content repeats the revision_id; changed content is retained",
            },
        )
        for table in FINANCIAL_TABLES
    )
    rows_read = sum(raw_rows.values())
    assets = {str(value) for frame in prepared.values() for value in (frame["ticker"].dropna().unique() if "ticker" in frame else [])}
    period_values = {to_date(value) for frame in prepared.values() for value in (frame["report_period"].dropna().tolist() if "report_period" in frame else [])}
    elapsed = max(0.0, monotonic() - started)
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=rows_read,
            rows_written=sum(item.rows_written for item in outputs),
            assets_processed=len(assets),
            dates_processed=len({item for item in period_values if item is not None}),
            database_write_seconds=write_seconds,
            stage_durations_seconds={"provider_and_clean": max(0.0, elapsed - write_seconds), "database_write": write_seconds},
            api_requests=report.api_requests,
            batches=report.batches,
            retries=report.retries,
        ),
        outputs=outputs,
    )


def _earnings_artifact_paths(context: PipelineRunContext, scope: Mapping[str, Any]) -> tuple[Path, Path, Path]:
    state_dir = Path(context.settings.paths.state_dir) / "earnings_forecast" / scope["run_tag"]
    raw_dir = Path(context.settings.paths.raw_dir) / "earnings_forecast" / scope["run_tag"]
    clean_dir = Path(context.settings.paths.canonical_dir) / "earnings_forecast" / scope["run_tag"]
    raw_path = scope["raw_path"] or raw_file_path(raw_dir, context.run_id)
    return Path(raw_path), cleaned_file_path(clean_dir, context.run_id), state_dir / "manifest.jsonl"


def _earnings_record(context: PipelineRunContext, scope: Mapping[str, Any], raw_path: Path, clean_path: Path, manifest_path: Path) -> tuple[ManifestStore, BatchRecord]:
    store = ManifestStore(manifest_path)
    record = BatchRecord(
        batch_id=context.run_id,
        dataset="earnings_forecast",
        key=scope["mode"],
        period=",".join(scope["periods"]) or None,
        start_date=scope["start_date"].isoformat() if scope["start_date"] else None,
        end_date=scope["end_date"].isoformat() if scope["end_date"] else None,
        raw_path=str(raw_path),
        cleaned_path=str(clean_path),
        started_at=utc_now_iso(),
        meta={
            "pipeline_id": context.pipeline_id,
            "ingestion_run_id": context.run_id,
            "mode": scope["mode"],
            "manifest_path": str(manifest_path),
            "source_record_identity": "ticker,event_type,report_period,announcement_date",
            "revision_identity": "normalized content signature including source_record_id",
        },
    )
    record.set_stage("fetch", STATUS_RUNNING, error=None)
    store.save(record)
    return store, record


def _execute_earnings(context: PipelineRunContext) -> BusinessExecution:
    started = monotonic()
    scope = _earnings_scope(context)
    raw_path, clean_path, manifest_path = _earnings_artifact_paths(context, scope)
    store, record = _earnings_record(context, scope, raw_path, clean_path, manifest_path)
    stage = "fetch"
    try:
        resolver = NextTradeDateResolver(db_path=str(context.settings.paths.duckdb_path))
        if scope["mode"] == "from_raw":
            _check_context(context)
            raw = load_parquet(raw_path)
            _validate_forecast_frame(raw, scope, "EARNINGS_RAW_IDENTITY_MISMATCH")
        else:
            report = ForecastFetchReport()
            raw = fetch_earnings_forecast(
                mode=scope["mode"],
                periods=scope["periods"] or None,
                tickers=scope["tickers"] or None,
                ann_dates=scope["ann_dates"] or None,
                start_date=scope["start_date"].strftime("%Y%m%d") if scope["start_date"] else None,
                end_date=scope["end_date"].strftime("%Y%m%d") if scope["end_date"] else None,
                settings=context.settings,
                execution_control=context.execution_control,
                report=report,
            )
            raw = raw if raw is not None else pd.DataFrame()
            _validate_forecast_frame(raw, scope, "EARNINGS_SCOPE_MISMATCH")
            _check_context(context)
            save_parquet(raw, raw_path)
            _check_context(context)
        raw = raw if raw is not None else pd.DataFrame()
        record.fetched_rows = len(raw)
        record.set_stage("fetch", STATUS_EMPTY if raw.empty else STATUS_SUCCESS, error=None, finished=True)
        store.save(record)
        _check_context(context)

        stage = "clean"
        record.set_stage("clean", STATUS_RUNNING, error=None)
        store.save(record)
        cleaned = clean_earnings_forecast(
            raw,
            trade_date_resolver=resolver,
            execution_control=context.execution_control,
        )
        if cleaned.empty:
            cleaned = pd.DataFrame()
        _check_context(context)
        save_parquet(cleaned, clean_path)
        record.cleaned_rows = len(cleaned)
        record.set_stage("clean", STATUS_EMPTY if cleaned.empty else STATUS_SUCCESS, error=None, finished=True)
        store.save(record)
        _check_context(context)

        stage = "load"
        record.set_stage("load", STATUS_RUNNING, error=None)
        store.save(record)
        inserted, write_seconds = _append_frames(context, {EARNINGS_TABLE: cleaned}, "EARNINGS_WRITE_FAILED")
        record.inserted_rows = inserted[EARNINGS_TABLE]
        record.set_stage("load", STATUS_EMPTY if cleaned.empty else STATUS_SUCCESS, error=None, finished=True)
        record.finished_at = utc_now_iso()
        store.save(record)
        _check_context(context)
    except ExecutionControlError as exc:
        record.set_stage(stage, STATUS_FAILED, error=exc.code, finished=True)
        record.finished_at = utc_now_iso()
        store.save(record)
        raise ContractError(exc.code, exc.detail) from exc
    except ContractError as exc:
        record.set_stage(stage, STATUS_FAILED, error=exc.code, finished=True)
        record.finished_at = utc_now_iso()
        store.save(record)
        raise
    except (CorruptParquetError, FileNotFoundError) as exc:
        record.set_stage(stage, STATUS_FAILED, error=type(exc).__name__, finished=True)
        record.finished_at = utc_now_iso()
        store.save(record)
        raise ContractError("EARNINGS_RAW_CORRUPT", type(exc).__name__) from exc
    except Exception as exc:
        record.set_stage(stage, STATUS_FAILED, error=type(exc).__name__, finished=True)
        record.finished_at = utc_now_iso()
        store.save(record)
        raise ContractError("EARNINGS_FORECAST_FAILED", f"{stage}:{type(exc).__name__}") from exc

    report = locals().get("report")
    api_requests = report.api_requests if isinstance(report, ForecastFetchReport) else 0
    batches = report.batches if isinstance(report, ForecastFetchReport) else 0
    retries = report.retries if isinstance(report, ForecastFetchReport) else 0
    assets = int(cleaned["ticker"].nunique()) if "ticker" in cleaned and not cleaned.empty else 0
    periods = int(cleaned["report_period"].nunique()) if "report_period" in cleaned and not cleaned.empty else 0
    temporary_bytes = _artifact_bytes(raw_path, clean_path, manifest_path)
    output = OutputResult(
        output_id=EARNINGS_TABLE,
        rows_written=record.inserted_rows,
        location=QUANT_DB_LOCATION,
        completed=True,
        detail={
            "ingestion_run_id": context.run_id,
            "manifest_path": str(manifest_path),
            "raw_path": str(raw_path),
            "cleaned_path": str(clean_path),
            "rows_received": len(raw),
            "rows_cleaned": len(cleaned),
            "rows_inserted": record.inserted_rows,
            "source_record_identity": "one disclosure identity can have multiple revision_id rows",
            "repeat_run_semantics": "same normalized content inserts zero new rows; changed content is retained",
            "empty_scope_result": len(raw) == 0,
        },
    )
    elapsed = max(0.0, monotonic() - started)
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=len(raw),
            rows_written=record.inserted_rows,
            assets_processed=assets,
            dates_processed=periods,
            database_write_seconds=write_seconds,
            stage_durations_seconds={"provider_and_clean": max(0.0, elapsed - write_seconds), "database_write": write_seconds},
            api_requests=api_requests,
            batches=batches,
            retries=retries,
            temporary_disk_bytes=temporary_bytes,
        ),
        outputs=(output,),
    )


def _fundamental_outputs() -> tuple[OutputContract, ...]:
    return tuple(
        OutputContract(
            output_id=table,
            physical_resource=QUANT_DB_RESOURCE,
            location=QUANT_DB_LOCATION,
            object_name=table,
            unique_key=get_table(table).primary_key,
            write_mode=WriteMode.APPEND,
            target_date_semantics="append all validated rows in the explicit report-period or ticker scope",
            completion=CompletionContract(
                marker=f"committed append is queryable in {table}",
                error_code=f"{table.upper()}_COMPLETION_MISSING",
                checker=_table_completion(table, f"{table}_completion", f"{table.upper()}_COMPLETION_MISSING"),
            ),
            quality_checks=(_revision_quality(table, f"{table}_revision_quality", f"{table.upper()}_REVISION_DUPLICATE"),),
            allow_empty=True,
        )
        for table in FINANCIAL_TABLES
    )


def _pit_outputs() -> tuple[OutputContract, ...]:
    return tuple(
        OutputContract(
            output_id=table,
            physical_resource=QUANT_DB_RESOURCE,
            location=QUANT_DB_LOCATION,
            object_name=table,
            unique_key=get_table(table).primary_key,
            write_mode=WriteMode.APPEND,
            target_date_semantics="append validated historical PIT revisions for the explicit backfill batches",
            completion=CompletionContract(
                marker=f"committed append is queryable in {table}",
                error_code=f"{table.upper()}_COMPLETION_MISSING",
                checker=_table_completion(table, f"{table}_completion", f"{table.upper()}_COMPLETION_MISSING"),
            ),
            quality_checks=(_revision_quality(table, f"{table}_revision_quality", f"{table.upper()}_REVISION_DUPLICATE"),),
            allow_empty=True,
        )
        for table in PIT_TABLES
    )


def _earnings_outputs() -> tuple[OutputContract, ...]:
    return (
        OutputContract(
            output_id=EARNINGS_TABLE,
            physical_resource=QUANT_DB_RESOURCE,
            location=QUANT_DB_LOCATION,
            object_name=EARNINGS_TABLE,
            unique_key=EARNINGS_FORECAST_EVENT.primary_key,
            write_mode=WriteMode.APPEND,
            target_date_semantics="append all validated forecast events in the explicit report-period, ticker, announcement-date, or raw-file scope",
            completion=CompletionContract(
                marker="committed append is queryable in earnings_forecast_event",
                error_code="EARNINGS_COMPLETION_MISSING",
                checker=_table_completion(EARNINGS_TABLE, "earnings_forecast_completion", "EARNINGS_COMPLETION_MISSING"),
            ),
            quality_checks=(_revision_quality(EARNINGS_TABLE, "earnings_forecast_revision_quality", "EARNINGS_REVISION_DUPLICATE"),),
            allow_empty=True,
        ),
    )


def _pit_parameters() -> tuple[ParameterContract, ...]:
    return (
        ParameterContract("run_tag", ParameterType.STRING, "Operator-provided artifact/recovery namespace; no dated default.", required=True),
        ParameterContract("mode", ParameterType.STRING, "full or plan-only; precheck is a legacy helper and is not formal.", default="full"),
        ParameterContract("datasets", ParameterType.STRING, "Comma-separated fundamentals, industry, and/or index.", default="fundamentals"),
        ParameterContract("stages", ParameterType.STRING, "Comma-separated fetch, clean, load stages in dependency order.", default="fetch,clean,load"),
        ParameterContract("resume", ParameterType.BOOLEAN, "Resume from the explicit run manifest and validated artifacts.", default=False),
        ParameterContract("offline_only", ParameterType.BOOLEAN, "Use existing raw artifacts only; missing/corrupt files fail closed.", default=False),
        ParameterContract("max_batches", ParameterType.INTEGER, "Optional positive batch cap; zero means no cap.", default=0),
        ParameterContract("financial_tables", ParameterType.STRING, "Financial tables or all.", default="all"),
        ParameterContract("financial_periods", ParameterType.STRING, "Comma-separated YYYYMMDD report periods.", default=""),
        ParameterContract("financial_start_date", ParameterType.DATE, "Inclusive financial batch-generation start date.", default=None),
        ParameterContract("financial_end_date", ParameterType.DATE, "Inclusive financial batch-generation end date.", default=None),
        ParameterContract("l1_codes", ParameterType.STRING, "Comma-separated SW2021 L1 codes when industry is selected.", default=""),
        ParameterContract("index_codes", ParameterType.STRING, "Comma-separated index codes when index is selected.", default=""),
        ParameterContract("index_start_date", ParameterType.DATE, "Inclusive index source start date.", default=None),
        ParameterContract("index_end_date", ParameterType.DATE, "Inclusive index source end date.", default=None),
    )


def _fundamentals_parameters() -> tuple[ParameterContract, ...]:
    return (
        ParameterContract("mode", ParameterType.STRING, "period or ticker.", default="period"),
        ParameterContract("tables", ParameterType.STRING, "Financial tables or all.", default="all"),
        ParameterContract("periods", ParameterType.STRING, "Comma-separated YYYYMMDD report periods.", default=""),
        ParameterContract("tickers", ParameterType.STRING, "Comma-separated ticker codes.", default=""),
        ParameterContract("start_date", ParameterType.DATE, "Optional inclusive provider date bound for ticker mode.", default=None),
        ParameterContract("end_date", ParameterType.DATE, "Optional inclusive provider date bound for ticker mode.", default=None),
    )


def _earnings_parameters() -> tuple[ParameterContract, ...]:
    return (
        ParameterContract("mode", ParameterType.STRING, "period, ticker, ann_date, or from_raw.", default="period"),
        ParameterContract("periods", ParameterType.STRING, "Comma-separated YYYYMMDD report periods.", default=""),
        ParameterContract("tickers", ParameterType.STRING, "Comma-separated ticker codes.", default=""),
        ParameterContract("ann_dates", ParameterType.STRING, "Comma-separated YYYYMMDD announcement dates.", default=""),
        ParameterContract("start_date", ParameterType.DATE, "Optional inclusive announcement date bound.", default=None),
        ParameterContract("end_date", ParameterType.DATE, "Optional inclusive announcement date bound.", default=None),
        ParameterContract("raw_path", ParameterType.STRING, "Explicit recovery parquet path; required only for from_raw.", default=""),
        ParameterContract("run_tag", ParameterType.STRING, "Artifact/manifest namespace.", default="earnings_forecast"),
    )


def _performance(scope: str, *, normal: float, warning: float, hard: int) -> PerformanceBudget:
    return PerformanceBudget(
        normal_budget_seconds=normal,
        warning_threshold_seconds=warning,
        hard_timeout_seconds=hard,
        benchmark_scope=scope,
        baseline_source=(
            "tests/pipeline/test_pit_fundamentals_contracts.py temporary-DuckDB acceptance and measured local baseline; "
            "src/qrp_atlas/pipeline/pit_backfill/runner.py artifact/manifest stages"
        ),
    )


PIT_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="pit_manual_observation_date",
    description="Use the scheduled Shanghai date as an operation observation label; source ranges remain explicit parameters.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_scheduled_observation,
    validate_explicit_date=_reject_explicit_target_date,
)

FUNDAMENTALS_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="fundamentals_manual_observation_date",
    description="Use the scheduled Shanghai date as an observation label; reports retain their own announcement and availability dates.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_scheduled_observation,
    validate_explicit_date=_reject_explicit_target_date,
)

EARNINGS_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="earnings_forecast_manual_observation_date",
    description="Use the scheduled Shanghai date as an observation label; forecast event availability is derived from announcement_date.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_scheduled_observation,
    validate_explicit_date=_reject_explicit_target_date,
)


PIT_BACKFILL = register_pipeline(
    PipelineContract(
        pipeline_id="pit_backfill",
        name="PIT historical backfill",
        description="Runs explicit, resumable PIT batch recovery over existing financial, industry, and index implementations.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_pit_execute,
        target_date_policy=PIT_TARGET_DATE_POLICY,
        parameters=_pit_parameters(),
        inputs=(
            _provider_input(
                input_id="external_tushare_pit",
                source="tushare historical endpoints",
                fields=("ts_code", "end_date", "ann_date", "revision source fields"),
                freshness_checker=_pit_provider_freshness,
                freshness_id="pit_provider_scope",
                freshness_error_code="PIT_PROVIDER_SCOPE_INVALID",
            ),
            _calendar_input(),
        ),
        outputs=_pit_outputs(),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(TRADING_CALENDAR_READ,),
        idempotency=IdempotencyContract(
            idempotency_key="manifest batch_id plus revision_id in each PIT table",
            repeat_run_semantics="same explicit scope and unchanged artifacts/provider payload produce zero new revisions",
            existing_target_handling="INSERT OR IGNORE by revision_id; changed source content remains as a later PIT revision",
            failure_recovery="resume only terminal manifest stages with validated artifacts; missing/corrupt parquet is re-fetched or fails in offline_only mode",
            uses_staging=False,
            atomic_replace_boundary="each batch raw/cleaned parquet write is atomic and each database load is one transaction",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="one append-only DuckDB transaction per manifest batch after its source and cleaned artifacts pass validation",
            failure_visibility="a provider, validation, cancellation, or batch write error rolls back that batch; committed prior batches remain auditable in the manifest",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=_performance("explicit multi-table PIT batch plan with atomic parquet artifacts and DuckDB batch loads", normal=1800.0, warning=1200.0, hard=2400),
        manual_execution_allowed=True,
    )
)


FUNDAMENTALS_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="fundamentals_ingest",
        name="Fundamentals ingestion",
        description="Fetches explicit financial report periods or tickers, preserves announcement availability, and appends revisions transactionally.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_fundamentals,
        target_date_policy=FUNDAMENTALS_TARGET_DATE_POLICY,
        parameters=_fundamentals_parameters(),
        inputs=(
            _provider_input(
                input_id="external_tushare_fundamentals",
                source="tushare financial VIP/ticker endpoints",
                fields=("ts_code", "end_date", "ann_date or f_ann_date"),
                freshness_checker=_fundamentals_provider_freshness,
                freshness_id="fundamentals_provider_scope",
                freshness_error_code="FUNDAMENTALS_PROVIDER_SCOPE_INVALID",
            ),
            _calendar_input(),
        ),
        outputs=_fundamental_outputs(),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(TRADING_CALENDAR_READ,),
        idempotency=IdempotencyContract(
            idempotency_key="revision_id; financial business identity includes ticker, report_period, announcement, report/update flags",
            repeat_run_semantics="same normalized report content produces the same revision_id and zero new rows",
            existing_target_handling="INSERT OR IGNORE preserves historical revisions; changed same-period content is appended",
            failure_recovery="all selected tables are prepared before one DuckDB transaction; any failure rolls the complete invocation back",
            uses_staging=False,
            atomic_replace_boundary="one database transaction around all selected financial tables",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="all selected table appends after provider scope, PIT cleaning, and schema validation",
            failure_visibility="no selected table is committed when another selected table, control check, or database operation fails",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=_performance("mocked explicit report-period/ticker fetch across four financial tables and one DuckDB transaction", normal=900.0, warning=600.0, hard=1200),
        manual_execution_allowed=True,
    )
)


EARNINGS_FORECAST_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="earnings_forecast_ingest",
        name="Earnings forecast ingestion",
        description="Fetches or recovers explicit earnings forecast scope, archives raw evidence, and retains disclosure revisions by source identity.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_earnings,
        target_date_policy=EARNINGS_TARGET_DATE_POLICY,
        parameters=_earnings_parameters(),
        inputs=(
            _provider_input(
                input_id="external_tushare_earnings_forecast",
                source="tushare forecast/forecast_vip",
                fields=tuple(FORECAST_CORE_FIELDS),
                freshness_checker=_earnings_provider_freshness,
                freshness_id="earnings_provider_scope",
                freshness_error_code="EARNINGS_PROVIDER_SCOPE_INVALID",
            ),
            _raw_input(),
            _calendar_input(),
        ),
        outputs=_earnings_outputs(),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(TRADING_CALENDAR_READ,),
        idempotency=IdempotencyContract(
            idempotency_key="revision_id; source_record_id identifies the disclosure and ingestion_run_id identifies the manifest run",
            repeat_run_semantics="same normalized disclosure content is ignored on repeat; changed content under the same source_record_id is retained",
            existing_target_handling="append-only INSERT OR IGNORE by revision_id; raw/cleaned parquet and JSONL manifest remain per invocation",
            failure_recovery="atomic raw/cleaned artifacts and one database transaction; from_raw reuses only the declared file after identity and schema checks",
            uses_staging=False,
            atomic_replace_boundary="raw/cleaned parquet replace and one earnings_forecast_event database transaction",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="one transaction around the prepared earnings forecast append after raw recovery, cleaning, and manifest stage updates",
            failure_visibility="missing/corrupt/out-of-scope input or cancellation before commit leaves no new event rows and the manifest records the failed stage",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=_performance("mocked forecast scope with raw archive, clean archive, and one DuckDB append transaction", normal=600.0, warning=360.0, hard=900),
        manual_execution_allowed=True,
    )
)


PIT_FUNDAMENTALS_CONTRACTS: tuple[PipelineContract, ...] = (
    PIT_BACKFILL,
    FUNDAMENTALS_INGEST,
    EARNINGS_FORECAST_INGEST,
)

__all__ = [
    "EARNINGS_FORECAST_INGEST",
    "EARNINGS_TARGET_DATE_POLICY",
    "FUNDAMENTALS_INGEST",
    "FUNDAMENTALS_TARGET_DATE_POLICY",
    "PIT_BACKFILL",
    "PIT_FUNDAMENTALS_CONTRACTS",
    "PIT_TARGET_DATE_POLICY",
]
