"""Formal Tushare contract for the index dictionary snapshot.

``index_basic`` is a manually executable reference-data Pipeline.  It is part of
the admitted contract catalog, but deployment schedules remain a separate
selection and are intentionally unchanged by this module.
"""

from __future__ import annotations

import re
import time
from datetime import date
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import (
    CREATED_AT,
    INDEX_BASIC,
    TUSHARE_INDEX_BASIC,
    align_to_schema,
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
    PipelineKind,
    PipelineMetrics,
    PipelineRunContext,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .registry import register_pipeline


CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
INDEX_BASIC_MARKETS: tuple[str, ...] = ("SSE", "SZSE", "CSI", "CICC", "MSCI", "SW", "OTH")
INDEX_BASIC_DEFAULT_MARKETS = ",".join(INDEX_BASIC_MARKETS)
INDEX_BASIC_CODE_PATTERN = re.compile(r"^\d{6}\.[A-Z]{2,4}$")
INDEX_BASIC_REQUIRED_FIELDS = (
    "ts_code",
    "name",
    "market",
    "publisher",
    "category",
    "base_date",
    "base_point",
    "list_date",
)


def _resolve_index_basic_target(invocation) -> TargetWindow:
    invocation.execution_control.check()
    target = invocation.scheduled_for.astimezone(CHINA_TZ).date()
    return TargetWindow.for_date(target)


def _validate_index_basic_date(target_date: date, _invocation) -> bool:
    return isinstance(target_date, date)


INDEX_BASIC_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="index_basic_snapshot_clock_date_v1",
    description="Uses the Asia/Shanghai execution date as the reference-data snapshot date; trading-calendar status is irrelevant.",
    trading_calendar_id="clock:Asia/Shanghai",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_resolve_index_basic_target,
    validate_explicit_date=_validate_index_basic_date,
)


def _tushare_configuration(context: PipelineRunContext) -> CheckResult:
    if not context.settings.external_services.tushare_token:
        return CheckResult.failure(
            "tushare_configuration",
            "TUSHARE_CONFIGURATION_MISSING",
            "TUSHARE_TOKEN must be configured by the approved QRP environment",
        )
    return CheckResult.success("tushare_configuration", configured=True)


def _snapshot_freshness(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    target = context.target_window.target_date
    return CheckResult.success(
        "index_basic_snapshot_freshness",
        target_date=target.isoformat() if target else None,
        semantics="provider snapshot is observed during this execution",
    )


def _table_completion(context: PipelineRunContext) -> CheckResult:
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            rows = int(connection.execute("SELECT COUNT(*) FROM index_basic").fetchone()[0])
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "index_basic_completion",
            "INDEX_BASIC_COMPLETION_MISSING",
            "index_basic could not be read after the write",
            exception=type(exc).__name__,
        )
    if rows <= 0:
        return CheckResult.failure(
            "index_basic_completion",
            "INDEX_BASIC_COMPLETION_MISSING",
            "index_basic contains no index definitions",
        )
    return CheckResult.success("index_basic_completion", rows=rows)


def _no_duplicate_quality(context: PipelineRunContext) -> CheckResult:
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
        try:
            duplicate_rows = connection.execute(
                """
                SELECT index_code, COUNT(*) AS row_count
                FROM index_basic
                GROUP BY index_code
                HAVING COUNT(*) > 1
                ORDER BY index_code
                """
            ).fetchall()
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "index_basic_unique_key_quality",
            "INDEX_BASIC_DUPLICATE_KEY",
            "index_basic uniqueness could not be checked",
            exception=type(exc).__name__,
        )
    if duplicate_rows:
        return CheckResult.failure(
            "index_basic_unique_key_quality",
            "INDEX_BASIC_DUPLICATE_KEY",
            "index_basic contains duplicate index codes",
            duplicate_keys=[row[0] for row in duplicate_rows],
        )
    return CheckResult.success("index_basic_unique_key_quality", duplicate_keys=0)


def _required_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ContractError("INDEX_BASIC_API_PARTIAL", ", ".join(missing))


def _parse_markets(context: PipelineRunContext) -> tuple[str, ...]:
    raw = str(context.parameter_overrides.get("markets", INDEX_BASIC_DEFAULT_MARKETS))
    markets = tuple(dict.fromkeys(part.strip().upper() for part in raw.split(",") if part.strip()))
    invalid = sorted(set(markets) - set(INDEX_BASIC_MARKETS))
    if not markets or invalid:
        raise ContractError("INDEX_BASIC_MARKETS_INVALID", ", ".join(invalid or ["empty"]))
    return markets


def _normalize_provider_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        raise ContractError("INDEX_BASIC_API_EMPTY", "all requested Tushare markets returned no rows")

    frame = pd.concat(frames, ignore_index=True, sort=False).copy()
    _required_columns(frame, INDEX_BASIC_REQUIRED_FIELDS)

    codes = frame["ts_code"].astype("string").str.strip().str.upper()
    invalid_codes = codes.isna() | ~codes.str.fullmatch(INDEX_BASIC_CODE_PATTERN.pattern, na=False)
    if invalid_codes.any():
        raise ContractError("INDEX_BASIC_API_PARTIAL", "invalid ts_code in index_basic response")
    frame["ts_code"] = codes

    for column in ("name", "market", "publisher", "category"):
        values = frame[column].astype("string").str.strip()
        if values.isna().any() or values.eq("").any():
            raise ContractError("INDEX_BASIC_API_PARTIAL", f"{column} contains empty values")
        frame[column] = values

    for column in ("base_date", "list_date"):
        parsed = pd.to_datetime(frame[column], errors="coerce")
        if parsed.isna().any():
            raise ContractError("INDEX_BASIC_API_PARTIAL", f"{column} contains invalid dates")
        frame[column] = parsed.dt.date

    if "exp_date" in frame.columns:
        raw_exp_date = frame["exp_date"].replace({"": None, "nan": None, "NaN": None})
        parsed_exp_date = pd.to_datetime(raw_exp_date, errors="coerce")
        invalid_exp_date = raw_exp_date.notna() & parsed_exp_date.isna()
        if invalid_exp_date.any():
            raise ContractError("INDEX_BASIC_API_PARTIAL", "exp_date contains invalid dates")
        frame["exp_date"] = parsed_exp_date.where(parsed_exp_date.notna(), None).dt.date

    frame["base_point"] = pd.to_numeric(frame["base_point"], errors="coerce")
    if frame["base_point"].isna().any():
        raise ContractError("INDEX_BASIC_API_PARTIAL", "base_point contains non-numeric values")

    compare_columns = [column for column in TUSHARE_INDEX_BASIC if column in frame.columns]
    duplicates = frame[frame.duplicated("ts_code", keep=False)]
    for code, group in duplicates.groupby("ts_code", sort=True):
        signatures = group.loc[:, compare_columns].astype("string").fillna("").drop_duplicates()
        if len(signatures) > 1:
            raise ContractError("INDEX_BASIC_CONFLICTING_DUPLICATE", str(code))
    return frame.drop_duplicates(subset=["ts_code"], keep="last").reset_index(drop=True)


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.rename(columns=TUSHARE_INDEX_BASIC).copy()
    prepared = align_to_schema(prepared, INDEX_BASIC.name, fill_missing_optional=True, drop_extra=True)
    return quick_validate(prepared, INDEX_BASIC.name, allow_extra=False)


def _upsert_frame(context: PipelineRunContext, frame: pd.DataFrame) -> tuple[int, float]:
    started = time.monotonic()
    connection = duckdb.connect(str(context.settings.paths.duckdb_path))
    try:
        context.execution_control.check()
        connection.execute("BEGIN TRANSACTION")
        columns = [column for column in INDEX_BASIC.column_names() if column != CREATED_AT]
        connection.register("_index_basic_rows", frame)
        try:
            connection.execute(
                f"INSERT OR REPLACE INTO {INDEX_BASIC.name} ({', '.join(columns)}) "
                f"SELECT {', '.join(columns)} FROM _index_basic_rows"
            )
        finally:
            connection.unregister("_index_basic_rows")
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()
    return len(frame), time.monotonic() - started


def execute_index_basic_update(context: PipelineRunContext) -> BusinessExecution:
    markets = _parse_markets(context)
    started = time.monotonic()
    frames: list[pd.DataFrame] = []
    rows_read = 0
    try:
        client = get_tushare_pro(settings=context.settings, execution_control=context.execution_control)
        for market in markets:
            context.execution_control.check()
            raw = client.index_basic(market=market)
            context.execution_control.check()
            if raw is None:
                continue
            if not isinstance(raw, pd.DataFrame):
                raise ContractError("INDEX_BASIC_API_FAILED", f"{market} returned {type(raw).__name__}")
            if raw.empty:
                continue
            _required_columns(raw, INDEX_BASIC_REQUIRED_FIELDS)
            frames.append(raw.copy())
            rows_read += len(raw)
    except ExecutionControlError:
        raise
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("INDEX_BASIC_API_FAILED", type(exc).__name__) from exc

    fetched_at = time.monotonic()
    normalized = _normalize_provider_frames(frames)
    prepared = _prepare_frame(normalized)
    normalized_at = time.monotonic()
    try:
        rows_written, database_seconds = _upsert_frame(context, prepared)
    except Exception as exc:
        raise ContractError("INDEX_BASIC_WRITE_FAILED", type(exc).__name__) from exc
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=rows_read,
            rows_written=rows_written,
            assets_processed=rows_written,
            dates_processed=1,
            database_write_seconds=database_seconds,
            stage_durations_seconds={
                "fetch": fetched_at - started,
                "normalize": normalized_at - fetched_at,
                "database_write": database_seconds,
            },
            api_requests=len(markets),
            batches=len(markets),
        ),
        outputs=(
            OutputResult(
                output_id="index_basic",
                rows_written=rows_written,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={"markets": list(markets), "index_count": rows_written},
            ),
        ),
    )


INDEX_BASIC_UPDATE = register_pipeline(
    PipelineContract(
        pipeline_id="index_basic_update",
        name="Tushare index basic dictionary",
        description="Synchronizes the Tushare index_basic dictionary for the requested provider markets into index_basic.",
        contract_version="1.0.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_index_basic_update,
        target_date_policy=INDEX_BASIC_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract(
                "markets",
                ParameterType.STRING,
                "Comma-separated Tushare index_basic markets.",
                default=INDEX_BASIC_DEFAULT_MARKETS,
            ),
        ),
        inputs=(
            InputContract(
                input_id="tushare_index_basic",
                kind=InputKind.EXTERNAL_API,
                source="tushare.pro.index_basic(market=...)",
                required_fields=INDEX_BASIC_REQUIRED_FIELDS,
                target_date_semantics="full provider dictionary snapshot observed during the execution",
                missing_error_code="TUSHARE_CONFIGURATION_MISSING",
                structure_check=_tushare_configuration,
                freshness=FreshnessContract(
                    check_id="index_basic_snapshot_freshness",
                    target_date_semantics="snapshot is fetched during the current execution",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
                    error_code="INDEX_BASIC_SNAPSHOT_STALE",
                    checker=_snapshot_freshness,
                ),
            ),
        ),
        outputs=(
            OutputContract(
                output_id="index_basic",
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=INDEX_BASIC.name,
                unique_key=INDEX_BASIC.primary_key,
                write_mode=WriteMode.UPSERT,
                target_date_semantics="reference-data snapshot committed during this execution",
                completion=CompletionContract(
                    marker="index_basic contains queryable index definitions",
                    error_code="INDEX_BASIC_COMPLETION_MISSING",
                    checker=_table_completion,
                ),
                quality_checks=(_no_duplicate_quality,),
                allow_empty=False,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=IdempotencyContract(
            idempotency_key="index_basic.index_code",
            repeat_run_semantics="repeated snapshots upsert the same index definitions without duplicating primary keys",
            existing_target_handling="existing definitions are replaced by the latest validated provider values",
            failure_recovery="rerun the same manual snapshot after correcting the provider or schema failure",
            uses_staging=False,
            atomic_replace_boundary="all validated dictionary rows are committed in one quant.db transaction",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="validated index_basic dictionary rows for one execution",
            failure_visibility="a failed write rolls back the complete dictionary batch",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=PerformanceBudget(
            normal_budget_seconds=120.0,
            warning_threshold_seconds=60.0,
            hard_timeout_seconds=300,
            benchmark_scope="end-to-end: one Tushare index_basic request per configured market and one dictionary upsert",
            baseline_source="internal:index_basic_update_v1",
        ),
        manual_execution_allowed=True,
    )
)


INDEX_BASIC_CONTRACTS = (INDEX_BASIC_UPDATE,)
