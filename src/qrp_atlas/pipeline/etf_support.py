"""Shared support for the independently registered ETF data Contracts."""

from __future__ import annotations

import math
import re
from datetime import date, time as clock_time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ADJ_FACTOR,
    AMOUNT,
    CHANGE,
    CLOSE,
    ETF_DAILY,
    HIGH,
    LOW,
    OPEN,
    PCT_CHANGE,
    PRE_CLOSE,
    TICKER,
    TRADE_DATE,
    TUSHARE_FUND_ADJ,
    TUSHARE_FUND_DAILY,
    VOLUME,
    align_to_schema,
    normalize_ticker,
    quick_validate,
)
from qrp_atlas.orchestration.models import OverlapPolicy

from .contracts import (
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
    PipelineRunContext,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)


CHINA_TZ = ZoneInfo("Asia/Shanghai")
DATA_AVAILABLE_AFTER = clock_time(16, 0)
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
FUND_DAILY_MAX_ROWS = 5000
FUND_ADJ_PAGE_SIZE = 2000
ETF_CODE_PATTERN = re.compile(r"^\d{6}\.(?:SH|SZ)$")

FUND_DAILY_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)
FUND_ADJ_FIELDS = ("ts_code", "trade_date", "adj_factor")


def _target_date(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("TARGET_DATE_REQUIRED")
    return target


def connect(context: PipelineRunContext, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    context.execution_control.check()
    return duckdb.connect(str(context.settings.paths.duckdb_path), read_only=read_only)


def _resolve_target_date(invocation) -> TargetWindow:
    invocation.execution_control.check()
    local = invocation.scheduled_for.astimezone(CHINA_TZ)
    cutoff_date = local.date() if local.time() >= DATA_AVAILABLE_AFTER else local.date() - timedelta(days=1)
    try:
        connection = duckdb.connect(str(invocation.settings.paths.duckdb_path), read_only=True)
        try:
            calendar_row = connection.execute(
                "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
                [cutoff_date],
            ).fetchone()
            if calendar_row is None:
                raise ContractError("TRADING_CALENDAR_STALE", cutoff_date.isoformat())
            row = connection.execute(
                """
                SELECT MAX(trade_date)
                FROM trading_calendar
                WHERE is_open = TRUE AND trade_date <= ?
                """,
                [cutoff_date],
            ).fetchone()
        finally:
            connection.close()
        invocation.execution_control.check()
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("TRADING_CALENDAR_UNAVAILABLE", type(exc).__name__) from exc
    if row is None or row[0] is None:
        raise ContractError("TRADING_CALENDAR_TARGET_UNAVAILABLE", cutoff_date.isoformat())
    return TargetWindow.for_date(row[0])


def _validate_explicit_date(target_date: date, invocation) -> bool:
    invocation.execution_control.check()
    try:
        connection = duckdb.connect(str(invocation.settings.paths.duckdb_path), read_only=True)
        try:
            row = connection.execute(
                "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
                [target_date],
            ).fetchone()
        finally:
            connection.close()
    except Exception:
        return False
    invocation.execution_control.check()
    return row is not None and row[0] is True


ETF_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="etf_close_calendar_v1",
    description=(
        "Uses the configured A-share trading calendar. Before 16:00 Asia/Shanghai it selects the previous open date; "
        "afterwards it selects the current open date, with weekends and holidays resolving to the latest open date."
    ),
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.PREVIOUS_TRADING_DAY,
    resolver=_resolve_target_date,
    validate_explicit_date=_validate_explicit_date,
)


def _require_table(
    context: PipelineRunContext,
    table_name: str,
    required_fields: tuple[str, ...],
    error_code: str,
    check_id: str | None = None,
) -> CheckResult:
    actual_check_id = check_id or f"{table_name}_structure"
    try:
        connection = connect(context, read_only=True)
        try:
            columns = set(connection.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"])
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            actual_check_id,
            error_code,
            f"required table {table_name} is unavailable",
            exception=type(exc).__name__,
        )
    missing = sorted(set(required_fields) - columns)
    if missing:
        return CheckResult.failure(
            actual_check_id,
            error_code,
            f"required table {table_name} is missing fields",
            missing_fields=missing,
        )
    return CheckResult.success(actual_check_id, table=table_name, fields=sorted(columns))


def _calendar_structure(context: PipelineRunContext) -> CheckResult:
    return _require_table(
        context,
        "trading_calendar",
        (TRADE_DATE, "is_open"),
        "TRADING_CALENDAR_STRUCTURE_MISSING",
        check_id="trading_calendar_structure",
    )


def _calendar_freshness(context: PipelineRunContext) -> CheckResult:
    target = _target_date(context)
    try:
        connection = connect(context, read_only=True)
        try:
            row = connection.execute(
                "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
                [target],
            ).fetchone()
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "trading_calendar_target_freshness",
            "TRADING_CALENDAR_STALE",
            "trading calendar could not be read",
            exception=type(exc).__name__,
            target_date=target.isoformat(),
        )
    if row is None or row[0] is not True:
        return CheckResult.failure(
            "trading_calendar_target_freshness",
            "TRADING_CALENDAR_STALE",
            "target date is not an open trading date in the configured calendar",
            target_date=target.isoformat(),
        )
    return CheckResult.success("trading_calendar_target_freshness", target_date=target.isoformat(), is_open=True)


def calendar_input() -> InputContract:
    return InputContract(
        input_id="trading_calendar",
        kind=InputKind.TABLE,
        source="quant_db.trading_calendar",
        required_fields=(TRADE_DATE, "is_open"),
        target_date_semantics="target must be an open date in the configured A-share calendar",
        missing_error_code="TRADING_CALENDAR_STRUCTURE_MISSING",
        structure_check=_calendar_structure,
        freshness=FreshnessContract(
            check_id="trading_calendar_target_freshness",
            target_date_semantics="target open date is present with zero trading-day lag",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.PREVIOUS_TRADING_DAY,
            error_code="TRADING_CALENDAR_STALE",
            checker=_calendar_freshness,
        ),
    )


def _tushare_configuration(context: PipelineRunContext) -> CheckResult:
    if not context.settings.external_services.tushare_token:
        return CheckResult.failure(
            "tushare_configuration",
            "TUSHARE_CONFIGURATION_MISSING",
            "TUSHARE_TOKEN must be configured by the approved QRP environment",
        )
    return CheckResult.success("tushare_configuration", configured=True, provider="tushare")


def _external_target_freshness(input_id: str, error_code: str):
    def check(context: PipelineRunContext) -> CheckResult:
        result = _calendar_freshness(context)
        if result.passed:
            return CheckResult.success(
                f"{input_id}_target_freshness",
                **dict(result.observed),
            )
        return CheckResult.failure(
            f"{input_id}_target_freshness",
            error_code,
            result.detail or "target date is not available for the external provider",
            **dict(result.observed),
        )

    check.__name__ = f"{input_id}_target_freshness"
    return check


def external_input(input_id: str, source: str, fields: tuple[str, ...]) -> InputContract:
    stale_error_code = f"{input_id.upper()}_STALE"
    return InputContract(
        input_id=input_id,
        kind=InputKind.EXTERNAL_API,
        source=source,
        required_fields=fields,
        target_date_semantics="provider response must be scoped to the resolved target trading date",
        missing_error_code="TUSHARE_CONFIGURATION_MISSING",
        structure_check=_tushare_configuration,
        freshness=FreshnessContract(
            check_id=f"{input_id}_target_freshness",
            target_date_semantics="target-date response is validated by the executor before any transaction",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.REJECT,
            error_code=stale_error_code,
            checker=_external_target_freshness(input_id, stale_error_code),
        ),
    )


def table_target_input(
    *,
    input_id: str,
    pipeline_id: str,
    table,
    required_fields: tuple[str, ...],
) -> InputContract:
    def structure(context: PipelineRunContext) -> CheckResult:
        return _require_table(
            context,
            table.name,
            required_fields,
            "ETF_DAILY_INPUT_STRUCTURE_MISSING",
            check_id=f"{input_id}_structure",
        )

    def freshness(context: PipelineRunContext) -> CheckResult:
        target = _target_date(context)
        try:
            connection = connect(context, read_only=True)
            try:
                rows = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM {table.name} WHERE trade_date = ?",
                        [target],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(
                f"{input_id}_freshness",
                "ETF_DAILY_INPUT_STALE",
                f"{table.name} target could not be checked",
                exception=type(exc).__name__,
            )
        if rows <= 0:
            return CheckResult.failure(
                f"{input_id}_freshness",
                "ETF_DAILY_INPUT_STALE",
                f"{pipeline_id} has not produced the target trading date",
                target_date=target.isoformat(),
            )
        return CheckResult.success(f"{input_id}_freshness", target_date=target.isoformat(), rows=rows)

    structure.__name__ = f"{input_id}_structure"
    freshness.__name__ = f"{input_id}_freshness"
    return InputContract(
        input_id=input_id,
        kind=InputKind.UPSTREAM_PIPELINE,
        source=f"{pipeline_id} / quant_db.{table.name}",
        required_fields=required_fields,
        target_date_semantics="same target trading date must contain the upstream ETF snapshot",
        missing_error_code="ETF_DAILY_INPUT_STRUCTURE_MISSING",
        structure_check=structure,
        freshness=FreshnessContract(
            check_id=f"{input_id}_freshness",
            target_date_semantics="same target trading date is required with zero lag",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.REJECT,
            error_code="ETF_DAILY_INPUT_STALE",
            checker=freshness,
        ),
        upstream_pipeline_id=pipeline_id,
    )


def output(
    output_id: str,
    table,
    completion,
    *,
    allow_empty: bool = False,
) -> OutputContract:
    return OutputContract(
        output_id=output_id,
        physical_resource=QUANT_DB_RESOURCE,
        location="settings.paths.duckdb_path",
        object_name=table.name,
        unique_key=table.primary_key,
        write_mode=WriteMode.REPLACE_TARGET_DATE,
        target_date_semantics="resolved target trading date",
        completion=CompletionContract(
            marker=f"{table.name} committed target-date output is queryable",
            error_code=f"{output_id.upper()}_COMPLETION_MISSING",
            checker=completion,
        ),
        quality_checks=(no_duplicate_quality(table, f"{output_id.upper()}_DUPLICATE_KEY"),),
        allow_empty=allow_empty,
    )


def _target_row_count(context: PipelineRunContext, table_name: str) -> int:
    connection = connect(context, read_only=True)
    try:
        return int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE trade_date = ?",
                [_target_date(context)],
            ).fetchone()[0]
        )
    finally:
        connection.close()


def non_empty_completion(table_name: str, error_code: str):
    def check(context: PipelineRunContext) -> CheckResult:
        try:
            rows = _target_row_count(context, table_name)
        except Exception as exc:
            return CheckResult.failure(
                f"{table_name}_completion",
                error_code,
                "output table could not be read after write",
                exception=type(exc).__name__,
            )
        if rows <= 0:
            return CheckResult.failure(
                f"{table_name}_completion",
                error_code,
                "target date has no persisted output rows",
                target_date=_target_date(context).isoformat(),
            )
        return CheckResult.success(f"{table_name}_completion", rows=rows)

    check.__name__ = f"{table_name}_non_empty_completion"
    return check


def no_duplicate_quality(table, error_code: str):
    keys = ", ".join(table.primary_key)

    def check(context: PipelineRunContext) -> CheckResult:
        try:
            connection = connect(context, read_only=True)
            try:
                duplicate_rows = int(
                    connection.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM (
                            SELECT {keys}, COUNT(*) AS row_count
                            FROM {table.name}
                            WHERE trade_date = ?
                            GROUP BY {keys}
                            HAVING COUNT(*) > 1
                        )
                        """,
                        [_target_date(context)],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(
                f"{table.name}_unique_key_quality",
                error_code,
                "output key quality could not be checked",
                exception=type(exc).__name__,
            )
        if duplicate_rows:
            return CheckResult.failure(
                f"{table.name}_unique_key_quality",
                error_code,
                "output contains duplicate logical keys for target date",
                duplicate_keys=duplicate_rows,
            )
        return CheckResult.success(f"{table.name}_unique_key_quality", duplicate_keys=0)

    check.__name__ = f"{table.name}_no_duplicate_quality"
    return check


def prepare_frame(frame: pd.DataFrame, table) -> pd.DataFrame:
    prepared = align_to_schema(frame, table.name, fill_missing_optional=True, drop_extra=True)
    return quick_validate(prepared, table.name, allow_extra=False)


def _insert_frame(connection: duckdb.DuckDBPyConnection, table, frame: pd.DataFrame) -> None:
    columns = list(table.column_names())
    if "created_at" in columns:
        columns.remove("created_at")
    connection.register("_etf_contract_rows", frame)
    try:
        connection.execute(
            f"INSERT INTO {table.name} ({', '.join(columns)}) "
            f"SELECT {', '.join(columns)} FROM _etf_contract_rows"
        )
    finally:
        connection.unregister("_etf_contract_rows")


def replace_target_date(
    context: PipelineRunContext,
    table,
    frame: pd.DataFrame,
    target: date,
) -> tuple[int, float]:
    prepared = prepare_frame(frame, table)
    import time

    started = time.monotonic()
    connection = connect(context)
    try:
        context.execution_control.check()
        connection.execute("BEGIN TRANSACTION")
        connection.execute(f"DELETE FROM {table.name} WHERE trade_date = ?", [target])
        if not prepared.empty:
            _insert_frame(connection, table, prepared)
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()
    return len(prepared), time.monotonic() - started


def _required_columns(frame: pd.DataFrame, required: tuple[str, ...], error_code: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ContractError(error_code, ", ".join(missing))


def _normalize_code(value: Any, error_code: str) -> str:
    if value is None or pd.isna(value):
        raise ContractError(error_code, "ts_code contains null")
    normalized = normalize_ticker(str(value))
    if ETF_CODE_PATTERN.fullmatch(normalized) is None:
        raise ContractError(error_code, f"invalid ETF ts_code: {value}")
    return normalized


def _normalize_dates(frame: pd.DataFrame, target: date, error_code: str) -> None:
    parsed = pd.to_datetime(frame[TRADE_DATE], errors="coerce")
    if parsed.isna().any():
        raise ContractError(error_code, "trade_date contains invalid values")
    dates = parsed.dt.date
    if set(dates) != {target}:
        raise ContractError(error_code, f"response trade_date must be exactly {target.isoformat()}")
    frame[TRADE_DATE] = dates


def _normalize_numeric(frame: pd.DataFrame, columns: tuple[str, ...], error_code: str) -> None:
    for column in columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or any(not math.isfinite(float(value)) for value in values):
            raise ContractError(error_code, f"{column} contains non-finite values")
        frame[column] = values


def _deduplicate(frame: pd.DataFrame, key: list[str], error_code: str) -> pd.DataFrame:
    frame = frame.drop_duplicates().reset_index(drop=True)
    if frame.duplicated(subset=key).any():
        raise ContractError(error_code, "response contains conflicting duplicate logical keys")
    return frame


def normalize_fund_daily(raw: pd.DataFrame, target: date) -> pd.DataFrame:
    if not isinstance(raw, pd.DataFrame):
        if raw is None:
            raise ContractError("ETF_DAILY_API_EMPTY", target.isoformat())
        raise ContractError("ETF_DAILY_API_FAILED", f"unexpected response type {type(raw).__name__}")
    if raw.empty:
        raise ContractError("ETF_DAILY_API_EMPTY", target.isoformat())
    _required_columns(raw, FUND_DAILY_FIELDS, "ETF_DAILY_API_PARTIAL")
    frame = raw.loc[:, FUND_DAILY_FIELDS].rename(columns=TUSHARE_FUND_DAILY).copy()
    _normalize_dates(frame, target, "ETF_DAILY_API_PARTIAL")
    try:
        frame[TICKER] = frame[TICKER].map(lambda value: _normalize_code(value, "ETF_DAILY_API_PARTIAL"))
    except ContractError:
        raise
    _normalize_numeric(
        frame,
        (OPEN, HIGH, LOW, CLOSE, PRE_CLOSE, CHANGE, PCT_CHANGE, VOLUME, AMOUNT),
        "ETF_DAILY_API_PARTIAL",
    )
    volume_in_shares = frame[VOLUME] * 100
    if (volume_in_shares % 1 != 0).any():
        raise ContractError("ETF_DAILY_API_PARTIAL", "vol cannot be represented as whole shares")
    frame[VOLUME] = volume_in_shares.astype("int64")
    frame[AMOUNT] = frame[AMOUNT] * 1000
    frame = _deduplicate(frame, [TRADE_DATE, TICKER], "ETF_DAILY_API_PARTIAL")
    return frame.loc[:, [TRADE_DATE, TICKER, OPEN, HIGH, LOW, CLOSE, PRE_CLOSE, CHANGE, PCT_CHANGE, VOLUME, AMOUNT]]


def normalize_fund_adj(raw: pd.DataFrame, target: date) -> pd.DataFrame:
    if not isinstance(raw, pd.DataFrame):
        if raw is None:
            raise ContractError("ETF_ADJ_FACTOR_API_EMPTY", target.isoformat())
        raise ContractError("ETF_ADJ_FACTOR_API_FAILED", f"unexpected response type {type(raw).__name__}")
    if raw.empty:
        raise ContractError("ETF_ADJ_FACTOR_API_EMPTY", target.isoformat())
    _required_columns(raw, FUND_ADJ_FIELDS, "ETF_ADJ_FACTOR_API_PARTIAL")
    frame = raw.loc[:, FUND_ADJ_FIELDS].rename(columns=TUSHARE_FUND_ADJ).copy()
    _normalize_dates(frame, target, "ETF_ADJ_FACTOR_API_PARTIAL")
    frame[TICKER] = frame[TICKER].map(lambda value: _normalize_code(value, "ETF_ADJ_FACTOR_API_PARTIAL"))
    _normalize_numeric(frame, (ADJ_FACTOR,), "ETF_ADJ_FACTOR_API_PARTIAL")
    if (frame[ADJ_FACTOR] <= 0).any():
        raise ContractError("ETF_ADJ_FACTOR_API_PARTIAL", "adj_factor must be positive")
    frame = _deduplicate(frame, [TICKER, TRADE_DATE], "ETF_ADJ_FACTOR_API_PARTIAL")
    return frame.loc[:, [TICKER, TRADE_DATE, ADJ_FACTOR]]


def fetch_fund_adj_pages(client: Any, target: date, execution_control) -> tuple[pd.DataFrame, int]:
    frames: list[pd.DataFrame] = []
    offset = 0
    requests = 0
    while True:
        execution_control.check()
        raw = client.fund_adj(
            trade_date=target.strftime("%Y%m%d"),
            offset=str(offset),
            limit=str(FUND_ADJ_PAGE_SIZE),
        )
        execution_control.check()
        requests += 1
        if raw is None:
            raise ContractError("ETF_ADJ_FACTOR_API_FAILED", "fund_adj returned None")
        if not isinstance(raw, pd.DataFrame):
            raise ContractError("ETF_ADJ_FACTOR_API_FAILED", f"unexpected response type {type(raw).__name__}")
        if raw.empty:
            if not frames:
                raise ContractError("ETF_ADJ_FACTOR_API_EMPTY", target.isoformat())
            break
        frames.append(raw.copy())
        if len(raw) < FUND_ADJ_PAGE_SIZE:
            break
        offset += len(raw)
        if requests >= 10000:
            raise ContractError("ETF_ADJ_FACTOR_API_PARTIAL", "fund_adj pagination did not reach a short page")
    return pd.concat(frames, ignore_index=True, sort=False), requests


def expected_etf_codes(context: PipelineRunContext, target: date) -> set[str]:
    try:
        connection = connect(context, read_only=True)
        try:
            values = connection.execute(
                f"SELECT {TICKER} FROM {ETF_DAILY.name} WHERE {TRADE_DATE} = ?",
                [target],
            ).fetchall()
        finally:
            connection.close()
    except Exception as exc:
        raise ContractError("ETF_DAILY_INPUT_STALE", type(exc).__name__) from exc
    codes = {str(value[0]).strip().upper() for value in values if value[0] is not None}
    if not codes:
        raise ContractError("ETF_DAILY_INPUT_STALE", target.isoformat())
    return codes


def idempotency(table_name: str, *, repeat_run_semantics: str, recovery: str) -> IdempotencyContract:
    return IdempotencyContract(
        idempotency_key=f"{table_name}.trade_date plus its schema primary key",
        repeat_run_semantics=repeat_run_semantics,
        existing_target_handling=repeat_run_semantics,
        failure_recovery=recovery,
        uses_staging=False,
        atomic_replace_boundary="one target trading date in the single quant.db transaction",
    )


def execution_policy() -> ExecutionPolicy:
    return ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1)


def transaction(table_name: str) -> TransactionContract:
    return TransactionContract(
        mode=TransactionMode.DATABASE_TRANSACTION,
        boundary=f"{table_name} rows for one target trading date",
        failure_visibility="target-date deletion and replacement commit together or the prior target set remains visible",
    )
