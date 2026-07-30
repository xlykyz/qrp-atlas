"""Formal source contracts for the daily market-data production Pipelines.

The module owns the production lifecycle semantics for the six daily market-data
tasks.  It deliberately uses the existing table schemas, cleaners and runtime;
there is no second scheduler or a shell based orchestration layer here.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import UTC, date, datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import akshare as ak
import duckdb
import pandas as pd

from qrp_atlas.config.tushare_client import get_tushare_pro
from qrp_atlas.contracts import (
    ADJ_FACTOR_CHANGES,
    CREATED_AT,
    DAILY_BASIC,
    DAILY_MARKET_SNAPSHOT,
    DT_POOL,
    INDEX_DAILY,
    SUSPEND_D,
    TRADING_CALENDAR,
    ZT_POOL,
    align_to_schema,
    normalize_ticker,
    quick_validate,
)

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
    PerformanceBudget,
    PipelineContract,
    PipelineMetrics,
    PipelineRunContext,
    PipelineKind,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .daily_basic.clean import clean_daily_basic
from .daily_update.clean import clean_daily_snapshot
from .daily_update.enrich import enrich_daily_snapshot
from .registry import register_pipeline
from .runtime.models import OverlapPolicy


CHINA_TZ = ZoneInfo("Asia/Shanghai")
DATA_AVAILABLE_AFTER = clock_time(16, 0)
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
INDEX_SERIES: tuple[tuple[str, str], ...] = (
    ("sh000001", "上证综指"),
    ("sz399001", "深证成指"),
    ("sz399006", "创业板指"),
    ("sh000688", "科创50"),
)


def _target_date(context: PipelineRunContext) -> date:
    target = context.target_window.target_date
    if target is None:
        raise ContractError("TARGET_DATE_REQUIRED")
    return target


def _connect(context: PipelineRunContext, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(context.settings.paths.duckdb_path), read_only=read_only)


def _table_columns(context: PipelineRunContext, table_name: str) -> set[str]:
    connection = _connect(context, read_only=True)
    try:
        return set(connection.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"].tolist())
    finally:
        connection.close()


def _require_table(context: PipelineRunContext, table_name: str, required_fields: tuple[str, ...], code: str) -> CheckResult:
    try:
        columns = _table_columns(context, table_name)
    except Exception as exc:
        return CheckResult.failure(
            f"{table_name}_structure",
            code,
            f"required table {table_name} is unavailable",
            exception=type(exc).__name__,
        )
    missing = sorted(set(required_fields) - columns)
    if missing:
        return CheckResult.failure(
            f"{table_name}_structure",
            code,
            f"required table {table_name} is missing fields",
            missing_fields=missing,
        )
    return CheckResult.success(f"{table_name}_structure", table=table_name, fields=sorted(columns))


def _calendar_structure(context: PipelineRunContext) -> CheckResult:
    return _require_table(
        context,
        TRADING_CALENDAR.name,
        ("trade_date", "is_open"),
        "TRADING_CALENDAR_STRUCTURE_MISSING",
    )


def _calendar_has_open_date(context: PipelineRunContext, *, error_code: str, check_id: str) -> CheckResult:
    target = _target_date(context)
    try:
        connection = _connect(context, read_only=True)
        try:
            row = connection.execute(
                "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
                [target],
            ).fetchone()
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            check_id,
            error_code,
            "trading calendar could not be read",
            exception=type(exc).__name__,
            target_date=target.isoformat(),
        )
    if row is None or row[0] is not True:
        return CheckResult.failure(
            check_id,
            error_code,
            "target date is not an open trading date in the configured calendar",
            target_date=target.isoformat(),
        )
    return CheckResult.success(check_id, target_date=target.isoformat(), is_open=True)


def _calendar_freshness(context: PipelineRunContext) -> CheckResult:
    return _calendar_has_open_date(
        context,
        error_code="TRADING_CALENDAR_STALE",
        check_id="trading_calendar_target_freshness",
    )


def _resolve_market_target_date(invocation) -> TargetWindow:
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
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("TRADING_CALENDAR_UNAVAILABLE", type(exc).__name__) from exc
    if row is None or row[0] is None:
        raise ContractError("TRADING_CALENDAR_TARGET_UNAVAILABLE", cutoff_date.isoformat())
    return TargetWindow.for_date(row[0])


def _valid_explicit_market_date(target_date: date, invocation) -> bool:
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
    return row is not None and row[0] is True


MARKET_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="cn_a_share_close_calendar_v1",
    description=(
        "Uses the configured trading_calendar. Before 16:00 Asia/Shanghai it selects the previous open date; "
        "afterwards it selects the current open date, with weekends and holidays resolving to the latest open date."
    ),
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.PREVIOUS_TRADING_DAY,
    resolver=_resolve_market_target_date,
    validate_explicit_date=_valid_explicit_market_date,
)


def _tushare_configuration(context: PipelineRunContext) -> CheckResult:
    if not context.settings.external_services.tushare_token:
        return CheckResult.failure(
            "tushare_configuration",
            "TUSHARE_CONFIGURATION_MISSING",
            "TUSHARE_TOKEN must be configured by the approved QRP environment",
        )
    return CheckResult.success("tushare_configuration", configured=True)


def _akshare_configuration(_context: PipelineRunContext) -> CheckResult:
    return CheckResult.success("akshare_configuration", client="akshare")


def _eastmoney_configuration(_context: PipelineRunContext) -> CheckResult:
    return CheckResult.success("eastmoney_configuration", endpoint="push2ex.eastmoney.com")


def _market_history_structure(context: PipelineRunContext) -> CheckResult:
    return _require_table(
        context,
        DAILY_MARKET_SNAPSHOT.name,
        ("trade_date", "ticker", "close", "pre_close"),
        "MARKET_HISTORY_STRUCTURE_MISSING",
    )


def _market_history_freshness(context: PipelineRunContext) -> CheckResult:
    """Only history before the replay target is eligible for enrichment."""

    target = _target_date(context)
    try:
        connection = _connect(context, read_only=True)
        try:
            row = connection.execute(
                "SELECT MAX(trade_date) FROM daily_market_snapshot WHERE trade_date < ?",
                [target],
            ).fetchone()
            future_rows = connection.execute(
                "SELECT COUNT(*) FROM daily_market_snapshot WHERE trade_date > ?",
                [target],
            ).fetchone()[0]
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "market_history_freshness",
            "MARKET_HISTORY_STALE",
            "daily market history could not be checked",
            exception=type(exc).__name__,
        )
    latest = row[0] if row else None
    return CheckResult.success(
        "market_history_freshness",
        latest_eligible_trade_date=latest.isoformat() if latest else None,
        target_date=target.isoformat(),
        future_rows_ignored=future_rows,
    )


def _market_target_structure(context: PipelineRunContext) -> CheckResult:
    return _require_table(
        context,
        DAILY_MARKET_SNAPSHOT.name,
        ("trade_date", "ticker", "close"),
        "MARKET_DAILY_INPUT_STRUCTURE_MISSING",
    )


def _market_target_freshness(context: PipelineRunContext) -> CheckResult:
    target = _target_date(context)
    try:
        connection = _connect(context, read_only=True)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM daily_market_snapshot WHERE trade_date = ?",
                [target],
            ).fetchone()[0]
        finally:
            connection.close()
    except Exception as exc:
        return CheckResult.failure(
            "market_daily_target_freshness",
            "MARKET_DAILY_INPUT_STALE",
            "market daily target could not be checked",
            exception=type(exc).__name__,
        )
    if count <= 0:
        return CheckResult.failure(
            "market_daily_target_freshness",
            "MARKET_DAILY_INPUT_STALE",
            "market_daily_update has not produced the target trading date",
            target_date=target.isoformat(),
        )
    return CheckResult.success("market_daily_target_freshness", target_date=target.isoformat(), rows=count)


def _expected_market_output_tickers(context: PipelineRunContext, target: date, *, error_code: str) -> set[str]:
    try:
        connection = _connect(context, read_only=True)
        try:
            expected = {
                normalize_ticker(row[0])
                for row in connection.execute(
                    "SELECT ticker FROM daily_market_snapshot WHERE trade_date = ?",
                    [target],
                ).fetchall()
            }
        finally:
            connection.close()
    except Exception as exc:
        raise ContractError(error_code, type(exc).__name__) from exc
    if not expected:
        raise ContractError(error_code, f"no market_daily_update tickers for {target.isoformat()}")
    return expected


def _external_target_freshness(error_code: str, input_id: str):
    """Require a calendar-supported target before asking a date-bound provider."""

    def check(context: PipelineRunContext) -> CheckResult:
        return _calendar_has_open_date(
            context,
            error_code=error_code,
            check_id=f"{input_id}_target_freshness",
        )

    check.__name__ = f"{input_id}_target_freshness"
    return check


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8")
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _prepare_frame(frame: pd.DataFrame, table) -> pd.DataFrame:
    prepared = align_to_schema(frame, table.name, fill_missing_optional=True, drop_extra=True)
    return quick_validate(prepared, table.name, allow_extra=False)


def _insert_frame(connection: duckdb.DuckDBPyConnection, table, frame: pd.DataFrame) -> None:
    columns = [column for column in table.column_names() if column != CREATED_AT]
    connection.register("_contract_rows", frame)
    try:
        connection.execute(
            f"INSERT INTO {table.name} ({', '.join(columns)}) "
            f"SELECT {', '.join(columns)} FROM _contract_rows"
        )
    finally:
        connection.unregister("_contract_rows")


def _replace_target_date(
    context: PipelineRunContext,
    table,
    frame: pd.DataFrame,
    target: date,
) -> tuple[int, float]:
    prepared = _prepare_frame(frame, table)
    started = time.monotonic()
    connection = _connect(context)
    try:
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


def _upsert_frame(context: PipelineRunContext, table, frame: pd.DataFrame) -> tuple[int, float]:
    prepared = _prepare_frame(frame, table)
    started = time.monotonic()
    connection = _connect(context)
    try:
        connection.execute("BEGIN TRANSACTION")
        if not prepared.empty:
            columns = [column for column in table.column_names() if column != CREATED_AT]
            connection.register("_contract_rows", prepared)
            try:
                connection.execute(
                    f"INSERT OR REPLACE INTO {table.name} ({', '.join(columns)}) "
                    f"SELECT {', '.join(columns)} FROM _contract_rows"
                )
            finally:
                connection.unregister("_contract_rows")
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


def _replace_zt_dt_target(
    context: PipelineRunContext,
    target: date,
    zt_frame: pd.DataFrame,
    dt_frame: pd.DataFrame,
) -> tuple[int, int, float]:
    zt_prepared = _prepare_frame(zt_frame, ZT_POOL)
    dt_prepared = _prepare_frame(dt_frame, DT_POOL)
    started = time.monotonic()
    connection = _connect(context)
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute("DELETE FROM zt_pool WHERE trade_date = ?", [target])
        connection.execute("DELETE FROM dt_pool WHERE trade_date = ?", [target])
        if not zt_prepared.empty:
            _insert_frame(connection, ZT_POOL, zt_prepared)
        if not dt_prepared.empty:
            _insert_frame(connection, DT_POOL, dt_prepared)
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        connection.close()
    return len(zt_prepared), len(dt_prepared), time.monotonic() - started


def _target_row_count(context: PipelineRunContext, table_name: str) -> int:
    connection = _connect(context, read_only=True)
    try:
        return int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE trade_date = ?",
                [_target_date(context)],
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _non_empty_completion(table_name: str, error_code: str):
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


def _index_completion(context: PipelineRunContext) -> CheckResult:
    try:
        rows = _target_row_count(context, INDEX_DAILY.name)
    except Exception as exc:
        return CheckResult.failure(
            "index_daily_completion",
            "INDEX_DAILY_COMPLETION_MISSING",
            "index output table could not be read after write",
            exception=type(exc).__name__,
        )
    if rows != len(INDEX_SERIES):
        return CheckResult.failure(
            "index_daily_completion",
            "INDEX_DAILY_COMPLETION_MISSING",
            "target date does not contain all required index series",
            expected=len(INDEX_SERIES),
            actual=rows,
        )
    return CheckResult.success("index_daily_completion", rows=rows)


def _allowed_empty_completion(table_name: str, error_code: str):
    """Verify the physical target is queryable; zero rows is a valid empty snapshot."""

    def check(context: PipelineRunContext) -> CheckResult:
        try:
            rows = _target_row_count(context, table_name)
        except Exception as exc:
            return CheckResult.failure(
                f"{table_name}_completion",
                error_code,
                "output table could not be read after committed replacement",
                exception=type(exc).__name__,
            )
        return CheckResult.success(f"{table_name}_completion", rows=rows, empty_snapshot=rows == 0)

    check.__name__ = f"{table_name}_allowed_empty_completion"
    return check


def _no_duplicate_quality(table, error_code: str):
    keys = ", ".join(table.primary_key)

    def check(context: PipelineRunContext) -> CheckResult:
        target = _target_date(context)
        try:
            connection = _connect(context, read_only=True)
            try:
                duplicates = connection.execute(
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
                    [target],
                ).fetchone()[0]
            finally:
                connection.close()
        except Exception as exc:
            return CheckResult.failure(
                f"{table.name}_unique_key_quality",
                error_code,
                "output key quality could not be checked",
                exception=type(exc).__name__,
            )
        if duplicates:
            return CheckResult.failure(
                f"{table.name}_unique_key_quality",
                error_code,
                "output contains duplicate logical keys for target date",
                duplicate_keys=duplicates,
            )
        return CheckResult.success(f"{table.name}_unique_key_quality", duplicate_keys=0)

    check.__name__ = f"{table.name}_no_duplicate_quality"
    return check


def _required_columns(frame: pd.DataFrame, columns: tuple[str, ...], code: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ContractError(code, ", ".join(missing))


def _ensure_target_rows(frame: pd.DataFrame, target: date, code: str) -> None:
    if frame.empty:
        raise ContractError(code, "external API returned no rows")
    values = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
    if values.isna().any() or set(values) != {target}:
        raise ContractError(code, f"response trade_date must be exactly {target.isoformat()}")


def _ensure_expected_ticker_coverage(
    frame: pd.DataFrame,
    expected: set[str],
    *,
    ticker_column: str,
    code: str,
) -> None:
    """Reject a target-day provider response that omits expected securities."""

    received = {
        normalize_ticker(value)
        for value in frame[ticker_column].dropna().astype(str)
    }
    missing = sorted(expected - received)
    if missing:
        raise ContractError(
            code,
            f"missing {len(missing)} expected tickers; examples={','.join(missing[:10])}",
        )


def _metrics(
    *,
    rows_read: int,
    rows_written: int,
    assets_processed: int,
    database_write_seconds: float,
    stages: dict[str, float],
    api_requests: int,
    batches: int = 1,
) -> PipelineMetrics:
    return PipelineMetrics(
        rows_read=rows_read,
        rows_written=rows_written,
        assets_processed=assets_processed,
        dates_processed=1,
        database_write_seconds=database_write_seconds,
        stage_durations_seconds=stages,
        api_requests=api_requests,
        batches=batches,
    )


def execute_market_daily_update(context: PipelineRunContext) -> BusinessExecution:
    target = _target_date(context)
    started = time.monotonic()
    try:
        client = get_tushare_pro(settings=context.settings)
        raw = client.daily(trade_date=target.strftime("%Y%m%d"))
        if raw is None or raw.empty:
            raise ContractError("MARKET_DAILY_API_EMPTY", target.isoformat())
        _required_columns(raw, ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"), "MARKET_DAILY_API_PARTIAL")
        # Tushare daily has no authoritative count or tradability field.  Do
        # not fabricate one from stock_info or unscheduled suspend_d data.
        _ensure_target_rows(raw, target, "MARKET_DAILY_API_PARTIAL")
        cleaned = clean_daily_snapshot(raw, source="tushare_daily")
        _ensure_target_rows(cleaned, target, "MARKET_DAILY_CLEAN_EMPTY")
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("MARKET_DAILY_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()
    try:
        raw_path = context.settings.paths.raw_dir / "daily_snapshot" / str(target.year) / f"{target.isoformat()}_Astock_tushare.csv"
        canonical_path = context.settings.paths.canonical_dir / "daily_market_snapshot" / f"{target.isoformat()}.csv"
        _atomic_csv(raw, raw_path)
        _atomic_csv(cleaned, canonical_path)
        connection = _connect(context)
        try:
            enriched = enrich_daily_snapshot(cleaned, target, connection)
            _ensure_target_rows(enriched, target, "MARKET_DAILY_ENRICH_EMPTY")
            prepared = _prepare_frame(enriched, DAILY_MARKET_SNAPSHOT)
            write_started = time.monotonic()
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute("DELETE FROM daily_market_snapshot WHERE trade_date = ?", [target])
                _insert_frame(connection, DAILY_MARKET_SNAPSHOT, prepared)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        finally:
            connection.close()
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("MARKET_DAILY_WRITE_FAILED", type(exc).__name__) from exc
    database_seconds = time.monotonic() - write_started
    rows = len(prepared)
    return BusinessExecution.success(
        metrics=_metrics(
            rows_read=len(raw),
            rows_written=rows,
            assets_processed=prepared["ticker"].nunique(),
            database_write_seconds=database_seconds,
            stages={"fetch": fetched_at - started, "clean_enrich_and_write": time.monotonic() - fetched_at},
            api_requests=1,
        ),
        outputs=(
            OutputResult(
                output_id="daily_market_snapshot",
                rows_written=rows,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={
                    "target_date": target.isoformat(),
                    "raw_snapshot": str(raw_path),
                    "canonical_snapshot": str(canonical_path),
                },
            ),
        ),
    )


def execute_daily_basic_update(context: PipelineRunContext) -> BusinessExecution:
    target = _target_date(context)
    started = time.monotonic()
    try:
        expected = _expected_market_output_tickers(context, target, error_code="DAILY_BASIC_COVERAGE_UNAVAILABLE")
        raw = get_tushare_pro(settings=context.settings).daily_basic(trade_date=target.strftime("%Y%m%d"))
        if raw is None or raw.empty:
            raise ContractError("DAILY_BASIC_API_EMPTY", target.isoformat())
        _required_columns(raw, ("ts_code", "trade_date", "close"), "DAILY_BASIC_API_PARTIAL")
        _ensure_target_rows(raw, target, "DAILY_BASIC_API_PARTIAL")
        _ensure_expected_ticker_coverage(raw, expected, ticker_column="ts_code", code="DAILY_BASIC_API_PARTIAL")
        cleaned = clean_daily_basic(raw)
        _ensure_target_rows(cleaned, target, "DAILY_BASIC_CLEAN_EMPTY")
        _ensure_expected_ticker_coverage(cleaned, expected, ticker_column="ticker", code="DAILY_BASIC_API_PARTIAL")
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("DAILY_BASIC_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()
    try:
        rows, database_seconds = _replace_target_date(context, DAILY_BASIC, cleaned, target)
    except Exception as exc:
        raise ContractError("DAILY_BASIC_WRITE_FAILED", type(exc).__name__) from exc
    return BusinessExecution.success(
        metrics=_metrics(
            rows_read=len(raw),
            rows_written=rows,
            assets_processed=cleaned["ticker"].nunique(),
            database_write_seconds=database_seconds,
            stages={"fetch_and_clean": fetched_at - started, "database_write": database_seconds},
            api_requests=1,
        ),
        outputs=(
            OutputResult(
                output_id="daily_basic",
                rows_written=rows,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={"target_date": target.isoformat(), "expected_tickers": len(expected)},
            ),
        ),
    )


def execute_adj_factor_daily(context: PipelineRunContext) -> BusinessExecution:
    target = _target_date(context)
    started = time.monotonic()
    try:
        connection = _connect(context, read_only=True)
        try:
            expected = {
                normalize_ticker(row[0])
                for row in connection.execute(
                    "SELECT ticker FROM daily_market_snapshot WHERE trade_date = ?", [target]
                ).fetchall()
            }
            if not expected:
                raise ContractError("ADJ_FACTOR_API_PARTIAL", f"no market_daily_update tickers for {target.isoformat()}")
            previous = dict(
                connection.execute(
                    """
                    SELECT ticker, adj_factor
                    FROM (
                        SELECT ticker, adj_factor,
                               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS row_number
                        FROM adj_factor_changes
                        WHERE trade_date < ?
                    )
                    WHERE row_number = 1
                    """,
                    [target],
                ).fetchall()
            )
        finally:
            connection.close()
        raw = get_tushare_pro(settings=context.settings).adj_factor(trade_date=target.strftime("%Y%m%d"))
        if raw is None or raw.empty:
            raise ContractError("ADJ_FACTOR_API_EMPTY", target.isoformat())
        _required_columns(raw, ("ts_code", "trade_date", "adj_factor"), "ADJ_FACTOR_API_PARTIAL")
        _ensure_target_rows(raw, target, "ADJ_FACTOR_API_PARTIAL")
        received = {normalize_ticker(value) for value in raw["ts_code"].dropna().astype(str)}
        missing = sorted(expected - received)
        if missing:
            raise ContractError("ADJ_FACTOR_API_PARTIAL", f"missing {len(missing)} target tickers")
        normalized = raw.loc[:, ["ts_code", "trade_date", "adj_factor"]].copy()
        normalized.columns = ["ticker", "trade_date", "adj_factor"]
        normalized["trade_date"] = pd.to_datetime(normalized["trade_date"], errors="raise").dt.date
        normalized["adj_factor"] = pd.to_numeric(normalized["adj_factor"], errors="raise")
        normalized = normalized.drop_duplicates(subset=["ticker", "trade_date"], keep="last")
        changes = normalized.loc[
            [previous.get(ticker) is None or abs(value - previous[ticker]) > 1e-9 for ticker, value in zip(normalized["ticker"], normalized["adj_factor"], strict=True)]
        ].copy()
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("ADJ_FACTOR_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()
    try:
        rows, database_seconds = _replace_target_date(context, ADJ_FACTOR_CHANGES, changes, target)
    except Exception as exc:
        raise ContractError("ADJ_FACTOR_WRITE_FAILED", type(exc).__name__) from exc
    return BusinessExecution.success(
        metrics=_metrics(
            rows_read=len(normalized),
            rows_written=rows,
            assets_processed=len(expected),
            database_write_seconds=database_seconds,
            stages={"fetch_and_compare": fetched_at - started, "database_write": database_seconds},
            api_requests=1,
        ),
        outputs=(
            OutputResult(
                output_id="adj_factor_changes",
                rows_written=rows,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={"target_date": target.isoformat(), "source_rows": len(normalized), "change_rows": rows},
            ),
        ),
    )


def execute_index_daily_update(context: PipelineRunContext) -> BusinessExecution:
    target = _target_date(context)
    started = time.monotonic()
    frames: list[pd.DataFrame] = []
    try:
        for index_code, index_name in INDEX_SERIES:
            raw = ak.stock_zh_index_daily(symbol=index_code)
            if raw is None or raw.empty:
                raise ContractError("INDEX_DAILY_API_EMPTY", index_code)
            _required_columns(raw, ("date", "open", "high", "low", "close", "volume"), "INDEX_DAILY_API_PARTIAL")
            frame = raw.rename(columns={"date": "trade_date"}).copy()
            frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
            frame = frame.loc[frame["trade_date"] == target].copy()
            if len(frame) != 1:
                raise ContractError("INDEX_DAILY_API_PARTIAL", f"{index_code} target rows={len(frame)}")
            frame["index_code"] = index_code
            frame["index_name"] = index_name
            frame["volume"] = pd.to_numeric(frame["volume"], errors="raise").astype("int64")
            frames.append(frame.loc[:, ["trade_date", "index_code", "index_name", "open", "high", "low", "close", "volume"]])
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("INDEX_DAILY_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()
    prepared = pd.concat(frames, ignore_index=True)
    try:
        rows, database_seconds = _upsert_frame(context, INDEX_DAILY, prepared)
    except Exception as exc:
        raise ContractError("INDEX_DAILY_WRITE_FAILED", type(exc).__name__) from exc
    return BusinessExecution.success(
        metrics=_metrics(
            rows_read=rows,
            rows_written=rows,
            assets_processed=len(INDEX_SERIES),
            database_write_seconds=database_seconds,
            stages={"fetch": fetched_at - started, "database_write": database_seconds},
            api_requests=len(INDEX_SERIES),
            batches=len(INDEX_SERIES),
        ),
        outputs=(
            OutputResult(
                output_id="index_daily",
                rows_written=rows,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={"target_date": target.isoformat(), "index_codes": [code for code, _ in INDEX_SERIES]},
            ),
        ),
    )


_EASTMONEY_UT = "7eea3edcaed734bea9cbfc24409ed989"
_EASTMONEY_DPT = "wz.ztzt"
_EASTMONEY_BASE = "http://push2ex.eastmoney.com"
_EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/ztb/detail",
}
_EASTMONEY_PAGE_SIZE = 200


def _eastmoney_date(value: Any) -> str:
    return str(value).replace("-", "")


def _fetch_eastmoney_pool_page(
    endpoint: str,
    target: date,
    *,
    sort: str,
    page_index: int,
) -> tuple[list[dict[str, Any]], int]:
    parameters = (
        f"ut={_EASTMONEY_UT}&dpt={_EASTMONEY_DPT}&Pageindex={page_index}&pagesize={_EASTMONEY_PAGE_SIZE}"
        f"&sort={sort}&date={target.strftime('%Y%m%d')}"
    )
    request = urllib.request.Request(f"{_EASTMONEY_BASE}/{endpoint}?{parameters}", headers=_EASTMONEY_HEADERS)
    with urllib.request.urlopen(request, timeout=15) as response:
        raw = response.read().decode("utf-8")
    if raw.startswith("jQuery"):
        raw = raw[raw.index("{") : raw.rindex("}") + 1]
    payload = json.loads(raw)
    if payload.get("rc") != 0:
        raise ContractError("ZT_DT_POOL_API_FAILED", f"eastmoney rc={payload.get('rc')}")
    data = payload.get("data")
    if not isinstance(data, dict) or "pool" not in data or not isinstance(data["pool"], list):
        raise ContractError("ZT_DT_POOL_API_PARTIAL", "response does not contain a pool list")
    if _eastmoney_date(data.get("date")) != target.strftime("%Y%m%d"):
        raise ContractError(
            "ZT_DT_POOL_API_PARTIAL",
            f"response date {data.get('date')!r} does not match {target.isoformat()}",
        )
    total = data.get("total")
    if isinstance(total, bool):
        raise ContractError("ZT_DT_POOL_API_PARTIAL", "response total must be a non-negative integer")
    try:
        total = int(total)
    except (TypeError, ValueError) as exc:
        raise ContractError("ZT_DT_POOL_API_PARTIAL", "response does not contain an integer total") from exc
    if total < 0:
        raise ContractError("ZT_DT_POOL_API_PARTIAL", "response total must be non-negative")
    return data["pool"], total


def _fetch_eastmoney_pool(endpoint: str, target: date, *, sort: str) -> tuple[list[dict[str, Any]], int]:
    """Fetch every reported page and prove the response matches its total."""

    records, total = _fetch_eastmoney_pool_page(endpoint, target, sort=sort, page_index=0)
    requests = 1
    if total == 0:
        if records:
            raise ContractError("ZT_DT_POOL_API_PARTIAL", "zero total response contains pool records")
        return [], requests
    if not records:
        raise ContractError("ZT_DT_POOL_API_PARTIAL", "non-zero total response has an empty first page")

    all_records = list(records)
    page_index = 1
    while len(all_records) < total:
        page, page_total = _fetch_eastmoney_pool_page(endpoint, target, sort=sort, page_index=page_index)
        requests += 1
        if page_total != total:
            raise ContractError(
                "ZT_DT_POOL_API_PARTIAL",
                f"page {page_index} total {page_total} does not match first-page total {total}",
            )
        if not page:
            raise ContractError(
                "ZT_DT_POOL_API_PARTIAL",
                f"page {page_index} is empty before reported total {total}",
            )
        all_records.extend(page)
        page_index += 1

    if len(all_records) != total:
        raise ContractError(
            "ZT_DT_POOL_API_PARTIAL",
            f"reported total {total} does not match {len(all_records)} returned records",
        )
    return all_records, requests


def _pool_price(value: Any) -> float | None:
    return None if value is None else float(value) / 1000


def _pool_time(value: Any) -> str | None:
    if value is None:
        return None
    text = str(int(value)).zfill(6)
    return f"{text[:2]}:{text[2:4]}:{text[4:]}"


def _zt_frame(records: list[dict[str, Any]], target: date) -> pd.DataFrame:
    now = datetime.now(UTC)
    rows = [
        {
            "trade_date": target,
            "ticker": item.get("c", ""),
            "name": item.get("n", ""),
            "close": _pool_price(item.get("p")),
            "pct_change": item.get("zdp"),
            "amount": item.get("amount"),
            "float_cap": item.get("ltsz"),
            "total_shares": item.get("tshare"),
            "turnover": item.get("hs"),
            "first_block_time": _pool_time(item.get("fbt")),
            "last_block_time": _pool_time(item.get("lbt")),
            "consecutive_boards": item.get("lbc", 0),
            "block_fund": item.get("fund"),
            "blast_count": item.get("zbc", 0),
            "block_stats": json.dumps(item.get("zttj"), ensure_ascii=False) if item.get("zttj") else None,
            "industry_name": item.get("hybk", ""),
            "created_at": now,
        }
        for item in records
    ]
    return pd.DataFrame(rows, columns=ZT_POOL.column_names())


def _dt_frame(records: list[dict[str, Any]], target: date) -> pd.DataFrame:
    now = datetime.now(UTC)
    rows = [
        {
            "trade_date": target,
            "ticker": item.get("c", ""),
            "name": item.get("n", ""),
            "close": _pool_price(item.get("p")),
            "pct_change": item.get("zdp"),
            "amount": item.get("amount"),
            "float_cap": item.get("ltsz"),
            "total_shares": item.get("tshare"),
            "turnover": item.get("hs"),
            "block_fund": item.get("fund"),
            "consecutive_days": item.get("days", 0),
            "open_count": item.get("oc", 0),
            "last_block_time": _pool_time(item.get("lbt")),
            "board_amount": item.get("fba"),
            "pe_ratio": item.get("pe"),
            "industry_name": item.get("hybk", ""),
            "created_at": now,
        }
        for item in records
    ]
    return pd.DataFrame(rows, columns=DT_POOL.column_names())


def _validate_pool_rows(frame: pd.DataFrame, output_id: str) -> None:
    if not frame.empty and (frame["ticker"].isna().any() or (frame["ticker"].astype(str).str.strip() == "").any()):
        raise ContractError("ZT_DT_POOL_API_PARTIAL", f"{output_id} contains a record without ticker")


def execute_zt_dt_pool_daily(context: PipelineRunContext) -> BusinessExecution:
    target = _target_date(context)
    started = time.monotonic()
    try:
        zt_records, zt_requests = _fetch_eastmoney_pool("getTopicZTPool", target, sort="fbt:asc")
        dt_records, dt_requests = _fetch_eastmoney_pool("getTopicDTPool", target, sort="fund:asc")
        zt = _zt_frame(zt_records, target)
        dt = _dt_frame(dt_records, target)
        _validate_pool_rows(zt, "zt_pool")
        _validate_pool_rows(dt, "dt_pool")
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("ZT_DT_POOL_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()
    try:
        zt_rows, dt_rows, database_seconds = _replace_zt_dt_target(context, target, zt, dt)
    except Exception as exc:
        raise ContractError("ZT_DT_POOL_WRITE_FAILED", type(exc).__name__) from exc
    assets = set(zt["ticker"]) if not zt.empty else set()
    assets.update(dt["ticker"] if not dt.empty else ())
    return BusinessExecution.success(
        metrics=_metrics(
            rows_read=len(zt_records) + len(dt_records),
            rows_written=zt_rows + dt_rows,
            assets_processed=len(assets),
            database_write_seconds=database_seconds,
            stages={"fetch": fetched_at - started, "database_write": database_seconds},
            api_requests=zt_requests + dt_requests,
            batches=zt_requests + dt_requests,
        ),
        outputs=(
            OutputResult(
                output_id="zt_pool",
                rows_written=zt_rows,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={"target_date": target.isoformat(), "empty_snapshot": zt_rows == 0},
            ),
            OutputResult(
                output_id="dt_pool",
                rows_written=dt_rows,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={"target_date": target.isoformat(), "empty_snapshot": dt_rows == 0},
            ),
        ),
    )


def execute_suspend_d_ingest(context: PipelineRunContext) -> BusinessExecution:
    target = _target_date(context)
    started = time.monotonic()
    try:
        raw = get_tushare_pro(settings=context.settings).suspend_d(
            start_date=target.strftime("%Y%m%d"),
            end_date=target.strftime("%Y%m%d"),
        )
        if raw is None:
            raise ContractError("SUSPEND_D_API_FAILED", "suspend_d returned None")
        if raw.empty:
            cleaned = pd.DataFrame(columns=SUSPEND_D.column_names())
        else:
            _required_columns(raw, ("ts_code", "trade_date", "suspend_type"), "SUSPEND_D_API_PARTIAL")
            from .suspend_d.clean import clean_suspend_d

            cleaned = clean_suspend_d(raw)
            _ensure_target_rows(cleaned, target, "SUSPEND_D_API_PARTIAL")
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("SUSPEND_D_API_FAILED", type(exc).__name__) from exc
    fetched_at = time.monotonic()
    try:
        rows, database_seconds = _replace_target_date(context, SUSPEND_D, cleaned, target)
    except Exception as exc:
        raise ContractError("SUSPEND_D_WRITE_FAILED", type(exc).__name__) from exc
    return BusinessExecution.success(
        metrics=_metrics(
            rows_read=len(raw),
            rows_written=rows,
            assets_processed=cleaned["ticker"].nunique() if not cleaned.empty else 0,
            database_write_seconds=database_seconds,
            stages={"fetch_and_clean": fetched_at - started, "database_write": database_seconds},
            api_requests=1,
        ),
        outputs=(
            OutputResult(
                output_id="suspend_d",
                rows_written=rows,
                location="settings.paths.duckdb_path",
                completed=True,
                detail={"target_date": target.isoformat(), "empty_snapshot": rows == 0},
            ),
        ),
    )


def _calendar_input() -> InputContract:
    return InputContract(
        input_id="trading_calendar",
        kind=InputKind.TABLE,
        source="quant_db.trading_calendar",
        required_fields=("trade_date", "is_open"),
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


def _external_input(input_id: str, source: str, fields: tuple[str, ...], *, tushare: bool = False, checker=None) -> InputContract:
    stale_error_code = f"{input_id.upper()}_STALE"
    return InputContract(
        input_id=input_id,
        kind=InputKind.EXTERNAL_API,
        source=source,
        required_fields=fields,
        target_date_semantics="provider response must be scoped to the resolved target trading date",
        missing_error_code="TUSHARE_CONFIGURATION_MISSING" if tushare else "EXTERNAL_API_CONFIGURATION_MISSING",
        structure_check=_tushare_configuration if tushare else (checker or _akshare_configuration),
        freshness=FreshnessContract(
            check_id=f"{input_id}_target_freshness",
            target_date_semantics="target-date response is validated by the executor before any transaction",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.REJECT,
            error_code=stale_error_code,
            checker=_external_target_freshness(stale_error_code, input_id),
        ),
    )


def _output(
    output_id: str,
    table,
    write_mode: WriteMode,
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
        write_mode=write_mode,
        target_date_semantics="resolved target trading date",
        completion=CompletionContract(
            marker=f"{table.name} committed target-date output is queryable",
            error_code=f"{output_id.upper()}_COMPLETION_MISSING",
            checker=completion,
        ),
        quality_checks=(_no_duplicate_quality(table, f"{output_id.upper()}_DUPLICATE_KEY"),),
        allow_empty=allow_empty,
    )


def _idempotency(table_name: str, *, mode: str, recovery: str) -> IdempotencyContract:
    return IdempotencyContract(
        idempotency_key=f"{table_name}.trade_date plus its schema primary key",
        repeat_run_semantics=mode,
        existing_target_handling=mode,
        failure_recovery=recovery,
        uses_staging=False,
        atomic_replace_boundary="one target trading date in the single quant.db transaction",
    )


def _execution() -> ExecutionPolicy:
    return ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1)


MARKET_DAILY_UPDATE = register_pipeline(
    PipelineContract(
        pipeline_id="market_daily_update",
        name="A-share daily market snapshot",
        description=(
            "Fetches, cleans, enriches, and atomically replaces one trading date of daily_market_snapshot. "
            "Tushare daily has no authoritative target-date total or per-security tradability field, so source "
            "validation is limited to a non-empty, required-schema, exact-target-date response."
        ),
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_market_daily_update,
        target_date_policy=MARKET_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            _calendar_input(),
            _external_input(
                "tushare_daily_market",
                "tushare.pro.daily(trade_date=YYYYMMDD)",
                ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"),
                tushare=True,
            ),
            InputContract(
                input_id="daily_market_history",
                kind=InputKind.TABLE,
                source="quant_db.daily_market_snapshot",
                required_fields=("trade_date", "ticker", "close", "pre_close"),
                target_date_semantics="prior rows are used only for existing enrichment rules; an empty first load is valid",
                missing_error_code="MARKET_HISTORY_STRUCTURE_MISSING",
                structure_check=_market_history_structure,
                freshness=FreshnessContract(
                    check_id="market_history_freshness",
                    target_date_semantics="only rows strictly before the target are eligible for enrichment; future rows are ignored",
                    maximum_lag_trading_days=1,
                    non_trading_day_policy=NonTradingDayPolicy.PREVIOUS_TRADING_DAY,
                    error_code="MARKET_HISTORY_STALE",
                    checker=_market_history_freshness,
                ),
            ),
        ),
        outputs=(
            _output(
                "daily_market_snapshot",
                DAILY_MARKET_SNAPSHOT,
                WriteMode.REPLACE_TARGET_DATE,
                _non_empty_completion(DAILY_MARKET_SNAPSHOT.name, "MARKET_DAILY_COMPLETION_MISSING"),
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=_idempotency(
            "daily_market_snapshot",
            mode="same target date deletes then replaces the target snapshot in one transaction",
            recovery="rerun the same target date; raw and canonical audit files are atomically replaced before the database commit",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="daily_market_snapshot rows for one target trading date",
            failure_visibility="uncommitted target replacement is rolled back; raw/canonical files are not downstream completion evidence",
        ),
        execution=_execution(),
        performance=PerformanceBudget(
            normal_budget_seconds=60.0,
            warning_threshold_seconds=30.0,
            hard_timeout_seconds=120,
            benchmark_scope="internal: 5,000-equity target-day processing; end-to-end: one configured Tushare request and one target-date replacement",
            baseline_source="docs/QRP产品蓝图v1.1/13_基础数据Pipeline性能基线.md#market_daily_update",
        ),
    )
)


DAILY_BASIC_UPDATE = register_pipeline(
    PipelineContract(
        pipeline_id="daily_basic_update",
        name="A-share daily basic indicators",
        description="Fetches, cleans, and atomically replaces one trading date of daily_basic.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_daily_basic_update,
        target_date_policy=MARKET_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            _calendar_input(),
            InputContract(
                input_id="market_daily_update_output",
                kind=InputKind.UPSTREAM_PIPELINE,
                source="market_daily_update / quant_db.daily_market_snapshot",
                required_fields=("trade_date", "ticker", "close"),
                target_date_semantics="daily_basic must cover every ticker in the same target-date formal market snapshot",
                missing_error_code="MARKET_DAILY_INPUT_STRUCTURE_MISSING",
                structure_check=_market_target_structure,
                freshness=FreshnessContract(
                    check_id="market_daily_target_freshness",
                    target_date_semantics="same target trading date is required",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.REJECT,
                    error_code="MARKET_DAILY_INPUT_STALE",
                    checker=_market_target_freshness,
                ),
                upstream_pipeline_id="market_daily_update",
            ),
            _external_input(
                "tushare_daily_basic",
                "tushare.pro.daily_basic(trade_date=YYYYMMDD)",
                ("ts_code", "trade_date", "close"),
                tushare=True,
            ),
        ),
        outputs=(
            _output(
                "daily_basic",
                DAILY_BASIC,
                WriteMode.REPLACE_TARGET_DATE,
                _non_empty_completion(DAILY_BASIC.name, "DAILY_BASIC_COMPLETION_MISSING"),
            ),
        ),
        dependencies=("market_daily_update",),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=_idempotency(
            "daily_basic",
            mode="same target date deletes then replaces all daily_basic rows in one transaction",
            recovery="rerun the target date after a failed transaction; historical dates are untouched",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="daily_basic rows for one target trading date",
            failure_visibility="the prior target snapshot remains visible until replacement commits",
        ),
        execution=_execution(),
        performance=PerformanceBudget(
            normal_budget_seconds=60.0,
            warning_threshold_seconds=30.0,
            hard_timeout_seconds=120,
            benchmark_scope="internal: 5,000-equity target-day processing; end-to-end: one configured Tushare request and one target-date replacement",
            baseline_source="docs/QRP产品蓝图v1.1/13_基础数据Pipeline性能基线.md#daily_basic_update",
        ),
    )
)


ADJ_FACTOR_DAILY = register_pipeline(
    PipelineContract(
        pipeline_id="adj_factor_daily",
        name="A-share daily adjustment-factor changes",
        description="Validates the market target universe and atomically replaces the complete calculated adjustment-factor change-point set for one date.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_adj_factor_daily,
        target_date_policy=MARKET_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            _calendar_input(),
            InputContract(
                input_id="market_daily_update_output",
                kind=InputKind.UPSTREAM_PIPELINE,
                source="market_daily_update / quant_db.daily_market_snapshot",
                required_fields=("trade_date", "ticker", "close"),
                target_date_semantics="market_daily_update must have completed the same target date",
                missing_error_code="MARKET_DAILY_INPUT_STRUCTURE_MISSING",
                structure_check=_market_target_structure,
                freshness=FreshnessContract(
                    check_id="market_daily_target_freshness",
                    target_date_semantics="same target trading date is required",
                    maximum_lag_trading_days=0,
                    non_trading_day_policy=NonTradingDayPolicy.REJECT,
                    error_code="MARKET_DAILY_INPUT_STALE",
                    checker=_market_target_freshness,
                ),
                upstream_pipeline_id="market_daily_update",
            ),
            _external_input(
                "tushare_adj_factor",
                "tushare.pro.adj_factor(trade_date=YYYYMMDD)",
                ("ts_code", "trade_date", "adj_factor"),
                tushare=True,
            ),
        ),
        outputs=(
            _output(
                "adj_factor_changes",
                ADJ_FACTOR_CHANGES,
                WriteMode.REPLACE_TARGET_DATE,
                _allowed_empty_completion(ADJ_FACTOR_CHANGES.name, "ADJ_FACTOR_COMPLETION_MISSING"),
                allow_empty=True,
            ),
        ),
        dependencies=("market_daily_update",),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=_idempotency(
            "adj_factor_changes",
            mode="same target date deletes then replaces the complete calculated change-point set, including an empty set",
            recovery="rerun after failure; the transaction removes obsolete target-date change points and never changes other dates",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="the complete calculated adj_factor_changes set for one target date",
            failure_visibility="target-date deletion and replacement commit together or the prior target set remains visible",
        ),
        execution=_execution(),
        performance=PerformanceBudget(
            normal_budget_seconds=60.0,
            warning_threshold_seconds=30.0,
            hard_timeout_seconds=120,
            benchmark_scope="internal: 5,000 target tickers and set-based change replacement; end-to-end: one configured Tushare request",
            baseline_source="docs/QRP产品蓝图v1.1/13_基础数据Pipeline性能基线.md#adj_factor_daily",
        ),
    )
)


INDEX_DAILY_UPDATE = register_pipeline(
    PipelineContract(
        pipeline_id="index_daily_update",
        name="Core index daily bars",
        description="Upserts the four existing core index series for one trading date only after all provider responses are complete.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_index_daily_update,
        target_date_policy=MARKET_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            _calendar_input(),
            _external_input(
                "akshare_index_daily",
                "akshare.stock_zh_index_daily(symbol) for sh000001, sz399001, sz399006, sh000688",
                ("date", "open", "high", "low", "close", "volume"),
            ),
        ),
        outputs=(
            _output("index_daily", INDEX_DAILY, WriteMode.UPSERT, _index_completion),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=_idempotency(
            "index_daily",
            mode="same target date upserts the four stable (trade_date, index_code) keys",
            recovery="rerun only after all four index responses can be validated; failed fetches never open a write transaction",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="all four core index rows for one target trading date",
            failure_visibility="no index row is committed when any required index response is missing",
        ),
        execution=_execution(),
        performance=PerformanceBudget(
            normal_budget_seconds=120.0,
            warning_threshold_seconds=60.0,
            hard_timeout_seconds=240,
            benchmark_scope="internal: four provider histories filtered to one date; end-to-end: four external index requests and one four-row upsert",
            baseline_source="docs/QRP产品蓝图v1.1/13_基础数据Pipeline性能基线.md#index_daily_update",
        ),
    )
)


ZT_DT_POOL_DAILY = register_pipeline(
    PipelineContract(
        pipeline_id="zt_dt_pool_daily",
        name="Eastmoney limit-up and limit-down pools",
        description="Fetches both Eastmoney pools and atomically replaces their target-date snapshots together.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_zt_dt_pool_daily,
        target_date_policy=MARKET_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            _calendar_input(),
            _external_input(
                "eastmoney_zt_pool",
                "push2ex.eastmoney.com/getTopicZTPool?date=YYYYMMDD",
                ("c", "n", "p", "zdp"),
                checker=_eastmoney_configuration,
            ),
            _external_input(
                "eastmoney_dt_pool",
                "push2ex.eastmoney.com/getTopicDTPool?date=YYYYMMDD",
                ("c", "n", "p", "zdp"),
                checker=_eastmoney_configuration,
            ),
        ),
        outputs=(
            _output(
                "zt_pool",
                ZT_POOL,
                WriteMode.REPLACE_TARGET_DATE,
                _allowed_empty_completion(ZT_POOL.name, "ZT_POOL_COMPLETION_MISSING"),
                allow_empty=True,
            ),
            _output(
                "dt_pool",
                DT_POOL,
                WriteMode.REPLACE_TARGET_DATE,
                _allowed_empty_completion(DT_POOL.name, "DT_POOL_COMPLETION_MISSING"),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=_idempotency(
            "zt_pool and dt_pool",
            mode="same target date replaces both pool snapshots in the same transaction, including valid empty pools",
            recovery="rerun the target date; the previous two-table snapshot remains visible until both replacements commit",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="zt_pool and dt_pool target-date snapshots together",
            failure_visibility="a failed second pool fetch or write leaves both previous snapshots intact",
        ),
        execution=_execution(),
        performance=PerformanceBudget(
            normal_budget_seconds=120.0,
            warning_threshold_seconds=60.0,
            hard_timeout_seconds=300,
            benchmark_scope="internal: 400 pool rows and dual-table replacement; end-to-end: all pages for two Eastmoney pools with a 15-second request timeout",
            baseline_source="docs/QRP产品蓝图v1.1/13_基础数据Pipeline性能基线.md#zt_dt_pool_daily",
        ),
    )
)


SUSPEND_D_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="suspend_d_ingest",
        name="A-share daily suspension events",
        description="Fetches and atomically replaces one target-date suspend_d snapshot; an empty provider snapshot is valid.",
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=execute_suspend_d_ingest,
        target_date_policy=MARKET_TARGET_DATE_POLICY,
        parameters=(),
        inputs=(
            _calendar_input(),
            _external_input(
                "tushare_suspend_d",
                "tushare.pro.suspend_d(start_date=YYYYMMDD, end_date=YYYYMMDD)",
                ("ts_code", "trade_date", "suspend_type"),
                tushare=True,
            ),
        ),
        outputs=(
            _output(
                "suspend_d",
                SUSPEND_D,
                WriteMode.REPLACE_TARGET_DATE,
                _allowed_empty_completion(SUSPEND_D.name, "SUSPEND_D_COMPLETION_MISSING"),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        idempotency=_idempotency(
            "suspend_d",
            mode="same target date replaces all suspension rows, including an explicit empty target snapshot",
            recovery="rerun the target date; a failed transaction leaves historical and prior target rows intact",
        ),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="suspend_d rows for one target trading date",
            failure_visibility="uncommitted target replacement is rolled back and cannot be consumed downstream",
        ),
        execution=_execution(),
        performance=PerformanceBudget(
            normal_budget_seconds=60.0,
            warning_threshold_seconds=30.0,
            hard_timeout_seconds=120,
            benchmark_scope="internal: 5,000 suspension rows and replacement; end-to-end: one configured Tushare request",
            baseline_source="docs/QRP产品蓝图v1.1/13_基础数据Pipeline性能基线.md#suspend_d_ingest",
        ),
    )
)


MARKET_DATA_CONTRACTS = (
    MARKET_DAILY_UPDATE,
    ADJ_FACTOR_DAILY,
    DAILY_BASIC_UPDATE,
    INDEX_DAILY_UPDATE,
    ZT_DT_POOL_DAILY,
    SUSPEND_D_INGEST,
)
