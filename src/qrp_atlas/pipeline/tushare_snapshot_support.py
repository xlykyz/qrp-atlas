"""Shared helpers for date-bounded Tushare snapshot Contracts.

The helpers in this module only own mechanics shared by the three small
Tushare datasets introduced together: scope parsing, provider response
validation, canonical frame preparation, and target-range replacement.  The
business identity and Contract registration remain in each endpoint module.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    CREATED_AT,
    align_to_schema,
    apply_mapping,
    get_table,
    quick_validate,
)
from qrp_atlas.orchestration.execution_control import ExecutionControlError

from .contracts import (
    CheckResult,
    ContractError,
    PipelineInvocation,
    PipelineRunContext,
    TargetWindow,
)

CHINA_TZ = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
LOGGER = logging.getLogger(__name__)


def parse_scope_date(value: object, field_name: str) -> date | None:
    """Parse the ISO and compact date forms accepted by the provider docs."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=CHINA_TZ).date()
        except ValueError:
            continue
    raise ContractError(
        "INVALID_DATE_RANGE",
        f"{field_name} must use YYYY-MM-DD or YYYYMMDD",
    )


def resolve_date_or_range_target(invocation: PipelineInvocation) -> TargetWindow:
    """Resolve explicit range parameters or the scheduled Shanghai date."""

    invocation.execution_control.check()
    values = invocation.parameter_overrides
    start_date = parse_scope_date(values.get("start_date"), "start_date")
    end_date = parse_scope_date(values.get("end_date"), "end_date")
    if (start_date is None) != (end_date is None):
        raise ContractError("INVALID_DATE_RANGE", "start_date and end_date must be supplied together")
    if start_date is not None and end_date is not None:
        if start_date > end_date:
            raise ContractError("INVALID_DATE_RANGE", "start_date must not be after end_date")
        return TargetWindow(start_date=start_date, end_date=end_date)
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("SCHEDULE_TIMEZONE_MISSING", "scheduled_for must be timezone-aware")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(CHINA_TZ).date())


def validate_single_date_override(target_date: date, invocation: PipelineInvocation) -> bool:
    """Allow an explicit trade date only when no provider range is present."""

    invocation.execution_control.check()
    values = invocation.parameter_overrides
    try:
        start_date = parse_scope_date(values.get("start_date"), "start_date")
        end_date = parse_scope_date(values.get("end_date"), "end_date")
    except ContractError:
        return False
    return isinstance(target_date, date) and start_date is None and end_date is None


def target_dates(window: TargetWindow) -> tuple[date, ...]:
    """Expand one target or an inclusive range into provider request dates."""

    if window.target_date is not None:
        return (window.target_date,)
    if window.start_date is None or window.end_date is None:
        raise ContractError("INVALID_TARGET_WINDOW")
    count = (window.end_date - window.start_date).days + 1
    return tuple(window.start_date + timedelta(days=offset) for offset in range(count))


def provider_configuration(context: PipelineRunContext) -> CheckResult:
    context.execution_control.check()
    if not context.settings.external_services.tushare_token:
        return CheckResult.failure(
            "tushare_configuration",
            "TUSHARE_CONFIGURATION_MISSING",
            "TUSHARE_TOKEN must be configured by the approved QRP environment",
        )
    return CheckResult.success("tushare_configuration", configured=True, provider="tushare")


def provider_freshness(context: PipelineRunContext, input_id: str) -> CheckResult:
    context.execution_control.check()
    return CheckResult.success(
        f"{input_id}_freshness",
        target_window=context.target_window.as_dict(),
        semantics=(
            "the provider request is date-bounded and each returned row is checked "
            "against the resolved inclusive target window"
        ),
        completeness_boundary=(
            "the endpoint exposes no authoritative total/page proof; the Contract "
            "uses one request per calendar date to stay within the documented per-request limit"
        ),
    )


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _provider_date(value: object) -> date:
    parsed = parse_scope_date(value, "trade_date")
    if parsed is None:
        raise ContractError("PROVIDER_DATE_INVALID", "trade_date is empty")
    return parsed


def validate_provider_frame(
    frame: object,
    *,
    required_fields: tuple[str, ...],
    target_window: TargetWindow,
    requested_date: date,
    endpoint: str,
) -> pd.DataFrame:
    """Validate one complete provider response for one requested date."""

    if frame is None:
        raise ContractError(
            "PROVIDER_RESPONSE_INVALID",
            f"{endpoint} returned None instead of a DataFrame",
        )
    if not isinstance(frame, pd.DataFrame):
        raise ContractError(
            "PROVIDER_RESPONSE_INVALID",
            f"{endpoint} returned {type(frame).__name__}, expected DataFrame",
        )
    if frame.empty:
        return frame.copy()
    missing = sorted(set(required_fields) - set(frame.columns))
    if missing:
        raise ContractError("PROVIDER_SCHEMA_MISSING", ",".join(missing))

    out = frame.copy()
    parsed_dates: list[date] = []
    for value in out["trade_date"].tolist():
        try:
            parsed_dates.append(_provider_date(value))
        except ContractError as exc:
            raise ContractError("PROVIDER_DATE_INVALID", endpoint) from exc
    out["trade_date"] = parsed_dates

    for row_number, row_date in enumerate(parsed_dates):
        if row_date != requested_date:
            raise ContractError(
                "PROVIDER_SCOPE_MISMATCH",
                f"row {row_number} trade_date is not {requested_date.isoformat()}",
            )
    if target_window.target_date is not None and requested_date != target_window.target_date:
        raise ContractError("INVALID_TARGET_WINDOW", "requested date is outside target date")
    if target_window.start_date is not None and not (
        target_window.start_date <= requested_date <= target_window.end_date
    ):
        raise ContractError("INVALID_TARGET_WINDOW", "requested date is outside target range")

    out["ts_code"] = out["ts_code"].astype("string").str.strip().str.upper()
    return out.reset_index(drop=True)


def fetch_by_calendar_date(
    context: PipelineRunContext,
    *,
    client: object,
    endpoint: str,
    required_fields: tuple[str, ...],
 ) -> tuple[pd.DataFrame, int, int, tuple[date, ...]]:
    """Fetch a date-bounded endpoint once per calendar date."""

    method = getattr(client, endpoint, None)
    if not callable(method):
        raise ContractError("PROVIDER_API_UNAVAILABLE", f"client missing {endpoint}")

    frames: list[pd.DataFrame] = []
    empty_dates: list[date] = []
    rows_read = 0
    requests = 0
    for requested_date in target_dates(context.target_window):
        context.execution_control.check()
        kwargs: dict[str, str] = {
            "trade_date": requested_date.strftime("%Y%m%d"),
        }
        try:
            raw = method(**kwargs)
        except ExecutionControlError:
            raise
        except Exception as exc:
            raise ContractError("PROVIDER_REQUEST_FAILED", type(exc).__name__) from exc
        context.execution_control.check()
        checked = validate_provider_frame(
            raw,
            required_fields=required_fields,
            target_window=context.target_window,
            requested_date=requested_date,
            endpoint=endpoint,
        )
        rows_read += len(checked)
        requests += 1
        if checked.empty:
            empty_dates.append(requested_date)
        else:
            frames.append(checked)
    if not frames:
        return pd.DataFrame(columns=list(required_fields)), rows_read, requests, tuple(empty_dates)
    return pd.concat(frames, ignore_index=True, sort=False), rows_read, requests, tuple(empty_dates)


def _normalise_text_column(
    frame: pd.DataFrame,
    column: str,
    *,
    required: bool,
    error_code: str,
) -> None:
    if column not in frame.columns:
        if required:
            raise ContractError(error_code, column)
        return
    values = frame[column].map(lambda value: None if _is_missing(value) else str(value).strip())
    if required and values.isna().any():
        raise ContractError(error_code, column)
    frame[column] = values


def _normalise_numeric_column(
    frame: pd.DataFrame,
    column: str,
    *,
    integer: bool = False,
    error_code: str,
) -> None:
    if column not in frame.columns:
        return
    values: list[float | int | None] = []
    for value in frame[column].tolist():
        if _is_missing(value):
            values.append(None)
            continue
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ContractError(error_code, column) from exc
        if not math.isfinite(number):
            raise ContractError(error_code, column)
        if integer:
            if not number.is_integer():
                raise ContractError(error_code, column)
            values.append(int(number))
        else:
            values.append(number)
    frame[column] = values


def prepare_canonical_frame(
    frame: pd.DataFrame,
    *,
    table_name: str,
    mapping_source: str,
    required_text_columns: Iterable[str] = (),
    numeric_columns: Iterable[str] = (),
    integer_columns: Iterable[str] = (),
    error_code: str,
) -> pd.DataFrame:
    """Map, normalize, validate, and conflict-check one provider batch."""

    table = get_table(table_name)
    if frame.empty:
        return pd.DataFrame(columns=list(table.column_names()))

    mapped = apply_mapping(frame.copy(), mapping_source)
    for column in required_text_columns:
        _normalise_text_column(mapped, column, required=True, error_code=error_code)
    for column in numeric_columns:
        _normalise_numeric_column(mapped, column, error_code=error_code)
    for column in integer_columns:
        _normalise_numeric_column(mapped, column, integer=True, error_code=error_code)

    try:
        prepared = align_to_schema(mapped, table_name, fill_missing_optional=True, drop_extra=True)
        prepared = quick_validate(prepared, table_name, allow_extra=False)
    except Exception as exc:
        raise ContractError(error_code, type(exc).__name__) from exc

    keys = list(table.primary_key)
    duplicates = prepared[prepared.duplicated(keys, keep=False)]
    if not duplicates.empty:
        business_columns = [column for column in table.column_names() if column != CREATED_AT]
        for _, group in duplicates.groupby(keys, sort=False, dropna=False):
            if len(group[business_columns].drop_duplicates()) > 1:
                raise ContractError(error_code, "conflicting duplicate primary key")
    return prepared.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)


def replace_target_window(
    context: PipelineRunContext,
    *,
    table_name: str,
    prepared: pd.DataFrame,
    empty_dates: Iterable[date],
    empty_response_error_code: str,
) -> tuple[int, float]:
    """Replace complete non-empty dates after protecting empty-date snapshots."""

    table = get_table(table_name)
    requested_dates = target_dates(context.target_window)
    requested_date_set = set(requested_dates)
    empty_date_set = set(empty_dates)
    if not empty_date_set <= requested_date_set:
        raise ContractError("INVALID_EMPTY_DATE_SCOPE", table_name)

    started = time.monotonic()
    connection: duckdb.DuckDBPyConnection | None = None
    transaction_open = False
    registered = False
    try:
        context.execution_control.check()
        connection = duckdb.connect(str(context.settings.paths.duckdb_path))
        connection.execute(table.duckdb_create_sql())
        for empty_date in requested_dates:
            if empty_date not in empty_date_set:
                continue
            context.execution_control.check()
            existing = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table.name} WHERE trade_date = ?",
                    [empty_date],
                ).fetchone()[0]
            )
            if existing:
                raise ContractError(
                    empty_response_error_code,
                    f"{table.name} returned an empty response for {empty_date.isoformat()} "
                    f"but {existing} existing rows would be removed",
                )
        context.execution_control.check()
        connection.execute("BEGIN TRANSACTION")
        transaction_open = True
        for requested_date in requested_dates:
            if requested_date in empty_date_set:
                continue
            connection.execute(
                f"DELETE FROM {table.name} WHERE trade_date = ?",
                [requested_date],
            )
            context.execution_control.check()
        if not prepared.empty:
            columns = [column for column in table.column_names() if column != CREATED_AT]
            connection.register("_tushare_snapshot_rows", prepared)
            registered = True
            connection.execute(
                f"INSERT INTO {table.name} ({', '.join(columns)}) "
                f"SELECT {', '.join(columns)} FROM _tushare_snapshot_rows"
            )
            connection.unregister("_tushare_snapshot_rows")
            registered = False
        context.execution_control.check()
        connection.execute("COMMIT")
        transaction_open = False
        return len(prepared), time.monotonic() - started
    except BaseException:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except duckdb.Error as rollback_error:
                LOGGER.debug("failed to roll back snapshot replacement", exc_info=rollback_error)
        raise
    finally:
        if connection is not None:
            if registered:
                try:
                    connection.unregister("_tushare_snapshot_rows")
                except duckdb.Error as unregister_error:
                    LOGGER.debug("failed to unregister snapshot rows", exc_info=unregister_error)
            connection.close()


def _target_bounds(context: PipelineRunContext) -> tuple[date, date]:
    window = context.target_window
    if window.target_date is not None:
        return window.target_date, window.target_date
    if window.start_date is None or window.end_date is None:
        raise ContractError("INVALID_TARGET_WINDOW")
    return window.start_date, window.end_date


def range_completion(table_name: str, error_code: str):
    """Return a completion check where an empty target range is valid."""

    def check(context: PipelineRunContext) -> CheckResult:
        try:
            start_date, end_date = _target_bounds(context)
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                rows = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM {table_name} "
                        "WHERE trade_date BETWEEN ? AND ?",
                        [start_date, end_date],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:  # noqa: BLE001 - convert any read failure to a CheckResult
            return CheckResult.failure(
                f"{table_name}_completion",
                error_code,
                "output table could not be read after committed replacement",
                exception=type(exc).__name__,
            )
        return CheckResult.success(
            f"{table_name}_completion",
            rows=rows,
            empty_snapshot=rows == 0,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

    check.__name__ = f"{table_name}_range_completion"
    return check


def range_unique_key_quality(table_name: str, primary_key: tuple[str, ...], error_code: str):
    """Return a target-range uniqueness quality check."""

    keys = ", ".join(primary_key)

    def check(context: PipelineRunContext) -> CheckResult:
        try:
            start_date, end_date = _target_bounds(context)
            connection = duckdb.connect(str(context.settings.paths.duckdb_path), read_only=True)
            try:
                duplicates = int(
                    connection.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM (
                            SELECT {keys}, COUNT(*) AS row_count
                            FROM {table_name}
                            WHERE trade_date BETWEEN ? AND ?
                            GROUP BY {keys}
                            HAVING COUNT(*) > 1
                        )
                        """,
                        [start_date, end_date],
                    ).fetchone()[0]
                )
            finally:
                connection.close()
        except Exception as exc:  # noqa: BLE001 - convert any read failure to a CheckResult
            return CheckResult.failure(
                f"{table_name}_unique_key_quality",
                error_code,
                "output key quality could not be checked",
                exception=type(exc).__name__,
            )
        if duplicates:
            return CheckResult.failure(
                f"{table_name}_unique_key_quality",
                error_code,
                "output contains duplicate primary keys",
                duplicate_keys=duplicates,
            )
        return CheckResult.success(
            f"{table_name}_unique_key_quality",
            duplicate_keys=0,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

    check.__name__ = f"{table_name}_unique_key_quality"
    return check
