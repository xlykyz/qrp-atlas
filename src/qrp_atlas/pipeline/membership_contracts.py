"""Formal contracts for industry membership and index component history.

These are two manual, explicit-scope historical data capabilities.  The
provider endpoints do not expose reliable pagination or total-count evidence,
so the contracts validate response scope and fail closed on malformed or
out-of-scope rows while documenting the remaining coverage boundary.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from datetime import date, datetime
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    EFFECTIVE_FROM,
    INDEX_COMPONENT_HISTORY,
    INDUSTRY_CODE,
    INDUSTRY_MEMBERSHIP_HISTORY,
    REVISION_ID,
    SNAPSHOT_DATE,
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
    PipelineKind,
    PipelineMetrics,
    PipelineInvocation,
    PipelineRunContext,
    TargetDatePolicy,
    TargetWindow,
    TransactionContract,
    TransactionMode,
    WriteMode,
)
from .index_component.clean import clean_index_component
from .index_component.fetch import (
    IndexComponentFetchReport,
    fetch_index_weights_with_report,
)
from .industry_membership.clean import clean_industry_membership
from .industry_membership.fetch import (
    IndustryMembershipFetchReport,
    fetch_industry_membership_with_report,
)
from .pit_utils import NextTradeDateResolver
from .registry import register_pipeline


MEMBERSHIP_TIMEZONE = ZoneInfo("Asia/Shanghai")
QUANT_DB_RESOURCE = "quant_db"
QUANT_DB_WRITER = "quant_db_writer"
TRADING_CALENDAR_READ = "duckdb://quant_db#trading_calendar"

INDUSTRY_TABLE = INDUSTRY_MEMBERSHIP_HISTORY.name
INDEX_TABLE = INDEX_COMPONENT_HISTORY.name
CODE_PATTERN = re.compile(r"^[0-9]{6}\.[A-Z]{2}$")

INDUSTRY_PROVIDER_FIELDS: tuple[str, ...] = (
    "ts_code",
    "l1_code",
    "l1_name",
    "l2_code",
    "l2_name",
    "l3_code",
    "l3_name",
    "in_date",
    "out_date",
)
INDEX_PROVIDER_FIELDS: tuple[str, ...] = (
    "index_code",
    "con_code",
    "trade_date",
    "weight",
)


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


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _provider_date(
    value: object,
    field_name: str,
    *,
    allow_none: bool = False,
    error_code: str = "MEMBERSHIP_PROVIDER_DATE_INVALID",
) -> date | None:
    if _is_missing(value) or _text(value) == "":
        if allow_none:
            return None
        raise ContractError(error_code, field_name)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value)
    if text.lower() in {"none", "nan", "nat", "null"}:
        if allow_none:
            return None
        raise ContractError(error_code, field_name)
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ContractError(error_code, field_name) from exc


def _parameter_date(value: object, name: str) -> date:
    if value is None:
        raise ContractError("REQUIRED_PARAMETER_MISSING", name)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ContractError("INVALID_PARAMETER", name) from exc
    raise ContractError("INVALID_PARAMETER", name)


def _canonical_code(value: object, field_name: str) -> str:
    value_text = _text(value).upper()
    if not CODE_PATTERN.fullmatch(value_text):
        raise ContractError("INVALID_SCOPE", field_name)
    return value_text


def _code_list(value: object, field_name: str, *, required: bool) -> tuple[str, ...]:
    raw = _text(value)
    if not raw:
        if required:
            raise ContractError("REQUIRED_PARAMETER_MISSING", field_name)
        return ()
    result: list[str] = []
    for item in raw.split(","):
        if not item.strip():
            raise ContractError("INVALID_SCOPE", field_name)
        code = _canonical_code(item, field_name)
        if code not in result:
            result.append(code)
    if required and not result:
        raise ContractError("INVALID_SCOPE", field_name)
    return tuple(result)


def _optional_code(value: object, field_name: str) -> str | None:
    text = _text(value)
    if not text:
        return None
    return _canonical_code(text, field_name)


def _industry_scope(context: PipelineRunContext) -> dict[str, Any]:
    _check_context(context)
    values = context.parameter_overrides
    tickers = _code_list(values.get("tickers", ""), "tickers", required=False)
    code_filters = {
        name: _optional_code(values.get(name, ""), name)
        for name in ("l1_code", "l2_code", "l3_code")
    }
    is_new = _text(values.get("is_new", "")).upper()
    if is_new not in {"", "Y", "N"}:
        raise ContractError("INVALID_PARAMETER", "is_new")
    if tickers and any(code_filters.values()):
        raise ContractError("INDUSTRY_SCOPE_AMBIGUOUS")
    if not tickers and not any(code_filters.values()):
        raise ContractError("INDUSTRY_SCOPE_REQUIRED")
    return {
        "tickers": tickers,
        "l1_code": code_filters["l1_code"],
        "l2_code": code_filters["l2_code"],
        "l3_code": code_filters["l3_code"],
        "is_new": is_new or None,
        "scope_units": len(tickers) if tickers else 1,
        "scope_mode": "tickers" if tickers else "industry_codes",
    }


def _index_scope(context: PipelineRunContext) -> dict[str, Any]:
    _check_context(context)
    values = context.parameter_overrides
    codes = _code_list(values.get("index_codes"), "index_codes", required=True)
    start_date = _parameter_date(values.get("start_date"), "start_date")
    end_date = _parameter_date(values.get("end_date"), "end_date")
    if start_date > end_date:
        raise ContractError("INVALID_DATE_RANGE", "start_date must not be after end_date")
    return {
        "index_codes": codes,
        "start_date": start_date,
        "end_date": end_date,
        "scope_units": len(codes),
    }


def _scheduled_observation_window(invocation: PipelineInvocation) -> TargetWindow:
    _check_invocation(invocation)
    if invocation.scheduled_for.tzinfo is None:
        raise ContractError("MEMBERSHIP_SCHEDULE_TIMEZONE_MISSING")
    return TargetWindow.for_date(invocation.scheduled_for.astimezone(MEMBERSHIP_TIMEZONE).date())


def _index_source_window(invocation: PipelineInvocation) -> TargetWindow:
    _check_invocation(invocation)
    start_date = _parameter_date(invocation.parameter_overrides.get("start_date"), "start_date")
    end_date = _parameter_date(invocation.parameter_overrides.get("end_date"), "end_date")
    if start_date > end_date:
        raise ContractError("INVALID_DATE_RANGE", "start_date must not be after end_date")
    return TargetWindow(start_date=start_date, end_date=end_date)


def _reject_explicit_target_date(target_date: date, invocation: PipelineInvocation) -> bool:
    _check_invocation(invocation)
    return False


def _connect(context: PipelineRunContext, *, read_only: bool) -> duckdb.DuckDBPyConnection:
    _check_context(context)
    return duckdb.connect(str(context.settings.paths.duckdb_path), read_only=read_only)


def _table_structure(
    context: PipelineRunContext,
    table_name: str,
    required_fields: tuple[str, ...],
    error_code: str,
    check_id: str,
) -> CheckResult:
    try:
        connection = _connect(context, read_only=True)
        try:
            columns = set(connection.execute(f"DESCRIBE {table_name}").fetchdf()["column_name"])
        finally:
            connection.close()
    except ContractError:
        raise
    except Exception as exc:
        return CheckResult.failure(check_id, error_code, f"required table {table_name} is unavailable", exception=type(exc).__name__)
    missing = sorted(set(required_fields) - columns)
    if missing:
        return CheckResult.failure(check_id, error_code, f"required table {table_name} is missing fields", missing_fields=missing)
    return CheckResult.success(check_id, table=table_name, fields=sorted(columns))


def _calendar_structure(context: PipelineRunContext) -> CheckResult:
    return _table_structure(
        context,
        TRADING_CALENDAR.name,
        ("trade_date", "is_open"),
        "TRADING_CALENDAR_STRUCTURE_MISSING",
        "membership_trading_calendar_structure",
    )


def _calendar_freshness(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    try:
        connection = _connect(context, read_only=True)
        try:
            open_dates = int(
                connection.execute(
                    "SELECT COUNT(*) FROM trading_calendar WHERE COALESCE(is_open, TRUE)"
                ).fetchone()[0]
            )
        finally:
            connection.close()
    except ContractError:
        raise
    except Exception as exc:
        return CheckResult.failure(
            "membership_trading_calendar_freshness",
            "TRADING_CALENDAR_UNAVAILABLE",
            "trading calendar could not be read",
            exception=type(exc).__name__,
        )
    if open_dates <= 0:
        return CheckResult.failure(
            "membership_trading_calendar_freshness",
            "TRADING_CALENDAR_UNAVAILABLE",
            "trading calendar has no open dates",
        )
    return CheckResult.success(
        "membership_trading_calendar_freshness",
        open_dates=open_dates,
        purpose="map source effective dates to the first strictly later open trade date",
    )


def _tushare_configuration(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    if not context.settings.external_services.tushare_token:
        return CheckResult.failure(
            "tushare_configuration",
            "TUSHARE_CONFIGURATION_MISSING",
            "TUSHARE_TOKEN must be configured by the approved QRP environment",
        )
    return CheckResult.success("tushare_configuration", configured=True, provider="tushare")


def _industry_provider_freshness(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    scope = _industry_scope(context)
    return CheckResult.success(
        "industry_provider_freshness",
        scope_mode=scope["scope_mode"],
        scope_units=scope["scope_units"],
        date_filter_applied=False,
        completeness_boundary=(
            "Tushare index_member_all provides no total/page count; the Contract proves only "
            "that every returned row is within the requested explicit scope"
        ),
    )


def _index_provider_freshness(context: PipelineRunContext) -> CheckResult:
    _check_context(context)
    scope = _index_scope(context)
    return CheckResult.success(
        "index_provider_freshness",
        index_codes=list(scope["index_codes"]),
        start_date=scope["start_date"].isoformat(),
        end_date=scope["end_date"].isoformat(),
        completeness_boundary=(
            "Tushare index_weight provides no total/page count; the Contract proves only "
            "that every returned row is within the requested index/date range"
        ),
    )


def _calendar_input() -> InputContract:
    return InputContract(
        input_id="trading_calendar",
        kind=InputKind.TABLE,
        source="quant_db.trading_calendar",
        required_fields=("trade_date", "is_open"),
        target_date_semantics="read-only calendar used only for PIT available_trade_date mapping",
        missing_error_code="TRADING_CALENDAR_STRUCTURE_MISSING",
        structure_check=_calendar_structure,
        freshness=FreshnessContract(
            check_id="membership_trading_calendar_freshness",
            target_date_semantics="at least one open calendar date must be available for strict next-open mapping",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code="TRADING_CALENDAR_UNAVAILABLE",
            checker=_calendar_freshness,
        ),
    )


def _tushare_input(
    *,
    input_id: str,
    source: str,
    fields: tuple[str, ...],
    freshness_checker,
    freshness_id: str,
    freshness_error_code: str,
) -> InputContract:
    return InputContract(
        input_id=input_id,
        kind=InputKind.EXTERNAL_API,
        source=source,
        required_fields=fields,
        target_date_semantics="explicit scope and source date/range are validated before any database transaction",
        missing_error_code="TUSHARE_CONFIGURATION_MISSING",
        structure_check=_tushare_configuration,
        freshness=FreshnessContract(
            check_id=freshness_id,
            target_date_semantics="provider has no reliable total/page evidence; scope boundary is retained in the result",
            maximum_lag_trading_days=0,
            non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
            error_code=freshness_error_code,
            checker=freshness_checker,
        ),
    )


def _provider_code(value: object, field_name: str, error_code: str) -> str:
    try:
        return _canonical_code(value, field_name)
    except ContractError as exc:
        raise ContractError(error_code, field_name) from exc


def _validate_industry_provider_frame(frame: object, scope: Mapping[str, Any]) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise ContractError("INDUSTRY_PROVIDER_RESPONSE_INVALID", "response must be a DataFrame")
    if frame.empty:
        return frame
    missing = sorted(set(INDUSTRY_PROVIDER_FIELDS) - set(frame.columns))
    if missing:
        raise ContractError("INDUSTRY_PROVIDER_SCHEMA_MISSING", ",".join(missing))

    expected_tickers = {item.upper() for item in scope["tickers"]}
    seen_identities: dict[tuple[Any, ...], str] = {}
    for row_number, (_, row) in enumerate(frame.iterrows()):
        asset = _provider_code(row.get("ts_code"), "ts_code", "INDUSTRY_PROVIDER_SCHEMA_INVALID")
        if expected_tickers and asset not in expected_tickers:
            raise ContractError("INDUSTRY_PROVIDER_SCOPE_MISMATCH", f"row {row_number}")
        effective_from = _provider_date(
            row.get("in_date"),
            "in_date",
            error_code="INDUSTRY_PROVIDER_DATE_INVALID",
        )
        effective_to = _provider_date(
            row.get("out_date"),
            "out_date",
            allow_none=True,
            error_code="INDUSTRY_PROVIDER_DATE_INVALID",
        )
        if effective_to is not None and effective_to <= effective_from:
            raise ContractError("INDUSTRY_PROVIDER_DATE_INVALID", f"row {row_number}")

        level_count = 0
        for level, (code_field, name_field) in enumerate(
            (("l1_code", "l1_name"), ("l2_code", "l2_name"), ("l3_code", "l3_name")),
            start=1,
        ):
            code = _text(row.get(code_field))
            name = _text(row.get(name_field))
            if code:
                level_count += 1
                _provider_code(code, code_field, "INDUSTRY_PROVIDER_SCHEMA_INVALID")
                if not name:
                    raise ContractError("INDUSTRY_PROVIDER_SCHEMA_INVALID", f"row {row_number} {name_field}")
                identity = (asset, level, code.upper(), effective_from.isoformat(), effective_to.isoformat() if effective_to else "")
                previous_name = seen_identities.get(identity)
                if previous_name is not None and previous_name != name:
                    raise ContractError("INDUSTRY_PROVIDER_DUPLICATE_CONFLICT", f"row {row_number}")
                seen_identities[identity] = name
            elif name:
                raise ContractError("INDUSTRY_PROVIDER_SCHEMA_INVALID", f"row {row_number} {code_field}")
            requested_code = scope.get(code_field)
            if requested_code and code.upper() != requested_code:
                raise ContractError("INDUSTRY_PROVIDER_SCOPE_MISMATCH", f"row {row_number} {code_field}")
        if level_count == 0:
            raise ContractError("INDUSTRY_PROVIDER_SCHEMA_INVALID", f"row {row_number} has no industry level")
        if scope.get("is_new") and "is_new" in frame.columns:
            response_is_new = _text(row.get("is_new")).upper()
            if response_is_new and response_is_new != scope["is_new"]:
                raise ContractError("INDUSTRY_PROVIDER_SCOPE_MISMATCH", f"row {row_number} is_new")
    return frame


def _validate_index_provider_frame(
    frame: object,
    scope: Mapping[str, Any],
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise ContractError("INDEX_PROVIDER_RESPONSE_INVALID", "response must be a DataFrame")
    if frame.empty:
        return frame
    missing = sorted(set(INDEX_PROVIDER_FIELDS) - set(frame.columns))
    if missing:
        raise ContractError("INDEX_PROVIDER_SCHEMA_MISSING", ",".join(missing))

    expected_codes = set(scope["index_codes"])
    seen_identities: dict[tuple[str, str, str], float | None] = {}
    for row_number, (_, row) in enumerate(frame.iterrows()):
        index_code = _provider_code(row.get("index_code"), "index_code", "INDEX_PROVIDER_SCHEMA_INVALID")
        asset = _provider_code(row.get("con_code"), "con_code", "INDEX_PROVIDER_SCHEMA_INVALID")
        if index_code not in expected_codes:
            raise ContractError("INDEX_PROVIDER_SCOPE_MISMATCH", f"row {row_number} index_code")
        snapshot_date = _provider_date(
            row.get("trade_date"),
            "trade_date",
            error_code="INDEX_PROVIDER_DATE_INVALID",
        )
        if not (scope["start_date"] <= snapshot_date <= scope["end_date"]):
            raise ContractError("INDEX_PROVIDER_SCOPE_MISMATCH", f"row {row_number} trade_date")
        weight_value: float | None = None
        if not _is_missing(row.get("weight")) and _text(row.get("weight")) != "":
            try:
                weight_value = float(row.get("weight"))
            except (TypeError, ValueError) as exc:
                raise ContractError("INDEX_PROVIDER_WEIGHT_INVALID", f"row {row_number}") from exc
            if not math.isfinite(weight_value) or weight_value < 0:
                raise ContractError("INDEX_PROVIDER_WEIGHT_INVALID", f"row {row_number}")
        identity = (index_code, asset, snapshot_date.isoformat())
        if identity in seen_identities and seen_identities[identity] != weight_value:
            raise ContractError("INDEX_PROVIDER_DUPLICATE_CONFLICT", f"row {row_number}")
        seen_identities[identity] = weight_value
    return frame


def _fetch_industry(
    context: PipelineRunContext,
    scope: Mapping[str, Any],
) -> tuple[pd.DataFrame, IndustryMembershipFetchReport]:
    _check_context(context)
    try:
        raw, report = fetch_industry_membership_with_report(
            tickers=scope["tickers"] or None,
            l1_code=scope["l1_code"],
            l2_code=scope["l2_code"],
            l3_code=scope["l3_code"],
            is_new=scope["is_new"],
            execution_control=context.execution_control,
        )
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("INDUSTRY_PROVIDER_REQUEST_FAILED", type(exc).__name__) from exc
    _check_context(context)
    raw = _validate_industry_provider_frame(raw, scope)
    _check_context(context)
    return raw, report


def _fetch_index(
    context: PipelineRunContext,
    scope: Mapping[str, Any],
) -> tuple[pd.DataFrame, IndexComponentFetchReport]:
    _check_context(context)
    try:
        raw, report = fetch_index_weights_with_report(
            scope["index_codes"],
            start_date=scope["start_date"].strftime("%Y%m%d"),
            end_date=scope["end_date"].strftime("%Y%m%d"),
            execution_control=context.execution_control,
        )
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("INDEX_PROVIDER_REQUEST_FAILED", type(exc).__name__) from exc
    _check_context(context)
    raw = _validate_index_provider_frame(raw, scope)
    _check_context(context)
    return raw, report


def _trade_date_resolver(context: PipelineRunContext) -> NextTradeDateResolver:
    _check_context(context)
    try:
        connection = _connect(context, read_only=True)
        try:
            rows = connection.execute(
                "SELECT trade_date FROM trading_calendar WHERE COALESCE(is_open, TRUE) ORDER BY trade_date"
            ).fetchall()
        finally:
            connection.close()
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("TRADING_CALENDAR_UNAVAILABLE", type(exc).__name__) from exc
    _check_context(context)
    dates = [item[0] for item in rows if item and item[0] is not None]
    if not dates:
        raise ContractError("TRADING_CALENDAR_UNAVAILABLE")
    try:
        return NextTradeDateResolver(dates)
    except Exception as exc:
        raise ContractError("TRADING_CALENDAR_UNAVAILABLE", type(exc).__name__) from exc


def _prepare_rows(frame: pd.DataFrame, table_name: str, error_code: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=get_table(table_name).column_names())
    try:
        prepared = align_to_schema(frame, table_name, fill_missing_optional=True, drop_extra=True)
        prepared = quick_validate(prepared, table_name, allow_extra=False)
    except Exception as exc:
        raise ContractError(error_code, type(exc).__name__) from exc
    table_revision = list(prepared[REVISION_ID].astype(str))
    if any(not item for item in table_revision):
        raise ContractError(error_code, "revision_id must be non-empty")
    return prepared.drop_duplicates(subset=[REVISION_ID], keep="last").reset_index(drop=True)


def _append_rows(
    context: PipelineRunContext,
    rows: pd.DataFrame,
    table_name: str,
    error_code: str,
) -> tuple[int, float]:
    started = monotonic()
    connection: duckdb.DuckDBPyConnection | None = None
    transaction_open = False
    view_name = f"{table_name}_contract_rows"
    registered = False
    try:
        _check_context(context)
        context.settings.paths.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(context.settings.paths.duckdb_path))
        _check_context(context)
        init_database(connection)
        _check_context(context)
        connection.execute("BEGIN TRANSACTION")
        transaction_open = True
        _check_context(context)
        inserted = 0
        if not rows.empty:
            connection.register(view_name, rows)
            registered = True
            _check_context(context)
            existing = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {view_name} AS incoming
                    JOIN {table_name} AS target
                      ON target.{REVISION_ID} = incoming.{REVISION_ID}
                    """
                ).fetchone()[0]
            )
            _check_context(context)
            columns = ", ".join(rows.columns)
            connection.execute(
                f"INSERT OR IGNORE INTO {table_name} ({columns}) SELECT {columns} FROM {view_name}"
            )
            _check_context(context)
            inserted = max(0, len(rows) - existing)
        if registered:
            connection.unregister(view_name)
            registered = False
        _check_context(context)
        connection.execute("COMMIT")
        transaction_open = False
        return inserted, monotonic() - started
    except ExecutionControlError as exc:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise ContractError(exc.code, exc.detail) from exc
    except ContractError:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise
    except Exception as exc:
        if connection is not None and transaction_open:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        raise ContractError(error_code, type(exc).__name__) from exc
    finally:
        if connection is not None:
            if registered:
                try:
                    connection.unregister(view_name)
                except Exception:
                    pass
            connection.close()


def _table_completion(context: PipelineRunContext, table_name: str, check_id: str, error_code: str) -> CheckResult:
    _check_context(context)
    try:
        connection = _connect(context, read_only=True)
        try:
            total_rows = int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
        finally:
            connection.close()
    except ContractError:
        raise
    except Exception as exc:
        return CheckResult.failure(check_id, error_code, "output table is not queryable after commit", exception=type(exc).__name__)
    _check_context(context)
    return CheckResult.success(check_id, table=table_name, total_rows=total_rows, marker="committed append is queryable")


def _revision_quality(context: PipelineRunContext, table_name: str, check_id: str, error_code: str) -> CheckResult:
    _check_context(context)
    try:
        connection = _connect(context, read_only=True)
        try:
            duplicate = connection.execute(
                f"""
                SELECT {REVISION_ID}
                FROM {table_name}
                GROUP BY {REVISION_ID}
                HAVING COUNT(*) > 1
                LIMIT 1
                """
            ).fetchone()
        finally:
            connection.close()
    except ContractError:
        raise
    except Exception as exc:
        return CheckResult.failure(check_id, error_code, "revision uniqueness query failed", exception=type(exc).__name__)
    _check_context(context)
    if duplicate is not None:
        return CheckResult.failure(check_id, error_code, "output contains duplicate revision_id")
    return CheckResult.success(check_id, unique_key=[REVISION_ID], revisions_unique=True)


def _industry_completion(context: PipelineRunContext) -> CheckResult:
    return _table_completion(context, INDUSTRY_TABLE, "industry_output_completion", "INDUSTRY_COMPLETION_MISSING")


def _industry_quality(context: PipelineRunContext) -> CheckResult:
    return _revision_quality(context, INDUSTRY_TABLE, "industry_revision_quality", "INDUSTRY_REVISION_DUPLICATE")


def _index_completion(context: PipelineRunContext) -> CheckResult:
    return _table_completion(context, INDEX_TABLE, "index_output_completion", "INDEX_COMPLETION_MISSING")


def _index_quality(context: PipelineRunContext) -> CheckResult:
    return _revision_quality(context, INDEX_TABLE, "index_revision_quality", "INDEX_REVISION_DUPLICATE")


def _execute_industry_impl(context: PipelineRunContext) -> BusinessExecution:
    started = monotonic()
    scope = _industry_scope(context)
    resolver = _trade_date_resolver(context)
    fetch_started = monotonic()
    raw, report = _fetch_industry(context, scope)
    _check_context(context)
    try:
        cleaned = clean_industry_membership(
            raw,
            trade_date_resolver=resolver,
            execution_control=context.execution_control,
        )
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("INDUSTRY_CLEAN_FAILED", type(exc).__name__) from exc
    _check_context(context)
    prepared = _prepare_rows(cleaned, INDUSTRY_TABLE, "INDUSTRY_CLEAN_SCHEMA_INVALID")
    prepared_at = monotonic()
    _check_context(context)
    inserted, write_seconds = _append_rows(context, prepared, INDUSTRY_TABLE, "INDUSTRY_WRITE_FAILED")
    _check_context(context)
    assets = int(prepared[ASSET_ID].nunique()) if not prepared.empty else 0
    industry_codes = int(prepared[INDUSTRY_CODE].nunique()) if not prepared.empty else 0
    dates = int(prepared[EFFECTIVE_FROM].nunique()) if not prepared.empty else 0
    output = OutputResult(
        output_id=INDUSTRY_TABLE,
        rows_written=inserted,
        location="settings.paths.duckdb_path",
        completed=True,
        detail={
            "scope_mode": scope["scope_mode"],
            "scope_units": scope["scope_units"],
            "rows_received": len(raw),
            "rows_cleaned": len(prepared),
            "rows_inserted": inserted,
            "asset_count": assets,
            "industry_count": industry_codes,
            "effective_date_count": dates,
            "api_requests": report.api_requests,
            "batches": report.batches,
            "retries": report.retries,
            "provider_completeness_boundary": report.completeness_boundary,
            "empty_scope_result": len(raw) == 0,
            "history_semantics": "append physical revisions; PIT readers resolve latest by business identity",
        },
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=len(raw),
            rows_written=inserted,
            assets_processed=assets,
            dates_processed=dates,
            database_write_seconds=write_seconds,
            stage_durations_seconds={
                "provider": max(0.0, fetch_started - started),
                "clean_and_prepare": max(0.0, prepared_at - fetch_started),
                "database_write": write_seconds,
            },
            api_requests=report.api_requests,
            batches=report.batches,
            retries=report.retries,
        ),
        outputs=(output,),
    )


def _execute_industry(context: PipelineRunContext) -> BusinessExecution:
    try:
        return _execute_industry_impl(context)
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc


def _execute_index_impl(context: PipelineRunContext) -> BusinessExecution:
    started = monotonic()
    scope = _index_scope(context)
    resolver = _trade_date_resolver(context)
    fetch_started = monotonic()
    raw, report = _fetch_index(context, scope)
    _check_context(context)
    try:
        cleaned = clean_index_component(
            raw,
            trade_date_resolver=resolver,
            execution_control=context.execution_control,
        )
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("INDEX_CLEAN_FAILED", type(exc).__name__) from exc
    _check_context(context)
    prepared = _prepare_rows(cleaned, INDEX_TABLE, "INDEX_CLEAN_SCHEMA_INVALID")
    prepared_at = monotonic()
    _check_context(context)
    inserted, write_seconds = _append_rows(context, prepared, INDEX_TABLE, "INDEX_WRITE_FAILED")
    _check_context(context)
    assets = int(prepared[ASSET_ID].nunique()) if not prepared.empty else 0
    dates = int(prepared[SNAPSHOT_DATE].nunique()) if not prepared.empty else 0
    output = OutputResult(
        output_id=INDEX_TABLE,
        rows_written=inserted,
        location="settings.paths.duckdb_path",
        completed=True,
        detail={
            "index_codes": list(scope["index_codes"]),
            "start_date": scope["start_date"].isoformat(),
            "end_date": scope["end_date"].isoformat(),
            "scope_units": scope["scope_units"],
            "rows_received": len(raw),
            "rows_cleaned": len(prepared),
            "rows_inserted": inserted,
            "asset_count": assets,
            "index_count": len(scope["index_codes"]),
            "snapshot_date_count": dates,
            "api_requests": report.api_requests,
            "batches": report.batches,
            "retries": report.retries,
            "provider_completeness_boundary": report.completeness_boundary,
            "empty_range_result": len(raw) == 0,
            "history_semantics": "append physical revisions; PIT readers resolve latest by index/snapshot/asset",
        },
    )
    return BusinessExecution.success(
        metrics=PipelineMetrics(
            rows_read=len(raw),
            rows_written=inserted,
            assets_processed=assets,
            dates_processed=dates,
            database_write_seconds=write_seconds,
            stage_durations_seconds={
                "provider": max(0.0, fetch_started - started),
                "clean_and_prepare": max(0.0, prepared_at - fetch_started),
                "database_write": write_seconds,
            },
            api_requests=report.api_requests,
            batches=report.batches,
            retries=report.retries,
        ),
        outputs=(output,),
    )


def _execute_index(context: PipelineRunContext) -> BusinessExecution:
    try:
        return _execute_index_impl(context)
    except ExecutionControlError as exc:
        raise ContractError(exc.code, exc.detail) from exc


def _industry_idempotency() -> IdempotencyContract:
    return IdempotencyContract(
        idempotency_key="revision_id; business identity=(classification_system, asset_id, industry_level, industry_code, effective_from)",
        repeat_run_semantics="same explicit scope and unchanged provider payload produce the same revision_id and zero new rows",
        existing_target_handling="INSERT OR IGNORE by revision_id; changed name or interval payload is retained as a new physical revision",
        failure_recovery="all scopes are fetched and validated before one database transaction; provider or write failure rolls back the transaction and the scope can be retried",
        uses_staging=False,
        atomic_replace_boundary="one transaction around the complete prepared explicit scope",
    )


def _index_idempotency() -> IdempotencyContract:
    return IdempotencyContract(
        idempotency_key="revision_id; business identity=(index_code, snapshot_date, asset_id)",
        repeat_run_semantics="same explicit index/date range and unchanged weight payload produce the same revision_id and zero new rows",
        existing_target_handling="INSERT OR IGNORE by revision_id; changed weight is retained as a new physical revision for PIT resolution",
        failure_recovery="all index scopes are fetched and validated before one database transaction; provider or write failure rolls back the transaction and the range can be retried",
        uses_staging=False,
        atomic_replace_boundary="one transaction around the complete prepared explicit index/date range",
    )


def _performance() -> PerformanceBudget:
    return PerformanceBudget(
        normal_budget_seconds=600.0,
        warning_threshold_seconds=300.0,
        hard_timeout_seconds=900,
        benchmark_scope="mocked explicit multi-scope historical fetch, cleaning, and one DuckDB append transaction",
        baseline_source=(
            "tests/pipeline/test_membership_contracts.py offline provider and temporary-DuckDB acceptance; "
            "src/qrp_atlas/pipeline/industry_membership/fetch.py and "
            "src/qrp_atlas/pipeline/index_component/fetch.py five-attempt retry behavior"
        ),
    )


INDUSTRY_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="industry_membership_manual_observation_date",
    description="Use the scheduled Shanghai calendar date only as an observation label; provider history is not date-filtered.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_scheduled_observation_window,
    validate_explicit_date=_reject_explicit_target_date,
)


INDEX_TARGET_DATE_POLICY = TargetDatePolicy(
    policy_id="index_component_manual_source_range",
    description="Use the required start_date and end_date parameters as an inclusive provider source range.",
    trading_calendar_id="quant_db.trading_calendar",
    non_trading_day_policy=NonTradingDayPolicy.ALLOW_CALENDAR_DATE,
    resolver=_index_source_window,
    validate_explicit_date=_reject_explicit_target_date,
)


INDUSTRY_MEMBERSHIP_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="industry_membership_ingest",
        name="Industry membership ingestion",
        description=(
            "Fetches Shenwan industry membership history for an explicit ticker or industry-code scope, "
            "validates the provider boundary, and appends PIT revisions transactionally."
        ),
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_industry,
        target_date_policy=INDUSTRY_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract("tickers", ParameterType.STRING, "Comma-separated stock codes; mutually exclusive with industry code filters.", default=""),
            ParameterContract("l1_code", ParameterType.STRING, "Optional Shenwan level-1 industry code.", default=""),
            ParameterContract("l2_code", ParameterType.STRING, "Optional Shenwan level-2 industry code.", default=""),
            ParameterContract("l3_code", ParameterType.STRING, "Optional Shenwan level-3 industry code.", default=""),
            ParameterContract("is_new", ParameterType.STRING, "Optional Tushare membership freshness filter: Y or N.", default=""),
        ),
        inputs=(
            _tushare_input(
                input_id="external_tushare_index_member_all",
                source="tushare.index_member_all",
                fields=INDUSTRY_PROVIDER_FIELDS,
                freshness_checker=_industry_provider_freshness,
                freshness_id="industry_provider_freshness",
                freshness_error_code="INDUSTRY_PROVIDER_INCOMPLETE",
            ),
            _calendar_input(),
        ),
        outputs=(
            OutputContract(
                output_id=INDUSTRY_TABLE,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=INDUSTRY_TABLE,
                unique_key=INDUSTRY_MEMBERSHIP_HISTORY.primary_key,
                write_mode=WriteMode.APPEND,
                target_date_semantics="append all validated history returned for the explicit scope; scheduled date is observation metadata only",
                completion=CompletionContract(
                    marker="committed append is queryable in industry_membership_history",
                    error_code="INDUSTRY_COMPLETION_MISSING",
                    checker=_industry_completion,
                ),
                quality_checks=(_industry_quality,),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(TRADING_CALENDAR_READ,),
        idempotency=_industry_idempotency(),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="all provider scope units are fetched, validated, cleaned, and appended in one transaction; no scope is committed early",
            failure_visibility="provider, validation, cancellation, or database failure leaves no new rows from the current invocation",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=_performance(),
        manual_execution_allowed=True,
    )
)


INDEX_COMPONENT_INGEST = register_pipeline(
    PipelineContract(
        pipeline_id="index_component_ingest",
        name="Index component ingestion",
        description=(
            "Fetches index component weight snapshots for explicit index codes and an inclusive date range, "
            "validates range boundaries, and appends PIT revisions transactionally."
        ),
        contract_version="1.1.0",
        kind=PipelineKind.ATOMIC,
        executor=_execute_index,
        target_date_policy=INDEX_TARGET_DATE_POLICY,
        parameters=(
            ParameterContract("index_codes", ParameterType.STRING, "Comma-separated Tushare index codes.", required=True),
            ParameterContract("start_date", ParameterType.DATE, "Inclusive provider source start date in ISO format.", required=True),
            ParameterContract("end_date", ParameterType.DATE, "Inclusive provider source end date in ISO format.", required=True),
        ),
        inputs=(
            _tushare_input(
                input_id="external_tushare_index_weight",
                source="tushare.index_weight",
                fields=INDEX_PROVIDER_FIELDS,
                freshness_checker=_index_provider_freshness,
                freshness_id="index_provider_freshness",
                freshness_error_code="INDEX_PROVIDER_INCOMPLETE",
            ),
            _calendar_input(),
        ),
        outputs=(
            OutputContract(
                output_id=INDEX_TABLE,
                physical_resource=QUANT_DB_RESOURCE,
                location="settings.paths.duckdb_path",
                object_name=INDEX_TABLE,
                unique_key=INDEX_COMPONENT_HISTORY.primary_key,
                write_mode=WriteMode.APPEND,
                target_date_semantics="append all validated index snapshots returned for each explicit index code within the inclusive source range",
                completion=CompletionContract(
                    marker="committed append is queryable in index_component_history",
                    error_code="INDEX_COMPLETION_MISSING",
                    checker=_index_completion,
                ),
                quality_checks=(_index_quality,),
                allow_empty=True,
            ),
        ),
        dependencies=(),
        resource_locks=(QUANT_DB_WRITER,),
        resource_reads=(TRADING_CALENDAR_READ,),
        idempotency=_index_idempotency(),
        transaction=TransactionContract(
            mode=TransactionMode.DATABASE_TRANSACTION,
            boundary="all requested index-code responses are fetched, validated, cleaned, and appended in one transaction; no index range is committed early",
            failure_visibility="provider, validation, cancellation, or database failure leaves no new rows from the current invocation",
        ),
        execution=ExecutionPolicy(overlap_policy=OverlapPolicy.FORBID, max_retries=1),
        performance=_performance(),
        manual_execution_allowed=True,
    )
)


MEMBERSHIP_CONTRACTS: tuple[PipelineContract, ...] = (
    INDUSTRY_MEMBERSHIP_INGEST,
    INDEX_COMPONENT_INGEST,
)


__all__ = [
    "INDEX_COMPONENT_INGEST",
    "INDEX_TARGET_DATE_POLICY",
    "INDUSTRY_MEMBERSHIP_INGEST",
    "INDUSTRY_TARGET_DATE_POLICY",
    "MEMBERSHIP_CONTRACTS",
]
