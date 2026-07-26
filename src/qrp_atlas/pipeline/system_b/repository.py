"""DuckDB persistence and set-based input access for System B monitoring."""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ACTUAL_PAIR_CONTIGUOUS,
    ASSET_ID,
    CALCULATION_VERSION,
    CLOSE,
    COMPLETED_AT,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    CREATED_AT,
    DIAGNOSTICS,
    INPUT_SNAPSHOT_ID,
    IS_ABOVE_OR_EQUAL_MA5,
    IS_TRADING_DAY,
    LATEST_ACTUAL_CLOSE,
    LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    LATEST_ACTUAL_MA5,
    LATEST_ACTUAL_MA5_WINDOW_COMPLETE,
    LATEST_ACTUAL_TRADE_DATE,
    LIFECYCLE_STATE,
    LISTING_TRADING_DAY_NUMBER,
    LISTING_TRADING_DAY_NUMBER_IS_EXACT,
    MA5,
    MA5_WINDOW_COMPLETE,
    MARKET_FACT_STATUS,
    PARAMETER_SET_ID,
    PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE,
    PREVIOUS_ACTUAL_TRADE_DATE,
    PREVIOUS_TREND_STATE,
    PRICE_ADJUSTMENT,
    PRODUCTION_RUN_ID,
    RULE_VERSION_SET_ID,
    SOURCE_RULE_IDS,
    STATE_CHANGED,
    STATE_BASIS_SEQUENCE_INTACT,
    SYSTEM_B_2_0_PARAMETER_SET_ID,
    SYSTEM_B_2_0_RULE_VERSION_SET_ID,
    SYSTEM_B_LATEST_STATE_VIEW,
    SYSTEM_B_PRODUCTION_RUN,
    SYSTEM_B_PRODUCTION_RUN_TABLE,
    SYSTEM_B_STATE_OBSERVATION,
    SYSTEM_B_STATE_OBSERVATION_TABLE,
    TRADE_DATE,
    TREND_STATE,
)


REQUIRED_INPUT_TABLES = (
    "stock_info",
    "trading_calendar",
    "daily_market_snapshot",
    "adj_factor_changes",
    "suspend_d",
)

ACTUAL_TRADING = "ACTUAL_TRADING"
EXPLICIT_NON_TRADING = "EXPLICIT_NON_TRADING"
UNRESOLVED_MISSING = "UNRESOLVED_MISSING"

STATE_INSERT_COLUMNS = (
    ASSET_ID,
    TRADE_DATE,
    LIFECYCLE_STATE,
    TREND_STATE,
    PREVIOUS_TREND_STATE,
    STATE_CHANGED,
    MARKET_FACT_STATUS,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    LISTING_TRADING_DAY_NUMBER_IS_EXACT,
    CLOSE,
    MA5,
    MA5_WINDOW_COMPLETE,
    IS_ABOVE_OR_EQUAL_MA5,
    LATEST_ACTUAL_TRADE_DATE,
    LATEST_ACTUAL_CLOSE,
    LATEST_ACTUAL_MA5,
    LATEST_ACTUAL_MA5_WINDOW_COMPLETE,
    LATEST_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_TRADE_DATE,
    PREVIOUS_ACTUAL_IS_ABOVE_OR_EQUAL_MA5,
    PREVIOUS_ACTUAL_MA5_WINDOW_COMPLETE,
    STATE_BASIS_SEQUENCE_INTACT,
    ACTUAL_PAIR_CONTIGUOUS,
    PRICE_ADJUSTMENT,
    RULE_VERSION_SET_ID,
    PARAMETER_SET_ID,
    SOURCE_RULE_IDS,
    DIAGNOSTICS,
)


class SystemBProductionError(RuntimeError):
    """Stable production-boundary failure with a machine-readable code."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


def open_database(path: Path, *, read_only: bool) -> duckdb.DuckDBPyConnection:
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def table_exists(connection: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and row[0])


def ensure_system_b_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(SYSTEM_B_STATE_OBSERVATION.duckdb_create_sql())
    connection.execute(SYSTEM_B_PRODUCTION_RUN.duckdb_create_sql())
    actual_columns = [
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
            """,
            [SYSTEM_B_STATE_OBSERVATION_TABLE],
        ).fetchall()
    ]
    expected_columns = [column.name for column in SYSTEM_B_STATE_OBSERVATION.columns]
    actual_primary_key = [
        row[1]
        for row in connection.execute(
            f"SELECT * FROM pragma_table_info('{SYSTEM_B_STATE_OBSERVATION_TABLE}') WHERE pk ORDER BY pk"
        ).fetchall()
    ]
    expected_primary_key = list(SYSTEM_B_STATE_OBSERVATION.primary_key)
    if actual_columns != expected_columns or actual_primary_key != expected_primary_key:
        raise SystemBProductionError(
            "SYSTEM_B_SCHEMA_RECREATION_REQUIRED",
            "existing System B state schema belongs to an incompatible calculation model",
        )
    connection.execute(
        f"""
        CREATE OR REPLACE VIEW {SYSTEM_B_LATEST_STATE_VIEW} AS
        WITH latest_date AS (
            SELECT
                observation.{ASSET_ID},
                observation.{RULE_VERSION_SET_ID},
                observation.{PARAMETER_SET_ID},
                max(observation.{TRADE_DATE}) AS {TRADE_DATE}
            FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} AS observation
            JOIN {SYSTEM_B_PRODUCTION_RUN_TABLE} AS run
              ON run.{PRODUCTION_RUN_ID} = observation.{PRODUCTION_RUN_ID}
             AND run.status = 'SUCCEEDED'
            GROUP BY 1, 2, 3
        )
        SELECT observation.*
        FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} AS observation
        JOIN {SYSTEM_B_PRODUCTION_RUN_TABLE} AS run
          ON run.{PRODUCTION_RUN_ID} = observation.{PRODUCTION_RUN_ID}
         AND run.status = 'SUCCEEDED'
        JOIN latest_date
          ON latest_date.{ASSET_ID} = observation.{ASSET_ID}
         AND latest_date.{RULE_VERSION_SET_ID} = observation.{RULE_VERSION_SET_ID}
         AND latest_date.{PARAMETER_SET_ID} = observation.{PARAMETER_SET_ID}
         AND latest_date.{TRADE_DATE} = observation.{TRADE_DATE}
        """
    )


def validate_source_schema(connection: duckdb.DuckDBPyConnection) -> None:
    missing = [name for name in REQUIRED_INPUT_TABLES if not table_exists(connection, name)]
    if missing:
        raise SystemBProductionError(
            "MISSING_SOURCE_TABLES",
            f"required source tables are missing: {', '.join(missing)}",
        )


def _register_asset_filter(
    connection: duckdb.DuckDBPyConnection,
    asset_ids: Sequence[str] | None,
) -> str:
    if not asset_ids:
        return ""
    normalized = sorted({str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()})
    if not normalized:
        raise SystemBProductionError("EMPTY_ASSET_FILTER", "asset filter contains no valid IDs")
    connection.register("system_b_selected_assets", pd.DataFrame({ASSET_ID: normalized}))
    return "JOIN system_b_selected_assets selected ON selected.asset_id = stock.ticker"


def standard_input_sql(
    *,
    end_date: date,
    target_date: date | None,
    asset_filter_join: str,
) -> tuple[str, list[Any]]:
    target_filter = "" if target_date is None else "QUALIFY row_number() OVER (PARTITION BY asset_id ORDER BY trade_date DESC) <= 2"
    target_stock_filter = (
        "" if target_date is None
        else "AND (stock.delist_date IS NULL OR stock.delist_date >= ?)"
    )
    params: list[Any] = [end_date]
    if target_date is not None:
        params.append(target_date)
    params.append(end_date)
    return (
        f"""
        WITH selected_stock AS (
            SELECT stock.ticker, stock.list_date, stock.delist_date
            FROM stock_info AS stock
            {asset_filter_join}
            WHERE stock.list_date IS NOT NULL AND stock.list_date <= ?
              {target_stock_filter}
        ),
        market_calendar AS (
            SELECT trade_date FROM trading_calendar
            WHERE is_open = TRUE AND trade_date <= ?
        ),
        domain AS (
            SELECT stock.ticker AS asset_id, calendar.trade_date
            FROM selected_stock AS stock
            JOIN market_calendar AS calendar
              ON calendar.trade_date >= stock.list_date
             AND (stock.delist_date IS NULL OR calendar.trade_date <= stock.delist_date)
        ),
        explicit_suspension AS (
            SELECT DISTINCT ticker AS asset_id, trade_date
            FROM suspend_d
            WHERE upper(coalesce(suspend_type, '')) NOT LIKE '%复牌%'
        ),
        domain_fact AS (
            SELECT
                domain.asset_id,
                domain.trade_date,
                CASE
                    WHEN suspension.asset_id IS NOT NULL THEN '{EXPLICIT_NON_TRADING}'
                    WHEN daily.ticker IS NOT NULL AND daily.volume = 0 THEN '{EXPLICIT_NON_TRADING}'
                    WHEN daily.ticker IS NOT NULL AND daily.close IS NOT NULL
                         AND coalesce(daily.volume, 0) > 0 THEN '{ACTUAL_TRADING}'
                    ELSE '{UNRESOLVED_MISSING}'
                END AS {MARKET_FACT_STATUS},
                daily.close AS raw_close,
                sum(CASE
                    WHEN suspension.asset_id IS NULL
                     AND NOT (daily.ticker IS NOT NULL AND daily.volume = 0)
                     AND NOT (daily.ticker IS NOT NULL AND daily.close IS NOT NULL
                              AND coalesce(daily.volume, 0) > 0)
                    THEN 1 ELSE 0 END
                ) OVER (PARTITION BY domain.asset_id ORDER BY domain.trade_date) AS unresolved_count
            FROM domain
            LEFT JOIN daily_market_snapshot AS daily
              ON daily.ticker = domain.asset_id AND daily.trade_date = domain.trade_date
            LEFT JOIN explicit_suspension AS suspension
              ON suspension.asset_id = domain.asset_id AND suspension.trade_date = domain.trade_date
        ),
        raw_actual AS (
            SELECT
                fact.asset_id,
                fact.trade_date,
                fact.raw_close,
                fact.unresolved_count,
                coalesce(factor.adj_factor, 1.0) AS adj_factor,
                row_number() OVER (PARTITION BY fact.asset_id ORDER BY fact.trade_date)::INTEGER
                    AS confirmed_listing_trading_day_count,
                row_number() OVER (
                    PARTITION BY fact.asset_id, fact.unresolved_count ORDER BY fact.trade_date
                )::INTEGER AS confirmed_segment_trading_day_count
            FROM domain_fact AS fact
            ASOF LEFT JOIN adj_factor_changes AS factor
              ON fact.asset_id = factor.ticker AND fact.trade_date >= factor.trade_date
            WHERE fact.{MARKET_FACT_STATUS} = '{ACTUAL_TRADING}'
        ),
        latest_factor AS (
            SELECT asset_id, coalesce(arg_max(adj_factor, trade_date), 1.0) AS latest_adj_factor
            FROM raw_actual GROUP BY asset_id
        ),
        adjusted_actual AS (
            SELECT
                actual.asset_id,
                actual.trade_date,
                actual.unresolved_count,
                actual.confirmed_listing_trading_day_count,
                actual.confirmed_segment_trading_day_count,
                actual.raw_close * actual.adj_factor / latest.latest_adj_factor AS close
            FROM raw_actual AS actual
            JOIN latest_factor AS latest USING (asset_id)
        ),
        indicator_values AS (
            SELECT
                asset_id,
                trade_date,
                unresolved_count,
                confirmed_listing_trading_day_count,
                confirmed_segment_trading_day_count,
                close,
                confirmed_segment_trading_day_count >= 5 AS ma5_window_complete,
                CASE WHEN confirmed_segment_trading_day_count >= 5 THEN
                    avg(close) OVER (
                        PARTITION BY asset_id, unresolved_count ORDER BY trade_date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    )
                END AS ma5
            FROM adjusted_actual
        ),
        actual_relation AS (
            SELECT *, CASE WHEN ma5 IS NULL THEN NULL ELSE close >= ma5 END AS is_above
            FROM indicator_values
        ),
        actual_history AS (
            SELECT
                *,
                lag(trade_date) OVER (PARTITION BY asset_id ORDER BY trade_date)
                    AS previous_actual_trade_date,
                lag(is_above) OVER (PARTITION BY asset_id ORDER BY trade_date)
                    AS previous_actual_is_above,
                lag(ma5_window_complete) OVER (PARTITION BY asset_id ORDER BY trade_date)
                    AS previous_actual_ma5_window_complete,
                coalesce(
                    unresolved_count = lag(unresolved_count) OVER (
                        PARTITION BY asset_id ORDER BY trade_date
                    ),
                    FALSE
                ) AS actual_pair_contiguous
            FROM actual_relation
        ),
        observation_basis AS (
            SELECT
                fact.asset_id,
                fact.trade_date,
                fact.{MARKET_FACT_STATUS},
                fact.{MARKET_FACT_STATUS} = '{ACTUAL_TRADING}' AS is_trading_day,
                CASE WHEN fact.unresolved_count = 0
                     THEN coalesce(actual.confirmed_listing_trading_day_count, 0)::INTEGER
                END AS listing_trading_day_number,
                coalesce(actual.confirmed_listing_trading_day_count, 0)::INTEGER
                    AS confirmed_listing_trading_day_count,
                fact.unresolved_count = 0 AS listing_trading_day_number_is_exact,
                CASE WHEN fact.{MARKET_FACT_STATUS} = '{ACTUAL_TRADING}' THEN actual.close END AS close,
                CASE WHEN fact.{MARKET_FACT_STATUS} = '{ACTUAL_TRADING}' THEN actual.ma5 END AS ma5,
                coalesce(
                    fact.{MARKET_FACT_STATUS} = '{ACTUAL_TRADING}'
                    AND actual.ma5_window_complete,
                    FALSE
                ) AS ma5_window_complete,
                actual.trade_date AS latest_actual_trade_date,
                actual.close AS latest_actual_close,
                actual.ma5 AS latest_actual_ma5,
                coalesce(actual.ma5_window_complete, FALSE)
                    AS latest_actual_ma5_window_complete,
                actual.is_above AS latest_actual_is_above,
                actual.previous_actual_trade_date,
                actual.previous_actual_is_above,
                coalesce(actual.previous_actual_ma5_window_complete, FALSE)
                    AS previous_actual_ma5_window_complete,
                coalesce(fact.unresolved_count = actual.unresolved_count, FALSE)
                    AS state_basis_sequence_intact,
                coalesce(actual.actual_pair_contiguous, FALSE) AS actual_pair_contiguous
            FROM domain_fact AS fact
            ASOF LEFT JOIN actual_history AS actual
              ON fact.asset_id = actual.asset_id AND fact.trade_date >= actual.trade_date
        )
        SELECT
            asset_id,
            trade_date,
            {MARKET_FACT_STATUS},
            is_trading_day,
            listing_trading_day_number,
            confirmed_listing_trading_day_count,
            listing_trading_day_number_is_exact,
            close,
            ma5,
            ma5_window_complete,
            latest_actual_trade_date,
            latest_actual_close,
            latest_actual_ma5,
            latest_actual_ma5_window_complete,
            latest_actual_is_above AS latest_actual_is_above_or_equal_ma5,
            previous_actual_trade_date,
            previous_actual_is_above AS previous_actual_is_above_or_equal_ma5,
            previous_actual_ma5_window_complete,
            state_basis_sequence_intact,
            actual_pair_contiguous
        FROM observation_basis
        {target_filter}
        ORDER BY asset_id, trade_date
        """,
        params,
    )


def execute_standard_input(
    connection: duckdb.DuckDBPyConnection,
    *,
    end_date: date,
    target_date: date | None = None,
    asset_ids: Sequence[str] | None = None,
) -> duckdb.DuckDBPyConnection:
    validate_source_schema(connection)
    asset_join = _register_asset_filter(connection, asset_ids)
    sql, params = standard_input_sql(
        end_date=end_date,
        target_date=target_date,
        asset_filter_join=asset_join,
    )
    return connection.execute(sql, params)


def iter_asset_batches(
    cursor: duckdb.DuckDBPyConnection,
    *,
    asset_batch_size: int,
    vectors_per_chunk: int = 32,
) -> Iterator[pd.DataFrame]:
    if asset_batch_size < 1:
        raise ValueError("asset_batch_size must be >= 1")
    pending = pd.DataFrame()
    while True:
        chunk = cursor.fetch_df_chunk(vectors_per_chunk)
        if chunk.empty:
            break
        pending = pd.concat([pending, chunk], ignore_index=True) if not pending.empty else chunk
        while True:
            assets = pending[ASSET_ID].drop_duplicates().tolist()
            if len(assets) <= asset_batch_size:
                break
            selected = set(assets[:asset_batch_size])
            mask = pending[ASSET_ID].isin(selected)
            yield pending.loc[mask].reset_index(drop=True)
            pending = pending.loc[~mask].reset_index(drop=True)
    if not pending.empty:
        yield pending.reset_index(drop=True)


def create_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    production_run_id: str,
    run_type: str,
    target_start_date: date | None,
    target_end_date: date | None,
    rule_version_set_id: str,
    parameter_set_id: str,
    calculation_version: str,
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    connection.execute(
        f"""
        INSERT INTO {SYSTEM_B_PRODUCTION_RUN_TABLE} (
            production_run_id, run_type, status, target_start_date, target_end_date,
            rule_version_set_id, parameter_set_id, calculation_version,
            asset_count, input_row_count, output_row_count, error_count,
            metrics, created_at
        ) VALUES (?, ?, 'RUNNING', ?, ?, ?, ?, ?, 0, 0, 0, 0, '{{}}', ?)
        """,
        [
            production_run_id,
            run_type,
            target_start_date,
            target_end_date,
            rule_version_set_id,
            parameter_set_id,
            calculation_version,
            now,
        ],
    )


def fail_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    production_run_id: str,
    error_code: str,
    error_detail: str,
    metrics: dict[str, Any],
) -> None:
    connection.execute(
        f"""
        UPDATE {SYSTEM_B_PRODUCTION_RUN_TABLE}
        SET status = 'FAILED', error_count = 1, error_code = ?, error_detail = ?,
            metrics = ?, completed_at = ?
        WHERE production_run_id = ?
        """,
        [
            error_code,
            error_detail[:4000],
            json.dumps(metrics, ensure_ascii=False, sort_keys=True),
            datetime.now(timezone.utc).replace(tzinfo=None),
            production_run_id,
        ],
    )


def import_staging(
    connection: duckdb.DuckDBPyConnection,
    *,
    parquet_glob: str,
    production_run_id: str,
    input_snapshot_id: str,
    calculation_version: str,
    metrics: dict[str, Any],
    replace_start_date: date,
    replace_end_date: date,
    replace_asset_ids: Sequence[str] | None = None,
) -> None:
    if replace_start_date > replace_end_date:
        raise ValueError("replace_start_date must not be after replace_end_date")
    state_columns = ", ".join(STATE_INSERT_COLUMNS)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    connection.execute("BEGIN")
    try:
        delete_sql = f"""
            DELETE FROM {SYSTEM_B_STATE_OBSERVATION_TABLE}
            WHERE rule_version_set_id = ?
              AND parameter_set_id = ?
              AND trade_date BETWEEN ? AND ?
        """
        delete_params: list[Any] = [
            SYSTEM_B_2_0_RULE_VERSION_SET_ID,
            SYSTEM_B_2_0_PARAMETER_SET_ID,
            replace_start_date,
            replace_end_date,
        ]
        if replace_asset_ids is not None:
            normalized = sorted({str(asset_id) for asset_id in replace_asset_ids})
            if not normalized:
                raise SystemBProductionError("EMPTY_ASSET_FILTER", "replacement asset scope is empty")
            delete_sql += f" AND asset_id IN ({', '.join('?' for _ in normalized)})"
            delete_params.extend(normalized)
        connection.execute(delete_sql, delete_params)
        connection.execute(
            f"""
            INSERT INTO {SYSTEM_B_STATE_OBSERVATION_TABLE} (
                {state_columns}, production_run_id, input_snapshot_id,
                calculation_version, created_at
            )
            SELECT {state_columns}, ?, ?, ?, ?
            FROM read_parquet(?)
            """,
            [production_run_id, input_snapshot_id, calculation_version, now, parquet_glob],
        )
        connection.execute(
            f"""
            UPDATE {SYSTEM_B_PRODUCTION_RUN_TABLE}
            SET status = 'SUCCEEDED', input_snapshot_id = ?,
                asset_count = ?, input_row_count = ?, output_row_count = ?,
                error_count = 0, metrics = ?, completed_at = ?
            WHERE production_run_id = ?
            """,
            [
                input_snapshot_id,
                int(metrics["asset_count"]),
                int(metrics["input_row_count"]),
                int(metrics["output_row_count"]),
                json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                now,
                production_run_id,
            ],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def update_run_metrics(
    connection: duckdb.DuckDBPyConnection,
    *,
    production_run_id: str,
    metrics: dict[str, Any],
) -> None:
    connection.execute(
        f"UPDATE {SYSTEM_B_PRODUCTION_RUN_TABLE} SET metrics = ? WHERE production_run_id = ?",
        [json.dumps(metrics, ensure_ascii=False, sort_keys=True), production_run_id],
    )


def latest_run(connection: duckdb.DuckDBPyConnection) -> dict[str, Any] | None:
    if not table_exists(connection, SYSTEM_B_PRODUCTION_RUN_TABLE):
        return None
    cursor = connection.execute(
        f"SELECT * FROM {SYSTEM_B_PRODUCTION_RUN_TABLE} ORDER BY created_at DESC LIMIT 1"
    )
    row = cursor.fetchone()
    return dict(zip([item[0] for item in cursor.description], row, strict=True)) if row else None
