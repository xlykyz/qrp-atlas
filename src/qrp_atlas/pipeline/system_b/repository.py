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
    ASSET_ID,
    CALCULATION_VERSION,
    CLOSE,
    COMPLETED_AT,
    CONSECUTIVE_ABOVE_MA5_DAYS,
    CONSECUTIVE_BELOW_MA5_DAYS,
    CREATED_AT,
    DIAGNOSTICS,
    INPUT_SNAPSHOT_ID,
    IS_ABOVE_OR_EQUAL_MA5,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    MA5,
    PARAMETER_SET_ID,
    PREVIOUS_TREND_STATE,
    PRICE_ADJUSTMENT,
    PRODUCTION_RUN_ID,
    RULE_VERSION_SET_ID,
    SOURCE_RULE_IDS,
    STATE_CHANGED,
    SYSTEM_B_LATEST_STATE_VIEW,
    SYSTEM_B_PRODUCTION_RUN,
    SYSTEM_B_PRODUCTION_RUN_TABLE,
    SYSTEM_B_STATE_OBSERVATION,
    SYSTEM_B_STATE_OBSERVATION_TABLE,
    TRADE_DATE,
    TREND_STATE,
    UNDERLYING_TREND_STATE,
    SystemBStateCheckpoint,
    SystemBTrendState,
)


REQUIRED_INPUT_TABLES = (
    "stock_info",
    "trading_calendar",
    "daily_market_snapshot",
    "adj_factor_changes",
    "suspend_d",
)

STATE_INSERT_COLUMNS = (
    ASSET_ID,
    TRADE_DATE,
    TREND_STATE,
    UNDERLYING_TREND_STATE,
    PREVIOUS_TREND_STATE,
    STATE_CHANGED,
    IS_TRADING_DAY,
    LISTING_TRADING_DAY_NUMBER,
    CLOSE,
    MA5,
    IS_ABOVE_OR_EQUAL_MA5,
    CONSECUTIVE_ABOVE_MA5_DAYS,
    CONSECUTIVE_BELOW_MA5_DAYS,
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
        ), candidates AS (
            SELECT
                observation.*,
                row_number() OVER (
                    PARTITION BY observation.{ASSET_ID},
                                 observation.{RULE_VERSION_SET_ID},
                                 observation.{PARAMETER_SET_ID}
                    ORDER BY observation.{TRADE_DATE} DESC,
                             run.{COMPLETED_AT} DESC NULLS LAST,
                             observation.{CREATED_AT} DESC,
                             observation.{INPUT_SNAPSHOT_ID} DESC
                ) AS _latest_rank
            FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} AS observation
            JOIN {SYSTEM_B_PRODUCTION_RUN_TABLE} AS run
              ON run.{PRODUCTION_RUN_ID} = observation.{PRODUCTION_RUN_ID}
             AND run.status = 'SUCCEEDED'
            JOIN latest_date
              ON latest_date.{ASSET_ID} = observation.{ASSET_ID}
             AND latest_date.{RULE_VERSION_SET_ID} = observation.{RULE_VERSION_SET_ID}
             AND latest_date.{PARAMETER_SET_ID} = observation.{PARAMETER_SET_ID}
             AND latest_date.{TRADE_DATE} = observation.{TRADE_DATE}
        )
        SELECT * EXCLUDE (_latest_rank)
        FROM candidates
        WHERE _latest_rank = 1
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
    asset_filter_join: str,
) -> tuple[str, list[Any]]:
    return (
        f"""
        WITH selected_stock AS (
            SELECT stock.ticker, stock.list_date, stock.delist_date
            FROM stock_info AS stock
            {asset_filter_join}
            WHERE stock.list_date IS NOT NULL
              AND stock.list_date <= ?
        ),
        first_actual_date AS (
            SELECT daily.ticker AS asset_id, min(daily.trade_date) AS first_trade_date
            FROM daily_market_snapshot AS daily
            JOIN selected_stock AS stock ON stock.ticker = daily.ticker
            WHERE daily.trade_date >= stock.list_date
              AND daily.close IS NOT NULL
              AND coalesce(daily.volume, 0) > 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM suspend_d AS suspension
                  WHERE suspension.ticker = daily.ticker
                    AND suspension.trade_date = daily.trade_date
                    AND upper(coalesce(suspension.suspend_type, '')) NOT LIKE '%复牌%'
              )
            GROUP BY daily.ticker
        ),
        market_calendar AS (
            SELECT trade_date
            FROM trading_calendar
            WHERE is_open = TRUE
              AND trade_date <= ?
        ),
        domain AS (
            SELECT
                stock.ticker AS asset_id,
                calendar.trade_date
            FROM selected_stock AS stock
            JOIN first_actual_date AS first_actual ON first_actual.asset_id = stock.ticker
            JOIN market_calendar AS calendar
              ON calendar.trade_date >= first_actual.first_trade_date
             AND (stock.delist_date IS NULL OR calendar.trade_date <= stock.delist_date)
            WHERE TRUE
        ),
        raw_actual AS (
            SELECT
                daily.ticker AS asset_id,
                daily.trade_date,
                daily.close AS raw_close,
                coalesce(factor.adj_factor, 1.0) AS adj_factor,
                row_number() OVER (
                    PARTITION BY daily.ticker ORDER BY daily.trade_date
                )::INTEGER AS listing_trading_day_number
            FROM daily_market_snapshot AS daily
            JOIN selected_stock AS stock ON stock.ticker = daily.ticker
            ASOF LEFT JOIN adj_factor_changes AS factor
              ON daily.ticker = factor.ticker
             AND daily.trade_date >= factor.trade_date
            WHERE daily.trade_date <= ?
              AND daily.trade_date >= stock.list_date
              AND (stock.delist_date IS NULL OR daily.trade_date <= stock.delist_date)
              AND daily.close IS NOT NULL
              AND coalesce(daily.volume, 0) > 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM suspend_d AS suspension
                  WHERE suspension.ticker = daily.ticker
                    AND suspension.trade_date = daily.trade_date
                    AND upper(coalesce(suspension.suspend_type, '')) NOT LIKE '%复牌%'
              )
        ),
        latest_factor AS (
            SELECT asset_id, coalesce(arg_max(adj_factor, trade_date), 1.0) AS latest_adj_factor
            FROM raw_actual
            GROUP BY asset_id
        ),
        adjusted_actual AS (
            SELECT
                actual.asset_id,
                actual.trade_date,
                actual.listing_trading_day_number,
                actual.raw_close * actual.adj_factor / latest.latest_adj_factor AS close
            FROM raw_actual AS actual
            JOIN latest_factor AS latest USING (asset_id)
        ),
        indicators AS (
            SELECT
                asset_id,
                trade_date,
                listing_trading_day_number,
                close,
                CASE
                    WHEN listing_trading_day_number >= 5 THEN
                        avg(close) OVER (
                            PARTITION BY asset_id ORDER BY trade_date
                            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                        )
                    ELSE NULL
                END AS ma5
            FROM adjusted_actual
        ),
        observation AS (
            SELECT
                domain.asset_id,
                domain.trade_date,
                coalesce(indicators.trade_date = domain.trade_date, FALSE) AS is_trading_day,
                coalesce(indicators.listing_trading_day_number, 0)::INTEGER
                    AS listing_trading_day_number,
                CASE WHEN indicators.trade_date = domain.trade_date THEN indicators.close END
                    AS close,
                CASE WHEN indicators.trade_date = domain.trade_date THEN indicators.ma5 END
                    AS ma5
            FROM domain
            ASOF LEFT JOIN indicators
              ON domain.asset_id = indicators.asset_id
             AND domain.trade_date >= indicators.trade_date
        )
        SELECT asset_id, trade_date, is_trading_day,
               listing_trading_day_number, close, ma5
        FROM observation
        ORDER BY asset_id, trade_date
        """,
        [end_date, end_date, end_date],
    )


def daily_standard_input_sql(
    *,
    target_date: date,
    asset_filter_join: str,
) -> tuple[str, list[Any]]:
    return (
        f"""
        WITH selected_stock AS (
            SELECT stock.ticker, stock.list_date, stock.delist_date
            FROM stock_info AS stock
            {asset_filter_join}
            WHERE stock.list_date IS NOT NULL
              AND stock.list_date <= ?
              AND (stock.delist_date IS NULL OR stock.delist_date >= ?)
        ),
        latest_factor AS (
            SELECT ticker, coalesce(arg_max(adj_factor, trade_date), 1.0) AS latest_adj_factor
            FROM adj_factor_changes
            WHERE trade_date <= ?
            GROUP BY ticker
        ),
        adjusted_actual AS (
            SELECT
                daily.ticker AS asset_id,
                daily.trade_date,
                daily.close * coalesce(factor.adj_factor, 1.0)
                    / coalesce(latest.latest_adj_factor, 1.0) AS close
            FROM daily_market_snapshot AS daily
            JOIN selected_stock AS stock ON stock.ticker = daily.ticker
            LEFT JOIN latest_factor AS latest ON latest.ticker = daily.ticker
            ASOF LEFT JOIN adj_factor_changes AS factor
              ON daily.ticker = factor.ticker
             AND daily.trade_date >= factor.trade_date
            WHERE daily.trade_date <= ?
              AND daily.trade_date >= stock.list_date
              AND daily.close IS NOT NULL
              AND coalesce(daily.volume, 0) > 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM suspend_d AS suspension
                  WHERE suspension.ticker = daily.ticker
                    AND suspension.trade_date = daily.trade_date
                    AND upper(coalesce(suspension.suspend_type, '')) NOT LIKE '%复牌%'
              )
        ),
        latest_observation AS (
            SELECT
                asset_id,
                count(*)::INTEGER AS listing_trading_day_number,
                max(trade_date) AS latest_trade_date,
                arg_max(close, trade_date) AS latest_close,
                list_avg(
                    list_transform(
                        list_sort(
                            max_by(
                                struct_pack(trade_date := trade_date, value := close),
                                trade_date,
                                5
                            )
                        ),
                        item -> item.value
                    )
                ) AS latest_ma5
            FROM adjusted_actual
            GROUP BY asset_id
        )
        SELECT
            asset_id,
            ?::DATE AS trade_date,
            latest_trade_date = ? AS is_trading_day,
            listing_trading_day_number,
            CASE WHEN latest_trade_date = ? THEN latest_close END AS close,
            CASE
                WHEN latest_trade_date = ? AND listing_trading_day_number >= 5
                THEN latest_ma5
            END AS ma5
        FROM latest_observation
        ORDER BY asset_id
        """,
        [
            target_date,
            target_date,
            target_date,
            target_date,
            target_date,
            target_date,
            target_date,
            target_date,
        ],
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
    if target_date is not None:
        sql, params = daily_standard_input_sql(
            target_date=target_date,
            asset_filter_join=asset_join,
        )
    else:
        sql, params = standard_input_sql(
            end_date=end_date,
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


def load_checkpoints(
    connection: duckdb.DuckDBPyConnection,
    *,
    before_date: date,
    rule_version_set_id: str,
    parameter_set_id: str,
    asset_ids: Sequence[str],
) -> tuple[SystemBStateCheckpoint, ...]:
    if not asset_ids:
        return ()
    connection.register(
        "system_b_checkpoint_assets",
        pd.DataFrame({ASSET_ID: sorted(set(asset_ids))}),
    )
    latest_rows = connection.execute(
        f"""
        SELECT observation.*
        FROM {SYSTEM_B_LATEST_STATE_VIEW} AS observation
        JOIN system_b_checkpoint_assets AS selected USING (asset_id)
        WHERE observation.rule_version_set_id = ?
          AND observation.parameter_set_id = ?
        ORDER BY observation.asset_id
        """,
        [rule_version_set_id, parameter_set_id],
    ).fetchdf()
    if latest_rows.empty:
        rows = latest_rows
        fallback_assets: list[str] = []
    else:
        latest_dates = pd.to_datetime(latest_rows[TRADE_DATE]).dt.date
        rows = latest_rows.loc[latest_dates < before_date].copy()
        fallback_assets = latest_rows.loc[
            latest_dates >= before_date, ASSET_ID
        ].astype(str).tolist()

    if fallback_assets:
        connection.register(
            "system_b_checkpoint_fallback_assets",
            pd.DataFrame({ASSET_ID: sorted(set(fallback_assets))}),
        )
        fallback = connection.execute(
            f"""
            WITH previous_date AS (
                SELECT max(observation.trade_date) AS trade_date
                FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} AS observation
                JOIN {SYSTEM_B_PRODUCTION_RUN_TABLE} AS run
                  ON run.{PRODUCTION_RUN_ID} = observation.{PRODUCTION_RUN_ID}
                 AND run.status = 'SUCCEEDED'
                WHERE observation.trade_date < ?
                  AND observation.rule_version_set_id = ?
                  AND observation.parameter_set_id = ?
            ), candidates AS (
                SELECT observation.*, run.completed_at
                FROM {SYSTEM_B_STATE_OBSERVATION_TABLE} AS observation
                JOIN {SYSTEM_B_PRODUCTION_RUN_TABLE} AS run
                  ON run.{PRODUCTION_RUN_ID} = observation.{PRODUCTION_RUN_ID}
                 AND run.status = 'SUCCEEDED'
                JOIN system_b_checkpoint_fallback_assets AS selected
                  ON selected.asset_id = observation.asset_id
                JOIN previous_date ON previous_date.trade_date = observation.trade_date
                WHERE observation.rule_version_set_id = ?
                  AND observation.parameter_set_id = ?
            )
            SELECT candidates.* EXCLUDE (completed_at)
            FROM candidates
            QUALIFY row_number() OVER (
                PARTITION BY candidates.asset_id
                ORDER BY candidates.completed_at DESC NULLS LAST,
                         candidates.created_at DESC,
                         candidates.input_snapshot_id DESC
            ) = 1
            ORDER BY candidates.asset_id
            """,
            [
                before_date,
                rule_version_set_id,
                parameter_set_id,
                rule_version_set_id,
                parameter_set_id,
            ],
        ).fetchdf()
        rows = pd.concat([rows, fallback], ignore_index=True)
    rows = rows.sort_values(ASSET_ID, kind="mergesort").reset_index(drop=True)
    return tuple(
        SystemBStateCheckpoint(
            asset_id=str(row[ASSET_ID]),
            last_observation_date=pd.Timestamp(row[TRADE_DATE]),
            trend_state=SystemBTrendState(str(row[TREND_STATE])),
            underlying_trend_state=SystemBTrendState(str(row[UNDERLYING_TREND_STATE])),
            listing_trading_day_number=int(row[LISTING_TRADING_DAY_NUMBER]),
            consecutive_above_ma5_days=int(row[CONSECUTIVE_ABOVE_MA5_DAYS]),
            consecutive_below_ma5_days=int(row[CONSECUTIVE_BELOW_MA5_DAYS]),
        )
        for row in rows.to_dict(orient="records")
    )


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


def find_succeeded_run(
    connection: duckdb.DuckDBPyConnection,
    *,
    run_type: str,
    target_start_date: date | None,
    target_end_date: date | None,
    rule_version_set_id: str,
    parameter_set_id: str,
    input_snapshot_id: str,
) -> dict[str, Any] | None:
    cursor = connection.execute(
        f"""
        SELECT * FROM {SYSTEM_B_PRODUCTION_RUN_TABLE}
        WHERE status = 'SUCCEEDED'
          AND run_type = ?
          AND target_start_date IS NOT DISTINCT FROM ?
          AND target_end_date IS NOT DISTINCT FROM ?
          AND rule_version_set_id = ?
          AND parameter_set_id = ?
          AND input_snapshot_id = ?
        ORDER BY completed_at DESC
        LIMIT 1
        """,
        [
            run_type,
            target_start_date,
            target_end_date,
            rule_version_set_id,
            parameter_set_id,
            input_snapshot_id,
        ],
    )
    row = cursor.fetchone()
    return dict(zip([item[0] for item in cursor.description], row, strict=True)) if row else None


def import_staging(
    connection: duckdb.DuckDBPyConnection,
    *,
    parquet_glob: str,
    production_run_id: str,
    input_snapshot_id: str,
    calculation_version: str,
    metrics: dict[str, Any],
) -> None:
    state_columns = ", ".join(STATE_INSERT_COLUMNS)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    connection.execute("BEGIN")
    try:
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
