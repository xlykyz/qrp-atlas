"""Shared market facts query helper implementing System B confirmed actual trading day semantics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    CONFIRMED_LISTING_TRADING_DAY_COUNT,
    IS_TRADING_DAY,
    MARKET_FACT_STATUS,
    TRADE_DATE,
)
from qrp_atlas.contracts.system_b import SystemBMarketFactStatus

ACTUAL_TRADING = SystemBMarketFactStatus.ACTUAL_TRADING.value
EXPLICIT_NON_TRADING = SystemBMarketFactStatus.EXPLICIT_NON_TRADING.value
UNRESOLVED_MISSING = SystemBMarketFactStatus.UNRESOLVED_MISSING.value


def query_confirmed_listing_facts(
    con: duckdb.DuckDBPyConnection,
    end_date: date,
    start_date: date | None = None,
    asset_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Fetch confirmed listing actual trading day facts strictly following System B semantics.

    Returns DataFrame with columns:
    - asset_id
    - trade_date
    - market_fact_status (ACTUAL_TRADING, EXPLICIT_NON_TRADING, UNRESOLVED_MISSING)
    - is_trading_day (bool)
    - confirmed_listing_trading_day_count (int)
    - unresolved_count (int)
    """
    params: list[Any] = [end_date]  # stock_info list_date <= end_date

    asset_filter = ""
    if asset_ids:
        normalized = sorted({str(a).strip() for a in asset_ids if str(a).strip()})
        if normalized:
            placeholders = ", ".join(["?"] * len(normalized))
            asset_filter = f"AND stock.ticker IN ({placeholders})"
            params.extend(normalized)

    params.append(end_date)  # trading_calendar trade_date <= end_date

    date_filter = ""
    if start_date is not None:
        date_filter = "WHERE trade_date BETWEEN ? AND ?"
        params.append(start_date)
        params.append(end_date)

    sql = f"""
    WITH selected_stock AS (
        SELECT stock.ticker, stock.list_date, stock.delist_date
        FROM stock_info AS stock
        WHERE stock.list_date IS NOT NULL AND stock.list_date <= ?
          {asset_filter}
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
            row_number() OVER (PARTITION BY fact.asset_id ORDER BY fact.trade_date)::INTEGER
                AS {CONFIRMED_LISTING_TRADING_DAY_COUNT}
        FROM domain_fact AS fact
        WHERE fact.{MARKET_FACT_STATUS} = '{ACTUAL_TRADING}'
    ),
    observation_basis AS (
        SELECT
            fact.asset_id,
            fact.trade_date,
            fact.{MARKET_FACT_STATUS},
            fact.{MARKET_FACT_STATUS} = '{ACTUAL_TRADING}' AS {IS_TRADING_DAY},
            coalesce(actual.{CONFIRMED_LISTING_TRADING_DAY_COUNT}, 0)::INTEGER
                AS {CONFIRMED_LISTING_TRADING_DAY_COUNT},
            fact.unresolved_count
        FROM domain_fact AS fact
        ASOF LEFT JOIN raw_actual AS actual
          ON fact.asset_id = actual.asset_id AND fact.trade_date >= actual.trade_date
    )
    SELECT
        asset_id,
        trade_date,
        {MARKET_FACT_STATUS},
        {IS_TRADING_DAY},
        {CONFIRMED_LISTING_TRADING_DAY_COUNT},
        unresolved_count
    FROM observation_basis
    {date_filter}
    ORDER BY asset_id, trade_date
    """
    try:
        return con.execute(sql, params).df()
    except Exception:
        return pd.DataFrame(
            columns=[
                ASSET_ID,
                TRADE_DATE,
                MARKET_FACT_STATUS,
                IS_TRADING_DAY,
                CONFIRMED_LISTING_TRADING_DAY_COUNT,
                "unresolved_count",
            ]
        )
