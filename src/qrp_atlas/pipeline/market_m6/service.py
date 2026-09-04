"""Production Service for M6 Market Sentiment complete facts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import date
from typing import Any

import duckdb
import pandas as pd

from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    CLOSE,
    CONSECUTIVE_LIMIT_UP_COUNT,
    CREATED_AT,
    DAILY_MARKET_SNAPSHOT,
    INPUT_SNAPSHOT_ID,
    IS_LIMIT_DOWN,
    IS_LIMIT_UP,
    LIMIT_DOWN_COUNT,
    LIMIT_UP_COUNT,
    M6_CALCULATION_VERSION,
    MARKET_M6_OBSERVATION,
    MARKET_M6_OBSERVATION_TABLE,
    MARKET_SCOPE,
    MARKET_SCOPE_ALL_MARKET,
    MARKET_SCOPE_BSE,
    MARKET_SCOPE_CHINEXT,
    MARKET_SCOPE_MAIN_BOARD,
    MARKET_SCOPE_STAR_MARKET,
    MARKET_SCOPES,
    MAX_CONSECUTIVE_LIMIT_UP_HEIGHT,
    PRE_LIMIT_UP_PREMIUM,
    PRODUCTION_RUN_ID,
    STOCK_INFO,
    SUSPEND_D,
    TICKER,
    TRADE_DATE,
    TRADING_CALENDAR,
)
from qrp_atlas.indicators.m6 import calculate_market_m6_observations
from qrp_atlas.orchestration.execution_control import ExecutionControl
from qrp_atlas.pipeline.contracts import ContractError


def resolve_canonical_market_scope(market: str | None, exchange: str | None) -> str | None:
    """Map canonical stock_info market and exchange to one of 4 submarket scopes.

    Strict rules:
    - MAIN_BOARD: market in ('主板', '中小板')
    - CHINEXT: market == '创业板'
    - STAR_MARKET: market == '科创板'
    - BSE: market == '北交所' or exchange == 'BSE'
    - Never infer from ticker prefix.
    """
    m_clean = str(market).strip() if market is not None else ""
    e_clean = str(exchange).strip().upper() if exchange is not None else ""

    if m_clean in ("主板", "中小板"):
        return MARKET_SCOPE_MAIN_BOARD
    if m_clean == "创业板":
        return MARKET_SCOPE_CHINEXT
    if m_clean == "科创板":
        return MARKET_SCOPE_STAR_MARKET
    if m_clean == "北交所" or e_clean == "BSE":
        return MARKET_SCOPE_BSE
    return None


class MarketM6PipelineService:
    """Service to produce, validate, and atomically persist M6 Market Sentiment Observations."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def run_m6_daily(
        self,
        trade_date: date,
        *,
        production_run_id: str | None = None,
        execution_control: ExecutionControl | None = None,
    ) -> pd.DataFrame:
        if execution_control is not None:
            execution_control.check()

        # 1. Verify target date is open in trading_calendar
        cal_row = self.con.execute(
            "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
            [trade_date],
        ).fetchone()
        if cal_row is None:
            raise ContractError("M6_CALENDAR_MISSING", f"trade_date {trade_date} not in trading_calendar")
        if not cal_row[0]:
            raise ContractError("M6_NON_TRADING_DAY", f"trade_date {trade_date} is not an open trading day")

        # 2. Query previous trading day D-1
        prev_row = self.con.execute(
            "SELECT max(trade_date) FROM trading_calendar WHERE is_open = TRUE AND trade_date < ?",
            [trade_date],
        ).fetchone()
        prev_date: date | None = prev_row[0] if prev_row and prev_row[0] is not None else None

        # 3. Read stock_info mapping
        stock_info_rows = self.con.execute(
            "SELECT ticker, market, exchange FROM stock_info"
        ).fetchall()
        if not stock_info_rows:
            raise ContractError("M6_STOCK_INFO_EMPTY", "stock_info table contains no records")

        ticker_to_scope: dict[str, str] = {}
        for t, m, ex in stock_info_rows:
            scope = resolve_canonical_market_scope(m, ex)
            if scope:
                ticker_to_scope[str(t).strip()] = scope

        # 4. Read today's daily_market_snapshot
        today_snapshot_rows = self.con.execute(
            """
            SELECT ticker, close, is_limit_up, is_limit_down, volume
            FROM daily_market_snapshot
            WHERE trade_date = ?
            """,
            [trade_date],
        ).fetchall()
        if not today_snapshot_rows:
            raise ContractError("M6_SNAPSHOT_EMPTY", f"daily_market_snapshot has no data for {trade_date}")

        # 5. Read explicit suspensions on D
        today_suspensions = {
            str(r[0]).strip()
            for r in self.con.execute(
                """
                SELECT DISTINCT ticker
                FROM suspend_d
                WHERE trade_date = ?
                  AND upper(coalesce(suspend_type, '')) NOT LIKE '%复牌%'
                """,
                [trade_date],
            ).fetchall()
        }

        # Build today_market DataFrame
        market_rows: list[dict[str, Any]] = []
        unresolved_tickers: list[str] = []

        for t_raw, close_val, is_up, is_down, vol in today_snapshot_rows:
            ticker_str = str(t_raw).strip()
            scope = ticker_to_scope.get(ticker_str)
            if scope is None:
                unresolved_tickers.append(ticker_str)
                continue

            is_suspended = (ticker_str in today_suspensions) or (vol is not None and vol == 0)
            is_trading = (not is_suspended) and (close_val is not None) and (vol is not None and vol > 0)

            market_rows.append(
                {
                    TICKER: ticker_str,
                    MARKET_SCOPE: scope,
                    IS_LIMIT_UP: bool(is_up) if (is_up is not None and is_trading) else False,
                    IS_LIMIT_DOWN: bool(is_down) if (is_down is not None and is_trading) else False,
                    CLOSE: float(close_val) if close_val is not None else None,
                    "is_trading": is_trading,
                }
            )

        if unresolved_tickers:
            raise ContractError(
                "M6_CANONICAL_MARKET_UNRESOLVED",
                f"{len(unresolved_tickers)} tickers cannot be mapped to canonical market scope: {unresolved_tickers[:5]}",
            )

        today_market = pd.DataFrame(market_rows)

        if execution_control is not None:
            execution_control.check()

        # 6. Resolve candidate limit-up stocks and compute natural streaks
        candidate_limit_up_tickers = today_market[
            today_market[IS_LIMIT_UP] & today_market["is_trading"]
        ][TICKER].tolist()

        consecutive_streaks: dict[str, int] = {}
        if candidate_limit_up_tickers:
            # Query actual trading days backwards up to D for candidate tickers
            hist_rows = self.con.execute(
                """
                WITH candidate_stocks AS (
                    SELECT UNNEST(?::VARCHAR[]) AS ticker
                ),
                explicit_suspension AS (
                    SELECT DISTINCT ticker, trade_date
                    FROM suspend_d
                    WHERE upper(coalesce(suspend_type, '')) NOT LIKE '%复牌%'
                      AND trade_date <= ?
                      AND ticker IN (SELECT ticker FROM candidate_stocks)
                ),
                stock_days AS (
                    SELECT
                        daily.ticker,
                        daily.trade_date,
                        daily.is_limit_up,
                        CASE
                            WHEN suspension.ticker IS NOT NULL THEN FALSE
                            WHEN daily.volume = 0 OR daily.close IS NULL THEN FALSE
                            WHEN daily.volume > 0 AND daily.close IS NOT NULL THEN TRUE
                            ELSE FALSE
                        END AS is_trading
                    FROM daily_market_snapshot daily
                    LEFT JOIN explicit_suspension suspension
                      ON suspension.ticker = daily.ticker AND suspension.trade_date = daily.trade_date
                    WHERE daily.ticker IN (SELECT ticker FROM candidate_stocks)
                      AND daily.trade_date <= ?
                )
                SELECT ticker, trade_date, is_limit_up
                FROM stock_days
                WHERE is_trading = TRUE
                ORDER BY ticker, trade_date ASC
                """,
                [candidate_limit_up_tickers, trade_date, trade_date],
            ).fetchall()

            from collections import defaultdict

            stock_history = defaultdict(list)
            for t_raw, d_val, up_val in hist_rows:
                stock_history[str(t_raw).strip()].append((d_val, bool(up_val)))

            for t_cand in candidate_limit_up_tickers:
                seq = stock_history.get(t_cand, [])
                streak = 0
                for _, is_up in reversed(seq):
                    if is_up:
                        streak += 1
                    else:
                        break
                consecutive_streaks[t_cand] = streak

        # 7. Read D-1 limit up facts for pre_limit_up_premium
        yesterday_limit_up_tickers: set[str] = set()
        yesterday_closes: dict[str, float] = {}

        if prev_date is not None:
            prev_rows = self.con.execute(
                """
                SELECT ticker, close, is_limit_up
                FROM daily_market_snapshot
                WHERE trade_date = ?
                """,
                [prev_date],
            ).fetchall()
            if not prev_rows:
                raise ContractError(
                    "M6_PREVIOUS_DAY_SNAPSHOT_MISSING",
                    f"daily_market_snapshot has no data for previous trading day {prev_date}",
                )

            for t_raw, c_val, is_up in prev_rows:
                t_str = str(t_raw).strip()
                if is_up and c_val is not None and c_val > 0:
                    yesterday_limit_up_tickers.add(t_str)
                    yesterday_closes[t_str] = float(c_val)

        # 8. Compute input_snapshot_id
        snapshot_payload = {
            "trade_date": trade_date.isoformat(),
            "prev_date": prev_date.isoformat() if prev_date else None,
            "today_rows": len(today_snapshot_rows),
            "stock_info_count": len(stock_info_rows),
            "candidate_limit_up_count": len(candidate_limit_up_tickers),
            "yesterday_limit_up_count": len(yesterday_limit_up_tickers),
        }
        input_snapshot_id = hashlib.sha256(
            json.dumps(snapshot_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

        # 9. Pure calculation
        observations_df = calculate_market_m6_observations(
            trade_date=trade_date,
            today_market=today_market,
            consecutive_streaks=consecutive_streaks,
            yesterday_limit_up_tickers=yesterday_limit_up_tickers,
            yesterday_closes=yesterday_closes,
            calculation_version=M6_CALCULATION_VERSION,
            production_run_id=production_run_id,
            input_snapshot_id=input_snapshot_id,
        )

        if execution_control is not None:
            execution_control.check()

        # 10. Atomic database write inside transaction
        self._atomic_persist(trade_date, observations_df)

        return observations_df

    def _atomic_persist(self, trade_date: date, observations_df: pd.DataFrame) -> None:
        """Atomically delete existing trade_date rows and insert exactly 5 observation rows."""
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute(
                f"DELETE FROM {MARKET_M6_OBSERVATION_TABLE} WHERE trade_date = ?",
                [trade_date],
            )
            for _, row in observations_df.iterrows():
                self.con.execute(
                    f"""
                    INSERT INTO {MARKET_M6_OBSERVATION_TABLE} (
                        trade_date,
                        market_scope,
                        limit_up_count,
                        limit_down_count,
                        consecutive_limit_up_count,
                        max_consecutive_limit_up_height,
                        pre_limit_up_premium,
                        calculation_version,
                        production_run_id,
                        input_snapshot_id,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        row[TRADE_DATE],
                        row[MARKET_SCOPE],
                        int(row[LIMIT_UP_COUNT]),
                        int(row[LIMIT_DOWN_COUNT]),
                        int(row[CONSECUTIVE_LIMIT_UP_COUNT]),
                        int(row[MAX_CONSECUTIVE_LIMIT_UP_HEIGHT]),
                        row[PRE_LIMIT_UP_PREMIUM] if row[PRE_LIMIT_UP_PREMIUM] is not None and not pd.isna(row[PRE_LIMIT_UP_PREMIUM]) else None,
                        row[CALCULATION_VERSION],
                        row[PRODUCTION_RUN_ID],
                        row[INPUT_SNAPSHOT_ID],
                        row[CREATED_AT],
                    ],
                )
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
