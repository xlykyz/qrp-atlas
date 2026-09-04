"""Internal Query and Lineage Audit Service for M6 Market Sentiment Observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    CONSECUTIVE_LIMIT_UP_COUNT,
    CREATED_AT,
    INPUT_SNAPSHOT_ID,
    LIMIT_DOWN_COUNT,
    LIMIT_UP_COUNT,
    MARKET_M6_OBSERVATION_TABLE,
    MARKET_SCOPE,
    MARKET_SCOPES,
    MAX_CONSECUTIVE_LIMIT_UP_HEIGHT,
    PRE_LIMIT_UP_PREMIUM,
    PRODUCTION_RUN_ID,
    TRADE_DATE,
)
from qrp_atlas.pipeline.contracts import ContractError
from qrp_atlas.pipeline.market_m6.service import MarketM6PipelineService


@dataclass(frozen=True)
class M6ObservationAuditReport:
    trade_date: date
    persisted_scopes: tuple[str, ...]
    is_reproducible: bool
    discrepancies: tuple[dict[str, Any], ...]
    discrepancy_reason: str | None = None


class MarketM6QueryService:
    """Query and lineage audit service for market_m6_observation."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def get_m6_observations(
        self,
        start_date: date,
        end_date: date,
        market_scope: str | None = None,
    ) -> pd.DataFrame:
        """Fetch persisted M6 observations over an inclusive date range."""
        params: list[Any] = [start_date, end_date]
        scope_filter = ""
        if market_scope:
            scope_filter = "AND market_scope = ?"
            params.append(market_scope)

        query = f"""
            SELECT
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
            FROM {MARKET_M6_OBSERVATION_TABLE}
            WHERE trade_date BETWEEN ? AND ?
              {scope_filter}
            ORDER BY trade_date ASC, market_scope ASC
        """
        df = self.con.execute(query, params).fetchdf()
        if not df.empty and TRADE_DATE in df.columns:
            df[TRADE_DATE] = pd.to_datetime(df[TRADE_DATE]).dt.date
        return df

    def audit_m6_observation(
        self,
        trade_date: date,
        market_scope: str | None = None,
    ) -> M6ObservationAuditReport:
        """Audit persisted M6 observations against pure on-the-fly recomputation from base facts."""
        persisted = self.get_m6_observations(trade_date, trade_date, market_scope)
        if persisted.empty:
            return M6ObservationAuditReport(
                trade_date=trade_date,
                persisted_scopes=(),
                is_reproducible=False,
                discrepancies=(),
                discrepancy_reason=f"No persisted M6 observations found for {trade_date}",
            )

        persisted_scopes = tuple(persisted[MARKET_SCOPE].tolist())

        # Re-run pipeline calculation in a read-only memory snapshot without committing
        service = MarketM6PipelineService(self.con)
        # We simulate the recalculation directly
        try:
            # We fetch base facts and recalculate using the service's logic
            cal_row = self.con.execute(
                "SELECT is_open FROM trading_calendar WHERE trade_date = ?",
                [trade_date],
            ).fetchone()
            if cal_row is None or not cal_row[0]:
                return M6ObservationAuditReport(
                    trade_date=trade_date,
                    persisted_scopes=persisted_scopes,
                    is_reproducible=False,
                    discrepancies=(),
                    discrepancy_reason=f"trade_date {trade_date} is not an open trading day",
                )

            # Re-derive expected without persisting
            prev_row = self.con.execute(
                "SELECT max(trade_date) FROM trading_calendar WHERE is_open = TRUE AND trade_date < ?",
                [trade_date],
            ).fetchone()
            prev_date: date | None = prev_row[0] if prev_row and prev_row[0] is not None else None

            from qrp_atlas.pipeline.market_m6.service import resolve_canonical_market_scope
            stock_info_rows = self.con.execute("SELECT ticker, market, exchange FROM stock_info").fetchall()
            ticker_to_scope = {
                str(t).strip(): resolve_canonical_market_scope(m, ex)
                for t, m, ex in stock_info_rows
                if resolve_canonical_market_scope(m, ex)
            }

            today_snapshot_rows = self.con.execute(
                "SELECT ticker, close, is_limit_up, is_limit_down, volume FROM daily_market_snapshot WHERE trade_date = ?",
                [trade_date],
            ).fetchall()

            today_suspensions = {
                str(r[0]).strip()
                for r in self.con.execute(
                    "SELECT DISTINCT ticker FROM suspend_d WHERE trade_date = ? AND upper(coalesce(suspend_type, '')) NOT LIKE '%复牌%'",
                    [trade_date],
                ).fetchall()
            }

            market_rows = []
            for t_raw, close_val, is_up, is_down, vol in today_snapshot_rows:
                t_str = str(t_raw).strip()
                scope = ticker_to_scope.get(t_str)
                if not scope:
                    continue
                is_suspended = (t_str in today_suspensions) or (vol is not None and vol == 0)
                is_trading = (not is_suspended) and (close_val is not None) and (vol is not None and vol > 0)
                market_rows.append(
                    {
                        "ticker": t_str,
                        "market_scope": scope,
                        "is_limit_up": bool(is_up) if (is_up is not None and is_trading) else False,
                        "is_limit_down": bool(is_down) if (is_down is not None and is_trading) else False,
                        "close": float(close_val) if close_val is not None else None,
                        "is_trading": is_trading,
                    }
                )
            today_market = pd.DataFrame(market_rows)

            candidate_limit_up_tickers = today_market[today_market["is_limit_up"] & today_market["is_trading"]]["ticker"].tolist()
            consecutive_streaks = {}
            if candidate_limit_up_tickers:
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

            yesterday_limit_up_tickers = set()
            yesterday_closes = {}
            if prev_date is not None:
                prev_rows = self.con.execute(
                    "SELECT ticker, close, is_limit_up FROM daily_market_snapshot WHERE trade_date = ?",
                    [prev_date],
                ).fetchall()
                for t_raw, c_val, is_up in prev_rows:
                    t_str = str(t_raw).strip()
                    if is_up and c_val is not None and c_val > 0:
                        yesterday_limit_up_tickers.add(t_str)
                        yesterday_closes[t_str] = float(c_val)

            from qrp_atlas.indicators.m6 import calculate_market_m6_observations
            expected = calculate_market_m6_observations(
                trade_date=trade_date,
                today_market=today_market,
                consecutive_streaks=consecutive_streaks,
                yesterday_limit_up_tickers=yesterday_limit_up_tickers,
                yesterday_closes=yesterday_closes,
            )
        except Exception as exc:
            return M6ObservationAuditReport(
                trade_date=trade_date,
                persisted_scopes=persisted_scopes,
                is_reproducible=False,
                discrepancies=(),
                discrepancy_reason=f"Failed to recalculate M6 observations: {exc}",
            )

        if market_scope:
            expected = expected[expected[MARKET_SCOPE] == market_scope]

        exp_dict = expected.set_index(MARKET_SCOPE).to_dict(orient="index")
        pers_dict = persisted.set_index(MARKET_SCOPE).to_dict(orient="index")

        discrepancies: list[dict[str, Any]] = []

        metrics_to_check = (
            LIMIT_UP_COUNT,
            LIMIT_DOWN_COUNT,
            CONSECUTIVE_LIMIT_UP_COUNT,
            MAX_CONSECUTIVE_LIMIT_UP_HEIGHT,
            PRE_LIMIT_UP_PREMIUM,
        )

        for sc, exp_row in exp_dict.items():
            if sc not in pers_dict:
                discrepancies.append({"market_scope": sc, "error": "MISSING_IN_PERSISTED"})
                continue
            p_row = pers_dict[sc]
            for m in metrics_to_check:
                exp_v = exp_row.get(m)
                pers_v = p_row.get(m)
                if m == PRE_LIMIT_UP_PREMIUM:
                    # Float / None comparison
                    if exp_v is None or pd.isna(exp_v):
                        if pers_v is not None and not pd.isna(pers_v):
                            discrepancies.append({"market_scope": sc, "metric": m, "expected": None, "persisted": pers_v})
                    else:
                        if pers_v is None or pd.isna(pers_v):
                            discrepancies.append({"market_scope": sc, "metric": m, "expected": exp_v, "persisted": None})
                        elif abs(float(exp_v) - float(pers_v)) > 1e-6:
                            discrepancies.append({"market_scope": sc, "metric": m, "expected": exp_v, "persisted": pers_v})
                else:
                    if int(exp_v) != int(pers_v):
                        discrepancies.append({"market_scope": sc, "metric": m, "expected": exp_v, "persisted": pers_v})

        is_reproducible = len(discrepancies) == 0
        return M6ObservationAuditReport(
            trade_date=trade_date,
            persisted_scopes=persisted_scopes,
            is_reproducible=is_reproducible,
            discrepancies=tuple(discrepancies),
            discrepancy_reason="Discrepancies found between recomputed facts and persisted observations" if not is_reproducible else None,
        )
