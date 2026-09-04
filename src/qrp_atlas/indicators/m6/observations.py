"""Pure calculation of Market Sentiment M6 Observations."""

from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    CALCULATION_VERSION,
    CLOSE,
    CONSECUTIVE_LIMIT_UP_COUNT,
    CREATED_AT,
    INPUT_SNAPSHOT_ID,
    IS_LIMIT_DOWN,
    IS_LIMIT_UP,
    LIMIT_DOWN_COUNT,
    LIMIT_UP_COUNT,
    M6_CALCULATION_VERSION,
    MARKET_SCOPE,
    MARKET_SCOPE_ALL_MARKET,
    MARKET_SCOPES,
    MAX_CONSECUTIVE_LIMIT_UP_HEIGHT,
    PRE_LIMIT_UP_PREMIUM,
    PRODUCTION_RUN_ID,
    TICKER,
    TRADE_DATE,
)


class M6ObservationError(ValueError):
    """Raised when M6 calculation inputs are invalid or incomplete."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


def calculate_market_m6_observations(
    trade_date: date,
    today_market: pd.DataFrame,
    consecutive_streaks: dict[str, int],
    yesterday_limit_up_tickers: set[str],
    yesterday_closes: dict[str, float],
    *,
    calculation_version: str = M6_CALCULATION_VERSION,
    production_run_id: str | None = None,
    input_snapshot_id: str | None = None,
    created_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Calculate point-in-time M6 Market Sentiment Observations for 5 market scopes.

    Contract Rules:
    1. Outputs exactly 5 rows corresponding to MARKET_SCOPES for trade_date.
    2. limit_up_count counts stocks whose final close is limit up (is_limit_up == True).
    3. limit_down_count counts stocks whose final close is limit down (is_limit_down == True).
    4. consecutive_limit_up_count counts stocks with natural consecutive limit up height >= 2.
    5. max_consecutive_limit_up_height is the maximum height of consecutive limit up stocks;
       if no consecutive limit up stock exists (no stock with height >= 2), output 0.
    6. pre_limit_up_premium is the equal-weighted average close return on D for stocks that:
       - closed at final limit up on D-1;
       - actually traded on D (suspensions dropped from denominator);
       - negative returns, low open, and limit-down are normally retained;
       - if no valid sample stocks exist in the scope, output None (NULL).
    7. ALL_MARKET pre_limit_up_premium directly averages all valid sample stocks across the market,
       without re-averaging sub-market means.
    8. Does not mutate inputs.
    """
    if not isinstance(trade_date, date):
        raise M6ObservationError("INVALID_TRADE_DATE", f"trade_date must be datetime.date, got {type(trade_date)}")

    required_cols = {TICKER, MARKET_SCOPE, IS_LIMIT_UP, IS_LIMIT_DOWN, CLOSE, "is_trading"}
    missing = required_cols - set(today_market.columns)
    if missing:
        raise M6ObservationError("MISSING_COLUMNS", f"today_market is missing required columns: {sorted(missing)}")

    timestamp = created_at if created_at is not None else pd.Timestamp.now(tz="UTC").tz_localize(None)

    # Work on an isolated copy
    df = today_market.copy()

    # Pre-extract booleans and prices for fast filtering
    tickers = df[TICKER].astype(str)
    scopes = df[MARKET_SCOPE].astype(str)
    is_up = df[IS_LIMIT_UP].fillna(False).astype(bool)
    is_down = df[IS_LIMIT_DOWN].fillna(False).astype(bool)
    is_trading = df["is_trading"].fillna(False).astype(bool)
    closes = pd.to_numeric(df[CLOSE], errors="coerce")

    # Clean ticker -> row indexing
    df_clean = pd.DataFrame(
        {
            TICKER: tickers,
            MARKET_SCOPE: scopes,
            IS_LIMIT_UP: is_up,
            IS_LIMIT_DOWN: is_down,
            "is_trading": is_trading,
            CLOSE: closes,
        }
    )

    records: list[dict[str, Any]] = []

    for scope in MARKET_SCOPES:
        if scope == MARKET_SCOPE_ALL_MARKET:
            sub = df_clean
        else:
            sub = df_clean[df_clean[MARKET_SCOPE] == scope]

        # 1. limit_up_count
        limit_up_sub = sub[sub[IS_LIMIT_UP]]
        limit_up_count = int(len(limit_up_sub))

        # 2. limit_down_count
        limit_down_count = int(sub[IS_LIMIT_DOWN].sum())

        # 3 & 4. consecutive_limit_up_count & max_consecutive_limit_up_height
        # Only stocks closing at limit up on day D have active streaks
        limit_up_tickers = limit_up_sub[TICKER].tolist()
        streaks = [int(consecutive_streaks.get(t, 0)) for t in limit_up_tickers]
        consecutive_streaks_ge_2 = [s for s in streaks if s >= 2]

        consecutive_count = int(len(consecutive_streaks_ge_2))
        max_height = int(max(consecutive_streaks_ge_2)) if consecutive_streaks_ge_2 else 0

        # 5. pre_limit_up_premium
        # Sample criteria:
        # - stock was final limit up on D-1 (in yesterday_limit_up_tickers);
        # - stock actually traded on D (is_trading == True and close is valid);
        # - yesterday_close is valid and positive.
        sample_mask = (
            sub[TICKER].isin(yesterday_limit_up_tickers)
            & sub["is_trading"]
            & sub[CLOSE].notna()
            & (sub[CLOSE] > 0)
        )
        sample_df = sub[sample_mask]

        valid_premiums: list[float] = []
        for _, row in sample_df.iterrows():
            t = row[TICKER]
            y_close = yesterday_closes.get(t)
            if y_close is not None and y_close > 0:
                d_close = float(row[CLOSE])
                premium = (d_close / float(y_close)) - 1.0
                valid_premiums.append(premium)

        if valid_premiums:
            pre_limit_up_premium: float | None = float(np.mean(valid_premiums))
        else:
            pre_limit_up_premium = None

        records.append(
            {
                TRADE_DATE: trade_date,
                MARKET_SCOPE: scope,
                LIMIT_UP_COUNT: limit_up_count,
                LIMIT_DOWN_COUNT: limit_down_count,
                CONSECUTIVE_LIMIT_UP_COUNT: consecutive_count,
                MAX_CONSECUTIVE_LIMIT_UP_HEIGHT: max_height,
                PRE_LIMIT_UP_PREMIUM: pre_limit_up_premium,
                CALCULATION_VERSION: calculation_version,
                PRODUCTION_RUN_ID: production_run_id,
                INPUT_SNAPSHOT_ID: input_snapshot_id,
                CREATED_AT: timestamp,
            }
        )

    out_df = pd.DataFrame(records)
    out_df[PRE_LIMIT_UP_PREMIUM] = out_df[PRE_LIMIT_UP_PREMIUM].astype(object)
    out_df[PRE_LIMIT_UP_PREMIUM] = out_df[PRE_LIMIT_UP_PREMIUM].where(out_df[PRE_LIMIT_UP_PREMIUM].notna(), None)
    # Ensure column order and types match TableSchema
    return out_df[
        [
            TRADE_DATE,
            MARKET_SCOPE,
            LIMIT_UP_COUNT,
            LIMIT_DOWN_COUNT,
            CONSECUTIVE_LIMIT_UP_COUNT,
            MAX_CONSECUTIVE_LIMIT_UP_HEIGHT,
            PRE_LIMIT_UP_PREMIUM,
            CALCULATION_VERSION,
            PRODUCTION_RUN_ID,
            INPUT_SNAPSHOT_ID,
            CREATED_AT,
        ]
    ]
