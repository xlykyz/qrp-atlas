"""Canonical System B market-series loading.

The state repository already defines the forward-adjusted price convention used
by System B.  Pools and Task06 must consume the same convention instead of
reading raw OHLC values directly.  This module is deliberately a pipeline
loader: it validates/loads facts, while all ranking and rolling calculations
remain in :mod:`qrp_atlas.indicators.system_b.asset_ranking`.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
import math
from typing import Any

import duckdb
import numpy as np
import pandas as pd


CANONICAL_PRICE_ADJUSTMENT = "FORWARD_ADJUSTED"
ACTUAL_TRADING = "ACTUAL_TRADING"
EXPLICIT_NON_TRADING = "EXPLICIT_NON_TRADING"
UNRESOLVED_MISSING = "UNRESOLVED_MISSING"


class CanonicalMarketSeriesError(RuntimeError):
    """Raised when canonical market facts cannot be formed safely."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _table_names(connection: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }


def _columns(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='main' AND table_name=?",
            [table_name],
        ).fetchall()
    }


def _as_date_series(values: Iterable[Any], *, code: str) -> pd.Series:
    parsed = pd.to_datetime(pd.Series(list(values)), errors="coerce").dt.date
    if parsed.isna().any():
        raise CanonicalMarketSeriesError(code, "trade_date contains an invalid value")
    return parsed


def _finite_numeric(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.notna() & np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan))


def _read_suspension_keys(
    connection: duckdb.DuckDBPyConnection,
    *,
    end_date: date,
) -> set[tuple[str, date]]:
    tables = _table_names(connection)
    if "suspend_d" not in tables:
        return set()
    cols = _columns(connection, "suspend_d")
    if not {"ticker", "trade_date"} <= cols:
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_SUSPEND_SCHEMA_MISSING",
            "suspend_d requires ticker and trade_date",
        )
    suspend_type = "suspend_type" if "suspend_type" in cols else "NULL"
    frame = connection.execute(
        f"SELECT ticker, trade_date, {suspend_type} AS suspend_type "
        "FROM suspend_d WHERE trade_date <= ?",
        [end_date],
    ).fetchdf()
    if frame.empty:
        return set()
    frame["trade_date"] = _as_date_series(
        frame["trade_date"], code="CANONICAL_MARKET_SUSPEND_DATE_INVALID"
    )
    result: set[tuple[str, date]] = set()
    for row in frame.itertuples(index=False):
        text = "" if pd.isna(row.suspend_type) else str(row.suspend_type).upper()
        if "复牌" not in text:
            result.add((str(row.ticker).strip(), row.trade_date))
    return result


def _read_calendar(
    connection: duckdb.DuckDBPyConnection,
    *,
    end_date: date,
) -> dict[date, bool] | None:
    tables = _table_names(connection)
    if "trading_calendar" not in tables:
        return None
    cols = _columns(connection, "trading_calendar")
    if not {"trade_date", "is_open"} <= cols:
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_CALENDAR_SCHEMA_MISSING",
            "trading_calendar requires trade_date and is_open",
        )
    frame = connection.execute(
        "SELECT trade_date, is_open FROM trading_calendar WHERE trade_date <= ?",
        [end_date],
    ).fetchdf()
    if frame.empty:
        return {}
    frame["trade_date"] = _as_date_series(
        frame["trade_date"], code="CANONICAL_MARKET_CALENDAR_DATE_INVALID"
    )
    if frame["trade_date"].duplicated().any():
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_CALENDAR_DUPLICATE", "trading_calendar has duplicate dates"
        )
    return {
        row.trade_date: bool(row.is_open)
        for row in frame.itertuples(index=False)
    }


def _read_adjustment_changes(
    connection: duckdb.DuckDBPyConnection,
    *,
    end_date: date,
) -> pd.DataFrame:
    tables = _table_names(connection)
    if "adj_factor_changes" not in tables:
        return pd.DataFrame(columns=["ticker", "trade_date", "adj_factor"])
    cols = _columns(connection, "adj_factor_changes")
    required = {"ticker", "trade_date", "adj_factor"}
    if not required <= cols:
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_ADJ_FACTOR_SCHEMA_MISSING",
            "adj_factor_changes requires ticker, trade_date and adj_factor",
        )
    frame = connection.execute(
        "SELECT ticker, trade_date, adj_factor FROM adj_factor_changes "
        "WHERE trade_date <= ? ORDER BY ticker, trade_date",
        [end_date],
    ).fetchdf()
    if frame.empty:
        return frame
    frame["ticker"] = frame["ticker"].astype(str).str.strip()
    frame["trade_date"] = _as_date_series(
        frame["trade_date"], code="CANONICAL_MARKET_ADJ_FACTOR_DATE_INVALID"
    )
    if frame.duplicated(["ticker", "trade_date"]).any():
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_ADJ_FACTOR_DUPLICATE",
            "adj_factor_changes has duplicate ticker/date keys",
        )
    frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="coerce")
    valid = frame["adj_factor"].notna() & np.isfinite(
        frame["adj_factor"].to_numpy(dtype=float, na_value=np.nan)
    ) & frame["adj_factor"].gt(0)
    if not valid.all():
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_ADJ_FACTOR_INVALID",
            "adj_factor_changes contains a non-finite or non-positive factor",
        )
    return frame


def _apply_forward_adjustment(
    frame: pd.DataFrame,
    changes: pd.DataFrame,
    *,
    end_date: date,
) -> pd.DataFrame:
    result = frame.copy(deep=True)
    result["adj_factor"] = 1.0
    result["latest_adj_factor"] = 1.0
    if not changes.empty:
        grouped_changes = {
            ticker: group.sort_values("trade_date", kind="stable")
            for ticker, group in changes.groupby("ticker", sort=False)
        }
        for ticker, indices in result.groupby("ticker", sort=False).groups.items():
            group = grouped_changes.get(str(ticker))
            if group is None or group.empty:
                continue
            change_dates = group["trade_date"].tolist()
            change_values = group["adj_factor"].to_numpy(dtype=float)
            row_dates = result.loc[indices, "trade_date"].tolist()
            asof_values = [
                float(change_values[position])
                if (position := int(np.searchsorted(change_dates, row_date, side="right") - 1)) >= 0
                else 1.0
                for row_date in row_dates
            ]
            # Match the existing System B repository: the target-normalizing
            # denominator is the factor attached to the last actual market
            # observation, not a factor change that may have arrived after the
            # last trading row (for example on a holiday).
            actual_flags = result.loc[indices, "market_fact_status"].eq(ACTUAL_TRADING)
            actual_factors = [
                value for value, is_actual in zip(asof_values, actual_flags, strict=True) if is_actual
            ]
            latest = float(actual_factors[-1]) if actual_factors else 1.0
            result.loc[indices, "adj_factor"] = asof_values
            result.loc[indices, "latest_adj_factor"] = latest

    price_columns = [column for column in ("open", "high", "low", "close") if column in result]
    for column in price_columns:
        result[column] = (
            pd.to_numeric(result[column], errors="coerce")
            * result["adj_factor"]
            / result["latest_adj_factor"]
        )
    result["canonical_price_adjustment"] = CANONICAL_PRICE_ADJUSTMENT
    result["adjustment_as_of_date"] = end_date
    return result


def load_canonical_market_series(
    connection: duckdb.DuckDBPyConnection,
    end_date: date,
    *,
    start_date: date | None = None,
    include_non_trading: bool = False,
) -> pd.DataFrame:
    """Load one target-truncated, forward-adjusted actual market series.

    The returned frame is deterministic and sorted by ``ticker, trade_date``.
    ``start_date`` only trims the returned rows; adjustment factors and
    rolling-history callers should generally leave it unset so that prior
    observations remain available for shift/rolling windows.
    """

    if not isinstance(end_date, date):
        raise CanonicalMarketSeriesError("CANONICAL_MARKET_END_DATE_INVALID")
    tables = _table_names(connection)
    if "daily_market_snapshot" not in tables:
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_MARKET_TABLE_MISSING", "daily_market_snapshot"
        )
    market_columns = _columns(connection, "daily_market_snapshot")
    required = {"trade_date", "ticker", "close"}
    if not required <= market_columns:
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_MARKET_SCHEMA_MISSING",
            ",".join(sorted(required - market_columns)),
        )
    selected = [
        column
        for column in (
            "trade_date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "amount",
            "volume",
            "float_cap",
            "is_limit_up",
        )
        if column in market_columns
    ]
    frame = connection.execute(
        f"SELECT {', '.join(selected)} FROM daily_market_snapshot WHERE trade_date <= ?",
        [end_date],
    ).fetchdf()
    output_columns = [
        "trade_date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "amount",
        "volume",
        "float_cap",
        "is_limit_up",
        "raw_close",
        "adj_factor",
        "latest_adj_factor",
        "canonical_price_adjustment",
        "adjustment_as_of_date",
        "is_trading_day",
        "market_fact_status",
    ]
    if frame.empty:
        return pd.DataFrame(columns=output_columns)
    frame["ticker"] = frame["ticker"].astype(str).str.strip()
    if frame["ticker"].eq("").any():
        raise CanonicalMarketSeriesError("CANONICAL_MARKET_IDENTITY_INVALID", "empty ticker")
    frame["trade_date"] = _as_date_series(
        frame["trade_date"], code="CANONICAL_MARKET_DATE_INVALID"
    )
    if frame.duplicated(["trade_date", "ticker"]).any():
        raise CanonicalMarketSeriesError(
            "CANONICAL_MARKET_DUPLICATE_KEY", "daily_market_snapshot has duplicate ticker/date rows"
        )
    frame = frame.sort_values(["ticker", "trade_date"], kind="mergesort").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "amount", "volume", "float_cap"):
        if column not in frame:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["raw_close"] = frame["close"]

    calendar = _read_calendar(connection, end_date=end_date)
    suspension_keys = _read_suspension_keys(connection, end_date=end_date)
    has_volume = "volume" in market_columns
    has_state = "system_b_state_observation" in tables
    state_status: dict[tuple[str, date], str] = {}
    if has_state:
        state_columns = _columns(connection, "system_b_state_observation")
        if {"asset_id", "trade_date", "market_fact_status"} <= state_columns:
            state_frame = connection.execute(
                "SELECT asset_id, trade_date, market_fact_status "
                "FROM system_b_state_observation WHERE trade_date <= ?",
                [end_date],
            ).fetchdf()
            if not state_frame.empty:
                state_frame["trade_date"] = _as_date_series(
                    state_frame["trade_date"], code="CANONICAL_MARKET_STATE_DATE_INVALID"
                )
                for key, group in state_frame.groupby(["asset_id", "trade_date"], sort=False):
                    statuses = {str(value) for value in group["market_fact_status"].dropna()}
                    if len(statuses) > 1:
                        raise CanonicalMarketSeriesError(
                            "CANONICAL_MARKET_STATE_CONFLICT",
                            f"conflicting state statuses for {key}",
                        )
                    if statuses:
                        state_status[(str(key[0]).strip(), key[1])] = statuses.pop()

    actual_flags: list[bool] = []
    statuses: list[str] = []
    for row in frame.itertuples(index=False):
        key = (row.ticker, row.trade_date)
        status = state_status.get(key)
        if status in {EXPLICIT_NON_TRADING, UNRESOLVED_MISSING}:
            actual = False
        elif key in suspension_keys:
            actual = False
            status = EXPLICIT_NON_TRADING
        elif calendar is not None and calendar.get(row.trade_date) is not True:
            actual = False
            status = EXPLICIT_NON_TRADING if row.trade_date in calendar else UNRESOLVED_MISSING
        else:
            close_valid = (
                row.close is not None
                and not pd.isna(row.close)
                and math.isfinite(float(row.close))
            )
            volume_valid = (
                (
                    row.volume is not None
                    and not pd.isna(row.volume)
                    and math.isfinite(float(row.volume))
                    and float(row.volume) > 0
                )
                if has_volume
                else True
            )
            actual = bool(close_valid and volume_valid and status != UNRESOLVED_MISSING)
            status = ACTUAL_TRADING if actual else (
                UNRESOLVED_MISSING if status == ACTUAL_TRADING else (status or UNRESOLVED_MISSING)
            )
        actual_flags.append(actual)
        statuses.append(status or (ACTUAL_TRADING if actual else UNRESOLVED_MISSING))
    frame["is_trading_day"] = actual_flags
    frame["market_fact_status"] = statuses
    frame = _apply_forward_adjustment(
        frame,
        _read_adjustment_changes(connection, end_date=end_date),
        end_date=end_date,
    )
    if not include_non_trading:
        frame = frame.loc[frame["market_fact_status"].eq(ACTUAL_TRADING)].copy()
    if start_date is not None:
        frame = frame.loc[frame["trade_date"].ge(start_date)].copy()
    return frame.sort_values(["ticker", "trade_date"], kind="mergesort").reset_index(drop=True)


__all__ = [
    "ACTUAL_TRADING",
    "CANONICAL_PRICE_ADJUSTMENT",
    "CanonicalMarketSeriesError",
    "EXPLICIT_NON_TRADING",
    "UNRESOLVED_MISSING",
    "load_canonical_market_series",
]
