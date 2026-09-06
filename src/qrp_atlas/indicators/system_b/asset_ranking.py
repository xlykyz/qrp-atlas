"""Pure Task06-A System B asset-relative ranking calculations.

The module deliberately contains no database or scheduler code.  It accepts
already resolved canonical facts and turns them into deterministic component
and dimension results.  The production service is responsible for resolving
the point-in-time universe and for persisting the returned frames.

The public helpers are intentionally small enough to be useful in unit tests:

``normalized_rank_score``
    Return the 0--100 score series for one cross-section.
``rank_component``
    Return the complete raw/rank/score/status evidence for one cross-section.
``calculate_asset_ranking``
    Calculate M1, M2 and M3 plus the component audit frame.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
import json
import math
from typing import Any

import numpy as np
import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    ASSET_RANK_CALCULATION_VERSION,
    ASSET_RANK_INCOMPLETE_COMPONENTS,
    ASSET_RANK_INSUFFICIENT_UNIVERSE,
    ASSET_RANK_MISSING_INPUT,
    ASSET_RANK_NO_VARIATION,
    ASSET_RANK_NOT_ELIGIBLE,
    ASSET_RANK_OK,
    COMPONENT,
    DIRECTION,
    DIMENSION,
    DIMENSION_RAW,
    EVIDENCE,
    FINAL_DIMENSION_RANK,
    FINAL_DIMENSION_SCORE,
    HEIGHT_SINCE_START_RETURN,
    INPUT_PROVENANCE,
    M1_RAW,
    M1_RANK,
    M1_SCORE,
    M1_STATUS,
    M1_UNIVERSE_SIZE,
    M2_RAW,
    M2_RANK,
    M2_SCORE,
    M2_STATUS,
    M2_UNIVERSE_SIZE,
    M3_RAW,
    M3_RANK,
    M3_SCORE,
    M3_STATUS,
    M3_UNIVERSE_SIZE,
    METADATA_JSON,
    NORMALIZED_RANK_SCORE,
    POPULARITY_AVAILABLE,
    POPULARITY_UNAVAILABLE,
    RAW_RANK,
    RAW_VALUE,
    RETURN5,
    RETURN10,
    SOURCE_PROVENANCE,
    STATUS,
    TIE_COUNT,
    TICKER,
    TRADE_DATE,
    VALID_SNAPSHOT_COUNT,
)


HIGHER_IS_BETTER = "HIGHER_IS_BETTER"
LOWER_IS_BETTER = "LOWER_IS_BETTER"

M1 = "M1"
M2 = "M2"
M3 = "M3"

M1_EPISODE_RETURN = "episode_return"
M1_AVG5_AMOUNT = "avg5_amount"
M2_HEIGHT_RETURN = "height_since_start_return"
M3_EPISODE_RETURN = "episode_return"
M3_RETURN5 = "return5"
M3_RETURN10 = "return10"
M3_POPULARITY = "popularity"

DC_HOT = "dc_hot"
THS_HOT = "ths_hot"

_DIMENSION_COLUMNS = {
    M1: (M1_SCORE, M1_RANK, M1_STATUS, M1_UNIVERSE_SIZE, M1_RAW),
    M2: (M2_SCORE, M2_RANK, M2_STATUS, M2_UNIVERSE_SIZE, M2_RAW),
    M3: (M3_SCORE, M3_RANK, M3_STATUS, M3_UNIVERSE_SIZE, M3_RAW),
}


class AssetRankingError(ValueError):
    """Raised when ranking inputs violate a deterministic fact contract."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


@dataclass(frozen=True)
class RankComponentResult:
    """Raw/rank/score evidence for one component."""

    frame: pd.DataFrame
    universe_size: int
    status: str

    @property
    def scores(self) -> pd.Series:
        return self.frame[NORMALIZED_RANK_SCORE]

    @property
    def ranks(self) -> pd.Series:
        return self.frame[RAW_RANK]


@dataclass(frozen=True)
class PopularityScores:
    """One-day scores and provenance for the two popularity sources."""

    frame: pd.DataFrame
    source_metadata: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True)
class AssetRankingResult:
    """Complete pure Task06-A result."""

    snapshot: pd.DataFrame
    component_audit: pd.DataFrame
    diagnostics: tuple[str, ...] = ()
    input_provenance: Mapping[str, Any] | None = None

    @property
    def frame(self) -> pd.DataFrame:
        """Compatibility alias for callers that call calculated output ``frame``."""

        return self.snapshot


def _as_series(values: Any) -> pd.Series:
    if isinstance(values, pd.Series):
        return values.copy()
    if isinstance(values, pd.DataFrame):
        if RAW_VALUE in values.columns:
            return values[RAW_VALUE].copy()
        if len(values.columns) != 1:
            raise AssetRankingError("RANK_INPUT_INVALID", "DataFrame input must have one column or raw_value")
        return values.iloc[:, 0].copy()
    if isinstance(values, Mapping):
        return pd.Series(values)
    if isinstance(values, (str, bytes)):
        raise AssetRankingError("RANK_INPUT_INVALID", "string is not a cross-section")
    try:
        return pd.Series(list(values))
    except TypeError as exc:
        raise AssetRankingError("RANK_INPUT_INVALID", type(values).__name__) from exc


def _numeric(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    finite = np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan))
    return numeric.where(finite)


def rank_component(
    values: Any,
    *,
    direction: str = HIGHER_IS_BETTER,
) -> RankComponentResult:
    """Rank one component with average business ties.

    ``universe_size`` counts only finite numeric values.  Missing values stay
    in the returned frame and receive ``MISSING_INPUT``; they never silently
    shrink the caller's output universe.  Ticker/identity order is not used
    to resolve ties.
    """

    if direction not in {HIGHER_IS_BETTER, LOWER_IS_BETTER}:
        raise AssetRankingError("RANK_DIRECTION_INVALID", str(direction))
    raw = _numeric(_as_series(values))
    valid = raw.notna()
    universe_size = int(valid.sum())
    ranks = pd.Series(np.nan, index=raw.index, dtype="float64")
    scores = pd.Series(np.nan, index=raw.index, dtype="float64")
    tie_counts = pd.Series(np.nan, index=raw.index, dtype="float64")

    if universe_size:
        valid_values = raw.loc[valid]
        ascending = direction == LOWER_IS_BETTER
        ranks.loc[valid] = valid_values.rank(method="average", ascending=ascending)
        counts = valid_values.value_counts(dropna=False)
        tie_counts.loc[valid] = valid_values.map(counts).astype(float)
        if universe_size >= 2 and int(valid_values.nunique(dropna=True)) > 1:
            scores.loc[valid] = 100.0 * (universe_size - ranks.loc[valid]) / (universe_size - 1)

    if universe_size < 2:
        overall_status = ASSET_RANK_INSUFFICIENT_UNIVERSE
    elif int(raw.loc[valid].nunique(dropna=True)) == 1:
        overall_status = ASSET_RANK_NO_VARIATION
    else:
        overall_status = ASSET_RANK_OK

    statuses = pd.Series(ASSET_RANK_MISSING_INPUT, index=raw.index, dtype="object")
    statuses.loc[valid] = overall_status
    result = pd.DataFrame(
        {
            RAW_VALUE: raw,
            RAW_RANK: ranks,
            NORMALIZED_RANK_SCORE: scores,
            "universe_size": universe_size,
            TIE_COUNT: tie_counts.fillna(0).astype("int64"),
            STATUS: statuses,
            DIRECTION: direction,
        },
        index=raw.index,
    )
    return RankComponentResult(result, universe_size, overall_status)


def normalized_rank_score(
    values: Any,
    *,
    direction: str = HIGHER_IS_BETTER,
    return_details: bool = False,
) -> pd.Series | RankComponentResult:
    """Return ``100 * (N - average_rank) / (N - 1)`` for a cross-section.

    The default return value is a Series aligned to the input.  Pass
    ``return_details=True`` when raw ranks, universe size and statuses are
    needed for audit materialization.
    """

    result = rank_component(values, direction=direction)
    return result if return_details else result.scores


normalized_rank_details = rank_component


def snapshot_hot_score(rank_position: Any, *, universe_size: int = 100) -> float | None:
    """Normalize one complete Top-N rank, where rank 1 is strongest."""

    try:
        rank = float(rank_position)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(rank) or not rank.is_integer():
        return None
    rank_int = int(rank)
    if universe_size < 1 or rank_int < 1 or rank_int > universe_size:
        return None
    if universe_size == 1:
        return None
    return 100.0 * (universe_size - rank_int) / (universe_size - 1)


def _canonical_source(value: Any) -> str:
    text = str(value).strip().lower()
    if text in {"dc", "dc_hot", "eastmoney", "tushare_dc_hot"}:
        return DC_HOT
    if text in {"ths", "ths_hot", "tonghuashun", "tushare_ths_hot"}:
        return THS_HOT
    return text


def _availability_map(availability: Any) -> dict[str, dict[str, Any]]:
    if availability is None:
        return {}
    if isinstance(availability, Mapping):
        result: dict[str, dict[str, Any]] = {}
        for key, value in availability.items():
            if isinstance(value, Mapping):
                item = dict(value)
                status = item.get("source_status", item.get("status"))
            else:
                item = {}
                status = value
            item["source_status"] = str(status).upper() if status is not None else None
            result[_canonical_source(key)] = item
        return result
    frame = pd.DataFrame(availability).copy()
    if frame.empty:
        return {}
    source_column = "source" if "source" in frame.columns else "popularity_source"
    status_column = "source_status" if "source_status" in frame.columns else "status"
    if source_column not in frame or status_column not in frame:
        raise AssetRankingError("POPULARITY_AVAILABILITY_SCHEMA_MISSING")
    result = {}
    for row in frame.to_dict(orient="records"):
        source = _canonical_source(row[source_column])
        item = dict(row)
        item["source_status"] = str(row[status_column]).upper()
        if source in result:
            raise AssetRankingError("POPULARITY_AVAILABILITY_DUPLICATE", source)
        result[source] = item
    return result


def _source_frame(popularity: Any, source: str) -> pd.DataFrame:
    if popularity is None:
        return pd.DataFrame()
    if isinstance(popularity, Mapping):
        for key, value in popularity.items():
            if _canonical_source(key) == source:
                return pd.DataFrame(value).copy()
        return pd.DataFrame()
    frame = pd.DataFrame(popularity).copy()
    if "source" in frame.columns:
        return frame.loc[frame["source"].map(_canonical_source).eq(source)].copy()
    return frame


def _prepare_hot_source(frame: pd.DataFrame, *, source: str, target_date: date) -> tuple[pd.DataFrame, list[int]]:
    if frame.empty:
        raise AssetRankingError("POPULARITY_SNAPSHOT_MISSING", source)
    data = frame.copy()
    if TRADE_DATE in data.columns:
        parsed = pd.to_datetime(data[TRADE_DATE], errors="coerce").dt.date
        if parsed.isna().any() or not parsed.eq(target_date).all():
            raise AssetRankingError("POPULARITY_DATE_INVALID", source)
        data[TRADE_DATE] = parsed
    else:
        data[TRADE_DATE] = target_date
    if TICKER not in data.columns:
        if ASSET_ID in data.columns:
            data[TICKER] = data[ASSET_ID]
        else:
            raise AssetRankingError("POPULARITY_SCHEMA_MISSING", f"{source}:ticker")
    if "snapshot_seq" not in data.columns:
        data["snapshot_seq"] = 1
    if "rank_position" not in data.columns:
        raise AssetRankingError("POPULARITY_SCHEMA_MISSING", f"{source}:rank_position")
    data[TICKER] = data[TICKER].astype(str).str.strip()
    if data[TICKER].eq("").any():
        raise AssetRankingError("POPULARITY_IDENTITY_INVALID", source)
    data["snapshot_seq"] = pd.to_numeric(data["snapshot_seq"], errors="coerce")
    data["rank_position"] = pd.to_numeric(data["rank_position"], errors="coerce")
    if data[["snapshot_seq", "rank_position"]].isna().any().any():
        raise AssetRankingError("POPULARITY_RANK_INVALID", source)
    if not (data["snapshot_seq"] % 1 == 0).all() or not (data["rank_position"] % 1 == 0).all():
        raise AssetRankingError("POPULARITY_RANK_INVALID", source)
    data["snapshot_seq"] = data["snapshot_seq"].astype(int)
    data["rank_position"] = data["rank_position"].astype(int)
    if data.duplicated([TRADE_DATE, "snapshot_seq", "rank_position"]).any():
        raise AssetRankingError("POPULARITY_DUPLICATE_KEY", source)
    sequences: list[int] = []
    for sequence, group in data.groupby("snapshot_seq", sort=True):
        if len(group) != 100:
            raise AssetRankingError("POPULARITY_INCOMPLETE_TOP100", f"{source}:{sequence}:{len(group)}")
        if group[TICKER].nunique() != 100:
            raise AssetRankingError("POPULARITY_INCOMPLETE_TOP100", f"{source}:{sequence}:tickers")
        if set(group["rank_position"]) != set(range(1, 101)):
            raise AssetRankingError("POPULARITY_INCOMPLETE_TOP100", f"{source}:{sequence}:ranks")
        sequences.append(int(sequence))
    if sequences != list(range(1, len(sequences) + 1)):
        raise AssetRankingError("POPULARITY_SNAPSHOT_SEQUENCE_INVALID", source)
    return data.sort_values(["snapshot_seq", "rank_position"], kind="mergesort"), sequences


def _availability_snapshot_seqs(info: Mapping[str, Any], *, source: str) -> list[int] | None:
    raw = info.get("snapshot_seqs")
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AssetRankingError("POPULARITY_AVAILABILITY_INVALID", source) from exc
    if not isinstance(raw, (list, tuple)):
        raise AssetRankingError("POPULARITY_AVAILABILITY_INVALID", source)
    try:
        values = [int(value) for value in raw]
    except (TypeError, ValueError) as exc:
        raise AssetRankingError("POPULARITY_AVAILABILITY_INVALID", source) from exc
    if values != list(range(1, len(values) + 1)):
        raise AssetRankingError("POPULARITY_AVAILABILITY_INVALID", source)
    return values


def calculate_popularity_scores(
    popularity: Any,
    *,
    target_date: date,
    availability: Any = None,
) -> PopularityScores:
    """Calculate per-ticker daily DC/THS scores using every valid snapshot.

    Availability is an explicit source/date fact in production.  For a pure
    helper without an availability frame, a non-empty source is treated as
    available and an empty source as unavailable for convenience; the
    production service always passes the persisted availability rows.
    """

    availability_map = _availability_map(availability)
    source_frames: dict[str, pd.DataFrame] = {}
    metadata: dict[str, dict[str, Any]] = {}
    all_tickers: set[str] = set()
    for source in (DC_HOT, THS_HOT):
        raw = _source_frame(popularity, source)
        info = dict(availability_map.get(source, {}))
        status = info.get("source_status")
        if status is None:
            status = POPULARITY_AVAILABLE if not raw.empty else POPULARITY_UNAVAILABLE
        status = str(status).upper()
        if status not in {POPULARITY_AVAILABLE, POPULARITY_UNAVAILABLE}:
            raise AssetRankingError("POPULARITY_SOURCE_STATUS_INVALID", f"{source}:{status}")
        if status == POPULARITY_AVAILABLE:
            prepared, sequences = _prepare_hot_source(raw, source=source, target_date=target_date)
            source_frames[source] = prepared
            all_tickers.update(prepared[TICKER].tolist())
            expected_count = info.get(VALID_SNAPSHOT_COUNT)
            try:
                expected = None if expected_count is None else int(expected_count)
            except (TypeError, ValueError) as exc:
                raise AssetRankingError("POPULARITY_AVAILABILITY_INVALID", source) from exc
            if expected is not None and expected != len(sequences):
                raise AssetRankingError("POPULARITY_AVAILABILITY_MISMATCH", source)
            expected_sequences = _availability_snapshot_seqs(info, source=source)
            if expected_sequences is not None and expected_sequences != sequences:
                raise AssetRankingError("POPULARITY_AVAILABILITY_MISMATCH", source)
        else:
            source_frames[source] = pd.DataFrame()
            sequences = []
        metadata[source] = {
            "source_status": status,
            "valid_snapshot_count": len(sequences),
            "snapshot_seqs": sequences,
            "input_version": str(info.get("input_version", "")),
            "source_provenance": info.get("source_provenance", {}),
        }

    rows: list[dict[str, Any]] = []
    for ticker in sorted(all_tickers):
        values: dict[str, Any] = {TICKER: ticker}
        for source in (DC_HOT, THS_HOT):
            info = metadata[source]
            if info["source_status"] == POPULARITY_UNAVAILABLE:
                values[f"{source}_score"] = None
                continue
            source_frame = source_frames[source]
            snapshot_scores: list[float] = []
            for _, snapshot in source_frame.groupby("snapshot_seq", sort=True):
                match = snapshot.loc[snapshot[TICKER].eq(ticker), "rank_position"]
                rank = int(match.iloc[0]) if not match.empty else None
                # A complete/healthy platform observation gives an off-list
                # ticker a real business score of zero.
                snapshot_scores.append(0.0 if rank is None else float(snapshot_hot_score(rank)))
            values[f"{source}_score"] = float(np.mean(snapshot_scores)) if snapshot_scores else None
        dc_value = values[f"{DC_HOT}_score"]
        ths_value = values[f"{THS_HOT}_score"]
        values["hot_rank_score"] = (
            (float(dc_value) + float(ths_value)) / 2.0
            if dc_value is not None and ths_value is not None
            else None
        )
        rows.append(values)
    columns = [TICKER, "dc_hot_score", "ths_hot_score", "hot_rank_score"]
    return PopularityScores(pd.DataFrame(rows, columns=columns), metadata)


compute_popularity_scores = calculate_popularity_scores


def _normalise_date(value: Any) -> date:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise AssetRankingError("ASSET_RANK_DATE_INVALID", str(value))
    return parsed.date()


def _normalise_universe(universe: pd.DataFrame, target_date: date) -> pd.DataFrame:
    if universe is None or not isinstance(universe, pd.DataFrame):
        raise AssetRankingError("ASSET_RANK_UNIVERSE_INVALID")
    data = universe.copy()
    if TICKER not in data.columns:
        if ASSET_ID in data.columns:
            data[TICKER] = data[ASSET_ID]
        else:
            raise AssetRankingError("ASSET_RANK_UNIVERSE_SCHEMA_MISSING", TICKER)
    if TRADE_DATE in data.columns:
        parsed = pd.to_datetime(data[TRADE_DATE], errors="coerce").dt.date
        if parsed.isna().any() or not parsed.eq(target_date).all():
            raise AssetRankingError("ASSET_RANK_UNIVERSE_DATE_INVALID")
        data[TRADE_DATE] = parsed
    else:
        data[TRADE_DATE] = target_date
    data[TICKER] = data[TICKER].astype(str).str.strip()
    if data[TICKER].eq("").any() or data[TICKER].duplicated().any():
        raise AssetRankingError("ASSET_RANK_UNIVERSE_IDENTITY_INVALID")
    return data[[TRADE_DATE, TICKER]].sort_values(TICKER, kind="mergesort").reset_index(drop=True)


def _actual_market(market: pd.DataFrame | None, target_date: date) -> pd.DataFrame:
    if market is None:
        return pd.DataFrame(columns=[TRADE_DATE, TICKER, "close", "amount"])
    data = market.copy()
    if TICKER not in data.columns and ASSET_ID in data.columns:
        data[TICKER] = data[ASSET_ID]
    if TICKER not in data.columns or TRADE_DATE not in data.columns:
        raise AssetRankingError("ASSET_RANK_MARKET_SCHEMA_MISSING")
    data[TICKER] = data[TICKER].astype(str).str.strip()
    data[TRADE_DATE] = pd.to_datetime(data[TRADE_DATE], errors="coerce").dt.date
    if data[TRADE_DATE].isna().any() or data[TRADE_DATE].gt(target_date).any():
        raise AssetRankingError("ASSET_RANK_MARKET_DATE_INVALID")
    if data.duplicated([TRADE_DATE, TICKER]).any():
        raise AssetRankingError("ASSET_RANK_MARKET_DUPLICATE_KEY")
    if "market_fact_status" in data.columns:
        data = data.loc[data["market_fact_status"].eq("ACTUAL_TRADING")].copy()
    elif "is_trading_day" in data.columns:
        data = data.loc[data["is_trading_day"].fillna(False).astype(bool)].copy()
    if "close" not in data.columns:
        raise AssetRankingError("ASSET_RANK_MARKET_SCHEMA_MISSING", "close")
    if "amount" not in data.columns:
        data["amount"] = np.nan
    for column in ("close", "amount"):
        data[column] = _numeric(data[column])
    return data.sort_values([TICKER, TRADE_DATE], kind="mergesort").reset_index(drop=True)


def _target_market_features(market: pd.DataFrame, target_date: date) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for ticker, group in market.groupby(TICKER, sort=False):
        group = group.sort_values(TRADE_DATE, kind="mergesort").reset_index(drop=True)
        target_rows = group.loc[group[TRADE_DATE].eq(target_date)]
        # Return5/10 are target-day observations.  A suspended/missing target
        # row must not be filled with a previous day's return.
        target_position = int(target_rows.index[-1]) if not target_rows.empty else None
        avg5 = np.nan
        return5 = np.nan
        return10 = np.nan
        if target_position is not None:
            amount_values = group["amount"].iloc[max(0, target_position - 4) : target_position + 1]
            if len(amount_values) == 5 and amount_values.notna().all():
                avg5 = float(amount_values.mean())
            close = group["close"]
            target_close = close.iloc[target_position]
            for name, shift in ((RETURN5, 4), (RETURN10, 9)):
                base_position = target_position - shift
                if base_position >= 0:
                    base = close.iloc[base_position]
                    if pd.notna(target_close) and pd.notna(base) and float(base) != 0:
                        value = float(target_close) / float(base) - 1.0
                        if math.isfinite(value):
                            if name == RETURN5:
                                return5 = value
                            else:
                                return10 = value
        rows.append({TICKER: ticker, "avg5_amount": avg5, RETURN5: return5, RETURN10: return10})
    return pd.DataFrame(rows, columns=[TICKER, "avg5_amount", RETURN5, RETURN10])


def _target_episode_values(episode_observations: pd.DataFrame | None, target_date: date) -> pd.DataFrame:
    if episode_observations is None:
        return pd.DataFrame(columns=[TICKER, M1_EPISODE_RETURN])
    data = episode_observations.copy()
    if TICKER not in data.columns and ASSET_ID in data.columns:
        data[TICKER] = data[ASSET_ID]
    required = {TICKER, TRADE_DATE, "episode_return"}
    if not required <= set(data.columns):
        raise AssetRankingError("ASSET_RANK_EPISODE_SCHEMA_MISSING", ",".join(sorted(required - set(data.columns))))
    data[TICKER] = data[TICKER].astype(str).str.strip()
    data[TRADE_DATE] = pd.to_datetime(data[TRADE_DATE], errors="coerce").dt.date
    if data[TRADE_DATE].isna().any() or data[TRADE_DATE].gt(target_date).any():
        raise AssetRankingError("ASSET_RANK_EPISODE_DATE_INVALID")
    if data.duplicated([TRADE_DATE, TICKER]).any():
        raise AssetRankingError("ASSET_RANK_EPISODE_DUPLICATE_KEY")
    target = data.loc[data[TRADE_DATE].eq(target_date), [TICKER, "episode_return"]].copy()
    target["episode_return"] = _numeric(target["episode_return"])
    return target


def _membership_sets(memberships: pd.DataFrame | None, target_date: date) -> tuple[dict[str, set[str]], dict[str, dict[str, Any]]]:
    if memberships is None:
        return {}, {}
    data = memberships.copy()
    if TICKER not in data.columns and ASSET_ID in data.columns:
        data[TICKER] = data[ASSET_ID]
    required = {TICKER, TRADE_DATE, "pool_type", "membership_state"}
    if not required <= set(data.columns):
        raise AssetRankingError("ASSET_RANK_MEMBERSHIP_SCHEMA_MISSING", ",".join(sorted(required - set(data.columns))))
    data[TICKER] = data[TICKER].astype(str).str.strip()
    data[TRADE_DATE] = pd.to_datetime(data[TRADE_DATE], errors="coerce").dt.date
    if data[TRADE_DATE].isna().any() or data[TRADE_DATE].gt(target_date).any():
        raise AssetRankingError("ASSET_RANK_MEMBERSHIP_DATE_INVALID")
    if data.duplicated([TRADE_DATE, TICKER, "pool_type"]).any():
        raise AssetRankingError("ASSET_RANK_MEMBERSHIP_DUPLICATE_KEY")
    target = data.loc[data[TRADE_DATE].eq(target_date)].copy()
    active: dict[str, set[str]] = {"CAPACITY": set(), "HEIGHT": set(), "RECOGNITION": set()}
    metrics: dict[str, dict[str, Any]] = {}
    for row in target.to_dict(orient="records"):
        pool = str(row["pool_type"]).upper()
        ticker = row[TICKER]
        if pool not in active:
            continue
        if str(row["membership_state"]).upper() == "IN_POOL":
            active[pool].add(ticker)
        metrics[f"{pool}:{ticker}"] = row
    return active, metrics


def _height_values(
    universe: pd.DataFrame,
    membership_rows: Mapping[str, Mapping[str, Any]],
    market: pd.DataFrame,
    target_date: date,
) -> pd.Series:
    values: dict[str, float | None] = {}
    for ticker in universe[TICKER]:
        row = membership_rows.get(f"HEIGHT:{ticker}", {})
        value = row.get(HEIGHT_SINCE_START_RETURN)
        metrics = row.get("metrics_json")
        if (value is None or pd.isna(value)) and metrics:
            try:
                parsed = json.loads(metrics) if isinstance(metrics, str) else dict(metrics)
                value = parsed.get(HEIGHT_SINCE_START_RETURN)
                start_date = parsed.get("height_start_date")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise AssetRankingError("ASSET_RANK_HEIGHT_METRICS_INVALID", ticker) from exc
        else:
            start_date = None
        if value is None and row:
            start_date = row.get("height_start_date", start_date)
            if start_date is not None:
                parsed_start = pd.to_datetime(start_date, errors="coerce")
                if not pd.isna(parsed_start):
                    start = parsed_start.date()
                    group = market.loc[market[TICKER].eq(ticker)].sort_values(TRADE_DATE, kind="mergesort")
                    prior = group.loc[group[TRADE_DATE].lt(start)]
                    current = group.loc[group[TRADE_DATE].eq(target_date)]
                    if not prior.empty and not current.empty:
                        base = prior.iloc[-1]["close"]
                        close = current.iloc[-1]["close"]
                        if pd.notna(base) and pd.notna(close) and float(base) != 0:
                            candidate = float(close) / float(base) - 1.0
                            value = candidate if math.isfinite(candidate) else None
        try:
            value_float = None if value is None or pd.isna(value) else float(value)
        except (TypeError, ValueError):
            value_float = None
        values[ticker] = value_float if value_float is not None and math.isfinite(value_float) else None
    return pd.Series(values, index=universe[TICKER])


def _target_raw_values(
    universe: pd.DataFrame,
    *,
    market: pd.DataFrame | None,
    episode_observations: pd.DataFrame | None,
    memberships: pd.DataFrame | None,
    popularity: Any,
    popularity_availability: Any,
    target_date: date,
) -> tuple[pd.DataFrame, dict[str, Mapping[str, Any]], tuple[str, ...]]:
    data = universe.copy()
    market_data = _actual_market(market, target_date)
    market_values = _target_market_features(market_data, target_date)
    episode_values = _target_episode_values(episode_observations, target_date)
    active, membership_rows = _membership_sets(memberships, target_date)
    data = data.merge(market_values, on=TICKER, how="left", validate="one_to_one")
    data = data.merge(episode_values, on=TICKER, how="left", validate="one_to_one")
    data["height_since_start_return"] = _height_values(data, membership_rows, market_data, target_date).reindex(data[TICKER]).to_numpy()
    pop = calculate_popularity_scores(popularity, target_date=target_date, availability=popularity_availability)
    data = data.merge(pop.frame, on=TICKER, how="left", validate="one_to_one")
    for column in ("dc_hot_score", "ths_hot_score", "hot_rank_score"):
        if column not in data:
            data[column] = np.nan
    for source in (DC_HOT, THS_HOT):
        source_metadata = pop.source_metadata.get(source, {})
        if source_metadata.get("source_status") == POPULARITY_AVAILABLE:
            data[f"{source}_score"] = data[f"{source}_score"].fillna(0.0)
    if (
        pop.source_metadata[DC_HOT]["source_status"] == POPULARITY_AVAILABLE
        and pop.source_metadata[THS_HOT]["source_status"] == POPULARITY_AVAILABLE
    ):
        data["hot_rank_score"] = data["hot_rank_score"].fillna(0.0)
    eligibility = {
        M1: data[TICKER].isin(active.get("CAPACITY", set())),
        M2: data[TICKER].isin(active.get("HEIGHT", set())),
        M3: data[TICKER].isin(active.get("RECOGNITION", set())),
    }
    for dimension, mask in eligibility.items():
        data[f"{dimension.lower()}_eligible"] = mask.to_numpy()
    diagnostics: list[str] = []
    for source in (DC_HOT, THS_HOT):
        status = pop.source_metadata[source]["source_status"]
        if status == POPULARITY_UNAVAILABLE:
            diagnostics.append(f"{source.upper()}_SOURCE_UNAVAILABLE")
    provenance = {source: dict(meta) for source, meta in pop.source_metadata.items()}
    return data, provenance, tuple(diagnostics)


def _safe_json(value: Any) -> str:
    def clean(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {str(key): clean(val) for key, val in item.items()}
        if isinstance(item, (list, tuple, set)):
            return [clean(val) for val in item]
        if isinstance(item, (date, pd.Timestamp)):
            return item.isoformat()
        if item is None or isinstance(item, (str, bool, int)):
            return item
        try:
            number = float(item)
        except (TypeError, ValueError):
            return str(item)
        return number if math.isfinite(number) else None

    return json.dumps(clean(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _dimension_result(
    data: pd.DataFrame,
    *,
    dimension: str,
    pool: str,
    components: Sequence[tuple[str, str, float]],
    provenance: Mapping[str, Mapping[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    eligibility = data[f"{dimension.lower()}_eligible"].astype(bool)
    rank_details: dict[str, RankComponentResult] = {}
    component_scores: dict[str, pd.Series] = {}
    for component, column, _weight in components:
        values = data[column].where(eligibility, np.nan)
        detail = rank_component(values)
        rank_details[component] = detail
        component_scores[component] = detail.scores

    if dimension == M2:
        raw_dimension = data[components[0][1]].where(eligibility, np.nan).astype(float)
        final_detail = rank_details[components[0][0]]
        complete = eligibility & raw_dimension.notna() & final_detail.scores.notna()
    else:
        raw_dimension = pd.Series(0.0, index=data.index, dtype="float64")
        complete = eligibility.copy()
        for component, _column, weight in components:
            score = component_scores[component]
            complete &= score.notna()
            raw_dimension += score.fillna(0.0) * weight
        raw_dimension = raw_dimension.where(complete, np.nan)
        final_detail = rank_component(raw_dimension)

    n_final = int(complete.sum())
    final_scores = final_detail.scores
    final_ranks = final_detail.ranks
    snapshot = pd.DataFrame({TICKER: data[TICKER].to_numpy()})
    score_values: list[float | None] = []
    rank_values: list[float | None] = []
    statuses: list[str] = []
    for index, is_eligible in enumerate(eligibility.tolist()):
        if not is_eligible:
            statuses.append(ASSET_RANK_NOT_ELIGIBLE)
            score_values.append(None)
            rank_values.append(None)
        elif not bool(complete.iloc[index]):
            # M2 uses the direct normalized component status.  M1/M3 are
            # composites, so any invalid required component is incomplete.
            if dimension == M2 and rank_details[components[0][0]].frame.iloc[index][STATUS] in {
                ASSET_RANK_INSUFFICIENT_UNIVERSE,
                ASSET_RANK_NO_VARIATION,
            }:
                statuses.append(str(rank_details[components[0][0]].frame.iloc[index][STATUS]))
            else:
                statuses.append(ASSET_RANK_INCOMPLETE_COMPONENTS)
            score_values.append(None)
            rank_values.append(None)
        else:
            statuses.append(str(final_detail.frame.iloc[index][STATUS]))
            score = final_scores.iloc[index]
            rank = final_ranks.iloc[index]
            score_values.append(None if pd.isna(score) else float(score))
            rank_values.append(None if pd.isna(rank) else float(rank))
    prefix = dimension.lower()
    snapshot[f"{prefix}_score"] = score_values
    snapshot[f"{prefix}_rank"] = rank_values
    snapshot[f"{prefix}_status"] = statuses
    snapshot[f"{prefix}_universe_size"] = [n_final if active else 0 for active in eligibility]
    snapshot[f"{prefix}_raw"] = [None if pd.isna(value) else float(value) for value in raw_dimension]

    audit_rows: list[dict[str, Any]] = []
    for row_index, ticker in enumerate(data[TICKER].tolist()):
        final_rank = final_ranks.iloc[row_index]
        final_score = final_scores.iloc[row_index]
        for component, column, weight in components:
            detail = rank_details[component]
            detail_row = detail.frame.iloc[row_index]
            status = ASSET_RANK_NOT_ELIGIBLE if not eligibility.iloc[row_index] else str(detail_row[STATUS])
            source = provenance.get(component, {})
            if dimension == M3 and component == M3_POPULARITY:
                source = {
                    "dc_source_status": provenance.get(DC_HOT, {}).get("source_status"),
                    "ths_source_status": provenance.get(THS_HOT, {}).get("source_status"),
                    "dc_hot_score": data.iloc[row_index].get("dc_hot_score"),
                    "ths_hot_score": data.iloc[row_index].get("ths_hot_score"),
                    "hot_rank_score": data.iloc[row_index].get("hot_rank_score"),
                    "dc_valid_snapshot_count": provenance.get(DC_HOT, {}).get("valid_snapshot_count", 0),
                    "ths_valid_snapshot_count": provenance.get(THS_HOT, {}).get("valid_snapshot_count", 0),
                    "dc_snapshot_seqs": provenance.get(DC_HOT, {}).get("snapshot_seqs", []),
                    "ths_snapshot_seqs": provenance.get(THS_HOT, {}).get("snapshot_seqs", []),
                    "dc_source_provenance": provenance.get(DC_HOT, {}).get("source_provenance", {}),
                    "ths_source_provenance": provenance.get(THS_HOT, {}).get("source_provenance", {}),
                    "popularity_input_version": {
                        "dc_hot": provenance.get(DC_HOT, {}).get("input_version", ""),
                        "ths_hot": provenance.get(THS_HOT, {}).get("input_version", ""),
                    },
                }
            audit_rows.append(
                {
                    TICKER: ticker,
                    DIMENSION: dimension,
                    COMPONENT: component,
                    RAW_VALUE: None if pd.isna(detail_row[RAW_VALUE]) else float(detail_row[RAW_VALUE]),
                    DIRECTION: str(detail_row[DIRECTION]),
                    RAW_RANK: None if pd.isna(detail_row[RAW_RANK]) else float(detail_row[RAW_RANK]),
                    NORMALIZED_RANK_SCORE: None if pd.isna(detail_row[NORMALIZED_RANK_SCORE]) else float(detail_row[NORMALIZED_RANK_SCORE]),
                    DIMENSION_RAW: None if pd.isna(raw_dimension.iloc[row_index]) else float(raw_dimension.iloc[row_index]),
                    FINAL_DIMENSION_RANK: None if pd.isna(final_rank) else float(final_rank),
                    FINAL_DIMENSION_SCORE: None if pd.isna(final_score) else float(final_score),
                    "universe_size": int(detail_row["universe_size"]),
                    TIE_COUNT: int(detail_row[TIE_COUNT]),
                    STATUS: status,
                    SOURCE_PROVENANCE: _safe_json(source),
                    METADATA_JSON: _safe_json({"pool": pool, "weight": weight, "final_universe_size": n_final}),
                }
            )
    return snapshot, pd.DataFrame(audit_rows)


def calculate_asset_ranking(
    universe: pd.DataFrame,
    *,
    trade_date: date | str,
    market_series: pd.DataFrame | None = None,
    episode_observations: pd.DataFrame | None = None,
    memberships: pd.DataFrame | None = None,
    popularity: Any = None,
    popularity_availability: Any = None,
    input_provenance: Mapping[str, Any] | None = None,
) -> AssetRankingResult:
    """Calculate deterministic M1/M2/M3 results for one target date."""

    target = _normalise_date(trade_date)
    canonical = _normalise_universe(universe, target)
    data, popularity_provenance, diagnostics = _target_raw_values(
        canonical,
        market=market_series,
        episode_observations=episode_observations,
        memberships=memberships,
        popularity=popularity,
        popularity_availability=popularity_availability,
        target_date=target,
    )
    m1_snapshot, m1_audit = _dimension_result(
        data,
        dimension=M1,
        pool="CAPACITY",
        components=(
            (M1_EPISODE_RETURN, "episode_return", 0.5),
            (M1_AVG5_AMOUNT, "avg5_amount", 0.5),
        ),
        provenance=popularity_provenance,
    )
    m2_snapshot, m2_audit = _dimension_result(
        data,
        dimension=M2,
        pool="HEIGHT",
        components=((M2_HEIGHT_RETURN, HEIGHT_SINCE_START_RETURN, 1.0),),
        provenance=popularity_provenance,
    )
    m3_snapshot, m3_audit = _dimension_result(
        data,
        dimension=M3,
        pool="RECOGNITION",
        components=(
            (M3_EPISODE_RETURN, "episode_return", 0.2),
            (M3_RETURN5, RETURN5, 0.2),
            (M3_RETURN10, RETURN10, 0.2),
            (M3_POPULARITY, "hot_rank_score", 0.4),
        ),
        provenance=popularity_provenance,
    )
    snapshot = canonical[[TRADE_DATE, TICKER]].copy()
    for piece in (m1_snapshot, m2_snapshot, m3_snapshot):
        snapshot = snapshot.merge(piece, on=TICKER, how="left", validate="one_to_one")
    # The pure layer emits stable JSON evidence; production can augment it
    # with source table/run identifiers before persistence.
    audit = pd.concat([m1_audit, m2_audit, m3_audit], ignore_index=True)
    audit[TRADE_DATE] = target
    snapshot[INPUT_PROVENANCE] = _safe_json(input_provenance or {})
    snapshot["diagnostics"] = _safe_json(list(diagnostics))
    evidence: list[str] = []
    for row in snapshot.to_dict(orient="records"):
        evidence.append(_safe_json(row))
    snapshot[EVIDENCE] = evidence
    audit["calculation_version"] = ASSET_RANK_CALCULATION_VERSION
    return AssetRankingResult(snapshot, audit, diagnostics, input_provenance)


calculate_asset_rank = calculate_asset_ranking


def calculate_m1(*args: Any, **kwargs: Any) -> pd.DataFrame:
    """Convenience wrapper returning the M1 portion of a full calculation."""

    result = calculate_asset_ranking(*args, **kwargs)
    return result.snapshot[[TICKER, M1_SCORE, M1_RANK, M1_STATUS, M1_UNIVERSE_SIZE, M1_RAW]]


def calculate_m2(*args: Any, **kwargs: Any) -> pd.DataFrame:
    """Convenience wrapper returning the M2 portion of a full calculation."""

    result = calculate_asset_ranking(*args, **kwargs)
    return result.snapshot[[TICKER, M2_SCORE, M2_RANK, M2_STATUS, M2_UNIVERSE_SIZE, M2_RAW]]


def calculate_m3(*args: Any, **kwargs: Any) -> pd.DataFrame:
    """Convenience wrapper returning the M3 portion of a full calculation."""

    result = calculate_asset_ranking(*args, **kwargs)
    return result.snapshot[[TICKER, M3_SCORE, M3_RANK, M3_STATUS, M3_UNIVERSE_SIZE, M3_RAW]]


__all__ = [
    "ASSET_RANK_CALCULATION_VERSION",
    "AssetRankingError",
    "AssetRankingResult",
    "RankComponentResult",
    "PopularityScores",
    "HIGHER_IS_BETTER",
    "LOWER_IS_BETTER",
    "M1",
    "M2",
    "M3",
    "M1_EPISODE_RETURN",
    "M1_AVG5_AMOUNT",
    "M2_HEIGHT_RETURN",
    "M3_EPISODE_RETURN",
    "M3_RETURN5",
    "M3_RETURN10",
    "M3_POPULARITY",
    "DC_HOT",
    "THS_HOT",
    "rank_component",
    "normalized_rank_score",
    "normalized_rank_details",
    "snapshot_hot_score",
    "calculate_popularity_scores",
    "compute_popularity_scores",
    "calculate_asset_ranking",
    "calculate_asset_rank",
    "calculate_m1",
    "calculate_m2",
    "calculate_m3",
]
