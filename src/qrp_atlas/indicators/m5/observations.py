"""Pure calculation of daily Theme M5 popularity observations.

The function in this module deliberately consumes already-prepared facts.  PIT
selection, source completeness, and persistence belong to the pipeline layer.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from collections.abc import Mapping

import pandas as pd

from qrp_atlas.contracts import (
    ASSET_ID,
    COLLECTION_ID,
    SOURCE,
    THEME_ID,
    TICKER,
    TRADE_DATE,
    THEME_HOT_LIST_APPEARANCE_COUNT,
    THEME_HOT_SOURCE_COUNT,
    THEME_HOT_STOCK_COUNT,
    THEME_HOT_STOCK_RATIO,
    THEME_MEMBER_COUNT,
    normalize_ticker,
)


class M5ObservationError(ValueError):
    """Raised when M5 calculation inputs cannot support a deterministic fact."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


_OUTPUT_COLUMNS = (
    THEME_ID,
    COLLECTION_ID,
    TRADE_DATE,
    THEME_MEMBER_COUNT,
    THEME_HOT_STOCK_COUNT,
    THEME_HOT_STOCK_RATIO,
    THEME_HOT_LIST_APPEARANCE_COUNT,
    THEME_HOT_SOURCE_COUNT,
)


def _records_to_frame(value: object, *, empty_columns: tuple[str, ...]) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy(deep=True)
    if value is None:
        return pd.DataFrame(columns=list(empty_columns))
    if isinstance(value, Mapping):
        return pd.DataFrame([dict(value)])
    if isinstance(value, (str, bytes)):
        raise M5ObservationError("M5_INPUT_TYPE_INVALID", "string inputs are not record collections")
    try:
        records: list[object] = list(value)  # type: ignore[arg-type]
    except TypeError as exc:
        raise M5ObservationError("M5_INPUT_TYPE_INVALID", type(value).__name__) from exc

    normalised: list[object] = []
    for item in records:
        if is_dataclass(item):
            normalised.append(asdict(item))
        elif isinstance(item, Mapping):
            normalised.append(dict(item))
        elif hasattr(item, "_asdict"):
            normalised.append(item._asdict())
        else:
            names = (THEME_ID, COLLECTION_ID, ASSET_ID, TICKER, SOURCE, TRADE_DATE)
            normalised.append({name: getattr(item, name) for name in names if hasattr(item, name)})
    return pd.DataFrame(normalised, columns=list(empty_columns) if not normalised else None)


def _normalise_date(value: object, *, code: str = "M5_TRADE_DATE_INVALID") -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return pd.to_datetime(value, errors="raise").date()
    except (TypeError, ValueError, OverflowError) as exc:
        raise M5ObservationError(code, f"cannot parse trade_date {value!r}") from exc


def _normalise_date_column(frame: pd.DataFrame, *, frame_name: str) -> pd.DataFrame:
    result = frame.copy(deep=True)
    if TRADE_DATE not in result.columns or result.empty:
        return result
    try:
        result[TRADE_DATE] = pd.to_datetime(result[TRADE_DATE], errors="raise").dt.date
    except (TypeError, ValueError, OverflowError) as exc:
        raise M5ObservationError(
            "M5_TRADE_DATE_INVALID",
            f"{frame_name}.{TRADE_DATE} contains an invalid value",
        ) from exc
    if result[TRADE_DATE].isna().any():
        raise M5ObservationError("M5_TRADE_DATE_INVALID", f"{frame_name}.{TRADE_DATE} contains NULL")
    return result


def _resolve_theme_universe(
    theme_universe: object,
    memberships: pd.DataFrame,
) -> pd.DataFrame:
    if theme_universe is None:
        if memberships.empty:
            return pd.DataFrame(columns=[THEME_ID, COLLECTION_ID])
        missing = {THEME_ID, COLLECTION_ID} - set(memberships.columns)
        if missing:
            raise M5ObservationError(
                "M5_THEME_UNIVERSE_MISSING",
                "theme_universe is required when memberships do not contain theme_id and collection_id",
            )
        universe = memberships[[THEME_ID, COLLECTION_ID]].copy(deep=True)
    elif isinstance(theme_universe, Mapping):
        pairs: list[dict[str, object]] = []
        keys = {str(key) for key in theme_universe}
        values = {str(value) for value in theme_universe.values()}
        member_collections = set(memberships[COLLECTION_ID].dropna().astype(str)) if COLLECTION_ID in memberships else set()
        member_themes = set(memberships[THEME_ID].dropna().astype(str)) if THEME_ID in memberships else set()
        key_is_collection = bool(keys & member_collections) and bool(values & member_themes)
        key_is_theme = bool(keys & member_themes) and bool(values & member_collections)
        if not key_is_collection and not key_is_theme:
            key_is_theme = any(str(key).startswith("THM:") for key in theme_universe)
            key_is_collection = not key_is_theme and any(str(key).startswith(("COLL:", "COL:")) for key in theme_universe)
        # A Theme-keyed mapping is the unambiguous fallback for simple test or
        # API identifiers such as {"THEME_A": "COLLECTION_A"}.
        if not key_is_collection and not key_is_theme:
            key_is_theme = True
        for key, value in theme_universe.items():
            if key_is_collection:
                pairs.append({THEME_ID: value, COLLECTION_ID: key})
            else:
                pairs.append({THEME_ID: key, COLLECTION_ID: value})
        universe = pd.DataFrame(pairs, columns=[THEME_ID, COLLECTION_ID])
    elif isinstance(theme_universe, pd.DataFrame):
        universe = theme_universe.copy(deep=True)
    else:
        try:
            pairs = list(theme_universe)  # type: ignore[arg-type]
        except TypeError as exc:
            raise M5ObservationError("M5_THEME_UNIVERSE_INVALID", type(theme_universe).__name__) from exc
        if pairs and isinstance(pairs[0], Mapping):
            universe = pd.DataFrame([dict(item) for item in pairs])
        else:
            universe = pd.DataFrame(pairs, columns=[THEME_ID, COLLECTION_ID])

    required = {THEME_ID, COLLECTION_ID}
    missing = required - set(universe.columns)
    if missing:
        raise M5ObservationError("M5_THEME_UNIVERSE_INVALID", f"missing columns: {sorted(missing)}")
    universe = _normalise_date_column(universe, frame_name="theme_universe")
    for column in (THEME_ID, COLLECTION_ID):
        if universe[column].isna().any() or universe[column].astype(str).str.strip().eq("").any():
            raise M5ObservationError("M5_THEME_UNIVERSE_INVALID", f"{column} contains NULL or empty values")

    duplicate = universe.duplicated(subset=[THEME_ID, COLLECTION_ID], keep=False)
    if duplicate.any():
        universe = universe.drop_duplicates(subset=[THEME_ID, COLLECTION_ID], keep="first")
    if universe[THEME_ID].duplicated().any():
        raise M5ObservationError("M5_THEME_UNIVERSE_INVALID", "theme_id maps to more than one collection_id")
    return universe.reset_index(drop=True)


def _normalise_asset_series(series: pd.Series, *, frame_name: str) -> pd.Series:
    values: list[str] = []
    for value in series.tolist():
        if pd.isna(value) or not str(value).strip():
            raise M5ObservationError("M5_ASSET_ID_INVALID", f"{frame_name} contains an empty ticker")
        try:
            normalised = normalize_ticker(str(value))
        except (TypeError, ValueError) as exc:
            raise M5ObservationError("M5_ASSET_ID_INVALID", f"invalid ticker {value!r}") from exc
        if not normalised or normalised.lower() in {"nan", "none"}:
            raise M5ObservationError("M5_ASSET_ID_INVALID", f"invalid ticker {value!r}")
        values.append(normalised)
    return pd.Series(values, index=series.index, dtype="string")


def _resolve_target_date(
    explicit: date | None,
    *frames: tuple[str, pd.DataFrame],
) -> date:
    candidates: set[date] = set()
    if explicit is not None:
        candidates.add(_normalise_date(explicit))
    for frame_name, frame in frames:
        if TRADE_DATE not in frame.columns or frame.empty:
            continue
        candidates.update(frame[TRADE_DATE].dropna().tolist())
    if not candidates:
        raise M5ObservationError("M5_TRADE_DATE_MISSING", "trade_date must be supplied or present in an input")
    if len(candidates) != 1:
        raise M5ObservationError(
            "M5_TRADE_DATE_MISMATCH",
            f"inputs contain multiple trade dates: {sorted(str(item) for item in candidates)}",
        )
    return next(iter(candidates))


def _filter_to_target_date(frame: pd.DataFrame, target_date: date, *, frame_name: str) -> pd.DataFrame:
    result = frame.copy(deep=True)
    if TRADE_DATE not in result.columns:
        result[TRADE_DATE] = target_date
        return result
    if result.empty:
        return result
    if (~result[TRADE_DATE].eq(target_date)).any():
        raise M5ObservationError(
            "M5_TRADE_DATE_MISMATCH",
            f"{frame_name} contains a date other than {target_date.isoformat()}",
        )
    return result


def calculate_m5_raw_observations(
    theme_memberships: object,
    popularity_records: object,
    theme_universe: object | None = None,
    *,
    trade_date: date | None = None,
) -> pd.DataFrame:
    """Calculate the five daily M5 Theme popularity facts.

    ``theme_memberships`` is expected to be the already PIT-resolved D-day
    membership set.  ``popularity_records`` contains every complete canonical
    B1 record from both sources.  Membership pairs are deduplicated for the
    denominator and mapping; popularity rows are intentionally never deduplicated.

    The returned frame contains one row for every Theme in ``theme_universe``.
    A Theme with members but no matching popularity record receives zero hot
    facts and a ratio of ``0.0``.  A Theme with zero members receives a NULL
    ratio, preserving the zero-denominator distinction.
    """
    membership_frame = _records_to_frame(
        theme_memberships,
        empty_columns=(THEME_ID, COLLECTION_ID, ASSET_ID, TRADE_DATE),
    )
    popularity_frame = _records_to_frame(
        popularity_records,
        empty_columns=(TICKER, SOURCE, TRADE_DATE),
    )
    membership_frame = _normalise_date_column(membership_frame, frame_name="theme_memberships")
    popularity_frame = _normalise_date_column(popularity_frame, frame_name="popularity_records")
    universe = _resolve_theme_universe(theme_universe, membership_frame)
    universe = _normalise_date_column(universe, frame_name="theme_universe")

    target_date = _resolve_target_date(
        trade_date,
        ("theme_memberships", membership_frame),
        ("popularity_records", popularity_frame),
        ("theme_universe", universe),
    )
    membership_frame = _filter_to_target_date(membership_frame, target_date, frame_name="theme_memberships")
    popularity_frame = _filter_to_target_date(popularity_frame, target_date, frame_name="popularity_records")
    universe = _filter_to_target_date(universe, target_date, frame_name="theme_universe")

    if THEME_ID not in membership_frame.columns:
        membership_frame = membership_frame.merge(
            universe[[THEME_ID, COLLECTION_ID]],
            on=COLLECTION_ID,
            how="left",
            validate="many_to_one",
        )
    else:
        if COLLECTION_ID not in membership_frame.columns:
            membership_frame = membership_frame.merge(
                universe[[THEME_ID, COLLECTION_ID]],
                on=THEME_ID,
                how="left",
                validate="many_to_one",
            )
        else:
            expected = universe[[THEME_ID, COLLECTION_ID]]
            membership_frame = membership_frame.merge(
                expected,
                on=[THEME_ID, COLLECTION_ID],
                how="inner",
                validate="many_to_one",
            )

    if "is_theme_member" in membership_frame.columns:
        membership_frame = membership_frame[membership_frame["is_theme_member"].fillna(False).astype(bool)].copy()

    if not membership_frame.empty:
        if ASSET_ID not in membership_frame.columns:
            if TICKER in membership_frame.columns:
                membership_frame[ASSET_ID] = membership_frame[TICKER]
            else:
                raise M5ObservationError("M5_MEMBERSHIP_COLUMNS_MISSING", f"missing {ASSET_ID}")
        membership_frame["_asset_id_norm"] = _normalise_asset_series(
            membership_frame[ASSET_ID], frame_name="theme_memberships"
        )
        membership_frame = membership_frame.drop_duplicates(
            subset=[THEME_ID, COLLECTION_ID, "_asset_id_norm"],
            keep="first",
        )
    else:
        membership_frame = membership_frame.copy()
        membership_frame["_asset_id_norm"] = pd.Series(dtype="string")

    if not popularity_frame.empty:
        ticker_column = TICKER if TICKER in popularity_frame.columns else ASSET_ID
        if ticker_column not in popularity_frame.columns:
            raise M5ObservationError("M5_POPULARITY_COLUMNS_MISSING", f"missing {TICKER}")
        if SOURCE not in popularity_frame.columns:
            raise M5ObservationError("M5_POPULARITY_COLUMNS_MISSING", f"missing {SOURCE}")
        if popularity_frame[SOURCE].isna().any() or popularity_frame[SOURCE].astype(str).str.strip().eq("").any():
            raise M5ObservationError("M5_POPULARITY_SOURCE_INVALID", "source contains NULL or empty values")
        popularity_frame["_asset_id_norm"] = _normalise_asset_series(
            popularity_frame[ticker_column], frame_name="popularity_records"
        )
    else:
        popularity_frame = popularity_frame.copy()
        if SOURCE not in popularity_frame.columns:
            popularity_frame[SOURCE] = pd.Series(dtype="string")
        popularity_frame["_asset_id_norm"] = pd.Series(dtype="string")

    # The map is the only many-to-many expansion: a single popularity record
    # joins once for every Theme that contains its ticker.
    mapped = popularity_frame.merge(
        membership_frame[[THEME_ID, COLLECTION_ID, "_asset_id_norm"]],
        on="_asset_id_norm",
        how="inner",
        sort=False,
    )

    member_counts = (
        membership_frame.groupby([THEME_ID, COLLECTION_ID], sort=False)["_asset_id_norm"]
        .nunique()
        .rename(THEME_MEMBER_COUNT)
        .reset_index()
        if not membership_frame.empty
        else pd.DataFrame(columns=[THEME_ID, COLLECTION_ID, THEME_MEMBER_COUNT])
    )
    if mapped.empty:
        hot_counts = pd.DataFrame(columns=[THEME_ID, COLLECTION_ID, THEME_HOT_STOCK_COUNT])
        appearance_counts = pd.DataFrame(columns=[THEME_ID, COLLECTION_ID, THEME_HOT_LIST_APPEARANCE_COUNT])
        source_counts = pd.DataFrame(columns=[THEME_ID, COLLECTION_ID, THEME_HOT_SOURCE_COUNT])
    else:
        hot_counts = (
            mapped.groupby([THEME_ID, COLLECTION_ID], sort=False)["_asset_id_norm"]
            .nunique()
            .rename(THEME_HOT_STOCK_COUNT)
            .reset_index()
        )
        appearance_counts = (
            mapped.groupby([THEME_ID, COLLECTION_ID], sort=False)
            .size()
            .rename(THEME_HOT_LIST_APPEARANCE_COUNT)
            .reset_index()
        )
        source_counts = (
            mapped.groupby([THEME_ID, COLLECTION_ID], sort=False)[SOURCE]
            .nunique()
            .rename(THEME_HOT_SOURCE_COUNT)
            .reset_index()
        )

    result = universe[[THEME_ID, COLLECTION_ID]].copy(deep=True)
    for aggregate in (member_counts, hot_counts, appearance_counts, source_counts):
        result = result.merge(
            aggregate,
            on=[THEME_ID, COLLECTION_ID],
            how="left",
            validate="one_to_one",
        )
    count_columns = (
        THEME_MEMBER_COUNT,
        THEME_HOT_STOCK_COUNT,
        THEME_HOT_LIST_APPEARANCE_COUNT,
        THEME_HOT_SOURCE_COUNT,
    )
    for column in count_columns:
        result[column] = result[column].fillna(0).astype("int64")
    ratios = []
    for member_count, hot_count in zip(result[THEME_MEMBER_COUNT], result[THEME_HOT_STOCK_COUNT]):
        ratios.append(None if int(member_count) == 0 else float(hot_count) / int(member_count))
    result[THEME_HOT_STOCK_RATIO] = pd.Series(ratios, index=result.index, dtype="object")
    result[TRADE_DATE] = target_date
    result = result[list(_OUTPUT_COLUMNS)].sort_values([THEME_ID, COLLECTION_ID]).reset_index(drop=True)
    return result


calculate_m5_observations = calculate_m5_raw_observations


__all__ = [
    "M5ObservationError",
    "calculate_m5_observations",
    "calculate_m5_raw_observations",
]
