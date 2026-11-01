"""Pure core normalization and an independently callable acquisition chain."""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from qrp_atlas.contracts import align_to_schema, quick_validate
from qrp_atlas.contracts.conventions import normalize_ticker, derive_limit_flags
from qrp_atlas.contracts.lake_core import CORE_DATASETS
from qrp_atlas.contracts.schema import get_table
from qrp_atlas.pipeline.daily_basic.clean import clean_daily_basic
from qrp_atlas.pipeline.daily_update.clean import clean_daily_snapshot


class CoreQualityError(ValueError):
    """A candidate failed its data contract and must not be published."""


_UNSET = object()


def _dates(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values.astype("string"), errors="raise").dt.date


def _strict_numbers(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column not in frame:
            continue
        numeric = pd.to_numeric(frame[column], errors="raise")
        if (~np.isfinite(numeric.dropna().to_numpy(dtype=float))).any():
            raise CoreQualityError(f"NONFINITE_VALUE:{column}")
        frame[column] = numeric


def normalize_core(dataset: str, raw: pd.DataFrame, *, target_date: date,
                   observed_at: datetime, complete: bool,
                   expected_tickers: Iterable[str] | None = None,
                   is_open: bool | None = None) -> pd.DataFrame:
    """Normalize only a complete batch, never silently drop malformed rows.

    Current stock reference snapshots are observed facts, not retrospectively
    known identities. Market snapshots contain actual provider bars; missing
    bars are never filled from the previous day here.
    """
    spec = CORE_DATASETS[dataset]
    if not complete:
        raise CoreQualityError("INCOMPLETE_RAW_BATCH")
    if observed_at.tzinfo is None:
        raise CoreQualityError("OBSERVED_AT_REQUIRES_TIMEZONE")
    if dataset == "stock_info" and target_date != observed_at.astimezone(ZoneInfo("Asia/Shanghai")).date():
        raise CoreQualityError("CURRENT_REFERENCE_CANNOT_BACKDATE")
    frame = raw.copy(deep=True)
    if frame.empty:
        legal = spec.allow_empty or (is_open is False and spec.date_field == "trade_date")
        if not legal:
            raise CoreQualityError("UNEXPECTED_EMPTY_RESPONSE")
        return pd.DataFrame(columns=spec.schema.column_names())
    missing = set(spec.required_provider_fields) - set(frame)
    if missing:
        raise CoreQualityError("PROVIDER_FIELDS_MISSING:" + ",".join(sorted(missing)))
    if frame[list(spec.provider_key)].isna().any().any():
        raise CoreQualityError("NULL_BUSINESS_KEY")
    if frame.duplicated(list(spec.provider_key)).any():
        raise CoreQualityError("DUPLICATE_BUSINESS_KEY")
    if spec.date_field:
        actual = _dates(frame[spec.date_field])
        if actual.isna().any() or not actual.eq(target_date).all():
            raise CoreQualityError("TARGET_DATE_MISMATCH")
        if is_open is False and spec.date_field == "trade_date":
            raise CoreQualityError("ROWS_ON_CLOSED_DATE")
    if "ts_code" in frame and dataset != "stock_info":
        if not frame.ts_code.astype("string").str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all():
            raise CoreQualityError("INVALID_TICKER")
        frame["ts_code"] = frame.ts_code.map(normalize_ticker)
    if dataset == "daily_market_snapshot":
        _strict_numbers(frame, ("open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"))
        if frame[["open", "high", "low", "close", "vol", "amount"]].isna().any().any():
            raise CoreQualityError("MISSING_MARKET_VALUES")
        if (frame[["open", "high", "low", "close"]] <= 0).any().any():
            raise CoreQualityError("NONPOSITIVE_PRICE")
        if (frame[["vol", "amount"]] < 0).any().any():
            raise CoreQualityError("NEGATIVE_ACTIVITY")
        if ((frame.high < frame[["open", "close", "low"]].max(axis=1)) |
                (frame.low > frame[["open", "close", "high"]].min(axis=1))).any():
            raise CoreQualityError("INVALID_OHLC_RANGE")
        frame = clean_daily_snapshot(frame, source="tushare_daily")
    elif dataset == "daily_basic":
        numeric = {column.name for column in spec.schema.columns if column.dtype in ("DOUBLE", "INTEGER")}
        _strict_numbers(frame, numeric | {"pe", "total_share"})
        if frame.close.isna().any() or (frame.close <= 0).any():
            raise CoreQualityError("INVALID_BASIC_CLOSE")
        frame = clean_daily_basic(frame)
    elif dataset == "trading_calendar":
        if not frame.is_open.isin([0, 1, False, True, "0", "1"]).all():
            raise CoreQualityError("INVALID_OPEN_FLAG")
        frame = pd.DataFrame({"trade_date": _dates(frame.cal_date),
                              "is_open": frame.is_open.astype(int).astype(bool)})
        frame["year"], frame["month"], frame["quarter"] = target_date.year, target_date.month, (target_date.month - 1) // 3 + 1
    elif dataset == "stock_info":
        frame = _stock_reference(frame, observed_at)
    else:
        frame = frame.rename(columns={"ts_code": "ticker"})
        frame["trade_date"] = _dates(frame.trade_date)
        if dataset == "adj_factor_changes":
            _strict_numbers(frame, ("adj_factor",))
            if frame.adj_factor.isna().any() or (frame.adj_factor <= 0).any():
                raise CoreQualityError("INVALID_ADJUSTMENT_FACTOR")
        elif frame.suspend_type.astype("string").str.strip().eq("").any():
            raise CoreQualityError("INVALID_SUSPENSION_TYPE")
    if expected_tickers is not None:
        expected = {normalize_ticker(item) for item in expected_tickers}
        if "ticker" not in frame or expected - set(frame.ticker):
            raise CoreQualityError("INCOMPLETE_TICKER_COVERAGE")
    # Freeze ingestion timestamps from raw evidence, not each retry's wall clock.
    for column in ("created_at", "updated_at"):
        if column in spec.schema.column_names():
            frame[column] = pd.Timestamp(observed_at).tz_convert("UTC").tz_localize(None)
    frame = align_to_schema(frame, dataset, fill_missing_optional=True, drop_extra=True)
    frame = quick_validate(frame, dataset, allow_extra=False)
    if frame.duplicated(list(spec.schema.primary_key)).any():
        raise CoreQualityError("NORMALIZED_KEY_COLLISION")
    return frame.sort_values(list(spec.schema.primary_key)).reset_index(drop=True)


def _stock_reference(frame: pd.DataFrame, observed_at: datetime) -> pd.DataFrame:
    frame = frame.copy()
    for column in CORE_DATASETS["stock_info"].required_provider_fields:
        frame[column] = frame[column].astype("string").str.strip().replace("", pd.NA)
    if frame[["ts_code", "exchange", "list_status"]].isna().any().any():
        raise CoreQualityError("STOCK_IDENTITY_MISSING")
    if not frame.list_status.isin(("L", "D", "P", "G")).all():
        raise CoreQualityError("UNSUPPORTED_LIST_STATUS")
    suffix = frame.exchange.map({"SSE": ".SH", "SZSE": ".SZ", "BSE": ".BJ"})
    if suffix.isna().any():
        raise CoreQualityError("UNSUPPORTED_EXCHANGE")
    standard = frame.ts_code.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)", na=False)
    if ((~standard) & frame.list_status.ne("D")).any():
        raise CoreQualityError("NONSTANDARD_ACTIVE_IDENTITY")
    if (frame.ts_code.str.contains(".", regex=False) & frame.ts_code.str[-3:].ne(suffix)).any():
        raise CoreQualityError("STOCK_EXCHANGE_CONFLICT")
    # Nonstandard historical identities remain in raw and in the companion
    # output produced by normalize_stock_identities(), never coerced to ticker.
    frame = frame.loc[standard].copy()
    if frame.empty:
        raise CoreQualityError("NO_STANDARD_STOCK_IDENTITIES")
    for column in ("list_date", "delist_date"):
        frame[column] = _dates(frame[column])
    frame["ticker"] = frame.ts_code
    frame["is_active"] = frame.list_status.eq("L")
    frame["updated_at"] = pd.Timestamp(observed_at).tz_convert("UTC").tz_localize(None)
    return frame


def normalize_stock_identities(raw: pd.DataFrame, *, target_date: date,
                               observed_at: datetime, complete: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    standard = normalize_core("stock_info", raw, target_date=target_date,
                              observed_at=observed_at, complete=complete)
    historical = raw.loc[~raw.ts_code.astype("string").str.fullmatch(r"\d{6}\.(SH|SZ|BJ)", na=False)].copy()
    table = "stock_info_historical_identity"
    historical["provider"] = "tushare"
    historical["identity_type"] = "provider_historical_id"
    historical["standard_ticker"] = None
    historical["isolation_reason"] = "non_standard_provider_identity"
    historical["snapshot_date"] = target_date
    historical["captured_at"] = pd.Timestamp(observed_at).tz_convert("UTC").tz_localize(None)
    for column in ("list_date", "delist_date"):
        historical[column] = _dates(historical[column].astype("string").str.strip().replace("", pd.NA))
    historical = align_to_schema(historical, table, fill_missing_optional=True, drop_extra=True)
    return standard, quick_validate(historical, table, allow_extra=False)


def enrich_market(frame: pd.DataFrame, stock_info: pd.DataFrame, target_date: date) -> pd.DataFrame:
    """Prepare names and existing limit rules from a pinned reference frame."""
    result = frame.copy()
    if stock_info.ticker.duplicated().any():
        raise CoreQualityError("REFERENCE_KEY_DUPLICATE")
    names = stock_info.set_index("ticker")["name"]
    result["name"] = result.ticker.map(names)
    if result.name.isna().any():
        raise CoreQualityError("STOCK_REFERENCE_COVERAGE_MISSING")
    result["is_st"] = result.name.astype("string").str.contains("ST", case=False, na=False)
    return derive_limit_flags(result, trade_date=target_date)


CORE_INPUTS = {
    "trading_calendar": (), "stock_info": (),
    "suspend_d": ("trading_calendar",),
    "daily_market_snapshot": ("trading_calendar", "stock_info", "suspend_d"),
    "daily_basic": ("trading_calendar", "daily_market_snapshot"),
    "adj_factor_changes": ("trading_calendar", "daily_market_snapshot"),
}


def _input_scope(dataset: str, target: date, inputs: dict[str, pd.DataFrame],
                 lineage: dict[str, Any]) -> tuple[bool | None, set[str] | None]:
    required = CORE_INPUTS[dataset]
    snapshots = lineage.get("input_snapshots", {})
    if any(name not in inputs or name not in snapshots for name in required):
        raise CoreQualityError("PINNED_INPUTS_REQUIRED")
    if not required:
        return None, None
    calendar = inputs["trading_calendar"]
    row = calendar.loc[_dates(calendar.trade_date).eq(target)]
    if len(row) != 1 or pd.isna(row.iloc[0].is_open):
        raise CoreQualityError("TARGET_CALENDAR_MISSING")
    opened = bool(row.iloc[0].is_open)
    if dataset == "daily_market_snapshot":
        stock = inputs["stock_info"]
        if stock.ticker.duplicated().any() or stock.list_date.isna().any():
            raise CoreQualityError("STOCK_REFERENCE_INVALID")
        listed = _dates(stock.list_date) <= target
        delisted = _dates(stock.delist_date)
        still_listed = delisted.map(lambda value: pd.isna(value) or value >= target)
        eligible = set(stock.loc[listed & still_listed, "ticker"])
        suspensions = inputs["suspend_d"]
        if not suspensions.empty and not _dates(suspensions.trade_date).eq(target).all():
            raise CoreQualityError("SUSPENSION_TARGET_MISMATCH")
        # Mirror existing canonical-series semantics: explicit resumption is
        # not a non-trading proof. Partial-day suspensions do not justify an
        # absent whole-day market bar.
        suspended = set()
        for item in suspensions.itertuples(index=False):
            kind = str(item.suspend_type).upper()
            timing = getattr(item, "suspend_timing", None)
            if kind in {"S", "停牌"} and (pd.isna(timing) or str(timing).strip() in {"", "全天"}):
                suspended.add(item.ticker)
        return opened, eligible - suspended
    if dataset in {"daily_basic", "adj_factor_changes"}:
        market = inputs["daily_market_snapshot"]
        if opened and market.empty:
            raise CoreQualityError("UPSTREAM_MARKET_EMPTY")
        if not market.empty and not _dates(market.trade_date).eq(target).all():
            raise CoreQualityError("UPSTREAM_MARKET_STALE")
        return opened, set(market.ticker)
    return opened, None


class CoreRunner:
    """Acquire, preserve, normalize and publish without a legacy writer lease."""
    def __init__(self, raw_store: Any, publisher: Any, provider: Any = None) -> None:
        self.raw_store, self.publisher, self.provider = raw_store, publisher, provider

    def replay(self, dataset: str, batch_id: str, *, target_date: date,
               expected_tickers: Iterable[str] | None = None, is_open: bool | None = None,
               lineage: dict[str, Any] | None = None,
               inputs: dict[str, pd.DataFrame] | None = None,
               expected_previous: str | None = None,
               expected_historical_previous: Any = _UNSET) -> Any:
        if dataset == "stock_info" and expected_historical_previous is _UNSET:
            expected_historical_previous = self.publisher.current_publication(
                "stock_info_historical_identity", target_date.isoformat(), whole_table=True)
        batch = self.raw_store.metadata(batch_id)
        if batch.dataset != dataset:
            raise CoreQualityError("RAW_DATASET_MISMATCH")
        observed = datetime.fromisoformat(batch.observed_at) if isinstance(batch.observed_at, str) else batch.observed_at
        raw = self.raw_store.load_frame(batch_id)
        spec = CORE_DATASETS[dataset]
        lineage = dict(lineage or {})
        inputs = inputs or {}
        opened, expected = _input_scope(dataset, target_date, inputs, lineage)
        if opened is not None:
            if is_open is not None and opened != is_open:
                raise CoreQualityError("CALENDAR_ARGUMENT_CONFLICT")
            is_open = opened
        if expected is not None:
            expected_tickers = expected | set(expected_tickers or ())
        frame = normalize_core(dataset, raw, target_date=target_date,
                               observed_at=observed, complete=batch.complete,
                               expected_tickers=expected_tickers, is_open=is_open)
        if dataset == "daily_market_snapshot" and not frame.empty:
            frame = enrich_market(frame, inputs["stock_info"], target_date)
        evidence = dict(lineage, raw_batch_id=batch_id, producer_version="lake-core-v1",
                        semantics="complete factor snapshot" if dataset == "adj_factor_changes" else "provider normalized")
        lineage_hash = hashlib.sha256(json.dumps(lineage, sort_keys=True, default=str).encode()).hexdigest()
        key = f"core-v1:{dataset}:{target_date}:{batch_id}:{lineage_hash}"
        if dataset == "stock_info":
            frame, historical = normalize_stock_identities(
                raw, target_date=target_date, observed_at=observed, complete=batch.complete)
            companion = self.publisher.publish(
                get_table("stock_info_historical_identity"), historical,
                partition=target_date.isoformat(), partition_column=None,
                idempotency_key=f"{key}:historical-identities",
                lineage=evidence, time_model="observed",
                expected_previous=expected_historical_previous,
                quality="legal_empty" if historical.empty else "complete")
            evidence["historical_identity_publication"] = companion.publication_id
        return self.publisher.publish(
            spec.schema, frame, partition=target_date.isoformat(),
            partition_column="trade_date" if spec.date_field else None,
            idempotency_key=key,
            expected_previous=expected_previous,
            lineage=evidence, time_model="observed", quality="legal_empty" if frame.empty else "complete")

    def acquire_and_publish(self, dataset: str, target_date: date, **kwargs: Any) -> Any:
        if self.provider is None:
            raise ValueError("provider required for acquisition")
        if dataset == "stock_info" and "expected_historical_previous" not in kwargs:
            kwargs["expected_historical_previous"] = self.publisher.current_publication(
                "stock_info_historical_identity", target_date.isoformat(), whole_table=True)
        if dataset == "trading_calendar":
            query = {"exchange": "SSE", "start_date": target_date.strftime("%Y%m%d"),
                     "end_date": target_date.strftime("%Y%m%d")}
        elif dataset == "stock_info":
            query = {}
        else:
            query = {"trade_date": target_date.strftime("%Y%m%d")}
        acquired = self.provider.acquire(dataset, query)
        return self.replay(dataset, acquired.batch_id, target_date=target_date, **kwargs)
