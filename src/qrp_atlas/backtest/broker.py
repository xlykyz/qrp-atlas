"""
broker.py - 单笔交易撮合

职责:
- build_price_index: 按 asset_id 建立行情索引，方便按日期定位 bar。
- simulate_signal: 把一条信号变成 Trade 或 Skipped。

引擎不依赖任何具体策略概念，只认识: 资产 / 日期 / 价格 / 入场 / 出场 / 成本 / MAE / MFE。

约定:
- 入场 timing:
    signal_close: 信号日当根 bar 入场，取该 bar 的 price_field
    next_open:    信号日后的下一根 bar 入场
    next_close:   信号日后的下一根 bar 入场
- 出场 type:
    hold_n_bars:  入场 bar 之后第 N 根 bar 出场（holding_bars = N）
- MAE / MFE 区间: [entry_bar, exit_bar]（含两端）。
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd

from .models import BacktestConfig, Skipped, Trade

REASON_NO_PRICE_DATA = "NO_PRICE_DATA"
REASON_SIGNAL_DATE_NOT_FOUND = "SIGNAL_DATE_NOT_FOUND"
REASON_NO_NEXT_BAR_FOR_ENTRY = "NO_NEXT_BAR_FOR_ENTRY"
REASON_NO_EXIT_BAR = "NO_EXIT_BAR"
REASON_INVALID_DIRECTION = "INVALID_DIRECTION"
REASON_INVALID_PRICE = "INVALID_PRICE"
REASON_INVALID_ENTRY_TIMING = "INVALID_ENTRY_TIMING"
REASON_INVALID_EXIT_TYPE = "INVALID_EXIT_TYPE"


@dataclass
class AssetPriceIndex:
    """单个资产的行情索引。

    Attributes:
        asset_id: 资产代码。
        df: 按 trade_date 升序的行情子 DataFrame（trade_date 已转为 Timestamp）。
        date_to_pos: trade_date Timestamp → 在 df 中的行号，便于 O(1) 定位信号日。
    """

    asset_id: str
    df: pd.DataFrame
    date_to_pos: Dict[pd.Timestamp, int]


def build_price_index(price_df: pd.DataFrame) -> Dict[str, AssetPriceIndex]:
    """按 asset_id 建立行情索引。

    Args:
        price_df: 标准 PriceFrame。

    Returns:
        dict[asset_id, AssetPriceIndex]。空输入返回 {}。
    """
    if price_df is None or len(price_df) == 0:
        return {}

    df = price_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["asset_id", "trade_date"])
    if df.empty:
        return {}
    df["asset_id"] = df["asset_id"].astype(str)
    df = df.sort_values(["asset_id", "trade_date"]).reset_index(drop=True)

    index: Dict[str, AssetPriceIndex] = {}
    for asset_id, sub in df.groupby("asset_id", sort=False):
        sub = sub.reset_index(drop=True)
        date_to_pos = {ts: i for i, ts in enumerate(sub["trade_date"].tolist())}
        index[asset_id] = AssetPriceIndex(
            asset_id=asset_id, df=sub, date_to_pos=date_to_pos
        )
    return index


def _to_timestamp(value: Any) -> Optional[pd.Timestamp]:
    """把任意日期输入转成 Timestamp，无法解析返回 None。"""
    if value is None:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts)


def _ts_to_iso(ts: Any) -> Optional[str]:
    """Timestamp / date / str → "YYYY-MM-DD"。无法转换返回 None。"""
    parsed = _to_timestamp(ts)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d")


def _safe_float(value: Any) -> Optional[float]:
    """转 float，None / NaN / 非数 → None。"""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(f):
        return None
    return f


def _is_valid_price(value: Optional[float]) -> bool:
    """价格合法：非 None 且 > 0。"""
    return value is not None and value > 0


def _get_value(signal: Union[pd.Series, Dict[str, Any]], key: str, default: Any = None) -> Any:
    """从 dict / Series 取值，None / NaN 一律返回 default。"""
    if isinstance(signal, dict):
        val = signal.get(key, default)
    else:
        if key in signal.index:
            val = signal[key]
        else:
            return default
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


def _to_clean_str(value: Any) -> Optional[str]:
    """转 str，None / NaN → None。"""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def simulate_signal(
    signal: Union[pd.Series, Dict[str, Any]],
    price_index: Dict[str, AssetPriceIndex],
    config: BacktestConfig,
) -> Union[Trade, Skipped]:
    """模拟单条信号，返回 Trade 或 Skipped。

    单条信号失败不抛异常，返回 Skipped；结构性错误（非法 timing/type）由
    validators 拦截，但这里仍做兜底保护。

    Args:
        signal: 单条信号（dict 或 pandas Series）。
        price_index: build_price_index 的返回结果。
        config: 回测配置。

    Returns:
        Trade 或 Skipped。
    """
    asset_id_raw = _get_value(signal, "asset_id", None)
    asset_id = str(asset_id_raw).strip() if asset_id_raw is not None else None

    direction_raw = _get_value(signal, "direction", "")
    direction = str(direction_raw).strip().lower() if direction_raw != "" else ""

    signal_date_raw = _get_value(signal, "signal_date", None)
    signal_ts = _to_timestamp(signal_date_raw)
    signal_date_iso = _ts_to_iso(signal_ts)

    signal_name = _to_clean_str(_get_value(signal, "signal_name", None))

    meta_raw = _get_value(signal, "meta", None)
    if isinstance(meta_raw, dict):
        meta: Dict[str, Any] = dict(meta_raw)
    else:
        meta = {}

    def skip(reason: str, detail: str) -> Skipped:
        return Skipped(
            asset_id=asset_id,
            signal_date=signal_date_iso,
            reason=reason,
            detail=detail,
        )

    if direction != "long":
        return skip(
            REASON_INVALID_DIRECTION,
            f"direction={direction!r}, only 'long' supported in v0.1",
        )

    if asset_id is None or asset_id in ("", "nan", "None"):
        return skip(
            REASON_NO_PRICE_DATA,
            "asset_id missing in signal",
        )

    if asset_id not in price_index:
        return skip(
            REASON_NO_PRICE_DATA,
            f"asset_id {asset_id!r} not found in price_df",
        )

    api = price_index[asset_id]

    if signal_ts is None or signal_ts not in api.date_to_pos:
        return skip(
            REASON_SIGNAL_DATE_NOT_FOUND,
            f"signal_date={signal_date_iso!r} not in price data for asset {asset_id!r}",
        )

    sig_pos = api.date_to_pos[signal_ts]
    n_bars = len(api.df)

    timing = config.entry.timing
    if timing == "signal_close":
        entry_pos = sig_pos
    elif timing in ("next_open", "next_close"):
        entry_pos = sig_pos + 1
        if entry_pos >= n_bars:
            return skip(
                REASON_NO_NEXT_BAR_FOR_ENTRY,
                f"signal_date={signal_date_iso!r} is the last bar of asset {asset_id!r}, "
                f"no next bar for entry timing={timing!r}",
            )
    else:
        return skip(
            REASON_INVALID_ENTRY_TIMING,
            f"entry.timing={timing!r} not supported",
        )

    entry_row = api.df.iloc[entry_pos]
    entry_price = _safe_float(entry_row.get(config.entry.price_field))
    entry_date = _ts_to_iso(entry_row["trade_date"])

    if config.exit.type == "hold_n_bars":
        exit_pos = entry_pos + config.exit.bars
        if exit_pos >= n_bars:
            return skip(
                REASON_NO_EXIT_BAR,
                f"not enough future bars for hold_n_bars={config.exit.bars}, "
                f"need pos={exit_pos}, have {n_bars}",
            )
    else:
        return skip(
            REASON_INVALID_EXIT_TYPE,
            f"exit.type={config.exit.type!r} not supported",
        )

    exit_row = api.df.iloc[exit_pos]
    exit_price = _safe_float(exit_row.get(config.exit.price_field))
    exit_date = _ts_to_iso(exit_row["trade_date"])

    if not _is_valid_price(entry_price) or not _is_valid_price(exit_price):
        return skip(
            REASON_INVALID_PRICE,
            f"entry_price={entry_price!r}, exit_price={exit_price!r} "
            f"(field={config.entry.price_field!r}/{config.exit.price_field!r})",
        )

    gross_return = exit_price / entry_price - 1.0
    cost = config.cost
    buy_cost = cost.commission_rate + cost.slippage_bps / 10000.0
    sell_cost = (
        cost.commission_rate + cost.stamp_tax_rate + cost.slippage_bps / 10000.0
    )
    net_return = gross_return - buy_cost - sell_cost

    window = api.df.iloc[entry_pos : exit_pos + 1]
    lows = window["low"].to_numpy(dtype=float)
    highs = window["high"].to_numpy(dtype=float)
    mae = float((lows / entry_price).min() - 1.0)
    mfe = float((highs / entry_price).max() - 1.0)

    asset_name = _to_clean_str(_get_value(entry_row, "asset_name", None))
    asset_type = _to_clean_str(_get_value(entry_row, "asset_type", None))

    return Trade(
        asset_id=asset_id,
        asset_name=asset_name,
        asset_type=asset_type,
        signal_date=signal_date_iso if signal_date_iso is not None else "",
        signal_name=signal_name,
        direction="long",
        entry_date=entry_date if entry_date is not None else "",
        entry_price=float(entry_price),
        exit_date=exit_date if exit_date is not None else "",
        exit_price=float(exit_price),
        holding_bars=config.exit.bars,
        gross_return=float(gross_return),
        net_return=float(net_return),
        mae=mae,
        mfe=mfe,
        meta=meta,
    )
