"""
validators.py - 回测输入校验

校验原则:
- 结构性错误（缺列、空 config、非法 timing/type 等）raise ValueError，由调用方处理。
- 单条信号无法交易（asset 不存在、缺未来 bar、价格非法等）不在这里抛错，
  而是由 broker 在执行时记录 skipped。
- 空 DataFrame 不视为结构性错误，允许通过（engine 会返回空 BacktestResult）。
"""

from typing import Iterable

import pandas as pd

from .models import BacktestConfig

PRICE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "trade_date",
    "asset_id",
    "asset_name",
    "asset_type",
    "open",
    "high",
    "low",
    "close",
)

PRICE_OPTIONAL_COLUMNS: tuple[str, ...] = (
    "volume",
    "amount",
    "turnover",
    "market_cap",
    "float_cap",
    "is_st",
    "is_limit_up",
    "is_limit_down",
)

SIGNALS_REQUIRED_COLUMNS: tuple[str, ...] = (
    "signal_date",
    "asset_id",
    "direction",
)

SIGNALS_OPTIONAL_COLUMNS: tuple[str, ...] = (
    "asset_type",
    "signal_name",
    "score",
    "weight",
    "meta",
)

VALID_ENTRY_TIMINGS: frozenset[str] = frozenset(
    {"signal_close", "next_open", "next_close"}
)
VALID_EXIT_TYPES: frozenset[str] = frozenset({"hold_n_bars"})
VALID_DIRECTIONS: frozenset[str] = frozenset({"long", "short"})


def _missing_columns(df_columns: Iterable[str], required: Iterable[str]) -> list[str]:
    present = set(df_columns)
    return [col for col in required if col not in present]


def validate_price_df(price_df: pd.DataFrame) -> None:
    """校验行情 DataFrame 的结构。

    Args:
        price_df: 标准 PriceFrame。

    Raises:
        ValueError: 缺少必需列、非 DataFrame、asset_id+trade_date 重复行等结构性错误。
    """
    if not isinstance(price_df, pd.DataFrame):
        raise ValueError("price_df must be a pandas DataFrame")

    missing = _missing_columns(price_df.columns, PRICE_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"price_df missing required columns: {missing}")

    if len(price_df) > 0:
        dup_mask = price_df.duplicated(subset=["asset_id", "trade_date"], keep=False)
        dup_count = int(dup_mask.sum())
        if dup_count > 0:
            raise ValueError(
                f"price_df has {dup_count} rows with duplicate (asset_id, trade_date) pairs"
            )


def validate_signals_df(signals_df: pd.DataFrame) -> None:
    """校验信号 DataFrame 的结构。

    Args:
        signals_df: 标准 SignalFrame。

    Raises:
        ValueError: 缺少必需列、非 DataFrame 等结构性错误。
    """
    if not isinstance(signals_df, pd.DataFrame):
        raise ValueError("signals_df must be a pandas DataFrame")

    missing = _missing_columns(signals_df.columns, SIGNALS_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"signals_df missing required columns: {missing}")


def validate_config(config: BacktestConfig) -> None:
    """校验 BacktestConfig 的合法性。

    Args:
        config: 回测总配置。

    Raises:
        ValueError: 字段非法（timing / exit type / bars 等）。
    """
    if not isinstance(config, BacktestConfig):
        raise ValueError("config must be a BacktestConfig instance")

    if config.entry.timing not in VALID_ENTRY_TIMINGS:
        raise ValueError(
            f"entry.timing must be one of {sorted(VALID_ENTRY_TIMINGS)}, "
            f"got: {config.entry.timing!r}"
        )

    if not config.entry.price_field:
        raise ValueError("entry.price_field must be non-empty")

    if config.exit.type not in VALID_EXIT_TYPES:
        raise ValueError(
            f"exit.type must be one of {sorted(VALID_EXIT_TYPES)}, "
            f"got: {config.exit.type!r}"
        )

    if not isinstance(config.exit.bars, int) or config.exit.bars <= 0:
        raise ValueError(f"exit.bars must be a positive int, got: {config.exit.bars!r}")

    if not config.exit.price_field:
        raise ValueError("exit.price_field must be non-empty")

    if config.position.initial_cash <= 0:
        raise ValueError(
            f"position.initial_cash must be positive, got: {config.position.initial_cash}"
        )

    if not (0 < config.position.position_pct <= 1.0 + 1e-9):
        raise ValueError(
            f"position.position_pct must be in (0, 1], got: {config.position.position_pct}"
        )

    if config.position.max_positions <= 0:
        raise ValueError(
            f"position.max_positions must be positive, got: {config.position.max_positions}"
        )

    if config.cost.commission_rate < 0:
        raise ValueError(
            f"cost.commission_rate must be >= 0, got: {config.cost.commission_rate}"
        )

    if config.cost.stamp_tax_rate < 0:
        raise ValueError(
            f"cost.stamp_tax_rate must be >= 0, got: {config.cost.stamp_tax_rate}"
        )

    if config.cost.slippage_bps < 0:
        raise ValueError(
            f"cost.slippage_bps must be >= 0, got: {config.cost.slippage_bps}"
        )
