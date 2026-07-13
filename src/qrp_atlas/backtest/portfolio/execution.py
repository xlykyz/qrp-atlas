"""Low-level A-share execution helpers."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from .models import (
    PortfolioBacktestConfig,
    REASON_LIMIT_DOWN_SELL_BLOCKED,
    REASON_LIMIT_UP_BUY_BLOCKED,
    REASON_SUSPENDED,
)


def iso_date(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def safe_price(value: Any) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price > 0 else None


def flag(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def is_suspended(row: pd.Series) -> bool:
    if flag(row.get("is_suspended")):
        return True
    suspend_type = row.get("suspend_type")
    if suspend_type is None:
        return False
    try:
        if pd.isna(suspend_type):
            return False
    except (TypeError, ValueError):
        pass
    return str(suspend_type).strip() != ""


def round_lot(quantity: float, lot_size: int) -> int:
    return 0 if quantity <= 0 else int(math.floor(quantity / lot_size + 1e-12) * lot_size)


def commission(gross_amount: float, config: PortfolioBacktestConfig) -> float:
    if gross_amount <= 0:
        return 0.0
    return max(
        config.execution.minimum_commission,
        gross_amount * config.cost.commission_rate,
    )


def execution_rejection(
    row: pd.Series,
    side: str,
    config: PortfolioBacktestConfig,
) -> str | None:
    if config.execution.enforce_suspension and is_suspended(row):
        return REASON_SUSPENDED
    if not config.execution.enforce_price_limits:
        return None
    if side == "BUY" and flag(row.get("is_limit_up")):
        return REASON_LIMIT_UP_BUY_BLOCKED
    if side == "SELL" and flag(row.get("is_limit_down")):
        return REASON_LIMIT_DOWN_SELL_BLOCKED
    return None


def affordable_quantity(
    requested_quantity: int,
    execution_price: float,
    cash: float,
    config: PortfolioBacktestConfig,
) -> int:
    quantity = round_lot(requested_quantity, config.execution.lot_size)
    while quantity > 0:
        gross = execution_price * quantity
        if gross + commission(gross, config) <= cash + 1e-9:
            return quantity
        quantity -= config.execution.lot_size
    return 0
