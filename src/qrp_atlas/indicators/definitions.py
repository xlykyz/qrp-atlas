"""Definitions for trading-system indicators.

The classes in this module describe what an indicator is. They do not define
how an indicator value is calculated.
"""

from dataclasses import dataclass
from enum import Enum


class IndicatorLayer(str, Enum):
    """Logical layer that owns an indicator."""

    BASIC = "basic"
    JUDGMENT = "judgment"
    SYSTEM_A = "system_a"
    SYSTEM_B = "system_b"
    RISK = "risk"
    REVIEW = "review"


class IndicatorScope(str, Enum):
    """Object scope that an indicator applies to."""

    STOCK = "stock"
    INDEX = "index"
    SECTOR = "sector"
    MARKET = "market"
    ACCOUNT = "account"
    TRADE = "trade"


class UpdateFrequency(str, Enum):
    """When an indicator is expected to update."""

    INTRADAY = "intraday"
    AFTER_CLOSE = "after_close"
    REALTIME = "realtime"
    MANUAL = "manual"


@dataclass(frozen=True)
class IndicatorDefinition:
    """Machine-readable metadata for one trading-system indicator."""

    code: str
    name: str
    layer: IndicatorLayer
    scope: IndicatorScope
    frequency: UpdateFrequency
    description: str
    unit: str | None = None
    allow_intraday_decision: bool = False
    is_veto_indicator: bool = False
    required: bool = True
