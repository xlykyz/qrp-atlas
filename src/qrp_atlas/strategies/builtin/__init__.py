"""Built-in QRP strategy implementations."""

from .classic import (
    DonchianBreakoutStrategy,
    DualSmaTrendStrategy,
    RollingZscoreMeanReversionStrategy,
    TimeSeriesMomentumStrategy,
)
from .residual import MarketResidualMeanReversionStrategy
from .cross_section import (
    CrossSectionalMomentumLongOnlyStrategy,
    MultifactorLongOnlyStrategy,
    compute_composite_score,
)
from .system_b_basic import SystemBBasicStrategy
from .event_drift import EventDriftBasicStrategy

__all__ = [
    "CrossSectionalMomentumLongOnlyStrategy",
    "DonchianBreakoutStrategy",
    "DualSmaTrendStrategy",
    "MarketResidualMeanReversionStrategy",
    "MultifactorLongOnlyStrategy",
    "RollingZscoreMeanReversionStrategy",
    "SystemBBasicStrategy",
    "EventDriftBasicStrategy",
    "TimeSeriesMomentumStrategy",
    "compute_composite_score",
]
