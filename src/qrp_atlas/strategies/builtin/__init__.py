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

__all__ = [
    "CrossSectionalMomentumLongOnlyStrategy",
    "DonchianBreakoutStrategy",
    "DualSmaTrendStrategy",
    "MarketResidualMeanReversionStrategy",
    "MultifactorLongOnlyStrategy",
    "RollingZscoreMeanReversionStrategy",
    "SystemBBasicStrategy",
    "TimeSeriesMomentumStrategy",
    "compute_composite_score",
]
