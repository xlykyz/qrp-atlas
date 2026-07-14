"""Built-in QRP strategy implementations."""

from .classic import (
    DonchianBreakoutStrategy,
    DualSmaTrendStrategy,
    RollingZscoreMeanReversionStrategy,
    TimeSeriesMomentumStrategy,
)
from .residual import MarketResidualMeanReversionStrategy
from .technical import (
    AdxDirectionalTrendStrategy,
    AtrVolatilityBreakoutStrategy,
    BollingerMeanReversionStrategy,
    DualEmaTrendStrategy,
    KeltnerBreakoutStrategy,
    LinearRegressionTrendStrategy,
    MacdTrendStrategy,
    RsiMeanReversionStrategy,
    StochasticMeanReversionStrategy,
    VolatilityAdjustedMomentumStrategy,
    VolumeConfirmedEmaTrendStrategy,
)
from .cross_section import (
    CrossSectionalMomentumLongOnlyStrategy,
    MultifactorLongOnlyStrategy,
    compute_composite_score,
)
from .system_b_basic import SystemBBasicStrategy
from .event_drift import EventDriftBasicStrategy

__all__ = [
    "AdxDirectionalTrendStrategy",
    "AtrVolatilityBreakoutStrategy",
    "BollingerMeanReversionStrategy",
    "CrossSectionalMomentumLongOnlyStrategy",
    "DonchianBreakoutStrategy",
    "DualEmaTrendStrategy",
    "DualSmaTrendStrategy",
    "EventDriftBasicStrategy",
    "KeltnerBreakoutStrategy",
    "LinearRegressionTrendStrategy",
    "MacdTrendStrategy",
    "MarketResidualMeanReversionStrategy",
    "MultifactorLongOnlyStrategy",
    "RollingZscoreMeanReversionStrategy",
    "RsiMeanReversionStrategy",
    "StochasticMeanReversionStrategy",
    "SystemBBasicStrategy",
    "TimeSeriesMomentumStrategy",
    "VolatilityAdjustedMomentumStrategy",
    "VolumeConfirmedEmaTrendStrategy",
    "compute_composite_score",
]