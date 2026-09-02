"""M4 indicators package."""

from .observations import (
    M4ObservationCalculationError,
    calculate_m4_raw_observations,
)

__all__ = [
    "calculate_m4_raw_observations",
    "M4ObservationCalculationError",
]
