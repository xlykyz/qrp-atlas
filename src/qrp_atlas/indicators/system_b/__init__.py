"""System B indicator capabilities.

Legacy 1.0 detector exports remain unchanged. The versioned 2.0 state machine is
an independent contract and implementation.
"""

from qrp_atlas.indicators.system_b.detector import (
    SYSTEM_B_EXIT_TRIGGERED,
    SYSTEM_B_TREND_VALID,
    calculate_system_b_basic_states,
    calculate_system_b_basic_states_from_prices,
    detect_system_b_basic_state,
    detect_system_b_basic_state_from_prices,
)
from qrp_atlas.indicators.system_b.state_machine_v2 import (
    DIAGNOSTIC_BROKEN_SEQUENCE,
    DIAGNOSTIC_INSUFFICIENT,
    DIAGNOSTIC_INPUT_SORTED,
    DIAGNOSTIC_MISSING_PREVIOUS_ACTUAL,
    DIAGNOSTIC_NON_TRADING_DERIVATION,
    DIAGNOSTIC_NO_UNIQUE_MATCH,
    DIAGNOSTIC_WARMUP,
    SystemBStateMachineError,
    calculate_system_b_2_0_states,
)
from qrp_atlas.indicators.system_b.episode import (
    SystemBEpisodeError,
    SystemBEpisodeResult,
    calculate_system_b_episodes,
)

__all__ = [
    "SYSTEM_B_EXIT_TRIGGERED",
    "SYSTEM_B_TREND_VALID",
    "calculate_system_b_basic_states",
    "calculate_system_b_basic_states_from_prices",
    "detect_system_b_basic_state",
    "detect_system_b_basic_state_from_prices",
    "DIAGNOSTIC_INPUT_SORTED",
    "DIAGNOSTIC_INSUFFICIENT",
    "DIAGNOSTIC_BROKEN_SEQUENCE",
    "DIAGNOSTIC_MISSING_PREVIOUS_ACTUAL",
    "DIAGNOSTIC_NO_UNIQUE_MATCH",
    "DIAGNOSTIC_NON_TRADING_DERIVATION",
    "DIAGNOSTIC_WARMUP",
    "SystemBStateMachineError",
    "calculate_system_b_2_0_states",
    "SystemBEpisodeError",
    "SystemBEpisodeResult",
    "calculate_system_b_episodes",
]
