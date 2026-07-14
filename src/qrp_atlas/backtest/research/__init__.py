"""Cross-sectional research analytics: labels, IC, groups, exposures.

Architecture boundary:

```text
indicators: factors available at signal time
strategies: selection and target decisions
backtest.research: post-hoc evaluation using future outcomes
```

Future returns, IC, and group performance never feed selection logic.
"""

from __future__ import annotations

from .exposures import TargetExposureResult, analyze_target_exposures
from .forward_returns import (
    DEFAULT_FORWARD_HORIZONS,
    ForwardReturnError,
    compute_forward_returns,
    forward_return_column,
)
from .groups import (
    GroupReturnResult,
    assign_factor_groups,
    compute_group_returns,
)
from .ic import (
    ICSummaryResult,
    compute_information_coefficient,
    summarize_information_coefficient,
)
from .pipeline import (
    CrossSectionResearchError,
    CrossSectionResearchResult,
    run_cross_section_research,
)
from .event_study import (
    DEFAULT_EVENT_HORIZONS,
    EarningsForecastEventStudyResult,
    EventStudyError,
    compute_event_forward_returns,
    event_forward_return_column,
    run_earnings_forecast_event_study,
    summarize_event_groups,
)

__all__ = [
    "DEFAULT_FORWARD_HORIZONS",
    "CrossSectionResearchError",
    "CrossSectionResearchResult",
    "ForwardReturnError",
    "GroupReturnResult",
    "ICSummaryResult",
    "TargetExposureResult",
    "analyze_target_exposures",
    "assign_factor_groups",
    "compute_forward_returns",
    "compute_group_returns",
    "compute_information_coefficient",
    "forward_return_column",
    "run_cross_section_research",
    "summarize_information_coefficient",
    "DEFAULT_EVENT_HORIZONS",
    "EarningsForecastEventStudyResult",
    "EventStudyError",
    "compute_event_forward_returns",
    "event_forward_return_column",
    "run_earnings_forecast_event_study",
    "summarize_event_groups",
]
