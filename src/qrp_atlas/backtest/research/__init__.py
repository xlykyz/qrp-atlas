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
from .residual import (
    ResidualResearchError,
    ResidualResearchResult,
    ResidualStrategyBacktestRun,
    run_market_residual_mean_reversion_backtest,
    run_residual_research,
)

__all__ = [
    "DEFAULT_FORWARD_HORIZONS",
    "CrossSectionResearchError",
    "CrossSectionResearchResult",
    "ForwardReturnError",
    "GroupReturnResult",
    "ICSummaryResult",
    "ResidualResearchError",
    "ResidualResearchResult",
    "ResidualStrategyBacktestRun",
    "TargetExposureResult",
    "analyze_target_exposures",
    "assign_factor_groups",
    "compute_forward_returns",
    "compute_group_returns",
    "compute_information_coefficient",
    "forward_return_column",
    "run_cross_section_research",
    "run_market_residual_mean_reversion_backtest",
    "run_residual_research",
    "summarize_information_coefficient",
]
