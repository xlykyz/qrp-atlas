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
    run_industry_residual_research,
    run_market_residual_mean_reversion_backtest,
    run_residual_research,
)
from .robustness import (
    DEFAULT_COST_SCENARIOS,
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_ROLLING_WINDOWS,
    DEFAULT_SELECTION_OBJECTIVE,
    CostStressScenario,
    ParameterCandidate,
    ResidualRobustnessError,
    ResidualRobustnessResult,
    WalkForwardConfig,
    WalkForwardSplit,
    build_parameter_candidates,
    build_walk_forward_splits,
    compute_portfolio_performance_metrics,
    compute_rolling_performance,
    run_residual_robustness_study,
    stitch_oos_equity,
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
    "DEFAULT_EVENT_HORIZONS",
    "CrossSectionResearchError",
    "CrossSectionResearchResult",
    "EarningsForecastEventStudyResult",
    "EventStudyError",
    "ForwardReturnError",
    "GroupReturnResult",
    "ICSummaryResult",
    "ResidualResearchError",
    "ResidualResearchResult",
    "ResidualStrategyBacktestRun",
    "TargetExposureResult",
    "analyze_target_exposures",
    "assign_factor_groups",
    "compute_event_forward_returns",
    "compute_forward_returns",
    "compute_group_returns",
    "compute_information_coefficient",
    "event_forward_return_column",
    "forward_return_column",
    "run_cross_section_research",
    "run_earnings_forecast_event_study",
    "run_industry_residual_research",
    "run_market_residual_mean_reversion_backtest",
    "run_residual_research",
    "run_residual_robustness_study",
    "build_walk_forward_splits",
    "build_parameter_candidates",
    "compute_portfolio_performance_metrics",
    "compute_rolling_performance",
    "stitch_oos_equity",
    "WalkForwardConfig",
    "WalkForwardSplit",
    "CostStressScenario",
    "ParameterCandidate",
    "ResidualRobustnessError",
    "ResidualRobustnessResult",
    "DEFAULT_COST_SCENARIOS",
    "DEFAULT_MAX_CANDIDATES",
    "DEFAULT_ROLLING_WINDOWS",
    "DEFAULT_SELECTION_OBJECTIVE",
    "summarize_event_groups",
    "summarize_information_coefficient",
]
