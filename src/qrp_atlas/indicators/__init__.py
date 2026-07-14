"""qrp_atlas.indicators - 市场复合指标计算层。

本包基于 contracts 字段规则和已有行情数据，计算市场宽度、市场风险、
个股趋势、系统 B 基础状态等复合指标，服务于市场监测、复盘、回测和
交易系统判断。

依赖方向：contracts → indicators → review / backtest / api / frontend。

子模块：
    - market/    市场宽度与风险
    - stock/     个股趋势
    - system_b/  系统 B 基础状态检测
    - cross_section/ 横截面基础算子与历史股票池
    - service    对外组合入口
"""

# 保留：指标元数据定义与注册表（非本次核心）
from qrp_atlas.indicators.definitions import (
    IndicatorDefinition,
    IndicatorLayer,
    IndicatorScope,
    UpdateFrequency,
)
from qrp_atlas.indicators.registry import get_indicator, list_indicators
from qrp_atlas.indicators.parameterized import (
    IndicatorCalculationDefinition,
    IndicatorConflictError,
    IndicatorParameterBinding,
    IndicatorParameterSpec,
    IndicatorRequest,
    IndicatorRequestError,
    UnknownIndicatorError,
    bind_indicator_request,
    calculate_indicators,
    get_calculation_definition,
    indicator_output_fields,
    resolve_indicator_requests,
)

# 新增：市场复合指标计算
from qrp_atlas.indicators.market import calculate_market_breadth, calculate_market_risk
from qrp_atlas.indicators.stock import calculate_stock_trend
from qrp_atlas.indicators.system_b import (
    detect_system_b_basic_state,
    detect_system_b_basic_state_from_prices,
)
from qrp_atlas.indicators.service import calculate_daily_market_snapshot

# 横截面研究基础（任务 04-A）
from qrp_atlas.indicators.cross_section import (
    CrossSectionFrameError,
    FACTOR_DEFINITIONS,
    FactorDefinition,
    FactorError,
    FactorParameterSpec,
    FactorRequest,
    FactorRequestError,
    HistoricalUniverseRequest,
    HistoricalUniverseSource,
    REQUIRED_CROSS_SECTION_COLUMNS,
    ResolvedFactorRequest,
    UnknownFactorError,
    apply_cross_section_operators,
    build_historical_universe,
    compute_book_to_price_factor,
    compute_log_market_cap_factor,
    compute_momentum_factor,
    compute_roe_factor,
    cross_section_percentile_rank,
    cross_section_rank,
    cross_section_winsorize,
    cross_section_zscore,
    enforce_cross_section_primary_key,
    ensure_cross_section_frame,
    generate_factor_frame,
    NeutralizationError,
    neutralize_cross_section,
    neutralize_factor_frame,
    get_factor_definition,
    list_factors,
    normalize_asset_id,
    normalize_feature_columns,
    normalize_trade_date,
    normalize_trade_dates,
    process_cross_section,
    resolve_factor_requests,
    resolve_historical_universe,
    sort_cross_section_frame,
)

__all__ = [
    # 元数据定义（保留）
    "IndicatorDefinition",
    "IndicatorLayer",
    "IndicatorScope",
    "UpdateFrequency",
    "get_indicator",
    "list_indicators",
    "IndicatorConflictError",
    "IndicatorCalculationDefinition",
    "IndicatorParameterBinding",
    "IndicatorParameterSpec",
    "IndicatorRequest",
    "IndicatorRequestError",
    "UnknownIndicatorError",
    "bind_indicator_request",
    "calculate_indicators",
    "get_calculation_definition",
    "indicator_output_fields",
    "resolve_indicator_requests",
    # 市场复合指标
    "calculate_market_breadth",
    "calculate_market_risk",
    "calculate_stock_trend",
    "detect_system_b_basic_state",
    "detect_system_b_basic_state_from_prices",
    "calculate_daily_market_snapshot",
    # 横截面研究基础
    "CrossSectionFrameError",
    "FACTOR_DEFINITIONS",
    "FactorDefinition",
    "FactorError",
    "FactorParameterSpec",
    "FactorRequest",
    "FactorRequestError",
    "HistoricalUniverseRequest",
    "HistoricalUniverseSource",
    "REQUIRED_CROSS_SECTION_COLUMNS",
    "ResolvedFactorRequest",
    "UnknownFactorError",
    "apply_cross_section_operators",
    "build_historical_universe",
    "compute_book_to_price_factor",
    "compute_log_market_cap_factor",
    "compute_momentum_factor",
    "compute_roe_factor",
    "cross_section_percentile_rank",
    "cross_section_rank",
    "cross_section_winsorize",
    "cross_section_zscore",
    "enforce_cross_section_primary_key",
    "ensure_cross_section_frame",
    "generate_factor_frame",
    "NeutralizationError",
    "neutralize_cross_section",
    "neutralize_factor_frame",
    "get_factor_definition",
    "list_factors",
    "normalize_asset_id",
    "normalize_feature_columns",
    "normalize_trade_date",
    "normalize_trade_dates",
    "process_cross_section",
    "resolve_factor_requests",
    "resolve_historical_universe",
    "sort_cross_section_frame",
]
