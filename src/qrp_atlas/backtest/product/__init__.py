"""Product orchestration layer for real backtest workflow APIs."""

from .catalog import (
    PRODUCT_SUPPORTED_STRATEGY_CODES,
    get_strategy_catalog_item,
    list_indicator_catalog,
    list_strategy_catalog,
)
from .schemas import (
    BacktestTaskRecord,
    CreateBacktestTaskRequest,
    CreateBacktestTaskResponse,
    IndicatorCatalogItem,
    StrategyCatalogItem,
)
from .cross_section import (
    CROSS_SECTIONAL_MOMENTUM_CODE,
    run_cross_sectional_momentum_product_backtest,
)
from .service import (
    BacktestProductService,
    BacktestTaskExecutionError,
    BacktestTaskValidationError,
    execute_validated_task,
    get_product_service,
    reset_product_service_for_tests,
    validate_create_request,
)
from .task_store import BacktestTaskStore
from .timing import REASON_NO_EXECUTION_DATE_IN_RANGE

__all__ = [
    "PRODUCT_SUPPORTED_STRATEGY_CODES",
    "list_indicator_catalog",
    "list_strategy_catalog",
    "run_cross_sectional_momentum_product_backtest",
    "CROSS_SECTIONAL_MOMENTUM_CODE",
    "get_strategy_catalog_item",
    "IndicatorCatalogItem",
    "StrategyCatalogItem",
    "CreateBacktestTaskRequest",
    "CreateBacktestTaskResponse",
    "BacktestTaskRecord",
    "BacktestTaskStore",
    "BacktestProductService",
    "BacktestTaskValidationError",
    "BacktestTaskExecutionError",
    "validate_create_request",
    "execute_validated_task",
    "get_product_service",
    "reset_product_service_for_tests",
    "REASON_NO_EXECUTION_DATE_IN_RANGE",
]
