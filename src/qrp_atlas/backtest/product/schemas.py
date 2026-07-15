"""Serializable catalog DTOs for indicators and strategies."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ParameterSpecDTO(BaseModel):
    type: str
    required: bool = False
    default: Any = None
    has_default: bool = False
    minimum: float | None = None
    maximum: float | None = None
    description: str | None = None
    label: str | None = None
    enum: list[Any] | None = None


class IndicatorCatalogItem(BaseModel):
    code: str
    name: str
    layer: str
    scope: str
    frequency: str
    description: str = ""


class StrategyCatalogItem(BaseModel):
    code: str
    name: str
    version: str
    family: str
    description: str
    scope: str
    strategy_type: str
    required_fields: list[str] = Field(default_factory=list)
    required_indicators: list[str] = Field(default_factory=list)
    parameter_schema: dict[str, ParameterSpecDTO] = Field(default_factory=dict)
    indicator_requests: list[dict[str, Any]] = Field(default_factory=list)
    # Product capability metadata (07-B1+)
    product_supported: bool = False
    requires_historical_universe: bool = False
    supported_universe_modes: list[str] = Field(default_factory=lambda: ["tickers"])
    supported_entry_timings: list[str] = Field(
        default_factory=lambda: ["next_open", "same_close", "next_close"]
    )
    requires_portfolio_config: bool = True


class BacktestCostConfigDTO(BaseModel):
    commission_rate: float = 0.00025
    stamp_tax_rate: float = 0.0005
    slippage_bps: float = 5.0


class BacktestPositionConfigDTO(BaseModel):
    initial_cash: float = 1_000_000.0
    max_positions: int = 10
    max_weight_per_symbol: float = 0.1


class BacktestExecutionConfigDTO(BaseModel):
    entry_timing: str = "next_open"


class CreateBacktestTaskRequest(BaseModel):
    name: str | None = None
    strategy_code: str
    strategy_version: str
    strategy_params: dict[str, Any] = Field(default_factory=dict)
    universe_mode: str = "tickers"
    universe_preset: str | None = None
    # 07-B1: PIT historical index membership universe.
    index_code: str | None = None
    tickers: list[str] | None = None
    start_date: str
    end_date: str
    # Optional product benchmark (index code). Missing data => diagnostics, no silent substitute.
    benchmark_id: str | None = None
    position: BacktestPositionConfigDTO = Field(default_factory=BacktestPositionConfigDTO)
    cost: BacktestCostConfigDTO = Field(default_factory=BacktestCostConfigDTO)
    execution: BacktestExecutionConfigDTO = Field(default_factory=BacktestExecutionConfigDTO)


class BacktestTaskRecord(BaseModel):
    task_id: str
    run_id: str | None = None
    name: str
    strategy_code: str
    strategy_version: str
    strategy_params: dict[str, Any] = Field(default_factory=dict)
    universe_mode: str
    universe_preset: str | None = None
    index_code: str | None = None
    tickers: list[str] = Field(default_factory=list)
    start_date: str
    end_date: str
    benchmark_id: str | None = None
    position: BacktestPositionConfigDTO
    cost: BacktestCostConfigDTO
    execution: BacktestExecutionConfigDTO
    status: str
    error_message: str | None = None
    created_at: str
    updated_at: str
    is_mock: bool = False
    request_snapshot: dict[str, Any] = Field(default_factory=dict)


class CreateBacktestTaskResponse(BaseModel):
    task: BacktestTaskRecord
