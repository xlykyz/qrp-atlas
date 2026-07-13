"""Structural validation for portfolio backtest inputs."""

import math

import pandas as pd

from .models import PortfolioBacktestConfig

TARGET_WEIGHT_REQUIRED_COLUMNS = ("trade_date", "asset_id", "target_weight")


def validate_portfolio_config(config: PortfolioBacktestConfig) -> None:
    if not isinstance(config, PortfolioBacktestConfig):
        raise ValueError("config must be a PortfolioBacktestConfig instance")
    if config.initial_cash <= 0:
        raise ValueError("initial_cash must be positive")
    if config.max_positions <= 0:
        raise ValueError("max_positions must be positive")
    if not 0 < config.max_weight_per_asset <= 1:
        raise ValueError("max_weight_per_asset must be in (0, 1]")
    if config.cost.commission_rate < 0:
        raise ValueError("commission_rate must be >= 0")
    if config.cost.stamp_tax_rate < 0:
        raise ValueError("stamp_tax_rate must be >= 0")
    if config.cost.slippage_bps < 0:
        raise ValueError("slippage_bps must be >= 0")
    execution = config.execution
    if not execution.price_field or not execution.mark_price_field:
        raise ValueError("execution price fields must be non-empty")
    if not isinstance(execution.lot_size, int) or execution.lot_size <= 0:
        raise ValueError("lot_size must be a positive int")
    if execution.minimum_commission < 0:
        raise ValueError("minimum_commission must be >= 0")


def validate_target_weights(
    target_weights_df: pd.DataFrame,
    config: PortfolioBacktestConfig,
) -> None:
    if not isinstance(target_weights_df, pd.DataFrame):
        raise ValueError("target_weights_df must be a pandas DataFrame")
    missing = [
        column
        for column in TARGET_WEIGHT_REQUIRED_COLUMNS
        if column not in target_weights_df.columns
    ]
    if missing:
        raise ValueError(f"target_weights_df missing required columns: {missing}")
    if target_weights_df.empty:
        return
    if target_weights_df.duplicated(["trade_date", "asset_id"], keep=False).any():
        raise ValueError("target_weights_df has duplicate (trade_date, asset_id) pairs")

    dates = pd.to_datetime(target_weights_df["trade_date"], errors="coerce")
    assets = target_weights_df["asset_id"].astype(str).str.strip()
    weights = pd.to_numeric(target_weights_df["target_weight"], errors="coerce")
    if dates.isna().any():
        raise ValueError("target_weights_df contains invalid trade_date values")
    if assets.eq("").any() or assets.isin({"nan", "None"}).any():
        raise ValueError("target_weights_df contains missing asset_id values")
    if weights.isna().any() or not weights.map(math.isfinite).all():
        raise ValueError("target_weight values must be finite numbers")
    if (weights < 0).any():
        raise ValueError("target_weight values must be >= 0")
    if (weights > config.max_weight_per_asset + 1e-12).any():
        raise ValueError("target_weight exceeds max_weight_per_asset")

    normalized = target_weights_df.assign(trade_date=dates, target_weight=weights)
    if (normalized.groupby("trade_date")["target_weight"].sum() > 1.0 + 1e-9).any():
        raise ValueError("target weights must sum to <= 1 on each trade_date")
    positive_counts = normalized[normalized["target_weight"] > 0].groupby("trade_date").size()
    if (positive_counts > config.max_positions).any():
        raise ValueError("positive target count exceeds max_positions")
