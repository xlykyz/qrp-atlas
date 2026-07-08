"""
engine.py - 回测引擎主流程

BacktestEngine.run 编排:
1. 校验 price_df / signals_df / config
2. 按 asset_id 建立行情索引
3. 遍历每条信号，调用 broker.simulate_signal 生成 Trade / Skipped
4. 调用 metrics.summarize_trades 汇总
5. 返回 BacktestResult

引擎本身不读数据库、不实现策略逻辑。
"""

from typing import List

import pandas as pd

from .broker import AssetPriceIndex, build_price_index, simulate_signal
from .metrics import summarize_trades
from .models import BacktestConfig, BacktestResult, Skipped, Trade
from .validators import validate_config, validate_price_df, validate_signals_df


class BacktestEngine:
    """通用回测引擎。

    引擎无状态，可重复调用 run。
    """

    def run(
        self,
        price_df: pd.DataFrame,
        signals_df: pd.DataFrame,
        config: BacktestConfig,
    ) -> BacktestResult:
        """执行一次回测。

        Args:
            price_df: 标准 PriceFrame。
            signals_df: 标准 SignalFrame。
            config: 回测配置。

        Returns:
            BacktestResult，包含 summary / trades / skipped / equity_curve。
        """
        validate_price_df(price_df)
        validate_signals_df(signals_df)
        validate_config(config)

        price_index: dict[str, AssetPriceIndex] = build_price_index(price_df)

        trades: List[Trade] = []
        skipped: List[Skipped] = []

        if len(signals_df) == 0:
            return BacktestResult(
                config=config,
                summary=summarize_trades(trades, skipped),
                trades=trades,
                skipped=skipped,
                equity_curve=[],
            )

        for _, signal in signals_df.iterrows():
            result = simulate_signal(signal, price_index, config)
            if isinstance(result, Trade):
                trades.append(result)
            else:
                skipped.append(result)

        return BacktestResult(
            config=config,
            summary=summarize_trades(trades, skipped),
            trades=trades,
            skipped=skipped,
            equity_curve=[],
        )


__all__ = ["BacktestEngine"]
