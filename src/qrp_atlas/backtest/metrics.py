"""
metrics.py - 交易汇总指标

只对 trades 列表做统计聚合，不依赖任何策略概念。

约定:
- 默认基于 net_return 计算 summary。
- win: net_return > 0；loss: net_return <= 0。
- 空交易返回合理空值（None / 0），不报错。
"""

import statistics
from typing import Any, Dict, List

from .models import Skipped, Trade


def summarize_trades(trades: List[Trade], skipped: List[Skipped]) -> Dict[str, Any]:
    """计算 trades 汇总指标。

    Args:
        trades: 成交交易列表。
        skipped: 被跳过的信号列表。

    Returns:
        summary dict。空交易时各比率/均值字段返回 None，计数返回 0。
    """
    trade_count = len(trades)
    skipped_count = len(skipped)

    if trade_count == 0:
        return {
            "trade_count": 0,
            "skipped_count": skipped_count,
            "win_count": 0,
            "loss_count": 0,
            "win_rate": None,
            "avg_return": None,
            "median_return": None,
            "max_return": None,
            "min_return": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_loss_ratio": None,
            "avg_holding_bars": None,
            "avg_mae": None,
            "avg_mfe": None,
            "worst_mae": None,
            "best_mfe": None,
        }

    returns = [t.net_return for t in trades]
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    holding_bars = [t.holding_bars for t in trades]
    maes = [t.mae for t in trades]
    mfes = [t.mfe for t in trades]

    win_count = len(wins)
    loss_count = len(losses)
    avg_win = statistics.fmean(wins) if wins else None
    avg_loss = statistics.fmean(losses) if losses else None

    if avg_win is not None and avg_loss is not None and avg_loss != 0:
        profit_loss_ratio = avg_win / abs(avg_loss)
    else:
        profit_loss_ratio = None

    return {
        "trade_count": trade_count,
        "skipped_count": skipped_count,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": win_count / trade_count,
        "avg_return": statistics.fmean(returns),
        "median_return": statistics.median(returns),
        "max_return": max(returns),
        "min_return": min(returns),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_loss_ratio": profit_loss_ratio,
        "avg_holding_bars": statistics.fmean(holding_bars),
        "avg_mae": statistics.fmean(maes),
        "avg_mfe": statistics.fmean(mfes),
        "worst_mae": min(maes),
        "best_mfe": max(mfes),
    }
