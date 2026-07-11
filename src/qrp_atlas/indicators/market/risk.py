"""市场风险指标。

基于单日全市场快照计算跌停数、大跌家数等风险指标，并给出风险等级。
用于描述当日市场下行压力。
"""

from __future__ import annotations

import pandas as pd

from qrp_atlas.contracts import (
    IS_LIMIT_DOWN,
    PCT_CHANGE,
    TICKER,
    derive_limit_flags,
    get_board,
)
from qrp_atlas.contracts.conventions import BOARD_CYB, BOARD_KCB

_REQUIRED_COLUMNS = (TICKER, PCT_CHANGE)

_DOWN_5_PCT = -5.0
_DOWN_10_PCT = -10.0


def _ensure_limit_flags(df: pd.DataFrame) -> pd.DataFrame:
    """返回保证含 is_limit_down 列的 DataFrame。"""
    if IS_LIMIT_DOWN in df.columns:
        return df
    return derive_limit_flags(df)


def _classify_risk(
    limit_down_count: int,
    down_gt_10pct_count: int,
    cyb_kcb_down_gt_10pct_count: int,
) -> tuple[str, str]:
    """根据跌停和大跌家数给出风险等级与描述。

    等级划分（经验阈值，仅作参考）：
        - low:      跌停 <= 5  且 大跌 >10% <= 20
        - medium:   跌停 <= 20 且 大跌 >10% <= 50
        - high:     跌停 <= 50 且 大跌 >10% <= 100
        - extreme:  跌停 > 50  或 大跌 >10% > 100
    """
    if limit_down_count > 50 or down_gt_10pct_count > 100:
        return "extreme", f"极端风险: 跌停{limit_down_count}家, 跌幅>10%共{down_gt_10pct_count}家"
    if limit_down_count > 20 or down_gt_10pct_count > 50:
        return "high", f"高风险: 跌停{limit_down_count}家, 跌幅>10%共{down_gt_10pct_count}家"
    if limit_down_count > 5 or down_gt_10pct_count > 20:
        return "medium", f"中等风险: 跌停{limit_down_count}家, 跌幅>10%共{down_gt_10pct_count}家"
    return "low", f"低风险: 跌停{limit_down_count}家, 跌幅>10%共{down_gt_10pct_count}家"


def calculate_market_risk(df: pd.DataFrame) -> dict:
    """计算单日市场风险。

    Args:
        df: 单日全市场行情快照，需含 ticker、pct_change；is_limit_down
            可选，缺失时按 contracts 规则派生。

    Returns:
        包含以下键的字典：
            - limit_down_count: 跌停家数
            - down_gt_5pct_count: 跌幅超过 5% 的家数
            - down_gt_10pct_count: 跌幅超过 10% 的家数
            - cyb_kcb_down_gt_10pct_count: 创业板/科创板跌幅超过 10% 的家数
            - risk_level: 风险等级 (low/medium/high/extreme)
            - description: 风险描述文本
    """
    if df is None or df.empty:
        return {
            "limit_down_count": 0,
            "down_gt_5pct_count": 0,
            "down_gt_10pct_count": 0,
            "cyb_kcb_down_gt_10pct_count": 0,
            "risk_level": "low",
            "description": "无数据",
        }

    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"calculate_market_risk 缺少必要列: {missing}")

    df = _ensure_limit_flags(df)

    pct = df[PCT_CHANGE]
    limit_down_count = int(df[IS_LIMIT_DOWN].fillna(False).astype(bool).sum())
    down_gt_5pct_count = int((pct <= _DOWN_5_PCT).sum())
    down_gt_10pct_count = int((pct <= _DOWN_10_PCT).sum())

    boards = df[TICKER].map(get_board)
    is_cyb_kcb = boards.isin((BOARD_CYB, BOARD_KCB))
    cyb_kcb_down_gt_10pct_count = int((is_cyb_kcb & (pct <= _DOWN_10_PCT)).sum())

    risk_level, description = _classify_risk(
        limit_down_count, down_gt_10pct_count, cyb_kcb_down_gt_10pct_count
    )

    return {
        "limit_down_count": limit_down_count,
        "down_gt_5pct_count": down_gt_5pct_count,
        "down_gt_10pct_count": down_gt_10pct_count,
        "cyb_kcb_down_gt_10pct_count": cyb_kcb_down_gt_10pct_count,
        "risk_level": risk_level,
        "description": description,
    }
