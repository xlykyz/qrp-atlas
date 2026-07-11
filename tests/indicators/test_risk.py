"""市场风险指标测试。"""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.indicators.market.risk import calculate_market_risk


def test_basic_risk(daily_market_df: pd.DataFrame) -> None:
    result = calculate_market_risk(daily_market_df)

    assert result["limit_down_count"] == 1
    assert result["down_gt_5pct_count"] == 3
    assert result["down_gt_10pct_count"] == 2
    assert result["cyb_kcb_down_gt_10pct_count"] == 2
    assert result["risk_level"] == "low"
    assert "低风险" in result["description"]


def test_empty_risk() -> None:
    result = calculate_market_risk(pd.DataFrame())
    assert result["limit_down_count"] == 0
    assert result["risk_level"] == "low"


def test_extreme_risk_level() -> None:
    rows = [
        {"ticker": f"00000{i}.SZ", "pct_change": -10.0, "close": 9.0, "pre_close": 10.0, "is_st": False}
        for i in range(120)
    ]
    result = calculate_market_risk(pd.DataFrame(rows))
    assert result["risk_level"] == "extreme"
    assert result["down_gt_10pct_count"] == 120


def test_cyb_kcb_isolation(daily_market_df: pd.DataFrame) -> None:
    df = pd.concat(
        [daily_market_df, pd.DataFrame([{"ticker": "000005.SZ", "pct_change": -10.0, "close": 9.0, "pre_close": 10.0, "is_st": False}])],
        ignore_index=True,
    )
    result = calculate_market_risk(df)
    assert result["cyb_kcb_down_gt_10pct_count"] == 2
    assert result["down_gt_10pct_count"] == 3


def test_missing_required_column_raises() -> None:
    df = pd.DataFrame({"ticker": ["000001.SZ"]})
    with pytest.raises(ValueError, match="缺少必要列"):
        calculate_market_risk(df)
