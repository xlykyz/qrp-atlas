"""indicators 对外入口测试。"""

from __future__ import annotations

import pandas as pd

from qrp_atlas.indicators.service import calculate_daily_market_snapshot


def test_snapshot_combines_breadth_and_risk(daily_market_df: pd.DataFrame) -> None:
    result = calculate_daily_market_snapshot(daily_market_df)

    assert "breadth" in result
    assert "risk" in result

    assert result["breadth"]["total_count"] == 7
    assert result["breadth"]["limit_up_count"] == 1

    assert result["risk"]["limit_down_count"] == 1
    assert result["risk"]["cyb_kcb_down_gt_10pct_count"] == 2


def test_snapshot_empty() -> None:
    result = calculate_daily_market_snapshot(pd.DataFrame())
    assert result["breadth"]["total_count"] == 0
    assert result["risk"]["risk_level"] == "low"


def test_snapshot_keys_consistent(daily_market_df: pd.DataFrame) -> None:
    result = calculate_daily_market_snapshot(daily_market_df)
    assert set(result.keys()) == {"breadth", "risk"}
