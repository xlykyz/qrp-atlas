"""市场宽度指标测试。"""

from __future__ import annotations

import pandas as pd
import pytest

from qrp_atlas.indicators.market.breadth import calculate_market_breadth


def test_basic_breadth(daily_market_df: pd.DataFrame) -> None:
    result = calculate_market_breadth(daily_market_df)

    assert result["total_count"] == 7
    assert result["up_count"] == 2
    assert result["down_count"] == 4
    assert result["flat_count"] == 1
    assert result["limit_up_count"] == 1
    assert result["limit_down_count"] == 1
    assert result["limit_up_down_diff"] == 0
    assert pytest.approx(result["up_ratio"], abs=1e-4) == 2 / 7
    assert pytest.approx(result["down_ratio"], abs=1e-4) == 4 / 7


def test_empty_breadth() -> None:
    result = calculate_market_breadth(pd.DataFrame())
    assert result["total_count"] == 0
    assert result["up_count"] == 0
    assert result["limit_up_down_diff"] == 0


def test_none_breadth() -> None:
    result = calculate_market_breadth(None)
    assert result["total_count"] == 0


def test_limit_flags_derived_when_missing(daily_market_df: pd.DataFrame) -> None:
    df = daily_market_df.drop(columns=["is_st"])
    result = calculate_market_breadth(df)
    assert result["limit_up_count"] == 1
    assert result["limit_down_count"] == 1


def test_missing_required_column_raises() -> None:
    df = pd.DataFrame({"ticker": ["000001.SZ"]})
    with pytest.raises(ValueError, match="缺少必要列"):
        calculate_market_breadth(df)
