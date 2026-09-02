from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qrp_atlas.contracts import (
    ACTIVE_SPRINT_NO, ANCHOR_CLOSE, ANCHOR_DATE, ASSET_ID, CLOSE,
    DAYS_SINCE_CONFIRMED, DAYS_SINCE_START, DRAWDOWN_FROM_PEAK,
    END_CLOSE, END_DATE, EPISODE_CONFIRMED_DATE, EPISODE_END_DATE,
    EPISODE_ID, EPISODE_NO, EPISODE_RETURN, EPISODE_START_DATE,
    IS_EPISODE_CONFIRMED, IS_EPISODE_END, IS_OPEN, MAX_DRAWDOWN,
    PEAK_CLOSE, PEAK_DATE, PEAK_RETURN, SEGMENT_ID, SEGMENT_NO,
    SEGMENT_RETURN, SEGMENT_STATE, START_CLOSE, START_DATE,
    TRADE_DATE, TRADING_DAYS, TREND_STATE,
)
from qrp_atlas.indicators.system_b import (
    calculate_system_b_episodes,
    calculate_system_b_episode_segments,
    SystemBEpisodeSegmentError,
)


def make_data(states, closes=None, ma10=None, start_date="2026-01-05"):
    n = len(states)
    closes = closes or [10.0 + i for i in range(n)]
    ma10 = ma10 or [8.0] * n
    return pd.DataFrame({
        "asset_id": ["000001.SZ"] * n,
        "trade_date": pd.bdate_range(start_date, periods=n),
        "close": closes,
        "ma5": [9.0] * n,
        "ma10": ma10,
        "trend_state": states,
    })


def run_pipeline(data: pd.DataFrame):
    ep_res = calculate_system_b_episodes(data)
    seg_res = calculate_system_b_episode_segments(ep_res.episodes, ep_res.observations)
    return ep_res, seg_res


def test_case_1_single_active():
    """Case 1: Single ACTIVE run."""
    data = make_data(["BASE", "CANDIDATE", "ACTIVE", "ACTIVE", "ACTIVE"], closes=[10.0, 10.0, 11.0, 12.0, 13.0])
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert len(segments) == 1
    seg1 = segments.iloc[0]
    assert seg1[SEGMENT_ID] == "000001.SZ_EP_0001_SEG_001"
    assert seg1[SEGMENT_NO] == 1
    assert seg1[SEGMENT_STATE] == "ACTIVE"
    assert seg1[ACTIVE_SPRINT_NO] == 1
    assert seg1[TRADING_DAYS] == 3
    assert seg1[ANCHOR_CLOSE] == pytest.approx(10.0)
    assert seg1[START_CLOSE] == pytest.approx(11.0)
    assert seg1[END_CLOSE] == pytest.approx(13.0)
    assert seg1[SEGMENT_RETURN] == pytest.approx(0.30)
    assert seg1[PEAK_CLOSE] == pytest.approx(13.0)
    assert seg1[PEAK_RETURN] == pytest.approx(0.30)
    assert seg1[MAX_DRAWDOWN] == pytest.approx(0.0)
    assert bool(seg1[IS_OPEN]) is True

    # Mathematical closure
    latest_ep_return = ep_res.observations[EPISODE_RETURN].iloc[-1]
    assert np.isclose(1.0 + seg1[SEGMENT_RETURN], 1.0 + latest_ep_return, rtol=1e-10, atol=1e-12)


def test_case_2_one_reentry():
    """Case 2: One reentry (ACTIVE -> NON_ACTIVE -> ACTIVE)."""
    states = ["BASE", "CANDIDATE", "ACTIVE", "ACTIVE", "BASE", "CANDIDATE", "ACTIVE"]
    closes = [10.0, 10.0, 12.0, 14.0, 13.0, 13.5, 15.0]
    data = make_data(states, closes=closes)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert len(segments) == 3
    assert segments[SEGMENT_STATE].tolist() == ["ACTIVE", "NON_ACTIVE", "ACTIVE"]
    sprint_nos = [None if pd.isna(x) else int(x) for x in segments[ACTIVE_SPRINT_NO]]
    assert sprint_nos == [1, None, 2]
    assert segments[SEGMENT_NO].tolist() == [1, 2, 3]

    # Reentry closure
    active_count = (segments[SEGMENT_STATE] == "ACTIVE").sum()
    ma5_reentry = ep_res.episodes.iloc[0]["ma5_reentry_count"]
    assert active_count == ma5_reentry + 1

    # Segment 1
    assert segments.iloc[0][ANCHOR_CLOSE] == pytest.approx(10.0)
    assert segments.iloc[0][END_CLOSE] == pytest.approx(14.0)
    assert segments.iloc[0][SEGMENT_RETURN] == pytest.approx(0.40)

    # Segment 2 (NON_ACTIVE)
    assert segments.iloc[1][ANCHOR_CLOSE] == pytest.approx(14.0)
    assert segments.iloc[1][START_CLOSE] == pytest.approx(13.0)
    assert segments.iloc[1][END_CLOSE] == pytest.approx(13.5)
    assert segments.iloc[1][SEGMENT_RETURN] == pytest.approx(13.5 / 14.0 - 1.0)
    assert segments.iloc[1][MAX_DRAWDOWN] == pytest.approx(13.0 / 14.0 - 1.0)

    # Segment 3 (ACTIVE 2)
    assert segments.iloc[2][ANCHOR_CLOSE] == pytest.approx(13.5)
    assert segments.iloc[2][END_CLOSE] == pytest.approx(15.0)
    assert segments.iloc[2][SEGMENT_RETURN] == pytest.approx(15.0 / 13.5 - 1.0)
    assert bool(segments.iloc[2][IS_OPEN]) is True

    # Mathematical telescoping closure
    prod_factors = np.prod(1.0 + segments[SEGMENT_RETURN].to_numpy())
    latest_ep_return = ep_res.observations[EPISODE_RETURN].iloc[-1]
    assert np.isclose(prod_factors, 1.0 + latest_ep_return, rtol=1e-10, atol=1e-12)


def test_case_3_multiple_reentries():
    """Case 3: Multiple reentries (Sprint 1, 2, 3)."""
    states = ["BASE", "CANDIDATE", "ACTIVE", "BASE", "CANDIDATE", "ACTIVE", "BASE", "CANDIDATE", "ACTIVE"]
    closes = [10.0, 10.0, 11.0, 10.5, 11.0, 13.0, 12.0, 12.5, 16.0]
    data = make_data(states, closes=closes)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert len(segments) == 5
    assert segments[SEGMENT_STATE].tolist() == ["ACTIVE", "NON_ACTIVE", "ACTIVE", "NON_ACTIVE", "ACTIVE"]
    sprint_nos = [None if pd.isna(x) else int(x) for x in segments[ACTIVE_SPRINT_NO]]
    assert sprint_nos == [1, None, 2, None, 3]

    # Return closure
    prod_factors = np.prod(1.0 + segments[SEGMENT_RETURN].to_numpy())
    latest_ep_return = ep_res.observations[EPISODE_RETURN].iloc[-1]
    assert np.isclose(prod_factors, 1.0 + latest_ep_return, rtol=1e-10, atol=1e-12)


def test_case_4_long_non_active_not_ended():
    """Case 4: Long NON_ACTIVE while MA10 holds, Episode does not end."""
    states = ["BASE", "CANDIDATE", "ACTIVE", "ACTIVE", "BASE", "BASE", "BASE", "BASE"]
    closes = [10.0, 10.0, 12.0, 14.0, 13.0, 13.5, 13.0, 13.2]
    ma10 = [8.0, 8.0, 8.0, 9.0, 10.0, 10.0, 10.0, 10.0]
    data = make_data(states, closes=closes, ma10=ma10)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert len(segments) == 2
    assert segments.iloc[1][SEGMENT_STATE] == "NON_ACTIVE"
    assert segments.iloc[1][TRADING_DAYS] == 4
    assert bool(segments.iloc[1][IS_OPEN]) is True
    assert pd.isna(ep_res.episodes.iloc[0][EPISODE_END_DATE])


def test_case_5_episode_ended():
    """Case 5: Episode ends on 2 consecutive close < ma10."""
    states = ["BASE", "CANDIDATE", "ACTIVE", "ACTIVE", "BASE", "BASE"]
    closes = [8.0, 9.0, 12.0, 14.0, 9.0, 8.0]
    ma10 = [10.0] * 6
    data = make_data(states, closes=closes, ma10=ma10)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert len(segments) == 2
    assert bool(segments.iloc[0][IS_OPEN]) is False
    assert bool(segments.iloc[1][IS_OPEN]) is False
    assert segments.iloc[1][END_DATE] == ep_res.episodes.iloc[0][EPISODE_END_DATE]


def test_case_6_open_episode():
    """Case 6: Open Episode last segment is_open is True."""
    states = ["BASE", "CANDIDATE", "ACTIVE", "ACTIVE"]
    closes = [10.0, 10.0, 12.0, 15.0]
    data = make_data(states, closes=closes)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert len(segments) == 1
    assert bool(segments.iloc[0][IS_OPEN]) is True


def test_case_7_first_active_large_jump():
    """Case 7: Anchor price recovered accurately on first day big jump."""
    states = ["BASE", "CANDIDATE", "ACTIVE", "ACTIVE"]
    closes = [10.0, 10.0, 15.0, 18.0]  # T0(CANDIDATE)=10, T1(ACTIVE)=15 (+50%)
    data = make_data(states, closes=closes)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    seg1 = segments.iloc[0]
    assert seg1[ANCHOR_CLOSE] == pytest.approx(10.0)
    assert seg1[START_CLOSE] == pytest.approx(15.0)
    assert seg1[END_CLOSE] == pytest.approx(18.0)
    assert seg1[SEGMENT_RETURN] == pytest.approx(0.80)


def test_case_8_multiple_episodes_isolated():
    """Case 8: Multiple episodes of the same asset do not merge segments."""
    states = [
        "BASE", "CANDIDATE", "ACTIVE", "BASE", "BASE",
        "BASE", "CANDIDATE", "ACTIVE", "ACTIVE",
    ]
    closes = [
        8.0, 9.0, 12.0, 9.0, 8.0,
        8.0, 9.0, 12.0, 14.0,
    ]
    ma10 = [10.0] * 5 + [8.0] * 4
    data = make_data(states, closes=closes, ma10=ma10)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert len(ep_res.episodes) == 2
    ep1_segs = segments[segments[EPISODE_ID] == "000001.SZ_EP_0001"]
    ep2_segs = segments[segments[EPISODE_ID] == "000001.SZ_EP_0002"]

    assert ep1_segs[SEGMENT_NO].tolist() == [1, 2]
    assert ep2_segs[SEGMENT_NO].tolist() == [1]
    assert ep2_segs.iloc[0][ACTIVE_SPRINT_NO] == 1


def test_case_9_calendar_gap_preserves_trading_days():
    """Case 9: Weekend / holiday gaps do not alter trading_days row count."""
    data = make_data(["BASE", "CANDIDATE", "ACTIVE", "ACTIVE", "ACTIVE"], closes=[10.0, 10.0, 11.0, 12.0, 13.0])
    data.loc[4, "trade_date"] += pd.Timedelta(days=10)
    ep_res, seg_res = run_pipeline(data)
    segments = seg_res.segments

    assert segments.iloc[0][TRADING_DAYS] == 3


def test_case_10_determinism():
    """Case 10: Repeated runs yield identical output."""
    data = make_data(["BASE", "CANDIDATE", "ACTIVE", "BASE", "CANDIDATE", "ACTIVE"])
    _, seg_res1 = run_pipeline(data)
    _, seg_res2 = run_pipeline(data)

    pd.testing.assert_frame_equal(seg_res1.segments, seg_res2.segments)


def test_empty_inputs_return_empty_result():
    empty_ep = pd.DataFrame(columns=[
        EPISODE_ID, ASSET_ID, EPISODE_NO, EPISODE_START_DATE,
        EPISODE_CONFIRMED_DATE, EPISODE_END_DATE,
    ])
    empty_obs = pd.DataFrame(columns=[
        TRADE_DATE, ASSET_ID, EPISODE_ID, CLOSE, TREND_STATE, EPISODE_RETURN,
    ])
    res = calculate_system_b_episode_segments(empty_ep, empty_obs)
    assert res.segments.empty
    assert len(res.segments.columns) == 19


def test_missing_required_columns_raises_error():
    with pytest.raises(SystemBEpisodeSegmentError, match="episodes missing required"):
        calculate_system_b_episode_segments(pd.DataFrame({"foo": [1]}), pd.DataFrame({"bar": [2]}))
