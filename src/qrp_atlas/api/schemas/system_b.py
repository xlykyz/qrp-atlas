"""Pydantic response models for System B monitoring endpoints.

These models document the JSON contract between the FastAPI backend and the
React frontend.  Field names use snake_case throughout (matching DuckDB column
names) so the frontend can consume responses without any camelCase conversion.

Key data conventions (see system-design-v0.1 §8):
  - ``episode_return`` / ``peak_return`` are decimals (0.186 = +18.6%).
  - ``drawdown_from_peak`` is a negative decimal or zero (-0.032 = -3.2%).
  - Date fields are ISO 8601 strings ("2026-07-27").
  - Datetime fields are ISO 8601 strings with an explicit UTC offset
    ("2026-07-27T18:30:22Z").
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

from pydantic import AwareDatetime, BaseModel, Field


# ── P0-1 / P0-2 / P0-7: Summary & production run ───────────────────────────


class SystemBSummaryDto(BaseModel):
    """Aggregated state counts and transition counts for one trade date."""

    trade_date: date
    base_count: int = 0
    candidate_count: int = 0
    active_count: int = 0
    new_listing_warmup_count: int = 0
    null_state_count: int = 0
    base_to_candidate_count: int = 0
    candidate_to_active_count: int = 0
    active_to_base_count: int = 0
    active_held_count: int = 0
    explicit_non_trading_count: int = 0
    unresolved_missing_count: int = 0
    diagnostic_count: int = 0
    calculation_completed_at: AwareDatetime | None = None
    production_run_id: Optional[str] = None


class ProductionRunDto(BaseModel):
    """Latest System B production run metadata (P0-7 data freshness)."""

    production_run_id: str
    run_type: str
    status: str
    target_start_date: date | None = None
    target_end_date: date | None = None
    rule_version_set_id: str
    parameter_set_id: str
    input_snapshot_id: str | None = None
    calculation_version: str
    asset_count: int
    input_row_count: int
    output_row_count: int
    error_count: int
    metrics: dict[str, Any] = Field(default_factory=dict)
    error_code: str | None = None
    error_detail: str | None = None
    created_at: AwareDatetime
    completed_at: AwareDatetime | None = None


# ── P0-3 / P0-4 / P0-5: Active episodes (灵魂清单) ─────────────────────────


class ActiveEpisodeDto(BaseModel):
    """One row in the ACTIVE strong-stock list.

    Combines episode observation metrics (episode_return, drawdown, etc.)
    with episode metadata (episode_no, start/confirmed dates) and the
    stock's display name from ``stock_info``.
    """

    asset_id: str
    name: Optional[str] = None
    trade_date: date
    close: float
    episode_id: str
    episode_no: int
    days_since_start: int
    days_since_confirmed: int
    episode_return: float
    peak_return: float
    drawdown_from_peak: float
    ma5_reentry_count: int
    episode_start_date: date | None = None
    episode_confirmed_date: date | None = None
    trend_state: str
    previous_trend_state: Optional[str] = None


# ── P0-6: Pool snapshot ─────────────────────────────────────────────────────


class PoolMemberDto(BaseModel):
    """One stock's membership in a pool on a given trade date."""

    asset_id: str
    pool_type: str
    membership_state: str
    pool_cycle_no: int
    entry_date: date | None = None
    exit_date: date | None = None
    episode_id: Optional[str] = None


class PoolTypeSnapshot(BaseModel):
    """All IN_POOL members for one pool type on a given trade date."""

    pool_type: str
    count: int
    members: list[PoolMemberDto] = Field(default_factory=list)


class PoolSnapshotResponse(BaseModel):
    """Three-pool snapshot response for ``/pools/snapshot`` endpoints."""

    trade_date: date
    pools: list[PoolTypeSnapshot] = Field(default_factory=list)
