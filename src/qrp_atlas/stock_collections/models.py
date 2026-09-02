"""Domain models, query contexts, and error types for StockCollection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


class StockCollectionError(ValueError):
    """Domain error for StockCollection operations."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class StockCollectionRecord:
    collection_id: str
    collection_type: str
    collection_scope: str
    namespace: str
    source_key: str
    canonical_name: str
    membership_model: str
    status: str
    effective_from: date
    effective_to: date | None
    available_trade_date: date
    source: str
    source_record_id: str | None
    revision_id: str
    ingested_at: datetime


@dataclass(frozen=True)
class ThemeRecord:
    theme_id: str
    collection_id: str
    canonical_name: str
    status: str
    effective_from: date
    effective_to: date | None
    available_trade_date: date
    source: str
    source_record_id: str | None
    revision_id: str
    ingested_at: datetime


@dataclass(frozen=True)
class ThemeMembershipRecord:
    membership_id: str
    theme_id: str
    collection_id: str
    asset_id: str
    weight: float | None
    effective_from: date
    effective_to: date | None
    available_trade_date: date
    source: str
    source_record_id: str | None
    revision_id: str
    ingested_at: datetime


@dataclass(frozen=True)
class StockCollectionQueryContext:
    as_of_date: date
    knowledge_date: date
    version_context: str = "default"
    allowed_scopes: tuple[str, ...] = ("CANONICAL",)


@dataclass(frozen=True)
class ResolvedMember:
    collection_id: str
    asset_id: str
    as_of_date: date
    weight: float | None
    membership_id: str
    revision_id: str
    effective_from: date
    effective_to: date | None
    available_trade_date: date
    source_table: str
    source_record_id: str | None


@dataclass(frozen=True)
class MembershipExplanation:
    collection_id: str
    asset_id: str
    as_of_date: date
    knowledge_date: date
    is_member: bool
    membership_id: str | None
    revision_id: str | None
    effective_from: date | None
    effective_to: date | None
    available_trade_date: date | None
    reason: str
    lifecycle_history: tuple[dict[str, Any], ...]
