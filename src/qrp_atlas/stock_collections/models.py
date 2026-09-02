"""Domain models and query contexts for StockCollection."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Mapping, Sequence

from qrp_atlas.contracts.stock_collection import (
    CollectionScope,
    CollectionStatus,
    CollectionType,
    MembershipModel,
)


class StockCollectionErrorCode(StrEnum):
    COLLECTION_NOT_FOUND = "COLLECTION_NOT_FOUND"
    COLLECTION_NOT_AVAILABLE_AS_OF = "COLLECTION_NOT_AVAILABLE_AS_OF"
    COLLECTION_SCOPE_NOT_ALLOWED = "COLLECTION_SCOPE_NOT_ALLOWED"
    COLLECTION_VERSION_REQUIRED = "COLLECTION_VERSION_REQUIRED"
    COLLECTION_VERSION_UNSUPPORTED = "COLLECTION_VERSION_UNSUPPORTED"
    COLLECTION_ADAPTER_NOT_FOUND = "COLLECTION_ADAPTER_NOT_FOUND"
    COLLECTION_IDENTITY_COLLISION = "COLLECTION_IDENTITY_COLLISION"
    COLLECTION_PIT_INVARIANT_VIOLATION = "COLLECTION_PIT_INVARIANT_VIOLATION"
    COLLECTION_SOURCE_INCONSISTENT = "COLLECTION_SOURCE_INCONSISTENT"


class StockCollectionError(ValueError):
    """Base error for stock collections."""

    def __init__(self, code: StockCollectionErrorCode | str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class StockCollectionRecord:
    """Registry record representing a stock collection revision."""
    collection_id: str
    collection_type: CollectionType | str
    collection_scope: CollectionScope | str
    namespace: str
    source_key: str
    canonical_name: str
    membership_model: MembershipModel | str
    status: CollectionStatus | str
    effective_from: date
    effective_to: date | None
    available_trade_date: date
    source: str
    source_record_id: str | None
    revision_id: str
    ingested_at: datetime


@dataclass(frozen=True)
class ThemeRecord:
    """Canonical theme domain entity revision (1:1 with THEME StockCollection)."""
    theme_id: str
    collection_id: str
    canonical_name: str
    status: CollectionStatus | str
    effective_from: date
    effective_to: date | None
    available_trade_date: date
    source: str
    source_record_id: str | None
    revision_id: str
    ingested_at: datetime


@dataclass(frozen=True)
class ThemeMembershipRecord:
    """Theme membership interval revision record."""
    membership_id: str
    theme_id: str
    collection_id: str
    asset_id: str
    effective_from: date
    effective_to: date | None
    available_trade_date: date
    source: str
    source_record_id: str | None
    revision_id: str
    ingested_at: datetime


@dataclass(frozen=True)
class ResolvedMember:
    """Normalized resolved member from StockCollectionResolver."""
    collection_id: str
    collection_type: CollectionType | str
    asset_id: str
    as_of_date: date
    weight: float | None = None
    source_table: str = ""
    source_record_id: str | None = None
    source_revision_id: str = ""
    source_rule_version: str | None = None


@dataclass(frozen=True)
class MembershipExplanation:
    """Explainability record detailing why an asset belongs or belonged to a collection."""
    collection_id: str
    asset_id: str
    is_member: bool
    as_of_date: date
    knowledge_date: date
    membership_id: str | None
    revision_id: str | None
    effective_from: date | None
    effective_to: date | None
    available_trade_date: date | None
    source: str | None
    source_record_id: str | None
    reasons: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class CollectionVersionContext:
    """Context holding rule and calculation versions."""
    rule_versions: Mapping[str, str] = field(default_factory=dict)
    calculation_versions: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class StockCollectionQueryContext:
    """Point-in-Time query context for resolving collections and members."""
    as_of_date: date
    knowledge_date: date
    version_context: CollectionVersionContext = field(default_factory=CollectionVersionContext)
    allowed_scopes: tuple[CollectionScope | str, ...] = (CollectionScope.CANONICAL,)
