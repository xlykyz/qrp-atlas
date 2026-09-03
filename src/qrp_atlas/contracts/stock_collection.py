"""Contracts and field constants for StockCollection and Theme domain."""

from __future__ import annotations

from enum import StrEnum
from typing import Final


class CollectionType(StrEnum):
    """Supported stock collection types.

    v1.1 Task 04-A strictly supports THEME in production.
    """
    THEME = "THEME"
    INDUSTRY = "INDUSTRY"
    INDEX = "INDEX"
    SYSTEM_POOL = "SYSTEM_POOL"
    USER_DEFINED = "USER_DEFINED"
    RESEARCH = "RESEARCH"


class CollectionScope(StrEnum):
    """Scope of the collection determining visibility and access control."""
    CANONICAL = "CANONICAL"
    USER = "USER"
    RESEARCH = "RESEARCH"


class MembershipModel(StrEnum):
    """Membership time model."""
    INTERVAL = "INTERVAL"
    DAILY_OBSERVATION = "DAILY_OBSERVATION"


class CollectionStatus(StrEnum):
    """Collection lifecycle status."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


# ── Field Constants ──
COLLECTION_ID: Final[str] = "collection_id"
COLLECTION_TYPE: Final[str] = "collection_type"
COLLECTION_SCOPE: Final[str] = "collection_scope"
NAMESPACE: Final[str] = "namespace"
SOURCE_KEY: Final[str] = "source_key"
CANONICAL_NAME: Final[str] = "canonical_name"
MEMBERSHIP_MODEL: Final[str] = "membership_model"
STATUS: Final[str] = "status"
EFFECTIVE_FROM: Final[str] = "effective_from"
EFFECTIVE_TO: Final[str] = "effective_to"
AVAILABLE_TRADE_DATE: Final[str] = "available_trade_date"
SOURCE: Final[str] = "source"
SOURCE_RECORD_ID: Final[str] = "source_record_id"
REVISION_ID: Final[str] = "revision_id"
INGESTED_AT: Final[str] = "ingested_at"

THEME_ID: Final[str] = "theme_id"
MEMBERSHIP_ID: Final[str] = "membership_id"
WEIGHT: Final[str] = "weight"
SOURCE_TABLE: Final[str] = "source_table"
SOURCE_REVISION_ID: Final[str] = "source_revision_id"
SOURCE_RULE_VERSION: Final[str] = "source_rule_version"

# ── Table Names & Versions ──
STOCK_COLLECTION_TABLE: Final[str] = "stock_collection"
THEME_TABLE: Final[str] = "theme"
THEME_MEMBERSHIP_HISTORY_TABLE: Final[str] = "theme_membership_history"

STOCK_COLLECTION_VERSION: Final[str] = "stock_collection@1.0.0"
THEME_VERSION: Final[str] = "theme@1.0.0"
THEME_MEMBERSHIP_VERSION: Final[str] = "theme_membership@1.0.0"
