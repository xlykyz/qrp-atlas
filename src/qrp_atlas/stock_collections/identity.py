"""Stable identity generator and validators for StockCollection."""

from __future__ import annotations

import re
from typing import Final

from qrp_atlas.contracts.stock_collection import CollectionType

COLLECTION_ID_PREFIX: Final[str] = "COLL"
_VALID_IDENTIFIER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Z0-9_\-\.\:\/]+$")


class StockCollectionIdentityError(ValueError):
    """Raised when identity generation or validation fails."""


def make_collection_id(
    collection_type: str | CollectionType,
    namespace: str,
    source_key: str,
) -> str:
    """Generate a deterministic, canonical collection_id.

    Format:
        COLL:{COLLECTION_TYPE}:{NAMESPACE}:{SOURCE_KEY}

    Example:
        >>> make_collection_id("THEME", "QRP", "AI_COMPUTE")
        'COLL:THEME:QRP:AI_COMPUTE'
    """
    type_str = str(collection_type).strip().upper()
    if not type_str:
        raise StockCollectionIdentityError("collection_type cannot be empty")

    ns_str = namespace.strip().upper()
    if not ns_str:
        raise StockCollectionIdentityError("namespace cannot be empty")
    if not _VALID_IDENTIFIER_PATTERN.match(ns_str):
        raise StockCollectionIdentityError(
            f"namespace contains invalid characters: '{namespace}'"
        )

    sk_str = source_key.strip().upper()
    if not sk_str:
        raise StockCollectionIdentityError("source_key cannot be empty")
    if not _VALID_IDENTIFIER_PATTERN.match(sk_str):
        raise StockCollectionIdentityError(
            f"source_key contains invalid characters: '{source_key}'"
        )

    return f"{COLLECTION_ID_PREFIX}:{type_str}:{ns_str}:{sk_str}"


def parse_collection_id(collection_id: str) -> tuple[str, str, str]:
    """Parse collection_id into (collection_type, namespace, source_key)."""
    parts = collection_id.strip().split(":")
    if len(parts) < 4 or parts[0] != COLLECTION_ID_PREFIX:
        raise StockCollectionIdentityError(
            f"Invalid collection_id format: '{collection_id}'"
        )
    collection_type = parts[1]
    namespace = parts[2]
    source_key = ":".join(parts[3:])
    return collection_type, namespace, source_key
