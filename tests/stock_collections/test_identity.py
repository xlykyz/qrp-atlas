"""Tests for StockCollection identity generation and validation."""

from __future__ import annotations

import pytest

from qrp_atlas.contracts.stock_collection import CollectionType
from qrp_atlas.stock_collections.identity import (
    StockCollectionIdentityError,
    make_collection_id,
    parse_collection_id,
)


def test_make_collection_id_deterministic():
    cid = make_collection_id("THEME", "QRP", "AI_COMPUTE")
    assert cid == "COLL:THEME:QRP:AI_COMPUTE"
    # Enum support
    cid_enum = make_collection_id(CollectionType.THEME, "qrp", "ai_compute")
    assert cid_enum == "COLL:THEME:QRP:AI_COMPUTE"


def test_theme_rename_does_not_change_collection_id():
    """Theme display name is decoupled from collection_id."""
    initial_cid = make_collection_id("THEME", "QRP", "ROBOTICS")
    # Even if name changes from "机器人生态" to "具身智能机器人", source_key and collection_id remain stable
    assert initial_cid == "COLL:THEME:QRP:ROBOTICS"


def test_make_collection_id_rejects_empty_or_invalid():
    with pytest.raises(StockCollectionIdentityError):
        make_collection_id("", "QRP", "KEY")
    with pytest.raises(StockCollectionIdentityError):
        make_collection_id("THEME", "", "KEY")
    with pytest.raises(StockCollectionIdentityError):
        make_collection_id("THEME", "QRP", "")
    with pytest.raises(StockCollectionIdentityError):
        make_collection_id("THEME", "QRP", "KEY WITH SPACES")


def test_parse_collection_id():
    ctype, ns, sk = parse_collection_id("COLL:THEME:QRP:AI_COMPUTE")
    assert ctype == "THEME"
    assert ns == "QRP"
    assert sk == "AI_COMPUTE"

    with pytest.raises(StockCollectionIdentityError):
        parse_collection_id("INVALID:FORMAT")
