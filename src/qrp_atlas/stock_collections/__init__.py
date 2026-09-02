"""StockCollection domain package for QRP Atlas v1.1."""

from .adapters.theme import ThemeAdapter
from .identity import (
    StockCollectionIdentityError,
    make_collection_id,
    parse_collection_id,
)
from .models import (
    CollectionVersionContext,
    MembershipExplanation,
    ResolvedMember,
    StockCollectionError,
    StockCollectionErrorCode,
    StockCollectionQueryContext,
    StockCollectionRecord,
    ThemeMembershipRecord,
    ThemeRecord,
)
from .repository import StockCollectionRepository
from .resolver import StockCollectionResolver
from .service import StockCollectionService

__all__ = [
    "StockCollectionIdentityError",
    "make_collection_id",
    "parse_collection_id",
    "StockCollectionErrorCode",
    "StockCollectionError",
    "StockCollectionRecord",
    "ThemeRecord",
    "ThemeMembershipRecord",
    "ResolvedMember",
    "MembershipExplanation",
    "CollectionVersionContext",
    "StockCollectionQueryContext",
    "StockCollectionRepository",
    "ThemeAdapter",
    "StockCollectionResolver",
    "StockCollectionService",
]
