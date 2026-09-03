"""Domain service managing StockCollection and Theme lifecycle."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timezone
import uuid

import duckdb

from qrp_atlas.contracts.stock_collection import (
    CollectionScope,
    CollectionStatus,
    CollectionType,
    MembershipModel,
)

from .identity import make_collection_id
from .models import (
    StockCollectionError,
    StockCollectionRecord,
    ThemeMembershipRecord,
    ThemeRecord,
)
from .repository import StockCollectionRepository
from .resolver import StockCollectionResolver

TimeProvider = Callable[[], datetime]


def system_clock() -> datetime:
    """Default production system clock: returns current UTC timestamp."""
    return datetime.now(timezone.utc)


class StockCollectionService:
    """Domain service for managing canonical collections, themes, and member revisions."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        clock: TimeProvider | None = None,
    ) -> None:
        self.con = con
        self.repo = StockCollectionRepository(con)
        self.resolver = StockCollectionResolver(con)
        self._clock: TimeProvider = clock or system_clock

    def _now(self) -> datetime:
        dt = self._clock()
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def create_canonical_theme(
        self,
        *,
        theme_name: str,
        source_key: str,
        namespace: str = "QRP",
        effective_from: date,
        available_trade_date: date,
        source: str = "MANUAL",
        source_record_id: str | None = None,
    ) -> tuple[ThemeRecord, StockCollectionRecord]:
        """Atomically create a 1:1 Canonical Theme and THEME StockCollection."""
        if not theme_name or not theme_name.strip():
            raise StockCollectionError("INVALID_THEME_NAME", "theme_name cannot be empty")
        if not source_key or not source_key.strip():
            raise StockCollectionError("INVALID_SOURCE_KEY", "source_key cannot be empty")

        collection_id = make_collection_id(CollectionType.THEME, namespace, source_key)
        theme_id = f"THM:{namespace.strip().upper()}:{source_key.strip().upper()}"

        # Collision check
        existing_colls = self.repo.get_collection_revisions(collection_id)
        if existing_colls:
            raise StockCollectionError(
                "COLLECTION_COLLISION", f"Collection {collection_id} already exists"
            )
        existing_themes = self.repo.get_theme_revisions(theme_id)
        if existing_themes:
            raise StockCollectionError(
                "THEME_COLLISION", f"Theme {theme_id} already exists"
            )

        now = self._now()
        coll_rec = StockCollectionRecord(
            collection_id=collection_id,
            collection_type=CollectionType.THEME,
            collection_scope=CollectionScope.CANONICAL,
            namespace=namespace.strip().upper(),
            source_key=source_key.strip().upper(),
            canonical_name=theme_name.strip(),
            membership_model=MembershipModel.INTERVAL,
            status=CollectionStatus.ACTIVE,
            effective_from=effective_from,
            effective_to=None,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=str(uuid.uuid4()),
            ingested_at=now,
        )

        theme_rec = ThemeRecord(
            theme_id=theme_id,
            collection_id=collection_id,
            canonical_name=theme_name.strip(),
            status=CollectionStatus.ACTIVE,
            effective_from=effective_from,
            effective_to=None,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=str(uuid.uuid4()),
            ingested_at=now,
        )

        self.repo.create_theme_collection_atomic(theme_rec, coll_rec)
        return theme_rec, coll_rec

    def _validate_membership_bounds(
        self,
        *,
        collection_id: str,
        theme_id: str,
        effective_from: date,
        effective_to: date | None,
    ) -> None:
        colls = self.repo.get_collection_revisions(collection_id)
        if not colls:
            raise StockCollectionError("COLLECTION_NOT_FOUND", f"Collection {collection_id} not found")
        themes = self.repo.get_theme_revisions(theme_id)
        if not themes:
            raise StockCollectionError("THEME_NOT_FOUND", f"Theme {theme_id} not found")

        coll = colls[-1]
        thm = themes[-1]

        if thm.collection_id != collection_id:
            raise StockCollectionError(
                "THEME_COLLECTION_MISMATCH",
                f"Theme {theme_id} belongs to {thm.collection_id}, not {collection_id}",
            )

        # 1:1 Invariant: Canonical Theme and StockCollection effective intervals must match
        if coll.effective_from != thm.effective_from or coll.effective_to != thm.effective_to:
            raise StockCollectionError(
                "PIT_INVARIANT_VIOLATION",
                f"Theme {theme_id} interval [{thm.effective_from}, {thm.effective_to}) does not match "
                f"Collection {collection_id} interval [{coll.effective_from}, {coll.effective_to})",
            )

        # Membership lifecycle must fall completely inside canonical Theme / Collection effective interval
        if effective_from < coll.effective_from:
            raise StockCollectionError(
                "MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE",
                f"membership.effective_from ({effective_from}) < canonical collection effective_from ({coll.effective_from})",
            )

        if coll.effective_to is not None:
            # Open-ended membership not allowed to span past closed Collection/Theme
            if effective_to is None:
                raise StockCollectionError(
                    "MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE",
                    f"membership must have effective_to <= canonical collection effective_to ({coll.effective_to})",
                )
            if effective_to > coll.effective_to:
                raise StockCollectionError(
                    "MEMBERSHIP_OUTSIDE_COLLECTION_LIFECYCLE",
                    f"membership.effective_to ({effective_to}) > canonical collection effective_to ({coll.effective_to})",
                )

    def add_member(
        self,
        *,
        theme_id: str,
        collection_id: str,
        asset_id: str,
        effective_from: date,
        effective_to: date | None = None,
        available_trade_date: date,
        weight: float | None = None,
        source: str = "MANUAL",
        source_record_id: str | None = None,
    ) -> ThemeMembershipRecord:
        """Add a new member to a theme, validating EQUITY domain and overlap."""
        # 1. Validate EQUITY
        if not self.repo.check_is_equity(asset_id):
            raise StockCollectionError(
                "NON_EQUITY_ASSET", f"Asset {asset_id} is not a valid EQUITY"
            )

        # 2. Validate Theme & Collection exist, match, and bound membership lifecycle
        self._validate_membership_bounds(
            collection_id=collection_id,
            theme_id=theme_id,
            effective_from=effective_from,
            effective_to=effective_to,
        )

        # 3. Validate Effective Interval
        if effective_to is not None and effective_to <= effective_from:
            raise StockCollectionError(
                "INVALID_EFFECTIVE_INTERVAL",
                f"effective_to ({effective_to}) must be > effective_from ({effective_from})",
            )

        # 4. Validate Non-overlapping with existing lifecycles
        existing_lifecycles = self.repo.get_asset_memberships(collection_id, asset_id)
        for lc in existing_lifecycles:
            # Check overlap: max(from1, from2) < min(to1, to2)
            lc_to = lc.effective_to if lc.effective_to is not None else date(9999, 12, 31)
            new_to = effective_to if effective_to is not None else date(9999, 12, 31)
            if max(effective_from, lc.effective_from) < min(new_to, lc_to):
                raise StockCollectionError(
                    "OVERLAPPING_MEMBERSHIP_LIFECYCLE",
                    f"Asset {asset_id} interval [{effective_from}, {effective_to}) overlaps with existing [{lc.effective_from}, {lc.effective_to})",
                )

        membership_id = f"MEM:{theme_id}:{asset_id}:{uuid.uuid4().hex[:8].upper()}"
        now = self._now()
        record = ThemeMembershipRecord(
            membership_id=membership_id,
            theme_id=theme_id,
            collection_id=collection_id,
            asset_id=asset_id,
            weight=weight,
            effective_from=effective_from,
            effective_to=effective_to,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id,
            revision_id=str(uuid.uuid4()),
            ingested_at=now,
        )
        self.repo.append_membership_revisions([record])
        return record

    def remove_member(
        self,
        *,
        membership_id: str,
        removal_date: date,
        available_trade_date: date,
        source: str = "MANUAL",
        source_record_id: str | None = None,
    ) -> ThemeMembershipRecord:
        """Close an existing membership lifecycle, keeping identity immutable."""
        revisions = self.repo.get_membership_revisions(membership_id)
        if not revisions:
            raise StockCollectionError(
                "MEMBERSHIP_NOT_FOUND", f"Membership {membership_id} not found"
            )
        latest = revisions[-1]

        if removal_date <= latest.effective_from:
            raise StockCollectionError(
                "INVALID_EFFECTIVE_INTERVAL",
                f"removal_date ({removal_date}) must be > effective_from ({latest.effective_from})",
            )

        now = self._now()
        record = ThemeMembershipRecord(
            membership_id=latest.membership_id,
            theme_id=latest.theme_id,
            collection_id=latest.collection_id,
            asset_id=latest.asset_id,
            weight=latest.weight,
            effective_from=latest.effective_from,
            effective_to=removal_date,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id or latest.source_record_id,
            revision_id=str(uuid.uuid4()),
            ingested_at=now,
        )
        self.repo.append_membership_revisions([record])
        return record

    def revise_member_late(
        self,
        *,
        membership_id: str,
        effective_from: date | None = None,
        effective_to: date | None | object = ...,
        available_trade_date: date,
        source: str = "MANUAL_REVISION",
        source_record_id: str | None = None,
    ) -> ThemeMembershipRecord:
        """Create a late revision for an existing lifecycle, preserving immutable identities."""
        revisions = self.repo.get_membership_revisions(membership_id)
        if not revisions:
            raise StockCollectionError(
                "MEMBERSHIP_NOT_FOUND", f"Membership {membership_id} not found"
            )
        latest = revisions[-1]

        # Allow revising effective_from if provided, otherwise inherit old value
        eff_from = effective_from if effective_from is not None else latest.effective_from
        eff_to = latest.effective_to if effective_to is ... else effective_to

        # Validate bounds against canonical Theme & Collection lifecycle
        self._validate_membership_bounds(
            collection_id=latest.collection_id,
            theme_id=latest.theme_id,
            effective_from=eff_from,
            effective_to=eff_to,
        )

        if eff_to is not None and eff_to <= eff_from:
            raise StockCollectionError(
                "INVALID_EFFECTIVE_INTERVAL",
                f"effective_to ({eff_to}) must be > effective_from ({eff_from})",
            )

        # Check overlap with other lifecycles of same asset
        other_lifecycles = [
            lc for lc in self.repo.get_asset_memberships(latest.collection_id, latest.asset_id)
            if lc.membership_id != membership_id
        ]
        new_to = eff_to if eff_to is not None else date(9999, 12, 31)
        for lc in other_lifecycles:
            lc_to = lc.effective_to if lc.effective_to is not None else date(9999, 12, 31)
            if max(eff_from, lc.effective_from) < min(new_to, lc_to):
                raise StockCollectionError(
                    "OVERLAPPING_MEMBERSHIP_LIFECYCLE",
                    f"Late revision overlaps with other lifecycle [{lc.effective_from}, {lc.effective_to})",
                )

        now = self._now()
        record = ThemeMembershipRecord(
            membership_id=latest.membership_id,
            theme_id=latest.theme_id,
            collection_id=latest.collection_id,
            asset_id=latest.asset_id,
            weight=latest.weight,
            effective_from=eff_from,
            effective_to=eff_to,
            available_trade_date=available_trade_date,
            source=source,
            source_record_id=source_record_id or latest.source_record_id,
            revision_id=str(uuid.uuid4()),
            ingested_at=now,
        )
        self.repo.append_membership_revisions([record])
        return record

    def reenter_member(
        self,
        *,
        theme_id: str,
        collection_id: str,
        asset_id: str,
        effective_from: date,
        effective_to: date | None = None,
        available_trade_date: date,
        weight: float | None = None,
        source: str = "MANUAL_REENTRY",
        source_record_id: str | None = None,
    ) -> ThemeMembershipRecord:
        """Re-enter an asset after previous lifecycle closed, creating a NEW membership_id."""
        existing_lifecycles = self.repo.get_asset_memberships(collection_id, asset_id)
        for lc in existing_lifecycles:
            if lc.effective_to is None or lc.effective_to > effective_from:
                raise StockCollectionError(
                    "PREVIOUS_LIFECYCLE_NOT_CLOSED",
                    f"Cannot reenter asset {asset_id}: existing lifecycle "
                    f"[{lc.effective_from}, {lc.effective_to}) must be closed before {effective_from}",
                )

        return self.add_member(
            theme_id=theme_id,
            collection_id=collection_id,
            asset_id=asset_id,
            effective_from=effective_from,
            effective_to=effective_to,
            available_trade_date=available_trade_date,
            weight=weight,
            source=source,
            source_record_id=source_record_id,
        )

    def add_members_batch(
        self,
        *,
        theme_id: str,
        collection_id: str,
        member_entries: Sequence[dict[str, Any]],
        available_trade_date: date,
        source: str = "BATCH_MANUAL",
    ) -> list[ThemeMembershipRecord]:
        """Atomically add multiple members, rolling back all if any validation fails."""
        colls = self.repo.get_collection_revisions(collection_id)
        if not colls:
            raise StockCollectionError("COLLECTION_NOT_FOUND", f"Collection {collection_id} not found")
        themes = self.repo.get_theme_revisions(theme_id)
        if not themes:
            raise StockCollectionError("THEME_NOT_FOUND", f"Theme {theme_id} not found")
        if themes[-1].collection_id != collection_id:
            raise StockCollectionError(
                "THEME_COLLECTION_MISMATCH",
                f"Theme {theme_id} belongs to {themes[-1].collection_id}, not {collection_id}",
            )

        records_to_append: list[ThemeMembershipRecord] = []
        now = self._now()
        all_intervals = [

            (lc.asset_id, lc.effective_from, lc.effective_to or date(9999, 12, 31))
            for lc in self.repo.get_asset_memberships(collection_id, None)
        ]

        for entry in member_entries:
            asset_id = str(entry["asset_id"])
            eff_from = entry["effective_from"]
            eff_to = entry.get("effective_to")

            self._validate_membership_bounds(
                collection_id=collection_id,
                theme_id=theme_id,
                effective_from=eff_from,
                effective_to=eff_to,
            )

            if not self.repo.check_is_equity(asset_id):
                raise StockCollectionError("NON_EQUITY_ASSET", f"Asset {asset_id} is not a valid EQUITY")

            if eff_to is not None and eff_to <= eff_from:
                raise StockCollectionError(
                    "INVALID_EFFECTIVE_INTERVAL",
                    f"effective_to ({eff_to}) must be > effective_from ({eff_from}) for {asset_id}",
                )

            new_to = eff_to if eff_to is not None else date(9999, 12, 31)
            for ex_asset, ex_from, ex_to in all_intervals:
                if ex_asset == asset_id and max(eff_from, ex_from) < min(new_to, ex_to):
                    raise StockCollectionError(
                        "OVERLAPPING_MEMBERSHIP_LIFECYCLE",
                        f"Asset {asset_id} interval overlaps with [{ex_from}, {ex_to})",
                    )
            all_intervals.append((asset_id, eff_from, new_to))

            membership_id = f"MEM:{theme_id}:{asset_id}:{uuid.uuid4().hex[:8].upper()}"
            record = ThemeMembershipRecord(
                membership_id=membership_id,
                theme_id=theme_id,
                collection_id=collection_id,
                asset_id=asset_id,
                weight=None,
                effective_from=eff_from,
                effective_to=eff_to,
                available_trade_date=available_trade_date,
                source=source,
                source_record_id=entry.get("source_record_id"),
                revision_id=str(uuid.uuid4()),
                ingested_at=now,
            )
            records_to_append.append(record)

        self.repo.append_membership_revisions(records_to_append)
        return records_to_append
