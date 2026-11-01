"""Bounded SDK acquisition with immutable raw pages before interpretation.

The caller supplies a configured client. No token, proxy, URL, or environment
is discovered here. Failure diagnostics deliberately exclude provider text.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from qrp_atlas.contracts.lake_core import CORE_DATASETS, STOCK_BASIC_PARTITIONS


class AcquisitionError(RuntimeError):
    def __init__(self, code: str, *, batch_ids: tuple[str, ...] = ()) -> None:
        super().__init__(code)
        self.code = code
        self.batch_ids = batch_ids


@dataclass(frozen=True)
class AcquiredBatch:
    batch_id: str
    page_ids: tuple[str, ...]
    complete: bool
    observed_at: datetime


class SDKProvider:
    def __init__(self, client: Any, raw_store: Any, *, sdk_version: str,
                 page_size: int = 4000, max_pages: int = 100) -> None:
        if page_size < 1 or max_pages < 1:
            raise ValueError("page_size and max_pages must be positive")
        self.client = client
        self.raw_store = raw_store
        self.sdk_version = sdk_version
        self.page_size = page_size
        self.max_pages = max_pages

    def acquire(self, dataset: str, parameters: dict[str, Any]) -> AcquiredBatch:
        """Use the endpoint's limit/offset contract, stopping on a short page.

        Repeated/overlapping pages are rejected later by business keys. A page
        at the configured maximum never proves completeness. The final batch
        contains the concatenated untouched SDK values, not normalized values.
        """
        spec = CORE_DATASETS[dataset]
        if "offset" in parameters or "limit" in parameters:
            raise ValueError("pagination is owned by SDKProvider")
        request_groups = [dict(parameters)]
        if dataset == "stock_info":
            request_groups = [dict(parameters, list_status=status, exchange=exchange,
                                   fields=",".join(spec.required_provider_fields))
                              for status, exchange in STOCK_BASIC_PARTITIONS]
        pages: list[pd.DataFrame] = []
        page_ids: list[str] = []
        observed = datetime.now(timezone.utc)
        for group in request_groups:
            seen: set[str] = set()
            for page in range(self.max_pages):
                query = dict(group, limit=self.page_size, offset=page * self.page_size)
                try:
                    frame = getattr(self.client, spec.endpoint)(**query)
                except Exception:
                    failed = self.raw_store.save_frame(
                        "tushare", dataset, pd.DataFrame(), parameters=query,
                        complete=False, status="failed", sdk_version=self.sdk_version)
                    page_ids.append(failed.batch_id)
                    raise AcquisitionError("PROVIDER_REQUEST_FAILED", batch_ids=tuple(page_ids)) from None
                valid = isinstance(frame, pd.DataFrame)
                raw = self.raw_store.save_frame(
                    "tushare", dataset, frame if valid else pd.DataFrame(),
                    parameters=query, complete=False,
                    status="partial" if valid else "failed", sdk_version=self.sdk_version)
                page_ids.append(raw.batch_id)
                if not valid:
                    raise AcquisitionError("PROVIDER_RESPONSE_INVALID", batch_ids=tuple(page_ids))
                if len(frame) > self.page_size:
                    raise AcquisitionError("PROVIDER_PAGE_LIMIT_IGNORED", batch_ids=tuple(page_ids))
                if len(frame):
                    if raw.checksum in seen:
                        raise AcquisitionError("PROVIDER_PAGE_REPEATED", batch_ids=tuple(page_ids))
                    seen.add(raw.checksum)
                    pages.append(frame.copy(deep=True))
                if len(frame) < self.page_size:
                    break
            else:
                raise AcquisitionError("PROVIDER_PAGINATION_INCOMPLETE", batch_ids=tuple(page_ids))
        combined = pd.concat(pages, ignore_index=True, sort=False) if pages else pd.DataFrame()
        batch = self.raw_store.save_frame(
            "tushare", dataset, combined,
            parameters={"query": parameters, "page_ids": page_ids,
                        "completion": "all partitions exhausted by short page"},
            complete=True, observed_at=observed, sdk_version=self.sdk_version,
            status="complete")
        return AcquiredBatch(batch.batch_id, tuple(page_ids), True, observed)


def configured_provider(settings: Any, raw_store: Any, **kwargs: Any) -> SDKProvider:
    """Construct lazily so loading the core does not discover credentials."""
    from importlib.metadata import version
    from qrp_atlas.config.tushare_client import get_tushare_pro
    return SDKProvider(get_tushare_pro(settings=settings), raw_store,
                       sdk_version=version("tushare"), **kwargs)
