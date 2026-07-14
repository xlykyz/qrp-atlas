"""Read-only indicator and strategy catalog APIs."""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from qrp_atlas.backtest.product import (
    IndicatorCatalogItem,
    StrategyCatalogItem,
    get_strategy_catalog_item,
    list_indicator_catalog,
    list_strategy_catalog,
)

router = APIRouter(prefix="/api", tags=["目录"])


@router.get("/indicators", response_model=List[IndicatorCatalogItem])
def api_list_indicators():
    """List registered indicators and calculation/factor catalog entries."""
    return list_indicator_catalog()


@router.get("/strategies", response_model=List[StrategyCatalogItem])
def api_list_strategies(
    all: bool = Query(
        False,
        description="When true, include strategies outside the 07-A product path.",
    ),
):
    """List strategy definitions from the live registry."""
    return list_strategy_catalog(product_only=not all)


@router.get("/strategies/{code}", response_model=StrategyCatalogItem)
def api_get_strategy(
    code: str,
    version: Optional[str] = Query(None, description="Optional strategy version"),
):
    """Return one strategy catalog item including parameter schema."""
    try:
        return get_strategy_catalog_item(code, version)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
