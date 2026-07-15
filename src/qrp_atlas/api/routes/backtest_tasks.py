"""Product backtest task create/list/get APIs."""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException
from qrp_atlas.auth.dependencies import CurrentUser

from qrp_atlas.backtest.product import (
    BacktestTaskRecord,
    CreateBacktestTaskRequest,
    CreateBacktestTaskResponse,
    get_product_service,
)
from qrp_atlas.backtest.product import service as product_service

router = APIRouter(prefix="/api/backtest", tags=["回测任务"])


@router.post("/tasks", response_model=CreateBacktestTaskResponse)
def api_create_task(request: CreateBacktestTaskRequest, user: CurrentUser):
    """Create and (by default) execute a real portfolio backtest task."""
    service = get_product_service()
    try:
        return service.create_task(request, owner_user_id=str(user.user_id))
    except product_service.BacktestTaskValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks", response_model=List[BacktestTaskRecord])
def api_list_tasks(user: CurrentUser):
    """List persisted product tasks, newest first."""
    return get_product_service().list_tasks(owner_user_id=str(user.user_id))


@router.get("/tasks/{task_id}", response_model=BacktestTaskRecord)
def api_get_task(task_id: str, user: CurrentUser):
    """Fetch one product task by id."""
    try:
        return get_product_service().get_task(task_id, owner_user_id=str(user.user_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
