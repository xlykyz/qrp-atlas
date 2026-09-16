"""自定义策略沙盒 API（旁路研究能力，不落库）。

只服务前端研究台的 ``POST /api/custom-strategies/sandbox-run``：接收一段
Python 策略代码，在隔离子进程中基于只读行情即时计算并返回绩效与日志。

本路由不是正式回测产品路径：不创建 backtest task、不写 run 目录、不写数据库。
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from qrp_atlas.auth.dependencies import CurrentUser
from qrp_atlas.backtest.sandbox import DEFAULT_TIMEOUT_SEC, execute_sandbox_code

router = APIRouter(prefix="/api/custom-strategies", tags=["自定义策略沙盒"])


class SandboxRunRequest(BaseModel):
    code: str = Field(..., min_length=1, description="Python 策略源代码")
    start_date: str = Field(..., description="回测起始交易日 YYYY-MM-DD")
    end_date: str = Field(..., description="回测结束交易日 YYYY-MM-DD")
    initial_cash: float = Field(default=1_000_000.0, gt=0, description="初始本金")
    benchmark_id: str | None = Field(default=None, description="基准指数代码")


class SandboxRunResponse(BaseModel):
    success: bool
    summary: dict[str, Any] | None = None
    equity_points: list[dict[str, Any]] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)
    error_message: str | None = None
    duration_ms: int = 0


@router.post("/sandbox-run", response_model=SandboxRunResponse)
def run_sandbox(request: SandboxRunRequest, user: CurrentUser) -> SandboxRunResponse:
    """在隔离子进程中即时运行用户策略代码。

    使用同步路由：请求可能长达 ``DEFAULT_TIMEOUT_SEC`` 秒，交给 FastAPI 的
    线程池执行，不阻塞事件循环。失败一律如实返回，绝不伪造回测结果。
    """

    _ = user  # 沙盒无 owner 数据，但仍要求可信认证身份
    outcome = execute_sandbox_code(
        request.model_dump(), timeout_sec=DEFAULT_TIMEOUT_SEC
    )
    return SandboxRunResponse(**outcome)
