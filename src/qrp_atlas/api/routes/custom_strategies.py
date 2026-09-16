"""自定义策略沙盒 API（旁路研究能力，不落库）。

服务前端研究台的两个端点：

- ``POST /api/custom-strategies/sandbox-run``：接收一段 Python 策略代码，在隔离
  子进程中基于只读行情即时计算并返回绩效与日志；
- ``POST /api/custom-strategies/sandbox-benchmark``：不执行任何用户代码，只按既有
  组合净值曲线重算基准/超额指标（切换基准无需重跑策略）。

本路由不是正式回测产品路径：不创建 backtest task、不写 run 目录、不写数据库。
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from qrp_atlas.auth.dependencies import CurrentUser
from qrp_atlas.backtest.sandbox import (
    DEFAULT_TIMEOUT_SEC,
    execute_sandbox_code,
    recompute_benchmark,
)

router = APIRouter(prefix="/api/custom-strategies", tags=["自定义策略沙盒"])


class SandboxRunRequest(BaseModel):
    code: str = Field(..., min_length=1, description="Python 策略源代码")
    start_date: str = Field(..., description="回测起始交易日 YYYY-MM-DD")
    end_date: str = Field(..., description="回测结束交易日 YYYY-MM-DD")
    initial_cash: float = Field(default=1_000_000.0, gt=0, description="初始本金")
    benchmark_id: str | None = Field(default=None, description="基准指数代码")


class SandboxBenchmarkSeriesPoint(BaseModel):
    date: str = Field(..., description="交易日 YYYY-MM-DD")
    benchmark_cumulative_return_pct: float | None = Field(
        default=None, description="基准累计收益（%）"
    )
    portfolio_cumulative_return_pct: float | None = Field(
        default=None, description="组合累计收益（%）"
    )
    excess_percentage_point_pct: float | None = Field(
        default=None, description="超额收益（百分点，%）"
    )


class SandboxRunResponse(BaseModel):
    success: bool
    summary: dict[str, Any] | None = None
    equity_points: list[dict[str, Any]] = Field(default_factory=list)
    series: list[SandboxBenchmarkSeriesPoint] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)
    error_message: str | None = None
    duration_ms: int = 0


class SandboxEquityPoint(BaseModel):
    date: str = Field(..., description="交易日 YYYY-MM-DD")
    equity: float | None = Field(default=None, description="当日权益")
    drawdown_pct: float | None = Field(default=None, description="当日回撤（%）")


class SandboxBenchmarkRequest(BaseModel):
    equity_points: list[SandboxEquityPoint] = Field(
        default_factory=list,
        description="与 sandbox-run 响应中的 equity_points 同构",
    )
    benchmark_id: str | None = Field(default=None, description="目标基准指数代码")


class SandboxBenchmarkResponse(BaseModel):
    benchmark_id: str | None = None
    benchmark_total_return_pct: float | None = None
    portfolio_total_return_pct: float | None = None
    excess_percentage_point_pct: float | None = None
    relative_return_pct: float | None = None
    excess_total_return_pct: float | None = None
    full_range_excess_available: bool | None = None
    benchmark_sharpe: float | None = None
    excess_sharpe: float | None = None
    daily_active_sharpe: float | None = None
    series: list[SandboxBenchmarkSeriesPoint] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)


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


@router.post("/sandbox-benchmark", response_model=SandboxBenchmarkResponse)
def recompute_sandbox_benchmark(
    request: SandboxBenchmarkRequest, user: CurrentUser
) -> SandboxBenchmarkResponse:
    """基于既有组合净值曲线重算基准/超额指标，不执行任何用户代码。

    只读查询目标指数行情后复用既有基准口径；无 RCE 面，也不产生任何落盘。
    常规请求超时即可，不需要 ``sandbox-run`` 的硬超时。
    """

    _ = user  # 沙盒无 owner 数据，但仍要求可信认证身份
    if not request.equity_points:
        raise HTTPException(status_code=400, detail="equity_points 不能为空。")
    try:
        outcome = recompute_benchmark(
            equity_points=[point.model_dump() for point in request.equity_points],
            benchmark_id=request.benchmark_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SandboxBenchmarkResponse(**outcome)
