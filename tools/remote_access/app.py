"""Standalone FastAPI application for temporary QRP read-only access."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any, Literal

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from auth import require_bearer_token
from config import DEFAULT_ROWS, MAX_OFFSET, MAX_ROWS, GatewaySettings, load_settings
from service import ReadOnlyDataService


class FilterSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str = Field(min_length=1, max_length=80)
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in"]
    value: Any


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fields: list[str] | None = Field(default=None, max_length=80)
    filters: list[FilterSpec] = Field(default_factory=list, max_length=20)
    date_from: date | None = None
    date_to: date | None = None
    order_by: str | None = Field(default=None, max_length=80)
    order_direction: Literal["asc", "desc"] = "asc"
    limit: int = Field(default=DEFAULT_ROWS, ge=1, le=MAX_ROWS)
    offset: int = Field(default=0, ge=0, le=MAX_OFFSET)

    @model_validator(mode="after")
    def validate_date_range(self) -> "QueryRequest":
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from must not be after date_to")
        return self


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    runtime_settings = settings or load_settings()
    service = ReadOnlyDataService(str(runtime_settings.database_path))
    require_auth = require_bearer_token(runtime_settings.token)
    app = FastAPI(
        title="QRP Temporary Read-Only Gateway",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def request_timeout(request: Request, call_next):
        try:
            return await asyncio.wait_for(
                call_next(request), timeout=runtime_settings.request_timeout_seconds
            )
        except TimeoutError:
            return JSONResponse(status_code=504, content={"detail": "Request timed out."})

    @app.get("/health")
    def health() -> dict[str, Any]:
        """Unauthenticated, non-sensitive liveness and read-query check."""
        return {"service": "qrp-temporary-read-only-gateway", **service.check_health()}

    @app.get("/v1/meta", dependencies=[Depends(require_auth)])
    def metadata() -> dict[str, Any]:
        return {
            "purpose": "Temporary development-only, read-only access to selected QRP market research data.",
            "mode": "standalone temporary read-only gateway",
            "authentication": "Authorization: Bearer <token>",
            "limits": {
                "max_rows_per_request": MAX_ROWS,
                "max_offset": MAX_OFFSET,
                "request_timeout_seconds": runtime_settings.request_timeout_seconds,
                "arbitrary_sql": False,
            },
            "endpoints": [
                "GET /health",
                "GET /v1/meta",
                "GET /v1/tables",
                "GET /v1/tables/{table}/schema",
                "GET /v1/tables/{table}/overview",
                "POST /v1/tables/{table}/query",
            ],
            "data_scope": "Explicit contracts-based allowlist only; no local configuration, credentials, logs, files, or execution history.",
        }

    @app.get("/v1/tables", dependencies=[Depends(require_auth)])
    def tables() -> dict[str, Any]:
        return {"tables": service.list_tables()}

    @app.get("/v1/tables/{table_name}/schema", dependencies=[Depends(require_auth)])
    def table_schema(table_name: str) -> dict[str, Any]:
        return service.get_schema(table_name)

    @app.get("/v1/tables/{table_name}/overview", dependencies=[Depends(require_auth)])
    def table_overview(table_name: str) -> dict[str, Any]:
        return service.overview(table_name)

    @app.post("/v1/tables/{table_name}/query", dependencies=[Depends(require_auth)])
    def table_query(table_name: str, payload: QueryRequest) -> dict[str, Any]:
        return service.query(
            table_name=table_name,
            fields=payload.fields,
            filters=[item.model_dump() for item in payload.filters],
            date_from=payload.date_from,
            date_to=payload.date_to,
            order_by=payload.order_by,
            order_direction=payload.order_direction,
            limit=payload.limit,
            offset=payload.offset,
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(_: Request, exc: HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers)

    return app

