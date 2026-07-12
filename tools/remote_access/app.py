"""Standalone FastAPI application for temporary QRP read-only access."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone
from typing import Any, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from auth import require_bearer_token
from config import DEFAULT_ROWS, MAX_OFFSET, MAX_ROWS, GatewaySettings, load_settings
from service import FILTER_OPERATORS, ReadOnlyDataService
from session import CAPABILITY_MAX_ROWS, CapabilitySession, load_session, SESSION_FILE, DEFAULT_RUNTIME_DIR


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


# ---------------------------------------------------------------------------
# Capability session helpers
# ---------------------------------------------------------------------------

def _validate_capability_session(session_id: str) -> CapabilitySession:
    """Load and validate a capability session, raising on any failure."""
    session = load_session()
    if session is None:
        raise HTTPException(
            status_code=404,
            detail="Capability session is not active. Use share_start.sh to create one.",
        )
    if session.session_id != session_id:
        raise HTTPException(
            status_code=401,
            detail="Invalid capability session identifier.",
        )
    if session.revoked:
        raise HTTPException(
            status_code=410,
            detail="Capability session has been revoked.",
        )
    try:
        expires = datetime.fromisoformat(session.expires_at)
    except (ValueError, TypeError):
        raise HTTPException(status_code=500, detail="Internal session error.")
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) >= expires:
        raise HTTPException(
            status_code=410,
            detail="Capability session has expired.",
        )
    return session


def _parse_capability_filters(raw: str | None) -> list[dict[str, Any]]:
    """Parse and validate filters from URL-encoded JSON query parameter."""
    if raw is None:
        return []
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        raise HTTPException(
            status_code=400,
            detail="filters must be a valid URL-encoded JSON array.",
        )
    if not isinstance(parsed, list):
        raise HTTPException(
            status_code=400,
            detail="filters must be a JSON array.",
        )
    if len(parsed) > 20:
        raise HTTPException(
            status_code=400,
            detail="At most 20 filters are allowed.",
        )
    result: list[dict[str, Any]] = []
    for i, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise HTTPException(
                status_code=400,
                detail=f"filters[{i}] must be a JSON object.",
            )
        if "field" not in item or "operator" not in item or "value" not in item:
            raise HTTPException(
                status_code=400,
                detail=f"filters[{i}] must contain 'field', 'operator', and 'value'.",
            )
        field = str(item["field"])
        operator = str(item["operator"])
        if operator not in FILTER_OPERATORS:
            raise HTTPException(
                status_code=400,
                detail=f"filters[{i}]: operator '{operator}' is not allowed.",
            )
        if operator == "in":
            if not isinstance(item["value"], list) or len(item["value"]) == 0 or len(item["value"]) > 50:
                raise HTTPException(
                    status_code=400,
                    detail=f"filters[{i}]: 'in' filter must contain 1 to 50 values.",
                )
        else:
            if isinstance(item["value"], (dict, list)):
                raise HTTPException(
                    status_code=400,
                    detail=f"filters[{i}]: filter values must be scalar except for 'in'.",
                )
        result.append({"field": field, "operator": operator, "value": item["value"]})
    return result


def _parse_capability_fields(raw: str | None) -> list[str] | None:
    """Parse comma-separated fields from query parameter."""
    if raw is None:
        return None
    parts = [f.strip() for f in raw.split(",") if f.strip()]
    if not parts:
        return None
    return parts


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(settings: GatewaySettings | None = None, capability_session: CapabilitySession | None = None) -> FastAPI:
    runtime_settings = settings or load_settings()
    service = ReadOnlyDataService(str(runtime_settings.database_path))
    require_auth = require_bearer_token(runtime_settings.token)
    app = FastAPI(
        title="QRP Temporary Read-Only Gateway",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    # Capability session: use injected (for tests) or load from disk
    _injected_session: CapabilitySession | None = capability_session

    def _check_session(session_id: str) -> CapabilitySession:
        """Validate capability session — uses injected session or on-disk file."""
        session = _injected_session if _injected_session is not None else load_session()
        if session is None:
            raise HTTPException(
                status_code=404,
                detail="Capability session is not active. Use share_start.sh to create one.",
            )
        if session.session_id != session_id:
            raise HTTPException(
                status_code=401,
                detail="Invalid capability session identifier.",
            )
        if session.revoked:
            raise HTTPException(
                status_code=410,
                detail="Capability session has been revoked.",
            )
        try:
            expires = datetime.fromisoformat(session.expires_at)
        except (ValueError, TypeError):
            raise HTTPException(status_code=500, detail="Internal session error.")
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) >= expires:
            raise HTTPException(
                status_code=410,
                detail="Capability session has expired.",
            )
        return session

    @app.middleware("http")
    async def request_timeout(request: Request, call_next):
        try:
            return await asyncio.wait_for(
                call_next(request), timeout=runtime_settings.request_timeout_seconds
            )
        except TimeoutError:
            return JSONResponse(status_code=504, content={"detail": "Request timed out."})

    # -----------------------------------------------------------------------
    # Bearer-token API (unchanged)
    # -----------------------------------------------------------------------

    @app.get("/health")
    def health() -> dict[str, Any]:
        """Unauthenticated, non-sensitive liveness and read-query check."""
        return {"service": "qrp-temporary-read-only-gateway", **service.check_health()}

    @app.get("/v1/meta", dependencies=[Depends(require_auth)])
    def metadata() -> dict[str, Any]:
        return {
            "purpose": "Temporary development-only, read-only access to selected QRP market research data.",
            "mode": "standalone temporary read-only gateway",
            "authentication": "Authorization: Bearer ***",
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

    # -----------------------------------------------------------------------
    # Capability-session /share/ API (GET-only, for ChatGPT web browsing)
    # -----------------------------------------------------------------------

    @app.get("/share/{session_id}/meta")
    def share_meta(session_id: str) -> dict[str, Any]:
        session = _check_session(session_id)
        return {
            "purpose": "Temporary development-only, read-only access to selected QRP market research data.",
            "mode": "URL-based capability session (ChatGPT compatible)",
            "session_expires_at": session.expires_at,
            "session_revoked": session.revoked,
            "limits": {
                "max_rows_per_request": CAPABILITY_MAX_ROWS,
                "max_offset": MAX_OFFSET,
                "arbitrary_sql": False,
            },
            "endpoints": [
                "GET /share/{session_id}/meta",
                "GET /share/{session_id}/tables",
                "GET /share/{session_id}/tables/{table}/schema",
                "GET /share/{session_id}/tables/{table}/overview",
                "GET /share/{session_id}/tables/{table}/query?fields=...&filters=[...]&date_from=...&limit=...",
            ],
            "data_scope": "Explicit contracts-based allowlist only; no local configuration, credentials, logs, files, or execution history.",
        }

    @app.get("/share/{session_id}/tables")
    def share_tables(session_id: str) -> dict[str, Any]:
        _check_session(session_id)
        return {"tables": service.list_tables()}

    @app.get("/share/{session_id}/tables/{table_name}/schema")
    def share_table_schema(session_id: str, table_name: str) -> dict[str, Any]:
        _check_session(session_id)
        return service.get_schema(table_name)

    @app.get("/share/{session_id}/tables/{table_name}/overview")
    def share_table_overview(session_id: str, table_name: str) -> dict[str, Any]:
        _check_session(session_id)
        return service.overview(table_name)

    @app.get("/share/{session_id}/tables/{table_name}/query")
    def share_table_query(
        session_id: str,
        table_name: str,
        fields: str | None = Query(default=None, description="Comma-separated field names"),
        filters: str | None = Query(default=None, description="URL-encoded JSON array of filter objects"),
        date_from: date | None = Query(default=None),
        date_to: date | None = Query(default=None),
        order_by: str | None = Query(default=None, max_length=80),
        order_direction: Literal["asc", "desc"] = Query(default="asc"),
        limit: int = Query(default=CAPABILITY_MAX_ROWS, ge=1, le=CAPABILITY_MAX_ROWS),
        offset: int = Query(default=0, ge=0, le=MAX_OFFSET),
    ) -> dict[str, Any]:
        _check_session(session_id)

        parsed_filters = _parse_capability_filters(filters)
        parsed_fields = _parse_capability_fields(fields)

        if date_from and date_to and date_from > date_to:
            raise HTTPException(
                status_code=400,
                detail="date_from must not be after date_to.",
            )

        return service.query(
            table_name=table_name,
            fields=parsed_fields,
            filters=parsed_filters,
            date_from=date_from,
            date_to=date_to,
            order_by=order_by,
            order_direction=order_direction,
            limit=limit,
            offset=offset,
        )

    # -----------------------------------------------------------------------
    # Exception handlers
    # -----------------------------------------------------------------------

    @app.exception_handler(HTTPException)
    async def http_exception_handler(_: Request, exc: HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers)

    return app