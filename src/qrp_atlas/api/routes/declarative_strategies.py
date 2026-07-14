"""Declarative strategy product APIs (validate/create/list/get/version/archive)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from qrp_atlas.auth.dependencies import CurrentUser
from qrp_atlas.strategies.declarative.store import (
    DeclarativeStoreError,
    get_declarative_store,
)

router = APIRouter(prefix="/api/declarative-strategies", tags=["声明式策略"])


class DeclarativeDefinitionBody(BaseModel):
    definition: dict[str, Any] = Field(default_factory=dict)


class StatusBody(BaseModel):
    status: str


def _owner(user: CurrentUser) -> str:
    return str(user.user_id)


@router.post("/validate")
def validate_definition(body: DeclarativeDefinitionBody, user: CurrentUser) -> dict[str, Any]:
    _ = user
    try:
        return get_declarative_store().validate(body.definition)
    except DeclarativeStoreError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_definition(body: DeclarativeDefinitionBody, user: CurrentUser) -> dict[str, Any]:
    try:
        record = get_declarative_store().create(body.definition, owner_user_id=_owner(user))
        return record.to_dict()
    except DeclarativeStoreError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("")
def list_definitions(user: CurrentUser, include_archived: bool = False) -> list[dict[str, Any]]:
    records = get_declarative_store().list(
        owner_user_id=_owner(user), include_archived=include_archived
    )
    return [r.to_dict() for r in records]


@router.get("/{code}/{version}")
def get_definition(code: str, version: str, user: CurrentUser) -> dict[str, Any]:
    try:
        record = get_declarative_store().get(code, version, owner_user_id=_owner(user))
        return record.to_dict()
    except DeclarativeStoreError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{code}/versions")
def create_version(code: str, body: DeclarativeDefinitionBody, user: CurrentUser) -> dict[str, Any]:
    try:
        record = get_declarative_store().create_new_version(
            code, body.definition, owner_user_id=_owner(user)
        )
        return record.to_dict()
    except DeclarativeStoreError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{code}/{version}/status")
def set_status(code: str, version: str, body: StatusBody, user: CurrentUser) -> dict[str, Any]:
    try:
        record = get_declarative_store().set_status(
            code, version, owner_user_id=_owner(user), status=body.status
        )
        return record.to_dict()
    except DeclarativeStoreError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
