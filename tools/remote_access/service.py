"""Read-only, parameterized DuckDB access for explicitly allowed contracts."""

from __future__ import annotations

import datetime as dt
import math
from collections.abc import Iterable
from typing import Any

import duckdb
from fastapi import HTTPException, status

from qrp_atlas.contracts import TABLE_BY_NAME

from config import FIELD_DESCRIPTIONS, REMOTE_TABLES


FILTER_OPERATORS = {
    "eq": "=",
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "in": "IN",
}


def _bad_request(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


def _quote_identifier(identifier: str) -> str:
    """Quote a contract-validated SQL identifier."""
    return f'"{identifier.replace("\"", "\"\"")}"'


class ReadOnlyDataService:
    """One read-only DuckDB connection per request; never exposes raw SQL."""

    def __init__(self, database_path: str) -> None:
        self._database_path = database_path

    def _connect(self):
        try:
            connection = duckdb.connect(self._database_path, read_only=True)
            connection.execute("SET enable_progress_bar = false")
            return connection
        except duckdb.Error as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Read-only database is unavailable.",
            ) from exc

    @staticmethod
    def _schema(table_name: str):
        if table_name not in REMOTE_TABLES:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table is not available through this temporary gateway.",
            )
        return TABLE_BY_NAME[table_name]

    def check_health(self) -> dict[str, Any]:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
                latest_market_date = connection.execute(
                    'SELECT MAX("trade_date") FROM "daily_market_snapshot"'
                ).fetchone()[0]
        except HTTPException:
            return {
                "status": "degraded",
                "database_connected": False,
                "read_query_available": False,
                "latest_market_date": None,
            }
        return {
            "status": "ok",
            "database_connected": True,
            "read_query_available": True,
            "latest_market_date": _json_value(latest_market_date),
        }

    def list_tables(self) -> list[dict[str, Any]]:
        result = []
        for table_name, description in REMOTE_TABLES.items():
            schema = TABLE_BY_NAME[table_name]
            result.append(
                {
                    "name": table_name,
                    "description": description,
                    "field_count": len(schema.columns),
                    "primary_key": list(schema.primary_key),
                }
            )
        return result

    def get_schema(self, table_name: str) -> dict[str, Any]:
        schema = self._schema(table_name)
        return {
            "table": table_name,
            "description": REMOTE_TABLES[table_name],
            "primary_key": list(schema.primary_key),
            "fields": [
                {
                    "name": column.name,
                    "type": column.dtype,
                    "nullable": column.nullable,
                    "primary_key": column.name in schema.primary_key,
                    "description": FIELD_DESCRIPTIONS.get(column.name, "QRP contracts 定义的研究数据字段。"),
                }
                for column in schema.columns
            ],
        }

    def overview(self, table_name: str) -> dict[str, Any]:
        schema = self._schema(table_name)
        columns = set(schema.column_names())
        select_parts = ["COUNT(*) AS total_rows"]
        if "trade_date" in columns:
            select_parts.extend(
                [
                    'MIN("trade_date") AS earliest_date',
                    'MAX("trade_date") AS latest_date',
                ]
            )
        if "created_at" in columns:
            select_parts.append('MAX("created_at") AS latest_update')
        elif "updated_at" in columns:
            select_parts.append('MAX("updated_at") AS latest_update')
        if "ticker" in columns:
            select_parts.append('COUNT(DISTINCT "ticker") AS asset_count')
        query = f"SELECT {', '.join(select_parts)} FROM {_quote_identifier(table_name)}"
        with self._connect() as connection:
            row = connection.execute(query).fetchone()
            names = [description[0] for description in connection.description]
        return {"table": table_name, **dict(zip(names, (_json_value(value) for value in row), strict=True))}

    def query(
        self,
        table_name: str,
        fields: list[str] | None,
        filters: Iterable[dict[str, Any]],
        date_from: dt.date | None,
        date_to: dt.date | None,
        order_by: str | None,
        order_direction: str,
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        schema = self._schema(table_name)
        allowed_fields = set(schema.column_names())
        selected_fields = fields or list(schema.column_names())
        self._validate_fields(selected_fields, allowed_fields, "fields")
        if order_by is not None and order_by not in allowed_fields:
            raise _bad_request("The requested sort field is not allowed for this table.")

        clauses: list[str] = []
        parameters: list[Any] = []
        for item in filters:
            field = item["field"]
            operator = item["operator"]
            value = item["value"]
            if field not in allowed_fields:
                raise _bad_request("The requested filter field is not allowed for this table.")
            if operator not in FILTER_OPERATORS:
                raise _bad_request("The requested filter operator is not allowed.")
            if operator == "in":
                if not isinstance(value, list) or not value or len(value) > 50:
                    raise _bad_request("An 'in' filter must contain 1 to 50 values.")
                clauses.append(f"{_quote_identifier(field)} IN ({', '.join('?' for _ in value)})")
                parameters.extend(value)
            else:
                if isinstance(value, (dict, list)):
                    raise _bad_request("Filter values must be scalar except for the 'in' operator.")
                clauses.append(f"{_quote_identifier(field)} {FILTER_OPERATORS[operator]} ?")
                parameters.append(value)

        if date_from is not None or date_to is not None:
            if "trade_date" not in allowed_fields:
                raise _bad_request("This table does not support trade-date range filtering.")
            if date_from is not None:
                clauses.append('"trade_date" >= ?')
                parameters.append(date_from)
            if date_to is not None:
                clauses.append('"trade_date" <= ?')
                parameters.append(date_to)

        projection = ", ".join(_quote_identifier(field) for field in selected_fields)
        query = f"SELECT {projection} FROM {_quote_identifier(table_name)}"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        if order_by is not None:
            direction = "DESC" if order_direction == "desc" else "ASC"
            query += f" ORDER BY {_quote_identifier(order_by)} {direction}"
        query += " LIMIT ? OFFSET ?"
        parameters.extend([limit, offset])

        with self._connect() as connection:
            cursor = connection.execute(query, parameters)
            names = [description[0] for description in cursor.description]
            records = [
                dict(zip(names, (_json_value(value) for value in row), strict=True))
                for row in cursor.fetchall()
            ]
        return {
            "table": table_name,
            "fields": selected_fields,
            "limit": limit,
            "offset": offset,
            "returned_rows": len(records),
            "data": records,
        }

    @staticmethod
    def _validate_fields(fields: list[str], allowed_fields: set[str], field_label: str) -> None:
        if not fields:
            raise _bad_request(f"{field_label} must include at least one allowed field.")
        if len(fields) > len(allowed_fields) or len(set(fields)) != len(fields):
            raise _bad_request(f"{field_label} contains duplicate or excessive fields.")
        unknown_fields = set(fields) - allowed_fields
        if unknown_fields:
            raise _bad_request(f"{field_label} contains fields not allowed for this table.")


def _json_value(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime, dt.time)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value
