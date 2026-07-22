"""Public helpers for creating and validating the v1.0 DuckDB schema."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import duckdb


BASE_TABLES = (
    "daily_market_snapshot",
    "market_phase",
    "trade_execution",
)

_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE daily_market_snapshot (
      trade_date DATE,
      ticker VARCHAR,
      name VARCHAR,
      open DOUBLE,
      high DOUBLE,
      low DOUBLE,
      close DOUBLE,
      pct_change DOUBLE,
      volume BIGINT,
      amount DOUBLE,
      turnover DOUBLE,
      market_cap DOUBLE,
      float_cap DOUBLE,
      is_st BOOLEAN,
      is_limit_up BOOLEAN,
      is_limit_down BOOLEAN,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (trade_date, ticker)
    )
    """,
    """
    CREATE TABLE market_phase (
      trade_date DATE PRIMARY KEY,
      phase VARCHAR,
      M1_core BOOLEAN,
      M2_front BOOLEAN,
      M3_identifiable BOOLEAN,
      V_triggered BOOLEAN,
      notes VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE trade_execution (
      trade_id VARCHAR PRIMARY KEY,
      ticker VARCHAR,
      entry_date DATE,
      entry_price DOUBLE,
      path_type VARCHAR,
      half_sell_trigger DOUBLE,
      half_sell_date DATE,
      half_sell_price DOUBLE,
      exit_date DATE,
      exit_price DOUBLE,
      position_pct DOUBLE,
      notes VARCHAR
    )
    """,
)


def create_empty_database(path: str | Path) -> tuple[str, ...]:
    """Create the empty v1.0 database without replacing an existing path."""

    database = Path(path)
    if database.exists():
        raise FileExistsError(f"database path already exists: {database}")
    if not database.parent.is_dir():
        raise FileNotFoundError(f"database parent directory does not exist: {database.parent}")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{database.name}.",
        suffix=".tmp",
        dir=database.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink()
    try:
        connection = duckdb.connect(str(temporary))
        try:
            connection.execute("BEGIN TRANSACTION")
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        temporary.replace(database)
    finally:
        temporary.unlink(missing_ok=True)
    return validate_existing_database(database)


def validate_existing_database(path: str | Path) -> tuple[str, ...]:
    """Open a DuckDB file read-only and return its table names."""

    database = Path(path)
    if not database.exists():
        raise FileNotFoundError(f"database file does not exist: {database}")
    if not database.is_file():
        raise ValueError(f"database path is not a regular file: {database}")

    connection = duckdb.connect(str(database), read_only=True)
    try:
        rows = connection.execute("SHOW TABLES").fetchall()
    finally:
        connection.close()
    return tuple(row[0] for row in rows)
