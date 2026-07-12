"""Configuration and explicit data allowlist for the temporary gateway."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from qrp_atlas.config import DB_PATH
from qrp_atlas.contracts import TABLE_BY_NAME


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNTIME_DIR = REPOSITORY_ROOT / ".runtime" / "remote_access"

MAX_ROWS = 200
DEFAULT_ROWS = 50
MAX_OFFSET = 100_000
REQUEST_TIMEOUT_SECONDS = 15


# This is deliberately narrower than contracts.ALL_TABLES. In particular, it
# excludes local execution history and document-like research/source tables.
REMOTE_TABLES: dict[str, str] = {
    "stock_info": "股票基础信息。",
    "trading_calendar": "交易日历。",
    "daily_market_snapshot": "股票日线市场快照。",
    "index_daily": "指数日线行情。",
    "adj_factor_changes": "股票复权因子。",
    "daily_basic": "日频基础估值与换手指标。",
    "zt_pool": "涨停股池市场数据。",
    "dt_pool": "跌停股池市场数据。",
    "suspend_d": "股票停复牌日数据。",
    "market_phase": "市场阶段指标结果。",
}

FIELD_DESCRIPTIONS: dict[str, str] = {
    "trade_date": "交易日期。",
    "ticker": "标准证券代码。",
    "name": "证券名称。",
    "index_code": "指数代码。",
    "index_name": "指数名称。",
    "created_at": "记录写入时间。",
    "updated_at": "记录更新时间。",
    "close": "收盘价。",
    "pct_change": "涨跌幅。",
    "volume": "成交量。",
    "amount": "成交额。",
}


def validate_allowlist() -> None:
    missing = set(REMOTE_TABLES) - set(TABLE_BY_NAME)
    if missing:
        raise RuntimeError(f"Remote table allowlist contains unknown contracts: {sorted(missing)}")


@dataclass(frozen=True)
class GatewaySettings:
    """Runtime-only gateway settings; database paths are never returned by APIs."""

    database_path: Path
    token: str
    request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS


def load_settings() -> GatewaySettings:
    """Load the generated token from a protected runtime file."""
    validate_allowlist()
    token_file = Path(
        os.environ.get("QRP_REMOTE_ACCESS_TOKEN_FILE", DEFAULT_RUNTIME_DIR / "token")
    )
    try:
        token = token_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError("Remote access token file is unavailable; use start.sh.") from exc
    if len(token) < 32:
        raise RuntimeError("Remote access token is invalid; restart the gateway.")

    configured_db_path = os.environ.get("QRP_REMOTE_ACCESS_DB_PATH")
    return GatewaySettings(
        database_path=Path(configured_db_path) if configured_db_path else DB_PATH,
        token=token,
    )
