from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CANONICAL_DIR = DATA_DIR / "canonical"
DB_DIR = DATA_DIR / "db"

DB_PATH = DB_DIR / "quant.db"

DAILY_SNAPSHOT_RAW_DIR = RAW_DIR / "daily_snapshot"
DAILY_MARKET_SNAPSHOT_CANONICAL_DIR = CANONICAL_DIR / "daily_market_snapshot"

WEB_DIR = PROJECT_ROOT / "web"


def ensure_dirs() -> None:
    for d in (DATA_DIR, RAW_DIR, CANONICAL_DIR, DB_DIR):
        d.mkdir(parents=True, exist_ok=True)
