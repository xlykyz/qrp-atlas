import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CANONICAL_DIR = DATA_DIR / "canonical"
DB_DIR = DATA_DIR / "db"

DB_PATH = DB_DIR / "quant.db"

DAILY_SNAPSHOT_RAW_DIR = RAW_DIR / "daily_snapshot"
DAILY_MARKET_SNAPSHOT_CANONICAL_DIR = CANONICAL_DIR / "daily_market_snapshot"

RESEARCH_PDFS_DIR = RAW_DIR / "research_pdfs"

WEB_DIR = PROJECT_ROOT / "web"

# Product and generic result API SSOT. Fixture dir is no longer the default.
BACKTEST_RUNS_DIR = Path(
    os.getenv("QRP_ATLAS_BACKTEST_RUNS_DIR")
    or (PROJECT_ROOT / "data" / "backtest_runs")
)

# Explicit fixture root for mock demos / unit tests only.
BACKTEST_FIXTURE_RUNS_DIR = PROJECT_ROOT / "tests" / "fixtures" / "backtest_runs"


def ensure_dirs() -> None:
    for d in (DATA_DIR, RAW_DIR, CANONICAL_DIR, DB_DIR, BACKTEST_RUNS_DIR):
        d.mkdir(parents=True, exist_ok=True)
