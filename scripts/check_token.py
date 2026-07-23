"""Verify the configured Tushare credential without exposing it."""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from qrp_atlas.config.settings import AppSettings
from qrp_atlas.config.tushare_client import get_tushare_pro

settings = AppSettings.load()
print(
    "TUSHARE_TOKEN: configured"
    if settings.external_services.tushare_token
    else "TUSHARE_TOKEN: not configured"
)
try:
    pro = get_tushare_pro(settings=settings)
    frame = pro.index_basic(limit=3)
except Exception as exc:
    print(f"API test: FAILED ({type(exc).__name__})")
    raise SystemExit(1) from exc
else:
    print(f"API test: SUCCESS, got {len(frame)} rows")
