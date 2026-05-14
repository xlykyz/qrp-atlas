"""Quick check token status"""
import os, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Load .env from multiple locations
from dotenv import load_dotenv
for p in [PROJECT_ROOT / ".env", Path.home() / ".hermes" / ".env", Path.home() / ".env"]:
    if p.exists():
        load_dotenv(p)
        print(f"Loaded .env: {p}")

token = os.getenv("TUSHARE_TOKEN")
if token:
    print(f"TUSHARE_TOKEN: found, length={len(token)}, first 8={token[:8]}...")
else:
    print("TUSHARE_TOKEN: NOT FOUND")

# Try connecting
import tushare as ts
CUSTOM_API_URL = "http://124.220.22.110:8020/"
pro = ts.pro_api(token)
pro._DataApi__http_url = CUSTOM_API_URL

try:
    df = pro.index_basic(limit=3)
    print(f"API test: SUCCESS, got {len(df)} rows")
    # Try a daily query for early data
    df2 = pro.daily(ts_code="000001.SZ", start_date="19901219", end_date="19901231")
    print(f"Historical daily test: {len(df2)} rows")
    if len(df2) > 0:
        print(df2.head())
except Exception as e:
    print(f"API test FAILED: {e}")
