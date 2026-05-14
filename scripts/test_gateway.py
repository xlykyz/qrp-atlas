"""Test custom API gateway without local token"""
import os, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dotenv import load_dotenv
load_dotenv()

import tushare as ts

# Try with minimal placeholder token via custom gateway
CUSTOM_API = "http://124.220.22.110:8020/"
pro = ts.pro_api("test_placeholder")
pro._DataApi__http_url = CUSTOM_API

try:
    df = pro.index_basic(limit=3)
    print(f"SUCCESS: {len(df)} rows")
    print(df.to_string())

    # Try a daily query
    df2 = pro.daily(trade_date="20100104")
    print(f"\nDaily 20100104: {len(df2)} rows")
    if len(df2) > 0:
        print(df2.head(3))
except Exception as e:
    print(f"FAILED: {e}")
