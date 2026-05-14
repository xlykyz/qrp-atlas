"""Check how tushare client communicates with the custom gateway"""
import os, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Let's see what URL the tushare client actually uses
import tushare as ts
pro = ts.pro_api("dummy")
pro._DataApi__http_url = "http://124.220.22.110:8020/"

# Inspect how tushare makes API calls
# Look at the __http_url and how requests are sent
print(f"API URL: {pro._DataApi__http_url}")
print(f"Token: {pro._DataApi__token[:10]}...{pro._DataApi__token[-5:]}")

# Let's try sending a raw request to the gateway
import requests
import json

# Try the tushare API format the client uses
payload = {
    "api_name": "index_basic",
    "token": "dummy",
    "params": {"limit": 3},
    "fields": ""
}
headers = {"Content-Type": "application/json"}
r = requests.post("http://124.220.22.110:8020/", json=payload, timeout=10)
print(f"\nRaw POST to gateway:")
print(f"Status: {r.status_code}")
print(f"Response: {r.text[:500]}")
