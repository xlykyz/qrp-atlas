import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")
