import os

from dotenv import load_dotenv

load_dotenv()

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "")
