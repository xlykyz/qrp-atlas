import argparse
from pathlib import Path
import re

import duckdb
import pandas as pd


# =====================
# 路径配置
# =====================

from qrp_atlas.config import DATA_DIR as QRP_DATA_DIR, DB_PATH

DATA_DIR = QRP_DATA_DIR / "daily"

DB_PATH.parent.mkdir(parents=True, exist_ok=True)


# =====================
# 字段映射
# =====================

COLUMN_MAP = {
    "trade_date": "trade_date",
    "代码": "ticker",
    "名称": "name",
    "今开": "open",
    "最高": "high",
    "最低": "low",
    "最新价": "close",
    "涨跌幅": "pct_change",
    "成交量": "volume",
    "成交额": "amount",
    "换手率": "turnover",
    "总市值": "market_cap",
    "流通市值": "float_cap",
    "昨收": "pre_close",
}

EXPECTED_COLUMNS = [
    "trade_date",
    "ticker",
    "name",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "pct_change",
    "volume",
    "amount",
    "turnover",
    "market_cap",
    "float_cap",
    "is_st",
    "is_limit_up",
    "is_limit_down",
]

NUMERIC_COLS = [
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "pct_change",
    "volume",
    "amount",
    "turnover",
    "market_cap",
    "float_cap",
]


# =====================
# SQL
# =====================

UPSERT_SQL = """
INSERT INTO daily_market_snapshot (trade_date, ticker, name, open, high, low, close, pre_close, pct_change, volume, amount, turnover, market_cap, float_cap, is_st, is_limit_up, is_limit_down)
SELECT trade_date, ticker, name, open, high, low, close, pre_close, pct_change, volume, amount, turnover, market_cap, float_cap, is_st, is_limit_up, is_limit_down FROM df
ON CONFLICT (trade_date, ticker) DO UPDATE SET
    name = excluded.name,
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    pre_close = excluded.pre_close,
    pct_change = excluded.pct_change,
    volume = excluded.volume,
    amount = excluded.amount,
    turnover = excluded.turnover,
    market_cap = excluded.market_cap,
    float_cap = excluded.float_cap,
    is_st = excluded.is_st,
    is_limit_up = excluded.is_limit_up,
    is_limit_down = excluded.is_limit_down
"""


# =====================
# 验证函数
# =====================

def validate_schema(df: pd.DataFrame):

    if list(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"Schema mismatch\nExpected: {EXPECTED_COLUMNS}\nGot: {list(df.columns)}"
        )


def validate_pk(df: pd.DataFrame):

    dup = df.duplicated(subset=["trade_date", "ticker"])

    if dup.any():
        raise ValueError(
            f"Duplicate PK rows:\n{df.loc[dup]}"
        )


def validate_trade_date(df: pd.DataFrame, file_path: Path):

    file_date = parse_trade_date_from_name(file_path)

    if not file_date:
        return

    unique = df["trade_date"].astype(str).unique()

    if len(unique) != 1:
        raise ValueError("Multiple trade_date values in file")

    if unique[0] != file_date:
        raise ValueError(
            f"trade_date mismatch file={file_date} content={unique[0]}"
        )


def validate_ticker(df: pd.DataFrame):

    invalid = df[~df["ticker"].astype(str).str.strip().str.upper().str.fullmatch(r"\d{6}\.(SH|SZ|BJ)")]

    if not invalid.empty:
        raise ValueError(
            f"Invalid ticker suffix:\n{invalid[['ticker']]}"
        )


def validate_not_empty(df: pd.DataFrame, file_path: Path):

    if df.empty:
        raise ValueError(f"{file_path} empty")


# =====================
# 工具函数
# =====================

def normalize_ticker(ticker: str) -> str:

    ticker = str(ticker).zfill(6)

    if ticker.startswith(("600", "601", "603", "605", "688", "689")):
        return ticker + ".SH"

    if ticker.startswith(("000", "001", "002", "003", "300", "301", "302")):
        return ticker + ".SZ"

    if ticker.startswith(("4", "8", "920")):
        return ticker + ".BJ"

    return ticker


def parse_trade_date_from_name(path: Path) -> str | None:

    match = re.search(r"(\d{4}-\d{2}-\d{2})_Astock\.csv$", path.name)

    return match.group(1) if match else None


def build_file_path(trade_date: str) -> Path:

    year = trade_date.split("-")[0]

    return DATA_DIR / year / f"{trade_date}_Astock.csv"


def find_latest_file() -> Path | None:

    if not DATA_DIR.exists():
        return None

    files = sorted(DATA_DIR.glob("**/*_Astock.csv"))

    return files[-1] if files else None


# =====================
# 核心清洗函数
# =====================

def clean_file(file_path: Path) -> pd.DataFrame:

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path, encoding="utf-8-sig")

    validate_not_empty(df, file_path)

    # 补 trade_date
    if "trade_date" not in df.columns:

        trade_date = parse_trade_date_from_name(file_path)

        if trade_date:
            df.insert(0, "trade_date", trade_date)

    # 检查字段存在
    missing = [k for k in COLUMN_MAP if k not in df.columns]

    if missing:
        raise ValueError(
            f"{file_path} missing columns: {missing}"
        )

    # rename
    df = df.rename(columns=COLUMN_MAP)

    # ticker normalize
    df["ticker"] = df["ticker"].map(normalize_ticker)

    df["name"] = df["name"].astype(str)

    # numeric
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # date
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # ST
    df["is_st"] = df["name"].str.contains("ST", case=False, na=False)

    # limit calc (based on pre_close, following exchange rules)
    df["limit_pct"] = 10.0

    df.loc[df["ticker"].str.startswith(("300", "301", "302", "688", "689")), "limit_pct"] = 20.0

    df.loc[df["ticker"].str.startswith(("4", "8", "920")), "limit_pct"] = 30.0

    main_board_st = df["is_st"] & df["ticker"].str.startswith(("600", "601", "603", "605", "000", "001", "002", "003"))

    df.loc[main_board_st, "limit_pct"] = 5.0

    df["limit_up_price"] = (df["pre_close"] * (1 + df["limit_pct"] / 100)).round(2)

    df["limit_down_price"] = (df["pre_close"] * (1 - df["limit_pct"] / 100)).round(2)

    df["is_limit_up"] = df["close"] >= (df["limit_up_price"] - 0.001)

    df["is_limit_down"] = df["close"] <= (df["limit_down_price"] + 0.001)

    df = df.drop(columns=["limit_pct", "limit_up_price", "limit_down_price"])

    # sort
    df = df.sort_values(["trade_date", "ticker"])

    # reorder
    df = df[EXPECTED_COLUMNS]

    # validations
    validate_schema(df)

    validate_pk(df)

    validate_ticker(df)

    validate_trade_date(df, file_path)

    return df


# =====================
# 入库函数
# =====================

def load_files(file_paths: list[Path]) -> int:

    total = 0

    with duckdb.connect(str(DB_PATH)) as con:

        con.execute("BEGIN TRANSACTION")

        try:

            for path in file_paths:

                df = clean_file(path)

                con.register("df", df)

                con.execute(UPSERT_SQL)

                con.unregister("df")

                print(f"Loaded {len(df)} rows from {path}")

                total += len(df)

            con.execute("COMMIT")

        except Exception:

            con.execute("ROLLBACK")

            raise

    return total


# =====================
# CLI
# =====================

def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--file")

    parser.add_argument("--date")

    parser.add_argument("--year")

    return parser.parse_args()


def main():

    args = parse_args()

    if args.file:

        files = [Path(args.file)]

    elif args.date:

        files = [build_file_path(args.date)]

    elif args.year:

        files = sorted((DATA_DIR / args.year).glob("*_Astock.csv"))

        if not files:
            raise FileNotFoundError(args.year)

    else:

        latest = find_latest_file()

        if not latest:
            raise FileNotFoundError(DATA_DIR)

        files = [latest]

    total = load_files(files)

    print(f"Total rows loaded: {total}")


if __name__ == "__main__":

    main()