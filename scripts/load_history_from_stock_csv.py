import argparse
from pathlib import Path

import duckdb
import pandas as pd


from qrp_atlas.config import DATA_DIR, DB_PATH

HISTORY_DIR = DATA_DIR / "history"

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

SOURCE_REQUIRED_COLUMNS = [
    "trade_date",
    "ticker",
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

UPSERT_SQL = """
INSERT INTO daily_market_snapshot (
    trade_date, ticker, name, open, high, low, close, pre_close,
    pct_change, volume, amount, turnover, market_cap, float_cap,
    is_st, is_limit_up, is_limit_down
)
SELECT
    trade_date, ticker, name, open, high, low, close, pre_close,
    pct_change, volume, amount, turnover, market_cap, float_cap,
    is_st, is_limit_up, is_limit_down
FROM df
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


def normalize_ticker(ticker: str) -> str:
    ticker = str(ticker).strip()
    if ticker.endswith((".SH", ".SZ", ".BJ")):
        return ticker

    code = ticker.zfill(6)
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return f"{code}.SH"
    if code.startswith(("000", "001", "002", "003", "300", "301", "302")):
        return f"{code}.SZ"
    if code.startswith(("4", "8", "920")):
        return f"{code}.BJ"
    return code


def validate_not_empty(df: pd.DataFrame, file_path: Path):
    if df.empty:
        raise ValueError(f"{file_path} empty")


def validate_source_schema(df: pd.DataFrame, file_path: Path):
    missing = [c for c in SOURCE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{file_path} missing columns: {missing}")


def validate_target_schema(df: pd.DataFrame):
    if list(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"Schema mismatch\\nExpected: {EXPECTED_COLUMNS}\\nGot: {list(df.columns)}"
        )


def validate_pk(df: pd.DataFrame):
    dup = df.duplicated(subset=["trade_date", "ticker"])
    if dup.any():
        raise ValueError(f"Duplicate PK rows:\\n{df.loc[dup, ['trade_date', 'ticker']]}")


def validate_ticker(df: pd.DataFrame):
    invalid = df[
        ~df["ticker"].astype(str).str.strip().str.upper().str.fullmatch(r"\d{6}\.(SH|SZ|BJ)")
    ]
    if not invalid.empty:
        raise ValueError(f"Invalid ticker suffix:\\n{invalid[['ticker']].head(20)}")


def ensure_pre_close_column(con: duckdb.DuckDBPyConnection):
    cols = con.execute("PRAGMA table_info('daily_market_snapshot')").fetchall()
    names = {row[1] for row in cols}
    if "pre_close" not in names:
        con.execute("ALTER TABLE daily_market_snapshot ADD COLUMN pre_close DOUBLE")


def clean_file(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path, encoding="utf-8-sig", dtype={"ticker": "string"})

    validate_not_empty(df, file_path)
    validate_source_schema(df, file_path)

    if "name" not in df.columns:
        df["name"] = pd.NA

    df["ticker"] = df["ticker"].map(normalize_ticker)

    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="raise").dt.date

    if df["name"].isna().all():
        df["is_st"] = False
    else:
        df["name"] = df["name"].astype(str)
        df["is_st"] = df["name"].str.contains("ST", case=False, na=False)

    df["limit_pct"] = 10.0
    df.loc[df["ticker"].str.startswith(("300", "301", "302", "688", "689")), "limit_pct"] = 20.0
    df.loc[df["ticker"].str.startswith(("4", "8", "920")), "limit_pct"] = 30.0

    main_board_st = df["is_st"] & df["ticker"].str.startswith(
        ("600", "601", "603", "605", "000", "001", "002", "003")
    )
    df.loc[main_board_st, "limit_pct"] = 5.0

    df["limit_up_price"] = (df["pre_close"] * (1 + df["limit_pct"] / 100)).round(2)
    df["limit_down_price"] = (df["pre_close"] * (1 - df["limit_pct"] / 100)).round(2)

    df["is_limit_up"] = df["close"] >= (df["limit_up_price"] - 0.001)
    df["is_limit_down"] = df["close"] <= (df["limit_down_price"] + 0.001)

    df = df.drop(columns=["limit_pct", "limit_up_price", "limit_down_price"])

    df = df.sort_values(["trade_date", "ticker"])
    df = df[EXPECTED_COLUMNS]

    validate_target_schema(df)
    validate_pk(df)
    validate_ticker(df)

    return df


def iter_history_files(single_file: str | None) -> list[Path]:
    if single_file:
        path = Path(single_file)
        if not path.exists():
            raise FileNotFoundError(path)
        return [path]

    if not HISTORY_DIR.exists():
        raise FileNotFoundError(HISTORY_DIR)

    files = sorted(HISTORY_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(HISTORY_DIR)

    return files


def load_files(file_paths: list[Path], limit: int | None = None) -> int:
    total = 0
    loaded_files = 0

    with duckdb.connect(str(DB_PATH)) as con:
        ensure_pre_close_column(con)

        con.execute("BEGIN TRANSACTION")
        try:
            for path in file_paths:
                if limit is not None and loaded_files >= limit:
                    break

                df = clean_file(path)
                con.register("df", df)
                con.execute(UPSERT_SQL)
                con.unregister("df")

                loaded_files += 1
                total += len(df)
                print(f"Loaded {len(df)} rows from {path.name}")

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    print(f"Loaded files: {loaded_files}")
    return total


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Load a single stock csv file")
    parser.add_argument("--limit", type=int, help="Only load first N files after sorting")
    return parser.parse_args()


def main():
    args = parse_args()
    files = iter_history_files(args.file)
    total = load_files(files, args.limit)
    print(f"Total rows loaded: {total}")


if __name__ == "__main__":
    main()
