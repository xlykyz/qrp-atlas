from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from .service import SystemBPoolProductionError, build_stock_pools


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build System B stock pools")
    parser.add_argument("--input-database", type=Path, required=True)
    parser.add_argument("--episode-database", type=Path)
    parser.add_argument("--output-database", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    try:
        result = build_stock_pools(
            args.input_database,
            args.output_database,
            start_date=args.start_date,
            end_date=args.end_date,
            episode_database=args.episode_database,
        )
    except SystemBPoolProductionError as exc:
        print(json.dumps({"status": "FAILED", "error_code": exc.code, "detail": exc.detail}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0
