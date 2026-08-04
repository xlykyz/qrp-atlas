from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path

from qrp_atlas.config import get_settings, reset_settings_cache
from qrp_atlas.indicators.system_b.pools import CAPACITY, HEIGHT, RECOGNITION

from .service import SystemBPoolProductionError, build_stock_pool


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build System B stock pools")
    parser.add_argument("--env-file")
    parser.add_argument("--input-database", type=Path)
    parser.add_argument("--episode-database", type=Path)
    parser.add_argument("--output-database", type=Path)
    parser.add_argument("--pool-type", choices=(HEIGHT, CAPACITY, RECOGNITION), required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    if args.env_file:
        os.environ["QRP_ENV_FILE"] = args.env_file
        reset_settings_cache()
    settings = get_settings()
    input_database = args.input_database or settings.paths.duckdb_path
    episode_database = args.episode_database or settings.paths.episode_db_path
    output_database = args.output_database or settings.paths.pool_db_path
    if output_database is None:
        raise SystemBPoolProductionError(
            "POOL_OUTPUT_DATABASE_NOT_CONFIGURED",
            "set QRP_POOL_DB_PATH or pass --output-database",
        )
    try:
        result = build_stock_pool(
            input_database,
            output_database,
            pool_type=args.pool_type,
            start_date=args.start_date,
            end_date=args.end_date,
            episode_database=episode_database,
        )
    except SystemBPoolProductionError as exc:
        print(json.dumps({"status": "FAILED", "error_code": exc.code, "detail": exc.detail}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0
