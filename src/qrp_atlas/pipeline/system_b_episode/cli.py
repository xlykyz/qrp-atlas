from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path

from qrp_atlas.config import get_settings, reset_settings_cache

from .service import SystemBEpisodeProductionError, rebuild_episodes, write_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Build System B market episodes")
    parser.add_argument("--env-file")
    parser.add_argument("--state-input-database", type=Path)
    parser.add_argument("--output-database", type=Path)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.env_file:
        os.environ["QRP_ENV_FILE"] = args.env_file
        reset_settings_cache()
    settings = get_settings()
    state_input = args.state_input_database or settings.paths.duckdb_path
    output = args.output_database or settings.paths.episode_db_path
    if output is None:
        raise SystemBEpisodeProductionError(
            "EPISODE_OUTPUT_DATABASE_NOT_CONFIGURED",
            "set QRP_EPISODE_DB_PATH or pass --output-database",
        )
    try:
        result = rebuild_episodes(state_input, output, end_date=args.end_date)
    except SystemBEpisodeProductionError as exc:
        print(json.dumps({"status": "FAILED", "error_code": exc.code, "detail": exc.detail}, ensure_ascii=False))
        return 1
    if args.report:
        write_report(result, args.report)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0
