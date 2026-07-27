from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from qrp_atlas.config import get_settings

from .service import SystemBEpisodeProductionError, rebuild_episodes, write_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Build System B market episodes")
    parser.add_argument("--state-input-database", type=Path)
    parser.add_argument("--output-database", type=Path, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    state_input = args.state_input_database or get_settings().paths.duckdb_path
    try:
        result = rebuild_episodes(state_input, args.output_database, end_date=args.end_date)
    except SystemBEpisodeProductionError as exc:
        print(json.dumps({"status": "FAILED", "error_code": exc.code, "detail": exc.detail}, ensure_ascii=False))
        return 1
    if args.report:
        write_report(result, args.report)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0
