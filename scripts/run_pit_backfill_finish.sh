#!/usr/bin/env bash
# Wait for fetch service to finish, then offline clean/load + audit once.
set -euo pipefail
ROOT="/home/claire/projects/qrp-atlas"
TAG="${1:-20260714}"
PY="$ROOT/.venv/bin/python"
LOG="$ROOT/data/logs/pit_backfill_${TAG}.finish.log"
FETCH_UNIT="qrp-pit-backfill-${TAG}-fetch.service"
cd "$ROOT"
set -a
[ -f "$ROOT/.env" ] && source "$ROOT/.env"
set +a
mkdir -p "$ROOT/data/logs"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }

log "watcher start; waiting for $FETCH_UNIT to leave activating/active"
while true; do
  st=$(systemctl --user is-active "$FETCH_UNIT" 2>/dev/null || true)
  # also check process
  if pgrep -f "python -m qrp_atlas.pipeline.pit_backfill --mode full --resume --stages fetch --run-tag ${TAG}" >/dev/null; then
    st="activating"
  fi
  pending=$("$PY" - <<PY
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore
s=ManifestStore("data/state/pit_backfill_${TAG}/manifest.jsonl")
print(sum(1 for r in s.iter_records() if r.fetch_status not in ("success","empty","failed")))
PY
)
  log "fetch_unit=$st fetch_not_terminal=$pending"
  if [[ "$st" != "activating" && "$st" != "active" && "$pending" == "0" ]]; then
    break
  fi
  sleep 60
done

log "fetch complete; starting offline clean,load"
set +e
"$PY" -m qrp_atlas.pipeline.pit_backfill \
  --mode full --resume --stages clean,load --offline-only \
  --run-tag "$TAG" --no-backup --audit \
  --log-path "$ROOT/data/logs/pit_backfill_${TAG}.cleanload.log" \
  >>"$LOG" 2>&1
rc=$?
set -e
log "clean/load finished rc=$rc"

# summary
"$PY" - <<PY | tee -a "$LOG"
from collections import Counter
from pathlib import Path
from qrp_atlas.pipeline.pit_backfill.manifest import ManifestStore
from qrp_atlas.config import DB_PATH
import duckdb
s=ManifestStore("data/state/pit_backfill_${TAG}/manifest.jsonl")
print("agg", dict(Counter(r.status for r in s.iter_records())))
print("fetch", dict(Counter(r.fetch_status for r in s.iter_records())))
print("clean", dict(Counter(r.clean_status for r in s.iter_records())))
print("load", dict(Counter(r.load_status for r in s.iter_records())))
con=duckdb.connect(str(DB_PATH), read_only=True)
for t in ["income_statement","balance_sheet","cashflow_statement","financial_indicator","industry_membership_history","index_component_history"]:
    try:
        print(t, con.execute(f"select count(*) from {t}").fetchone()[0])
    except Exception as e:
        print(t, e)
con.close()
print("db_size_gb", round(Path(DB_PATH).stat().st_size/1024**3, 2))
PY
log "all done"
exit $rc
