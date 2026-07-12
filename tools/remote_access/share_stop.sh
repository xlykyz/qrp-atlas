#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime/remote_access"
SESSION_FILE="$RUNTIME_DIR/capability_session.json"

if [[ ! -f "$SESSION_FILE" ]]; then
    echo "Capability Session: 没有活跃的会话。"
    exit 0
fi

# Mark as revoked
"$ROOT_DIR/.venv/bin/python" - "$SESSION_FILE" <<'PYEOF'
import json, sys

session_file = sys.argv[1]
with open(session_file) as f:
    session = json.load(f)

session["revoked"] = True
with open(session_file, "w") as f:
    json.dump(session, f, indent=2, ensure_ascii=False)

sid = session.get("session_id", "?")
print(f"Capability Session 已撤销。")
print(f"  会话 ID: {sid[:16]}...（截断）")
print(f"  该会话现已失效，新的请求将返回 410 Gone。")
print(f"  用 share_start.sh 重新生成新会话。")
PYEOF