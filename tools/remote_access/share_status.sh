#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime/remote_access"
SESSION_FILE="$RUNTIME_DIR/capability_session.json"
PORT="${QRP_REMOTE_ACCESS_PORT:-8765}"

if [[ ! -f "$SESSION_FILE" ]]; then
    echo "Capability Session: 未激活（没有会话文件）"
    echo "用 share_start.sh 创建新会话。"
    exit 0
fi

# Parse session info with Python
"$ROOT_DIR/.venv/bin/python" - "$SESSION_FILE" "$RUNTIME_DIR" <<'PYEOF'
import json, sys
from datetime import datetime, timezone

session_file = sys.argv[1]
runtime_dir = sys.argv[2]

try:
    with open(session_file) as f:
        session = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    print("Capability Session: 会话文件损坏或不可读。")
    sys.exit(0)

session_id = session.get("session_id", "?")[:16] + "..."
created = session.get("created_at", "?")
expires = session.get("expires_at", "?")
revoked = session.get("revoked", False)
duration = session.get("duration_minutes", 30)
max_rows = session.get("max_rows", 50)

# Check expiry
now = datetime.now(timezone.utc)
try:
    expires_dt = datetime.fromisoformat(expires)
    if expires_dt.tzinfo is None:
        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    expired = now >= expires_dt
except (ValueError, TypeError):
    expired = False

print(f"Capability Session: {'已激活' if not revoked and not expired else '已失效'}")
print(f"  会话 ID:     {session_id}")
print(f"  创建时间:    {created}")
print(f"  到期时间:    {expires}")
print(f"  持续时间:    {duration} 分钟")
print(f"  单次最大行:  {max_rows}")
print(f"  已撤销:      {'是' if revoked else '否'}")
print(f"  已过期:      {'是' if expired else '否'}")

# Check public URL
public_url_file = f"{runtime_dir}/public_url"
try:
    with open(public_url_file) as f:
        url = f.read().strip()
    if url:
        print(f"  公网入口:    {url}/share/{session.get('session_id', '')}")
except FileNotFoundError:
    pass

print()
if not revoked and not expired:
    print("状态正常，可供 ChatGPT 等工具使用。")
elif revoked:
    print("已撤销。用 share_start.sh 重新生成。")
elif expired:
    print("已过期。用 share_start.sh 重新生成。")
PYEOF