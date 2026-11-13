#!/usr/bin/env bash
# chatbot-muse-bridge/dingtalk keepalive：daemon 死了就拉起来，活着就不动。
# 由 hook 每 5 秒调一次，也可手动跑。只管本桥。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE="$ROOT/state"
LOCK="$STATE/keepalive.lock"
mkdir -p "$STATE" "$ROOT/logs"

exec 9>"$LOCK"
if ! flock -n 9; then
  exit 0
fi

# 30 秒内刚拉过一次就别重复拉（防 hook 高频调用时连环启动）
LAST_START="$STATE/last-start.txt"
if [ -f "$LAST_START" ]; then
  NOW=$(date +%s)
  LAST=$(cat "$LAST_START" 2>/dev/null || echo 0)
  if [ $((NOW - LAST)) -lt 30 ]; then
    exit 0
  fi
fi

# 心跳新鲜（90 秒内）且 pid 还活着 -> 不用管
HB="$STATE/heartbeat.json"
if [ -f "$HB" ]; then
  HB_AGE=$(python3 -c "import time,os; print(int(time.time()-os.path.getmtime('$HB')))" 2>/dev/null || echo 9999)
  PID=$(python3 -c "import json; print(json.load(open('$HB')).get('pid',''))" 2>/dev/null || echo "")
  if [ "$HB_AGE" -lt 90 ] && [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    exit 0
  fi
fi

# 拉起来（用自带 venv，里面装了 dingtalk-stream）
date +%s > "$LAST_START"
cd "$ROOT"
nohup "$ROOT/.venv/bin/python" daemon.py >> "$ROOT/logs/daemon.log" 2>&1 &
disown || true
echo "dingtalk-muse daemon (re)started pid $!"
