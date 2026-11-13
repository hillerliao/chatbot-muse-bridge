#!/usr/bin/env bash
# 微信 iLink hook：每 5 秒跑一次。
# 1) 调 keepalive.sh，保证 daemon 活着（心跳停 90 秒就重拉）；
# 2) inbox 里有没见过的新消息 -> wake，让 worker 回复；
# 3) 其余情况 silent。
# 只传消息 ID，不传正文、不执行正文；回复由被唤醒的 worker 写 outbox，daemon 发回。
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

ROOT="${CHATBOT_BRIDGE_WECHAT:-$HOME/workspace/chatbot-muse-bridge/wechat}"
SEEN_FILE="$HOME/hooks/state/wechat-ilink-seen.txt"
mkdir -p "$(dirname "$SEEN_FILE")"
touch "$SEEN_FILE"

# 1) 保活（脚本内部有 30 秒防重复拉闸）
bash "$ROOT/keepalive.sh" >> "$ROOT/logs/keepalive.log" 2>&1 || log "keepalive failed" '{}'

# 2) daemon 是不是还活着：心跳超过 120 秒没更新 -> 每小时最多报一次
HB="$ROOT/state/heartbeat.json"
if [ -f "$HB" ]; then
  HB_AGE=$(python3 -c "import time,os; print(int(time.time()-os.path.getmtime('$HB')))" 2>/dev/null || echo 0)
  if [ "$HB_AGE" -gt 120 ]; then
    LAST_REPORT="$HOME/hooks/state/wechat-ilink-dead-reported.txt"
    NOW=$(date +%s)
    LAST=$(cat "$LAST_REPORT" 2>/dev/null || echo 0)
    if [ $((NOW - LAST)) -gt 3600 ]; then
      echo "$NOW" > "$LAST_REPORT"
      log "wechat-ilink daemon heartbeat stale" "{\"age_secs\": $HB_AGE}"
      wake "wechat-ilink daemon heartbeat stale" "{\"age_secs\": $HB_AGE}"
    else
      silent "wechat-ilink daemon heartbeat stale, already reported" '{}'
    fi
    exit 0
  fi
fi

# 3) 新消息？
NEW_IDS=""
for f in "$ROOT"/inbox/*.json; do
  [ -e "$f" ] || continue
  id=$(basename "$f" .json)
  if ! grep -qx "$id" "$SEEN_FILE" 2>/dev/null; then
    NEW_IDS="${NEW_IDS}${id} "
    echo "$id" >> "$SEEN_FILE"
  fi
done

if [ -z "$NEW_IDS" ]; then
  silent "no new wechat messages" '{}'
  exit 0
fi

IDS_JSON=$(printf '%s' "$NEW_IDS" | tr ' ' '\n' | grep -v '^$' | jq -R . | jq -s .)
log "new wechat message ids" "{\"ids\": $IDS_JSON}"
wake "new wechat messages" "{\"ids\": $IDS_JSON}"
