#!/usr/bin/env bash
# 飞书接 Muse hook：每 5 秒跑一次。
# 1) 调 keepalive.sh，保证 daemon 活着（无凭证时 keepalive 直接退出）；
# 2) inbox 里有没见过的新消息 -> wake，让 worker 回复；
# 3) 其余情况 silent。
# 只传消息 ID，不传正文、不执行正文；回复由被唤醒的 worker 写 outbox，daemon 发回。
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

ROOT="${CHATBOT_BRIDGE_FEISHU:-$HOME/workspace/chatbot-muse-bridge/feishu}"
SEEN_FILE="$HOME/hooks/state/feishu-muse-seen.txt"
mkdir -p "$(dirname "$SEEN_FILE")"
touch "$SEEN_FILE"

# 没凭证：桥还没接通，静默（不保活、不报心跳）
if [ ! -f "$ROOT/state/credentials.json" ]; then
  silent "feishu bridge has no credentials yet" '{}'
  exit 0
fi

# 1) 保活（脚本内部有 30 秒防重复拉闸）
bash "$ROOT/keepalive.sh" >> "$ROOT/logs/keepalive.log" 2>&1 || log "keepalive failed" '{}'

# 2) daemon 是不是还活着：心跳超过 120 秒没更新 -> 每个停滞期只报一次
#    报告标记绑定心跳文件修改时间，心跳恢复后自然失效，不会永久静默。
HB="$ROOT/state/heartbeat.json"
if [ -f "$HB" ]; then
  HB_AGE=$(python3 -c "import time,os; print(int(time.time()-os.path.getmtime('$HB')))" 2>/dev/null || echo 0)
  if [ "$HB_AGE" -gt 120 ]; then
    LAST_REPORT="$HOME/hooks/state/feishu-muse-dead-reported.txt"
    HB_MTIME=$(python3 -c "import os; print(int(os.path.getmtime('$HB')))" 2>/dev/null || echo 0)
    LAST=$(cat "$LAST_REPORT" 2>/dev/null || echo "")
    if [ "$LAST" != "$HB_MTIME" ]; then
      echo "$HB_MTIME" > "$LAST_REPORT"
      log "feishu-muse daemon heartbeat stale" "{\"age_secs\": $HB_AGE}"
      wake "feishu-muse daemon heartbeat stale" "{\"age_secs\": $HB_AGE}"
    else
      silent "feishu-muse daemon heartbeat stale, already reported" '{}'
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
  silent "no new feishu messages" '{}'
  exit 0
fi

IDS_JSON=$(printf '%s' "$NEW_IDS" | tr ' ' '\n' | grep -v '^$' | jq -R . | jq -s .)
log "new feishu message ids" "{\"ids\": $IDS_JSON}"
wake "new feishu messages" "{\"ids\": $IDS_JSON}"
