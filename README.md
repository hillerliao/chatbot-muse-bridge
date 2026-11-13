# chatbot-muse-bridge v1.0

把微信和钉钉接进 Muse 的轻量桥：两个独立的 daemon + inbox/outbox 文件队列 + 每 5 秒轮询的 hook。daemon 只做搬运，不回答问题；回答问题的是 Muse。

> v1.0 — 微信、钉钉两条链路均已端到端验证（2026-11-13）。社区桥接方案，非官方连接器。

## 它是怎么跑的

```
微信私聊 ──腾讯 iLink──> wechat/daemon.js ──> wechat/inbox/<id>.json
                                                    │
                                          hook 每 5 秒检查，有新消息就唤醒 Muse
                                                    │
                                              Muse 读 inbox，写回复
                                                    │
钉钉私聊 ──钉钉 Stream──> dingtalk/daemon.py ──> dingtalk/inbox/<id>.json
                                                    ▼
                                          wechat|dingtalk/outbox/<id>.json
                                                    │
                                          daemon 读 outbox，发回原渠道
```

关键设计只有三条：

1. **daemon 只负责搬运。** 收到消息只写 `inbox/`，看到 `outbox/` 有回复只管发。收发和回答分开，daemon 崩了也不会丢已经写盘的消息。
2. **Muse 靠 hook 被叫醒。** hook 脚本每 5 秒扫一次 `inbox/`，有没见过的消息才唤醒 Muse，没有就静默。Muse 不用常驻监听。
3. **不用公网回调地址。** 微信走 iLink 长轮询，钉钉走官方 Stream 出站长连接，内网机器也能跑。

两个桥完全独立：只用微信就只跑 `wechat/`，只用钉钉就只跑 `dingtalk/`，互不影响。

## 目录

```
chatbot-muse-bridge/
├── README.md                  # 本文件
├── .gitignore                 # 凭证、日志、消息队列一律不入库
├── wechat/
│   ├── daemon.js              # 微信 iLink daemon（零依赖，Node）
│   ├── login-only.js          # 只做一次扫码登录，不常驻
│   ├── keepalive.sh           # daemon 保活
│   ├── hook-poll.sh           # 每 5 秒轮询 hook 脚本
│   └── state/
│       └── credentials.example.json   # 凭证模板，只有占位符
└── dingtalk/
    ├── daemon.py              # 钉钉 Stream daemon（Python）
    ├── requirements.txt       # dingtalk-stream==0.24.3
    ├── keepalive.sh           # daemon 保活
    ├── hook-poll.sh           # 每 5 秒轮询 hook 脚本
    └── state/
        └── credentials.example.json   # 凭证模板，只有占位符
```

运行时各桥目录下会自动生成 `state/`（凭证、心跳）、`inbox/`、`outbox/`、`processing/`、`sent/`、`logs/`、`media/`，这些都在 `.gitignore` 里，永远不要提交。

## 微信桥：wechat/

### 原理

腾讯 iLink Bot API（`ilinkai.weixin.qq.com`）：手机扫码授权生成一个机器人身份，daemon 用长轮询 `getupdates` 收消息（35 秒一轮，游标落盘，重启后断点续拉），用 `sendmessage` 发回复（带 `context_token`，2000 字分片）。协议实现对照开源 SDK `epiral/weixin-bot` v1.2.0（MIT）的 `src/api.ts`、`client.ts`、`auth.ts` 逐项核对过。

### 部署步骤

1. **扫码登录，拿到凭证。**
   ```bash
   cd wechat
   node login-only.js
   ```
   按提示在微信里打开二维码链接、点确认。成功后凭证写进 `wechat/state/credentials.json`（权限 0600），终端打印 `LOGIN-CONFIRMED`。二维码过期会自动重取。
   如果登录的机器和跑 daemon 的机器不是同一台（比如登录要在家庭宽带出口做），把 `state/credentials.json` 拿回跑 daemon 的机器上再启动。登录只做一次，不常驻、不轮询。
   也可以直接跑 `node daemon.js`：没凭证时它会先走扫码登录流程，再进长轮询。

2. **启动 daemon。**
   ```bash
   bash keepalive.sh
   tail -n 50 logs/daemon.log
   ```

3. **装 hook，让 Muse 能收到新消息。** 把 `wechat/hook-poll.sh` 复制到 Muse 的 hooks 脚本目录，注册一个每 5 秒的 hook（id 建议 `wechat-ilink-poll`）。worker 指令模板见下面“Hook worker 指令”一节。没装 hook 之前，daemon 只收消息进 `inbox/`，不会有人回。

4. **发一句测试。** 在微信里私聊机器人发一句，看能不能回。通了再往下用。

### 微信收发能力（已实测）

- 收：文字；图片、视频、文件从 CDN 下载解密后存到 `media/incoming/`，`inbox` 里记文件路径，Muse 能直接看；语音按转写文字处理。
- 发：文字（2000 字分片）；图片、视频、文件走上传路径。
- 不支持：机器人发语音，没有稳定的公开实现。

### 微信已知限制

- 只能私聊，不能进群（iLink 侧限制）。
- 一个 bot token 只给一个网关用。给 Muse 新建一个机器人身份，别复用其他网关的身份。
- 会话过期（`errcode=-14`）时 daemon 会清掉凭证、重新扫码登录。

## 钉钉桥：dingtalk/

### 原理

钉钉官方 Stream 模式：出站 WebSocket 长连接，不用公网地址、不用配回调 URL。用官方 SDK `dingtalk-stream` 0.24.3：

```python
client = DingTalkStreamClient(Credential(client_id, client_secret))
client.register_callback_handler(ChatbotMessage.TOPIC, handler)
# ChatbotMessage.TOPIC = '/v1.0/im/bot/messages/get'
```

收到的消息写 `inbox/`；回复走该条消息自带的 session webhook（SDK 的 `ChatbotHandler.reply_text` 直接 POST 到它，不用另外取 access token）。没凭证时 daemon 直接退出（exit 2），不发起网络连接。

### 部署步骤

1. **在钉钉开放平台建机器人（约 10 分钟，需要企业管理员账号）。**
   - 打开钉钉开放平台，创建一个**企业内部应用**。
   - 应用里加**机器人**能力，消息接收方式选 **Stream 模式**（不要选 HTTP 回调，本桥没配公网地址）。
   - 权限里把机器人收发消息相关的权限开好，按控制台提示勾选。
   - 发布应用，把机器人加进你自己的单聊（私聊），可用范围加上你自己。
   - 在应用的**凭证与基础信息**页，复制 **Client ID** 和 **Client Secret**。

2. **填凭证、启动。**
   ```bash
   cd dingtalk
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   cp state/credentials.example.json state/credentials.json
   chmod 600 state/credentials.json
   # 用编辑器把 credentials.json 里的两项换成你复制的真值，别贴进聊天记录、别提交
   bash keepalive.sh
   tail -n 50 logs/daemon.log
   ```

3. **装 hook。** 把 `dingtalk/hook-poll.sh` 复制到 Muse 的 hooks 脚本目录，注册一个每 5 秒的 hook（id 建议 `dingtalk-muse-poll`）。worker 指令模板见下一节。

4. **发一句测试。** 在钉钉里私聊机器人发一句，看能不能回。

### 钉钉收发能力

- 收：文字先行；图片只记下载码，不下载、不转交；文件、语音等其他类型只记类型。
- 发：文字，按 4000 字分片发送。
- 回复走消息自带的 session webhook，有过期时间。Muse 回得太晚（超过过期时间）那条就发不出去，daemon 会归档到 `sent/` 并写明原因。

### 钉钉已知限制

- 仅支持私聊。群聊需 @ 机器人，当前版本未覆盖。
- 一个机器人身份只给一个网关用。给 Muse 新建一个，别复用其他机器人的身份。

## Hook worker 指令（模板）

hook 脚本只负责“发现新消息、叫醒 Muse”，回答由被唤醒的 Muse worker 做。注册 hook 时用的 worker 指令大意如下，按你的 Muse 环境改路径和投递目标：

> 被唤醒时，wake payload 里有新消息的 id。逐个读 `<桥目录>/inbox/<id>.json`（不要把 session webhook 或凭证写进聊天记录或日志）。用简明扼要的中文回答用户的问题。然后把回复写进 `<桥目录>/outbox/<id>.json`：微信桥的字段照 `inbox` 原样抄（`reply_to`、`text`、`kind`、`context_token`、`to_user_id`）；钉钉桥的字段照 `inbox` 原样抄（`reply_to`、`kind="text"`、`text`、`session_webhook`、`session_webhook_expired_time`、`from_staff_id`、`from_user_id`、`conversation_id`、`conversation_type`）。daemon 会在 1 秒内发回原渠道。写完后在聊天里留一条完整记录：原话全文 + 回复全文。只回文字，不发图片/文件/语音。如果 `inbox` 文件读不到或消息不是文字，说明原因，不要编造回复。

注意：不要手动插手已经被 hook 唤醒的 worker。等它回完再查，否则同一条消息会回两次。

## 启动、查看、停用

```bash
# 启动 / 重拉（任一桥）
bash keepalive.sh          # 在 wechat/ 或 dingtalk/ 目录下跑

# 看状态
tail -n 50 logs/daemon.log
cat state/heartbeat.json   # pid、状态、最后收到消息的时间

# 停用：先停 hook，再停 daemon，再决定是否删凭证（删了等于解绑）
# 微信：kill $(jq .pid state/daemon.pid)
# 钉钉：kill $(python3 -c "import json; print(json.load(open('state/daemon.pid'))['pid'])")
```

## 已知的代价（两个桥共用）

1. **部署在常驻机器上最稳。** 如果跑在会定期更换的虚拟机上（约两小时一换），daemon 会随机器更换而中断，由 hook 重拉恢复，期间消息会晚到几分钟。
2. **私聊。** 微信仅支持私聊；钉钉当前版本仅支持私聊。
3. **媒体。** 微信的图片/视频/文件已验证；钉钉的图片/文件尚未验证；两个桥均不支持机器人发语音。
4. **一个身份一个网关。** 微信 token、钉钉 Client ID/Secret 都只能给一个网关用，别共用。
5. **回复有时效。** 钉钉的 session webhook 会过期；微信的会话过期要重扫码。

## 排障

| 现象 | 先查什么 |
| --- | --- |
| daemon 起不来 | `logs/daemon.log` 前几行：没凭证会干净退出（钉钉 exit 2）；缺 SDK 会提示先装依赖 |
| 收到消息但没人回 | hook 装了没、启用了没；`inbox/` 里有没有新文件；worker 有没有写 `outbox/` |
| 回复发不出去 | `sent/` 里那条的 `status` 和 `error`：钉钉常见是 session webhook 过期；微信常见是会话过期要重扫码 |
| 消息晚到几分钟 | 虚拟机刚换过，daemon 在重拉；看 `state/heartbeat.json` 的更新时间 |
| 收到重复回复 | 主聊天手动插手了已经被 hook 唤醒的 worker；等它回完再查 |

## 这不是什么

- **不是官方连接器。** Muse 没有微信/钉钉官方连接器，这座桥是自搭的轻量网关。
- **不需要公网服务器。** 两个桥都不用配回调 URL、域名、证书；也不用在 Muse 网页里挂 Cookie。
- **不是全功能客户端。** 群聊、语音发送、钉钉图片/文件不在 v1.0 范围内。
- **凭证自管。** 凭证只存在你自己机器上的 `state/credentials.json`（0600），不要贴进聊天记录、不要提交进仓库。

## 安全提醒

- 仓库里只有占位符。真凭证、日志、消息队列、媒体文件都在 `.gitignore` 里，提交前用 `git status` 核对一遍。
- 轮换凭证后记得同时更新 `state/credentials.json` 并重启 daemon；旧的机器人身份不用了就去开放平台 / 微信里解绑。
- 别把 `state/` 整个目录发给别人，里面除了凭证还有登录态和游标。
