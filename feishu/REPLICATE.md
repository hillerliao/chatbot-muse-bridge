# 飞书 ↔ Muse 桥：复刻说明

> 这份文档写给另一个 Muse 实例：照着做，你就能给你的用户搭一条同样的飞书桥。
> 本桥已在 2026-11-21 搭成并端到端实测通过，2026-11-23 加了群聊 @ 支持。
> 全文不含任何真实凭证和真实 ID，尖括号里都是占位符，换成你那边的实际值。

## 0. 先说清为什么要自建

Muse 本身没有飞书通道（原生消息通道只有 WhatsApp）。所以路线是**自建桥**：
飞书机器人 ↔ 一个常驻小 daemon ↔ Muse 的 hook 机制。daemon 只做搬运、不回答问题；
回答问题的是被 hook 唤醒的 Muse worker。这样桥的逻辑和模型解耦，daemon 挂了不影响
Muse，Muse 不在线时消息先落盘排队。

整体结构（和钉钉桥、微信桥同构，如果你那边已有任意一座桥，照它的骨架改最省事）：

```
飞书私聊/群聊
  │  官方 SDK lark-oapi 长连接 (WebSocket) 订阅 im.message.receive_v1
  ▼
daemon.py ──写──> inbox/<message_id>.json
  ▲                    │ hook 每 5 秒轮询，发现新文件就 wake worker
  │                    ▼
  └──发回── outbox/ <── worker 读 inbox、思考、把回复写成 outbox/<message_id>.json
        发送前先 rename 到 processing/ 认领，发完归档到 sent/ 并写 status
```

为什么用长连接而不是 webhook 回调：不需要公网地址、不需要域名和证书，
在 NAT 后面的家用机/VM 上就能跑。这是飞书官方支持的事件订阅方式之一。

## 1. 飞书侧：建应用（用户手动操作，你给步骤）

1. 打开飞书开放平台（中国版 https://open.feishu.cn ；国际版 Lark 用 open.larksuite.com，
   两套系统不能混用，先问清用户用哪个）。
2. 创建**自建应用**，开启**机器人**能力。
3. 权限：开通发消息/收消息相关权限（`im:message` 系列：接收消息、以应用身份发消息）。
4. 事件订阅：订阅方式选**长连接**，添加事件 `im.message.receive_v1`（接收消息）。
5. 发布/启用应用，把机器人拉进要用的私聊和群。
6. 记下 App ID 和 App Secret——**只让用户通过安全方式给你**，落盘到
   `state/credentials.json`（权限 0600），不要写进聊天记录、日志、记忆或代码。

## 2. 本地结构

```
feishu/
├── daemon.py                  # 搬运 daemon（核心）
├── keepalive.sh               # 保活：daemon 死了拉起来，活着不动
├── hook-poll.sh               # Muse hook 脚本：保活 + 新消息 wake
├── requirements.txt           # lark-oapi>=1.4, requests
├── .venv/                     # 独立 venv，不要污染系统 Python
├── state/
│   ├── credentials.json       # 凭证，0600，gitignore，绝不入库
│   ├── credentials.example.json
│   ├── daemon.pid             # {"pid": ..., "started_at": ...}
│   └── heartbeat.json         # daemon 每 5 秒刷新
├── inbox/  outbox/  processing/  sent/   # 消息流水线，全部 gitignore
├── media/incoming/            # 下载的图片/文件，上限 20 MB
└── logs/daemon.log            # 只记消息 ID 和长度，不记正文、不记凭证
```

`credentials.json` 格式：

```json
{
  "appId": "<APP_ID>",
  "appSecret": "<APP_SECRET>",
  "domain": "feishu",
  "ownerOpenId": "<可选，群聊@ 用>",
  "botOpenId": "<可选，群聊@ 用>"
}
```

`domain` 填 `feishu`（中国版）或 `lark`（国际版）。

## 3. daemon.py 的关键设计

- **只搬运**：收到事件 → 解析 → 写 inbox 文件；outbox 每 1 秒扫一轮，发送走 SDK 的
  REST：有原消息可引用时用 `message.reply`，否则用 `message.create` 发到 chat_id。
- **inbox 记录字段**：`message_id`、`from_user_id`（发送者 open_id）、`union_id`、
  `sender_type`、`chat_id`、`chat_type`、`type`（text/image/file/voice/video/other）、
  `msgtype`、`text`、`received_at`；媒体类先用 `message_resource.get` 下载到
  `media/incoming/<message_id>.<ext>`，记录里给相对路径，下载失败就把失败原因写进 text，
  不要编造内容。
- **防自问自答**：`sender_type == "app"`（机器人自己发的）直接跳过。
- **outbox 防重发**：先把文件 rename 到 `processing/` 再发；发完写 `sent/<name>`，
  带 `status: sent/failed`；若 `sent/` 里已有相同 reply_to + 相同正文，跳过不重发。
  失败最多重试 3 次后归档 failed。长文本按 4000 字分片发。
- **单实例保护**：启动时读 `daemon.pid`，旧 pid 活着时**必须再核对
  `/proc/<pid>/cmdline` 里确实有本 daemon** 才算活——只 `kill(pid, 0)` 判活会被
  pid 复用坑（别的进程占了旧 pid，会误判 daemon 还在、永远不拉新实例）。
- **心跳**：独立线程每 5 秒写 `heartbeat.json`（pid、started_at、status、last_msg_at）。

## 4. 五个实测踩过的坑（最重要，照着避）

1. **代理补丁**：如果运行环境出网要走环境代理（https_proxy），lark-oapi 的 ws client
   默认把 websockets 的 `proxy` 参数设为 None 强制直连，直连会报 SSL
   `WRONG_VERSION_NUMBER` 连不上。解法是运行时 monkeypatch，不改 SDK 文件：

   ```python
   import lark_oapi.ws.client as _ws_mod
   _orig = _ws_mod._ws_connect_kwargs
   def _patched(connection_headers=None):
       kwargs = _orig(connection_headers)
       kwargs['proxy'] = True   # 改为按环境变量走代理
       return kwargs
   _ws_mod._ws_connect_kwargs = _patched
   ```

2. **事件对象要解包**：SDK 回调拿到的对象是 `P2ImMessageReceiveV1`，真正的
   `sender` / `message` 在它的 `.event` 属性上（类型 `P2ImMessageReceiveV1Data`）。
   直接读 `data.sender` 会 AttributeError——事件到了却写不了 inbox，而且**飞书不会
   重发**，消息就永久丢了。先 `payload = data.event or data` 再读字段。

3. **保活拉起必须做会话分离**：用 `setsid nohup <venv>/python daemon.py >> logs/daemon.log 2>&1 &`。
   只用 `&` 后台跑，拉起它的 hook/worker 会话一结束，daemon 会被一起带走
   （实测一天死三次且日志无退出记录，就是这个原因）。

4. **保活的“已报告死亡”标记要绑定具体事件**：hook 报告 daemon 死亡时，标记里存
   心跳文件的 mtime；心跳恢复后标记自然失效。只存一个布尔标记的话，第一次死亡
   报过之后会永久静默，新实例再死也没人知道。

5. **keepalive 防连环拉**：30 秒内刚拉过就不再拉（hook 是 5 秒一轮，没这个闸会
   在 daemon 启动慢时叠启多个实例；单实例保护是第二道防线，两道都要）。

## 5. hook 集成（Muse 侧）

- hook 每 5 秒跑 `hook-poll.sh`，三件事：
  1. 没有 `state/credentials.json` → 直接 silent 退出（桥还没接通时不要报错刷屏）；
  2. 调 `keepalive.sh` 保活；心跳超过 120 秒没更新 → wake 报一次（按第 4 条的标记法）；
  3. 扫 `inbox/*.json`，把没见过的 message_id 记进一个 seen 文件，**只带消息 ID**
     wake worker（不带正文，正文由 worker 自己去读文件）。
- worker 被唤醒后：读 inbox 文件 → 在自己的会话里回复用户（让用户在主会话能看到
  完整记录）→ 把要发回飞书的文本写成 outbox 文件：
  `{"reply_to": "<message_id>", "kind": "text", "text": "...", "chat_id": "<chat_id>"}`
  → daemon 1 秒内发走并归档到 `sent/`，worker 回读 `sent/` 确认 `status: sent` 才算完。
- 图片/文件发送：代码路径是先用 `im.v1.image.create` / `im.v1.file.create` 上传拿 key
  再发消息；**本桥未实测**，你那边第一次用时先小量验证，不要直接当可用。

## 6. 群聊支持（v1.1）

默认只处理私聊。群聊规则（用户拍板的安全口径，建议照抄）：

- 群消息只处理**「机主本人 @ 机器人」**这一种：发送者 open_id == `ownerOpenId`，
  且 `message.mentions` 里有 open_id == `botOpenId` 的一项，才写 inbox；其余群消息
  （别人发的、没 @ 的）一律跳过并记一行日志。
- 理由：群里其他人的话不能当成机主的指令执行；机器人也不该在群里逢话就接。
- 两个 open_id 都是**应用维度**的：同一个人/机器人在不同应用下 open_id 不同。
  `ownerOpenId` 的查法：让用户私聊机器人发一句话，看 inbox 记录里的 `from_user_id`。
  `botOpenId` 在开放平台应用页/机器人资料里查。
- 回复路径不用改：outbox 回复用 reply 接口引用原消息，群聊里同样有效。

## 7. 上线验证清单（按顺序，全过才算接通）

1. `credentials.json` 就位且权限 600；跑 `keepalive.sh`，`logs/daemon.log` 出现
   启动行且无报错，`heartbeat.json` 在 5 秒内刷新、pid 对得上真实进程
   （`ps -p <pid>` 的 cmdline 是本 daemon）。
2. 用户私聊机器人发一句话 → `inbox/` 出现对应文件、内容正确 → worker 写 outbox →
   `sent/` 出现 `status: sent` → 用户在飞书里收到回复。
3. 杀掉 daemon → 等一轮 hook → 确认被自动拉起且能继续收发（模拟 VM 替换后的自愈）。
4. （配了群聊时）用户在群里 @ 机器人发一句话 → 能回复；用户不 @ 发一句话 →
   不回复、日志有一行跳过记录。

## 8. 运维与安全口径

- 凭证只存在 `state/credentials.json` 一处；日志只记消息 ID、类型、字数；
  排查问题先看 `logs/daemon.log` 和 `sent/` 归档，不要把正文打进日志。
- daemon 死了先看日志尾部再重启，不要盲目连环重启；长连接偶发断开由 SDK 自行重连。
- 整个 `feishu/` 目录建议 git 管理，但 `state/credentials.json` 和
  inbox/outbox/processing/sent/media/logs/.venv 必须 gitignore——消息正文和凭证
  都不进仓库。
- 参考实现（机主的仓库，含钉钉/微信两个同构桥可对照）：
  `chatbot-muse-bridge` 仓库的 `feishu/` 目录。
