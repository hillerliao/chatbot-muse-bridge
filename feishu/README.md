# feishu/ —— 飞书（Lark）↔ Muse 桥

用飞书官方 SDK 的长连接（WebSocket）收消息，不需要公网回调地址、域名或证书。
daemon 只做搬运：收到的消息写成 `inbox/<message_id>.json`，Muse 侧的回复写
`outbox/<message_id>.json` 后由 daemon 发回飞书。回答问题的是 Muse，不是 daemon。

## 处理范围

- **私聊（p2p）**：全部处理。
- **群聊**：只处理「机主本人 @ 机器人」的消息。其他成员的消息、没有 @ 机器人的
  群消息一律跳过。需要在凭证里配 `ownerOpenId` 和 `botOpenId` 才启用（见下）。
- 机器人自己发的消息跳过，防自问自答。
- 文字直接转纯文本；图片/文件/语音/视频下载到 `media/incoming/`（上限 20 MB）供
  Muse 侧读取。回复发送文字已实测；图片/文件发送已写码、未实测。

## 文件

| 文件 | 作用 |
| --- | --- |
| `daemon.py` | 长连接收消息 + outbox 发送（1 秒一轮），自带单实例保护与心跳 |
| `keepalive.sh` | daemon 死了就拉起来（setsid 分离会话）；没有凭证文件时直接退出 |
| `hook-poll.sh` | 给 Muse 的 hook 用：保活 + 发现 inbox 新消息时唤醒 worker |
| `requirements.txt` | `lark-oapi>=1.4`、`requests` |
| `state/credentials.json` | 凭证（权限 0600，已被 .gitignore 排除，绝不入库） |
| `state/credentials.example.json` | 凭证格式示例 |

运行时目录（均已 gitignore）：`inbox/`、`outbox/`、`processing/`、`sent/`、
`media/`、`logs/`、`.venv/`。

## 部署

```bash
cd feishu
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp state/credentials.example.json state/credentials.json
chmod 600 state/credentials.json   # 填入你自己的 appId / appSecret
bash keepalive.sh                  # 拉起 daemon，之后靠 hook 或手动保活
```

凭证字段：

- `appId` / `appSecret`：飞书开放平台自建应用的凭证。应用需开通机器人能力，
  并订阅事件 `im.message.receive_v1`。
- `domain`：`feishu`（中国版 open.feishu.cn，默认）或 `lark`（国际版）。两套不能混用。
- `ownerOpenId`（可选）：你自己在**本应用**维度下的 open_id。填了才处理群聊里你
  @ 机器人的消息。
- `botOpenId`（可选）：本应用机器人的 open_id，用来判定群消息有没有 @ 到机器人。

open_id 是应用维度的：同一个人在不同应用下 open_id 不同，别拿别的应用的来填。
查法：先私聊机器人发一句话，看 `inbox/` 里记录的 `from_user_id` 即是 ownerOpenId；
botOpenId 可在飞书开放平台应用页或机器人资料里查。

## 本机实测要点（踩过的坑）

- 出网走代理的环境里，lark-oapi 的 ws client 默认强制直连，会 SSL 失败；
  daemon.py 里有一段运行时补丁把 `proxy` 改回 `True`（走环境变量代理），
  不改 SDK 文件。直连环境不受影响。
- SDK 回调给的对象是 `P2ImMessageReceiveV1`，`sender` / `message` 在 `.event`
  里，直接读 `data.sender` 会抛 AttributeError 且消息丢失（飞书不会重发）。
- 单实例判活不能只看 pid：pid 会被无关进程复用，daemon 额外核对了
  `/proc/<pid>/cmdline`。

## 出处与致谢

本桥代码为本仓库原创，架构与同仓库的钉钉桥、微信桥同构。
「用飞书官方 SDK（lark-oapi）长连接订阅消息事件」这一接入思路，
参考了开源项目 [Slaslin/muse-feishu](https://github.com/Slaslin/muse-feishu)，在此致谢；
具体实现（文件流水线、保活、群聊 @ 口径等）均为本仓库自行编写。
