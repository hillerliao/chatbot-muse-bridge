#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Feishu / Lark <-> Muse daemon v1 —— 只做搬运，不回答问题。

    飞书私聊 -> 飞书长连接 (WebSocket) -> daemon -> inbox/<message_id>.json
    Muse worker 回复 -> 写 outbox/<message_id>.json -> daemon 发回飞书

传输（已核实，lark-oapi 官方 SDK，本机 venv 实测 API 表面）：
    官方 SDK lark-oapi 的 ws.Client 出站长连接订阅
    im.message.receive_v1，不用公网回调地址、不用域名证书。
      ws.Client(app_id, app_secret, event_handler=..., domain=...)
      EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(handler).build()
    回复走 REST client（同一个 SDK 自动取 tenant_access_token）：
      client.im.v1.message.reply(ReplyMessageRequest)
      普通发消息：client.im.v1.message.create(CreateMessageRequest)

凭证：只读 state/credentials.json
    {"appId": "...", "appSecret": "...", "domain": "feishu" | "lark",
     "ownerOpenId": "...", "botOpenId": "..."}
    后两个可省：填了才启用群聊 @ 处理（ownerOpenId = 你自己的 open_id，
    botOpenId = 本应用机器人的 open_id），不填则只处理私聊。
    domain 可省，默认 feishu（中国版 open.feishu.cn）；
    lark = 国际版 open.larksuite.com。两套系统不能混用。
    文件权限应为 0600。凭证值绝不打进日志，绝不写到别处。
    没有凭证时 daemon 直接退出（exit 2），不碰网络，不假装连接。

范围（v1.2）：私聊（chat_type=p2p）全处理；群聊只处理「用户本人
    @ 机器人」的消息（2026-11-23 用户催办后加），其他成员的消息、
    没 @ 机器人的群消息一律跳过并记日志。机器人自己的消息
    （sender_type=app）跳过，防止自问自答。收到用户消息后先点一个
    👍 再等 worker 回复（2026-11-23 用户说“你定吧”后定为加、用 👍）。
文字先行。图片/文件/音频/视频用 message_resource 下载到 media/incoming/
    供 worker 直接看；下载失败只记原因，不编造内容。
日志只记消息长度和消息 ID，不记正文，不记凭证。
"""

import json
import logging
import os
import re
import signal
import sys
import threading
import time
import uuid

ROOT = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(ROOT, 'state')
INBOX = os.path.join(ROOT, 'inbox')
OUTBOX = os.path.join(ROOT, 'outbox')
PROCESSING = os.path.join(ROOT, 'processing')
SENT = os.path.join(ROOT, 'sent')
LOGS = os.path.join(ROOT, 'logs')
MEDIA = os.path.join(ROOT, 'media', 'incoming')
MAX_MEDIA_BYTES = 20 * 1024 * 1024
CRED_PATH = os.path.join(STATE, 'credentials.json')
HEARTBEAT_PATH = os.path.join(STATE, 'heartbeat.json')
PID_PATH = os.path.join(STATE, 'daemon.pid')

CHUNK_LIMIT = 4000
OUTBOX_MAX_ATTEMPTS = 3

STARTED_AT = time.strftime('%Y-%m-%dT%H:%M:%S%z')
STOP = threading.Event()
STATUS = {'status': 'starting', 'last_poll_at': None, 'last_msg_at': None}

log = logging.getLogger('feishu-muse-daemon')

DOMAINS = {
    'feishu': 'https://open.feishu.cn',
    'lark': 'https://open.larksuite.com',
}

# 群聊 @ 判定（v1.1）：用户本人的 open_id 与机器人本应用的 open_id，
# 均为本应用维度 ID、不是凭证。值从 credentials.json 的 ownerOpenId /
# botOpenId 读（见 load_credentials），启动时填入；任一缺失则群聊不处理、
# 只做私聊。每个部署者填自己的，不要把别人的 ID 写死在代码里。
USER_OPEN_ID = ''
BOT_OPEN_ID = ''


def setup_logging():
    os.makedirs(LOGS, exist_ok=True)
    handler = logging.FileHandler(os.path.join(LOGS, 'daemon.log'), encoding='utf-8')
    handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
    log.addHandler(handler)
    log.addHandler(logging.StreamHandler(sys.stderr))
    log.setLevel(logging.INFO)


def log_line(msg):
    log.info(msg)


def atomic_write_json(path, obj, mode=0o644):
    tmp = '%s.tmp-%d' % (path, os.getpid())
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write('\n')
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def read_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except (ValueError, OSError):
        return None


def safe_msg_id(raw):
    raw = str(raw or '')
    if raw and re.fullmatch(r'[A-Za-z0-9_.\-]+', raw):
        return raw
    return uuid.uuid4().hex


def load_credentials():
    """只读 state/credentials.json。缺失或格式不对返回 None，不碰网络。"""
    creds = read_json(CRED_PATH)
    if not isinstance(creds, dict):
        return None
    app_id = creds.get('appId') or creds.get('app_id')
    app_secret = creds.get('appSecret') or creds.get('app_secret')
    if not isinstance(app_id, str) or not app_id.strip():
        return None
    if not isinstance(app_secret, str) or not app_secret.strip():
        return None
    domain_key = str(creds.get('domain') or 'feishu').strip().lower()
    domain = DOMAINS.get(domain_key)
    if domain is None:
        # 也允许直接填完整域名 URL
        if domain_key.startswith('https://'):
            domain = domain_key
        else:
            return None
    owner_open_id = str(creds.get('ownerOpenId') or '').strip()
    bot_open_id = str(creds.get('botOpenId') or '').strip()
    return app_id.strip(), app_secret.strip(), domain, owner_open_id, bot_open_id


def chunk_text(text, limit=CHUNK_LIMIT):
    chars = list(text)
    out = [''.join(chars[i:i + limit]) for i in range(0, len(chars), limit)]
    return out if out else ['']


def parse_content(raw):
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def extract_text(msg_type, content):
    """从飞书消息 content 里提取纯文本。图片等类型返回 (kind, text, keys)。"""
    if msg_type == 'text':
        text = str(content.get('text') or '')
        # 去掉 @ 占位符 @_user_1 之类，正文里保留可读性即可
        text = re.sub(r'@_user_\d+', '', text).strip()
        return 'text', text, {}
    if msg_type == 'post':
        parts = []
        body = content.get('content') or []
        title = content.get('title')
        if title:
            parts.append(str(title))
        for para in body if isinstance(body, list) else []:
            line = ''
            if isinstance(para, list):
                for el in para:
                    if isinstance(el, dict) and el.get('tag') == 'text':
                        line += str(el.get('text') or '')
                    elif isinstance(el, dict) and el.get('tag') == 'a':
                        line += str(el.get('text') or el.get('href') or '')
            if line:
                parts.append(line)
        return 'text', '\n'.join(parts), {}
    if msg_type == 'image':
        return 'image', '', {'image_key': content.get('image_key')}
    if msg_type == 'file':
        return 'file', '', {'file_key': content.get('file_key'),
                             'file_name': content.get('file_name')}
    if msg_type == 'audio':
        return 'voice', '', {'file_key': content.get('file_key'),
                             'duration_ms': content.get('duration')}
    if msg_type in ('media', 'video'):
        return 'video', '', {'file_key': content.get('file_key'),
                             'image_key': content.get('image_key'),
                             'duration_ms': content.get('duration')}
    return 'other', '', {}


def sniff_ext(data, kind):
    if data[:3] == b'\xff\xd8\xff':
        return 'jpg'
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        return 'png'
    if data[:6] in (b'GIF87a', b'GIF89a'):
        return 'gif'
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'webp'
    if data[4:8] == b'ftyp':
        return 'mp4'
    if data[:4] == b'%PDF':
        return 'pdf'
    if data[:2] == b'PK':
        return 'zip'
    return {'image': 'jpg', 'video': 'mp4', 'voice': 'amr', 'file': 'bin'}.get(kind, 'bin')


class FeishuTransport:
    """传输层隔离：daemon 其余部分只认 inbox/outbox 文件，不认 SDK。
    REST client 在这里建，长连接 handler 也在这里注册。"""

    def __init__(self, lark, app_id, app_secret, domain):
        self.lark = lark
        self.client = (lark.Client.builder()
                       .app_id(app_id).app_secret(app_secret)
                       .domain(domain)
                       .log_level(lark.LogLevel.WARNING)
                       .build())
        self.app_id = app_id
        self.app_secret = app_secret
        self.domain = domain

    # ---------------------------------------------------------- 下载媒体
    def download_resource(self, message_id, file_key, res_type, kind, file_name=None):
        """message_resource.get -> 存 media/incoming/。返回 (相对路径, 错误)。"""
        if not file_key:
            return None, '没有 file_key/image_key'
        from lark_oapi.api.im.v1 import GetMessageResourceRequest
        req = (GetMessageResourceRequest.builder()
               .message_id(message_id).file_key(file_key).type(res_type)
               .build())
        try:
            resp = self.client.im.v1.message_resource.get(req)
        except Exception as e:
            return None, '下载请求失败：%s' % e
        if not resp.success():
            return None, '下载失败 code=%s msg=%s' % (resp.code, resp.msg)
        data = None
        file_obj = getattr(resp, 'file', None)
        if file_obj is not None:
            try:
                if hasattr(file_obj, 'seek'):
                    file_obj.seek(0)
                data = file_obj.read()
            except Exception as e:
                return None, '读下载内容失败：%s' % e
        if data is None:
            raw = getattr(resp, 'raw', None)
            data = getattr(raw, 'content', None) if raw is not None else None
        if not isinstance(data, (bytes, bytearray)) or not data:
            return None, '下载到空文件'
        data = bytes(data)
        if len(data) > MAX_MEDIA_BYTES:
            return None, '文件太大（%d 字节，上限 %d）' % (len(data), MAX_MEDIA_BYTES)
        ext = sniff_ext(data, kind)
        if kind == 'file' and file_name and '.' in os.path.basename(str(file_name)):
            ext = os.path.basename(str(file_name)).rsplit('.', 1)[-1][:10] or ext
        rel = 'media/incoming/%s.%s' % (message_id, ext)
        path = os.path.join(ROOT, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            f.write(data)
        return rel, None

    # ---------------------------------------------------------- 表情回应
    def react_thumbsup(self, message_id):
        """给收到的消息点一个 👍（emoji_type=THUMBSUP）。
        只尽力而为：失败抛错由调用方记日志，不阻塞收信/回信。"""
        from lark_oapi.api.im.v1 import (
            CreateMessageReactionRequest, CreateMessageReactionRequestBody, Emoji)
        body = (CreateMessageReactionRequestBody.builder()
                .reaction_type(Emoji.builder().emoji_type('THUMBSUP').build())
                .build())
        req = (CreateMessageReactionRequest.builder()
               .message_id(message_id).request_body(body).build())
        resp = self.client.im.v1.message_reaction.create(req)
        if not resp.success():
            raise RuntimeError('reaction failed code=%s msg=%s' % (resp.code, resp.msg))
        return resp

    # ---------------------------------------------------------- 发送
    def reply_text(self, message_id, text):
        from lark_oapi.api.im.v1 import ReplyMessageRequest, ReplyMessageRequestBody
        body = (ReplyMessageRequestBody.builder()
                .content(json.dumps({'text': text}, ensure_ascii=False))
                .msg_type('text')
                .build())
        req = ReplyMessageRequest.builder().message_id(message_id).request_body(body).build()
        resp = self.client.im.v1.message.reply(req)
        if not resp.success():
            raise RuntimeError('reply failed code=%s msg=%s' % (resp.code, resp.msg))
        return resp

    def send_text(self, chat_id, text):
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
        body = (CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .content(json.dumps({'text': text}, ensure_ascii=False))
                .msg_type('text')
                .build())
        req = (CreateMessageRequest.builder()
               .receive_id_type('chat_id').request_body(body).build())
        resp = self.client.im.v1.message.create(req)
        if not resp.success():
            raise RuntimeError('send failed code=%s msg=%s' % (resp.code, resp.msg))
        return resp

    def send_media(self, chat_id, media_path, kind):
        """图片：先 im.v1.image.create 上传拿 image_key 再发；
        文件：先 im.v1.file.create 上传拿 file_key 再发。未实测前失败会
        如实抛错，outbox 归档 failed，不假装发成功。"""
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
        path = media_path if os.path.isabs(media_path) else os.path.join(ROOT, media_path)
        if kind == 'image':
            from lark_oapi.api.im.v1 import CreateImageRequest, CreateImageRequestBody
            with open(path, 'rb') as f:
                body = CreateImageRequestBody.builder().image_type('message').image(f).build()
                req = CreateImageRequest.builder().request_body(body).build()
                resp = self.client.im.v1.image.create(req)
            if not resp.success():
                raise RuntimeError('image upload failed code=%s msg=%s' % (resp.code, resp.msg))
            key = resp.data.image_key
            content = json.dumps({'image_key': key})
            msg_type = 'image'
        elif kind == 'file':
            from lark_oapi.api.im.v1 import CreateFileRequest, CreateFileRequestBody
            ext = os.path.splitext(path)[1].lstrip('.').lower() or 'stream'
            file_type = ext if ext in ('opus', 'mp4', 'pdf', 'doc', 'xls', 'ppt', 'stream') else 'stream'
            with open(path, 'rb') as f:
                body = (CreateFileRequestBody.builder()
                        .file_type(file_type).file_name(os.path.basename(path)).file(f).build())
                req = CreateFileRequest.builder().request_body(body).build()
                resp = self.client.im.v1.file.create(req)
            if not resp.success():
                raise RuntimeError('file upload failed code=%s msg=%s' % (resp.code, resp.msg))
            key = resp.data.file_key
            content = json.dumps({'file_key': key})
            msg_type = 'file'
        else:
            raise RuntimeError('kind=%s send untested' % kind)
        body = (CreateMessageRequestBody.builder()
                .receive_id(chat_id).content(content).msg_type(msg_type).build())
        req = (CreateMessageRequest.builder()
               .receive_id_type('chat_id').request_body(body).build())
        resp = self.client.im.v1.message.create(req)
        if not resp.success():
            raise RuntimeError('media send failed code=%s msg=%s' % (resp.code, resp.msg))
        return resp


# ---------------------------------------------------------------- 收到消息

def build_inbox_record(transport, data):
    """P2ImMessageReceiveV1Data -> inbox 文件内容。只存回复必需字段。"""
    msg = data.message
    sender = data.sender
    msg_id = safe_msg_id(getattr(msg, 'message_id', None))
    msg_type = getattr(msg, 'message_type', None) or 'unknown'
    content = parse_content(getattr(msg, 'content', None))
    kind, text, extra_keys = extract_text(msg_type, content)

    record_extra = {}
    if kind in ('image', 'file', 'voice', 'video'):
        key = extra_keys.get('image_key') if kind == 'image' else extra_keys.get('file_key')
        res_type = 'image' if kind == 'image' else 'file'
        rel, err = transport.download_resource(msg_id, key, res_type, kind,
                                               extra_keys.get('file_name'))
        label = {'image': '图片', 'file': '文件', 'voice': '语音', 'video': '视频'}[kind]
        name_part = ('：%s' % extra_keys['file_name']) if extra_keys.get('file_name') else ''
        if rel:
            text = '[%s%s，已下载：%s]' % (label, name_part, rel)
            record_extra['media_path'] = rel
        else:
            text = '[%s%s，下载失败：%s]' % (label, name_part, err)
        record_extra.update({k: v for k, v in extra_keys.items() if v is not None})

    sender_id = getattr(sender, 'sender_id', None)
    record = {
        'message_id': msg_id,
        'from_user_id': getattr(sender_id, 'open_id', None) if sender_id else None,
        'union_id': getattr(sender_id, 'union_id', None) if sender_id else None,
        'sender_type': getattr(sender, 'sender_type', None),
        'chat_id': getattr(msg, 'chat_id', None),
        'chat_type': getattr(msg, 'chat_type', None),
        'type': kind,
        'msgtype': msg_type,
        'text': text,
        'received_at': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
    }
    record.update(record_extra)
    return record


def build_event_handler(transport):
    lark = transport.lark

    def on_message(data):
        try:
            # SDK 回调给的是 P2ImMessageReceiveV1，sender/message 在 .event 里
            # （P2ImMessageReceiveV1Data）；2026-11-21 实测踩坑：直接读
            # data.sender 会 AttributeError，事件到达却写不了 inbox。
            payload = getattr(data, 'event', None) or data
            sender_type = getattr(payload.sender, 'sender_type', None)
            if sender_type == 'app':
                return  # 机器人自己的消息，跳过防自问自答
            chat_type = getattr(payload.message, 'chat_type', None)
            if chat_type == 'group':
                # v1.1 群聊：只处理「用户本人 @ 机器人」的消息，其余跳过
                sender_id = getattr(payload.sender, 'sender_id', None)
                sender_open = getattr(sender_id, 'open_id', None) if sender_id else None
                mentions = getattr(payload.message, 'mentions', None) or []
                bot_mentioned = any(
                    getattr(getattr(m, 'id', None), 'open_id', None) == BOT_OPEN_ID
                    for m in mentions)
                if (not USER_OPEN_ID or not BOT_OPEN_ID
                        or sender_open != USER_OPEN_ID or not bot_mentioned):
                    log_line('群消息跳过（本人=%s，@机器人=%s）%s' % (
                        sender_open == USER_OPEN_ID, bot_mentioned,
                        getattr(payload.message, 'message_id', '')))
                    return
            elif chat_type != 'p2p':
                log_line('非私聊消息（chat_type=%s），跳过 %s' % (
                    chat_type, getattr(payload.message, 'message_id', '')))
                return
            record = build_inbox_record(transport, payload)
            msg_id = record['message_id']
            inbox_file = os.path.join(INBOX, msg_id + '.json')
            if os.path.exists(inbox_file):
                return  # 去重
            atomic_write_json(inbox_file, record, 0o644)
            # v1.2：收到即先点一个 👍 再等 worker 回复（2026-11-23 用户说“你定吧”，
            # 定为：加，表情就用 👍）。失败只记日志，不影响收信。
            try:
                transport.react_thumbsup(msg_id)
                log_line('已给消息 %s 点 👍' % msg_id)
            except Exception as e:
                log_line('给消息 %s 点 👍 失败：%s' % (msg_id, e))
            STATUS['last_msg_at'] = time.strftime('%Y-%m-%dT%H:%M:%S%z')
            STATUS['last_poll_at'] = STATUS['last_msg_at']
            log_line('收到飞书消息 %s（%s，%d 字）' % (
                msg_id, record['type'], len(record.get('text') or '')))
        except Exception as e:
            log_line('处理飞书事件出错：%s' % e)

    handler = (lark.EventDispatcherHandler.builder('', '')
               .register_p2_im_message_receive_v1(on_message)
               .register_p2_im_message_reaction_created_v1(lambda data: None)
               .register_p2_im_message_reaction_deleted_v1(lambda data: None)
               .build())
    return handler


# ---------------------------------------------------------------- outbox
# 1 秒一轮。先认领到 processing/ 再发，防多轮重叠重复发。

def archive(claimed_path, name, job):
    atomic_write_json(os.path.join(SENT, name), job, 0o644)
    try:
        os.remove(claimed_path)
    except OSError:
        pass


def process_outbox(transport):
    try:
        names = sorted(n for n in os.listdir(OUTBOX) if n.endswith('.json'))
    except FileNotFoundError:
        return
    for name in names:
        outbox_file = os.path.join(OUTBOX, name)
        claimed = os.path.join(PROCESSING, name)
        try:
            os.rename(outbox_file, claimed)
        except OSError:
            continue
        job = read_json(claimed)
        if job is None:
            try:
                os.remove(claimed)
            except OSError:
                pass
            continue
        reply_to = str(job.get('reply_to') or name[:-5])
        attempts = (job.get('_attempts') or 0) + 1
        kind = job.get('kind') or 'text'
        chat_id = job.get('chat_id')
        try:
            if kind in ('text', 'markdown'):
                # 飞书机器人回复没有独立 markdown 类型，markdown 按 text 发
                text = job.get('text')
                if not isinstance(text, str) or not text:
                    archive(claimed, name, dict(job, status='failed', error='empty text'))
                    continue
                # 去重：sent/ 里已有相同 reply_to + 相同正文，不重发
                already = read_json(os.path.join(SENT, name))
                if (isinstance(already, dict) and already.get('status') == 'sent'
                        and already.get('text') == text):
                    archive(claimed, name, dict(job, status='duplicate',
                                                error='already sent same text, skipped'))
                    log_line('outbox %s 已发过相同正文，跳过不重发' % reply_to)
                    continue
                for chunk in chunk_text(text):
                    if reply_to and os.path.exists(os.path.join(INBOX, reply_to + '.json')):
                        transport.reply_text(reply_to, chunk)
                    elif chat_id:
                        transport.send_text(chat_id, chunk)
                    else:
                        raise RuntimeError('no reply_to inbox record and no chat_id')
                archive(claimed, name, dict(job, status='sent',
                                            sent_at=time.strftime('%Y-%m-%dT%H:%M:%S%z')))
                log_line('已发回飞书：reply_to=%s（%d 字）' % (reply_to, len(text)))
            elif kind in ('image', 'file'):
                media_path = job.get('media_path')
                if not media_path or not chat_id:
                    archive(claimed, name, dict(job, status='failed',
                                                error='image/file needs media_path and chat_id'))
                    continue
                transport.send_media(chat_id, media_path, kind)
                archive(claimed, name, dict(job, status='sent',
                                            sent_at=time.strftime('%Y-%m-%dT%H:%M:%S%z')))
                log_line('已发回飞书媒体：reply_to=%s（%s）' % (reply_to, kind))
            else:
                archive(claimed, name, dict(job, status='unsupported',
                                            error='kind=%s send untested' % kind))
                log_line('outbox %s 的 kind=%s 未实测，不发，已归档' % (reply_to, kind))
        except Exception as e:
            log_line('发回失败（第 %d 次）reply_to=%s：%s' % (attempts, reply_to, e))
            if attempts >= OUTBOX_MAX_ATTEMPTS:
                archive(claimed, name, dict(job, status='failed', error=str(e)[:200]))
            else:
                job['_attempts'] = attempts
                atomic_write_json(claimed, job, 0o644)
                try:
                    os.rename(claimed, outbox_file)
                except OSError:
                    pass


def outbox_loop(transport):
    while not STOP.is_set():
        try:
            process_outbox(transport)
        except Exception as e:
            log_line('outbox 处理出错：%s' % e)
        STOP.wait(1.0)


def heartbeat_loop():
    pid = os.getpid()
    while not STOP.is_set():
        try:
            atomic_write_json(HEARTBEAT_PATH, {
                'pid': pid,
                'started_at': STARTED_AT,
                'status': STATUS['status'],
                'last_poll_at': STATUS['last_poll_at'],
                'last_msg_at': STATUS['last_msg_at'],
                'updated_at': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
            }, 0o644)
        except OSError:
            pass
        STOP.wait(5.0)


def main():
    setup_logging()
    for d in (STATE, INBOX, OUTBOX, PROCESSING, SENT, LOGS, MEDIA):
        os.makedirs(d, exist_ok=True)

    try:
        for n in os.listdir(PROCESSING):
            if n.endswith('.json'):
                try:
                    os.rename(os.path.join(PROCESSING, n), os.path.join(OUTBOX, n))
                except OSError:
                    pass
    except OSError:
        pass

    # 单实例保护：只 kill(pid,0) 判活不够，pid 可能被无关进程复用
    # （钉钉桥 2026-11-20 实测教训），须再核对 /proc/<pid>/cmdline。
    old = read_json(PID_PATH)
    if old and old.get('pid') and old['pid'] != os.getpid():
        alive = False
        try:
            os.kill(old['pid'], 0)
            alive = True
        except OSError:
            pass
        if alive:
            try:
                with open('/proc/%s/cmdline' % old['pid'], 'rb') as f:
                    cmdline = f.read().replace(b'\0', b' ').decode('utf-8', 'replace')
                if 'feishu' not in cmdline or 'daemon.py' not in cmdline:
                    log_line('pid %s 已被其他进程复用，按过期处理' % old['pid'])
                    alive = False
            except OSError:
                pass
        if alive:
            log_line('已有 daemon 在跑（pid %s），退出' % old['pid'])
            return 0
    atomic_write_json(PID_PATH, {'pid': os.getpid(), 'started_at': STARTED_AT}, 0o644)

    creds = load_credentials()
    if creds is None:
        log_line('no credentials: state/credentials.json 缺 appId/appSecret 或 domain 不对，退出')
        return 2
    app_id, app_secret, domain, owner_open_id, bot_open_id = creds
    global USER_OPEN_ID, BOT_OPEN_ID
    USER_OPEN_ID, BOT_OPEN_ID = owner_open_id, bot_open_id
    log_line('daemon 启动 pid=%s（v1.2，凭证已读到，不显示内容，domain=%s，群聊@=%s）' % (
        os.getpid(), domain, '开' if (USER_OPEN_ID and BOT_OPEN_ID) else '关（未配 ownerOpenId/botOpenId）'))

    try:
        import lark_oapi as lark
    except ImportError:
        log_line('lark-oapi 未安装：先跑 .venv/bin/pip install -r requirements.txt，退出')
        return 3

    logging.getLogger('lark_oapi').setLevel(logging.WARNING)

    # 本 VM 出网必须走环境代理（https_proxy）。lark-oapi 的 ws client 默认
    # 把 websockets 的 proxy 参数设为 None 强制直连，直连会 SSL
    # WRONG_VERSION_NUMBER 失败（2026-11-21 实测）。运行时补丁改回
    # proxy=True（按环境变量走代理），不改 venv 里的 SDK 文件。
    try:
        import lark_oapi.ws.client as _ws_mod
        _orig_ws_kwargs = _ws_mod._ws_connect_kwargs

        def _ws_kwargs_with_proxy(connection_headers=None):
            kwargs = _orig_ws_kwargs(connection_headers)
            kwargs['proxy'] = True
            return kwargs

        _ws_mod._ws_connect_kwargs = _ws_kwargs_with_proxy
        log_line('ws 代理补丁已应用（proxy=True，走环境代理）')
    except Exception as e:
        log_line('ws 代理补丁失败，长连接可能连不上：%s' % e)

    transport = FeishuTransport(lark, app_id, app_secret, domain)
    handler = build_event_handler(transport)
    ws_client = lark.ws.Client(app_id, app_secret,
                               event_handler=handler,
                               domain=domain,
                               log_level=lark.LogLevel.WARNING)
    STATUS['status'] = 'polling'

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=outbox_loop, args=(transport,), daemon=True).start()

    def _stop(signum, frame):
        log_line('收到信号 %s，退出' % signum)
        STOP.set()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    try:
        ws_client.start()
    except KeyboardInterrupt:
        pass
    STOP.set()
    log_line('daemon 退出')
    return 0


if __name__ == '__main__':
    sys.exit(main())
