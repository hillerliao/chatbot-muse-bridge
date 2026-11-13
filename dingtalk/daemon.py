#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DingTalk <-> Muse daemon v1 —— 只做搬运，不回答问题。

    钉钉私聊/群聊 -> 钉钉 Stream -> daemon -> inbox/<message_id>.json
    Muse worker 回复 -> 写 outbox/<message_id>.json -> daemon 发回钉钉

传输（已核实）：
    钉钉官方 Stream 模式，出站 WebSocket，不用公网回调地址。
    用官方 SDK dingtalk-stream 0.24.3：
      client = DingTalkStreamClient(Credential(clientId, clientSecret))
      client.register_callback_handler(ChatbotMessage.TOPIC, handler)
      ChatbotMessage.TOPIC = '/v1.0/im/bot/messages/get'
    回复走 SDK ChatbotHandler.reply_text：POST 到该条消息自带的
    sessionWebhook，不用另外取 access token。

凭证：只读 state/credentials.json {"clientId": "...", "clientSecret": "..."}，
    文件权限 0600。凭证值绝不打进日志，绝不写到别处，代码里没有任何写死的凭证。
    没有凭证时 daemon 直接退出（exit 2），不假装连接。

文字先行。图片/视频/文件用 downloadCode 换临时下载链接，下载到
media/incoming/ 供 worker 直接看；语音按钉钉服务端给的 recognition
转写文字回。超过 20MB 的媒体不下载，只记原因。
日志只记消息长度和消息 ID，不记正文，不记 sessionWebhook。
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

# 回复分片长度：只是个保守猜测，不是钉钉核实过的上限，联调后按实测调。
CHUNK_LIMIT = 4000
OUTBOX_MAX_ATTEMPTS = 3

STARTED_AT = time.strftime('%Y-%m-%dT%H:%M:%S%z')
STOP = threading.Event()
STATUS = {'status': 'starting', 'last_poll_at': None, 'last_msg_at': None}

log = logging.getLogger('dingtalk-muse-daemon')


def log_line(msg):
    """只记状态和长度，不记正文、不记凭证、不记 sessionWebhook。"""
    log.info(msg)


def setup_logging():
    os.makedirs(LOGS, exist_ok=True)
    handler = logging.FileHandler(os.path.join(LOGS, 'daemon.log'), encoding='utf-8')
    handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
    log.addHandler(handler)
    log.addHandler(logging.StreamHandler(sys.stderr))
    log.setLevel(logging.INFO)


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
    """inbox/outbox 文件名只允许安全字符，防路径穿越。"""
    raw = str(raw or '')
    if raw and re.fullmatch(r'[A-Za-z0-9_.\-]+', raw):
        return raw
    return uuid.uuid4().hex


def load_credentials():
    """只读 state/credentials.json。缺失或格式不对返回 None，不碰网络。"""
    creds = read_json(CRED_PATH)
    if not isinstance(creds, dict):
        return None
    client_id = creds.get('clientId')
    client_secret = creds.get('clientSecret')
    if not isinstance(client_id, str) or not client_id.strip():
        return None
    if not isinstance(client_secret, str) or not client_secret.strip():
        return None
    return client_id.strip(), client_secret.strip()


def chunk_text(text, limit=CHUNK_LIMIT):
    chars = list(text)
    out = [''.join(chars[i:i + limit]) for i in range(0, len(chars), limit)]
    return out if out else ['']


# ---------------------------------------------------------------- SDK 传入
# 传输层隔离在这里：daemon 其余部分只认 inbox/outbox 文件，不认 SDK。
# SDK 不可用时直接退出，不假装连接。

def get_raw_content(msg):
    """SDK 只解析 text/picture/richText 的 content；audio/video/file 的
    content 原样落在 msg.extensions['content'] 里（dict 或 JSON 字符串）。"""
    content = getattr(msg, 'extensions', {}).get('content')
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except ValueError:
            return {}
    return content if isinstance(content, dict) else {}


def sniff_ext(data, kind):
    if data[:3] == b'\xff\xd8\xff':
        return 'jpg'
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        return 'png'
    if data[:6] in (b'GIF87a', b'GIF89a'):
        return 'gif'
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'webp'
    if data[:5] == b'#!AMR':
        return 'amr'
    if data[:4] == b'SILK':
        return 'silk'
    if data[4:8] == b'ftyp':
        return 'mp4'
    if data[:4] == b'%PDF':
        return 'pdf'
    if data[:2] == b'PK':
        return 'zip'
    return {'image': 'jpg', 'video': 'mp4', 'voice': 'amr'}.get(kind, 'bin')


def download_media(handler, download_code, msg_id, kind, content):
    """downloadCode -> 临时下载链接 -> 存到 media/incoming/。返回
    (相对路径, 错误原因)，失败返回 (None, 原因)。不把 downloadCode、
    链接、凭证写进日志。"""
    if not download_code:
        return None, '没有 downloadCode'
    try:
        url = handler.get_image_download_url(download_code)
    except Exception as e:
        return None, '换下载链接失败：%s' % e
    if not url:
        return None, '换下载链接失败'
    try:
        import requests
        resp = requests.get(url, timeout=60)
    except Exception as e:
        return None, '下载失败：%s' % e
    if resp.status_code != 200:
        return None, '下载 HTTP %s' % resp.status_code
    data = resp.content
    if len(data) > MAX_MEDIA_BYTES:
        return None, '文件太大（%d 字节，上限 %d）' % (len(data), MAX_MEDIA_BYTES)
    if not data:
        return None, '下载到空文件'
    ext = sniff_ext(data, kind)
    if kind == 'file':
        name = content.get('fileName') if isinstance(content, dict) else None
        if name and '.' in os.path.basename(str(name)):
            ext = os.path.basename(str(name)).rsplit('.', 1)[-1][:10] or ext
    rel = 'media/incoming/%s.%s' % (msg_id, ext)
    path = os.path.join(ROOT, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(data)
    return rel, None


def build_handler(dingtalk_stream):
    """返回一个 ChatbotHandler 子类实例：收到的消息只写 inbox，不回答。"""

    class MuseChatbotHandler(dingtalk_stream.ChatbotHandler):
        async def process(self, callback):
            msg = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
            # 群聊（conversation_type=2）只在被 @ 时才收得到；没 @ 的群消息
            # 钉钉本就不该推过来，万一来了也跳过，不写 inbox、不唤醒 worker。
            # is_in_at_list 为 None（字段缺失）时按收到处理，不丢消息。
            if str(getattr(msg, 'conversation_type', '')) == '2' and getattr(msg, 'is_in_at_list', None) is False:
                log_line('群聊未 @ 的消息，跳过 %s' % getattr(msg, 'message_id', ''))
                return dingtalk_stream.AckMessage.STATUS_OK, 'OK'
            record = build_inbox_record(
                msg,
                lambda code, mid, kind, content: download_media(self, code, mid, kind, content))
            msg_id = record['message_id']
            inbox_file = os.path.join(INBOX, msg_id + '.json')
            if os.path.exists(inbox_file):
                return dingtalk_stream.AckMessage.STATUS_OK, 'OK'  # 去重
            atomic_write_json(inbox_file, record, 0o644)
            STATUS['last_msg_at'] = time.strftime('%Y-%m-%dT%H:%M:%S%z')
            STATUS['last_poll_at'] = STATUS['last_msg_at']
            log_line('收到钉钉消息 %s（%s，%d 字）来自 %s' % (
                msg_id, record['type'],
                len(record.get('text') or ''),
                record.get('sender_nick') or 'unknown'))
            return dingtalk_stream.AckMessage.STATUS_OK, 'OK'

    return MuseChatbotHandler()


def build_inbox_record(msg, download_fn=None):
    """ChatbotMessage -> inbox 文件内容。只存回复必需的字段。
    图片/视频/文件下载到 media/incoming/，语音按 recognition 转写文字。"""
    msg_id = safe_msg_id(getattr(msg, 'message_id', None))
    msgtype = getattr(msg, 'message_type', None) or 'unknown'
    record_extra = {}
    content = get_raw_content(msg)
    if msgtype == 'text' and getattr(msg, 'text', None):
        kind, text = 'text', msg.text.content or ''
    elif msgtype == 'richText':
        kind = 'richText'
        try:
            text = '\n'.join(msg.get_text_list() or [])
        except Exception:
            text = ''
    elif msgtype == 'picture':
        kind = 'image'
        codes = []
        try:
            codes = msg.get_image_list() or []
        except Exception:
            codes = []
        rel, err = (None, '未下载')
        if download_fn and codes:
            rel, err = download_fn(codes[0], msg_id, 'image', content)
        text = ('[图片，已下载：%s]' % rel) if rel else ('[图片，下载失败：%s]' % err)
        record_extra = {'download_codes': codes}
        if rel:
            record_extra['media_path'] = rel
    elif msgtype == 'audio':
        kind = 'voice'
        recognition = (content.get('recognition') or '').strip()
        duration = content.get('duration')
        code = content.get('downloadCode')
        rel = None
        if download_fn and code:
            rel, _err = download_fn(code, msg_id, 'voice', content)
        secs = ('约 %d 秒' % round(duration / 1000)) if isinstance(duration, (int, float)) and duration else ''
        text = recognition if recognition else '[语音，未识别出文字，请发文字]'
        if secs:
            text += '（%s）' % secs
        record_extra = {'duration_ms': duration}
        if rel:
            record_extra['media_path'] = rel
    elif msgtype == 'video':
        kind = 'video'
        code = content.get('downloadCode')
        duration = content.get('duration')
        rel, err = (None, '未下载')
        if download_fn and code:
            rel, err = download_fn(code, msg_id, 'video', content)
        secs = ('约 %d 秒' % round(duration / 1000)) if isinstance(duration, (int, float)) and duration else ''
        text = ('[视频，已下载：%s%s]' % (rel, ('，' + secs) if secs else '')) if rel else ('[视频，下载失败：%s]' % err)
        record_extra = {'duration_ms': duration}
        if rel:
            record_extra['media_path'] = rel
    elif msgtype == 'file':
        kind = 'file'
        code = content.get('downloadCode')
        name = content.get('fileName') or content.get('file_name') or '未命名文件'
        size = content.get('fileSize') or content.get('file_size')
        rel, err = (None, '未下载')
        if download_fn and code:
            rel, err = download_fn(code, msg_id, 'file', content)
        size_txt = ('，%d 字节' % size) if isinstance(size, (int, float)) else ''
        text = ('[文件：%s%s，已下载：%s]' % (name, size_txt, rel)) if rel else ('[文件：%s%s，下载失败：%s]' % (name, size_txt, err))
        record_extra = {'file_name': name, 'file_size': size}
        if rel:
            record_extra['media_path'] = rel
    else:
        kind, text = 'other', '[不支持的消息类型：%s]' % msgtype
        record_extra = {}
    record = {
        'message_id': msg_id,
        'from_staff_id': getattr(msg, 'sender_staff_id', None),
        'from_user_id': getattr(msg, 'sender_id', None),
        'sender_nick': getattr(msg, 'sender_nick', None),
        'conversation_id': getattr(msg, 'conversation_id', None),
        'conversation_type': getattr(msg, 'conversation_type', None),
        'conversation_title': getattr(msg, 'conversation_title', None),
        'is_in_at_list': getattr(msg, 'is_in_at_list', None),
        'type': kind,
        'msgtype': msgtype,
        'text': text,
        # 回复必需：发回钉钉走这条消息自带的 sessionWebhook，有过期时间。
        # 只放 inbox/outbox 文件里，不打进日志，不对外发。
        'session_webhook': getattr(msg, 'session_webhook', None),
        'session_webhook_expired_time': getattr(msg, 'session_webhook_expired_time', None),
        'received_at': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
    }
    record.update(record_extra)
    return record


def reconstruct_message(job):
    """outbox 任务 -> 仅够 reply_text 用的 ChatbotMessage。

    reply_text 只用 session_webhook 和 sender_staff_id（见 SDK 源码
    chatbot.py 的 reply_text），其余字段按原消息回填。
    """
    import dingtalk_stream
    return dingtalk_stream.ChatbotMessage.from_dict({
        'msgId': job.get('reply_to') or '',
        'sessionWebhook': job.get('session_webhook'),
        'senderStaffId': job.get('from_staff_id'),
        'senderId': job.get('from_user_id'),
        'conversationId': job.get('conversation_id'),
        'conversationType': job.get('conversation_type'),
    })


# ---------------------------------------------------------------- outbox
# 1 秒一轮。先认领到 processing/ 再发，防多轮重叠重复发。

def process_outbox(handler):
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
            continue  # 已被另一轮认领
        job = read_json(claimed)
        reply_to = str((job or {}).get('reply_to') or name[:-5])
        attempts = ((job or {}).get('_attempts') or 0) + 1
        if job is None:
            os.remove(claimed)
            continue
        kind = job.get('kind') or 'text'
        text = job.get('text')
        webhook = job.get('session_webhook')
        if kind != 'text':
            # 图片/视频/文件发送未实测：不发，直接归档，写明原因。
            archive(claimed, name, dict(job, status='unsupported',
                                        error='kind=%s send untested' % kind))
            log_line('outbox %s 的 kind=%s 未实测，不发，已归档' % (reply_to, kind))
            continue
        if not isinstance(text, str) or not text:
            archive(claimed, name, dict(job, status='failed', error='empty text'))
            continue
        if not webhook:
            archive(claimed, name, dict(job, status='failed',
                                        error='no session webhook'))
            log_line('outbox %s 没有 session webhook，发不出去，已归档' % reply_to)
            continue
        expiry_ms = job.get('session_webhook_expired_time')
        if isinstance(expiry_ms, (int, float)) and time.time() * 1000 > expiry_ms:
            archive(claimed, name, dict(job, status='failed',
                                        error='session webhook expired'))
            log_line('outbox %s 的 session webhook 已过期，发不出去，已归档' % reply_to)
            continue
        try:
            incoming = reconstruct_message(job)
            for chunk in chunk_text(text):
                handler.reply_text(chunk, incoming)
            archive(claimed, name, dict(job, status='sent',
                                        sent_at=time.strftime('%Y-%m-%dT%H:%M:%S%z')))
            log_line('已发回钉钉：reply_to=%s（%d 字）' % (reply_to, len(text)))
        except Exception as e:
            log_line('发回失败（第 %d 次）reply_to=%s：%s' % (attempts, reply_to, e))
            if attempts >= OUTBOX_MAX_ATTEMPTS:
                archive(claimed, name, dict(job, status='failed',
                                            error=str(e)[:200]))
            else:
                job['_attempts'] = attempts
                atomic_write_json(claimed, job, 0o644)
                try:
                    os.rename(claimed, outbox_file)
                except OSError:
                    pass


def archive(claimed_path, name, job):
    atomic_write_json(os.path.join(SENT, name), job, 0o644)
    try:
        os.remove(claimed_path)
    except OSError:
        pass


def outbox_loop(handler):
    while not STOP.is_set():
        try:
            process_outbox(handler)
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
    for d in (STATE, INBOX, OUTBOX, PROCESSING, SENT, LOGS):
        os.makedirs(d, exist_ok=True)

    # 上次崩溃时卡在 processing/ 的认领件放回 outbox，免得丢回复。
    try:
        for n in os.listdir(PROCESSING):
            if n.endswith('.json'):
                try:
                    os.rename(os.path.join(PROCESSING, n), os.path.join(OUTBOX, n))
                except OSError:
                    pass
    except OSError:
        pass

    # 单实例保护。
    old = read_json(PID_PATH)
    if old and old.get('pid') and old['pid'] != os.getpid():
        try:
            os.kill(old['pid'], 0)
            log_line('已有 daemon 在跑（pid %s），退出' % old['pid'])
            return 0
        except OSError:
            pass
    atomic_write_json(PID_PATH, {'pid': os.getpid(), 'started_at': STARTED_AT}, 0o644)

    # 凭证：没有就干净退出，不碰网络，不假装连接。
    creds = load_credentials()
    if creds is None:
        log_line('no credentials: state/credentials.json 缺 clientId 或 clientSecret，退出')
        return 2
    client_id, client_secret = creds
    log_line('daemon 启动 pid=%s（v1.0，凭证已读到，不显示内容）' % os.getpid())

    try:
        import dingtalk_stream
    except ImportError:
        log_line('dingtalk-stream 未安装：先跑 .venv/bin/pip install -r requirements.txt，退出')
        return 3

    # SDK 自身日志只留 WARNING 以上，不让它把消息内容打进我们的日志。
    logging.getLogger('dingtalk_stream').setLevel(logging.WARNING)

    credential = dingtalk_stream.Credential(client_id, client_secret)
    client = dingtalk_stream.DingTalkStreamClient(credential)
    handler = build_handler(dingtalk_stream)
    client.register_callback_handler(dingtalk_stream.chatbot.ChatbotMessage.TOPIC, handler)
    STATUS['status'] = 'polling'

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=outbox_loop, args=(handler,), daemon=True).start()

    def _stop(signum, frame):
        log_line('收到信号 %s，退出' % signum)
        STOP.set()
        raise SystemExit(0)  # 主线程正在 start_forever 里，抛出去才能停下来

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    # start_forever 自带断线重连（SDK 内部 10 秒重试）；跑在当前线程，
    # 收到信号时 STOP 已置位，进程随主线程退出而结束。
    try:
        client.start_forever()
    except KeyboardInterrupt:
        pass
    STOP.set()
    log_line('daemon 退出')
    return 0


if __name__ == '__main__':
    sys.exit(main())
