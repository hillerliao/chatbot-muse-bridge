// chatbot-muse-bridge/wechat daemon — 轻量 iLink 网关 v1
// 作用：微信 iLink <-> 本机 Muse。只做搬运，不回答问题。
//   收到微信消息 -> 写 inbox/<message_id>.json
//   Muse worker 回复 -> 写 outbox/<message_id>.json -> daemon 发回微信
// 协议来源：腾讯 iLink（ilinkai.weixin.qq.com），参考开源 SDK
//   epiral/weixin-bot v1.2.0（MIT，src/api.ts + client.ts + auth.ts）
//   及 docs/protocol-spec.md。daemon 本身是零依赖纯 Node 实现，
//   不引整套 Agent 框架。
// 凭证：state/credentials.json 权限 0600，只存 bot_token；不要发给任何人，
//   不要贴进聊天记录。同一 token 只能一个网关用，别和其他网关共用。
// 文本先行：收到的图片/视频/文件/语音从 CDN 下载解密，存到 media/incoming/；
// 发图/视频/文件走上传路径，语音发送不支持。
'use strict';

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');

const BASE_URL = 'https://ilinkai.weixin.qq.com';
const CDN_BASE = 'https://novac2c.cdn.weixin.qq.com/c2c';
const MAX_MEDIA_BYTES = 20 * 1024 * 1024; // 单文件上限 20MB
const MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024; // 收到媒体的下载上限，跟 wx-bot 一致；手机发的视频常超 20MB
const CHANNEL_VERSION = '1.0.0';
const ROOT = __dirname;
const STATE = path.join(ROOT, 'state');
const INBOX = path.join(ROOT, 'inbox');
const OUTBOX = path.join(ROOT, 'outbox');
const PROCESSING = path.join(ROOT, 'processing'); // 先认领再发送：防多轮重叠重复发
const SENT = path.join(ROOT, 'sent');
const INCOMING_DIR = path.join(ROOT, 'media', 'incoming'); // 收到的媒体解密后存这里
const LOGS = path.join(ROOT, 'logs');
const CRED_PATH = path.join(STATE, 'credentials.json');
const CURSOR_PATH = path.join(STATE, 'cursor.json');
const TOKENS_PATH = path.join(STATE, 'context-tokens.json');
const QR_PATH = path.join(STATE, 'qr.json');
const HEARTBEAT_PATH = path.join(STATE, 'heartbeat.json');
const PID_PATH = path.join(STATE, 'daemon.pid');
const LOGIN_PATH = path.join(STATE, 'login.json');

const startedAt = new Date().toISOString();
let logStream = null;
let lastPollAt = null;
let lastMsgAt = null;
let status = 'starting';

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  // logStream 和 stderr 经常指向同一个文件（nohup 重定向），只写一处，防重复行
  if (logStream) logStream.write(line + '\n');
  else process.stderr.write(line + '\n');
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function atomicWriteJson(file, obj, mode) {
  const tmp = `${file}.tmp-${process.pid}`;
  await fsp.writeFile(tmp, `${JSON.stringify(obj, null, 2)}\n`, { mode: mode ?? 0o600 });
  await fsp.rename(tmp, file);
}

async function readJson(file) {
  try {
    return JSON.parse(await fsp.readFile(file, 'utf8'));
  } catch (e) {
    if (e.code === 'ENOENT') return null;
    throw e;
  }
}

// ---------- iLink HTTP 层（对照 weixin-bot src/api.ts） ----------

function randomUin() {
  const v = crypto.randomBytes(4).readUInt32BE(0);
  return Buffer.from(String(v), 'utf8').toString('base64');
}

function headers(token) {
  return {
    'Content-Type': 'application/json',
    AuthorizationType: 'ilink_bot_token',
    Authorization: `Bearer ${token}`,
    'X-WECHAT-UIN': randomUin(),
  };
}

class ApiError extends Error {
  constructor(message, code) { super(message); this.name = 'ApiError'; this.code = code; }
}

async function parseJson(resp, label) {
  const text = await resp.text();
  const payload = text ? JSON.parse(text) : {};
  if (!resp.ok) throw new ApiError(payload.errmsg || `${label} HTTP ${resp.status}`, payload.errcode);
  if (typeof payload.ret === 'number' && payload.ret !== 0) {
    throw new ApiError(payload.errmsg || `${label} ret=${payload.ret}`, payload.errcode ?? payload.ret);
  }
  return payload;
}

async function apiPost(token, endpoint, body, timeoutMs = 40000, signal) {
  const timeout = AbortSignal.timeout(timeoutMs);
  const sig = signal ? AbortSignal.any([signal, timeout]) : timeout;
  const resp = await fetch(`${BASE_URL}${endpoint}`, {
    method: 'POST', headers: headers(token), body: JSON.stringify(body), signal: sig,
  });
  return parseJson(resp, endpoint);
}

async function apiGet(pathSuffix, extraHeaders = {}) {
  const resp = await fetch(`${BASE_URL}${pathSuffix}`, { method: 'GET', headers: extraHeaders });
  return parseJson(resp, pathSuffix);
}

function buildTextMessage(userId, contextToken, text) {
  return {
    from_user_id: '',
    to_user_id: userId,
    client_id: crypto.randomUUID(),
    message_type: 2, // BOT
    message_state: 2, // FINISH
    context_token: contextToken,
    item_list: [{ type: 1, text_item: { text } }],
  };
}

function chunkText(text, limit = 2000) {
  const chars = Array.from(text);
  const out = [];
  for (let i = 0; i < chars.length; i += limit) out.push(chars.slice(i, i + limit).join(''));
  return out.length ? out : [''];
}

// ---------- 凭证 ----------

async function loadCredentials() {
  const c = await readJson(CRED_PATH);
  if (c && typeof c.token === 'string' && typeof c.accountId === 'string') return c;
  return null;
}

async function saveCredentials(c) {
  await fsp.mkdir(STATE, { recursive: true, mode: 0o700 });
  await atomicWriteJson(CRED_PATH, c, 0o600);
  await fsp.chmod(CRED_PATH, 0o600);
}

async function clearCredentials() {
  await fsp.rm(CRED_PATH, { force: true });
  await fsp.rm(QR_PATH, { force: true });
}

// ---------- 登录（扫码） ----------

async function login() {
  for (;;) {
    const qr = await apiGet('/ilink/bot/get_bot_qrcode?bot_type=3');
    // qrcode_img_content 是微信打开完成授权的链接，写进 qr.json 供外面取用
    await atomicWriteJson(QR_PATH, {
      qrcode: qr.qrcode,
      qrcode_img_content: qr.qrcode_img_content,
      fetched_at: new Date().toISOString(),
    }, 0o644);
    log(`登录二维码已生成，请在微信里打开：${qr.qrcode_img_content}`);
    status = 'waiting-qr';

    for (;;) {
      const st = await apiGet(
        `/ilink/bot/get_qrcode_status?qrcode=${encodeURIComponent(qr.qrcode)}`,
        { 'iLink-App-ClientVersion': '1' },
      );
      if (st.status === 'scaned') log('已扫码，等手机上点确认…');
      if (st.status === 'confirmed') {
        if (!st.bot_token || !st.ilink_bot_id || !st.ilink_user_id) {
          throw new Error('扫码已确认，但接口没返回 bot 凭证');
        }
        const creds = {
          token: st.bot_token,
          baseUrl: st.baseurl || BASE_URL,
          accountId: st.ilink_bot_id,
          userId: st.ilink_user_id,
        };
        await saveCredentials(creds);
        await fsp.rm(QR_PATH, { force: true });
        await atomicWriteJson(LOGIN_PATH, {
          accountId: creds.accountId, userId: creds.userId,
          logged_in_at: new Date().toISOString(),
        }, 0o644);
        log(`登录成功，bot 身份 ${creds.accountId}`);
        status = 'logged-in';
        return creds;
      }
      if (st.status === 'expired') {
        log('二维码过期，重新取一个…');
        break;
      }
      await sleep(2000);
    }
  }
}

// ---------- 消息收发 ----------

const contextTokens = new Map(); // userId -> context_token（内存；同时落盘）

async function persistContextTokens() {
  await atomicWriteJson(TOKENS_PATH, Object.fromEntries(contextTokens), 0o600);
}

function extractText(items) {
  const parts = [];
  for (const item of items || []) {
    if (item.type === 1) parts.push(item.text_item?.text ?? '');
    else if (item.type === 2) parts.push('[图片]');
    else if (item.type === 3) parts.push(item.voice_item?.text || '[语音]');
    else if (item.type === 4) parts.push(`[文件 ${item.file_item?.file_name ?? ''}]`);
    else if (item.type === 5) parts.push('[视频]');
  }
  return parts.filter(Boolean).join('\n');
}

function detectType(items) {
  const t = items?.[0]?.type;
  return t === 2 ? 'image' : t === 3 ? 'voice' : t === 4 ? 'file' : t === 5 ? 'video' : 'text';
}

// ---------- 媒体接收（逻辑照 wx-bot core client.ts downloadMedia） ----------
// 收到的图片/视频/文件/语音，item 里带 media{encrypt_query_param, aes_key}
// 或 image_item.aeskey（32 位 hex）；从 CDN /download 拉回密文，AES-128-ECB 解密后存盘。
function parseAesKey(s) {
  if (typeof s !== 'string' || !s) throw new Error('没有 aes_key');
  if (s.length === 16) return Buffer.from(s, 'latin1');
  if (s.length === 32 && /^[0-9a-fA-F]{32}$/.test(s)) return Buffer.from(s, 'hex');
  const decoded = Buffer.from(s, 'base64');
  if (decoded.length === 32) {
    const asText = decoded.toString('latin1');
    if (/^[0-9a-fA-F]{32}$/.test(asText)) return Buffer.from(asText, 'hex');
  }
  if (decoded.length === 16) return decoded;
  throw new Error(`aes_key 格式不对（长度 ${s.length}）`);
}

function decryptAesEcb(buf, key) {
  const decipher = crypto.createDecipheriv('aes-128-ecb', key, null); // 默认 PKCS7 去填充
  return Buffer.concat([decipher.update(buf), decipher.final()]);
}

async function downloadMedia(media, aesKeyHex) {
  const key = aesKeyHex ? Buffer.from(aesKeyHex, 'hex') : parseAesKey(media.aes_key || '');
  let url;
  if (media.full_url) url = media.full_url;
  else if (media.encrypt_query_param) url = `${CDN_BASE}/download?encrypted_query_param=${encodeURIComponent(media.encrypt_query_param)}`;
  else throw new Error('media 里既没有 full_url 也没有 encrypt_query_param');
  let lastErr = new Error('cdn download failed');
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(90000) });
      if (resp.status !== 200) { lastErr = new Error(`cdn download: status ${resp.status}`); }
      else {
        const len = Number(resp.headers.get('content-length') || '0');
        if (len > MAX_DOWNLOAD_BYTES) throw new Error('媒体太大（>100MB）');
        const enc = Buffer.from(await resp.arrayBuffer());
        if (enc.length > MAX_DOWNLOAD_BYTES) throw new Error('媒体太大（>100MB）');
        return decryptAesEcb(enc, key); // 解密失败不重试，直接抛
      }
    } catch (e) { lastErr = e; }
    if (attempt < 3) await sleep(attempt === 1 ? 1000 : 3000);
  }
  throw lastErr;
}

function guessExt(kind, buf, fileName) {
  if (fileName && path.extname(fileName)) return path.extname(fileName);
  if (kind === 'video') return '.mp4';
  if (kind === 'voice') {
    if (buf.subarray(0, 4).toString('latin1') === 'SILK') return '.silk';
    if (buf.subarray(0, 5).toString('latin1') === '#!AMR') return '.amr';
    return '.amr';
  }
  if (kind === 'image') {
    if (buf[0] === 0xff && buf[1] === 0xd8) return '.jpg';
    if (buf[0] === 0x89 && buf[1] === 0x50) return '.png';
    if (buf.subarray(0, 3).toString('latin1') === 'GIF') return '.gif';
    if (buf.subarray(0, 4).toString('latin1') === 'RIFF' && buf.subarray(8, 12).toString('latin1') === 'WEBP') return '.webp';
    return '.jpg';
  }
  return '.bin';
}

// 从 item_list 里取出带 media 的项（图片/语音/文件/视频）
function extractMediaItems(items) {
  const out = [];
  for (const item of items || []) {
    if (item.type === 2 && item.image_item?.media) out.push({ kind: 'image', media: item.image_item.media, aesKeyHex: item.image_item.aeskey });
    else if (item.type === 3 && item.voice_item?.media) out.push({ kind: 'voice', media: item.voice_item.media, text: item.voice_item.text || '' });
    else if (item.type === 4 && item.file_item?.media) out.push({ kind: 'file', media: item.file_item.media, fileName: item.file_item.file_name || '' });
    else if (item.type === 5 && item.video_item?.media) out.push({ kind: 'video', media: item.video_item.media });
  }
  return out;
}

// 下载收到的媒体并存盘；失败也不阻塞消息入库，保留 media 信息供之后重试
async function saveIncomingMedia(msgId, items) {
  const saved = [];
  const mediaItems = extractMediaItems(items);
  for (let i = 0; i < mediaItems.length; i++) {
    const it = mediaItems[i];
    try {
      const raw = await downloadMedia(it.media, it.aesKeyHex);
      await fsp.mkdir(INCOMING_DIR, { recursive: true });
      const ext = guessExt(it.kind, raw, it.fileName);
      const name = mediaItems.length > 1 ? `${msgId}_${i}${ext}` : `${msgId}${ext}`;
      const file = path.join(INCOMING_DIR, name);
      await fsp.writeFile(file, raw, { mode: 0o644 });
      saved.push({ kind: it.kind, file, size: raw.length, ...(it.fileName ? { file_name: it.fileName } : {}) });
      log(`收到媒体已存本地：${file}（${raw.length} 字节）`);
    } catch (e) {
      log(`媒体下载失败（${msgId} ${it.kind}）：${e.message}，已保留 media 信息供重试`);
      saved.push({ kind: it.kind, error: e.message, media: it.media, ...(it.aesKeyHex ? { aeskey: it.aesKeyHex } : {}) });
    }
  }
  return saved;
}

async function sendTyping(token, userId, contextToken, on) {
  try {
    const cfg = await apiPost(token, '/ilink/bot/getconfig', {
      ilink_user_id: userId, context_token: contextToken,
      base_info: { channel_version: CHANNEL_VERSION },
    }, 15000);
    if (!cfg.typing_ticket) return;
    await apiPost(token, '/ilink/bot/sendtyping', {
      ilink_user_id: userId, typing_ticket: cfg.typing_ticket, status: on ? 1 : 2,
      base_info: { channel_version: CHANNEL_VERSION },
    }, 15000);
  } catch { /* typing 失败不影响收发 */ }
}

async function sendReply(token, userId, contextToken, text) {
  for (const chunk of chunkText(text)) {
    await apiPost(token, '/ilink/bot/sendmessage', {
      msg: buildTextMessage(userId, contextToken, chunk),
      base_info: { channel_version: CHANNEL_VERSION },
    }, 15000);
  }
  await sendTyping(token, userId, contextToken, false);
}

// ---------- 媒体发送（协议文档 §8：getuploadurl → AES-128-ECB → CDN upload → sendmessage） ----------
// 图片/视频/文件走同一条上传路径，media_type 1=图片 2=视频 3=文件；
// 语音发送（media_type 4）：公开实现没有稳定的独立发送方法，不支持发送；收语音走转写，不走这里。
const MEDIA_TYPES = { image: 1, video: 2, file: 3 };

function md5Hex(buf) { return crypto.createHash('md5').update(buf).digest('hex'); }

function encryptAesEcb(buf, key) {
  const cipher = crypto.createCipheriv('aes-128-ecb', key, null);
  return Buffer.concat([cipher.update(buf), cipher.final()]);
}

async function uploadMedia(token, toUserId, filePath, mediaType) {
  const fp = path.isAbsolute(filePath) ? filePath : path.join(ROOT, filePath);
  const WORKSPACE_ROOT = path.join(process.env.HOME || '', 'workspace');
  if (!fp.startsWith(WORKSPACE_ROOT + path.sep)) throw new Error('媒体文件必须在 workspace 内');
  const raw = await fsp.readFile(fp);
  if (raw.length > MAX_MEDIA_BYTES) throw new Error(`文件太大（${raw.length} 字节），上限 20MB`);
  const aeskey = crypto.randomBytes(16);
  const aeskeyHex = aeskey.toString('hex');
  const filekey = crypto.randomBytes(16).toString('hex');
  const filesize = Math.ceil((raw.length + 1) / 16) * 16; // AES-128-ECB + PKCS7 填充后大小
  const up = await apiPost(token, '/ilink/bot/getuploadurl', {
    filekey, media_type: mediaType, to_user_id: toUserId,
    rawsize: raw.length, rawfilemd5: md5Hex(raw), filesize,
    no_need_thumb: true, aeskey: aeskeyHex,
    base_info: { channel_version: CHANNEL_VERSION },
  }, 30000);
  if (!up.upload_param) throw new Error('getuploadurl 没返回 upload_param');
  const enc = encryptAesEcb(raw, aeskey);
  const uploadUrl = `${CDN_BASE}/upload?encrypted_query_param=${encodeURIComponent(up.upload_param)}&filekey=${filekey}`;
  const resp = await fetch(uploadUrl, {
    method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: enc,
    signal: AbortSignal.timeout(60000),
  });
  if (!resp.ok) throw new Error(`CDN 上传失败 HTTP ${resp.status}`);
  const encParam = resp.headers.get('x-encrypted-param');
  if (!encParam) throw new Error('CDN 上传没返回 x-encrypted-param');
  return {
    media: {
      encrypt_query_param: encParam,
      aes_key: Buffer.from(aeskeyHex, 'utf8').toString('base64'), // 官方写法：base64(hex 字符串)
      encrypt_type: 1,
    },
    filesize, rawsize: raw.length, rawfilemd5: md5Hex(raw),
  };
}

function buildMediaItem(kind, up, job) {
  if (kind === 'image') return { type: 2, image_item: { media: up.media, mid_size: up.filesize } };
  if (kind === 'video') return { type: 5, video_item: { media: up.media, video_size: up.filesize, video_md5: up.rawfilemd5 } };
  return { type: 4, file_item: { media: up.media, file_name: job.file_name || path.basename(job.file), md5: up.rawfilemd5, len: String(up.rawsize) } };
}

async function sendMedia(token, userId, contextToken, job) {
  const up = await uploadMedia(token, userId, job.file, MEDIA_TYPES[job.kind]);
  const item = buildMediaItem(job.kind, up, job);
  const msg = {
    from_user_id: '', to_user_id: userId, client_id: crypto.randomUUID(),
    message_type: 2, message_state: 2, context_token: contextToken, item_list: [item],
  };
  await apiPost(token, '/ilink/bot/sendmessage', { msg, base_info: { channel_version: CHANNEL_VERSION } }, 30000);
  await sendTyping(token, userId, contextToken, false);
}

// ---------- outbox 轮询 ----------

const outboxAttempts = new Map();
let outboxRunning = false; // 1 秒一轮的定时器不重叠：上一轮没跑完就跳过

async function processOutbox(creds) {
  let files;
  try { files = await fsp.readdir(OUTBOX); } catch { return; }
  for (const name of files.filter((n) => n.endsWith('.json')).sort()) {
    const file = path.join(OUTBOX, name);
    let job;
    try { job = JSON.parse(await fsp.readFile(file, 'utf8')); } catch { continue; }
    // 先认领：挪到 processing/，别的轮次就看不到这个文件了。
    // 认领失败说明已被另一轮取走，直接跳过。
    const claimed = path.join(PROCESSING, name);
    try { await fsp.rename(file, claimed); } catch { continue; }
    const replyTo = String(job.reply_to || name.replace(/\.json$/, ''));
    const toUser = job.to_user_id;
    const text = job.text;
    const kind = job.kind || 'text';
    const isMedia = kind === 'image' || kind === 'video' || kind === 'file';
    if (kind === 'voice' || (kind !== 'text' && !isMedia)) {
      // 语音发送没有稳定的公开实现，kind 写错的也直接归档，不重试
      await atomicWriteJson(path.join(SENT, name), { ...job, status: 'unsupported', failed_at: new Date().toISOString() }, 0o644);
      await fsp.rm(claimed, { force: true });
      log(`outbox ${replyTo} 的 kind=${kind} 不支持，已归档`);
      continue;
    }
    if (!toUser || (isMedia ? (typeof job.file !== 'string' || !job.file) : (typeof text !== 'string' || text.length === 0))) {
      await fsp.rename(claimed, path.join(SENT, name)).catch(() => {});
      continue;
    }
    // context_token 优先用 outbox 文件自带的，其次内存，再次 inbox 原消息，最后落盘缓存
    let ctx = job.context_token || contextTokens.get(toUser) || null;
    if (!ctx) {
      const inboxMsg = await readJson(path.join(INBOX, `${replyTo}.json`));
      ctx = inboxMsg?.context_token || null;
    }
    if (!ctx) {
      const saved = await readJson(TOKENS_PATH);
      ctx = saved?.[toUser] || null;
      if (ctx) contextTokens.set(toUser, ctx);
    }
    if (!ctx) {
      log(`outbox ${replyTo} 没有 context_token，发不出去，先放回 outbox`);
      await fsp.rename(claimed, file).catch(() => {});
      continue;
    }
    try {
      if (isMedia) {
        // 协议习惯：带说明文字的媒体拆成两条，先文字后媒体
        if (typeof text === 'string' && text.length) await sendReply(creds.token, toUser, ctx, text);
        await sendMedia(creds.token, toUser, ctx, job);
      } else {
        await sendReply(creds.token, toUser, ctx, text);
      }
      await atomicWriteJson(path.join(SENT, name), { ...job, status: 'sent', sent_at: new Date().toISOString() }, 0o644);
      await fsp.rm(claimed, { force: true });
      log(`已发回微信（${kind}）：reply_to=${replyTo} 给 ${toUser}${isMedia ? ` 文件=${job.file}` : `（${text.length} 字）`}`);
    } catch (e) {
      const n = (outboxAttempts.get(name) || 0) + 1;
      outboxAttempts.set(name, n);
      log(`发回失败（第 ${n} 次）reply_to=${replyTo}：${e.message}`);
      if (n >= 3) {
        await atomicWriteJson(path.join(SENT, name), { ...job, status: 'failed', error: e.message, failed_at: new Date().toISOString() }, 0o644);
        await fsp.rm(claimed, { force: true });
      } else {
        // 放回 outbox 等下一轮重试（不留在 processing，避免卡住）
        await fsp.rename(claimed, file).catch(() => {});
      }
    }
  }
}

// ---------- 主循环 ----------

async function main() {
  await fsp.mkdir(STATE, { recursive: true, mode: 0o700 });
  await fsp.mkdir(INBOX, { recursive: true });
  await fsp.mkdir(OUTBOX, { recursive: true });
  await fsp.mkdir(PROCESSING, { recursive: true });
  await fsp.mkdir(SENT, { recursive: true });
  await fsp.mkdir(LOGS, { recursive: true });

  // 上次崩溃时卡在 processing/ 的认领件放回 outbox，免得丢回复
  try {
    for (const n of await fsp.readdir(PROCESSING)) {
      if (n.endsWith('.json')) await fsp.rename(path.join(PROCESSING, n), path.join(OUTBOX, n)).catch(() => {});
    }
  } catch { /* 空目录不管 */ }

  // 单实例保护
  const oldPid = await readJson(PID_PATH).catch(() => null);
  if (oldPid?.pid && oldPid.pid !== process.pid) {
    try { process.kill(oldPid.pid, 0); log(`已有 daemon 在跑（pid ${oldPid.pid}），退出`); process.exit(0); }
    catch { /* 旧 pid 已死，继续 */ }
  }
  await atomicWriteJson(PID_PATH, { pid: process.pid, started_at: startedAt }, 0o644);

  logStream = fs.createWriteStream(path.join(LOGS, 'daemon.log'), { flags: 'a' });
  log(`daemon 启动 pid=${process.pid}（v1.0，零依赖）`);

  // 心跳：每 5 秒写一次，供 keepalive / hook 判断死活
  setInterval(async () => {
    await atomicWriteJson(HEARTBEAT_PATH, {
      pid: process.pid, started_at: startedAt, status,
      last_poll_at: lastPollAt, last_msg_at: lastMsgAt,
      updated_at: new Date().toISOString(),
    }, 0o644).catch(() => {});
  }, 5000);

  // 恢复落盘的 context_token（daemon 重启后还能主动发）
  const savedTokens = await readJson(TOKENS_PATH);
  if (savedTokens) for (const [k, v] of Object.entries(savedTokens)) contextTokens.set(k, v);

  // outbox 轮询：上一轮没跑完就跳过，不重叠
  let creds = await loadCredentials();
  setInterval(() => {
    if (!creds || outboxRunning) return;
    outboxRunning = true;
    processOutbox(creds)
      .catch((e) => log(`outbox 处理出错：${e.message}`))
      .finally(() => { outboxRunning = false; });
  }, 1000);

  let retryDelayMs = 1000;
  for (;;) {
    if (!creds) {
      status = 'waiting-qr';
      try { creds = await login(); } catch (e) { log(`登录出错：${e.message}，5 秒后重试`); await sleep(5000); continue; }
    }
    status = 'polling';
    try {
      const cursorDoc = await readJson(CURSOR_PATH);
      const cursor = cursorDoc?.get_updates_buf || '';
      const controller = new AbortController();
      const updates = await apiPost(creds.token, '/ilink/bot/getupdates', {
        get_updates_buf: cursor,
        base_info: { channel_version: CHANNEL_VERSION },
      }, 40000, controller.signal);
      lastPollAt = new Date().toISOString();
      if (updates.get_updates_buf) {
        await atomicWriteJson(CURSOR_PATH, { get_updates_buf: updates.get_updates_buf }, 0o644);
      }
      retryDelayMs = 1000;

      for (const raw of updates.msgs || []) {
        if (raw.message_type !== 1) continue; // 只处理用户发来的
        const from = raw.from_user_id;
        if (from && raw.context_token) {
          contextTokens.set(from, raw.context_token);
          await persistContextTokens().catch(() => {});
        }
        const msgId = String(raw.message_id);
        const inboxFile = path.join(INBOX, `${msgId}.json`);
        if (fs.existsSync(inboxFile)) continue; // 去重
        const msg = {
          message_id: msgId,
          from_user_id: from,
          to_user_id: raw.to_user_id,
          type: detectType(raw.item_list),
          text: extractText(raw.item_list),
          context_token: raw.context_token,
          create_time_ms: raw.create_time_ms,
          received_at: new Date().toISOString(),
        };
        // 先下载媒体再入库：worker 读到 inbox 时文件已在 media/incoming/
        const mediaFiles = await saveIncomingMedia(msgId, raw.item_list);
        if (mediaFiles.length) msg.media_files = mediaFiles;
        await atomicWriteJson(inboxFile, msg, 0o644);
        lastMsgAt = new Date().toISOString();
        log(`收到微信消息 ${msgId}（${msg.type}，${msg.text.length} 字）来自 ${from}`);
        // 对方正在输入中（尽力而为）
        sendTyping(creds.token, from, raw.context_token, true).catch(() => {});
      }
    } catch (e) {
      if (e.code === -14) {
        log('会话过期（-14），清掉凭证，重新扫码登录');
        creds = null;
        await clearCredentials();
        await fsp.rm(CURSOR_PATH, { force: true });
        contextTokens.clear();
        retryDelayMs = 1000;
        continue;
      }
      log(`轮询出错：${e.message}，${retryDelayMs}ms 后重试`);
      await sleep(retryDelayMs);
      retryDelayMs = Math.min(retryDelayMs * 2, 10000);
    }
  }
}

process.on('SIGTERM', () => { log('收到 SIGTERM，退出'); process.exit(0); });
process.on('SIGINT', () => { log('收到 SIGINT，退出'); process.exit(0); });

main().catch((e) => { log(`daemon 崩溃：${e.stack || e.message}`); process.exit(1); });
