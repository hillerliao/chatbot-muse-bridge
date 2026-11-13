// login-only.js — 在一台能直连微信服务器的机器上只做一次 iLink 登录，不常驻、不轮询消息。
// 用法：node login-only.js [工作目录，默认当前目录]
//   扫码链接写进 <workdir>/state/qr.json；确认后凭证写进
//   <workdir>/state/credentials.json（0600），打印 LOGIN-CONFIRMED 后退出。
//   二维码过期自动重取。跑完后把 credentials.json 拿回本机再启动 daemon。
'use strict';
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');

const BASE_URL = 'https://ilinkai.weixin.qq.com';
const ROOT = process.argv[2] ? path.resolve(process.argv[2]) : process.cwd();
const STATE = path.join(ROOT, 'state');

function log(m) { process.stdout.write(`[${new Date().toISOString()}] ${m}\n`); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
async function atomicWriteJson(file, obj, mode) {
  const tmp = `${file}.tmp-${process.pid}`;
  await fsp.writeFile(tmp, `${JSON.stringify(obj, null, 2)}\n`, { mode: mode ?? 0o600 });
  await fsp.rename(tmp, file);
}
async function apiGet(pathSuffix, extraHeaders = {}) {
  const resp = await fetch(`${BASE_URL}${pathSuffix}`, { method: 'GET', headers: extraHeaders });
  const text = await resp.text();
  const payload = text ? JSON.parse(text) : {};
  if (!resp.ok) throw new Error(`${pathSuffix} HTTP ${resp.status}: ${payload.errmsg || text}`);
  if (typeof payload.ret === 'number' && payload.ret !== 0) {
    throw new Error(`${pathSuffix} ret=${payload.ret}: ${payload.errmsg || ''}`);
  }
  return payload;
}

async function main() {
  await fsp.mkdir(STATE, { recursive: true, mode: 0o700 });
  for (;;) {
    const qr = await apiGet('/ilink/bot/get_bot_qrcode?bot_type=3');
    await atomicWriteJson(path.join(STATE, 'qr.json'), {
      qrcode: qr.qrcode,
      qrcode_img_content: qr.qrcode_img_content,
      fetched_at: new Date().toISOString(),
    }, 0o644);
    log(`登录二维码已生成，请在微信里打开：${qr.qrcode_img_content}`);
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
        await atomicWriteJson(path.join(STATE, 'credentials.json'), creds, 0o600);
        await fsp.chmod(path.join(STATE, 'credentials.json'), 0o600);
        await fsp.rm(path.join(STATE, 'qr.json'), { force: true });
        await atomicWriteJson(path.join(STATE, 'login.json'), {
          accountId: creds.accountId, userId: creds.userId,
          logged_in_at: new Date().toISOString(),
        }, 0o644);
        log(`LOGIN-CONFIRMED ${creds.accountId}`);
        return;
      }
      if (st.status === 'expired') { log('二维码过期，重新取一个…'); break; }
      await sleep(2000);
    }
  }
}

main().catch((e) => { log(`登录失败：${e.stack || e.message}`); process.exit(1); });
