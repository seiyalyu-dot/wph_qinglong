#!/usr/bin/env node
'use strict';
/**
 * 唯品会自动任务 —— 青龙(qinglong) 单文件入口
 *
 * 部署：把整个 qinglong/ 目录放到青龙脚本区，定时任务命令填：
 *     node qinglong/wph.js
 *
 * 账号配置（均用环境变量，无需落地文件）：
 *   WPH_COOKIE   多账号用换行分隔，每行一个完整 cookie 串（推荐、最简单）
 *   WPH_ACCOUNTS JSON 数组，格式同 wph_accounts.json：[{"cookie":"...","name":"账号1"}]
 *
 * 常用环境变量开关：
 *   WPH_PARALLEL        多账号并行数，默认 2（设 1 可串行，风控更稳）
 *   WPH_FAST            安全加速倍数(仅压缩装饰性等待)
 *   WPH_REDPACKET       all=抽完 / skip=不抽（非交互默认不抽）
 *   WPH_DO_EXCHANGE     1=执行津贴抢兑
 *   WPH_HAR_TASKS       收藏/加购 HAR 文件绝对路径（需抓包 task.har）
 *   WPH_AUTO_REFRESH    1=会话过期自动续期（需 playwright）
 *   QL_URL / QL_CLIENT_ID / QL_CLIENT_SECRET  青龙 OpenAPI 通知推送
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// ================== 内联 wph_all_api 全部逻辑 ==================
const api = (function(){
/**
 * 唯品会「所有任务」纯 API 总入口（自算签名，非浏览器、非 HAR 重放）。
 * 依次完成三大子系统，全部实测 code=1 通过：
 *   1) commonTask  (兔兔爱合成 / 天天剪羊毛 的任务组, actId=H3gRnE1Xi18, relateId=3QA9JBmLAfKaKOAU4QLCfw)
 *   2) checkRoomTask (胡萝卜/在线任务组, actId=czg91SQsPfs, relateId=5Zev7kIeYbKaKOAU4QLCfw)
 *   3) feedSheep  (天天剪羊毛玩法: 赚草料→喂草料→剪羊毛→任务领奖)
 *   4) signIn     (签到有礼: info 查状态 → exec 执行签到)
 * 运行: node wph_all_api.js   (输出重定向可加 > wph_all_api.log 2>&1)
 * 会话来源: 优先 wph_accounts.json / wph_cookie.json(H5 登录所得, 见 wph_login.js)。
 * HAR 扫描默认禁用(会拖慢启动并刷屏);需用时设置 WPH_ENABLE_HAR=1。
 * 提速开关：多账号默认并行(WPH_PARALLEL，默认2)；WPH_FAST=2/3 仅压缩"空闲/装饰性"等待(模拟分心/模块间隔/关卡停顿)，
 *   不缩放浏览20s与限流退避，安全可日常用。注意 WPH_SPEED 是内部调试字段，会破坏浏览任务，勿日常用。
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const crypto = require('crypto');
const CryptoJS = require('crypto-js');
const { AsyncLocalStorage } = require('async_hooks');
const HAR_DIR = process.env.WPH_HAR_DIR || 'E:\\xwechat_files\\waiting827759_b0d5\\msg\\file';
const AUTO_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1';
const SPEED = (parseFloat(process.env.WPH_SPEED) || 1); // 调试加速(内部字段)：设小值(如0.05)可秒级跑完，但会连同浏览20s等功能性等待一起缩放→可能破坏任务，日常勿用
const sleep = (ms) => new Promise((r) => setTimeout(r, Math.max(1, Math.round(ms * SPEED))));
const FAST = Math.max(1, parseFloat(process.env.WPH_FAST) || 1); // 安全加速：仅缩放"空闲/装饰性"等待(模拟分心/模块间隔/关卡停顿)，默认1=不加速
const nap = (min, max) => sleep(rand(Math.max(1, Math.round(min / FAST)), Math.max(1, Math.round(max / FAST)))); // 装饰性等待(受 WPH_FAST 缩放)
const napMs = (ms) => sleep(Math.max(1, Math.round(ms / FAST))); // 单值装饰性等待(受 WPH_FAST 缩放)
const rand = (a, b) => Math.floor(Math.random() * (b - a + 1)) + a;
// 运行日志缓冲区：供青龙(qinglong)等环境在任务结束后推送通知使用
const RUN_LOG = [];
const MAX_LOG = 500;
// 账号上下文：并发跑多账号时，用 AsyncLocalStorage 给每条日志加账号名前缀。
const acctCtx = new AsyncLocalStorage(); // store: { name }
// 串行化写出：所有日志经同一条 promise 链、按调用顺序写出，避免并行多账号写到
// 非 TTY 管道(stdout)时缓冲块乱序/滞后刷出（表现为“跑完了还冒出一条日志”）。
// 链用 write 回调确认落盘，天然处理背压；main() 末尾 await drainLog() 保证进程退出前全部刷完。
let _writeChain = Promise.resolve();
function emitLine(s) {
  _writeChain = _writeChain.then(() => new Promise((resolve) => {
    process.stdout.write(s + '\n', () => resolve());
  })).catch(() => {});
  return _writeChain;
}
function drainLog() { return _writeChain; }
const log = (...a) => {
  const s = a.map((x) => (typeof x === 'string' ? x : JSON.stringify(x))).join(' ');
  const ctx = acctCtx.getStore();
  const tagged = ctx ? `[${ctx.name}] ${s}` : s; // 全局日志不加账号前缀
  RUN_LOG.push(tagged);
  if (RUN_LOG.length > MAX_LOG) RUN_LOG.shift();
  emitLine(tagged); // 实时写出（由串行链保证顺序与落盘）
};
// 所有对外 HTTP 请求统一超时，避免某个接口无响应时 Promise 永久挂起（表现为“卡死”）。
const FETCH_TIMEOUT = Number(process.env.WPH_FETCH_TIMEOUT) || 30000;
async function pfetch(url, opts = {}) {
  return fetch(url, Object.assign({}, opts, { signal: AbortSignal.timeout(FETCH_TIMEOUT) }));
}

const API_KEY = '8cec5243ade04ed3a02c5972bcda0d3f';
const AES_SECRET = 'qyrohlf5sjazleru';
// 内联 sign_wap.js 中的加密配置（原来需要 fs.readFileSync 读取整个 webpack 打包文件）
const EN_STRINGS = { '70f71280d5d547b2a7bb370a529aeea1': 'U2FsdGVkX197SM3Eh62XyjAwTXznW9DdALdNR1gKNsewAg3fzwA0x/+UQldlbi3oYBn8eFHgTtBUcGneYPCjIA==', '8cec5243ade04ed3a02c5972bcda0d3f': 'U2FsdGVkX1+ZmG8rT/n9qDbrWBnK0K3G0gsoPo0N6/6qx8AklnZmXLyulj0KAy07ixFAu6oMKmOY0+VH3DjQ2Q==', 'adf779847ac641dd9590ccc5674e25d2': 'U2FsdGVkX1/VI+95aRUsSZCDB3rmMe2DPSUO+rSH7U/tlNnA5u9anTM3oHI+XgIeHWA5XDAo0Z19ddwzFeHFXA==' };
function enString(k) { return EN_STRINGS[k] || ''; }
function getSecret(k) { const e = enString(k); if (!e) return ''; try { return CryptoJS.AES.decrypt(e, AES_SECRET).toString(CryptoJS.enc.Utf8); } catch (_) { return e; } }
function sha1hex(s) { return crypto.createHash('sha1').update(s, 'utf8').digest('hex'); }
function replaceHost(u) { if (u) { if (u.indexOf('?') != -1) u = u.split('?')[0]; u = u.replace(/^https?:\/\/[^\/]*/, '').replace(/^\/\//, ''); } return u; }

// 通用 hashParam（不含 url query，供 checkRoom / feedSheep 用）
function hashParam(p) { const ks = Object.keys(p).sort(); let r = ''; for (const k of ks) { if (k === 'api_key') continue; r += '&' + k + '=' + (p[k] !== undefined && p[k] !== null ? p[k] : ''); } if (r.length > 0) r = r.substring(1); return sha1hex(r); }
// commonTask 专用 hashParam（把 url query 也并入签名，wph_do 原逻辑）
function hashParamDo(param, url) {
  param = Object.assign({}, param);
  if (url && url.indexOf('?') != -1) { let q = url.split('?')[1]; if (q.indexOf('#') != -1) q = q.split('#')[0]; q.split('&').forEach((pair) => { const i = pair.indexOf('='); const k = i > 0 ? pair.slice(0, i) : pair; const v = i > 0 ? pair.slice(i + 1) : ''; param[k] = v; }); }
  const keys = Object.keys(param).sort(); let rs = '';
  for (const k of keys) { if (k === 'api_key') continue; rs += '&' + k + '=' + (param[k] !== undefined && param[k] !== null ? param[k] : ''); }
  if (rs.length > 0) rs = rs.substring(1);
  return sha1hex(rs);
}
function signBase(u, p, cid, sid) { return 'OAuth api_sign=' + sha1hex(replaceHost(u) + hashParam(p) + cid + sid + getSecret(API_KEY)); }
function signDo(u, p, cid, sid, apiKey) { return 'OAuth api_sign=' + sha1hex(replaceHost(u) + hashParamDo(p, u) + cid + sid + getSecret(apiKey)); }

// 扫描 HAR 目录，返回【去重后】携带 session_id 且通过活动接口校验的 cookie 字符串（多账号场景：HAR 里可能有多个账号）
async function findValidCookies() {
  const bySig = new Map(); // 账号身份签名 → 代表性 cookie（同一会话的多个请求只留一个，避免逐个校验刷屏）
  (function walk(d) {
    let es; try { es = fs.readdirSync(d, { withFileTypes: true }); } catch (_) { return; }
    for (const e of es) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) { walk(p); continue; }
      if (!e.name.toLowerCase().endsWith('.har')) continue;
      try {
        const har = JSON.parse(fs.readFileSync(p, 'utf8'));
        for (const en of (har.log && har.log.entries) || []) {
          const ck = (en.request.headers || []).find((h) => h.name.toLowerCase() === 'cookie');
          if (!ck || !/session_id=/.test(ck.value)) continue;
          const sid = (ck.value.match(/session_id=([^;]+)/) || [])[1] || '';
          const cid = (ck.value.match(/mars_cid=([^;]+)/) || [])[1] || '';
          const sig = sid || cid; // 按登录会话去重，同一账号的 cookie 字符串略有差异也只校验一次
          if (sig && !bySig.has(sig)) bySig.set(sig, ck.value);
        }
      } catch (_) {}
    }
  })(HAR_DIR);
  const valid = [];
  for (const ck of bySig.values()) {
    const cid = (ck.match(/mars_cid=([^;]+)/) || [])[1] || '';
    const sid = (ck.match(/mars_sid=([^;]+)/) || [])[1] || '';
    try {
      const url = 'https://act-ug.vip.com/commonTask/getTaskList?api_key=' + API_KEY + '&time=0&is_front=1&fdc_area_id=911101114112';
      const base = baseParams(cid, 'H3gRnE1Xi18', '3QA9JBmLAfKaKOAU4QLCfw');
      const param = Object.assign({}, base);
      const auth = signDo(url, param, cid, sid, API_KEY);
      const body = Object.keys(param).map((k) => k + '=' + encodeURIComponent(param[k])).join('&');
      const r = pfetch(url, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Origin': 'https://mst.vip.com', 'Referer': 'https://mst.vip.com/', 'User-Agent': AUTO_UA, 'Authorization': auth, 'Cookie': ck, 'Accept': '*/*' }, body });
      const j = await r.then((x) => x.json()).catch(() => null);
      if (j && j.code === 1) valid.push(ck);
    } catch (_) {}
  }
  if (valid.length) log(`✅ HAR 扫描到 ${valid.length} 个有效账号 cookie`);
  return valid;
}
async function findValidCookie() { const a = await findValidCookies(); return a[0] || null; }

// 优先使用 H5 登录导出的 wph_cookie.json；失败再回退 HAR。
const SESSION_FILE = process.env.WPH_COOKIE_FILE || path.join(__dirname, 'wph_cookie.json');
async function validateCookie(cookie, cid, sid) {
  try {
    const url = 'https://act-ug.vip.com/commonTask/getTaskList?api_key=' + API_KEY + '&time=0&is_front=1&fdc_area_id=911101114112';
    const base = baseParams(cid, 'H3gRnE1Xi18', '3QA9JBmLAfKaKOAU4QLCfw');
    const auth = signDo(url, base, cid, sid, API_KEY);
    const body = Object.keys(base).map((k) => k + '=' + encodeURIComponent(base[k])).join('&');
    const j = await pfetch(url, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Origin': 'https://mst.vip.com', 'Referer': 'https://mst.vip.com/', 'User-Agent': AUTO_UA, 'Authorization': auth, 'Cookie': cookie, 'Accept': '*/*' }, body }).then((x) => x.json()).catch(() => null);
    return !!(j && j.code === 1);
  } catch (_) { return false; }
}
// 自动续期会话：mars_sid 由前端 JS 生成（非服务端 Set-Cookie，已实测 m.vip.com 首页/活动接口均不回 Set-Cookie），
// 故必须用无头浏览器重新访问 m.vip.com，让前端 JS 重新签发 mars_sid。
// 前提：保存的 cookie 中「底层登录态」(如 WAP[login]) 仍有效 —— 若整登录态已过期，重新签发的 mars_sid 也绑不上用户，续期无效，需 wph_login.js 重登。
// 默认关闭，设 WPH_AUTO_REFRESH=1 启用（青龙环境若无 playwright 则自动跳过，不影响普通运行）。
const REFRESH_UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1';
async function refreshSession(cookieStr) {
  let playwright;
  try { playwright = require('playwright'); } catch (_) { log('  ⚠️ 续期需要 playwright（npm i playwright），已跳过'); return null; }
  let browser;
  try {
    browser = await playwright.chromium.launch({ headless: true });
    const ctx = await browser.newContext({ userAgent: REFRESH_UA, viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
    // 把已保存的 cookie 注入上下文（统一挂到 .vip.com 域），保留其中尚有效的底层登录态
    const pairs = String(cookieStr).split(';').map((s) => s.trim()).filter(Boolean).map((kv) => {
      const i = kv.indexOf('='); const name = kv.slice(0, i); const value = kv.slice(i + 1);
      return { name, value, domain: '.vip.com', path: '/' };
    });
    try { await ctx.addCookies(pairs); } catch (e) { log('  ⚠️ 注入 cookie 失败:', e.message); }
    const page = await ctx.newPage();
    await page.goto('https://m.vip.com/', { waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});
    // 轮询等待前端重新签发 mars_sid（与旧值不同即视为已刷新）
    const oldSid = (cookieStr.match(/mars_sid=([^;]+)/) || [])[1] || '';
    let newSid = '';
    for (let i = 0; i < 20; i++) {
      const cs = await ctx.cookies();
      const m = cs.find((c) => c.name === 'mars_sid');
      if (m && m.value && m.value !== oldSid) { newSid = m.value; break; }
      await new Promise((r) => setTimeout(r, 1000));
    }
    const cs = await ctx.cookies();
    if (!newSid) { const m = cs.find((c) => c.name === 'mars_sid'); if (m) newSid = m.value; }
    const newCookie = cs.map((c) => `${c.name}=${c.value}`).join('; ');
    await browser.close();
    const cid = (newCookie.match(/mars_cid=([^;]+)/) || [])[1] || '';
    const sid = (newCookie.match(/mars_sid=([^;]+)/) || [])[1] || '';
    const ok = await validateCookie(newCookie, cid, sid);
    if (ok) { log('  🔄 会话已自动续期（新 mars_sid=' + (newSid ? newSid.slice(0, 8) + '…' : '?') + '）'); return newCookie; }
    log('  ⚠️ 续期后活动 API 仍校验失败（底层登录态可能已过期，需重新 npm run login）');
    return null;
  } catch (e) {
    log('  ⚠️ 续期异常:', e.message);
    try { if (browser) await browser.close(); } catch (_) {}
    return null;
  }
}
// 续期成功后将新 cookie 回写到 wph_accounts.json / wph_cookie.json 中匹配的账号
async function persistRefreshed(acc) {
  const writeBack = (file) => {
    try {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      const arr = Array.isArray(data) ? data : [data];
      const hit = arr.find((x) => (acc.mars_cid && x.mars_cid === acc.mars_cid) || x.cookie === acc._oldCookie);
      if (hit) {
        hit.cookie = acc.cookie;
        if (acc.mars_cid) hit.mars_cid = acc.mars_cid;
        if (acc.mars_sid) hit.mars_sid = acc.mars_sid;
        fs.writeFileSync(file, JSON.stringify(Array.isArray(data) ? arr : arr[0], null, 2));
        log('  💾 已回写 ' + file);
      }
    } catch (_) {}
  };
  writeBack(process.env.WPH_ACCOUNTS_FILE || path.join(__dirname, 'wph_accounts.json'));
  writeBack(SESSION_FILE);
}
// 把账号候选归一化为 {cookie,cid,sid,name} 并去重（按 cookie 字符串）
function normalizeAccounts(rawList) {
  const out = []; const seen = new Set();
  for (const a of rawList || []) {
    if (!a || !a.cookie) continue;
    if (seen.has(a.cookie)) continue; seen.add(a.cookie);
    const cid = a.mars_cid || (a.cookie.match(/mars_cid=([^;]+)/) || [])[1] || '';
    const sid = a.mars_sid || (a.cookie.match(/mars_sid=([^;]+)/) || [])[1] || '';
    out.push({ cookie: a.cookie, cid, sid, name: a.name || ('账号' + (out.length + 1)) });
  }
  return out;
}
// 收集账号候选：合并 1) wph_accounts.json（显式多账号） + 2) wph_cookie.json（末次登录） + 3) HAR 扫描，
// 按 (cookie 精确相同 / 非空 mars_cid 相同) 去重，避免任一来源只含一个号时整体只跑一个。
async function gatherAccounts() {
  const merged = [];
  const seenCookie = new Set();
  const seenCid = new Set();
  const add = (acc) => {
    if (!acc || !acc.cookie) return;
    if (seenCookie.has(acc.cookie)) return;
    if (acc.cid && seenCid.has(acc.cid)) return;
    seenCookie.add(acc.cookie);
    if (acc.cid) seenCid.add(acc.cid);
    merged.push(acc);
  };
  // 0) 环境变量 WPH_COOKIE（青龙标准：多账号用换行 \n 分隔，每行一个完整 cookie 串）
  //    优先级最高，便于青龙面板直接维护账号，无需落地 json 文件。
  const envCk = process.env.WPH_COOKIE;
  if (envCk) {
    const lines = String(envCk).split(/[\r\n]+/).map((s) => s.trim()).filter(Boolean);
    for (const ck of lines) add(normalizeAccounts([{ cookie: ck, name: 'WPH账号' + (merged.length + 1) }])[0]);
    if (lines.length) log(`✅ 从环境变量 WPH_COOKIE 读取 ${lines.length} 个账号`);
  }
  // 0-bis) 环境变量 WPH_ACCOUNTS（青龙高级：直接粘贴 wph_accounts.json 的 JSON 数组内容，
  //        支持 name、mars_cid、mars_sid 等字段，适合精细管理多账号）。
  const envAcc = process.env.WPH_ACCOUNTS;
  if (envAcc) {
    try {
      const data = JSON.parse(envAcc);
      const arr = Array.isArray(data) ? data : [data];
      for (const a of arr) add(normalizeAccounts([a])[0]);
      if (arr.length) log(`✅ 从环境变量 WPH_ACCOUNTS 读取 ${arr.length} 个账号`);
    } catch (_) {}
  }
  // 1) 显式多账号文件：支持数组或单对象，每项 {cookie, mars_cid?, mars_sid?, name?}
  const AF = process.env.WPH_ACCOUNTS_FILE || path.join(__dirname, 'wph_accounts.json');
  try {
    const data = JSON.parse(fs.readFileSync(AF, 'utf8'));
    for (const a of (Array.isArray(data) ? data : [data])) add(normalizeAccounts([a])[0]);
  } catch (_) {}
  // 2) wph_cookie.json（末次单账号登录产物）：合并进来，去重后不会与上面重复
  try {
    const data = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf8'));
    for (const a of (Array.isArray(data) ? data : [data])) add(normalizeAccounts([a])[0]);
  } catch (_) {}
  // 3) HAR 扫描（多账号：HAR 里可能有多个账号的 session）
  //    默认禁用：HAR_DIR 通常指向微信文件目录，文件极多会拖慢启动并刷屏。
  //    需要时用 WPH_ENABLE_HAR=1 开启（仍按会话去重，不会刷屏）。
  const enableHar = process.env.WPH_ENABLE_HAR === '1';
  if (enableHar) {
    try {
      const hars = await findValidCookies();
      hars.forEach((c, i) => add(normalizeAccounts([{ cookie: c, name: 'HAR账号' + (i + 1) }])[0]));
    } catch (_) {}
  } else {
    log('ℹ️ HAR 扫描已禁用（默认）。如需启用请设置 WPH_ENABLE_HAR=1');
  }
  if (merged.length) log(`✅ 合并收集到 ${merged.length} 个账号候选 (wph_accounts.json + wph_cookie.json${enableHar ? ' + HAR' : ''}，已去重)`);
  return merged;
}
// 校验并过滤出有效账号（无效/过期会话：若开启自动续期则先尝试无头浏览器刷新）
async function loadSessions() {
  const candidates = await gatherAccounts();
  const valid = [];
  const autoRefresh = process.env.WPH_AUTO_REFRESH === '1';
  for (const acc of candidates) {
    let ok = acc.cid && (await validateCookie(acc.cookie, acc.cid, acc.sid));
    if (!ok && autoRefresh) {
      log(`⚠️ 账号「${acc.name}」会话无效/过期，尝试自动续期...`);
      acc._oldCookie = acc.cookie;
      const nc = await refreshSession(acc.cookie);
      if (nc) {
        acc.cookie = nc;
        acc.sid = (nc.match(/mars_sid=([^;]+)/) || [])[1] || acc.sid;
        acc.mars_cid = (nc.match(/mars_cid=([^;]+)/) || [])[1] || acc.mars_cid;
        ok = true;
        await persistRefreshed(acc);
      }
    }
    if (ok) valid.push(acc);
    else log(`⚠️ 账号「${acc.name}」会话无效/过期，已跳过`);
  }
  return valid;
}
// 兼容旧调用：返回首个有效 cookie 字符串（无则 null）
async function loadSession() {
  const arr = await loadSessions();
  return arr.length ? arr[0].cookie : null;
}

/* ===================== 4) signIn (签到有礼) ===================== */
const SIGN_BASE = 'https://act-ug.vip.com/signIn';
function mrSign(cid) {
  return {
    source_app: 'app', client_type: 'wap', app_name: 'shop_iphone', client: 'iphone',
    api_key: API_KEY, app_version: '9.79.7', mobile_platform: '3',
    mobile_channel: 'ng00010v:al80ssgp:37u8zn0w:ng00010p', mars_cid: cid,
    warehouse: 'VIP_NH', fdc_area_id: '911101114112', province_id: '911101114112',
    wap_consumer: 'C2-4-2', time: '0', is_front: '1', app_theme_mode: '0', app_theme_action: '0',
    tfs_fp_token: TFS, sd_tuijian: '1',
    bussCode: 'app_sign_in', openid: '', youngType: '0', sceneCode: '0'
  };
}
async function processSign(cookie, cid, sid) {
  log(`\n########## signIn (签到有礼) ##########`);
  const base = mrSign(cid);
  // 11001 限流退避（查询类请求遇限流重试，避免整轮被跳过）
  const safeApi = async (p, extra) => {
    for (let a = 0; a <= 3; a++) {
      const r = await callApi(SIGN_BASE, p, extra, cookie, cid, sid, API_KEY, base);
      if (r && r.code === 11001 && a < 3) { log(`  ⚠️ 11001 限流，退避重试(${a+1})`); await sleep(rand(8000, 15000)); continue; }
      return r;
    }
  };
  const info = await safeApi('/info', {});
  if (!info || info.code !== 1) { log('  ❌ signIn/info 失败', info && info.code, info && info.msg); return; }
  const b = info.data && info.data.basicInfo;
  const taskActId = (b && b.taskActId) || 'tW11D6RjjC0';
  if (b) log(`  📋 actId=${b.actId} 今日已签=${b.isSignInForDay} 连续=${b.nonStopDays}天 总=${b.totalDays}天 任务组actId=${taskActId}`);
  else log('  info.data:', JSON.stringify(info.data).slice(0, 300));
  // 1) 每日签到（exec 的 11001 视作会话失效，不重试）
  if (b && b.isSignInForDay === 1) { log('  ✅ 今日已签到，跳过'); }
  else {
    await sleep(rand(2000, 4000));
    const execBase = Object.assign({}, base, { entranceParam: 'grzxsk', actId: (b && b.actId) || 'eck9ma3QfXQ', taskActId: taskActId, youngType: '0' });
    const ex = await callApi(SIGN_BASE, '/exec', {}, cookie, cid, sid, API_KEY, execBase);
    if (ex && ex.code === 1) log('  ✅ 签到成功!', JSON.stringify(ex.data).slice(0, 200));
    else if (ex && ex.code === 30022) log('  ✅ 今日已签到 (30022)');
    else if (ex && ex.code === 11001) log('  ⚠️ 会话失效 (not authorized)，cookie 可能不被活动 API 接受');
    else log('  ⚠️ 签到返回:', ex && ex.code, ex && ex.msg);
  }
  // 2) 做任务领奖励：签到活动自带的任务组（commonTask 体系，actId=taskActId，relateId 用空串）
  await sleep(rand(3000, 6000));
  await processCommon(cookie, cid, sid, API_KEY, taskActId, '', '签到有礼-做任务领奖励');
}

/* ===================== 1) commonTask ===================== */
const TFS = 'BRHPQ/nsqKzcbikjzSvtoQTD5XddGpqlGdM9wrlITxfgfqUmgPDFs76Uu5n/vnNrCY1Yp3cacO1b/lM6kvZx+BA==SKhE6DrwYN0oXHKjl5dFfQbPIyXN0m';
function baseParams(cid, actId, relateId) {
  return { source_app: 'app', client_type: 'wap', app_name: 'shop_iphone', client: 'iphone', api_key: API_KEY, app_version: '9.79.6', mobile_platform: '3', mobile_channel: 'ng00010v:al80ssgp:37u8zn0w:ng00010p', mars_cid: cid, warehouse: 'VIP_NH', fdc_area_id: '911101114112', province_id: '911101114112', wap_consumer: 'C2-4-2', time: '0', is_front: '1', app_theme_mode: '0', app_theme_action: '0', tfs_fp_token: TFS, sd_tuijian: '1', actId, relateId };
}
async function callApi(BASE, p, extra, cookie, cid, sid, apiKey, base) {
  const url = BASE + p + `?api_key=${apiKey}&time=0&is_front=1&fdc_area_id=911101114112`;
  const param = Object.assign({}, base, extra);
  // 防风控：用登录 cookie 里真实的 tfs_fp_token，与 mars_cid/会话同源，
  // 避免硬编码指纹与登录态不一致被风控识别为"伪造设备"
  const _fp = cookie && cookie.match(/tfs_fp_token=([^;]+)/);
  if (_fp && _fp[1]) param.tfs_fp_token = decodeURIComponent(_fp[1]);
  const auth = signDo(url, param, cid, sid, apiKey);
  const body = Object.keys(param).map((k) => k + '=' + encodeURIComponent(param[k])).join('&');
  const r = await pfetch(url, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Origin': 'https://mst.vip.com', 'Referer': 'https://mst.vip.com/', 'User-Agent': AUTO_UA, 'Authorization': auth, 'Cookie': cookie, 'Accept': '*/*', 'Accept-Language': 'zh-CN,zh-Hans;q=0.9' }, body });
  return r.json().catch(() => ({ code: -1, msg: 'json_error' }));
}
// type=18 在线时长任务：finishTask 返回 30022（时长不足）时，若 WPH_ONLINE_WAIT>0，
// 则每约 60s 重试一次 finishTask，直到时长满足或超时。
// 前提：用户必须保持唯品会活动页打开，由真实页面累计 onlineTime（脚本无法自刷，因无心跳接口）。
async function waitForOnline(doFinish, maxWaitSec) {
  if (!maxWaitSec || maxWaitSec <= 0) return null;
  let waited = 0;
  while (waited < maxWaitSec) {
    await napMs(60000); waited += 60;
    const r = await doFinish();
    log(`    finishTask(在线重试):`, r && r.code, r && r.msg || '');
    if (r.code !== 30022) return r;
    log(`    ⏳ 在线时长仍不足，已等 ${waited}s，继续（请保持活动页打开）…`);
  }
  return null;
}
// ===================== HAR 业务动作重放（收藏/加购任务专用）=====================
// 兔兔爱合成/剪羊毛的「收藏商品」「加购商品」任务，真实 App 是通过 mapi.appvipshop.com 的业务
// 接口联动完成的（收藏 = fav/goods/add_by_mid；加购 = cart/add_cart/v3），并非 finishTask。
// 这些接口需 App 签名(api_sign)与 App 会话令牌，纯 H5 cookie 无法自签。故当提供对应账号的 HAR
// （含已完成这些动作的“已签名请求”）时，原样重放该请求来完成任务。仅当 HAR 账号 vipruid 与当前
// 账号一致才重放（避免误操作别的号）。需设置 WPH_HAR_TASKS=路径 启用；默认关闭，行为不变。
let _harBiz = undefined;                 // undefined=未加载; false=无/加载失败; object=已加载
let _harReplayed = { fav: false, addcart: false }; // 签名多为一次性，每跑每类只重放一次
function loadHarBiz() {
  if (_harBiz !== undefined) return _harBiz;
  const p = process.env.WPH_HAR_TASKS;
  _harBiz = false;
  if (!p) return _harBiz;
  try {
    const har = JSON.parse(fs.readFileSync(p, 'utf8'));
    const ents = (har.log && har.log.entries) || [];
    const pick = (re) => ents.find((e) => re.test(e.request.url));
    const hdr = (e, n) => { const h = (e.request.headers || []).find((x) => x.name.toLowerCase() === n.toLowerCase()); return h ? h.value : ''; };
    const mk = (e) => e ? { url: e.request.url, method: e.request.method, auth: hdr(e, 'authorization'), ua: hdr(e, 'user-agent'), cookie: hdr(e, 'cookie'), body: (e.request.postData && e.request.postData.text) || null } : null;
    const fav = pick(/fav\/goods\/add_by_mid/);
    const add = pick(/cart\/add_cart\/v3/);
    const vipruid = (JSON.stringify(har).match(/vipruid=(\d+)/) || [])[1] || '';
    _harBiz = { fav: mk(fav), addcart: mk(add), vipruid };
    log(`ℹ️ 已加载 HAR 业务动作重放配置 (HAR账号vipruid=${vipruid || '?'} / 收藏请求=${fav ? '有' : '无'} / 加购请求=${add ? '有' : '无'})`);
  } catch (e) { log('  ⚠️ 加载 HAR 业务动作失败:', e.message); _harBiz = false; }
  return _harBiz;
}
async function replayHarBiz(req) {
  if (!req) return null;
  const headers = { 'Authorization': req.auth || '', 'User-Agent': req.ua || 'Spec/9.80.2 (iPhone; iOS 26.5.2; Scale/3.00)', 'Accept': '*/*', 'X-VIP-Host': 'mapi.appvipshop.com' };
  if (req.cookie) headers['Cookie'] = req.cookie;
  const opt = { method: req.method, headers };
  if (req.method === 'POST' && req.body) { headers['Content-Type'] = 'application/x-www-form-urlencoded'; opt.body = req.body; }
  try {
    const r = await fetch(req.url, opt);
    const t = await r.text();
    try { return JSON.parse(t); } catch (_) { return { code: -1, msg: (t || '').slice(0, 120) }; }
  } catch (e) { return { code: -1, msg: e.message }; }
}
function accVipruid(cookie) { return (cookie.match(/m_vipruid=(\d+)/) || cookie.match(/vipruid=(\d+)/) || [])[1] || ''; }
function isFavTask(name, type) { return /收藏|collect|favorite/i.test(name || ''); }
function isAddTask(name, type) { return type === 43 || /加购|多买多省|种草|购车|加入购物车/i.test(name || ''); }

async function processCommon(cookie, cid, sid, apiKey, actId, relateId, label) {
  log(`\n########## ${label} (actId=${actId}) ##########`);
  const BASE = 'https://act-ug.vip.com/commonTask';
  const base = baseParams(cid, actId, relateId);
  // 在线时长任务(type=18)：WPH_ONLINE_WAIT>0 时，在“保持活动页打开”前提下进入等待重试，累计够在线时长后当次领取。
  const ONLINE_WAIT = Number(process.env.WPH_ONLINE_WAIT) || 0;
  let j = await callApi(BASE, '/getTaskList', {}, cookie, cid, sid, apiKey, base);
  if (!j || j.code !== 1) { log('  ❌ getTaskList 失败', j && j.code, j && j.msg); return; }
  const tasks = (j.data && j.data.taskList) || [];
  log(`  📋 共 ${tasks.length} 个任务`);
  let done = 0, skip = 0, fail = 0, failStreak = 0;
  // 本地安全调用：遇 11001(瞬时限流)自动退避重试，吸收限流而不漏任务/狂刷
  const safeCall = async (ep, data, retries = 3) => {
    for (let a = 0; a <= retries; a++) {
      const r = await callApi(BASE, ep, data, cookie, cid, sid, apiKey, base);
      if (r && r.code === 11001 && a < retries) { log(`    ⚠️ 11001 限流，退避重试(${a+1})`); await sleep(rand(8000, 15000)); continue; }
      return r;
    }
  };
  for (const t of tasks) {
    const name = t.taskName || ''; const type = t.taskType; let utid = t.userTaskId || '';
    const isOpen = [1, 2, 3, 33, 5].includes(type); // 打开页面/浏览/分享类（如惊喜低价、超值精选等跳转任务）
    log(`\n  — 「${name}」 type=${type} status=${t.taskStatus} utid=${utid || '(空)'} jumpUrl=${(t.url || t.appUrl || '').slice(0, 60)}${isOpen ? ' (打开/浏览类)' : ''}`);
    // —— HAR 业务动作重放：收藏/加购类任务（需 WPH_HAR_TASKS 且 HAR 账号与本账号一致）——
    const hb = loadHarBiz();
    const canReplay = hb && (isFavTask(name, type) || isAddTask(name, type)) && hb.vipruid === accVipruid(cookie);
    if (canReplay) {
      const kind = isFavTask(name, type) ? 'fav' : 'addcart';
      const req = hb[kind];
      if (!req) { log(`    跳过(HAR 未含${kind === 'fav' ? '收藏' : '加购'}请求)`); skip++; failStreak = 0; await sleep(rand(4000, 9000)); continue; }
      // 先领取 userTaskId（与正常流程一致），再重放业务动作，最后领奖
      let gt = await safeCall('/getTask', { taskId: t.taskId, unionid: '', openid: '' });
      if (gt && gt.code === 1 && gt.data && gt.data.userTaskId) utid = gt.data.userTaskId;
      log('    getTask:', gt && gt.code, gt && gt.msg || '', utid ? '(utid=' + utid + ')' : '');
      if (_harReplayed[kind]) { log(`    🔁 本跑已重放过${kind === 'fav' ? '收藏' : '加购'}动作(签名一次性)，直接领奖`); }
      else {
        log(`    🔁 重放 HAR ${kind === 'fav' ? '收藏' : '加购'}业务动作以完成任务...`);
        const rp = await replayHarBiz(req);
        _harReplayed[kind] = true;
        log('    重放返回:', rp && rp.code, rp && rp.msg || '');
        if (!rp || rp.code !== 1) { log('    ⚠️ 业务动作未成功(可能触发验证码/风控)，需真人处理'); skip++; failStreak = 0; await sleep(rand(4000, 9000)); continue; }
      }
      await sleep(rand(6000, 12000));
      const aw = await safeCall('/getAward', { platformActId: '', actId, relateId, taskId: t.taskId, userTaskId: utid });
      const alreadyDone = aw && aw.code === 30014;
      if (aw && aw.code === 1) { log('    ✅ 领取成功(业务动作完成)'); done++; failStreak = 0; }
      else if (alreadyDone) { log('    ✅ 任务已达成(已领过奖/达上限)'); done++; failStreak = 0; }
      else { log('    ⚠️ 领取:', aw && aw.code, aw && aw.msg || ''); fail++; failStreak++; if (failStreak >= 3) { log('  🛑 连续失败 3 次，熔断停止（防风控）'); break; } }
      await nap(5000, 10000);
      continue;
    }
    if (type === 4) { log('    跳过(购买类，需真实下单)'); skip++; failStreak = 0; await sleep(rand(4000, 9000)); continue; }
    if (type === 43) { log('    跳过(加购类，需 App 内真实加购)'); skip++; failStreak = 0; await sleep(rand(4000, 9000)); continue; }
    if (t.taskStatus === 2) { log('    已领取，跳过'); skip++; failStreak = 0; continue; }
    // 偶发"分心"长停顿，打破固定节奏（防风控）
    if (Math.random() < 0.12) { const lp = rand(30000, 90000); log(`  ⏸️ 模拟分心，暂停 ${(lp/1000).toFixed(0)}s`); await napMs(lp); }
    let gt = await safeCall('/getTask', { taskId: t.taskId, unionid: '', openid: '' });
    if (gt && gt.code === 1 && gt.data && gt.data.userTaskId) utid = gt.data.userTaskId;
    log('    getTask:', gt && gt.code, gt && gt.msg || '', utid ? '(utid=' + utid + ')' : '');
    if (!utid) { log('    ⚠️ 无 userTaskId，无法完成'); fail++; failStreak++; if (failStreak >= 3) { log('  🛑 连续失败 3 次，熔断停止（防风控）'); break; } await sleep(rand(5000, 10000)); continue; }
    // 浏览任务需真实浏览 browseTime 秒（getTaskDetail.browseTime=20），
    // 服务端会校验 getTask→finishTask 间隔，必须明显大于 20s 才稳妥（避免踩时长门槛）
    await sleep(rand(22000, 30000));
    const ftData = { actId, relateId, taskId: t.taskId, userTaskId: utid, taskName: name, icon: t.icon || '', wxUrl: t.url || '', appUrl: t.appUrl || '', unionid: '', openid: '' };
    let ft;
    if (type === 42) {
      // 进入会场类：finishTask 会返 30006「该任务类型不可操作」，只能直接领奖（已领过则 30014）
      log('    进入会场类(type=42 不可用 finishTask)，跳过完成直接领奖');
    } else {
      ft = await safeCall('/finishTask', ftData);
      log('    finishTask:', ft && ft.code, ft && ft.msg || '');
    }
    // 在线时长任务(type=18)时长不够时 finishTask 返回 30022，此时未达成，不能领奖，留待下次/真人累计在线时长后再领
    // 若设置了 WPH_ONLINE_WAIT>0：在「保持活动页打开」的前提下进入等待重试循环，累计够在线时长后当次领取。
    if (ft && ft.code === 30022) {
      let need = ''; try { const e = JSON.parse(t.extJson || '{}'); if (e.onlineTotalTime) need = ` (需累计在线 ${e.onlineTotalTime}s，当前不足)`; } catch (_) {}
      if (ONLINE_WAIT > 0) {
        log(`    ⏳ 在线时长未满足${need}，进入等待重试（最长 ${ONLINE_WAIT}s；需保持唯品会活动页打开以累计在线时长）`);
        let waited = 0, ok = false;
        while (waited < ONLINE_WAIT) {
          const step = Math.min(30000, ONLINE_WAIT - waited);
          await sleep(step); waited += step;
          const rf = await safeCall('/finishTask', ftData);
          if (rf && rf.code === 1) { log(`    ✅ 在线时长已满足（等 ${(waited / 1000) | 0}s），继续领奖`); ok = true; break; }
          log(`    ⏳ 仍不足（已等 ${(waited / 1000) | 0}s），继续...`);
        }
        if (!ok) { log('    ⏳ 等待结束仍未满足，留待下次运行'); await sleep(rand(6000, 12000)); continue; }
        // ok=true：落到下方 getAward 领取
      } else {
        log(`    ⏳ 在线时长未满足${need}，跳过领奖；保持活动页打开累计在线时长，下次运行再领`);
        await sleep(rand(6000, 12000)); continue;
      }
    }
    await sleep(rand(6000, 12000)); // 完成到领奖之间也会有停顿
    let aw = await safeCall('/getAward', { platformActId: '', actId, relateId, taskId: t.taskId, userTaskId: utid });
    // 已达成判定（命中任一项即视为正常完成，不计入失败、不触发熔断）：
    //   ft.code 10052/30023 = finishTask 时已达上限/已达成；
    //   aw.code 30014 = 当天已领过奖(达最大领奖次数)，如 type=42 进入会场类任务（finishTask 会返 30006 不可用，但领奖已达上限=已完成）
    const alreadyDone = (ft && (ft.code === 10052 || ft.code === 30023)) || (aw && aw.code === 30014);
    if (aw && aw.code === 1) { log('    ✅ 领取成功'); done++; failStreak = 0; }
    else if (alreadyDone) { log('    ✅ 任务已达成(已领过奖/达上限)，跳过领奖'); done++; failStreak = 0; }
    else { log('    ⚠️ 领取:', aw && aw.code, aw && aw.msg || ''); fail++; failStreak++; if (failStreak >= 3) { log('  🛑 连续失败 3 次，熔断停止（防风控）'); break; } }
    await nap(5000, 10000); // 任务之间真人会歇一下再点下一个
  }
  log(`\n  —— ${label}：完成 ${done}，跳过 ${skip}，失败 ${fail}`);
  j = await callApi(BASE, '/getTaskList', {}, cookie, cid, sid, apiKey, base);
  const after = (j.data && j.data.taskList) || [];
  log(`  🔁 复检：可领 ${after.filter((x) => x.taskStatus === 1).length}，已领 ${after.filter((x) => x.taskStatus === 2).length} / 共 ${after.length}`);
}

/* ===================== 2) 拔萝卜在线时长任务 (原 checkRoomTask 胡萝卜组) ===================== */
// 注：胡萝卜/拔萝卜在线时长任务实际挂在 commonTask 体系下（actId=czg91SQsPfs, relateId=5Zev7kIeYbKaKOAU4QLCfw），
// 用 checkRoomTask base 调会返回 20099，故见下方 processCheckRoom（已改用 commonTask base）。
const CRID = '5Zev7kIeYbKaKOAU4QLCfw';
function splitActUrl(url) { const m = url.match(/^(https?:\/\/[^\/]+)\/([^\/]+)(\/.*)?$/); return m ? [m[1] + '/' + m[2], m[3] || '/'] : [url, '/']; }
async function processCheckRoom(cookie, cid, sid) {
  // 重要：胡萝卜/拔萝卜在线时长任务实际挂在 commonTask 体系下（actId=czg91SQsPfs, relateId=5Zev7kIeYbKaKOAU4QLCfw），
  // 并非 checkRoomTask base（用 checkRoomTask base 调会返回 20099，拿不到任务）。故此处用 commonTask base。
  // 在线时长任务(type=18)：服务端按用户在线累计 onlineTime，必须达到 extJson.onlineTotalTime 才能 finishTask 成功。
  // finishTask 在时长不够时返回 30022「在线时长任务未满足时长条件」——此时【不可领奖】，应留作未完成，
  // 待用户保持唯品会活动页打开累计够在线时长、下次运行再领取（脚本无法自刷时长，因无心跳接口）。
  log(`\n########## 拔萝卜在线时长任务 (commonTask actId=czg91SQsPfs) ##########`);
  const actId = 'czg91SQsPfs', relateId = '5Zev7kIeYbKaKOAU4QLCfw';
  const BASE = 'https://act-ug.vip.com/commonTask';
  const base = baseParams(cid, actId, relateId);
  // 在线时长任务(type=18)：WPH_ONLINE_WAIT>0 时，在“保持活动页打开”前提下进入等待重试，累计够在线时长后当次领取。
  const ONLINE_WAIT = Number(process.env.WPH_ONLINE_WAIT) || 0;
  const safeCR = async (ep, data, retries = 3) => {
    for (let a = 0; a <= retries; a++) {
      const r = await callApi(BASE, ep, Object.assign({ actId, relateId }, data), cookie, cid, sid, API_KEY, base);
      if (r && r.code === 11001 && a < retries) { log(`    ⚠️ 11001 限流，退避重试(${a+1})`); await sleep(rand(8000, 15000)); continue; }
      return r;
    }
  };
  const tl = await safeCR('/getTaskList', {});
  log('getTaskList code=', tl.code);
  if (tl.code === 1 && tl.data) {
    const list = (tl.data.taskList || []);
    log('任务数:', list.length);
    for (const t of list) {
      log(`  — 「${t.taskName}」 type=${t.taskType} status=${t.taskStatus} utid=${t.userTaskId || '(空)'}`);
      if (t.taskStatus === 2) { log('    已领取，跳过'); await sleep(rand(3000, 7000)); continue; }
      let ut = t.userTaskId;
      if (!ut) { const g = await safeCR('/getTask', { taskId: t.taskId }); if (g.code !== 1) { log('    getTask:', g.code, g.msg); await sleep(rand(3000, 7000)); continue; } ut = g.data && g.data.userTaskId; log('    getTask:', g.code, 'utid=' + ut); await sleep(rand(3000, 7000)); }
      const f = await safeCR('/finishTask', { actId, relateId, taskId: t.taskId, userTaskId: ut, taskName: t.taskName, icon: t.icon || '', wxUrl: t.url || '', appUrl: t.appUrl || '', unionid: '', openid: '' });
      log('    finishTask:', f.code, f.msg || '');
      if (f.code === 30022) {
        let need = ''; try { const e = JSON.parse(t.extJson || '{}'); if (e.onlineTotalTime) need = ` (需累计在线 ${e.onlineTotalTime}s，当前不足)`; } catch (_) {}
        if (ONLINE_WAIT > 0) {
          log(`    ⏳ 在线时长未满足${need}，进入等待重试（最长 ${ONLINE_WAIT}s；需保持活动页打开）`);
          let waited = 0, ok = false;
          while (waited < ONLINE_WAIT) {
            const step = Math.min(30000, ONLINE_WAIT - waited);
            await sleep(step); waited += step;
            const rf = await safeCR('/finishTask', { actId, relateId, taskId: t.taskId, userTaskId: ut, taskName: t.taskName, icon: t.icon || '', wxUrl: t.url || '', appUrl: t.appUrl || '', unionid: '', openid: '' });
            if (rf && rf.code === 1) { log(`    ✅ 在线时长已满足（等 ${(waited / 1000) | 0}s）`); f = rf; ok = true; break; }
            log(`    ⏳ 仍不足（已等 ${(waited / 1000) | 0}s），继续...`);
          }
          if (!ok) { log('    ⏳ 等待结束仍未满足，留待下次'); await sleep(rand(3000, 7000)); continue; }
        } else {
          log(`    ⏳ 在线时长未满足${need}，本跑不领奖；保持唯品会活动页打开累计在线时长，下次运行再领取`);
          await sleep(rand(3000, 7000)); continue;
        }
      }
      if (f.code !== 1 && f.code !== 30023) { log('    ⚠️ 未完成:', f.msg); await sleep(rand(3000, 7000)); continue; }
      const a = await safeCR('/getAward', { platformActId: '', actId, relateId, taskId: t.taskId, userTaskId: ut });
      if (a.code === 1) log('    ✅ 领取成功'); else log('    ⚠️ 领取:', a.code, a.msg);
      await sleep(rand(3000, 7000));
    }
  } else log('TASKLIST:', JSON.stringify(tl).slice(0, 400));
}

/* ===================== 2b) checkRoom 合成小游戏 (兔兔爱合成: 合包包) ===================== */
// 玩法：12 格棋盘，每格一个包(goodsLevel)。两个【同级】包合成 → 高一级包，同时源格清空。
// 高级包产币更快、可解锁地图/兑换津贴。核心接口(base=/checkRoom, actId=H3gRnE1Xi18, 需 checkRoomId)：
//   /checkRoomInfo  查棋盘(goodsListInfo:[{goodsLevel,position}]) + recBuyGoodsInfo + userInfo
//   /mergeGoods     合成 {sourcePosition,targetPosition}（两个同级 → target 变 level+1）
//   /shoppingGoods  买包 {goodsLevel,userLocalTime}（买 recBuyLevel 级包，落到第一个空格）
// 合成/买包响应只回传【变更的格子】(data.goodsListInfo[0])，故本地维护棋盘，避免频繁全量拉取。
const GAME_BASE = 'https://act-ug.vip.com/checkRoom';
const GAME_ACT = 'H3gRnE1Xi18';
const GAME_SLOTS = 12; // 棋盘格子总数（源自游戏 initBox: new Array(12)）
function mrGame(cid) { return { source_app: 'app', client_type: '', app_name: 'wap', client: 'iphone', api_key: API_KEY, app_version: '9.79.6', mobile_platform: '3', mobile_channel: 'nature', mars_cid: cid, warehouse: 'VIP_NH', fdc_area_id: '104104', province_id: '104104', wap_consumer: 'A1' }; }
async function callGame(cookie, cid, sid, ep, data, checkRoomId) {
  const url = GAME_BASE + ep;
  // 11001 not authorized 多为短时间高频请求触发的瞬时限流，退避重试即可恢复
  for (let attempt = 0; attempt < 3; attempt++) {
    const param = Object.assign({}, mrGame(cid), { actId: GAME_ACT, checkRoomId: checkRoomId || '' }, data || {});
    const query = Object.keys(param).map((k) => k + '=' + encodeURIComponent(param[k])).join('&');
    const signedUrl = url + '?' + query;
    const auth = signDo(signedUrl, param, cid, sid, API_KEY);
    const r = await pfetch(signedUrl, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Origin': 'https://mst.vip.com', 'Referer': 'https://mst.vip.com/', 'User-Agent': AUTO_UA, 'Authorization': auth, 'Cookie': cookie, 'Accept': '*/*' }, body: query });
    const j = await r.json().catch(() => ({ code: -1, msg: 'json_error' }));
    if (j && j.code === 11001 && attempt < 2) { await sleep(rand(3000, 6000)); continue; }
    return j;
  }
}
// 兔兔爱合成·幸运红包(myCheckroom)专属接口：act-ug.vip.com/luckylottery/withSign
// 与 checkRoom 同域同签名体系(signDo + OAuth api_sign)，故复用 mrGame/signDo。
async function callLucky(cookie, cid, sid, ep, data) {
  const url = 'https://act-ug.vip.com/luckylottery/withSign' + ep;
  for (let attempt = 0; attempt < 3; attempt++) {
    const param = Object.assign({}, mrGame(cid), data || {});
    const query = Object.keys(param).map((k) => k + '=' + encodeURIComponent(param[k])).join('&');
    const signedUrl = url + '?' + query;
    const auth = signDo(signedUrl, param, cid, sid, API_KEY);
    const r = await pfetch(signedUrl, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8', 'Origin': 'https://mst.vip.com', 'Referer': 'https://mst.vip.com/', 'User-Agent': AUTO_UA, 'Authorization': auth, 'Cookie': cookie, 'Accept': '*/*' }, body: query });
    const j = await r.json().catch(() => ({ code: -1, msg: 'json_error' }));
    if (j && j.code === 11001 && attempt < 2) { await sleep(rand(3000, 6000)); continue; }
    return j;
  }
}
// 获取当前登录用户真实的 checkRoomId（每账号不同）。checkRoom 系接口(mapList/getConfig/getMapAward)
// 会校验 checkRoomId 与登录用户是否一致，传活动常量 CRID 对非建房账号会报 10032「用户与登录用户不符」。
const _crIdCache = {};
async function getCheckRoomId(cookie, cid, sid) {
  if (_crIdCache[cid]) return _crIdCache[cid];
  const info = await callGame(cookie, cid, sid, '/checkRoomInfo', { openid: '' }, '');
  const rid = info && info.data && info.data.userInfo && info.data.userInfo.checkRoomId;
  if (rid) _crIdCache[cid] = rid;
  return rid || '';
}
// 从棋盘(position->level 的 Map)里找一对同级包；优先返回【最高等级】的同级对（追最高级策略），没有则 null。
function findMergePairHighest(board) {
  const byLevel = new Map();
  for (const [pos, lv] of board) { if (!byLevel.has(lv)) byLevel.set(lv, []); byLevel.get(lv).push(pos); }
  const levels = [...byLevel.keys()].sort((a, b) => b - a); // 高 → 低
  for (const lv of levels) { if (byLevel.get(lv).length >= 2) return [byLevel.get(lv)[0], byLevel.get(lv)[1]]; }
  return null;
}
// 查询道具仓库（含魔力扫把等），返回 propList（每项含 propType/propName/propNum）。
// 魔力扫把用于棋盘满时清除格子（见 mergeBags 第 4 步）。
async function getWarehouse(cookie, cid, sid, checkRoomId) {
  const r = await callGame(cookie, cid, sid, '/warehouseDetail', {}, checkRoomId);
  if (r.code !== 1 || !r.data) return [];
  return (r.data.propList || []);
}
async function mergeBags(cookie, cid, sid) {
  log(`\n########## 兔兔爱合成 (合包包·追最高级) ##########`);
  // 策略（按最高等级兑换红包，合理计算）：
  //  1) 合成时优先合并【最高级】同级对，使高级包持续升级 → 高等级包对应更高红包/津贴收益；
  //  2) 金币够(enoughToBuy=1)且有空格时，买推荐级包凑成新同级对，进入下一轮继续升级；
  //  3) 当棋盘满(无空格)或买不了包且无可合并对(卡住)时，用道具仓库的「魔力扫把」
  //     (/recycleGoods 清掉最低级格) 腾出位置，继续合成；无扫把则结束。
  const MAX_ACTIONS = 60;   // 合成+买包+清格 总操作上限，防风控
  const MAX_BROOM = 8;      // 单轮最多使用扫把次数
  let actions = 0, mergeTotal = 0, buyTotal = 0, broomUsed = 0, checkRoomId = '';
  for (let round = 0; ; round++) {
    if (actions >= MAX_ACTIONS) { log('  ⏹ 达操作上限，结束'); break; }
    // 拉取棋盘（每轮同步一次，保证 recBuy/彩蛋占位准确）
    const info = await callGame(cookie, cid, sid, '/checkRoomInfo', { openid: '' }, checkRoomId);
    if (info.code !== 1 || !info.data) { log('  checkRoomInfo 失败:', info.code, info.msg || ''); return; }
    if (!checkRoomId) checkRoomId = (info.data.userInfo && info.data.userInfo.checkRoomId) || '';
    if (!checkRoomId) { log('  未取到 checkRoomId，跳过'); return; }
    const goods = info.data.goodsListInfo || [];
    const eggs = info.data.easterEggListInfo || [];
    const recBuy = info.data.recBuyGoodsInfo || {};
    const grade = info.data.userInfo && info.data.userInfo.currentGrade;
    if (round === 0) log(`  房间=${checkRoomId} 最高级=Lv${grade} 场上=${goods.length}/${GAME_SLOTS}格`);
    // 占位集合（包 + 彩蛋），本地棋盘 position->level
    const occupied = new Set();
    const board = new Map();
    for (const g of goods) { occupied.add(g.position); board.set(g.position, Number(g.goodsLevel)); }
    for (const e of eggs) { if (e.position) occupied.add(e.position); }
    // 1) 合成：优先最高级同级对（追最高等级）
    let mergedThisRound = 0;
    while (actions < MAX_ACTIONS) {
      const pair = findMergePairHighest(board);
      if (!pair) break;
      const [src, tgt] = pair;
      const lv = board.get(tgt);
      await sleep(rand(2500, 5000));
      const r = await callGame(cookie, cid, sid, '/mergeGoods', { sourcePosition: src, targetPosition: tgt }, checkRoomId);
      actions++;
      if (r.code === 1 && r.data && r.data.goodsListInfo && r.data.goodsListInfo.length) {
        const cell = r.data.goodsListInfo[0];
        board.delete(src); occupied.delete(src);
        board.set(cell.position, Number(cell.goodsLevel));
        mergedThisRound++; mergeTotal++;
        log(`  ✅ 合成 位${src}+位${tgt} → Lv${cell.goodsLevel}`);
      } else {
        log(`  ⚠️ 合成失败 位${src}+位${tgt}(Lv${lv}): ${r.code}/${r.msg || ''}`);
        break;
      }
    }
    // 2) 买包填空：金币够且有空格时，买 recBuyLevel 级包造新同级对
    const freeSlots = GAME_SLOTS - occupied.size;
    const recLevel = Number(recBuy.recBuyLevel || 0);
    let boughtThisRound = 0;
    if (recBuy.enoughToBuy === 1 && freeSlots > 0 && recLevel > 0 && actions < MAX_ACTIONS) {
      const buyN = Math.min(freeSlots, Number(recBuy.recBuyGoodsNum) || 1, MAX_ACTIONS - actions);
      for (let k = 0; k < buyN; k++) {
        await sleep(rand(2500, 5000));
        const r = await callGame(cookie, cid, sid, '/shoppingGoods', { goodsLevel: recLevel, userLocalTime: Date.now() }, checkRoomId);
        actions++;
        if (r.code === 1) { boughtThisRound++; buyTotal++; log(`  🛒 买包 Lv${recLevel} #${k + 1} ✅`); }
        else { log(`  🛒 买包 Lv${recLevel} 停止: ${r.code}/${r.msg || ''}`); break; }
      }
    }
    // 3) 卡住判定：无新合成、无新买包，且(棋盘满 或 买不了包)
    const cannotBuy = recBuy.enoughToBuy !== 1 || recLevel === 0;
    const stuck = mergedThisRound === 0 && boughtThisRound === 0 && (freeSlots === 0 || cannotBuy);
    if (!stuck) {
      if (mergedThisRound === 0 && boughtThisRound === 0) { log('  ✅ 稳定，无可操作'); break; }
      continue; // 有进展 → 下一轮重新拉棋盘继续
    }
    // 4) 卡住处理：只有「棋盘满 且 买得起包」才用扫把清最低级格腾位（清位后买包可凑成新对继续升级）；
    //    若卡住原因是「金币不足/买不起」，清格毫无收益（空位也填不上、也无对可合），应停止，否则会
    //    白白烧掉稀缺的魔力扫把、还可能把刚花钱买的包清掉，与目标(追最高级包)背道而驰。
    const boardFull = freeSlots === 0;
    if (!boardFull) {
      log('  ℹ️ 卡住=金币不足且无可合成对，清格无收益，停止（去做消消乐/任务攒金币后再跑）');
      break;
    }
    if (cannotBuy) {
      log('  ℹ️ 棋盘满但金币不足、无法买包凑对，清格无意义，停止');
      break;
    }
    if (broomUsed >= MAX_BROOM) { log(`  ℹ️ 扫把使用达上限(${MAX_BROOM})，结束`); break; }
    const wh = await getWarehouse(cookie, cid, sid, checkRoomId);
    const broom = wh.find((p) => /扫把|魔力/.test(p.propName || ''));
    if (broom && Number(broom.propNum) > 0) {
      const lowest = [...board.entries()].sort((a, b) => a[1] - b[1])[0];
      if (!lowest) { log('  ℹ️ 无可清除的包，结束'); break; }
      const pos = lowest[0], lv = lowest[1];
      await sleep(rand(2500, 5000));
      const r = await callGame(cookie, cid, sid, '/recycleGoods', { sourcePosition: pos }, checkRoomId);
      actions++;
      if (r.code === 1) {
        broomUsed++; occupied.delete(pos); board.delete(pos);
        log(`  🧹 魔力扫把清除 位${pos}(Lv${lv}) ✅ 剩余扫把≈${Number(broom.propNum) - broomUsed}`);
        continue; // 腾出位置，下一轮继续合成
      } else { log(`  🧹 扫把清除失败 位${pos}: ${r.code}/${r.msg || ''}`); break; }
    } else {
      log('  ℹ️ 道具仓库无魔力扫把(数量为0)，无法清格，结束');
      break;
    }
  }
  log(`  —— 兔兔爱合成：合成 ${mergeTotal} 次，买包 ${buyTotal} 个，扫把清格 ${broomUsed} 次`);
}

/* ===================== 2b+) 幸运红包 (LuckyRedpacket·抢红包) ===================== */
// 兔兔爱合成里的「幸运红包」模块，actId 由 frontendConfig.luckyRedpacketActId 动态下发
// （与 miningActId 同机制，经 /getConfig 拿到）。走 commonTask 体系领奖。
// 流程：先 queryRedpacket 拉可抢红包（含面额）→ 交互确认抢哪些 → processRedPacket 抢。
let _rpActId = '';
async function getRedpacketActId(cookie, cid, sid) {
  if (_rpActId) return _rpActId;
  const roomId = await getCheckRoomId(cookie, cid, sid);
  const cfg = await callGame(cookie, cid, sid, '/getConfig', {}, roomId || CRID);
  if (cfg.code === 1 && cfg.data && cfg.data.frontendConfig) {
    try { const fc = JSON.parse(cfg.data.frontendConfig); _rpActId = fc.luckyRedpacketActId || ''; } catch (_) {}
  } else {
    log('  ⚠️ getConfig 失败', cfg.code, cfg.msg || '');
  }
  return _rpActId;
}
// 查询某账号可抢红包列表（myCheckroom getAwardList）；返回 { actId, list } 或 null
async function queryRedpacket(cookie, cid, sid) {
  const actId = await getRedpacketActId(cookie, cid, sid);
  if (!actId) { log('  未取到 luckyRedpacketActId，无幸运红包活动'); return null; }
  const j = await callLucky(cookie, cid, sid, '/getAwardList', { actId });
  if (!j || j.code !== 1) { log('  getAwardList 失败', j && j.code, j && j.msg, 'raw=', JSON.stringify(j).slice(0, 600)); return null; }
  const list = (j.data && j.data.rewardList) || [];
  return { actId, list, data: j.data };
}
// 全局抢红包选择：null=预览不抢；'skip'=跳过；'all'=全抢；数组=只抢指定面额
let RP_CHOICE = null;
// 交互询问抢哪些（仅 TTY 下调用）
function askRedpacketChoice() {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
    log('👉 确认抢红包(九宫格随机抽奖，中到哪个格子随机，无法指定面额)? 输入抽奖次数(如 5) / all(抽完当日次数) / n(跳过):');
    let done = false;
    const finish = (v) => { if (done) return; done = true; try { rl.close(); } catch (_) {} resolve(v); };
    rl.on('line', (line) => {
      const s = (line || '').trim().toLowerCase();
      if (!s || s === 'n') return finish('skip');
      if (s === 'all') return finish('all');
      if (/^\d+$/.test(s)) return finish(Number(s));
      finish('skip');
    });
    rl.on('close', () => finish('skip'));
  });
}
async function processRedPacket(cookie, cid, sid) {
  log(`\n########## 幸运红包 (LuckyRedpacket·九宫格抽奖) ##########`);
  const q = await queryRedpacket(cookie, cid, sid);
  if (!q) return;
  const { actId, list } = q;
  if (!list.length) { log('  九宫格暂无奖品配置'); return; }
  log('  九宫格奖品：');
  list.forEach((t, i) => log(`    [${i + 1}] ${t.name}（type=${t.type}）`));
  if (RP_CHOICE === null) { log('  ℹ️ 预览模式：未确认，不自动抢。设 WPH_REDPACKET=all / 次数(如 5) / skip 可指定；TTY 下默认直接抽 all（WPH_REDPACKET_ASK=1 恢复询问）。'); return; }
  if (RP_CHOICE === 'skip') { log('  ⏭️ 已选择跳过抢红包'); return; }
  // 抽奖次数：'all' = 抽完当日次数（循环到接口返回非成功）；数字 = 抽指定次
  const isAll = RP_CHOICE === 'all';
  const times = isAll ? Infinity : (Number(RP_CHOICE) || 0);
  if (!isAll && (!times || times <= 0)) { log('  ⚠️ 无效抽奖次数，跳过'); return; }
  log(`  🎰 开始抽奖（${isAll ? '抽完当日次数' : times + ' 次'}）...`);
  let n = 0, hit = 0, noWin = 0;
  while (n < (isFinite(times) ? times : 999)) {
    n++;
    const r = await callLucky(cookie, cid, sid, '/lotteryDraw', { actId });
    if (!r || r.code !== 1) {
      log(`    #${n} 停止：code=${r && r.code} msg=${r && r.msg || ''} raw=${JSON.stringify(r).slice(0, 240)}`);
      break;
    }
    const reward = (r.data && r.data.reward) || null;
    if (reward && String(reward.type) === '0') {
      noWin++;
      log(`    #${n} 未中奖（${r.msg || ''}）`);
    } else if (reward) {
      hit++;
      const nm = reward.name || (reward.rewardList && reward.rewardList[0] && reward.rewardList[0].name) || '';
      log(`    #${n} 🎁 中奖：${nm} raw=${JSON.stringify(reward).slice(0, 200)}`);
    } else {
      log(`    #${n} 返回异常 raw=${JSON.stringify(r).slice(0, 240)}`);
    }
    await nap(3000, 8000); // 真人抽奖间隔，防风控
  }
  log(`  —— 抽奖结束：共 ${n} 次，中奖 ${hit} 次，未中 ${noWin} 次`);
}

/* ===================== 2c) 兔兔采矿 (mining) ===================== */
// 采矿是 commonTask 下的独立子活动，actId 来自 frontendConfig.miningActId（动态），relateId=CRID。
// 顺序任务：先 getTask 启动“采矿”，taskStatus=0 时 finishTask 推进，taskStatus=1 时 getAward 领奖；
// 一个任务进行中(active)时另一个 getTask 会报 -6（顺序约束），故每次只处理单个活动任务。
let _miningActId = '';
async function getMiningActId(cookie, cid, sid) {
  if (_miningActId) return _miningActId;
  const roomId = await getCheckRoomId(cookie, cid, sid);
  const cfg = await callGame(cookie, cid, sid, '/getConfig', {}, roomId || CRID);
  if (cfg.code === 1 && cfg.data && cfg.data.frontendConfig) {
    try { const fc = JSON.parse(cfg.data.frontendConfig); _miningActId = fc.miningActId || ''; } catch (_) {}
  }
  return _miningActId;
}
async function processMining(cookie, cid, sid) {
  log(`\n########## 兔兔采矿 (mining) ##########`);
  const MINING_ACT = await getMiningActId(cookie, cid, sid);
  if (!MINING_ACT) { log('  未取到 miningActId，跳过采矿'); return; }
  const base = baseParams(cid, MINING_ACT, CRID);
  // 11001 限流退避（commonTask 体系，遇限流重试）
  const safeApi = async (p, extra) => {
    for (let a = 0; a <= 3; a++) {
      const r = await callApi('https://act-ug.vip.com/commonTask', p, extra, cookie, cid, sid, API_KEY, base);
      if (r && r.code === 11001 && a < 3) { log(`  ⚠️ 11001 限流，退避重试(${a+1})`); await sleep(rand(8000, 15000)); continue; }
      return r;
    }
  };
  const j = await safeApi('/getTaskList', {});
  if (!j || j.code !== 1) { log('  getTaskList 失败', j && j.code, j && j.msg); return; }
  const list = (j.data && j.data.taskList) || [];
  if (!list.length) { log('  无采矿任务'); return; }
  list.sort((a, b) => (Number(a.taskOrder) || 0) - (Number(b.taskOrder) || 0));
  const active = list.find((t) => t.hasGetTask === 1 && t.taskStatus !== 2);
  const startable = list.find((t) => t.hasGetTask === 0);
  if (active) {
    const t = active, name = t.taskName || t.taskId, ut = t.userTaskId || '';
    const ft = Number(t.finishTaskNum) || 0, mr = Number(t.maxRepeatCount) || 0, y = mr - ft;
    if (t.taskStatus === 1) {
      const a = await safeApi('/getAward', { platformActId: '', actId: MINING_ACT, relateId: CRID, taskId: t.taskId, userTaskId: ut });
      log(`  🎁 领取采矿奖励「${name}」: ${a.code}/${a.msg || ''}${a.code === 1 ? ' ✅' : ''}`);
    } else if (t.taskStatus === 0 && y > 0 && t.nextTaskTime === '0') {
      const f = await safeApi('/finishTask', { actId: MINING_ACT, relateId: CRID, taskId: t.taskId, userTaskId: ut, taskName: name, icon: t.icon || '', wxUrl: t.url || '', appUrl: t.appUrl || '', unionid: '', openid: '' });
      log(`  ⛏️ 采矿进度「${name}」: ${f.code}/${f.msg || ''}${f.code === 1 ? ' ✅' : ''}`);
    } else {
      log(`  ⏳ 「${name}」采矿进行中/次日回访(status=${t.taskStatus} ${ft}/${mr})，等待完成`);
    }
    await sleep(rand(2000, 4000));
  } else if (startable) {
    const g = await safeApi('/getTask', { taskId: startable.taskId, unionid: '', openid: '' });
    if (g && g.code === 1 && g.data && g.data.userTaskId) log(`  🚀 启动采矿「${startable.taskName || startable.taskId}」✅ utid=${g.data.userTaskId}`);
    else log(`  🚀 启动采矿「${startable.taskName || startable.taskId}」: ${g && g.code}/${g && g.msg || ''}`);
    await sleep(rand(2000, 4000));
  } else {
    log('  ✅ 采矿任务已全部完成');
  }
}

/* ===================== 2d) 地图任务奖励 (checkRoomMap) ===================== */
// 地图(城市)宝箱奖励通过 checkRoom/mapList 列出，status=0 表示可领，调用 getMapAward(locationId,indexId) 领取。
// 随账号解锁新城市/达成地图进度，会出现 status=0 的可领奖励。
async function processMapAward(cookie, cid, sid) {
  log(`\n########## 地图任务奖励 (checkRoomMap) ##########`);
  const roomId = await getCheckRoomId(cookie, cid, sid);
  if (!roomId) { log('  未取到 checkRoomId，跳过地图奖励'); return; }
  const ml = await callGame(cookie, cid, sid, '/mapList', {}, roomId);
  if (ml.code !== 1 || !ml.data) { log('  mapList 失败:', ml.code, ml.msg || ''); return; }
  const boxes = (ml.data.awardBoxInfo || []);
  const pending = boxes.filter((b) => b.status === 0);
  log(`  地图宝箱共 ${boxes.length} 个，可领 ${pending.length} 个 (currentGrade=${ml.data.userInfo && ml.data.userInfo.currentGrade})`);
  let ok = 0;
  for (const b of pending) {
    const r = await callGame(cookie, cid, sid, '/getMapAward', { locationId: b.locationId, indexId: b.indexId }, roomId);
    if (r.code === 1) { log(`  🎁 领取地图奖励 城市${b.locationId}#${b.indexId} ✅`); ok++; }
    else log(`  🎁 领取地图奖励 城市${b.locationId}#${b.indexId}: ${r.code}/${r.msg || ''}`);
    await sleep(rand(2000, 4000));
  }
  if (pending.length === 0) log('  （暂无可领地图奖励，随解锁新城市会出现）');
  else log(`  —— 地图奖励领取 ${ok}/${pending.length}`);
}

/* ===================== 2e) 离开后领取奖励 (兔兔爱合成·离线/挂机金币) ===================== */
// 玩法：离开小游戏一段时间后再进入，服务端会计算「离开期间」产出的金币（离线/挂机收益），
// 在 checkRoomInfo 的响应里返回 offlineAwardGoldNum（已【自动并入】 userInfo.currentGoldNum）。
// 前端弹出的「offilineIncomePopup(离线收益)」领取弹窗仅做金币飞入动画，不调任何领奖接口。
// 因此「离开后领取奖励」= 调用 checkRoomInfo 触发服务端入账 + 读出 offlineAwardGoldNum 回报即可，
// 无需、也不存在单独的 getOfflineAward / claim 接口（已核对游戏包体确认）。
async function processOfflineAward(cookie, cid, sid) {
  log(`\n########## 离开后领取奖励 (兔兔爱合成·离线/挂机金币) ##########`);
  const roomId = await getCheckRoomId(cookie, cid, sid);
  if (!roomId) { log('  未取到 checkRoomId，跳过离线收益'); return; }
  const info = await callGame(cookie, cid, sid, '/checkRoomInfo', { openid: '' }, roomId);
  if (info.code !== 1 || !info.data) { log('  checkRoomInfo 失败:', info.code, info.msg || ''); return; }
  // offlineAwardGoldNum 在 checkRoomInfo 顶层 data，部分版本可能挂在 userInfo 下，二者都兜底读。
  const d = info.data || {};
  const gold = Number((d.userInfo && d.userInfo.currentGoldNum) || 0);
  const offline = Number(d.offlineAwardGoldNum || (d.userInfo && d.userInfo.offlineAwardGoldNum) || 0);
  const maxHours = Number(d.offlineAwardMax || (d.userInfo && d.userInfo.offlineAwardMax) || 0);
  if (offline > 0) {
    log(`  ✅ 检测到离开期间收益，已自动入账 金币 +${offline}（最多可累积 ${maxHours} 小时收益）`);
    log(`  💰 当前金币余额: ${gold}`);
  } else {
    log('  ℹ️ 暂无常驻离线收益（刚领过 / 离开时间不足 / 未达产出阈值）');
  }
  await sleep(rand(2000, 4000));
}

/* ===================== 3) feedSheep (天天剪羊毛玩法) ===================== */
const SHEEP_BASE = 'https://act-ug.vip.com/feedSheep';
function mrSheep(cid) { return { actId: 'H3gRnE1Xi18', source_app: 'app', client_type: 'wap', app_name: 'shop_iphone', client: 'iphone', api_key: API_KEY, app_version: '9.79.7', mobile_platform: '3', mobile_channel: 'ng00010v:al80ssgp:37u8zn0w:ng00010p', mars_cid: cid, warehouse: 'VIP_NH', fdc_area_id: '911101114112', province_id: '911101114112', wap_consumer: 'C2-4-2' }; }
async function callSheep(url, data, cookie, cid, sid, extra) {
  const [B, p] = splitActUrl(url);
  // 11001 限流退避（与 callGame 同策略，遇瞬时限流自动退避重试）
  for (let attempt = 0; attempt < 3; attempt++) {
    const r = await callApi(B, p, Object.assign({}, extra || {}, data), cookie, cid, sid, API_KEY, mrSheep(cid));
    if (r && r.code === 11001 && attempt < 2) { await sleep(rand(8000, 15000)); continue; }
    return r;
  }
}
// 赚草料：完成 feedSheep 任务列表里的任务以获得草料（浏览/分享/签到等）。
// 关键发现：浏览类任务(type=2/3/33 等)的 userTaskId 已经在 getTaskList 返回里预置好，
// 且任务已被自动激活(getTask 会报 10020“已存在”)。正确流程是：
//   直接用列表里的 userTaskId → finishTask(标记浏览完成) → getAward(领奖，receiveAwardNum=领到的草料)
// 之前代码用 getTask 去拿 userTaskId 失败就跳过了，所以浏览任务全被漏掉。
const BROWSE_TYPES = new Set([2, 3, 33]);
async function earnGrass(cookie, cid, sid, FSID) {
  log(`\n--- 赚草料 (完成任务得草料) ---`);
  log('  ℹ️ 若某任务(如收藏商品/打开今日种草好物)显示“需真人操作，跳过”，请把它打印的 type= 与 finishTask 返回码发我，便于精确支持');
  for (const tparam of [{ type: 'friend' }, {}]) {
    const tl = await callSheep(SHEEP_BASE + '/withSign/getTaskList', Object.assign({ feedSheepId: FSID }, tparam), cookie, cid, sid);
    if (tl.code !== 1) { log('  getTaskList', JSON.stringify(tparam), '失败:', tl.code); continue; }
    const list = (tl.data && tl.data.taskList) || [];
    log(`  任务列表(type=${tparam.type || 'all'}): ${list.length} 个`);
    for (const t of list) {
      const ut0 = t.userTaskId || '';
      log(`    — 「${t.taskName}」 type=${t.taskType} status=${t.taskStatus} utid=${ut0 || '(空)'} jumpUrl=${(t.url || t.appUrl || '').slice(0, 60)}`);
      if (t.taskStatus === 2) { log('      已领取，跳过'); continue; }
      // —— HAR 业务动作重放：收藏/加购类任务 ——
      const hb2 = loadHarBiz();
      const canReplay2 = hb2 && (isFavTask(t.taskName, t.taskType) || isAddTask(t.taskName, t.taskType)) && hb2.vipruid === accVipruid(cookie);
      if (canReplay2) {
        const kind = isFavTask(t.taskName, t.taskType) ? 'fav' : 'addcart';
        const req = hb2[kind];
        if (!req) { log('      跳过(HAR 未含对应请求)'); await sleep(rand(3000, 6000)); continue; }
        let ut = ut0;
        if (!ut) {
          const gt = await callSheep(SHEEP_BASE + '/withSign/getTask', { feedSheepId: FSID, taskId: t.taskId, subscribeMsg: '0', unionid: '', openid: '' }, cookie, cid, sid);
          ut = (gt.data && gt.data.userTaskId) || '';
          log('      getTask:', gt.code, gt.msg || '', ut ? '(utid=' + ut + ')' : '');
        }
        if (_harReplayed[kind]) { log(`      🔁 本跑已重放过${kind === 'fav' ? '收藏' : '加购'}动作，直接领奖`); }
        else {
          log(`      🔁 重放 HAR ${kind === 'fav' ? '收藏' : '加购'}业务动作...`);
          const rp = await replayHarBiz(req);
          _harReplayed[kind] = true;
          log('      重放返回:', rp && rp.code, rp && rp.msg || '');
          if (!rp || rp.code !== 1) { log('      ⚠️ 业务动作未成功(可能风控/验证码)，需真人处理'); await sleep(rand(3000, 6000)); continue; }
        }
        await sleep(rand(3000, 6000));
        const aw = await callSheep(SHEEP_BASE + '/withSign/getAward', { feedSheepId: FSID, taskId: t.taskId, userTaskId: ut }, cookie, cid, sid);
        const AW_DONE = new Set([1, 10053, 30014]);
        if (AW_DONE.has(aw.code)) { log(`      领取${aw.code === 1 ? '✅ 草料+' + ((aw.data && aw.data.receiveAwardNum) || '?') : '✅ 已达成(今日已领过)'}`); }
        else { log(`      领取失败 ${aw.code}/${aw.msg || ''}`); }
        await sleep(rand(3000, 7000));
        continue;
      }
      // 需真实下单/加购的任务无法脚本完成，跳过
      if (t.taskType === 4 || t.taskType === 43) { log('      跳过(需真人下单/加购)'); await sleep(rand(3000, 6000)); continue; }
      // 1) 取 userTaskId：优先用列表预置值(浏览任务已激活)，否则 getTask 领取
      let ut = ut0;
      if (!ut) {
        const gt = await callSheep(SHEEP_BASE + '/withSign/getTask', { feedSheepId: FSID, taskId: t.taskId, subscribeMsg: '0', unionid: '', openid: '' }, cookie, cid, sid);
        ut = (gt.data && gt.data.userTaskId) || '';
        log('      getTask:', gt.code, gt.msg || '', ut ? '(utid=' + ut + ')' : '');
        if (!ut) { log('      无法领取(userTaskId 空)，跳过'); await sleep(rand(2500, 5000)); continue; }
        await sleep(rand(2000, 5000));
      }
      // 参与任务的浏览类需真实停留 browseTime 秒，与 commonTask 一致留足间隔
      if (BROWSE_TYPES.has(t.taskType) || (t.extJson && /browseTime/.test(t.extJson))) {
        await sleep(rand(22000, 30000));
      }
      // 2) 标记任务/浏览完成（通用任务组件在浏览满 browseTime 秒后调 finishTask）
      const ft = await callSheep(SHEEP_BASE + '/withSign/finishTask', { feedSheepId: FSID, taskId: t.taskId, userTaskId: ut }, cookie, cid, sid);
      // 完成判定：对齐 commonTask 的成功码集合
      //   1    = 成功完成； 10052 = 当天已达上限/已达成(仍尝试领奖)； 30023 = 已达成(同成功)
      const ftOk = (ft.code === 1 || ft.code === 10052 || ft.code === 30023);
      if (!ftOk) {
        log(`      finishTask 失败 ${ft.code}/${ft.msg || ''}（该任务可能需真人操作：如收藏商品/打开指定页面），跳过领奖`);
        await sleep(rand(2500, 5000));
        continue;
      }
      if (ft.code !== 1) { log(`      当天已完成/已达上限(code=${ft.code})，尝试领奖(若已领则略过)`); }
      await sleep(rand(2000, 5000));
      // 3) 领奖
      const aw = await callSheep(SHEEP_BASE + '/withSign/getAward', { feedSheepId: FSID, taskId: t.taskId, userTaskId: ut }, cookie, cid, sid);
      // 领取成功码：1=成功；10053/30014=当天已领过奖(达最大领奖次数)，属正常已完成，不计失败
      const AW_DONE = new Set([1, 10053, 30014]);
      if (AW_DONE.has(aw.code)) {
        log(`      领取${aw.code === 1 ? '✅ 草料+' + ((aw.data && aw.data.receiveAwardNum) || '?') : '✅ 已达成(今日已领过，草料+' + ((aw.data && aw.data.receiveAwardNum) || '0') + ')'}`);
      } else {
        log(`      领取失败 ${aw.code}/${aw.msg || ''}`);
      }
      await sleep(rand(3000, 7000));
    }
    await sleep(rand(3000, 6000));
  }
}

// 喂草料：先送草(收草料)，再把草料一口口喂给小羊(喂满为止)。
// 小羊外出冒险时 feed 会被拦截(10040)，属正常周期，优雅跳过。
async function feedGrass(cookie, cid, sid, FSID, adventureText) {
  log(`\n--- 喂草料 (送草 + 喂羊) ---`);
  const g = await callSheep(SHEEP_BASE + '/sendGrass', { feedSheepId: FSID, buserConfigId: '1375', sceneCode: 'feed', version: '5' }, cookie, cid, sid);
  log('  送草 sendGrass:', g.code, g.msg || '', g.data ? ('flag=' + g.data.flag) : '');
  await sleep(rand(3000, 6000));
  let reached = false, adventure = false;
  for (let k = 0; k < 12 && !reached; k++) {
    const f = await callSheep(SHEEP_BASE + '/withSign/feed', { feedSheepId: FSID, isTimeLimit: '0', version: '5' }, cookie, cid, sid);
    if (f.code === 10040) { adventure = true; log(`    ⏸️ 小羊正在冒险中（${adventureText || '外出中'}），暂停喂养，等小羊回来再跑本脚本即可。`); break; }
    log(`    喂羊#${k + 1}:`, f.code, f.msg || '', f.data ? ('currentFeed=' + f.data.currentFeedCount + ' isReachMax=' + f.data.isReachMaxFeedCount + ' leftFeed=' + f.data.leftFeedCount + ' canCut=' + f.data.canCutWoolAmount) : '');
    if (f.code === 1 && f.data && f.data.isReachMaxFeedCount === 1) { reached = true; log('    ✅ 喂羊已喂满'); }
    else if (f.code !== 1) {
      const m = f.msg || '';
      if (/草料不足|去探险|草料不够|草料为0|不足/.test(m)) {
        // 草料不足：派小羊去探险获取草料（探险归来后由 finishAdventure 领奖恢复草料）
        log('    🍃 草料不足，派小羊去探险以获取草料...');
        const sa = await callSheep(SHEEP_BASE + '/withSign/startAdventure', { feedSheepId: FSID }, cookie, cid, sid);
        if (sa.code === 1) { adventure = true; log('    ✅ 已发起探险: ' + (sa.msg || '') + (sa.data ? (' ' + JSON.stringify(sa.data).slice(0, 120)) : '')); }
        else { log('    ⚠️ 发起探险失败: ' + sa.code + '/' + (sa.msg || '')); }
      } else {
        log('    ⚠️ 喂羊异常:', m);
      }
      break;
    }
    await sleep(rand(4000, 9000));
  }
  return { adventure };
}

// 剪羊毛：剪羊毛得金币(羊毛)。小羊冒险时由调用方跳过。
async function doCutWool(cookie, cid, sid, FSID) {
  log(`\n--- 剪羊毛 (剪羊毛得金币) ---`);
  let totalCut = 0;
  for (let k = 0; k < 8; k++) {
    const c = await callSheep(SHEEP_BASE + '/withSign/cutWool', { feedSheepId: FSID }, cookie, cid, sid);
    if (c.code === 1 && c.data) { log(`    剪羊毛#${k + 1}: ✅ woolAmount=+${c.data.woolAmount} 累计total=${c.data.totalWoolAmount}`); totalCut += c.data.woolAmount || 0; }
    else { log(`    剪羊毛#${k + 1}:`, c.code, c.msg || (c.data ? JSON.stringify(c.data).slice(0, 80) : '')); if (c.code !== 1) break; }
    await sleep(rand(3000, 7000));
  }
  log('  本次剪羊毛共得金币(羊毛):', totalCut);
  return totalCut;
}

// 探险处理：小羊归来则领取探险奖励（恢复草料）；仍在探险中则跳过喂养/剪毛，等回来再跑。
// sheepStatus=5 表示探险中；adventureShowText 为归来提示（如“傍晚回来”）。
async function handleAdventure(cookie, cid, sid, FSID, d) {
  const st = d.sheepStatus;
  // 1) 小羊已归来（不在探险中）→ 尝试领取探险奖励
  if (st !== 5) {
    const fa = await callSheep(SHEEP_BASE + '/withSign/finishAdventure', { feedSheepId: FSID }, cookie, cid, sid);
    if (fa.code === 1) { log('  🎁 探险归来，领取奖励: ' + (fa.msg || '') + (fa.data ? (' ' + JSON.stringify(fa.data).slice(0, 160)) : '')); }
    else if (fa.code && fa.code !== -1) { log('  ℹ️ 领取探险奖励返回: ' + fa.code + '/' + (fa.msg || '')); }
    return { adventuring: false };
  }
  // 2) 仍在探险中：本轮跳过喂养与剪毛
  log('  ⏳ 小羊正在探险（' + (d.adventureShowText || '外出中') + '），尚未归来；本轮跳过喂养与剪毛，等回来后再跑即可。');
  return { adventuring: true };
}

async function processSheep(cookie, cid, sid) {
  log(`\n########## feedSheep (天天剪羊毛: 赚草料 + 喂草料 + 剪羊毛 + 探险) ##########`);
  const info = await callSheep(SHEEP_BASE + '/withSign/info', {}, cookie, cid, sid);
  if (info.code !== 1) { log('info 失败:', info.code, info.msg); return; }
  const d = info.data || {}; const FSID = d.feedSheepId;
  log(`牧场: feedSheepId=${FSID} 有羊=${d.hasSheep} 今日已喂=${d.currentFeedCount} 喂满=${d.isReachMaxFeedCount} 可剪羊毛=${d.canCutWoolAmount} leftFeedCount=${d.leftFeedCount} sheepStatus=${d.sheepStatus} 累计草料=${d.totalGrassAmount}`);
  await sleep(rand(2000, 4000));
  // 0) 探险：先领取已归来的探险奖励（恢复草料），探险中则本轮跳过
  const adv = await handleAdventure(cookie, cid, sid, FSID, d);
  if (adv.adventuring) { log('  ⏸️ 小羊探险中，跳过赚草料/喂养/剪毛。'); return; }
  // 1) 赚草料：完成任务列表领取草料奖励
  await earnGrass(cookie, cid, sid, FSID);
  // 2) 喂草料：送草 + 喂羊（草料不足会自动派小羊去探险）
  const fRes = await feedGrass(cookie, cid, sid, FSID, d.adventureShowText);
  // 3) 剪羊毛：小羊冒险时跳过（剪羊毛同会被拦截）
  if (fRes.adventure) { log('  ⏸️ 小羊冒险中，跳过剪羊毛（剪羊毛同会被拦截）。'); }
  else { await doCutWool(cookie, cid, sid, FSID); }
  // 复盘
  const info2 = await callSheep(SHEEP_BASE + '/withSign/info', {}, cookie, cid, sid);
  if (info2.code === 1) { const d2 = info2.data || {}; log(`复盘: 今日已喂=${d2.currentFeedCount} 喂满=${d2.isReachMaxFeedCount} 可剪羊毛=${d2.canCutWoolAmount} 累计羊毛=${d2.totalWoolAmount} sheepStatus=${d2.sheepStatus}`); }
}

/* ===================== 2g) 兔兔爱消消 (happyCrush 消消乐) ===================== */
// 玩法：消消乐小游戏。签到有礼领取体力；完成关卡任务产出金币/钻石等，其奖励（含魔力扫把等道具）
// 进入兔兔爱合成「道具仓库」，供 mergeBags 用 /recycleGoods 清格。
// 接口(base=https://act-ug.vip.com/happyCrush, 同域同签名机制，复用 callApi)：
//   /mapInfo      查地图: physicalNum=当前体力, stageList=关卡(stageStatus:1 可玩/可重刷), diamondNum/starNum
//   /stageInfo    查某关奖励(awardList)与初始棋盘
//   /signInInfo   签到状态(isSignInForDay); /signInExec(bussCode=happyCrush_sign_in) 领体力(awardType=20)
//   /startGame    开始关卡(stageId) → 返回 gameId
//   /finishGame   提交通关(gameId + 已解棋盘 gamePanel + score + actualUsageSteps) → 返回 awardList
// 说明：finishGame 必须提交一份「已解棋盘」。这里重放抓包得到的、已通关关卡(stageId=JdsVm7Fua5s)的棋盘
// 作为自动通关的固定解；服务端按 score 结算发奖（实测返回 金币/钻石 等），重刷同一关即可持续产出。
const CRUSH_BASE = 'https://act-ug.vip.com/happyCrush';
const CRUSH_ACT = 'H3gRnE1Xi18';       // happyCrush 游戏 actId（mapInfo/startGame/finishGame 用）
const CRUSH_SIGN_ACT = 'Qw-3bbI0kSY';  // happyCrush 签到 actId（signInInfo/signInExec 用）
const CRUSH_TASK_ACT = 'pBZZJDC5foY';  // happyCrush「赚体力」入口背后的 commonTask 任务集（HAR 抓到的全局浏览任务集，type=2/4/42/43；奖励积分/体力，跑一次看日志确认）
const CRUSH_STAGE_ID = 'JdsVm7Fua5s';  // 已抓取的可重玩关卡（stageNo=2，已 3 星仍可重刷产出）
const CRUSH_REPLAY_SCORE = 5460, CRUSH_REPLAY_STEPS = 20;
// 抓包得到的该关卡「已解棋盘」（重放用，原样作为字符串提交）
const CRUSH_REPLAY_PANEL = `[[{"x":1,"y":1},{"x":2,"y":1},{"x":3,"y":1},{"x":4,"y":1,"type1":1},{"x":5,"y":1,"type1":3},{"x":6,"y":1},{"x":7,"y":1},{"x":8,"y":1}],[{"x":1,"y":2},{"x":2,"y":2},{"x":3,"y":2,"type1":3},{"x":4,"y":2,"type1":5},{"x":5,"y":2,"type1":3},{"x":6,"y":2,"type1":2},{"x":7,"y":2},{"x":8,"y":2}],[{"x":1,"y":3},{"x":2,"y":3,"type1":1},{"x":3,"y":3,"type1":2},{"x":4,"y":3,"type1":2},{"x":5,"y":3,"type1":1},{"x":6,"y":3,"type1":2},{"x":7,"y":3,"type1":3},{"x":8,"y":3}],[{"x":1,"y":4,"type1":3},{"x":2,"y":4,"type1":3},{"x":3,"y":4,"type1":1},{"x":4,"y":4,"type1":2},{"x":5,"y":4,"type1":5},{"x":6,"y":4,"type1":1},{"x":7,"y":4,"type1":5},{"x":8,"y":4,"type1":3}],[{"x":1,"y":5},{"x":2,"y":5,"type1":2},{"x":3,"y":5,"type1":3},{"x":4,"y":5,"type1":5},{"x":5,"y":5,"type1":3},{"x":6,"y":5,"type1":3},{"x":7,"y":5,"type1":5},{"x":8,"y":5}],[{"x":1,"y":6},{"x":2,"y":6},{"x":3,"y":6,"type1":3},{"x":4,"y":6,"type1":5},{"x":5,"y":6,"type1":3},{"x":6,"y":6,"type1":5},{"x":7,"y":6},{"x":8,"y":6}],[{"x":1,"y":7},{"x":2,"y":7},{"x":3,"y":7},{"x":4,"y":7,"type1":2},{"x":5,"y":7,"type1":2},{"x":6,"y":7},{"x":7,"y":7},{"x":8,"y":7}],[{"x":1,"y":8},{"x":2,"y":8},{"x":3,"y":8},{"x":4,"y":8},{"x":5,"y":8},{"x":6,"y":8},{"x":7,"y":8},{"x":8,"y":8}]]`;
function mrCrush(cid) {
  return { source_app: 'app', client_type: '', app_name: 'shop_iphone', client: 'iphone', api_key: API_KEY, app_version: '9.79.8', mobile_platform: '3', mobile_channel: 'ng00010v:al80ssgp:37u8zn0w:ng00010p', mars_cid: cid, warehouse: 'VIP_NH', fdc_area_id: '911101114112', province_id: '911101114112', wap_consumer: 'A1', openid: '', unionid: '' };
}
async function callCrush(cookie, cid, sid, ep, data, actId) {
  const base = Object.assign(mrCrush(cid), { actId: actId || CRUSH_ACT });
  return callApi(CRUSH_BASE, ep, data || {}, cookie, cid, sid, API_KEY, base);
}
function crushAwardName(t) {
  return ({ 1: '金币', 2: '钻石', 4: '道具', 16: '红包', 17: '券', 18: '金币包', 19: '其他', 20: '体力' })[t] || ('类型' + t);
}
// 从关卡/接口返回里解析三星分数线。scoreRange 形如 "一星,二星,三星"（也可能叫 scoreLine），
// 取最后一段(三星线)；+ 余量确保评 3 星。返回数值或 null。
function crushStarLine(obj) {
  if (!obj) return null;
  const cand = obj.scoreRange || obj.scoreLine ||
    (obj.stageInfo && (obj.stageInfo.scoreRange || obj.stageInfo.scoreLine)) ||
    (obj.data && (obj.data.scoreRange || obj.data.scoreLine));
  if (!cand) return null;
  const p = String(cand).split(',').map((x) => Number(x));
  const t = [p[2], p[1], p[0]].find((v) => v != null && !isNaN(v) && v > 0);
  return t != null ? Number(t) : null;
}
async function processHappyCrush(cookie, cid, sid) {
  log(`\n########## 兔兔爱消消 (happyCrush 消消乐) ##########`);
  // 1) 领体力：签到（bussCode=happyCrush_sign_in）
  const si = await callCrush(cookie, cid, sid, '/signInInfo', { bussCode: 'happyCrush_sign_in' }, CRUSH_SIGN_ACT);
  if (si.code === 1 && si.data) {
    const bi = si.data.basicInfo || {};
    const sgi = si.data.signInInfo || {};
    const signed = bi.isSignInForDay === 1 || sgi.todaySinged === 1;
    if (!signed) {
      // 真人会先浏览签到页几秒再点
      await sleep(rand(8000, 16000));
      const se = await callCrush(cookie, cid, sid, '/signInExec', { bussCode: 'happyCrush_sign_in', isReSign: 0 }, CRUSH_SIGN_ACT);
      if (se.code === 1) {
        const oa = (se.data && se.data.otherAward) || se.data || {};
        log(`  ✅ 签到领体力成功 (+${oa.amount || (oa.awardNum) || '?'} ${crushAwardName(oa.awardType)}，awardType=${oa.awardType})`);
      } else if (se.code === 30022) log('  ✅ 今日已签到 (30022)');
      else log('  ⚠️ 签到失败:', se.code, se.msg || '');
    } else log('  ✅ 今日已签到，跳过');
  } else log('  ⚠️ signInInfo 失败:', si.code, si.msg || '');
  // 1.5) 做任务领体力（commonTask 体系：浏览/看视频类任务 → 领奖励，产出体力/积分）
  await nap(5000, 10000); // 签到后缓一下再进任务页
  await processCommon(cookie, cid, sid, API_KEY, CRUSH_TASK_ACT, '', 'happyCrush 做任务领体力');
  // 2) 查体力与关卡地图
  await nap(5000, 10000);
  const map = await callCrush(cookie, cid, sid, '/mapInfo', {});
  if (map.code !== 1 || !map.data) { log('  ⚠️ mapInfo 失败:', map.code, map.msg || ''); return; }
  // 防风控：服务端风控标记（riskControlFlag≠0 表示已被风控，立即停止避免加重）
  const rcFlag = map.data.riskControlFlag;
  if (rcFlag !== undefined && rcFlag !== '0' && rcFlag !== 0) {
    log(`  🛑 触发风控 (riskControlFlag=${rcFlag})，立即停止本账号 happyCrush 操作`);
    return;
  }
  let physical = Number(map.data.physicalNum || 0);
  log(`  💪 当前体力=${physical} 最高关卡=${map.data.maxStageNum || 0} (钻石${map.data.diamondNum || 0} 星星${map.data.starNum || 0})`);
  if (physical <= 0) { log('  ℹ️ 体力为0，无法自动闯关（签到外无其他体力来源，明天再来）'); return; }
  // 收集所有可玩(stageStatus=1)且未达三星(stageStarNum<3)的关卡，稍后补刷三星
  const allStages = [];
  for (const u of (map.data.unitList || [])) for (const s of (u.stageList || [])) allStages.push(s);
  const needStar = allStages.filter((s) => s.stageStatus === 1 && Number(s.stageStarNum || 0) < 3);
  log(`  🌟 地图中共 ${allStages.length} 关，需补三星 ${needStar.length} 关`);
  // 2.5) 补三星：重玩未达三星(stageStarNum<3)的可玩关卡。
  // 关键修正：每关三星分数线不同。接口在 startGame/stageInfo 返回 scoreRange("一星,二星,三星线")，
  // 分数 ≥ 最后一段即评 3 星；原先固定 5460 对三星线≥5460 的关只够 2 星，故补星失败。
  // 故此处按该关 scoreRange 的三星线 + 余量动态提交分数 → 真正补到 3 星。
  // 30007 = 该关当日重玩次数/今日游戏次数耗尽(与 physicalNum 无关)，跳过该关(明日再补)；
  //   连续 3 关 30007 判定今日次数已耗尽，整体停止补星。-3/11001 = 冷却/限流，退避重试。
  let noMorePlay = false; // 今日次数耗尽/风控，整体停止补星
  let noPlayStreak = 0;   // 连续 30007 计数(判定今日次数耗尽)
  for (const st of needStar) {
    if (noMorePlay || physical <= 2) { log('  🔋 体力/次数将尽（留 2 点余量），停止补星'); break; }
    await nap(6000, 15000);
    if (Math.random() < 0.12) { const lp = rand(30000, 90000); log(`  ⏸️ 模拟分心，暂停 ${(lp / 1000).toFixed(0)}s`); await napMs(lp); }
    const sg = await callCrush(cookie, cid, sid, '/startGame', { stageId: st.stageId });
    if (sg.code !== 1 || !sg.data || !sg.data.gameId) {
      if (sg.code === 30007) {
        log(`  ⏭️ 补星「${st.stageNo}关」今日不可重玩(30007)，跳过(明日再补)`);
        if (++noPlayStreak >= 3) { log('  🔋 连续多关 30007，判定今日重玩次数已耗尽，停止补星'); noMorePlay = true; }
        continue;
      }
      noPlayStreak = 0;
      if (sg.code === -3 || sg.code === 11001) { await sleep(rand(8000, 15000)); continue; } // 冷却/限流退避
      log(`  ⚠️ 补星 startGame「${st.stageNo}关」失败:`, sg.code, sg.msg || ''); continue;
    }
    noPlayStreak = 0; // 成功 startGame，重置耗尽计数
    // 该关三星分数线三级兜底：① mapInfo 关卡对象自带 → ② startGame 返回 → ③ stageInfo 接口；
    // 取最后一段(三星线) + 200 余量确保评 3 星。都取不到时回退到环境变量 WPH_CRUSH_STARSCORE，
    // 再不行才沿用固定分（多半不足三星）。
    let starLine = null, rangeSrc = '';
    if (st.scoreRange) { const t = crushStarLine(st); if (t) { starLine = t + 200; rangeSrc = 'mapInfo'; } }
    if (!starLine && sg.data) { const t = crushStarLine(sg.data); if (t) { starLine = t + 200; rangeSrc = 'startGame'; } }
    if (!starLine) {
      const sinfo = await callCrush(cookie, cid, sid, '/stageInfo', { stageId: st.stageId });
      if (sinfo.code === 1) { const t = crushStarLine(sinfo.data); if (t) { starLine = t + 200; rangeSrc = 'stageInfo'; } }
    }
    if (!starLine) {
      const envLine = Number(process.env.WPH_CRUSH_STARSCORE);
      if (envLine > 0) { starLine = envLine; rangeSrc = 'env:WPH_CRUSH_STARSCORE'; }
      else { starLine = CRUSH_REPLAY_SCORE; rangeSrc = '固定分(未取到scoreRange)'; log(`  ⚠️ 未取到 scoreRange，沿用固定分 ${CRUSH_REPLAY_SCORE}（多半不足三星，可设 WPH_CRUSH_STARSCORE 或开 WPH_DEBUG 排查）`); }
    }
    const gotRange = rangeSrc !== '固定分(未取到scoreRange)';
    if (process.env.WPH_DEBUG) log(`  🔍 三星线=${starLine} 来源=${rangeSrc} | stageKeys=${Object.keys(st).join(',')}`);
    await nap(3000, 8000);
    const fg = await callCrush(cookie, cid, sid, '/finishGame', {
      gameId: sg.data.gameId, gamePanel: CRUSH_REPLAY_PANEL, score: starLine,
      actualUsageSteps: CRUSH_REPLAY_STEPS, currentScoure: starLine
    });
    physical -= 1; // 重玩扣 1 体力
    if (fg.code !== 1) {
      if (fg.code === 11001 || fg.code === -3) { await sleep(rand(8000, 15000)); physical += 1; continue; } // 限流/冷却未消耗，退回重试
      log(`  ⚠️ 补星 finishGame「${st.stageNo}关」失败:`, fg.code, fg.msg || ''); continue;
    }
    // 回查星级是否到 3
    const m3 = await callCrush(cookie, cid, sid, '/mapInfo', {});
    if (m3.code === 1 && m3.data) {
      const rc3 = m3.data.riskControlFlag;
      if (rc3 !== undefined && rc3 !== '0' && rc3 !== 0) { log(`  🛑 触发风控 (riskControlFlag=${rc3})，立即停止`); noMorePlay = true; physical = 0; break; }
      let nowStar = 0;
      for (const u of (m3.data.unitList || [])) { const f = (u.stageList || []).find((x) => x.stageId === st.stageId); if (f) { nowStar = Number(f.stageStarNum || 0); break; } }
      if (nowStar >= 3) log(`  ⭐ 补星成功：「${st.stageNo}关」达成 3 星（提交分 ${starLine}${gotRange ? '' : '，未取到分数线'}）`);
      else log(`  ➰ 「${st.stageNo}关」重玩后 ${nowStar} 星（提交分 ${starLine}/三星线${gotRange ? '' : '未知'}，仍未满，明日再补）`);
      physical = Number(m3.data.physicalNum || physical);
    }
    if (physical <= 0) { noMorePlay = true; break; }
  }
  // 3) 自动闯关：重放已通关关卡棋盘（持续产出金币/钻石/道具）
  const MAX_PLAYS = Math.min(physical, 12); // 单次最多刷 12 关，防风控
  let plays = 0, okPlays = 0, failStreak = 0;
  while (plays < MAX_PLAYS) {
    await nap(6000, 15000); // 真人每关之间会停几到十几秒
    // 偶发"长停顿"模拟分心离开，打破固定节奏（防风控）
    if (Math.random() < 0.12) { const lp = rand(30000, 90000); log(`  ⏸️ 模拟分心，暂停 ${(lp/1000).toFixed(0)}s`); await napMs(lp); }
    const sg = await callCrush(cookie, cid, sid, '/startGame', { stageId: CRUSH_STAGE_ID });
    if (sg.code !== 1 || !sg.data || !sg.data.gameId) {
      log('  ⚠️ startGame 失败:', sg.code, sg.msg || '');
      failStreak++;
      if (sg.code === 11001) { await sleep(rand(8000, 15000)); continue; } // 11001 多为瞬时限流，退避重试
      if (failStreak >= 3) { log('  🛑 连续失败 3 次，熔断停止（防风控）'); break; }
      continue;
    }
    // 关卡内思考/操作停顿（真人不会 start 完立刻 finish）
    await nap(3000, 8000);
    const fg = await callCrush(cookie, cid, sid, '/finishGame', {
      gameId: sg.data.gameId, gamePanel: CRUSH_REPLAY_PANEL, score: CRUSH_REPLAY_SCORE,
      actualUsageSteps: CRUSH_REPLAY_STEPS, currentScoure: CRUSH_REPLAY_SCORE
    });
    plays++;
    if (fg.code === 1 && fg.data && fg.data.awardList) {
      okPlays++; failStreak = 0;
      const aw = fg.data.awardList.map((x) => `${crushAwardName(x.awardType)}×${x.awardNum}`).join('、');
      log(`  ✅ 闯关 #${plays} 成功 → 奖励[${aw}]`);
    } else {
      log(`  ⚠️ finishGame 失败 #${plays}:`, fg.code, fg.msg || JSON.stringify(fg.data || '').slice(0, 200));
      failStreak++;
      if (fg.code === 11001) { await sleep(rand(8000, 15000)); continue; }
      if (failStreak >= 3) { log('  🛑 连续失败 3 次，熔断停止（防风控）'); break; }
      continue;
    }
    await nap(3000, 8000);
    const m2 = await callCrush(cookie, cid, sid, '/mapInfo', {});       // 刷新体力（startGame 一般扣 1）
    if (m2.code === 1 && m2.data) {
      // 每关后复查风控标记
      const rc2 = m2.data.riskControlFlag;
      if (rc2 !== undefined && rc2 !== '0' && rc2 !== 0) { log(`  🛑 触发风控 (riskControlFlag=${rc2})，立即停止`); break; }
      physical = Number(m2.data.physicalNum || 0);
    } else physical -= 1;
    if (physical <= 2) { log('  🔋 体力将尽（留 2 点余量，避免刷空被识别为脚本），停止闯关'); break; }
  }
  log(`  —— 兔兔爱消消：闯关 ${okPlays}/${plays} 成功`);
}

// 单账号跑全套任务（commonTask + checkRoom + 合成 + feedSheep + signIn）
/* ===================== 等级红包/津贴兑换 (commonExchange) =====================
 * 你所说的"兑换红包"就是这里：兔兔爱合成的【兑换商城】(commonExchange)。
 * 它和 lotteryDraw(幸运红包九宫格 luckylottery/withSign) 完全是两套，不要混淆。
 * 商城里 exchangeType=4 是按等级兑换的现金津贴档位：
 *   2级→0.1元 / 20级→5元 / 30级→20元 / 40级→50元 / 50级→100元（每级各有包包/鞋子/衣服3档）。
 * 列表接口 getExchangeList(exchangeType=4)；兑换提交接口 exchange，参数 exchangeConfigId。
 * 注意：commonExchange 用 wap_consumer=C2-1-2（与普通 commonTask 的 C2-4-2 不同）。
 * 安全性：兑换接口服务端会校验"是否已合成对应等级包包"，条件不足返回 10070 且不扣款，可放心调用。
 * 默认不执行，需设置 WPH_DO_EXCHANGE=1 才在 runAccount 末尾运行（真实消耗福卡换取现金津贴）。 */
const EXCHANGE_BASE = 'https://act-ug.vip.com/commonExchange';
const EXCHANGE_ACT = 'H3gRnE1Xi18';
function mrExchangeBase(cid, relateId) {
  return { source_app: 'app', client_type: 'wap', app_name: 'shop_iphone', client: 'iphone',
    app_version: '9.79.8', mobile_platform: '3', mobile_channel: 'ng00010v:al80ssgp:37u8zn0w:ng00010p',
    warehouse: 'VIP_NH', fdc_area_id: '911101114112', province_id: '911101114112', wap_consumer: 'C2-1-2',
    from: 'app', open_id: '', actId: EXCHANGE_ACT, relateId: relateId || '', time: '0', is_front: '1',
    app_theme_mode: '0', app_theme_action: '0', sd_tuijian: '1' };
}
// 解析单个档位：等级 / 需求描述 / 是否限量抢兑 / 状态文字
function parseExchangeItem(it) {
  let lv = '?', need = '';
  try {
    const c = JSON.parse(it.exchangeCondition || '[]');
    const f = c.find((x) => String(x.conditionType) === '1'); // conditionType=1: 需合成N级商品
    if (f) { lv = f.conditionValue; need = f.desc || `需合成${lv}级商品`; }
    const f2 = c.find((x) => String(x.conditionType) === '2'); // conditionType=2: 商品类型(包包/鞋子/衣服)
    if (f2 && f2.desc) need = (need ? need + ' · ' : '') + f2.desc;
  } catch (_) {}
  let ext = {};
  try { ext = JSON.parse(it.extJson || '{}'); } catch (_) {}
  const limited = String(ext.isShowStockLimitText) === '1' || ext.isShowStockLimitText === 1;
  const stock = parseInt(it.stock, 10);
  return { lv, need, limited, stock: isNaN(stock) ? -1 : stock,
    status: it.typeDesc || '', limitTxt: it.exchangeTips || '',
    name: it.exchangeName || '', cfgId: it.exchangeConfigId || '', consume: it.consumeAmount, subsidy: it.subsidyAmount };
}

// 只读预览：列出每账号可兑档位（仅调用 getExchangeList，不调用 exchange，不消耗福卡）
async function processExchangePreview(cookie, cid, sid) {
  log('\n########## 等级红包/津贴兑换【只读预览】(不消耗福卡) ##########');
  const relateId = await getCheckRoomId(cookie, cid, sid);
  if (!relateId) { log('  ❌ 未取到 checkRoomId'); return; }
  const base = mrExchangeBase(cid, relateId);
  const list = await callApi(EXCHANGE_BASE, '/getExchangeList', {}, cookie, cid, sid, API_KEY, base);
  if (!list || list.code !== 1 || !list.data) { log('  ❌ getExchangeList 失败', list && list.code, list && list.msg); return; }
  let items = [];
  for (const grp of (list.data.exchangeList || [])) if (grp.exchangeType === 4) for (const it of (grp.exchangeTypeList || [])) items.push(parseExchangeItem(it));
  items.sort((a, b) => parseInt(a.lv) - parseInt(b.lv));
  let cand = 0, done = 0, grabl = 0;
  for (const it of items) {
    const doneFlag = /已兑换|已用完|已达/.test(it.status);
    const tag = it.limited ? '⚡抢兑' : '    ';
    const st = doneFlag ? '已兑换' : '可兑(满足条件即兑)';
    if (doneFlag) done++; else { cand++; if (it.limited) grabl++; }
    log(`  ${tag} ${it.lv}级 ${it.name} | 福卡${it.consume}→津贴${it.subsidy}分 | ${it.limitTxt} | 库存${it.stock} | ${it.need} | [${st}]`);
  }
  log(`  📊 共 ${items.length} 档：未兑换候选 ${cand}（其中限量抢兑 ${grabl}） / 已兑换 ${done}`);
  log('  说明：预览只查列表、不消耗福卡；"可兑"指该档位当前未兑换，实际兑换仍由服务端校验「是否已合成对应等级商品」，未合成会返回 10070 且不扣款。');
}

// 单次兑换
function exchangeOnce(base, cookie, cid, sid, cfgId) {
  return callApi(EXCHANGE_BASE, '/exchange', { exchangeConfigId: cfgId }, cookie, cid, sid, API_KEY, base);
}

// 真实兑换：限量档位(40/50级包包)自动抢兑重试，其余单次尝试
async function processExchange(cookie, cid, sid) {
  log('\n########## 等级红包/津贴兑换 (commonExchange·真实兑换) ##########');
  const relateId = await getCheckRoomId(cookie, cid, sid);
  if (!relateId) { log('  ❌ 未取到 checkRoomId，跳过兑换'); return; }
  const base = mrExchangeBase(cid, relateId);
  const list = await callApi(EXCHANGE_BASE, '/getExchangeList', {}, cookie, cid, sid, API_KEY, base);
  if (!list || list.code !== 1 || !list.data) { log('  ❌ getExchangeList 失败', list && list.code, list && list.msg); return; }
  let items = [];
  for (const grp of (list.data.exchangeList || [])) if (grp.exchangeType === 4) for (const it of (grp.exchangeTypeList || [])) items.push(parseExchangeItem(it));
  // WPH_GRAB_ONLY=1：只抢 40/50 级高价值档位，跳过低价值的 2/20/30 级
  if (process.env.WPH_GRAB_ONLY) {
    const before = items.length;
    items = items.filter((it) => parseInt(it.lv, 10) >= 40);
    log(`  🎯 WPH_GRAB_ONLY 开启：跳过 ${before - items.length} 个低价值档位(2/20/30级)，仅保留 40/50 级（共 ${items.length} 档）`);
  }
  log(`  📋 共 ${items.length} 个津贴档位（按等级）`);
  const GRAB_MAX = parseInt(process.env.WPH_GRAB_TRIES) || 20; // 抢兑重试上限
  let ok = 0, skip = 0, fail = 0, grabOk = 0;
  for (const it of items) {
    await sleep(rand(1500, 3500));
    let done = false, res = null, tries = 0;
    const maxTries = it.limited ? GRAB_MAX : 1; // 限量档位抢兑重试
    while (!done && tries < maxTries) {
      tries++;
      res = await exchangeOnce(base, cookie, cid, sid, it.cfgId);
      if (res && res.code === 1) { done = true; ok++; if (it.limited) grabOk++; log(`  ✅ ${it.lv}级 ${it.name} 兑换成功! (第${tries}次) ${JSON.stringify(res.data).slice(0, 140)}`); }
      else if (res && res.code === 10070) { done = true; skip++; /* 条件未达：未合成对应等级商品，不扣款 */ }
      else if (res && (res.code === 10071 || /已兑换|今日已|上限|已用完/.test(res.msg || ''))) { done = true; skip++; log(`  ⏭️ ${it.lv}级 ${it.name} 已兑换/已达上限`); }
      else {
        const busy = /抢|光|库存|火爆|频繁|稍后|网络|busy|try|again/i.test((res && res.msg) || '');
        if (it.limited && busy) {
          if (tries < maxTries) { log(`  🔁 ${it.lv}级 ${it.name} 抢兑未成(code=${res && res.code} ${res && res.msg})，重试(${tries}/${maxTries})`); await sleep(rand(800, 2500)); continue; }
          done = true; fail++; log(`  ⚠️ ${it.lv}级 ${it.name} 抢兑 ${GRAB_MAX} 次仍失败 code=${res && res.code} msg=${res && res.msg}`);
        } else { done = true; fail++; log(`  ⚠️ ${it.lv}级 ${it.name} 兑换返回 code=${res && res.code} msg=${res && res.msg}`); }
      }
    }
  }
  log(`  💰 兑换完成：成功 ${ok}（抢兑成功 ${grabOk}） / 跳过(未达或已兑) ${skip} / 异常 ${fail}`);
}

async function runAccount(acc, idx, total) {
  const { cookie, cid, sid, name } = acc;
  const ctx = { name };
  await acctCtx.run(ctx, async () => {
  log(`\n################ 账号 ${idx}/${total}：${name} ################`);
  await processCommon(cookie, cid, sid, API_KEY, 'H3gRnE1Xi18', '3QA9JBmLAfKaKOAU4QLCfw', 'commonTask(兔兔/剪羊毛任务)');
  await nap(8000, 18000);
  await processCheckRoom(cookie, cid, sid);
  await nap(8000, 18000);
  await processHappyCrush(cookie, cid, sid);
  await nap(8000, 18000);
  await mergeBags(cookie, cid, sid);
  await nap(8000, 18000);
  await processRedPacket(cookie, cid, sid);
  await nap(8000, 18000);
  await processMapAward(cookie, cid, sid);
  await nap(8000, 18000);
  await processMining(cookie, cid, sid);
  await nap(8000, 18000);
  await processSheep(cookie, cid, sid);
  await nap(8000, 18000);
  await processSign(cookie, cid, sid);
  if (process.env.WPH_PREVIEW_EXCHANGE) {
    await sleep(rand(2000, 5000));
    await processExchangePreview(cookie, cid, sid);
  }
  if (process.env.WPH_DO_EXCHANGE) {
    await nap(5000, 10000);
    await processExchange(cookie, cid, sid);
  }
  // 拔萝卜在线时长任务：放在本账号所有任务(含模拟分心长停顿)都跑完之后。
  // 注：onlineTime 由唯品会 App/活动页端真实在线累计(脚本发 API 无法自刷)，故脚本能做的只是
  // “在真实运行时间最长、用户 App 累计在线最充分时尝试领取”——时间够了就领(30022 则留待下次)。
  await nap(8000, 18000);
  await processCheckRoom(cookie, cid, sid);
  log(`\n—— 账号「${name}」任务结束 ——`);
  });
}

async function main() {
  log('===== 唯品会 所有任务 (commonTask + checkRoom + 合成 + feedSheep + signIn) 多账号 =====');
  const accounts = await loadSessions();
  if (!accounts.length) { log('❌ 找不到有效会话：既无 wph_accounts.json / wph_cookie.json（H5 登录产物），HAR 里的 session 也全部过期。请先运行 `node wph_login.js` 登录，或重新导出微信会话 .har 文件。'); process.exit(1); }
  log(`共 ${accounts.length} 个有效账号`);
  const PARALLEL = Math.max(1, parseInt(process.env.WPH_PARALLEL) || 2); // 默认2：多账号自动并行(单号节奏不变、零风控风险)；如需串行设 WPH_PARALLEL=1
  // 幸运红包：九宫格随机抽奖。先预览奖品，默认直接抽(抽完当日次数)；设 WPH_REDPACKET 可指定 all/次数/skip，WPH_REDPACKET_ASK=1 恢复交互询问
  if (process.env.WPH_REDPACKET) {
    const v = String(process.env.WPH_REDPACKET).trim().toLowerCase();
    if (v === 'all') RP_CHOICE = 'all';
    else if (v === 'skip' || v === 'n') RP_CHOICE = 'skip';
    else if (/^\d+$/.test(v)) RP_CHOICE = Number(v);
    else RP_CHOICE = null;
    log(`🧧 红包抽奖选择(WPH_REDPACKET)=${v}`);
  } else if (accounts.length && process.stdin.isTTY) {
    const a0 = accounts[0];
    log(`\n🧧 正在预览「${a0.name}」可抢红包...`);
    const q = await queryRedpacket(a0.cookie, a0.cid, a0.sid);
    if (q && q.list.length) {
      log('🧧 幸运红包九宫格奖品（账号「' + a0.name + '」预览，抽奖为中到随机格）:');
      q.list.forEach((t, i) => log(`  [${i + 1}] ${t.name}（type=${t.type}）`));
      // 默认直接抽(抽完当日次数)；设 WPH_REDPACKET_ASK=1 才恢复交互询问次数
      if (process.env.WPH_REDPACKET_ASK) {
        RP_CHOICE = await askRedpacketChoice();
      } else {
        RP_CHOICE = 'all';
        log('🧧 直接抽(抽完当日次数)；如需手动选次数设 WPH_REDPACKET_ASK=1');
      }
    } else {
      RP_CHOICE = 'skip';
      log('🧧 该账号暂无可抢红包，跳过抢红包');
    }
  } else {
    RP_CHOICE = null; // 非交互（如重定向日志）：仅预览提示，不抢
  }
  if (process.env.WPH_PREVIEW_REDPACKET) {
    log('\n🧧 预览所有账号可抢红包（仅查询，不抢兑）:');
    for (const a of accounts) {
      log(`\n— 账号「${a.name}」—`);
      const q = await queryRedpacket(a.cookie, a.cid, a.sid);
      if (!q) { log('  未取到红包活动(luckyRedpacketActId)'); continue; }
      if (!q.list.length) { log('  暂无可抢红包任务'); continue; }
      q.list.forEach((t, i) => log(`  [${i + 1}] ${JSON.stringify(t).slice(0, 700)}`));
      log('  📦 getAwardList.data 全量(看剩余次数/活动状态): ' + JSON.stringify(q.data).slice(0, 1000));
    }
    log('\n🧧 预览结束（未执行抢兑）');
    return;
  }
  if (PARALLEL > 1) {
    log(`并行模式：每批 ${PARALLEL} 个账号（WPH_PARALLEL=${PARALLEL}）`);
    for (let i = 0; i < accounts.length; i += PARALLEL) {
      const chunk = accounts.slice(i, i + PARALLEL);
      await Promise.all(chunk.map((acc, k) => runAccount(acc, i + k + 1, accounts.length)));
      if (i + PARALLEL < accounts.length) await sleep(rand(20000, 40000));
    }
  } else {
    for (let i = 0; i < accounts.length; i++) {
      await runAccount(accounts[i], i + 1, accounts.length);
      if (i < accounts.length - 1) await nap(5000, 10000);
    }
  }
  log('\n===== 全部账号任务完成 =====');
  await drainLog(); // 确保所有日志（含并行账号的最后一块缓冲）在进程退出前按序落盘
}
return { main, runAccount, gatherAccounts, processSheep, earnGrass, feedGrass, doCutWool, processCheckRoom, mergeBags, processHappyCrush, processMining, processMapAward, getMiningActId, loadSessions, loadSession, processCommon, processSign, processExchange, processExchangePreview, callApi, callGame, callSheep, SHEEP_BASE, mrSheep, callCrush, CRUSH_BASE, CRUSH_ACT, CRUSH_STAGE_ID, CRUSH_REPLAY_SCORE, CRUSH_REPLAY_STEPS, CRUSH_REPLAY_PANEL, mrSign, SIGN_BASE, API_KEY, TFS, baseParams, CRID, RUN_LOG };

})();

// ================== 青龙通知推送 ==================
function getToken(url, cid, secret) {
  return new Promise((resolve, reject) => {
    const qs = '?client_id=' + encodeURIComponent(cid) + '&client_secret=' + encodeURIComponent(secret);
    const req = (url.startsWith('https') ? https : http).get(url.replace(/\/$/, '') + '/open/auth/token' + qs, (res) => {
      let d = '';
      res.on('data', (c) => (d += c));
      res.on('end', () => { try { const j = JSON.parse(d); resolve(j && j.data && j.data.token); } catch (e) { reject(e); } });
    });
    req.on('error', reject);
  });
}
function push(url, token, title, content) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ title: title, content: String(content || '') });
    const u = new URL(url.replace(/\/$/, '') + '/open/notify');
    const opt = {
      method: 'POST',
      hostname: u.hostname,
      port: u.port || (url.startsWith('https') ? 443 : 80),
      path: u.pathname,
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token, 'Accept': 'application/json' }
    };
    const req = (url.startsWith('https') ? https : http).request(opt, (res) => { let d = ''; res.on('data', (c) => (d += c)); res.on('end', () => resolve(d)); });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}
async function sendNotify(title, content) {
  const { QL_URL, QL_CLIENT_ID, QL_CLIENT_SECRET } = process.env;
  if (!QL_URL || !QL_CLIENT_ID || !QL_CLIENT_SECRET) {
    console.log('ℹ️ 未配置青龙推送，跳过通知');
    return;
  }
  try {
    const token = await getToken(QL_URL, QL_CLIENT_ID, QL_CLIENT_SECRET);
    if (!token) { console.log('⚠️ 青龙 token 获取失败'); return; }
    await push(QL_URL, token, title, content);
    console.log('✅ 青龙通知已发送');
  } catch (e) {
    console.log('⚠️ 通知发送失败:', e && e.message ? e.message : e);
  }
}

// ================== 主流程 ==================
(async () => {
  try {
    await api.main();
  } catch (e) {
    console.error('FATAL', e);
    if (api.RUN_LOG) api.RUN_LOG.push('FATAL ' + (e && e.stack ? e.stack : e));
  } finally {
    try { await sendNotify('唯品会自动任务', api.RUN_LOG ? api.RUN_LOG.join('\n') : '（无输出）'); } catch (_) {}
  }
})();
