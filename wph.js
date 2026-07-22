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

// ================== 加载主脚本 ==================
// 优先同目录 wph_all_api.js（qinglong/ 目录自包含部署）；
// 回退父目录（整仓订阅部署，如 ql repo 拉库）
let api;
const localApi = path.join(__dirname, 'lib', 'wph_all_api.js');
const parentApi = path.join(__dirname, '..', 'qinglong', 'lib', 'wph_all_api.js');
if (fs.existsSync(localApi)) {
  api = require(localApi);
} else if (fs.existsSync(parentApi)) {
  api = require(parentApi);
} else {
  console.error('❌ 找不到 wph_all_api.js，请确保 lib/ 目录包含该文件或整仓订阅部署');
  process.exit(1);
}
const { main, RUN_LOG } = api;

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
    await main();
  } catch (e) {
    console.error('FATAL', e);
    if (RUN_LOG) RUN_LOG.push('FATAL ' + (e && e.stack ? e.stack : e));
  } finally {
    try { await sendNotify('唯品会自动任务', RUN_LOG ? RUN_LOG.join('\n') : '（无输出）'); } catch (_) {}
  }
})();
