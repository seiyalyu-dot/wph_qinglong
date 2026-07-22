# 唯品会自动任务 - 青龙面板独立入口

精简自包含，一个文件夹即可运行。

## 青龙拉库

青龙面板 → 订阅管理 → 新建订阅：

```bash
ql repo https://github.com/seiyalyu-dot/wph_qinglong.git "" "" "" "main"
```

## 快速配置

### 1. 装依赖

青龙面板 → 依赖管理 → NodeJS，添加 `crypto-js`。

> 如需自动续期还需装 `playwright`，青龙无浏览器环境可跳过。

### 2. 拿 Cookie

在本地电脑（能开浏览器）拉主仓库并登录：

```bash
git clone https://github.com/seiyalyu-dot/wph.git
cd wph
npm install
npm run login
```

会弹出手机浏览器 → 微信扫码/验证码登录 → 按回车保存。打开生成的 `wph_cookie.json`，复制 `cookie` 字段值。

### 3. 配环境变量

青龙面板 → 环境变量 → 新建：

| 名称 | 值 | 说明 |
|------|------|------|
| `WPH_COOKIE` | 第 2 步的 cookie 串 | **必填**，多账号换行分隔 |
| `WPH_AUTO_REFRESH` | `1` | **推荐**，自动续期 mars_sid |
| `WPH_REDPACKET` | `all` | 可选，自动抢红包 |
| `WPH_DO_EXCHANGE` | `1` | 可选，自动兑换等级红包 |

### 4. 建定时任务

青龙面板 → 定时任务 → 新建：

| 字段 | 值 |
|------|------|
| 命令 | `node wph.js` |
| 定时规则 | `0 9 * * *` |

## 全部环境变量

| 变量 | 说明 |
|------|------|
| `WPH_COOKIE` | 必填，Cookie 完整串，多账号换行分隔 |
| `WPH_ACCOUNTS` | JSON 数组格式精细管理多账号 |
| `WPH_AUTO_REFRESH` | `1`=自动续期 mars_sid（推荐） |
| `WPH_REDPACKET` | `all`/`skip`/`N`次 |
| `WPH_DO_EXCHANGE` | `1`=执行福卡兑换 |
| `WPH_PREVIEW_EXCHANGE` | `1`=仅预览兑换 |
| `WPH_GRAB_ONLY` | 兑换只抢高价值档位 |
| `WPH_PARALLEL` | 多账号并行数，默认 2 |
| `WPH_FAST` | 加速倍数，默认 1 |
| `WPH_DEBUG` | `1`=调试日志 |
| `WPH_FETCH_TIMEOUT` | 请求超时(ms)，默认 30000 |

> 通知推送：在青龙「系统设置 → 应用设置」创建应用，拿到凭证后设 `QL_URL`、`QL_CLIENT_ID`、`QL_CLIENT_SECRET` 三个环境变量。
