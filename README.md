# tg-watchbot

tg-watchbot 是一个轻量级 Python 服务，把 **Telegram 双向客服机器人** 和 **Web/RSS 监控推送** 合在一起：

- 普通用户私聊 Bot，消息会转发给管理员；
- 管理员可以直接回复、主动发文字/图片、封禁/备注用户；
- 后台定时监控 RSS 或网页，命中关键词、新条目、价格/库存变化后推送给管理员；
- 自带一个 Web 管理面板，可配置监控目标、编辑 YAML、查看收件箱和日志。

项目为单文件应用，适合个人服务器、NAT 小鸡、轻量 VPS 直接用 systemd 跑。

## 功能

### Telegram 双向机器人

- 使用官方 Telegram Bot API，不做 userbot/selfbot。
- `/start` 建立用户和管理员之间的联系。
- 用户消息先写入 SQLite，再转发给管理员，避免转发失败时丢消息。
- 管理员可通过“回复转发消息”直接回给原用户。
- 支持显式命令：
  - `/reply <user_id> <内容>`：给指定用户发文字；
  - `/sendpic <user_id> [说明]`：给指定用户发图片；
  - `/block <user_id>`：封禁用户；
  - `/unblock <user_id>`：解封用户；
  - `/note <user_id> <备注>`：给用户加备注；
  - `/who <user_id>`：查看用户信息；
  - `/cancel`：取消待发送图片。
- 普通用户有简单限流，防止刷屏。

### Web/RSS 监控

- 支持两类监控：
  - `rss`：解析 RSS/Atom 条目；
  - `web`：用 CSS selector 抓网页条目、标题、链接、价格、库存。
- 支持触发条件：
  - 关键词命中；
  - 新条目；
  - 价格变化；
  - 库存变化。
- 支持论坛 RSS 增强字段：作者、分类、tags、摘要。
- 支持去重，避免同一条反复推送。
- 支持屏蔽词、作者、分类过滤（YAML 高级配置）。
- 默认最低监控间隔为 60 秒。

### Web 管理面板

- 登录页 + HttpOnly session cookie，不使用丑陋的浏览器 Basic Auth。
- 监控列表、新增、编辑、删除、手动检查、预览。
- NodeSeek / Linux.do RSS 模板。
- 批量新增监控。
- YAML 高级编辑。
- Bot Token / 管理员 ID / 面板账号配置页。
- 收件箱页面，可查看用户消息和重试转发。
- 主动发消息页面 `/send`，发送成功后会在页面显示结果，并给管理员聊天发送确认提醒。
- 自动清理监控/RSS/网站状态数据；不会删除用户、收件箱、双向对话消息。
- 日志页面和健康检查 `/health`。

## 借鉴 / 使用的开源库

本项目的业务逻辑为自写，主要使用并参考了以下开源库的公开 API 和常见用法：

- [`aiogram`](https://github.com/aiogram/aiogram)：Telegram Bot API、命令、消息处理、复制/发送消息。
- [`FastAPI`](https://github.com/fastapi/fastapi)：Web 管理面板、表单、路由、中间件。
- [`Uvicorn`](https://github.com/encode/uvicorn)：ASGI 服务运行。
- [`APScheduler`](https://github.com/agronholm/apscheduler)：异步定时监控任务。
- [`httpx`](https://github.com/encode/httpx)：异步 HTTP 抓取。
- [`feedparser`](https://github.com/kurtmckee/feedparser)：RSS/Atom 解析。
- [`Beautiful Soup`](https://www.crummy.com/software/BeautifulSoup/)：HTML 解析和 CSS selector 抽取。
- [`PyYAML`](https://pyyaml.org/)：`config.yaml` 配置读写。
- [`python-dotenv`](https://github.com/theskumar/python-dotenv)：读取 `.env`。
- Python 标准库 `sqlite3`：消息、用户、去重、监控状态持久化。

## 安全 / 脱敏说明

- 仓库不包含真实 `TELEGRAM_BOT_TOKEN`、`ADMIN_CHAT_ID`、面板密码、Session Secret、数据库、日志。
- 真实配置放在 `.env`，不要提交。
- `.gitignore` 已忽略 `.env`、SQLite、日志、虚拟环境和缓存。
- 如果要把面板暴露到公网，建议使用 Cloudflare Access / 反代鉴权，并使用强密码。
- Bot 只能给“已经主动私聊过 Bot 的用户”发消息，这是 Telegram Bot API 的限制。

## 快速开始

```bash
git clone <YOUR_REPO_URL> tg-watchbot
cd tg-watchbot
python3 -m venv .venv
./.venv/bin/pip install -U pip
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
cp config.example.yaml config.yaml
nano .env
```

至少填写：

```dotenv
TELEGRAM_BOT_TOKEN=<your_bot_token>
ADMIN_CHAT_ID=<your_admin_chat_id>
WEB_PANEL_USER=admin
WEB_PANEL_PASSWORD=<strong_password>
```

启动：

```bash
./.venv/bin/python app.py
```

打开面板：

```text
http://127.0.0.1:8765
```

如果只是先配置面板、还没有 Telegram Token：

```bash
./.venv/bin/python app.py --panel-only
```

手动跑一次监控：

```bash
./.venv/bin/python app.py --run-once
```

## systemd 部署

推荐部署到 `/opt/tg-watchbot`：

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin tg-watchbot || true
sudo mkdir -p /opt/tg-watchbot
sudo chown -R "$USER:$USER" /opt/tg-watchbot

cd /opt/tg-watchbot
git clone <YOUR_REPO_URL> .
python3 -m venv .venv
./.venv/bin/pip install -U pip
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
cp config.example.yaml config.yaml
nano .env

sudo chown -R tg-watchbot:tg-watchbot /opt/tg-watchbot
sudo chmod 600 /opt/tg-watchbot/.env
sudo cp systemd/tg-watchbot.service /etc/systemd/system/tg-watchbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now tg-watchbot
sudo journalctl -u tg-watchbot -f
```

健康检查：

```bash
curl http://127.0.0.1:8765/health
```

## 配置说明

### `.env`

| 变量 | 说明 |
|---|---|
| `TELEGRAM_BOT_TOKEN` | BotFather 创建的 Telegram Bot Token |
| `ADMIN_CHAT_ID` | 管理员 Telegram 数字 chat id，用于接收用户消息和监控通知 |
| `LOG_LEVEL` | 日志级别，默认 `INFO` |
| `WEB_PANEL_ENABLED` | 是否启用 Web 面板，默认 `true` |
| `WEB_PANEL_HOST` | 面板监听地址，默认 `127.0.0.1` |
| `WEB_PANEL_PORT` | 面板端口，默认 `8765` |
| `WEB_PANEL_USER` | 面板用户名 |
| `WEB_PANEL_PASSWORD` | 面板密码 |
| `WEB_PANEL_SESSION_SECRET` | Session Secret，留空会自动生成并写回 `.env` |

### `config.yaml`

监控数据自动清理示例：

```yaml
cleanup:
  enabled: true
  interval_minutes: 60              # 每多少分钟执行一次清理
  monitor_retention_minutes: 1440   # RSS/网站监控状态保留多久
```

清理范围只包括：

- `monitor_state`：网站/RSS 条目状态、价格/库存状态；
- `sent_events`：监控推送去重记录。

不会删除：

- `users`；
- `message_map`；
- `inbox_messages`；
- 任何双向对话/客服消息记录。

RSS 示例：

```yaml
monitors:
  - name: NodeSeek 新帖
    type: rss
    url: https://rss.nodeseek.com/
    interval_seconds: 60
    keywords:
      - VPS
      - 优惠
    exclude_keywords:
      - 出号
    authors: []
    categories: []
    notify_on:
      keyword_match: true
      new_item: true
      price_change: false
      stock_change: false
    forum: true
```

网页示例：

```yaml
monitors:
  - name: Example Deals
    type: web
    url: https://example.com/deals
    interval_seconds: 300
    keywords:
      - discount
    selectors:
      item: article, .deal, li
      title: h1, h2, h3, a
      link: a
      price: .price
      stock: .stock
    notify_on:
      keyword_match: true
      new_item: true
      price_change: true
      stock_change: true
```

## 管理命令

管理员在 Telegram 里可用：

```text
/reply <user_id> <内容>
/sendpic <user_id> [图片说明]
/block <user_id>
/unblock <user_id>
/note <user_id> <备注>
/who <user_id>
/cancel
```

也可以直接“回复 Bot 转发给管理员的用户消息”，Bot 会按映射把回复发回原用户。

## 面板路由

| 路由 | 说明 |
|---|---|
| `/` | 监控列表 |
| `/monitor/new` | 新增监控 |
| `/monitor/templates` | 论坛模板 |
| `/monitor/bulk` | 批量新增 |
| `/monitor/{idx}/preview` | 预览抓取结果，不写入状态、不推送 |
| `/monitor/{idx}/run` | 手动检查单个监控 |
| `/run-once` | 手动检查全部监控 |
| `/yaml` | YAML 高级编辑 |
| `/settings` | `.env` 设置和监控清理策略 |
| `/send` | 主动发消息给已私聊过 Bot 的用户 |
| `/inbox` | 收件箱 |
| `/logs` | 日志 |
| `/health` | 健康检查 |

## 注意事项

- Telegram Bot 不能主动私聊陌生人；对方必须先给 Bot 发过 `/start` 或任意消息。
- 对公网暴露 Web 面板前，务必改默认密码。
- RSS 监控建议 60 秒起步；网页监控建议更保守，避免对目标站造成压力。
- 媒体消息当前只保证记录文本/说明和转发状态；转发失败后的媒体补发需要额外做本地附件存储。

## License

MIT
