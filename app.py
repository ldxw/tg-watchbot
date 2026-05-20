#!/usr/bin/env python3
"""tg-watchbot: Telegram two-way support bot + web/RSS monitor.

- Official Telegram Bot API via aiogram (no userbot/selfbot).
- SQLite state for dedupe, users, admin-message mapping, blocks, notes, monitor state.
- APScheduler async jobs for monitoring.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import logging
import os
import re
import secrets
import signal
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import feedparser
import httpx
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
import uvicorn

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "tg-watchbot.sqlite3"
CONFIG_PATH = BASE_DIR / "config.yaml"
ENV_PATH = BASE_DIR / ".env"
LOG_PATH = BASE_DIR / "tg-watchbot.log"
MIN_INTERVAL_SECONDS = 60

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 tg-watchbot/1.0"
)

logger = logging.getLogger("tg-watchbot")
router = Router()
bot: Bot | None = None
admin_chat_id: int | None = None
config: dict[str, Any] = {}
rate_buckets: dict[int, list[float]] = {}
pending_sendpic: dict[int, dict[str, Any]] = {}
scheduler_ref: AsyncIOScheduler | None = None


def setup_logging(level: str = "INFO") -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH, encoding="utf-8")],
    )


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"missing config: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def monitor_cleanup_settings() -> dict[str, int | bool]:
    cleanup = (config.get("cleanup") or {}) if isinstance(config, dict) else {}
    return {
        "enabled": bool(cleanup.get("enabled", True)),
        "interval_minutes": max(1, int(cleanup.get("interval_minutes", 60))),
        "retention_minutes": max(1, int(cleanup.get("monitor_retention_minutes", 1440))),
    }


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db()) as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                note TEXT DEFAULT '',
                blocked INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS message_map (
                admin_chat_id INTEGER NOT NULL,
                admin_message_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_message_id INTEGER,
                created_at TEXT NOT NULL,
                PRIMARY KEY (admin_chat_id, admin_message_id)
            );
            CREATE TABLE IF NOT EXISTS sent_events (
                event_key TEXT PRIMARY KEY,
                monitor_name TEXT NOT NULL,
                title TEXT,
                link TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS monitor_state (
                monitor_name TEXT NOT NULL,
                item_key TEXT NOT NULL,
                price TEXT,
                stock TEXT,
                title TEXT,
                link TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (monitor_name, item_key)
            );
            CREATE TABLE IF NOT EXISTS inbox_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                user_message_id INTEGER,
                message_type TEXT,
                text TEXT,
                forwarded INTEGER DEFAULT 0,
                admin_header_message_id INTEGER,
                admin_copy_message_id INTEGER,
                created_at TEXT NOT NULL,
                forwarded_at TEXT,
                error TEXT
            );
            """
        )
        conn.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def html_escape(text: Any) -> str:
    return html.escape(str(text or ""), quote=False)


def user_display(message: Message) -> tuple[int, str, str | None]:
    u = message.from_user
    if not u:
        return 0, "unknown", None
    full = " ".join(x for x in [u.first_name, u.last_name] if x).strip() or str(u.id)
    return u.id, full, u.username


def upsert_user(user_id: int, full_name: str, username: str | None) -> None:
    ts = now_iso()
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, username, full_name, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                updated_at=excluded.updated_at
            """,
            (user_id, username, full_name, ts, ts),
        )
        conn.commit()


def get_user(user_id: int) -> sqlite3.Row | None:
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()


def is_blocked(user_id: int) -> bool:
    row = get_user(user_id)
    return bool(row and row["blocked"])


def set_block(user_id: int, blocked: bool) -> None:
    with closing(db()) as conn:
        conn.execute(
            "UPDATE users SET blocked=?, updated_at=? WHERE user_id=?",
            (1 if blocked else 0, now_iso(), user_id),
        )
        conn.commit()


def set_note(user_id: int, note: str) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE users SET note=?, updated_at=? WHERE user_id=?", (note, now_iso(), user_id))
        conn.commit()


def rate_limited(user_id: int) -> bool:
    rl = (config.get("bot") or {}).get("rate_limit") or {}
    window = int(rl.get("window_seconds", 10))
    max_messages = int(rl.get("max_messages", 3))
    t = time.time()
    bucket = [x for x in rate_buckets.get(user_id, []) if t - x <= window]
    bucket.append(t)
    rate_buckets[user_id] = bucket
    return len(bucket) > max_messages


def save_message_map(admin_msg: Message, user_id: int, user_message_id: int | None) -> None:
    with closing(db()) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO message_map(admin_chat_id, admin_message_id, user_id, user_message_id, created_at) VALUES(?,?,?,?,?)",
            (admin_msg.chat.id, admin_msg.message_id, user_id, user_message_id, now_iso()),
        )
        conn.commit()




def create_inbox_message(message: Message, user_id: int, full_name: str, username: str | None) -> int:
    msg_type = "text" if message.text else (message.content_type or "message")
    text = message.text or message.caption or ""
    with closing(db()) as conn:
        cur = conn.execute(
            """
            INSERT INTO inbox_messages(user_id, username, full_name, user_message_id, message_type, text, created_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (user_id, username, full_name, message.message_id, msg_type, text, now_iso()),
        )
        conn.commit()
        return int(cur.lastrowid)


def mark_inbox_forwarded(inbox_id: int, header_id: int | None = None, copy_id: int | None = None) -> None:
    with closing(db()) as conn:
        conn.execute(
            "UPDATE inbox_messages SET forwarded=1, admin_header_message_id=?, admin_copy_message_id=?, forwarded_at=?, error=NULL WHERE id=?",
            (header_id, copy_id, now_iso(), inbox_id),
        )
        conn.commit()


def mark_inbox_error(inbox_id: int, error: str) -> None:
    with closing(db()) as conn:
        conn.execute("UPDATE inbox_messages SET error=? WHERE id=?", (error[:1000], inbox_id))
        conn.commit()


def pending_inbox(limit: int = 50) -> list[sqlite3.Row]:
    with closing(db()) as conn:
        return list(conn.execute("SELECT * FROM inbox_messages WHERE forwarded=0 ORDER BY id ASC LIMIT ?", (limit,)).fetchall())


def lookup_reply_target(admin_chat: int, admin_message_id: int) -> int | None:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT user_id FROM message_map WHERE admin_chat_id=? AND admin_message_id=?",
            (admin_chat, admin_message_id),
        ).fetchone()
        return int(row["user_id"]) if row else None


def parse_user_id_and_text(args: str | None) -> tuple[int, str]:
    if not args:
        raise ValueError("缺少参数")
    parts = args.strip().split(maxsplit=1)
    if len(parts) < 2:
        raise ValueError("格式应为：/reply <user_id> <内容>")
    return int(parts[0]), parts[1]


def parse_user_id(args: str | None) -> int:
    if not args:
        raise ValueError("缺少 user_id")
    return int(args.strip().split()[0])


def parse_user_id_and_optional_text(args: str | None) -> tuple[int, str]:
    if not args:
        raise ValueError("缺少 user_id")
    parts = args.strip().split(maxsplit=1)
    uid = int(parts[0])
    caption = parts[1] if len(parts) > 1 else ""
    return uid, caption


def describe_sendpic_target(user_id: int) -> str:
    row = get_user(user_id)
    if not row:
        return str(user_id)
    username = f"@{row['username']}" if row['username'] else ""
    full_name = row['full_name'] or str(user_id)
    return f"{full_name} {username}".strip()


async def admin_send(text: str) -> None:
    if not bot or admin_chat_id is None:
        logger.error("admin_send called before bot/admin init: %s", text)
        return
    try:
        await bot.send_message(admin_chat_id, text, disable_web_page_preview=False)
    except Exception:
        logger.exception("failed to send admin notification")


def is_admin_chat(message: Message) -> bool:
    """Dynamic admin-chat filter."""
    return admin_chat_id is not None and message.chat.id == admin_chat_id


def is_admin_action_message(message: Message) -> bool:
    """Only catch admin messages that are part of an action flow.

    A broad admin-chat handler would swallow ordinary admin messages before the
    fallback handler. Keep it narrow: reply-to-user and /sendpic photo flow only.
    """
    if not is_admin_chat(message):
        return False
    if pending_sendpic.get(message.chat.id):
        return True
    return bool(message.reply_to_message and message.text)


@router.message(Command("start"))
async def start(message: Message) -> None:
    uid, full, username = user_display(message)
    if not uid:
        return
    upsert_user(uid, full, username)
    if is_blocked(uid):
        await message.answer("你当前无法发送消息。")
        return
    await message.answer("已连接客服/管理员。你发来的消息会转交给管理员，请直接输入内容。")


async def send_text_to_user_from_admin(message: Message, args: str | None, command_name: str) -> None:
    if message.chat.id != admin_chat_id:
        return
    try:
        uid, text = parse_user_id_and_text(args)
        if not get_user(uid):
            await message.reply(f"错误：找不到用户 {uid}。对方需要先私聊 Bot 或 /start，Telegram Bot 才能主动发送。")
            return
        if is_blocked(uid):
            await message.reply(f"错误：用户 {uid} 已被封禁，先 /unblock {uid}")
            return
        if not bot:
            await message.reply("错误：Bot 尚未初始化")
            return
        sent = await bot.send_message(uid, text)  # type: ignore[union-attr]
        await message.reply(f"{command_name} 成功：已发送给用户 {uid}，message_id={sent.message_id}")
    except Exception as e:
        logger.exception("/%s failed", command_name)
        await message.reply(f"/{command_name} 失败：{e}\n用法：/{command_name} <user_id> <内容>")


@router.message(Command("reply"))
async def cmd_reply(message: Message, command: CommandObject) -> None:
    await send_text_to_user_from_admin(message, command.args, "reply")


@router.message(Command("send"))
async def cmd_send(message: Message, command: CommandObject) -> None:
    await send_text_to_user_from_admin(message, command.args, "send")


@router.message(Command("sendpic"))
async def cmd_sendpic(message: Message, command: CommandObject) -> None:
    if message.chat.id != admin_chat_id:
        return
    try:
        uid, caption = parse_user_id_and_optional_text(command.args)
        if not get_user(uid):
            await message.reply(f"错误：找不到用户 {uid}，对方需要先 /start 机器人")
            return
        if is_blocked(uid):
            await message.reply(f"错误：用户 {uid} 已被封禁，先 /unblock {uid}")
            return
        pending_sendpic[message.chat.id] = {"target": uid, "caption": caption, "created_at": time.time()}
        suffix = f"\n说明文字：{caption}" if caption else ""
        await message.reply(
            f"请发送需要转发给 {uid}（{html_escape(describe_sendpic_target(uid))}）的图片。{suffix}\n"
            "2 分钟内发送一张图片即可；发送 /cancel 取消。"
        )
    except Exception as e:
        logger.exception("/sendpic failed")
        await message.reply(f"/sendpic 失败：{e}\n用法：/sendpic 用户ID [可选图片说明]")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message) -> None:
    if message.chat.id == admin_chat_id and pending_sendpic.pop(message.chat.id, None):
        await message.reply("已取消待发送图片。")


@router.message(Command("block"))
async def cmd_block(message: Message, command: CommandObject) -> None:
    if message.chat.id != admin_chat_id:
        return
    try:
        uid = parse_user_id(command.args)
        if not get_user(uid):
            await message.reply(f"错误：找不到用户 {uid}")
            return
        set_block(uid, True)
        await message.reply(f"已封禁用户 {uid}")
    except Exception as e:
        logger.exception("/block failed")
        await message.reply(f"/block 失败：{e}")


@router.message(Command("unblock"))
async def cmd_unblock(message: Message, command: CommandObject) -> None:
    if message.chat.id != admin_chat_id:
        return
    try:
        uid = parse_user_id(command.args)
        if not get_user(uid):
            await message.reply(f"错误：找不到用户 {uid}")
            return
        set_block(uid, False)
        await message.reply(f"已解封用户 {uid}")
    except Exception as e:
        logger.exception("/unblock failed")
        await message.reply(f"/unblock 失败：{e}")


@router.message(Command("note"))
async def cmd_note(message: Message, command: CommandObject) -> None:
    if message.chat.id != admin_chat_id:
        return
    try:
        uid, note = parse_user_id_and_text(command.args)
        if not get_user(uid):
            await message.reply(f"错误：找不到用户 {uid}")
            return
        set_note(uid, note)
        await message.reply(f"已更新用户 {uid} 备注")
    except Exception as e:
        logger.exception("/note failed")
        await message.reply(f"/note 失败：{e}")


@router.message(Command("who"))
async def cmd_who(message: Message, command: CommandObject) -> None:
    if message.chat.id != admin_chat_id:
        return
    try:
        uid = parse_user_id(command.args)
        row = get_user(uid)
        if not row:
            await message.reply(f"错误：找不到用户 {uid}")
            return
        await message.reply(
            "用户信息\n"
            f"user_id: {row['user_id']}\n"
            f"username: @{row['username']}\n"
            f"full_name: {row['full_name']}\n"
            f"blocked: {bool(row['blocked'])}\n"
            f"note: {row['note'] or ''}\n"
            f"created_at: {row['created_at']}\n"
            f"updated_at: {row['updated_at']}"
        )
    except Exception as e:
        logger.exception("/who failed")
        await message.reply(f"/who 失败：{e}")


@router.message(is_admin_action_message)
async def admin_reply_by_message(message: Message) -> None:
    # Pending /sendpic flow: after /sendpic <uid>, the next admin photo is copied to target.
    pending = pending_sendpic.get(message.chat.id)
    if pending:
        if time.time() - float(pending.get("created_at", 0)) > 120:
            pending_sendpic.pop(message.chat.id, None)
            await message.reply("发送图片超时，已取消。请重新使用 /sendpic 用户ID。")
            return
        if message.photo:
            target = int(pending["target"])
            caption = (message.caption or pending.get("caption") or "")[:1024]
            try:
                if is_blocked(target):
                    pending_sendpic.pop(message.chat.id, None)
                    await message.reply(f"错误：用户 {target} 已被封禁，先 /unblock {target}")
                    return
                await bot.send_photo(target, message.photo[-1].file_id, caption=caption or None)  # type: ignore[union-attr]
                pending_sendpic.pop(message.chat.id, None)
                await message.reply(f"已发送图片给用户 {target}")
            except TelegramAPIError as e:
                logger.exception("/sendpic photo forwarding failed")
                await message.reply(f"图片发送失败：{e}")
            return
        if message.text and message.text.startswith("/"):
            return
        await message.reply("请发送一张图片；或发送 /cancel 取消。")
        return

    # Admin replies to forwarded/copy notification in admin chat.
    if not message.reply_to_message or not message.text:
        return
    target = lookup_reply_target(message.chat.id, message.reply_to_message.message_id)
    if not target:
        return
    try:
        if is_blocked(target):
            await message.reply(f"错误：用户 {target} 已被封禁，先 /unblock {target}")
            return
        sent = await bot.send_message(target, message.text)  # type: ignore[union-attr]
        await message.reply(f"已发送给用户 {target}，message_id={sent.message_id}")
    except TelegramAPIError as e:
        logger.exception("admin reply forwarding failed")
        await message.reply(f"发送失败：{e}")


@router.message(is_admin_chat)
async def admin_plain_message(message: Message) -> None:
    # Do not silently swallow ordinary admin messages.
    if message.text and not message.text.startswith("/"):
        await message.reply(
            "管理员普通消息不会自动转发。请使用：\n"
            "/send <user_id> <内容>\n"
            "/reply <user_id> <内容>\n"
            "或在收件箱里回复某条用户消息；也可以打开面板的「主动发消息」。"
        )


@router.message()
async def user_message(message: Message) -> None:
    # Only relay private user chats to admin.
    logger.info("incoming message chat_id=%s chat_type=%s from_user=%s content_type=%s text=%r", message.chat.id, message.chat.type, getattr(message.from_user, 'id', None), message.content_type, (message.text or '')[:80])
    if message.chat.id == admin_chat_id:
        logger.info("incoming message is admin plain message; ignored by user relay")
        return
    if message.chat.type != "private":
        logger.info("incoming message ignored because chat_type is not private: %s", message.chat.type)
        return
    uid, full, username = user_display(message)
    if not uid:
        return
    upsert_user(uid, full, username)
    if is_blocked(uid):
        await message.answer("你当前无法发送消息。")
        return
    if rate_limited(uid):
        await message.answer("发送太快了，请稍后再试。")
        return
    inbox_id = create_inbox_message(message, uid, full, username)
    user_row = get_user(uid)
    note = user_row["note"] if user_row and "note" in user_row.keys() else ""
    header = (
        f"[用户消息 #{inbox_id}]\n"
        f"user_id: <code>{uid}</code>\n"
        f"name: {html_escape(full)}\n"
        f"username: @{html_escape(username or '')}\n"
        f"note: {html_escape(note)}\n"
        f"time: {html_escape(now_iso())}"
    )
    try:
        sent = await bot.send_message(admin_chat_id, header)  # type: ignore[union-attr]
        save_message_map(sent, uid, message.message_id)
        copied = await message.copy_to(admin_chat_id, reply_to_message_id=sent.message_id)  # type: ignore[arg-type]
        save_message_map(copied, uid, message.message_id)
        mark_inbox_forwarded(inbox_id, sent.message_id, copied.message_id)
        await message.answer("已转交管理员。")
    except Exception as e:
        mark_inbox_error(inbox_id, repr(e))
        logger.exception("failed to relay user message, saved inbox_id=%s", inbox_id)
        await message.answer("已收到留言，但转发管理员暂时失败；系统会稍后自动重试。")


@dataclass
class MonitorItem:
    key: str
    title: str
    link: str
    text: str
    price: str | None = None
    stock: str | None = None
    author: str | None = None
    published: str | None = None
    category: str | None = None


def stable_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


def extract_price(text: str) -> str | None:
    m = re.search(r"(?:¥|￥|\$|USD|CNY)?\s*\d+(?:[.,]\d{1,2})?", text, re.I)
    return m.group(0).strip() if m else None


def keyword_hits(text: str, keywords: list[str]) -> list[str]:
    low = text.lower()
    return [k for k in keywords if k and k.lower() in low]


def item_blocked(item: MonitorItem, monitor: dict[str, Any]) -> tuple[bool, str]:
    text = f"{item.title} {item.text} {item.author or ''} {item.category or ''}"
    exclude_hits = keyword_hits(text, monitor.get("exclude_keywords") or [])
    if exclude_hits:
        return True, "屏蔽词 " + ", ".join(exclude_hits)
    authors = [a.lower() for a in (monitor.get("authors") or []) if a]
    if authors and (item.author or "").lower() not in authors:
        return True, "作者不匹配"
    categories = [c.lower() for c in (monitor.get("categories") or []) if c]
    if categories and not any(c in (item.category or "").lower() for c in categories):
        return True, "分类不匹配"
    return False, ""


async def fetch_url(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def parse_web_items(monitor: dict[str, Any], body: str) -> list[MonitorItem]:
    selectors = monitor.get("selectors") or {}
    item_sel = selectors.get("item") or "article, .thread, .post, li"
    title_sel = selectors.get("title") or "h1, h2, h3, a"
    link_sel = selectors.get("link") or "a"
    price_sel = selectors.get("price")
    stock_sel = selectors.get("stock")
    soup = BeautifulSoup(body, "html.parser")
    nodes = soup.select(item_sel)[:100]
    if not nodes:
        nodes = [soup]
    items: list[MonitorItem] = []
    for node in nodes:
        title_node = node.select_one(title_sel) if title_sel else None
        link_node = node.select_one(link_sel) if link_sel else None
        title = (title_node.get_text(" ", strip=True) if title_node else node.get_text(" ", strip=True)[:120]).strip()
        href = link_node.get("href") if link_node else ""
        link = urljoin(monitor.get("url", ""), href) if href else monitor.get("url", "")
        text = node.get_text(" ", strip=True)
        price = None
        stock = None
        if price_sel and (p := node.select_one(price_sel)):
            price = p.get_text(" ", strip=True)
        else:
            price = extract_price(text)
        if stock_sel and (s := node.select_one(stock_sel)):
            stock = s.get_text(" ", strip=True)
        for hint in ["有货", "无货", "缺货", "in stock", "out of stock", "sold out", "available"]:
            if hint.lower() in text.lower():
                stock = hint
                break
        if title or text:
            key = stable_key(link, title or text[:80])
            items.append(MonitorItem(key=key, title=title or "(no title)", link=link, text=text, price=price, stock=stock))
    return items


def canonical_forum_key(link: str, entry_id: str = "") -> str:
    """Return a stable topic/post key that survives title edits and RSS updated ids."""
    target = link or entry_id
    patterns = [
        r"nodeseek\.com/post-(\d+)",
        r"linux\.do/t/(?:[^/]+/)?(\d+)",
        r"/t/(?:[^/]+/)?(\d+)",
    ]
    for value in [target, entry_id, link]:
        for pattern in patterns:
            m = re.search(pattern, value or "", re.I)
            if m:
                return m.group(1)
    return stable_key(link or entry_id)


def parse_rss_items(monitor: dict[str, Any], body: str) -> list[MonitorItem]:
    feed = feedparser.parse(body)
    items: list[MonitorItem] = []
    for e in feed.entries[:100]:
        title = getattr(e, "title", "(no title)")
        link = getattr(e, "link", monitor.get("url", ""))
        summary = getattr(e, "summary", "")
        content = " ".join([c.get("value", "") for c in getattr(e, "content", []) if isinstance(c, dict)])
        published = getattr(e, "published", "") or getattr(e, "updated", "")
        author = getattr(e, "author", "") or getattr(e, "dc_creator", "")
        category = ""
        tags = getattr(e, "tags", None) or []
        if tags:
            category = ", ".join([t.get("term", "") for t in tags if isinstance(t, dict) and t.get("term")])
        entry_id = getattr(e, "id", "") or getattr(e, "guid", "")
        key = canonical_forum_key(link, entry_id) if (monitor.get("forum") or monitor.get("type") == "rss") else stable_key(entry_id, link, title)
        items.append(MonitorItem(key=key, title=title, link=link, text=f"{title} {summary} {content} {published} {author} {category}", author=author, published=published, category=category))
    return items


def should_notify_and_update(monitor: dict[str, Any], item: MonitorItem, hits: list[str]) -> list[str]:
    name = monitor["name"]
    notify_on = monitor.get("notify_on") or {}
    reasons: list[str] = []
    with closing(db()) as conn:
        prev = conn.execute(
            "SELECT * FROM monitor_state WHERE monitor_name=? AND item_key=?",
            (name, item.key),
        ).fetchone()
        is_forum = bool(monitor.get("forum") or monitor.get("type") == "rss")
        if is_forum:
            # 论坛/RSS 帖子只在首次出现并命中时通知一次。
            # 后续 RSS 因回复/编辑把同一链接重新排到前面时，只更新状态，不再重复推送。
            if prev is None:
                if notify_on.get("new_item", False):
                    reasons.append("新条目")
                if notify_on.get("keyword_match", True) and hits:
                    reasons.append("关键词 " + ", ".join(hits))
        else:
            if prev is None:
                if notify_on.get("new_item", False):
                    reasons.append("新条目")
            else:
                if notify_on.get("price_change", False) and (item.price or "") != (prev["price"] or ""):
                    reasons.append(f"价格变化 {prev['price'] or '-'} -> {item.price or '-'}")
                if notify_on.get("stock_change", False) and (item.stock or "") != (prev["stock"] or ""):
                    reasons.append(f"库存变化 {prev['stock'] or '-'} -> {item.stock or '-'}")
            if notify_on.get("keyword_match", True) and hits:
                reasons.append("关键词 " + ", ".join(hits))
        conn.execute(
            """
            INSERT INTO monitor_state(monitor_name, item_key, price, stock, title, link, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(monitor_name, item_key) DO UPDATE SET
                price=excluded.price, stock=excluded.stock, title=excluded.title,
                link=excluded.link, updated_at=excluded.updated_at
            """,
            (name, item.key, item.price, item.stock, item.title, item.link, now_iso()),
        )
        conn.commit()
    return reasons


def event_not_sent(event_key: str, monitor_name: str, title: str, link: str) -> bool:
    with closing(db()) as conn:
        try:
            conn.execute(
                "INSERT INTO sent_events(event_key, monitor_name, title, link, created_at) VALUES(?,?,?,?,?)",
                (event_key, monitor_name, title, link, now_iso()),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


async def run_monitor(monitor: dict[str, Any]) -> int:
    name = monitor.get("name", "unnamed")
    mtype = monitor.get("type", "web")
    url = monitor.get("url")
    if not url:
        logger.error("monitor %s missing url", name)
        return 0
    keywords = monitor.get("keywords") or []
    timeout = int((config.get("http") or {}).get("timeout_seconds", 20))
    ua = (config.get("http") or {}).get("user_agent") or DEFAULT_UA
    headers = {"User-Agent": ua, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    sent_count = 0
    try:
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            body = await fetch_url(client, url)
        items = parse_rss_items(monitor, body) if mtype == "rss" else parse_web_items(monitor, body)
        for item in items:
            blocked, block_reason = item_blocked(item, monitor)
            if blocked:
                logger.debug("monitor %s skipped item %s: %s", name, item.title, block_reason)
                continue
            hits = keyword_hits(f"{item.title} {item.text}", keywords)
            # If keywords are configured and keyword_match is enabled, do not push unrelated new posts.
            notify_on = monitor.get("notify_on") or {}
            if keywords and notify_on.get("keyword_match", True) and not hits and not (notify_on.get("price_change") or notify_on.get("stock_change")):
                should_notify_and_update(monitor, item, [])  # still remember state to avoid later old flood
                continue
            reasons = should_notify_and_update(monitor, item, hits)
            if not reasons:
                continue
            # 论坛/RSS 以帖子本身作为事件键；不要把“命中原因/检查时间/编辑变化”放进去，避免同一帖重复发。
            is_forum = monitor.get("forum") or mtype == "rss"
            event_key = stable_key(name, item.key) if is_forum else stable_key(name, item.key, "|".join(reasons), item.price or "", item.stock or "")
            if not event_not_sent(event_key, name, item.title, item.link):
                continue
            if is_forum:
                text = (
                    f"[新帖命中] {html_escape(name)}\n"
                    f"标题：{html_escape(item.title)}\n"
                    f"作者：{html_escape(item.author or '-')}\n"
                    f"分类：{html_escape(item.category or '-')}\n"
                    f"链接：{html_escape(item.link)}\n"
                    f"命中：{html_escape('; '.join(reasons))}\n"
                    f"发布时间：{html_escape(item.published or '-')}\n"
                    f"检查时间：{html_escape(now_iso())}"
                )
            else:
                text = (
                    f"[库存/关键词命中] {html_escape(name)}\n"
                    f"标题：{html_escape(item.title)}\n"
                    f"链接：{html_escape(item.link)}\n"
                    f"命中：{html_escape('; '.join(reasons))}\n"
                    f"价格：{html_escape(item.price or '-')}\n"
                    f"库存：{html_escape(item.stock or '-')}\n"
                    f"时间：{html_escape(now_iso())}"
                )
            await admin_send(text)
            sent_count += 1
    except Exception:
        logger.exception("monitor failed: %s %s", name, url)
    return sent_count



def cleanup_monitor_data(retention_minutes: int) -> tuple[int, int]:
    """Delete only website/RSS monitor state older than retention.

    Keeps two-way conversation tables intact: users, message_map, inbox_messages.
    """
    cutoff_ts = time.time() - max(1, int(retention_minutes)) * 60
    cutoff = datetime.fromtimestamp(cutoff_ts, timezone.utc).astimezone().isoformat(timespec="seconds")
    with closing(db()) as conn:
        cur1 = conn.execute("DELETE FROM monitor_state WHERE updated_at < ?", (cutoff,))
        cur2 = conn.execute("DELETE FROM sent_events WHERE created_at < ?", (cutoff,))
        conn.commit()
        return int(cur1.rowcount or 0), int(cur2.rowcount or 0)


async def cleanup_monitor_loop() -> None:
    while True:
        settings = monitor_cleanup_settings()
        await asyncio.sleep(int(settings["interval_minutes"]) * 60)
        if not settings["enabled"]:
            continue
        try:
            state_n, sent_n = cleanup_monitor_data(int(settings["retention_minutes"]))
            logger.info(
                "monitor cleanup done retention=%smin deleted monitor_state=%s sent_events=%s",
                settings["retention_minutes"], state_n, sent_n,
            )
        except Exception:
            logger.exception("monitor cleanup failed")



async def flush_pending_inbox() -> None:
    if not bot or admin_chat_id is None:
        return
    rows = pending_inbox(50)
    if not rows:
        return
    logger.info("flushing pending inbox messages: %d", len(rows))
    for row in rows:
        try:
            text = (
                f"[补发用户消息 #{row['id']}]\n"
                f"user_id: <code>{row['user_id']}</code>\n"
                f"name: {html_escape(row['full_name'])}\n"
                f"username: @{html_escape(row['username'] or '')}\n"
                f"原消息ID: {row['user_message_id']}\n"
                f"类型: {html_escape(row['message_type'])}\n"
                f"时间: {html_escape(row['created_at'])}\n\n"
                f"内容：{html_escape(row['text'] or '(非文本/媒体消息，原始媒体无法补发，仅保留记录)')}"
            )
            sent = await bot.send_message(admin_chat_id, text)
            save_message_map(sent, int(row['user_id']), int(row['user_message_id']) if row['user_message_id'] else None)
            mark_inbox_forwarded(int(row['id']), sent.message_id, None)
        except Exception as e:
            mark_inbox_error(int(row['id']), repr(e))
            logger.exception("failed to flush inbox message id=%s", row['id'])


async def flush_pending_loop() -> None:
    while True:
        try:
            await flush_pending_inbox()
        except Exception:
            logger.exception("flush_pending_loop failed")
        await asyncio.sleep(60)


async def run_all_monitors_once() -> None:
    monitors = config.get("monitors") or []
    logger.info("manual/all monitor run start, count=%d", len(monitors))
    total = 0
    for m in monitors:
        total += await run_monitor(m)
    logger.info("manual/all monitor run done, notifications=%d", total)


def schedule_monitors(scheduler: AsyncIOScheduler) -> None:
    for idx, m in enumerate(config.get("monitors") or []):
        name = m.get("name", "unnamed")
        requested = int(m.get("interval_seconds", MIN_INTERVAL_SECONDS))
        interval = max(requested, MIN_INTERVAL_SECONDS)
        if requested < MIN_INTERVAL_SECONDS:
            logger.warning("monitor %s interval %s raised to %s", name, requested, interval)
        # Use index+hash, not just name: duplicate names should not crash saving/reloading.
        job_key = stable_key(str(idx), name, m.get("url", ""))[:16]
        scheduler.add_job(run_monitor, "interval", seconds=interval, args=[m], id=f"monitor:{idx}:{job_key}", max_instances=1, coalesce=True, replace_existing=True, next_run_time=datetime.now(timezone.utc))
        logger.info("scheduled monitor %s every %ss", name, interval)



# -----------------------------
# Web admin panel
# -----------------------------

def panel_enabled() -> bool:
    return os.getenv("WEB_PANEL_ENABLED", "true").lower() not in {"0", "false", "no", "off"}


def session_secret() -> str:
    secret = os.getenv("WEB_PANEL_SESSION_SECRET", "").strip()
    if not secret:
        secret = secrets.token_urlsafe(32)
        vals = env_values()
        vals["WEB_PANEL_SESSION_SECRET"] = secret
        write_env_values(vals)
    return secret


def session_token(username: str) -> str:
    raw = f"{username}|{session_secret()}"
    return hashlib.sha256(raw.encode()).hexdigest()


def is_logged_in(request: Request) -> bool:
    username = os.getenv("WEB_PANEL_USER", "admin")
    token = request.cookies.get("tg_watchbot_session", "")
    return bool(token) and secrets.compare_digest(token, session_token(username))


def panel_auth(request: Request) -> str:
    # Actual redirect is handled by middleware; dependencies cannot reliably return redirects.
    return os.getenv("WEB_PANEL_USER", "admin")


def login_page(error: str = "") -> str:
    err = f"<div class='login-error'>{html_escape(error)}</div>" if error else ""
    return f"""<!doctype html><html lang=zh-CN><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>
<title>登录 · tg-watchbot</title>
<style>
:root{{color-scheme:light}}*{{box-sizing:border-box}}body{{margin:0;min-height:100vh;font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:radial-gradient(circle at 18% 10%,#bfdbfe88,transparent 30%),radial-gradient(circle at 82% 12%,#bbf7d088,transparent 26%),linear-gradient(135deg,#f8fbff,#eef6ff 48%,#f7fee7);color:#172033;display:grid;place-items:center;padding:22px}}
.login-card{{width:min(430px,100%);padding:34px;border:1px solid #dbeafe;border-radius:28px;background:rgba(255,255,255,.86);box-shadow:0 24px 70px #93c5fd44;backdrop-filter:blur(18px)}}
.logo{{width:58px;height:58px;border-radius:18px;background:linear-gradient(135deg,#60a5fa,#86efac);display:grid;place-items:center;font-weight:900;color:#0f172a;font-size:26px;box-shadow:0 12px 32px #60a5fa55;margin-bottom:18px}}
h1{{margin:0 0 8px;font-size:28px;color:#0f172a}}p{{margin:0 0 24px;color:#64748b;line-height:1.6}}label{{display:block;margin:14px 0 7px;color:#475569;font-size:14px}}input{{width:100%;border:1px solid #cbd5e1;border-radius:14px;background:#ffffff;color:#0f172a;padding:13px 14px;font-size:15px;outline:none}}input:focus{{border-color:#60a5fa;box-shadow:0 0 0 4px #bfdbfe88}}button{{width:100%;margin-top:22px;border:0;border-radius:14px;padding:13px 16px;background:linear-gradient(135deg,#3b82f6,#22c55e);color:white;font-weight:800;font-size:15px;cursor:pointer;box-shadow:0 14px 36px #60a5fa55}}button:hover{{filter:brightness(1.04)}}.login-error{{background:#fef2f2;border:1px solid #fecaca;color:#991b1b;padding:10px 12px;border-radius:12px;margin-bottom:16px}}.foot{{margin-top:18px;color:#94a3b8;font-size:13px;text-align:center}}
</style></head><body><main class=login-card><div class=logo>⚡</div><h1>tg-watchbot</h1><p>登录后管理 Telegram 机器人、关键词监控、库存/价格提醒。</p>{err}<form method=post action=/login><label>用户名</label><input name=username autocomplete=username autofocus><label>密码</label><input name=password type=password autocomplete=current-password><button type=submit>登录面板</button></form><div class=foot>your-domain.example · Cloudflare Tunnel</div></main></body></html>"""


def env_values() -> dict[str, str]:
    load_dotenv(ENV_PATH, override=True)
    return {
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "ADMIN_CHAT_ID": os.getenv("ADMIN_CHAT_ID", ""),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        "WEB_PANEL_ENABLED": os.getenv("WEB_PANEL_ENABLED", "true"),
        "WEB_PANEL_HOST": os.getenv("WEB_PANEL_HOST", "127.0.0.1"),
        "WEB_PANEL_PORT": os.getenv("WEB_PANEL_PORT", "8765"),
        "WEB_PANEL_USER": os.getenv("WEB_PANEL_USER", "admin"),
        "WEB_PANEL_PASSWORD": os.getenv("WEB_PANEL_PASSWORD", "admin"),
        "WEB_PANEL_SESSION_SECRET": os.getenv("WEB_PANEL_SESSION_SECRET", ""),
    }


def write_env_values(values: dict[str, str]) -> None:
    lines = [
        "# tg-watchbot environment",
        f"TELEGRAM_BOT_TOKEN={values.get('TELEGRAM_BOT_TOKEN','')}",
        f"ADMIN_CHAT_ID={values.get('ADMIN_CHAT_ID','')}",
        f"LOG_LEVEL={values.get('LOG_LEVEL','INFO')}",
        "",
        "# Web 管理面板；默认只监听本机，建议用 SSH 隧道或反代再暴露",
        f"WEB_PANEL_ENABLED={values.get('WEB_PANEL_ENABLED','true')}",
        f"WEB_PANEL_HOST={values.get('WEB_PANEL_HOST','127.0.0.1')}",
        f"WEB_PANEL_PORT={values.get('WEB_PANEL_PORT','8765')}",
        f"WEB_PANEL_USER={values.get('WEB_PANEL_USER','admin')}",
        f"WEB_PANEL_PASSWORD={values.get('WEB_PANEL_PASSWORD','admin')}",
        f"WEB_PANEL_SESSION_SECRET={values.get('WEB_PANEL_SESSION_SECRET','')}",
        "",
    ]
    ENV_PATH.write_text("\n".join(lines), encoding="utf-8")
    ENV_PATH.chmod(0o600)
    load_dotenv(ENV_PATH, override=True)


def cfg_load_fresh() -> dict[str, Any]:
    return load_config()


def cfg_save(new_cfg: dict[str, Any]) -> None:
    if not isinstance(new_cfg, dict):
        raise ValueError("配置根节点必须是对象")
    monitors = new_cfg.setdefault("monitors", [])
    if not isinstance(monitors, list):
        raise ValueError("monitors 必须是列表")
    for m in monitors:
        if not isinstance(m, dict):
            raise ValueError("每个 monitor 必须是对象")
        if int(m.get("interval_seconds", MIN_INTERVAL_SECONDS)) < MIN_INTERVAL_SECONDS:
            m["interval_seconds"] = MIN_INTERVAL_SECONDS
    CONFIG_PATH.write_text(yaml.safe_dump(new_cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
    global config
    config = new_cfg
    reload_scheduler_jobs()


def reload_scheduler_jobs() -> None:
    if scheduler_ref:
        for job in list(scheduler_ref.get_jobs()):
            if job.id.startswith("monitor:"):
                scheduler_ref.remove_job(job.id)
        schedule_monitors(scheduler_ref)


def parse_lines(text: str) -> list[str]:
    return [x.strip() for x in (text or "").splitlines() if x.strip()]


def monitor_from_form(
    original_index: int | None,
    name: str,
    mtype: str,
    url: str,
    interval_seconds: int,
    keywords: str,
    item_selector: str,
    title_selector: str,
    link_selector: str,
    price_selector: str,
    stock_selector: str,
    keyword_match: bool,
    new_item: bool,
    price_change: bool,
    stock_change: bool,
) -> dict[str, Any]:
    m: dict[str, Any] = {
        "name": name.strip(),
        "type": mtype,
        "url": url.strip(),
        "interval_seconds": max(int(interval_seconds or MIN_INTERVAL_SECONDS), MIN_INTERVAL_SECONDS),
        "keywords": parse_lines(keywords),
        "notify_on": {
            "keyword_match": keyword_match,
            "new_item": new_item,
            "price_change": price_change,
            "stock_change": stock_change,
        },
    }
    if mtype == "rss":
        m["forum"] = True
    if mtype == "web":
        selectors = {
            "item": item_selector.strip() or "article, .thread, .post, li",
            "title": title_selector.strip() or "h1, h2, h3, a",
            "link": link_selector.strip() or "a",
        }
        if price_selector.strip():
            selectors["price"] = price_selector.strip()
        if stock_selector.strip():
            selectors["stock"] = stock_selector.strip()
        m["selectors"] = selectors
    if not m["name"] or not m["url"]:
        raise ValueError("名称和 URL 必填")
    return m


def layout(title: str, body: str) -> str:
    return f"""<!doctype html><html lang=zh-CN><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>
<title>{html_escape(title)} · tg-watchbot</title>
<style>
*{{box-sizing:border-box}}body{{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:radial-gradient(circle at 12% -8%,#dbeafe,transparent 30%),radial-gradient(circle at 88% -4%,#dcfce7,transparent 26%),linear-gradient(180deg,#f8fbff,#f3f7fb);color:#172033;margin:0}}
a{{color:#2563eb;text-decoration:none}}a:hover{{color:#1d4ed8}} .shell{{display:grid;grid-template-columns:260px 1fr;min-height:100vh}}aside{{border-right:1px solid #e2e8f0;background:rgba(255,255,255,.78);backdrop-filter:blur(16px);padding:22px;position:sticky;top:0;height:100vh;box-shadow:8px 0 30px #dbeafe55}}main{{padding:30px;min-width:0}}.brand{{display:flex;gap:12px;align-items:center;margin-bottom:24px}}.mark{{width:42px;height:42px;border-radius:14px;background:linear-gradient(135deg,#60a5fa,#86efac);display:grid;place-items:center;color:#0f172a;font-weight:900;box-shadow:0 12px 28px #bfdbfe}}.brand b{{font-size:18px;color:#0f172a}}.brand small{{display:block;color:#64748b;margin-top:2px}}nav{{display:grid;gap:8px}}nav a{{padding:11px 12px;border-radius:12px;color:#475569;border:1px solid transparent;font-weight:650}}nav a:hover{{background:#eff6ff;border-color:#bfdbfe;color:#1d4ed8}}.top{{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:18px}}.top h1{{margin:0;font-size:28px;color:#0f172a}}.logout{{color:#dc2626}}.btn{{background:#ffffff;color:#334155;padding:8px 12px;border-radius:10px;border:1px solid #cbd5e1;display:inline-block;cursor:pointer;box-shadow:0 4px 14px #e2e8f066}}.btn:hover{{background:#f8fafc}}.btn.primary{{background:linear-gradient(135deg,#3b82f6,#22c55e);border-color:#60a5fa;color:white}}.btn.danger{{background:#fef2f2;border-color:#fecaca;color:#b91c1c}}.btn.ok{{background:#ecfdf5;border-color:#bbf7d0;color:#15803d}}
.card{{background:rgba(255,255,255,.88);border:1px solid #e2e8f0;border-radius:20px;padding:18px;margin:14px 0;box-shadow:0 16px 45px #dbeafe66}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}}
input,select,textarea{{width:100%;box-sizing:border-box;background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:12px;padding:11px;outline:none}}input:focus,select:focus,textarea:focus{{border-color:#60a5fa;box-shadow:0 0 0 4px #bfdbfe88}}textarea{{min-height:120px;font-family:ui-monospace,Consolas,monospace}} label{{display:block;margin:10px 0 5px;color:#475569;font-weight:650}}
small,.muted{{color:#64748b;line-height:1.6}} table{{width:100%;border-collapse:separate;border-spacing:0}} td,th{{border-bottom:1px solid #e2e8f0;padding:11px;text-align:left;vertical-align:top}} th{{color:#64748b;font-size:13px;background:#f8fafc}}.badge{{padding:3px 9px;border-radius:999px;background:#eef2ff;color:#3730a3}} .msg{{padding:12px;border-radius:12px;background:#ecfdf5;border:1px solid #bbf7d0;color:#166534;margin:10px 0}}pre{{white-space:pre-wrap;background:#f8fafc;color:#334155;padding:12px;border-radius:12px;border:1px solid #e2e8f0;max-height:420px;overflow:auto}}
@media(max-width:800px){{.shell{{grid-template-columns:1fr}}aside{{position:relative;height:auto}}main{{padding:18px}}}}
</style></head><body><div class=shell><aside><div class=brand><div class=mark>⚡</div><div><b>tg-watchbot</b><small>Telegram 自动化</small></div></div><nav><a href='/'>监控面板</a><a href='/inbox'>收件箱</a><a href='/send'>主动发消息</a><a href='/monitor/new'>新增监控</a><a href='/settings'>Bot / 面板设置</a><a href='/yaml'>YAML 高级编辑</a><a href='/logs'>运行日志</a><a href='/run-once'>手动检查</a><a href='/restart' onclick='return confirm("确定重启机器人服务？")'>重启机器人</a><a class=logout href='/logout'>退出登录</a></nav></aside><main><div class=top><h1>{html_escape(title)}</h1><span class=badge>WatchBot Panel</span></div>
{body}</main></div></body></html>"""


def monitor_form_html(m: dict[str, Any] | None = None, idx: int | None = None) -> str:
    m = m or {"type": "web", "interval_seconds": 60, "notify_on": {"keyword_match": True, "new_item": True, "price_change": True, "stock_change": True}, "selectors": {}}
    selectors = m.get("selectors") or {}
    no = m.get("notify_on") or {}
    keywords = "\n".join(m.get("keywords") or [])
    action = "/monitor/save" if idx is not None else "/monitor/create"
    hidden = f"<input type=hidden name=original_index value='{idx}'>" if idx is not None else ""
    def checked(k: str) -> str:
        return "checked" if no.get(k, False) else ""
    return f"""<form method=post action='{action}' class=card>{hidden}
<div class=grid><div><label>名称</label><input name=name value='{html_escape(m.get('name',''))}' required></div>
<div><label>类型</label><select name=mtype><option value=web {'selected' if m.get('type')=='web' else ''}>Web 网页</option><option value=rss {'selected' if m.get('type')=='rss' else ''}>RSS</option></select></div>
<div><label>URL</label><input name=url value='{html_escape(m.get('url',''))}' required></div>
<div><label>间隔秒数（最低 60）</label><input name=interval_seconds type=number min=60 value='{html_escape(m.get('interval_seconds',60))}'></div></div>
<label>关键词（一行一个）</label><textarea name=keywords>{html_escape(keywords)}</textarea>
<h3>Web 选择器（RSS 可忽略）</h3><div class=grid>
<div><label>条目选择器</label><input name=item_selector value='{html_escape(selectors.get('item','article, .thread, .post, li'))}'></div>
<div><label>标题选择器</label><input name=title_selector value='{html_escape(selectors.get('title','h1, h2, h3, a'))}'></div>
<div><label>链接选择器</label><input name=link_selector value='{html_escape(selectors.get('link','a'))}'></div>
<div><label>价格选择器</label><input name=price_selector value='{html_escape(selectors.get('price',''))}'></div>
<div><label>库存选择器</label><input name=stock_selector value='{html_escape(selectors.get('stock',''))}'></div></div>
<h3>提醒条件</h3>
<label><input style='width:auto' type=checkbox name=keyword_match {checked('keyword_match')}> 关键词命中</label>
<label><input style='width:auto' type=checkbox name=new_item {checked('new_item')}> 新条目</label>
<label><input style='width:auto' type=checkbox name=price_change {checked('price_change')}> 价格变化</label>
<label><input style='width:auto' type=checkbox name=stock_change {checked('stock_change')}> 库存变化</label>
<p><button class='btn primary' type=submit>保存</button> <a class=btn href='/'>取消</a></p></form>"""


def create_panel_app() -> FastAPI:
    app = FastAPI(title="tg-watchbot Panel")

    @app.middleware("http")
    async def require_login_middleware(request: Request, call_next):
        public_paths = {"/login", "/health", "/favicon.ico"}
        if request.url.path in public_paths or is_logged_in(request):
            return await call_next(request)
        return RedirectResponse("/login", status_code=303)

    @app.get("/login", response_class=HTMLResponse)
    async def login_get(request: Request):
        if is_logged_in(request):
            return RedirectResponse("/", status_code=303)
        return HTMLResponse(login_page())

    @app.post("/login")
    async def login_post(username: str = Form(""), password: str = Form("")):
        expected_user = os.getenv("WEB_PANEL_USER", "admin")
        expected_pass = os.getenv("WEB_PANEL_PASSWORD", "admin")
        if secrets.compare_digest(username, expected_user) and secrets.compare_digest(password, expected_pass):
            resp = RedirectResponse("/", status_code=303)
            resp.set_cookie("tg_watchbot_session", session_token(expected_user), httponly=True, secure=True, samesite="lax", max_age=60 * 60 * 24 * 14)
            return resp
        return HTMLResponse(login_page("用户名或密码错误"), status_code=401)

    @app.get("/logout")
    async def logout() -> RedirectResponse:
        resp = RedirectResponse("/login", status_code=303)
        resp.delete_cookie("tg_watchbot_session")
        return resp

    @app.get("/", response_class=HTMLResponse)
    async def index(_: str = Depends(panel_auth)) -> str:
        cfg = cfg_load_fresh()
        rows = []
        for i, m in enumerate(cfg.get("monitors") or []):
            rows.append(f"""<tr><td><span class=badge>{html_escape(m.get('type','web'))}</span></td><td><b>{html_escape(m.get('name',''))}</b><br><small>{html_escape(m.get('url',''))}</small></td><td>{html_escape(m.get('interval_seconds',60))}s</td><td>{html_escape(', '.join(m.get('keywords') or []))}</td><td><a class=btn href='/monitor/{i}/edit'>编辑</a> <a class='btn ok' href='/monitor/{i}/preview'>预览</a> <a class='btn ok' href='/monitor/{i}/run'>检查</a> <a class='btn danger' href='/monitor/{i}/delete' onclick='return confirm("确定删除？")'>删除</a></td></tr>""")
        body = f"""<div class=card><div style='display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap'><div><h2 style='margin:0 0 6px'>监控目标</h2><p class=muted style='margin:0'>当前 {len(cfg.get('monitors') or [])} 个；不限制数量，可继续新增。保存后自动重载定时任务。</p></div><div><a class='btn primary' href='/monitor/templates'>论坛模板</a> <a class='btn primary' href='/monitor/new'>+ 新增监控</a> <a class='btn ok' href='/monitor/bulk'>批量新增</a></div></div><table style='margin-top:16px'><tr><th>类型</th><th>目标</th><th>间隔</th><th>关键词</th><th>操作</th></tr>""" + "".join(rows) + "</table></div>"
        return layout("监控", body)

    @app.get("/monitor/new", response_class=HTMLResponse)
    async def new_monitor(_: str = Depends(panel_auth)) -> str:
        return layout("新增监控", "<div class=card><p class=muted>这里是新增单个监控。要一次加多个网站，用左侧/首页的「批量新增」。</p></div>" + monitor_form_html())

    @app.get("/monitor/templates", response_class=HTMLResponse)
    async def monitor_templates(_: str = Depends(panel_auth)) -> str:
        body = """<div class=card><h2>论坛监控模板</h2><p class=muted>NodeSeek / Linux.do 建议用 RSS，不抓网页 HTML，抗 Cloudflare 更稳。</p><div class=grid><a class='btn primary' href='/monitor/template/nodeseek'>NodeSeek 新帖</a><a class='btn primary' href='/monitor/template/linuxdo'>Linux.do 最新</a><a class='btn primary' href='/monitor/template/linuxdo-resource'>Linux.do 资源荟萃</a></div></div>"""
        return layout("监控模板", body)

    @app.get("/monitor/template/{kind}", response_class=HTMLResponse)
    async def monitor_template(kind: str, _: str = Depends(panel_auth)) -> str:
        templates = {
            "nodeseek": {"name": "NodeSeek 新帖", "type": "rss", "url": "https://rss.nodeseek.com/", "interval_seconds": 60, "keywords": ["NAT", "优惠", "补货", "VPS", "免费"], "forum": True, "notify_on": {"keyword_match": True, "new_item": True, "price_change": False, "stock_change": False}},
            "linuxdo": {"name": "Linux.do 最新", "type": "rss", "url": "https://linux.do/latest.rss", "interval_seconds": 60, "keywords": ["Claude", "Codex", "API", "VPS", "NAT"], "forum": True, "notify_on": {"keyword_match": True, "new_item": True, "price_change": False, "stock_change": False}},
            "linuxdo-resource": {"name": "Linux.do 资源荟萃", "type": "rss", "url": "https://linux.do/c/resource/14.rss", "interval_seconds": 60, "keywords": ["免费", "开源", "API", "Claude"], "forum": True, "notify_on": {"keyword_match": True, "new_item": True, "price_change": False, "stock_change": False}},
        }
        m = templates.get(kind)
        if not m:
            raise HTTPException(404, "template not found")
        return layout("使用模板新增", "<div class=card><p class=muted>这是预设模板，保存即可加入监控；也可以先调整关键词。</p></div>" + monitor_form_html(m))

    @app.get("/monitor/bulk", response_class=HTMLResponse)
    async def bulk_monitor(_: str = Depends(panel_auth)) -> str:
        sample = """NodeSeek|https://www.nodeseek.com/|免费鸡,优惠码,NAT
Linux.do|https://linux.do|公益,codex,claude
HostLoc|https://hostloc.com|VPS,补货,优惠"""
        body = f"""<div class=card><h2>批量新增监控</h2><p class=muted>一行一个网站，格式：<code>名称|URL|关键词1,关键词2,关键词3</code>。保存后会追加到现有列表，不会覆盖原有监控。</p><form method=post action='/monitor/bulk'><label>批量列表</label><textarea name=items style='min-height:260px' placeholder='{html_escape(sample)}'></textarea><div class=grid><div><label>类型</label><select name=mtype><option value=web>Web 网页</option><option value=rss>RSS</option></select></div><div><label>间隔秒数（最低 60）</label><input name=interval_seconds type=number min=60 value=60></div></div><h3>默认提醒条件</h3><label><input style='width:auto' type=checkbox name=keyword_match checked> 关键词命中</label><label><input style='width:auto' type=checkbox name=new_item checked> 新条目</label><label><input style='width:auto' type=checkbox name=price_change> 价格变化</label><label><input style='width:auto' type=checkbox name=stock_change> 库存变化</label><p><button class='btn primary' type=submit>批量添加</button> <a class=btn href='/'>取消</a></p></form></div>"""
        return layout("批量新增", body)

    @app.post("/monitor/bulk")
    async def bulk_monitor_save(_: str = Depends(panel_auth), items: str = Form(""), mtype: str = Form("web"), interval_seconds: int = Form(300), keyword_match: str | None = Form(None), new_item: str | None = Form(None), price_change: str | None = Form(None), stock_change: str | None = Form(None)):
        cfg = cfg_load_fresh()
        monitors = cfg.setdefault("monitors", [])
        added = 0
        errors = []
        for line_no, raw in enumerate(items.splitlines(), 1):
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            parts = [x.strip() for x in line.split('|')]
            if len(parts) < 2:
                errors.append(f"第 {line_no} 行格式错误：{html_escape(line)}")
                continue
            name, url = parts[0], parts[1]
            keywords = parts[2] if len(parts) >= 3 else ""
            try:
                monitors.append(monitor_from_form(None, name, mtype, url, interval_seconds, keywords.replace(',', '\n'), "article, .thread, .post, li", "h1, h2, h3, a", "a", "", "", bool(keyword_match), bool(new_item), bool(price_change), bool(stock_change)))
                added += 1
            except Exception as e:
                errors.append(f"第 {line_no} 行失败：{html_escape(e)}")
        try:
            cfg_save(cfg)
        except Exception as e:
            logger.exception("bulk save failed")
            return HTMLResponse(layout("批量新增失败", f"<div class=card><pre>{html_escape(e)}</pre></div>"), status_code=500)
        if errors:
            return HTMLResponse(layout("批量新增完成", f"<div class=msg>已新增 {added} 个，部分行有问题：</div><div class=card><pre>{'<br>'.join(errors)}</pre></div><p><a class=btn href='/'>返回</a></p>"))
        return RedirectResponse("/", status_code=303)

    @app.get("/monitor/{idx}/edit", response_class=HTMLResponse)
    async def edit_monitor(idx: int, _: str = Depends(panel_auth)) -> str:
        monitors = cfg_load_fresh().get("monitors") or []
        if idx < 0 or idx >= len(monitors):
            raise HTTPException(404, "monitor not found")
        return layout("编辑监控", "<h2>编辑监控</h2>" + monitor_form_html(monitors[idx], idx))

    async def save_form_common(
        original_index: int | None,
        name: str,
        mtype: str,
        url: str,
        interval_seconds: int,
        keywords: str,
        item_selector: str,
        title_selector: str,
        link_selector: str,
        price_selector: str,
        stock_selector: str,
        keyword_match: str | None,
        new_item: str | None,
        price_change: str | None,
        stock_change: str | None,
    ) -> RedirectResponse:
        cfg = cfg_load_fresh()
        monitors = cfg.setdefault("monitors", [])
        m = monitor_from_form(original_index, name, mtype, url, interval_seconds, keywords, item_selector, title_selector, link_selector, price_selector, stock_selector, bool(keyword_match), bool(new_item), bool(price_change), bool(stock_change))
        if original_index is None:
            monitors.append(m)
        else:
            monitors[original_index] = m
        try:
            cfg_save(cfg)
        except Exception as e:
            logger.exception("save monitor failed")
            return HTMLResponse(layout("保存失败", f"<div class=card><h2>保存失败</h2><pre>{html_escape(e)}</pre></div><p><a class=btn href='/'>返回</a></p>"), status_code=500)
        return RedirectResponse("/", status_code=303)

    @app.post("/monitor/create")
    async def create_monitor(_: str = Depends(panel_auth), name: str = Form(...), mtype: str = Form(...), url: str = Form(...), interval_seconds: int = Form(300), keywords: str = Form(""), item_selector: str = Form(""), title_selector: str = Form(""), link_selector: str = Form(""), price_selector: str = Form(""), stock_selector: str = Form(""), keyword_match: str | None = Form(None), new_item: str | None = Form(None), price_change: str | None = Form(None), stock_change: str | None = Form(None)) -> RedirectResponse:
        return await save_form_common(None, name, mtype, url, interval_seconds, keywords, item_selector, title_selector, link_selector, price_selector, stock_selector, keyword_match, new_item, price_change, stock_change)

    @app.post("/monitor/save")
    async def save_monitor(_: str = Depends(panel_auth), original_index: int = Form(...), name: str = Form(...), mtype: str = Form(...), url: str = Form(...), interval_seconds: int = Form(300), keywords: str = Form(""), item_selector: str = Form(""), title_selector: str = Form(""), link_selector: str = Form(""), price_selector: str = Form(""), stock_selector: str = Form(""), keyword_match: str | None = Form(None), new_item: str | None = Form(None), price_change: str | None = Form(None), stock_change: str | None = Form(None)) -> RedirectResponse:
        return await save_form_common(original_index, name, mtype, url, interval_seconds, keywords, item_selector, title_selector, link_selector, price_selector, stock_selector, keyword_match, new_item, price_change, stock_change)

    @app.get("/monitor/{idx}/delete")
    async def delete_monitor(idx: int, _: str = Depends(panel_auth)) -> RedirectResponse:
        cfg = cfg_load_fresh(); monitors = cfg.get("monitors") or []
        if 0 <= idx < len(monitors):
            monitors.pop(idx); cfg_save(cfg)
        return RedirectResponse("/", status_code=303)


    @app.get("/monitor/{idx}/preview", response_class=HTMLResponse)
    async def monitor_preview(idx: int, _: str = Depends(panel_auth)) -> str:
        cfg = cfg_load_fresh(); monitors = cfg.get("monitors") or []
        if idx < 0 or idx >= len(monitors):
            raise HTTPException(404, "monitor not found")
        m = monitors[idx]
        timeout = int((cfg.get("http") or {}).get("timeout_seconds", 20))
        ua = (cfg.get("http") or {}).get("user_agent") or DEFAULT_UA
        headers = {"User-Agent": ua}
        try:
            async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
                body = await fetch_url(client, m.get("url"))
            items = parse_rss_items(m, body) if m.get("type") == "rss" else parse_web_items(m, body)
            rows=[]
            for it in items[:15]:
                blocked, br = item_blocked(it, m)
                hits = keyword_hits(f"{it.title} {it.text}", m.get("keywords") or [])
                rows.append(f"""<tr><td>{html_escape(it.title)}<br><small>{html_escape(it.link)}</small></td><td>{html_escape(it.author or '-')}</td><td>{html_escape(it.category or '-')}</td><td>{html_escape(', '.join(hits) or '-')}</td><td>{'跳过: '+html_escape(br) if blocked else '可推送/记录'}</td></tr>""")
            body_html = "<div class=card><h2>抓取预览</h2><p class=muted>只预览最近 15 条，不写入去重状态、不推送。</p><table><tr><th>标题/链接</th><th>作者</th><th>分类</th><th>命中</th><th>状态</th></tr>" + "".join(rows) + "</table></div>"
            return layout("抓取预览", body_html)
        except Exception as e:
            return HTMLResponse(layout("抓取预览失败", f"<div class=card><pre>{html_escape(e)}</pre></div>"), status_code=500)

    @app.get("/monitor/{idx}/run", response_class=HTMLResponse)
    async def run_monitor_now(idx: int, _: str = Depends(panel_auth)) -> str:
        cfg = cfg_load_fresh(); monitors = cfg.get("monitors") or []
        if idx < 0 or idx >= len(monitors):
            raise HTTPException(404, "monitor not found")
        count = await run_monitor(monitors[idx])
        return layout("检查完成", f"<div class=msg>已手动检查：{html_escape(monitors[idx].get('name'))}，推送 {count} 条。</div><p><a class=btn href='/'>返回</a></p>")

    @app.get("/run-once", response_class=HTMLResponse)
    async def run_once_page(_: str = Depends(panel_auth)) -> str:
        await run_all_monitors_once()
        return layout("手动检查完成", "<div class=msg>已执行全部监控检查。具体结果看日志/Telegram 推送。</div><p><a class=btn href='/'>返回</a></p>")

    @app.get("/yaml", response_class=HTMLResponse)
    async def yaml_edit(_: str = Depends(panel_auth)) -> str:
        text = CONFIG_PATH.read_text(encoding="utf-8")
        return layout("YAML 高级编辑", f"<h2>YAML 高级编辑</h2><form method=post><textarea name=content style='min-height:520px'>{html_escape(text)}</textarea><p><button class='btn primary' type=submit>保存 YAML</button></p></form>")

    @app.post("/yaml", response_class=HTMLResponse)
    async def yaml_save(_: str = Depends(panel_auth), content: str = Form(...)) -> str:
        try:
            data = yaml.safe_load(content) or {}
            cfg_save(data)
            return layout("已保存", "<div class=msg>YAML 已保存并重载。</div><p><a class=btn href='/'>返回</a></p>")
        except Exception as e:
            return layout("保存失败", f"<div class=card><h2>保存失败</h2><pre>{html_escape(e)}</pre></div><p><a class=btn href='/yaml'>返回</a></p>")

    @app.get("/settings", response_class=HTMLResponse)
    async def settings(_: str = Depends(panel_auth)) -> str:
        v = env_values()
        cleanup = (cfg_load_fresh().get("cleanup") or {})
        body = f"""<h2>Bot / 面板设置</h2><div class=card><form method=post>
<label>Telegram Bot Token</label><input name=TELEGRAM_BOT_TOKEN value='{html_escape(v['TELEGRAM_BOT_TOKEN'])}' placeholder='123456:ABC...'>
<label>管理员 ADMIN_CHAT_ID</label><input name=ADMIN_CHAT_ID value='{html_escape(v['ADMIN_CHAT_ID'])}'>
<div class=grid><div><label>日志级别</label><input name=LOG_LEVEL value='{html_escape(v['LOG_LEVEL'])}'></div><div><label>面板监听地址</label><input name=WEB_PANEL_HOST value='{html_escape(v['WEB_PANEL_HOST'])}'></div><div><label>面板端口</label><input name=WEB_PANEL_PORT value='{html_escape(v['WEB_PANEL_PORT'])}'></div><div><label>面板用户</label><input name=WEB_PANEL_USER value='{html_escape(v['WEB_PANEL_USER'])}'></div><div><label>面板密码</label><input name=WEB_PANEL_PASSWORD value='{html_escape(v['WEB_PANEL_PASSWORD'])}'></div></div>
<h3>监控数据自动清理</h3><p class=muted>只清理 RSS/网站监控状态和去重记录，不删除用户、收件箱、双向对话消息。</p><div class=grid><div><label>清理间隔（分钟）</label><input name=CLEANUP_INTERVAL_MINUTES type=number min=1 value='{html_escape(cleanup.get("interval_minutes", 60))}'></div><div><label>保留监控数据（分钟）</label><input name=CLEANUP_RETENTION_MINUTES type=number min=1 value='{html_escape(cleanup.get("monitor_retention_minutes", 1440))}'></div></div>
<input type=hidden name=WEB_PANEL_ENABLED value='true'><p><button class='btn primary' type=submit>保存 .env / 清理设置</button></p><small>改 Token、chat_id 或面板监听端口后建议重启服务：sudo systemctl restart tg-watchbot</small></form></div>"""
        return layout("设置", body)

    @app.post("/settings", response_class=HTMLResponse)
    async def settings_save(_: str = Depends(panel_auth), TELEGRAM_BOT_TOKEN: str = Form(""), ADMIN_CHAT_ID: str = Form(""), LOG_LEVEL: str = Form("INFO"), WEB_PANEL_ENABLED: str = Form("true"), WEB_PANEL_HOST: str = Form("127.0.0.1"), WEB_PANEL_PORT: str = Form("8765"), WEB_PANEL_USER: str = Form("admin"), WEB_PANEL_PASSWORD: str = Form("admin"), CLEANUP_INTERVAL_MINUTES: int = Form(60), CLEANUP_RETENTION_MINUTES: int = Form(1440)) -> str:
        write_env_values(locals() | {"WEB_PANEL_ENABLED": WEB_PANEL_ENABLED})
        cfg = cfg_load_fresh()
        cfg["cleanup"] = {
            "enabled": True,
            "interval_minutes": max(1, int(CLEANUP_INTERVAL_MINUTES)),
            "monitor_retention_minutes": max(1, int(CLEANUP_RETENTION_MINUTES)),
        }
        cfg_save(cfg)
        return layout("已保存", "<div class=msg>.env 和监控清理设置已保存。Token/chat_id 需重启服务后生效。</div><p><a class=btn href='/settings'>返回</a></p>")


    @app.get("/send", response_class=HTMLResponse)
    async def send_page(_: str = Depends(panel_auth)) -> str:
        with closing(db()) as conn:
            users = conn.execute(
                "SELECT user_id, username, full_name, blocked, note, updated_at FROM users ORDER BY updated_at DESC LIMIT 200"
            ).fetchall()
        options = []
        for u in users:
            blocked = "（已封禁）" if u["blocked"] else ""
            username = f"@{u['username']}" if u["username"] else ""
            label = f"{u['full_name'] or u['user_id']} {username} · {u['user_id']} {blocked}"
            options.append(f"<option value='{u['user_id']}'>{html_escape(label)}</option>")
        body = f"""<div class=card><h2>主动发消息</h2><p class=muted>只能发送给已经私聊过 Bot 的用户；这是 Telegram Bot API 限制。</p><form method=post action='/send'>
<label>选择用户</label><select name=user_id>{''.join(options)}</select>
<label>或手动输入 user_id</label><input name=manual_user_id placeholder='例如 123456789'>
<label>消息内容</label><textarea name=text style='min-height:180px' required></textarea>
<p><button class='btn primary' type=submit>发送消息</button> <a class=btn href='/inbox'>查看收件箱</a></p></form></div>"""
        return layout("主动发消息", body)

    @app.post("/send", response_class=HTMLResponse)
    async def send_save(_: str = Depends(panel_auth), user_id: str = Form(""), manual_user_id: str = Form(""), text: str = Form("")) -> str:
        raw_uid = (manual_user_id or user_id or "").strip()
        if not raw_uid:
            return layout("发送失败", "<div class=card><pre>缺少 user_id</pre></div><p><a class=btn href='/send'>返回</a></p>")
        if not text.strip():
            return layout("发送失败", "<div class=card><pre>消息内容不能为空</pre></div><p><a class=btn href='/send'>返回</a></p>")
        try:
            uid = int(raw_uid)
            if not get_user(uid):
                return layout("发送失败", f"<div class=card><pre>找不到用户 {uid}，对方需要先私聊 Bot。</pre></div><p><a class=btn href='/send'>返回</a></p>")
            if is_blocked(uid):
                return layout("发送失败", f"<div class=card><pre>用户 {uid} 已被封禁，请先 /unblock。</pre></div><p><a class=btn href='/send'>返回</a></p>")
            if not bot:
                return layout("发送失败", "<div class=card><pre>Bot 尚未初始化；请确认服务以正常模式运行，而不是 --panel-only。</pre></div><p><a class=btn href='/send'>返回</a></p>")
            sent = await bot.send_message(uid, text.strip())
            logger.info("panel sent message to user_id=%s message_id=%s", uid, sent.message_id)
            await admin_send(f"[主动发送成功]\nuser_id: <code>{uid}</code>\nmessage_id: {sent.message_id}\n时间：{html_escape(now_iso())}")
            return layout("发送成功", f"<div class=msg>已发送给用户 {uid}，message_id={sent.message_id}。Bot 也已给管理员发送确认提醒。</div><p><a class=btn href='/send'>继续发送</a> <a class=btn href='/inbox'>收件箱</a></p>")
        except TelegramAPIError as e:
            logger.exception("panel send failed")
            return layout("发送失败", f"<div class=card><pre>{html_escape(e)}</pre></div><p><a class=btn href='/send'>返回</a></p>")
        except Exception as e:
            logger.exception("panel send failed")
            return layout("发送失败", f"<div class=card><pre>{html_escape(e)}</pre></div><p><a class=btn href='/send'>返回</a></p>")

    @app.get("/inbox", response_class=HTMLResponse)
    async def inbox_page(_: str = Depends(panel_auth)) -> str:
        with closing(db()) as conn:
            rows = conn.execute("SELECT * FROM inbox_messages ORDER BY id DESC LIMIT 200").fetchall()
        trs = []
        for r in rows:
            status_txt = "已转发" if r["forwarded"] else "未转发"
            status_cls = "ok" if r["forwarded"] else "danger"
            content = html_escape(r["text"] or "(非文本/媒体消息)")
            trs.append(f"""<tr><td>#{r['id']}<br><span class='badge {status_cls}'>{status_txt}</span></td><td><b>{html_escape(r['full_name'])}</b><br><small>{r['user_id']} @{html_escape(r['username'] or '')}</small></td><td>{html_escape(r['message_type'])}<br><small>{html_escape(r['created_at'])}</small></td><td>{content}<br><small style='color:#fca5a5'>{html_escape(r['error'] or '')}</small></td><td><a class=btn href='/inbox/{r['id']}/retry'>重试转发</a></td></tr>""")
        body = "<div class=card><h2>收件箱</h2><p class=muted>用户消息会先写入 SQLite，再转发给管理员；转发失败的消息可以在这里重试。</p><table><tr><th>ID/状态</th><th>用户</th><th>类型/时间</th><th>内容/错误</th><th>操作</th></tr>" + "".join(trs) + "</table></div>"
        return layout("收件箱", body)

    @app.get("/inbox/{msg_id}/retry")
    async def retry_inbox(msg_id: int, _: str = Depends(panel_auth)) -> RedirectResponse:
        with closing(db()) as conn:
            conn.execute("UPDATE inbox_messages SET forwarded=0, error=NULL WHERE id=?", (msg_id,))
            conn.commit()
        await flush_pending_inbox()
        return RedirectResponse("/inbox", status_code=303)

    @app.get("/restart", response_class=HTMLResponse)
    async def restart_page(_: str = Depends(panel_auth)) -> str:
        body = """<div class=card><h2>重启机器人</h2><p class=muted>用于修改 Token、管理员 ID、面板设置等需要重启生效的配置。</p><form method=post action='/restart'><button class='btn danger' type=submit>确认重启 tg-watchbot</button></form></div>"""
        return layout("重启机器人", body)

    @app.post("/restart")
    async def restart_post(_: str = Depends(panel_auth)) -> HTMLResponse:
        async def delayed_restart():
            await asyncio.sleep(1.0)
            # Exit with failure so systemd Restart=on-failure brings the service back up.
            os._exit(1)
        asyncio.create_task(delayed_restart())
        return HTMLResponse(layout("正在重启", "<div class=msg>已发送重启命令，约 5-10 秒后刷新页面。</div><p><a class=btn href='/'>返回首页</a></p>"))

    @app.get("/logs", response_class=HTMLResponse)
    async def logs(_: str = Depends(panel_auth)) -> str:
        text = LOG_PATH.read_text(encoding="utf-8", errors="replace")[-20000:] if LOG_PATH.exists() else "暂无日志"
        return layout("日志", f"<h2>最近应用日志</h2><pre>{html_escape(text)}</pre>")

    @app.get("/health", response_class=PlainTextResponse)
    async def health() -> str:
        return "ok"

    return app


async def start_panel_server() -> uvicorn.Server | None:
    if not panel_enabled():
        logger.info("web panel disabled")
        return None
    host = os.getenv("WEB_PANEL_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_PANEL_PORT", "8765"))
    server = uvicorn.Server(uvicorn.Config(create_panel_app(), host=host, port=port, log_level="info"))
    asyncio.create_task(server.serve())
    logger.info("web panel listening on http://%s:%s", host, port)
    return server

def validate_env() -> tuple[str, int]:
    load_dotenv(ENV_PATH)
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    admin = os.getenv("ADMIN_CHAT_ID", "").strip()
    if not token:
        raise RuntimeError(f"TELEGRAM_BOT_TOKEN is missing in {ENV_PATH}")
    if not admin:
        raise RuntimeError(f"ADMIN_CHAT_ID is missing in {ENV_PATH}")
    return token, int(admin)


async def main_async(run_once: bool = False, panel_only: bool = False) -> None:
    global bot, admin_chat_id, config, scheduler_ref
    load_dotenv(ENV_PATH, override=True)
    config = load_config()
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    init_db()
    if panel_only:
        await start_panel_server()
        logger.info("panel-only mode start")
        while True:
            await asyncio.sleep(3600)
    if run_once:
        # If .env is filled, send notifications during manual test; otherwise just log.
        try:
            token, admin_chat_id = validate_env()
            bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        except Exception as e:
            logger.warning("run-once without Telegram notification: %s", e)
        await run_all_monitors_once()
        if bot:
            await bot.session.close()
        return
    token, admin_chat_id = validate_env()
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
    scheduler_ref = scheduler
    schedule_monitors(scheduler)
    scheduler.start()
    await start_panel_server()
    asyncio.create_task(flush_pending_loop())
    asyncio.create_task(cleanup_monitor_loop())
    await admin_send(f"tg-watchbot 已启动\n时间：{now_iso()}")
    logger.info("bot polling start")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-once", action="store_true", help="run all monitors once and exit; does not need Telegram token unless notification is sent")
    parser.add_argument("--panel-only", action="store_true", help="start only the web admin panel, useful before Telegram token is configured")
    args = parser.parse_args()
    try:
        asyncio.run(main_async(run_once=args.run_once, panel_only=args.panel_only))
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("fatal error")
        raise


if __name__ == "__main__":
    main()
