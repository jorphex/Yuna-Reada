import warnings
warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API\..*",
    category=UserWarning,
)

import asyncio
import logging
import os
import re
import sys
import time
import calendar
import sqlite3
import html
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import requests
from dotenv import load_dotenv
import feedparser
from bs4 import BeautifulSoup as bs
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
    JobQueue
)
import nest_asyncio
nest_asyncio.apply()

load_dotenv()

sys.stdout = sys.stderr

# --- Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_PATH = os.getenv("YUNAREADA_DB", "yunareada.db")

def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=3000")
    return conn

def init_db():
    with _get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS feeds (
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                name TEXT NOT NULL,
                latest_ts REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, url)
            );
            CREATE TABLE IF NOT EXISTS blocked_words (
                chat_id INTEGER NOT NULL,
                word TEXT NOT NULL,
                PRIMARY KEY (chat_id, word)
            );
            CREATE TABLE IF NOT EXISTS seen_entries (
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                entry_id TEXT NOT NULL,
                seen_ts REAL NOT NULL,
                PRIMARY KEY (chat_id, url, entry_id)
            );
            CREATE TABLE IF NOT EXISTS feed_failures (
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                count INTEGER NOT NULL,
                next_retry REAL NOT NULL,
                last_status INTEGER NOT NULL,
                PRIMARY KEY (chat_id, url)
            );
            CREATE TABLE IF NOT EXISTS settings (
                chat_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (chat_id, key)
            );
            CREATE INDEX IF NOT EXISTS idx_seen_entries_seen_ts
                ON seen_entries (seen_ts);
            """
        )

def get_chat_ids():
    with _get_db() as conn:
        rows = conn.execute("SELECT DISTINCT chat_id FROM feeds").fetchall()
    return [row["chat_id"] for row in rows]

def get_feeds(chat_id: int):
    with _get_db() as conn:
        rows = conn.execute(
            "SELECT url, name, latest_ts FROM feeds WHERE chat_id = ? ORDER BY name",
            (chat_id,),
        ).fetchall()
    return [{"xmlUrl": row["url"], "text": row["name"], "latest_ts": row["latest_ts"]} for row in rows]

def feed_exists(chat_id: int, url: str) -> bool:
    with _get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM feeds WHERE chat_id = ? AND url = ?",
            (chat_id, url),
        ).fetchone()
    return row is not None

def upsert_feed(chat_id: int, url: str, name: str):
    with _get_db() as conn:
        conn.execute(
            """
            INSERT INTO feeds (chat_id, url, name, latest_ts)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(chat_id, url) DO UPDATE SET name = excluded.name
            """,
            (chat_id, url, name),
        )

def remove_feed(chat_id: int, url: str):
    with _get_db() as conn:
        conn.execute("DELETE FROM feeds WHERE chat_id = ? AND url = ?", (chat_id, url))
        conn.execute("DELETE FROM seen_entries WHERE chat_id = ? AND url = ?", (chat_id, url))
        conn.execute("DELETE FROM feed_failures WHERE chat_id = ? AND url = ?", (chat_id, url))

def update_latest_ts(chat_id: int, url: str, latest_ts: float):
    with _get_db() as conn:
        conn.execute(
            """
            UPDATE feeds
            SET latest_ts = CASE WHEN latest_ts < ? THEN ? ELSE latest_ts END
            WHERE chat_id = ? AND url = ?
            """,
            (latest_ts, latest_ts, chat_id, url),
        )

def get_blocked_words(chat_id: int):
    with _get_db() as conn:
        rows = conn.execute(
            "SELECT word FROM blocked_words WHERE chat_id = ?",
            (chat_id,),
        ).fetchall()
    return {row["word"] for row in rows}

def add_blocked_word(chat_id: int, word: str):
    with _get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO blocked_words (chat_id, word) VALUES (?, ?)",
            (chat_id, word),
        )

def remove_blocked_word(chat_id: int, word: str):
    with _get_db() as conn:
        conn.execute(
            "DELETE FROM blocked_words WHERE chat_id = ? AND word = ?",
            (chat_id, word),
        )

def get_setting(chat_id: int, key: str, default: str | None = None) -> str | None:
    with _get_db() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE chat_id = ? AND key = ?",
            (chat_id, key),
        ).fetchone()
    return row["value"] if row else default

def set_setting(chat_id: int, key: str, value: str):
    with _get_db() as conn:
        conn.execute(
            """
            INSERT INTO settings (chat_id, key, value)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, key) DO UPDATE SET value = excluded.value
            """,
            (chat_id, key, value),
        )

def get_feed_failures(chat_id: int):
    with _get_db() as conn:
        rows = conn.execute(
            "SELECT url, count, next_retry, last_status FROM feed_failures WHERE chat_id = ? ORDER BY next_retry",
            (chat_id,),
        ).fetchall()
    return [dict(row) for row in rows]

def get_feed_failure(chat_id: int, url: str):
    with _get_db() as conn:
        row = conn.execute(
            "SELECT count, next_retry, last_status FROM feed_failures WHERE chat_id = ? AND url = ?",
            (chat_id, url),
        ).fetchone()
    return dict(row) if row else None

def clear_feed_failure(chat_id: int, url: str):
    with _get_db() as conn:
        conn.execute(
            "DELETE FROM feed_failures WHERE chat_id = ? AND url = ?",
            (chat_id, url),
        )

def record_feed_failure(chat_id: int, url: str, status_code: int, retry_delay: int | None):
    now = time.time()
    failure = get_feed_failure(chat_id, url) or {"count": 0, "next_retry": 0, "last_status": None}
    count = failure["count"] + 1
    if status_code == 404:
        base = 24 * 60 * 60
    elif status_code == 403:
        base = 6 * 60 * 60
    elif status_code == 429:
        base = 15 * 60
    else:
        base = 30 * 60
    delay = max(base, retry_delay or 0)
    delay = min(int(delay * (1.5 ** (count - 1))), 24 * 60 * 60)
    with _get_db() as conn:
        conn.execute(
            """
            INSERT INTO feed_failures (chat_id, url, count, next_retry, last_status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, url) DO UPDATE SET
                count = excluded.count,
                next_retry = excluded.next_retry,
                last_status = excluded.last_status
            """,
            (chat_id, url, count, now + delay, status_code),
        )

def mark_seen_entries(chat_id: int, url: str, entry_ids: list[str]):
    if not entry_ids:
        return
    now = time.time()
    with _get_db() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO seen_entries (chat_id, url, entry_id, seen_ts) VALUES (?, ?, ?, ?)",
            [(chat_id, url, entry_id, now) for entry_id in entry_ids],
        )

def get_seen_entry_ids(chat_id: int, url: str, entry_ids: list[str]) -> set[str]:
    if not entry_ids:
        return set()
    seen = set()
    with _get_db() as conn:
        for i in range(0, len(entry_ids), 900):
            chunk = entry_ids[i:i + 900]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"""
                SELECT entry_id FROM seen_entries
                WHERE chat_id = ? AND url = ? AND entry_id IN ({placeholders})
                """,
                (chat_id, url, *chunk),
            ).fetchall()
            seen.update(row["entry_id"] for row in rows)
    return seen

def prune_seen_entries(chat_id: int, url: str, max_age_days: int = 90):
    cutoff = time.time() - (max_age_days * 24 * 60 * 60)
    with _get_db() as conn:
        conn.execute(
            "DELETE FROM seen_entries WHERE chat_id = ? AND url = ? AND seen_ts < ?",
            (chat_id, url, cutoff),
        )

# Guard against overlapping update cycles
UPDATE_LOCK = asyncio.Lock()

# Conversation states
FEED_URL, REMOVE_FEED, BLOCK_WORD, UNBLOCK_WORD = range(1, 5)

# --- Scheduling Helpers ---
def _next_aligned_run(now_ts: float, frequency: str) -> float:
    now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    if frequency == "daily":
        next_midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
        return next_midnight.timestamp()

    minutes_since_midnight = now.hour * 60 + now.minute
    next_slot = (minutes_since_midnight // 15 + 1) * 15
    next_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    if next_slot >= 24 * 60:
        next_day += timedelta(days=1)
        next_slot = 0
    return (next_day + timedelta(minutes=next_slot)).timestamp()

def _get_frequency(chat_id: int) -> str:
    freq = get_setting(chat_id, "frequency", "15m") or "15m"
    return freq if freq in {"15m", "daily"} else "15m"

def _set_next_run(chat_id: int, next_run_ts: float):
    set_setting(chat_id, "next_run", str(next_run_ts))

def _get_next_run(chat_id: int) -> float | None:
    value = get_setting(chat_id, "next_run")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None

# --- RSS Fetching (blocking) ---
def _entry_id(entry) -> str:
    return (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or ((entry.get("title") or "") + (entry.get("summary") or ""))
    )

def fetch_rss(chat_id: int, url: str, latest_post_time: float, blocked_words: set[str]):
    start_time = time.time()
    # logging.info(f"Start fetching RSS for chat {chat_id}, URL {url}")

    try:
        failure = get_feed_failure(chat_id, url)
        if failure and time.time() < failure.get("next_retry", 0):
            logging.info(
                f"Skipping fetch for {url}; retry after "
                f"{datetime.fromtimestamp(failure['next_retry'], tz=timezone.utc)}"
            )
            return [], [], latest_post_time, None

        response = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "application/rss+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=(5, 20),
        )
        if response.status_code in (403, 404, 429):
            retry_after = response.headers.get("Retry-After")
            retry_delay = None
            if retry_after:
                if retry_after.isdigit():
                    retry_delay = int(retry_after)
                else:
                    try:
                        retry_dt = parsedate_to_datetime(retry_after)
                        retry_delay = int((retry_dt - datetime.now(tz=timezone.utc)).total_seconds())
                    except Exception:
                        retry_delay = None
            record_feed_failure(chat_id, url, response.status_code, retry_delay)
            logging.warning(f"HTTP {response.status_code} fetching RSS for {url}")
            return [], [], latest_post_time, None
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        entries = []
        blocked_entries = []
        new_latest_post_time = latest_post_time
        clear_feed_failure(chat_id, url)

        raw_items = []
        for entry in feed.entries:
            entry_time = entry.get("published_parsed") or entry.get("updated_parsed")
            if entry_time:
                entry_timestamp = calendar.timegm(entry_time)
                has_timestamp = True
                if entry_timestamp < latest_post_time:
                    continue
            else:
                entry_timestamp = time.time()
                has_timestamp = False

            entry_id = _entry_id(entry)
            if not entry_id:
                entry_id = f"{entry_timestamp}:{entry.get('title', '')}"
            raw_items.append((entry, entry_timestamp, has_timestamp, entry_id))

        seen_lookup = get_seen_entry_ids(chat_id, url, [item[3] for item in raw_items])
        seen_entry_ids = []
        for entry, entry_timestamp, has_timestamp, entry_id in raw_items:
            if entry_id in seen_lookup:
                continue
            entry._entry_id = entry_id
            entry._timestamp = entry_timestamp
            entry._has_timestamp = has_timestamp
            seen_entry_ids.append(entry_id)

            # Clean HTML from description/summary
            entry.title = entry.get("title") or "Untitled"
            entry.description = bs(entry.get("description", ""), "html.parser").get_text()
            entry.summary = bs(entry.get("summary", entry.description), "html.parser").get_text()

            content = (entry.title + " " + entry.description + " " + entry.summary).lower()
            if any(word in content for word in blocked_words):
                blocked_entries.append(entry)
            else:
                entries.append(entry)

        if entries or blocked_entries:
            ts_candidates = [entry._timestamp for entry in entries + blocked_entries if entry._has_timestamp]
            if ts_candidates:
                new_latest_post_time = max(ts_candidates)
        if hasattr(feed, "feed") and getattr(feed, "feed", None):
            feed_name = feed.feed.get("title") or "Unknown Feed"
        else:
            feed_name = "Unknown Feed"
        # logging.info(f"Fetched {len(entries)} entries from {feed_name} (URL: {url})")

        mark_seen_entries(chat_id, url, seen_entry_ids)
        prune_seen_entries(chat_id, url)

    except requests.RequestException as e:
        logging.error(f"HTTP error fetching RSS for chat {chat_id}, URL {url}: {e}", exc_info=True)
        return [], [], latest_post_time, None
    except Exception as e:
        logging.error(f"Error fetching RSS for chat {chat_id}, URL {url}: {e}", exc_info=True)
        return [], [], latest_post_time, None

    # logging.info(f"Finished fetching RSS for chat {chat_id}, URL {url} in {time.time()-start_time:.2f}s")
    return entries, blocked_entries, new_latest_post_time, feed_name

# --- Update Functions ---
async def fetch_and_send_updates(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    feeds = get_feeds(chat_id)
    if not feeds:
        return
    bot = context.bot
    all_entries = []
    blocked_entries = []
    blocked_words = get_blocked_words(chat_id)

    # Run fetch_rss for each feed in a thread
    tasks = [
        asyncio.to_thread(fetch_rss, chat_id, feed["xmlUrl"], feed["latest_ts"], blocked_words)
        for feed in feeds
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    for result, feed in zip(results, feeds):
        fetched_entries, blocked, new_latest, feed_name = result
        if fetched_entries:
            all_entries.extend(fetched_entries)
        if blocked:
            blocked_entries.extend(blocked)
        if new_latest is not None and new_latest > feed.get("latest_ts", 0):
            update_latest_ts(chat_id, feed["xmlUrl"], new_latest)
        # feed_label = feed_name or feed.get("text") or feed.get("xmlUrl")
        # if not fetched_entries and not blocked:
            # logging.warning(f"No new entries from {feed_label}")

    # Sort by published time (most recent first)
    all_entries.sort(key=lambda e: e._timestamp, reverse=True)

    digest_enabled = get_setting(chat_id, "digest", "0") == "1"
    if digest_enabled:
        await _send_digest(bot, chat_id, all_entries)
    else:
        # Send each update sequentially to preserve ordering and avoid rate spikes.
        for entry in all_entries:
            try:
                published = datetime.fromtimestamp(entry._timestamp, tz=timezone.utc)
                timestamp = published.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                content = entry.description if len(entry.description) < len(entry.summary) else entry.summary
                content = bs(content, "html.parser").get_text(separator="\n")
                if len(content) > 400:
                    content = content[:397] + "..."
                safe_title = html.escape(entry.title)
                safe_content = html.escape(content)
                safe_link = html.escape(entry.link or "", quote=True)
                message = (f"<a href='{safe_link}'><b>{safe_title}</b></a>\n"
                           f"<code>{timestamp}</code>\n\n{safe_content}")
                await bot.send_message(chat_id=chat_id, text=message, parse_mode=ParseMode.HTML)
                await asyncio.sleep(0.25)
            except Exception as e:
                logging.error(f"Error sending message for chat {chat_id}: {e}", exc_info=True)

    # Send summary of blocked entries (if any)
    if blocked_entries:
        blocked_message = f"🚫 Blocked {len(blocked_entries)} new posts\n"
        for entry in blocked_entries:
            safe_title = html.escape(entry.title)
            safe_link = html.escape(entry.link or "", quote=True)
            blocked_message += f'<a href="{safe_link}">{safe_title}</a>\n'
        await bot.send_message(chat_id=chat_id, text=blocked_message, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await asyncio.sleep(0.25)

async def run_immediate_then_sync(context: ContextTypes.DEFAULT_TYPE):
    if UPDATE_LOCK.locked():
        logging.warning("Skipping immediate sync: previous cycle still running.")
        return
    async with UPDATE_LOCK:
        chat_ids = get_chat_ids()
        if chat_ids:
            await asyncio.gather(*(fetch_and_send_updates(context, chat_id) for chat_id in chat_ids))
        now = time.time()
        for chat_id in chat_ids:
            freq = _get_frequency(chat_id)
            _set_next_run(chat_id, _next_aligned_run(now, freq))

async def scheduled_tick(context: ContextTypes.DEFAULT_TYPE):
    if UPDATE_LOCK.locked():
        logging.warning("Skipping scheduled tick: previous cycle still running.")
        return
    async with UPDATE_LOCK:
        now = time.time()
        chat_ids = get_chat_ids()
        if not chat_ids:
            return

        due = []
        for chat_id in chat_ids:
            freq = _get_frequency(chat_id)
            next_run = _get_next_run(chat_id)
            if next_run is None:
                _set_next_run(chat_id, _next_aligned_run(now, freq))
                continue
            if now >= next_run:
                due.append(chat_id)

        if due:
            await asyncio.gather(*(fetch_and_send_updates(context, chat_id) for chat_id in due))
            now = time.time()
            for chat_id in due:
                freq = _get_frequency(chat_id)
                _set_next_run(chat_id, _next_aligned_run(now, freq))

async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text("🔍 Looking for new posts... This might take a moment!")
    await fetch_and_send_updates(context, chat_id)
    await update.message.reply_text("🌟 Refreshed all feeds and sent new updates!")

async def _send_digest(bot, chat_id: int, entries: list):
    if not entries:
        return
    header = f"📰 New posts ({len(entries)})"
    lines = [header]
    for entry in entries:
        published = datetime.fromtimestamp(entry._timestamp, tz=timezone.utc)
        timestamp = published.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
        safe_title = html.escape(entry.title)
        safe_link = html.escape(entry.link or "", quote=True)
        lines.append(f"<a href='{safe_link}'>{safe_title}</a> — <code>{timestamp}</code>")

    chunks = []
    current = []
    current_len = 0
    for line in lines:
        line_len = len(line) + 1
        if current and current_len + line_len > 3500:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += line_len
    if current:
        chunks.append("\n".join(current))

    for chunk in chunks:
        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await asyncio.sleep(0.25)

# --- OPML Helpers ---
def write_opml(feeds, filename):
    root = ET.Element("opml")
    body = ET.SubElement(root, "body")
    outline_parent = ET.SubElement(body, "outline")
    for feed in feeds:
        outline = ET.SubElement(outline_parent, "outline")
        text = feed.get("text") or feed.get("xmlUrl") or "Unknown Feed"
        outline.set("text", str(text))
        outline.set("xmlUrl", str(feed.get("xmlUrl", "")))
    indent(root)
    tree = ET.ElementTree(root)
    tree.write(filename, encoding="utf-8", xml_declaration=True)

def indent(elem, level=0):
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for child in elem:
            indent(child, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

# --- Conversation Handlers ---
async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("📄 Please send an RSS feed URL or URLs separated by commas, spaces, or line breaks.")
    return FEED_URL

async def add_receive_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    await update.message.reply_text("👨‍💻 Processing. Please wait!")
    raw_urls = update.message.text.strip()
    url_list = [url.strip() for url in re.split(r",|\n| ", raw_urls) if url.strip()]
    all_entries = []
    blocked_words = get_blocked_words(chat_id)

    for url in url_list:
        if not is_valid_url(url):
            await update.message.reply_text(f"🙃 Invalid URL detected: {url}")
            continue
        if feed_exists(chat_id, url):
            await update.message.reply_text(f"😕 RSS feed is already added: {url}")
            continue
        try:
            fetched_entries, _, new_latest, feed_name = await asyncio.to_thread(
                fetch_rss, chat_id, url, 0, blocked_words
            )
            if not feed_name:
                feed_name = url
            upsert_feed(chat_id, url, feed_name)
            all_entries.extend(fetched_entries)
            if new_latest is not None:
                update_latest_ts(chat_id, url, new_latest)
        except Exception as e:
            logging.error(f"Error fetching URL {url}: {e}", exc_info=True)
            await update.message.reply_text(f"😤 Failed to fetch RSS feed from {url}.")
            continue

    all_entries.sort(key=lambda e: e._timestamp)
    if all_entries:
        await context.bot.send_message(chat_id=chat_id, text="📰 Here are the five latest items from your feeds:")
        latest_entries = sorted(all_entries[-5:], key=lambda e: e._timestamp)
        for entry in latest_entries:
            try:
                published = datetime.fromtimestamp(entry._timestamp, tz=timezone.utc)
                timestamp = published.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                content = entry.description if len(entry.description) < len(entry.summary) else entry.summary
                content = bs(content, "html.parser").get_text(separator="\n")
                if len(content) > 400:
                    content = content[:397] + "..."
                safe_title = html.escape(entry.title)
                safe_content = html.escape(content)
                safe_link = html.escape(entry.link or "", quote=True)
                message = (f"<a href='{safe_link}'><b>{safe_title}</b></a>\n"
                           f"<code>{timestamp}</code>\n\n{safe_content}")
                await context.bot.send_message(chat_id=chat_id, text=message, parse_mode=ParseMode.HTML)
                await asyncio.sleep(0.25)
            except Exception as e:
                logging.error(f"Error sending feed entry: {e}", exc_info=True)
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text="🕒 No recent items found yet. I’ll notify you when new posts appear."
        )

    await update.message.reply_text("☺️ Successfully added the RSS feeds:\n" + "\n".join(url_list))
    await context.bot.send_message(chat_id=chat_id, text="🗃 Here's an XML/OPML file of your feeds for backup:")
    filename = f"{chat_id}_feeds.xml"
    try:
        write_opml(get_feeds(chat_id), filename)
        with open(filename, "rb") as doc:
            await context.bot.send_document(chat_id=chat_id, document=doc)
    except Exception as e:
        logging.error(f"Error sending document: {e}", exc_info=True)
        await update.message.reply_text("😥 Failed to send backup document.")
    return ConversationHandler.END

def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled adding a feed.")
    return ConversationHandler.END

async def remove_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    feeds = get_feeds(chat_id)
    if not feeds:
        await update.message.reply_text("🤷‍♀️ You have no feeds to remove. Use /add to add some.")
        return ConversationHandler.END
    feed_list = sorted([(feed["text"], feed["xmlUrl"]) for feed in feeds], key=lambda x: x[0])
    message = "Your current feeds:\n\n" + "\n".join(f"{name}: {url}" for name, url in feed_list)
    message += "\n\n🤔 Which feed would you like to remove? Send its URL."
    await update.message.reply_text(message)
    return REMOVE_FEED

async def remove_receive_feed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    feed_url = update.message.text.strip().lower()
    feeds = get_feeds(chat_id)
    feed_to_remove = next((feed for feed in feeds if feed["xmlUrl"].lower() == feed_url), None)
    if not feed_to_remove:
        await update.message.reply_text("🤷‍♀️ No matching feed found with that URL.")
        return ConversationHandler.END

    remove_feed(chat_id, feed_to_remove["xmlUrl"])
    opml_filename = f"{chat_id}_feeds.xml"
    try:
        write_opml(get_feeds(chat_id), opml_filename)
        with open(opml_filename, "rb") as opml_file:
            await context.bot.send_document(chat_id=chat_id, document=opml_file)
        await update.message.reply_text(f"🧹 Removed feed: {feed_to_remove['text']}")
    except Exception as e:
        await update.message.reply_text(f"😥 Error updating your feed list: {e}")
    return ConversationHandler.END

async def list_feeds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    feeds = get_feeds(chat_id)
    if not feeds:
        await update.message.reply_text("🤷‍♀️ You do not have any feeds. Use /add to add some.")
    else:
        feed_list = sorted(f"<a href='{feed['xmlUrl']}'>{feed['xmlUrl']}</a>" for feed in feeds)
        await update.message.reply_text("\n".join(feed_list), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        opml_filename = f"{chat_id}_feeds.xml"
        write_opml(feeds, opml_filename)
        with open(opml_filename, "rb") as opml_file:
            await context.bot.send_document(chat_id=chat_id, document=opml_file)

async def block_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("🔍 Enter word(s) or phrase(s) to block (comma-separated is OK):")
    return BLOCK_WORD

async def block_receive_word(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    raw = update.message.text.strip().lower()
    terms = [term.strip() for term in raw.split(",") if term.strip()]
    if not terms:
        await update.message.reply_text("🤷‍♀️ No valid terms found.")
        return ConversationHandler.END
    blocked = get_blocked_words(chat_id)
    added = []
    already = []
    for term in terms:
        if term in blocked:
            already.append(term)
        else:
            add_blocked_word(chat_id, term)
            added.append(term)
    if added:
        await update.message.reply_text("🚫 Blocked: " + ", ".join(added))
    if already:
        await update.message.reply_text("🤷‍♀️ Already blocked: " + ", ".join(already))
    return ConversationHandler.END

async def unblock_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    blocked = get_blocked_words(chat_id)
    if blocked:
        await update.message.reply_text("🗒 Blocked words:\n" + "\n".join(blocked) + "\nSend word(s) to unblock (comma-separated is OK):")
        return UNBLOCK_WORD
    await update.message.reply_text("🤷‍♀️ No blocked words.")
    return ConversationHandler.END

async def unblock_receive_word(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    raw = update.message.text.strip().lower()
    terms = [term.strip() for term in raw.split(",") if term.strip()]
    if not terms:
        await update.message.reply_text("🤷‍♀️ No valid terms found.")
        return ConversationHandler.END
    blocked = get_blocked_words(chat_id)
    removed = []
    missing = []
    for term in terms:
        if term in blocked:
            remove_blocked_word(chat_id, term)
            removed.append(term)
        else:
            missing.append(term)
    if removed:
        await update.message.reply_text("🟢 Unblocked: " + ", ".join(removed))
    if missing:
        await update.message.reply_text("❕ Not found: " + ", ".join(missing))
    return ConversationHandler.END

async def list_blocked(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    blocked = get_blocked_words(chat_id)
    if blocked:
        await update.message.reply_text("🚫 Blocked words:\n" + "\n".join(blocked))
    else:
        await update.message.reply_text("No blocked words.")

async def digest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    desired = None
    if context.args:
        arg = context.args[0].strip().lower()
        if arg in {"on", "enable", "enabled", "1", "true"}:
            desired = True
        elif arg in {"off", "disable", "disabled", "0", "false"}:
            desired = False
    if desired is None:
        current = get_setting(chat_id, "digest", "0") == "1"
        desired = not current
    set_setting(chat_id, "digest", "1" if desired else "0")
    if desired:
        message = (
            "🗞️ Digest mode is now ON.\n"
            "You’ll receive a single summary message per refresh with a list of new posts.\n"
            "Use /digest off to go back to individual posts."
        )
    else:
        message = (
            "📰 Digest mode is now OFF.\n"
            "You’ll receive each new post as a separate message.\n"
            "Use /digest on to switch back to a summary."
        )
    await update.message.reply_text(message)

async def frequency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("Daily", callback_data="frequency:daily"),
            InlineKeyboardButton("15m", callback_data="frequency:15m"),
        ]
    ]
    await update.message.reply_text(
        "Choose update frequency:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def frequency_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return
    if not query.data.startswith("frequency:"):
        return
    choice = query.data.split(":", 1)[1]
    if choice not in {"daily", "15m"}:
        await query.answer("Unknown choice.", show_alert=False)
        return
    await query.answer()
    chat_id = query.message.chat_id
    set_setting(chat_id, "frequency", choice)
    next_run = _next_aligned_run(time.time(), choice)
    _set_next_run(chat_id, next_run)
    next_dt = datetime.fromtimestamp(next_run, tz=timezone.utc)
    if choice == "daily":
        label = "Daily at 00:00 UTC"
    else:
        label = "Every 15 minutes from 00:00 UTC"
    await query.edit_message_text(
        f"✅ Update frequency set to {label}.\n"
        f"Next scheduled run: {next_dt:%Y-%m-%d %H:%M:%S} UTC"
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    feeds = get_feeds(chat_id)
    blocked = get_blocked_words(chat_id)
    failures = get_feed_failures(chat_id)
    lines = [
        f"📌 Feeds: {len(feeds)}",
        f"🚫 Blocked words: {len(blocked)}",
    ]
    if failures:
        lines.append(f"⏳ Paused feeds: {len(failures)}")
        for failure in failures[:5]:
            retry_at = datetime.fromtimestamp(failure['next_retry'], tz=timezone.utc)
            lines.append(
                f"- {failure['url']} (HTTP {failure['last_status']}, retry {retry_at:%Y-%m-%d %H:%M:%S} UTC)"
            )
        if len(failures) > 5:
            lines.append(f"...and {len(failures) - 5} more")
    await update.message.reply_text("\n".join(lines))

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("🛌 Command cancelled.")
    return ConversationHandler.END

async def welcome_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    welcome = (
        "I am Yuna Reada, an RSS reader bot! 📚\n\n"
        "Available commands:\n"
        "/help 📘 Show help\n"
        "/add ➕ Add feeds (and get 5 latest entries)\n"
        "/remove ➖ Remove feeds\n"
        "/block 🚫 Block words\n"
        "/unblock 🟢 Unblock words\n"
        "/blocked 📔 List blocked words\n"
        "/list 📓 List saved feeds\n"
        "/refresh 🌀 Force refresh feeds\n"
        "/digest 🗞️ Toggle digest mode\n"
        "/frequency 🕒 Set update frequency\n"
        "/status 📊 Show status\n"
        "/cancel 🛌 Cancel command\n\n"
        "I check feeds every 15 minutes. Use /frequency to change it.\n"
        "Use /add to begin!"
    )
    await context.bot.send_message(chat_id=chat_id, text=welcome)

async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Try /add to add feeds or /start for help.")

# --- Main ---
async def main():
    if not TELEGRAM_BOT_TOKEN:
        logging.error("Missing TELEGRAM_BOT_TOKEN environment variable.")
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    init_db()
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    await application.bot.set_my_commands([
        BotCommand("help", "Show help"),
        BotCommand("add", "Add RSS feeds"),
        BotCommand("remove", "Remove a feed"),
        BotCommand("block", "Block words/phrases"),
        BotCommand("unblock", "Unblock words/phrases"),
        BotCommand("blocked", "List blocked words"),
        BotCommand("list", "List saved feeds"),
        BotCommand("refresh", "Fetch updates now"),
        BotCommand("digest", "Toggle digest mode"),
        BotCommand("frequency", "Set update frequency"),
        BotCommand("status", "Show status"),
        BotCommand("cancel", "Cancel the current command"),
    ])

    if application.job_queue is None:
        job_queue = JobQueue()
        job_queue.set_application(application)
        application.job_queue = job_queue
        job_queue.start()

    # Run immediately on startup, then sync to the next scheduled update time.
    application.job_queue.run_once(
        run_immediate_then_sync,
        when=0,
        job_kwargs={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 60,
        },
    )

    # Tick schedule to send due updates (aligned to UTC).
    application.job_queue.run_repeating(
        scheduled_tick,
        interval=60,
        first=60,
        job_kwargs={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 60,
        },
    )

    add_conv = ConversationHandler(
        entry_points=[CommandHandler("add", add_start)],
        states={FEED_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_receive_url)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    remove_conv = ConversationHandler(
        entry_points=[CommandHandler("remove", remove_start)],
        states={REMOVE_FEED: [MessageHandler(filters.TEXT & ~filters.COMMAND, remove_receive_feed)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    block_conv = ConversationHandler(
        entry_points=[CommandHandler("block", block_start)],
        states={BLOCK_WORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, block_receive_word)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    unblock_conv = ConversationHandler(
        entry_points=[CommandHandler("unblock", unblock_start)],
        states={UNBLOCK_WORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, unblock_receive_word)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    application.add_handler(add_conv)
    application.add_handler(remove_conv)
    application.add_handler(CommandHandler("help", welcome_user))
    application.add_handler(CommandHandler("start", welcome_user))
    application.add_handler(CommandHandler("list", list_feeds))
    application.add_handler(CommandHandler("refresh", refresh))
    application.add_handler(CommandHandler("blocked", list_blocked))
    application.add_handler(CommandHandler("digest", digest))
    application.add_handler(CommandHandler("frequency", frequency))
    application.add_handler(CallbackQueryHandler(frequency_choice))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(block_conv)
    application.add_handler(unblock_conv)
    # Fallback: greet user on any text not caught by commands
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))

    await application.run_polling(close_loop=False)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
