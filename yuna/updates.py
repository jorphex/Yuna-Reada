import asyncio
import logging
import time

from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from . import config
from .db import get_blocked_words, get_chat_ids, get_feeds, get_setting, update_latest_ts
from .rss import fetch_rss
from .scheduling import get_frequency, get_next_run, next_aligned_run, set_next_run
from .ui import chunk_lines, entry_link_html, format_digest_line, format_entry_message

UPDATE_LOCK = asyncio.Lock()
FETCH_SEMAPHORE = asyncio.Semaphore(config.MAX_CONCURRENT_FETCHES)


async def fetch_feed_with_limit(chat_id: int, feed: dict, blocked_words: set[str]):
    async with FETCH_SEMAPHORE:
        return await asyncio.to_thread(
            fetch_rss,
            chat_id,
            feed["xmlUrl"],
            feed["latest_ts"],
            blocked_words,
        )


async def fetch_and_send_updates(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    feeds = get_feeds(chat_id)
    if not feeds:
        return {"feeds": 0, "entries": 0, "blocked": 0}
    bot = context.bot
    all_entries = []
    blocked_entries = []
    blocked_words = get_blocked_words(chat_id)

    tasks = [fetch_feed_with_limit(chat_id, feed, blocked_words) for feed in feeds]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result, feed in zip(results, feeds):
        if isinstance(result, Exception):
            logging.error(
                "Failed to fetch feed for chat %s, URL %s: %s",
                chat_id,
                feed["xmlUrl"],
                result,
                exc_info=(type(result), result, result.__traceback__),
            )
            continue
        fetched_entries, blocked, new_latest, _feed_name = result
        if fetched_entries:
            all_entries.extend(fetched_entries)
        if blocked:
            blocked_entries.extend(blocked)
        if new_latest is not None and new_latest > feed.get("latest_ts", 0):
            update_latest_ts(chat_id, feed["xmlUrl"], new_latest)

    all_entries.sort(key=lambda entry: entry._timestamp, reverse=True)

    digest_enabled = get_setting(chat_id, "digest", "0") == "1"
    if digest_enabled:
        await send_digest(bot, chat_id, all_entries)
    else:
        for entry in all_entries:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=format_entry_message(entry),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await asyncio.sleep(0.25)
            except Exception as e:
                logging.error(f"Error sending message for chat {chat_id}: {e}", exc_info=True)

    if blocked_entries:
        blocked_lines = [f"Hidden posts: {len(blocked_entries)}"]
        for entry in blocked_entries:
            blocked_lines.append(entry_link_html(entry))
        for chunk in chunk_lines(blocked_lines):
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await asyncio.sleep(0.25)
    return {"feeds": len(feeds), "entries": len(all_entries), "blocked": len(blocked_entries)}


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
            freq = get_frequency(chat_id)
            set_next_run(chat_id, next_aligned_run(now, freq))


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
            freq = get_frequency(chat_id)
            next_run = get_next_run(chat_id)
            if next_run is None:
                set_next_run(chat_id, next_aligned_run(now, freq))
                continue
            if now >= next_run:
                due.append(chat_id)

        if due:
            await asyncio.gather(*(fetch_and_send_updates(context, chat_id) for chat_id in due))
            now = time.time()
            for chat_id in due:
                freq = get_frequency(chat_id)
                set_next_run(chat_id, next_aligned_run(now, freq))


async def send_digest(bot, chat_id: int, entries: list):
    if not entries:
        return
    lines = [f"📰 New posts ({len(entries)})"]
    for entry in entries:
        lines.append(format_digest_line(entry))

    for chunk in chunk_lines(lines):
        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await asyncio.sleep(0.25)
