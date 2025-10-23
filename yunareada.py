import asyncio
import io
import logging
import os
import re
import shelve
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import wraps
from time import mktime
from typing import List, Optional
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import indent  # Requires Python 3.9+

import feedparser
from bs4 import BeautifulSoup as bs
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# --- Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# --- Configuration ---
TELEGRAM_BOT_TOKEN = "6133397985:AAGJhnVYZm2Z9SqSKSAW6Nh7gcqVdtLdMuw"
if not TELEGRAM_BOT_TOKEN:
    logger.critical("TELEGRAM_BOT_TOKEN environment variable not set.")
    exit(1)

# --- Constants ---
REFRESH_INTERVAL_SECONDS = 15 * 60
MAX_MESSAGE_LENGTH = 400
TRUNCATION_SUFFIX = "..."
TRUNCATED_LENGTH = MAX_MESSAGE_LENGTH - len(TRUNCATION_SUFFIX)

# --- Data Model & Persistence ---
class UserData:
    """Represents the data stored for each user."""
    def __init__(self):
        self.latest_post_time = {}  # feed URL -> last seen timestamp
        self.blocked_words = set()  # words/phrases to block
        self.blocked_entries = []   # transient list for blocked entries summary
        self.feeds = []             # list of dicts: {'xmlUrl': url, 'text': feed_name}

def get_user_data(shelf, chat_id: int) -> UserData:
    """
    Retrieves or creates a UserData object for a given chat_id from the shelf.
    Shelve keys must be strings.
    """
    chat_id_str = str(chat_id)
    if chat_id_str not in shelf:
        shelf[chat_id_str] = UserData()
    return shelf[chat_id_str]

def save_user_data(shelf, chat_id: int, user_data: UserData):
    """Explicitly saves the UserData object to the shelf for crash-proof persistence."""
    shelf[str(chat_id)] = user_data

# --- Decorators ---
def get_user(func):
    """Decorator to retrieve user_data and pass it to the handler."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        chat_id = update.effective_chat.id
        shelf = context.bot_data['db']
        user_data = get_user_data(shelf, chat_id)
        return await func(update, context, user_data=user_data, *args, **kwargs)
    return wrapper

def user_lock(func):
    """Decorator to acquire a user-specific lock before modifying data to prevent race conditions."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        chat_id = update.effective_chat.id
        locks = context.bot_data.setdefault('user_locks', {})
        lock = locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            return await func(update, context, *args, **kwargs)
    return wrapper

# --- Conversation States ---
class ConversationState(Enum):
    """Defines the states for conversation handlers."""
    FEED_URL = 1
    REMOVE_FEED = 2
    BLOCK_WORD = 3
    UNBLOCK_WORD = 4

# --- RSS Fetching (blocking) ---
@dataclass
class FetchResult:
    """Structured result for the fetch_rss function."""
    entries: List[feedparser.FeedParserDict] = field(default_factory=list)
    blocked_entries: List[feedparser.FeedParserDict] = field(default_factory=list)
    new_latest_post_time: float = 0.0
    feed_name: Optional[str] = None

def fetch_rss(url: str, latest_post_time: float, blocked_words: set) -> FetchResult:
    """
    Fetches and parses an RSS feed. This is a blocking I/O function.
    """
    start_time = time.time()
    logger.info(f"Start fetching RSS for URL {url}")

    try:
        feed = feedparser.parse(url)
        if feed.bozo:
            raise ValueError(f"Malformed feed: {feed.bozo_exception}")

        all_new_entries = [
            entry for entry in feed.entries
            if entry.get("published_parsed") and mktime(entry.published_parsed) > latest_post_time
        ]

        if not all_new_entries:
            return FetchResult(
                new_latest_post_time=latest_post_time,
                feed_name=feed.feed.get("title", "Unknown Feed")
            )

        # Correct Timestamp Logic: Calculate from ALL new entries before filtering
        new_latest_post_time = max(mktime(e.published_parsed) for e in all_new_entries)

        entries, blocked_entries = [], []
        for entry in all_new_entries:
            entry.description = bs(entry.get("description", ""), "html.parser").get_text()
            entry.summary = bs(entry.get("summary", entry.description), "html.parser").get_text()

            # Optimize Blocked Word Check
            title = (entry.title or "").lower()
            summary = (entry.summary or "").lower()
            description = (entry.description or "").lower()

            if any(word in title or word in summary or word in description for word in blocked_words):
                blocked_entries.append(entry)
            else:
                entries.append(entry)

        feed_name = feed.feed.get("title", "Unknown Feed")
        logger.info(f"Fetched {len(entries)} new entries from {feed_name} (URL: {url})")
        return FetchResult(entries, blocked_entries, new_latest_post_time, feed_name)

    except Exception as e:
        logger.error(f"Error fetching RSS for URL {url}: {e}", exc_info=True)
        return FetchResult(new_latest_post_time=latest_post_time)
    finally:
        logger.info(f"Finished fetching RSS for URL {url} in {time.time()-start_time:.2f}s")

# --- Formatting ---
def format_entry_message(entry) -> str:
    """Formats a feed entry into an HTML message for Telegram."""
    published = datetime.fromtimestamp(mktime(entry.published_parsed))
    timestamp = published.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
    
    # Prefer summary, fallback to description. Content is already plain text.
    content = entry.summary or entry.description or ""
    
    if len(content) > MAX_MESSAGE_LENGTH:
        content = content[:TRUNCATED_LENGTH] + TRUNCATION_SUFFIX
        
    return (
        f"<a href='{entry.link}'><b>{entry.title}</b></a>\n"
        f"<code>{timestamp}</code>\n\n{content}"
    )

# --- Update Functions ---
async def fetch_and_send_updates(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Fetches updates for a single user and sends them, with concurrency control."""
    locks = context.bot_data.setdefault('user_locks', {})
    lock = locks.setdefault(chat_id, asyncio.Lock())
    
    async with lock:
        shelf = context.bot_data['db']
        user_data = get_user_data(shelf, chat_id)
        if not user_data.feeds:
            return

        bot = context.bot
        all_entries = []
        data_modified = False

        tasks = [
            asyncio.to_thread(
                fetch_rss,
                feed["xmlUrl"],
                user_data.latest_post_time.get(feed["xmlUrl"], 0),
                user_data.blocked_words
            ) for feed in user_data.feeds
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result, feed in zip(results, user_data.feeds):
            if isinstance(result, Exception):
                logger.error(f"Task for {feed['xmlUrl']} failed: {result}")
                continue
            
            if result.entries:
                all_entries.extend(result.entries)
                user_data.latest_post_time[feed["xmlUrl"]] = result.new_latest_post_time
                data_modified = True
            if result.blocked_entries:
                user_data.blocked_entries.extend(result.blocked_entries)
        
        if data_modified:
            save_user_data(shelf, chat_id, user_data)

        all_entries.sort(key=lambda e: e.published_parsed, reverse=True)

        for entry in all_entries:
            try:
                await bot.send_message(chat_id=chat_id, text=format_entry_message(entry), parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Error sending message for chat {chat_id}: {e}", exc_info=True)

        if user_data.blocked_entries:
            blocked_message = f"🚫 Blocked {len(user_data.blocked_entries)} new posts\n" + "\n".join(
                f'<a href="{entry.link}">{entry.title}</a>' for entry in user_data.blocked_entries
            )
            await bot.send_message(chat_id=chat_id, text=blocked_message, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            user_data.blocked_entries = []

async def send_rss_updates(context: ContextTypes.DEFAULT_TYPE):
    """Job callback to send updates to all users."""
    shelf = context.bot_data['db']
    chat_ids = [int(chat_id_str) for chat_id_str in shelf.keys()]
    await asyncio.gather(*(fetch_and_send_updates(context, chat_id) for chat_id in chat_ids))

async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /refresh command. Triggers an immediate update check for the user."""
    chat_id = update.effective_chat.id
    await update.message.reply_text("🔍 Looking for new posts... This might take a moment!")
    await fetch_and_send_updates(context, chat_id)
    await update.message.reply_text("🌟 Refreshed all feeds and sent new updates!")

# --- OPML Helpers ---
def write_opml(feeds) -> bytes:
    """Writes a list of feeds to an in-memory OPML file and returns the bytes."""
    root = ET.Element("opml", version="2.0")
    body = ET.SubElement(root, "body")
    for feed in feeds:
        ET.SubElement(body, "outline", text=feed["text"], type="rss", xmlUrl=feed["xmlUrl"])
    indent(root)
    tree = ET.ElementTree(root)
    
    with io.BytesIO() as buffer:
        tree.write(buffer, encoding="utf-8", xml_declaration=True)
        return buffer.getvalue()

# --- Conversation Handlers ---
async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("📄 Please send an RSS feed URL or URLs separated by commas, spaces, or line breaks.")
    return ConversationState.FEED_URL

@user_lock
@get_user
async def add_receive_url(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData) -> int:
    chat_id = update.effective_chat.id
    shelf = context.bot_data['db']
    await update.message.reply_text("👨‍💻 Processing. Please wait!")

    raw_urls = update.message.text.strip()
    url_list = [url.strip() for url in re.split(r"[,\n\s]+", raw_urls) if url.strip()]
    all_entries, newly_added_urls, data_modified = [], [], False

    tasks = []
    for url in url_list:
        if not is_valid_url(url):
            await update.message.reply_text(f"🙃 Invalid URL detected: {url}")
            continue
        if any(feed["xmlUrl"] == url for feed in user_data.feeds):
            await update.message.reply_text(f"😕 RSS feed is already added: {url}")
            continue
        tasks.append(asyncio.to_thread(fetch_rss, url, 0, user_data.blocked_words))
        newly_added_urls.append(url)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for url, result in zip(newly_added_urls, results):
        if isinstance(result, Exception) or result.feed_name is None:
            logger.error(f"Error fetching URL {url}: {result}", exc_info=True)
            await update.message.reply_text(f"😤 Failed to fetch RSS feed from {url}.")
            continue

        user_data.feeds.append({"xmlUrl": url, "text": result.feed_name})
        all_entries.extend(result.entries)
        user_data.latest_post_time[url] = result.new_latest_post_time
        data_modified = True

    if not data_modified:
        return ConversationHandler.END

    save_user_data(shelf, chat_id, user_data)
    await update.message.reply_text("☺️ Successfully added RSS feeds:\n" + "\n".join(newly_added_urls))

    latest_entries = sorted(all_entries, key=lambda e: e.published_parsed, reverse=True)[:5]
    if latest_entries:
        await context.bot.send_message(chat_id=chat_id, text="📰 Here are the five latest items from your new feeds:")
        for entry in latest_entries:
            await context.bot.send_message(chat_id=chat_id, text=format_entry_message(entry), parse_mode=ParseMode.HTML)

    await context.bot.send_message(chat_id=chat_id, text="🗃 Here's an XML/OPML file of your feeds for backup:")
    opml_bytes = write_opml(user_data.feeds)
    await context.bot.send_document(chat_id=chat_id, document=opml_bytes, filename=f"{chat_id}_feeds.xml")
    return ConversationHandler.END

def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

@get_user
async def remove_start(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData) -> int:
    if not user_data.feeds:
        await update.message.reply_text("🤷‍♀️ You have no feeds to remove. Use /add to add some.")
        return ConversationHandler.END
    feed_list = sorted([(feed["text"], feed["xmlUrl"]) for feed in user_data.feeds], key=lambda x: x[0])
    message = "Your current feeds:\n\n" + "\n".join(f"{name}: {url}" for name, url in feed_list)
    message += "\n\n🤔 Which feed would you like to remove? Send its URL."
    await update.message.reply_text(message)
    return ConversationState.REMOVE_FEED

@user_lock
@get_user
async def remove_receive_feed(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData):
    chat_id = update.effective_chat.id
    shelf = context.bot_data['db']
    feed_url = update.message.text.strip()
    feed_to_remove = next((feed for feed in user_data.feeds if feed["xmlUrl"] == feed_url), None)

    if not feed_to_remove:
        await update.message.reply_text("🤷‍♀️ No matching feed found with that URL.")
        return ConversationHandler.END

    user_data.feeds.remove(feed_to_remove)
    user_data.latest_post_time.pop(feed_to_remove['xmlUrl'], None) # Fix stale data bug
    save_user_data(shelf, chat_id, user_data)

    await update.message.reply_text(f"🧹 Removed feed: {feed_to_remove['text']}")
    opml_bytes = write_opml(user_data.feeds)
    await context.bot.send_document(chat_id=chat_id, document=opml_bytes, filename=f"{chat_id}_feeds.xml")
    return ConversationHandler.END

@get_user
async def list_feeds(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData):
    chat_id = update.effective_chat.id
    if not user_data.feeds:
        await update.message.reply_text("🤷‍♀️ You do not have any feeds. Use /add to add some.")
    else:
        feed_list = sorted(f"<a href='{feed['xmlUrl']}'>{feed['text']}</a>" for feed in user_data.feeds)
        await update.message.reply_text("Your feeds:\n" + "\n".join(feed_list), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        opml_bytes = write_opml(user_data.feeds)
        await context.bot.send_document(chat_id=chat_id, document=opml_bytes, filename=f"{chat_id}_feeds.xml")

async def block_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("🔍 Please enter the word or phrase to block:")
    return ConversationState.BLOCK_WORD

@user_lock
@get_user
async def block_receive_word(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData) -> int:
    word = update.message.text.strip().lower()
    if not word:
        await update.message.reply_text("🤷‍♀️ Cannot block an empty word.")
    elif word not in user_data.blocked_words:
        user_data.blocked_words.add(word)
        save_user_data(context.bot_data['db'], update.effective_chat.id, user_data)
        await update.message.reply_text(f"🚫 Word blocked: {word}")
    else:
        await update.message.reply_text(f"🤷‍♀️ Word already blocked: {word}")
    return ConversationHandler.END

@get_user
async def unblock_start(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData) -> int:
    if user_data.blocked_words:
        blocked_list = "\n".join(sorted(list(user_data.blocked_words)))
        await update.message.reply_text(f"🗒 Blocked words:\n{blocked_list}\n\nSend the word to unblock:")
        return ConversationState.UNBLOCK_WORD
    await update.message.reply_text("🤷‍♀️ No blocked words.")
    return ConversationHandler.END

@user_lock
@get_user
async def unblock_receive_word(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData) -> int:
    word = update.message.text.strip().lower()
    if word in user_data.blocked_words:
        user_data.blocked_words.remove(word)
        save_user_data(context.bot_data['db'], update.effective_chat.id, user_data)
        await update.message.reply_text(f"🟢 Word unblocked: {word}")
    else:
        await update.message.reply_text(f"❕ Word not found in blocked list: {word}")
    return ConversationHandler.END

@get_user
async def list_blocked(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData):
    if user_data.blocked_words:
        await update.message.reply_text("🚫 Blocked words:\n" + "\n".join(sorted(list(user_data.blocked_words))))
    else:
        await update.message.reply_text("No blocked words.")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("🛌 Command cancelled.")
    return ConversationHandler.END

@user_lock
@get_user
async def welcome_user(update: Update, context: ContextTypes.DEFAULT_TYPE, user_data: UserData):
    # The @get_user decorator ensures user_data is created. We save it here to persist the new user.
    save_user_data(context.bot_data['db'], update.effective_chat.id, user_data)
    welcome = (
        "I am Yuna Reada, an RSS reader bot! 📚\n\n"
        "Available commands:\n"
        "/add ➕ Add feeds\n"
        "/remove ➖ Remove feeds\n"
        "/list 📓 List saved feeds\n"
        "/refresh 🌀 Force refresh feeds\n\n"
        "/block 🚫 Block words\n"
        "/unblock 🟢 Unblock words\n"
        "/blocked 📔 List blocked words\n\n"
        "/cancel 🛌 Cancel command\n\n"
        f"I check feeds every {REFRESH_INTERVAL_SECONDS // 60} minutes. Use /add to begin!"
    )
    await context.bot.send_message(chat_id=update.effective_chat.id, text=welcome)

# --- Main Application Setup ---
def main():
    """Sets up and runs the bot."""
    with shelve.open('user_data.db', writeback=False) as db:
        application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        application.bot_data['db'] = db

        add_conv = ConversationHandler(
            entry_points=[CommandHandler("add", add_start)],
            states={ConversationState.FEED_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_receive_url)]},
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        remove_conv = ConversationHandler(
            entry_points=[CommandHandler("remove", remove_start)],
            states={ConversationState.REMOVE_FEED: [MessageHandler(filters.TEXT & ~filters.COMMAND, remove_receive_feed)]},
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        block_conv = ConversationHandler(
            entry_points=[CommandHandler("block", block_start)],
            states={ConversationState.BLOCK_WORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, block_receive_word)]},
            fallbacks=[CommandHandler("cancel", cancel)],
        )
        unblock_conv = ConversationHandler(
            entry_points=[CommandHandler("unblock", unblock_start)],
            states={ConversationState.UNBLOCK_WORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, unblock_receive_word)]},
            fallbacks=[CommandHandler("cancel", cancel)],
        )

        application.add_handler(add_conv)
        application.add_handler(remove_conv)
        application.add_handler(block_conv)
        application.add_handler(unblock_conv)
        application.add_handler(CommandHandler("list", list_feeds))
        application.add_handler(CommandHandler("refresh", refresh))
        application.add_handler(CommandHandler("blocked", list_blocked))
        application.add_handler(CommandHandler("start", welcome_user))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, welcome_user))

        application.job_queue.run_repeating(send_rss_updates, interval=REFRESH_INTERVAL_SECONDS, first=10)
        application.run_polling()

if __name__ == "__main__":
    main()
