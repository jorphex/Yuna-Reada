import asyncio
import logging
import re

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from .config import MAIN_KEYBOARD
from .db import feed_exists, get_blocked_words, get_feeds, remove_feed, update_latest_ts, upsert_feed
from .opml import send_opml
from .rss import fetch_rss, normalize_feed_url
from .ui import (
    chunk_lines,
    feed_html_line,
    feed_plain_line,
    force_reply,
    format_entry_message,
    format_refresh_summary,
    send_message_chunks,
    truncate_text,
)
from .updates import fetch_and_send_updates

FEED_URL, REMOVE_FEED = range(1, 3)


async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text("Checking your feeds now. This can take a moment.", reply_markup=MAIN_KEYBOARD)
    result = await fetch_and_send_updates(context, chat_id)
    await update.message.reply_text(format_refresh_summary(result), reply_markup=MAIN_KEYBOARD)


async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Send one or more public RSS feed URLs. Separate multiple URLs with spaces, commas, or new lines.",
        reply_markup=force_reply("https://example.com/feed.xml"),
    )
    return FEED_URL


async def add_receive_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    await update.message.reply_text("Checking those feeds now. I’ll add each one that looks valid.")
    raw_urls = update.message.text.strip()
    raw_url_list = [url.strip() for url in re.split(r"[\s,]+", raw_urls) if url.strip()]
    url_list = []
    invalid_urls = []
    for raw_url in raw_url_list:
        normalized_url = normalize_feed_url(raw_url)
        if normalized_url:
            url_list.append(normalized_url)
        else:
            invalid_urls.append(raw_url)
    all_entries = []
    blocked_words = get_blocked_words(chat_id)
    added_urls = []
    duplicate_urls = []
    failed_urls = []

    for url in url_list:
        if feed_exists(chat_id, url):
            duplicate_urls.append(url)
            continue
        try:
            fetched_entries, _, new_latest, feed_name = await asyncio.to_thread(
                fetch_rss, chat_id, url, 0, blocked_words
            )
            if not feed_name:
                failed_urls.append(url)
                continue
            upsert_feed(chat_id, url, feed_name)
            added_urls.append(url)
            all_entries.extend(fetched_entries)
            if new_latest is not None:
                update_latest_ts(chat_id, url, new_latest)
        except Exception as e:
            logging.error(f"Error fetching URL {url}: {e}", exc_info=True)
            failed_urls.append(url)
            continue

    all_entries.sort(key=lambda entry: entry._timestamp)
    if all_entries:
        await context.bot.send_message(chat_id=chat_id, text="Here are the five latest items I found:", reply_markup=MAIN_KEYBOARD)
        latest_entries = sorted(all_entries[-5:], key=lambda entry: entry._timestamp)
        for entry in latest_entries:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=format_entry_message(entry),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await asyncio.sleep(0.25)
            except Exception as e:
                logging.error(f"Error sending feed entry: {e}", exc_info=True)
    elif added_urls:
        await context.bot.send_message(
            chat_id=chat_id,
            text="No recent items found yet. I’ll notify you when new posts appear.",
            reply_markup=MAIN_KEYBOARD,
        )

    summary_lines = []
    if added_urls:
        summary_lines.append("Added feeds:")
        summary_lines.extend(added_urls)
    if duplicate_urls:
        summary_lines.append("Already added:")
        summary_lines.extend(duplicate_urls)
    if failed_urls:
        summary_lines.append("Could not add:")
        summary_lines.extend(failed_urls)
    if invalid_urls:
        summary_lines.append("Skipped:")
        summary_lines.extend(truncate_text(url, 220) for url in invalid_urls)
    if not summary_lines:
        summary_lines.append("No feeds were added.")
    await send_message_chunks(context.bot, chat_id, "\n".join(summary_lines), reply_markup=MAIN_KEYBOARD)

    if added_urls:
        await context.bot.send_message(
            chat_id=chat_id,
            text="I’m sending an OPML backup of your feed list.",
            reply_markup=MAIN_KEYBOARD,
        )
        try:
            await send_opml(context.bot, chat_id, get_feeds(chat_id))
        except Exception as e:
            logging.error(f"Error sending document: {e}", exc_info=True)
            await update.message.reply_text("The feeds were added, but I could not send the OPML backup.", reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


async def remove_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    feeds = get_feeds(chat_id)
    if not feeds:
        await update.message.reply_text("No feeds to remove yet. Use /add to add one first.", reply_markup=MAIN_KEYBOARD)
        return ConversationHandler.END
    sorted_feeds = sorted(feeds, key=lambda feed: feed["text"].casefold())
    context.user_data["remove_feed_choices"] = {
        str(index): feed["xmlUrl"]
        for index, feed in enumerate(sorted_feeds, start=1)
    }
    message = "Send the number of the feed to remove, or paste its URL:\n\n"
    message += "\n".join(feed_plain_line(index, feed) for index, feed in enumerate(sorted_feeds, start=1))
    await send_message_chunks(context.bot, chat_id, message, reply_markup=force_reply("1"))
    return REMOVE_FEED


async def remove_receive_feed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    raw_choice = update.message.text.strip()
    normalized_choice = raw_choice.casefold()
    choices = context.user_data.get("remove_feed_choices", {})
    selected_url = choices.get(raw_choice)
    feeds = get_feeds(chat_id)
    feed_to_remove = None
    if selected_url:
        feed_to_remove = next((feed for feed in feeds if feed["xmlUrl"] == selected_url), None)
    if not feed_to_remove:
        feed_to_remove = next(
            (
                feed for feed in feeds
                if feed["xmlUrl"].casefold() == normalized_choice or feed["text"].casefold() == normalized_choice
            ),
            None,
        )
    if not feed_to_remove:
        await update.message.reply_text(
            "I couldn’t match that feed. Send a number from the list, or paste the feed URL.",
            reply_markup=force_reply("1"),
        )
        return REMOVE_FEED

    remove_feed(chat_id, feed_to_remove["xmlUrl"])
    context.user_data.pop("remove_feed_choices", None)
    await update.message.reply_text(f"Removed feed: {truncate_text(feed_to_remove['text'], 120)}", reply_markup=MAIN_KEYBOARD)
    try:
        await send_opml(context.bot, chat_id, get_feeds(chat_id))
    except Exception as e:
        logging.error("Error sending OPML after feed removal: %s", e, exc_info=True)
        await update.message.reply_text(
            "The feed was removed, but I could not send the updated OPML backup.",
            reply_markup=MAIN_KEYBOARD,
        )
    return ConversationHandler.END


async def list_feeds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    feeds = get_feeds(chat_id)
    if not feeds:
        await update.message.reply_text("No feeds yet. Use /add and paste an RSS feed URL to start.", reply_markup=MAIN_KEYBOARD)
        return

    sorted_feeds = sorted(feeds, key=lambda feed: feed["text"].casefold())
    feed_list = [
        feed_html_line(index, feed)
        for index, feed in enumerate(sorted_feeds, start=1)
    ]
    await context.bot.send_message(chat_id=chat_id, text=f"Feeds ({len(feeds)}):")
    for chunk in chunk_lines(feed_list):
        await context.bot.send_message(
            chat_id=chat_id,
            text=chunk,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await asyncio.sleep(0.25)
    await send_opml(context.bot, chat_id, feeds)
