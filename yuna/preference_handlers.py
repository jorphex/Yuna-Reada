from datetime import datetime, timezone
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands, Update
from telegram.ext import ContextTypes, ConversationHandler

from .config import BOT_COMMANDS, BOT_DESCRIPTION, BOT_SHORT_DESCRIPTION, MAIN_KEYBOARD
from .db import (
    add_blocked_word,
    get_blocked_words,
    get_feed_failures,
    get_feeds,
    get_setting,
    remove_blocked_word,
    set_setting,
)
from .rss import parse_block_terms
from .scheduling import next_aligned_run, set_next_run
from .ui import force_reply, format_status_lines, help_text, send_message_chunks

BLOCK_WORD, UNBLOCK_WORD = range(3, 5)


async def block_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Send words or phrases to hide from new posts. Separate multiple terms with commas.",
        reply_markup=force_reply("politics, spoilers"),
    )
    return BLOCK_WORD


async def block_receive_word(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    terms = parse_block_terms(update.message.text.strip())
    if not terms:
        await update.message.reply_text(
            "I didn’t find any terms to block. Send a word or phrase, or /cancel.",
            reply_markup=force_reply("politics, spoilers"),
        )
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
        await update.message.reply_text("Hidden terms added: " + ", ".join(added), reply_markup=MAIN_KEYBOARD)
    if already:
        await update.message.reply_text("Already hidden: " + ", ".join(already), reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


async def unblock_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    blocked = get_blocked_words(chat_id)
    if blocked:
        await send_message_chunks(
            context.bot,
            chat_id,
            "Hidden terms:\n" + "\n".join(sorted(blocked)) + "\n\nSend terms to unhide, separated by commas.",
            reply_markup=force_reply("politics"),
        )
        return UNBLOCK_WORD
    await update.message.reply_text("No hidden terms yet. Use /block to add one.", reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


async def unblock_receive_word(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    terms = parse_block_terms(update.message.text.strip())
    if not terms:
        await update.message.reply_text(
            "I didn’t find any terms to unblock. Send a term, or /cancel.",
            reply_markup=force_reply("politics"),
        )
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
        await update.message.reply_text("No longer hidden: " + ", ".join(removed), reply_markup=MAIN_KEYBOARD)
    if missing:
        await update.message.reply_text("Not in your hidden terms: " + ", ".join(missing), reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


async def list_blocked(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    blocked = get_blocked_words(chat_id)
    if blocked:
        await send_message_chunks(context.bot, chat_id, "Hidden terms:\n" + "\n".join(sorted(blocked)), reply_markup=MAIN_KEYBOARD)
    else:
        await update.message.reply_text("No hidden terms yet. Use /block to add one.", reply_markup=MAIN_KEYBOARD)


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
            "Digest mode is on.\n"
            "You’ll receive one summary message per refresh with a list of new posts.\n"
            "Use /digest off to go back to individual posts."
        )
    else:
        message = (
            "Digest mode is off.\n"
            "You’ll receive each new post as a separate message.\n"
            "Use /digest on to switch back to a summary."
        )
    await update.message.reply_text(message, reply_markup=MAIN_KEYBOARD)


async def frequency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("Daily", callback_data="frequency:daily"),
            InlineKeyboardButton("Every 15 min", callback_data="frequency:15m"),
            InlineKeyboardButton("Cancel", callback_data="frequency:cancel"),
        ]
    ]
    msg = await update.message.reply_text(
        "Choose how often I should check your feeds:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    set_setting(update.effective_chat.id, "frequency_prompt_msg", str(msg.message_id))


async def frequency_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return
    if not query.data.startswith("frequency:"):
        return
    choice = query.data.split(":", 1)[1]
    if choice == "cancel":
        chat_id = query.message.chat_id
        await query.answer()
        set_setting(chat_id, "frequency_prompt_msg", "")
        await query.edit_message_text("Frequency unchanged.")
        await context.bot.send_message(chat_id=chat_id, text="Use the keyboard below for the next action.", reply_markup=MAIN_KEYBOARD)
        return
    if choice not in {"daily", "15m"}:
        await query.answer("Unknown choice.", show_alert=False)
        return
    await query.answer()
    chat_id = query.message.chat_id
    set_setting(chat_id, "frequency", choice)
    next_run = next_aligned_run(time.time(), choice)
    set_next_run(chat_id, next_run)
    next_dt = datetime.fromtimestamp(next_run, tz=timezone.utc)
    label = "daily at 00:00 UTC" if choice == "daily" else "every 15 minutes"
    set_setting(chat_id, "frequency_prompt_msg", "")
    await query.edit_message_text(
        f"Update frequency set to {label}.\n"
        f"Next check: {next_dt:%Y-%m-%d %H:%M:%S} UTC"
    )
    await context.bot.send_message(chat_id=chat_id, text="Keyboard restored.", reply_markup=MAIN_KEYBOARD)


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    feeds = get_feeds(chat_id)
    blocked = get_blocked_words(chat_id)
    failures = get_feed_failures(chat_id)
    lines = format_status_lines(chat_id, feeds, blocked, failures)
    await send_message_chunks(context.bot, chat_id, "\n".join(lines), reply_markup=MAIN_KEYBOARD)


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    prompt_id = get_setting(chat_id, "frequency_prompt_msg")
    if prompt_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=int(prompt_id))
        except Exception:
            pass
        set_setting(chat_id, "frequency_prompt_msg", "")
    await update.message.reply_text("Command cancelled.", reply_markup=MAIN_KEYBOARD)
    context.user_data.pop("remove_feed_choices", None)
    return ConversationHandler.END


async def welcome_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text=help_text(), reply_markup=MAIN_KEYBOARD)


async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "I didn’t recognize that. Use /help for commands, or /add to add a feed.",
        reply_markup=MAIN_KEYBOARD,
    )


async def configure_bot_ui(bot):
    await bot.set_my_commands(BOT_COMMANDS)
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    await bot.set_my_short_description(BOT_SHORT_DESCRIPTION)
    await bot.set_my_description(BOT_DESCRIPTION)
