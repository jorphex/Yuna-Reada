import os

from dotenv import load_dotenv
from telegram import BotCommand, ReplyKeyboardMarkup

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_PATH = os.getenv("YUNAREADA_DB", "yunareada.db")

TELEGRAM_MESSAGE_LIMIT = 4096
MESSAGE_CHUNK_LIMIT = 3900
MAX_URL_LENGTH = 2048
MAX_ENTRY_ID_LENGTH = 500
MAX_BLOCK_TERM_LENGTH = 120
FORCE_REPLY_PLACEHOLDER_LIMIT = 64

BOT_SHORT_DESCRIPTION = "RSS reader that sends new posts, summaries, and hidden-term controls."
BOT_DESCRIPTION = (
    "Yuna Reada follows RSS feeds and sends new posts to this chat.\n\n"
    "Use /add to add feeds, /list to review them, /refresh to check now, and /status to see settings."
)


def env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return max(minimum, value)


MAX_CONCURRENT_FETCHES = env_int("YUNAREADA_MAX_CONCURRENT_FETCHES", 8)

BOT_COMMANDS = [
    BotCommand("help", "Show help"),
    BotCommand("add", "Add RSS feeds"),
    BotCommand("remove", "Remove a feed"),
    BotCommand("block", "Hide words/phrases"),
    BotCommand("unblock", "Unhide words/phrases"),
    BotCommand("blocked", "List hidden terms"),
    BotCommand("list", "List saved feeds"),
    BotCommand("refresh", "Fetch updates now"),
    BotCommand("digest", "Toggle digest mode"),
    BotCommand("frequency", "Set update frequency"),
    BotCommand("status", "Show status"),
    BotCommand("cancel", "Cancel the current command"),
]

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["/add", "/list", "/refresh"],
        ["/status", "/digest", "/frequency"],
    ],
    resize_keyboard=True,
    input_field_placeholder="Paste a feed URL or choose a command",
    is_persistent=True,
)
