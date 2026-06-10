import logging
import sys
import warnings

warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API\..*",
    category=UserWarning,
)

from telegram.ext import AIORateLimiter, ApplicationBuilder, ContextTypes  # noqa: E402

from . import config  # noqa: E402
from .db import init_db  # noqa: E402
from .handlers import add_handlers  # noqa: E402
from .preference_handlers import configure_bot_ui  # noqa: E402
from .updates import run_immediate_then_sync, scheduled_tick  # noqa: E402


def configure_logging():
    sys.stdout = sys.stderr
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def post_init(application):
    await configure_bot_ui(application.bot)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error("Unhandled Telegram update error: update=%r", update, exc_info=context.error)


def build_application():
    application = (
        ApplicationBuilder()
        .token(config.TELEGRAM_BOT_TOKEN)
        .rate_limiter(AIORateLimiter(max_retries=1))
        .post_init(post_init)
        .build()
    )
    if application.job_queue is None:
        raise RuntimeError("Job queue is unavailable; install python-telegram-bot[job-queue].")

    application.job_queue.run_once(
        run_immediate_then_sync,
        when=0,
        job_kwargs={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 60,
        },
    )
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
    add_handlers(application)
    application.add_error_handler(error_handler)
    return application


def main():
    configure_logging()
    if not config.TELEGRAM_BOT_TOKEN:
        logging.error("Missing TELEGRAM_BOT_TOKEN environment variable.")
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    init_db()
    build_application().run_polling(close_loop=False)
