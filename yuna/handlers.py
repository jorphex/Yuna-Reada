from telegram.ext import CallbackQueryHandler, CommandHandler, ConversationHandler, MessageHandler, filters

from .feed_handlers import (
    FEED_URL,
    REMOVE_FEED,
    add_receive_url,
    add_start,
    list_feeds,
    refresh,
    remove_receive_feed,
    remove_start,
)
from .preference_handlers import (
    BLOCK_WORD,
    UNBLOCK_WORD,
    block_receive_word,
    block_start,
    cancel,
    digest,
    frequency,
    frequency_choice,
    list_blocked,
    status,
    unblock_receive_word,
    unblock_start,
    unknown_text,
    welcome_user,
)


def add_handlers(application):
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
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("blocked", list_blocked))
    application.add_handler(CommandHandler("digest", digest))
    application.add_handler(CommandHandler("frequency", frequency))
    application.add_handler(CallbackQueryHandler(frequency_choice))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(block_conv)
    application.add_handler(unblock_conv)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))
