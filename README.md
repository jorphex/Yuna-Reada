# Yuna Reada
RSS reader Telegram bot

## Features
⌨️ Persistent Telegram command keyboard for common actions
➕ `/add` - Add a feed, send 5 latest entries
➖ `/remove` - Remove a feed
🚫 `/block` - Hide words or phrases
🟢 `/unblock` - Unhide words or phrases
📔 `/blocked` - List hidden terms
📓 `/list` - List saved feeds
🌀 `/refresh` - Force refresh feeds
🛌 `/cancel` - Cancel command
🗞️ `/digest` - Toggle digest mode
🕒 `/frequency` - Set update frequency
📊 `/status` - Show status

## Documentation
See [`projects/docs/yunareada/`](../docs/yunareada/) for comprehensive documentation including:
- Setup guide
- API reference
- Implementation details
- Troubleshooting

## Configuration
Set environment variables:
- `TELEGRAM_BOT_TOKEN` - Your bot token from @BotFather
- `YUNAREADA_DB` (optional) - Database path

Run with: `python3 yunareada.py`

## Notes
I think this should be able to handle multiple users, but not having multiple Telegram accounts, I haven't tested it.  
