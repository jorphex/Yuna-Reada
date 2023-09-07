# Yuna Reada
RSS reader Telegram bot

## Features
➕ /add - Add a feed, send 5 latest entries  
➖ /remove - Remove a feed  
📌 /tag - Tag a post  
🧹 /untag - Untag a tagged post  
🗒 /tags - List tags, filter posts  
🚫 /block - Block words  
🟢 /unblock - Unblock words  
📔 /blocked - List blocked words  
📓 /list - List saved feeds  
🌀 /refresh - Force refresh feeds  
🛌 /cancel - Cancel command  

I think this should be able to handle multiple users, but not having multiple Telegram accounts, I haven't tested it.  
I'm not really sure about tagging. Best to ignore it as it seems confusing to use on Telegram.

## Usage
Replace `TELEGRAM_BOT_TOKEN` with bot token from Botfather  
Replace `interval` in `context.job_queue.run_repeating(send_rss_updates, interval=900, first=0, context=context)` under `welcome_user` to your prefererd update interval in seconds. For an RSS reader, 900 (15 minutes) or more is recommended. (optional)  
🏃 Run yunareada.py in a Docker container

![asdasd](https://github.com/Unknowing9428/Yuna-Reada/assets/144300469/ca0d2b9f-35b3-48f7-9d26-87ee226813c8)
