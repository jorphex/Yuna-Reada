from telegram.ext import Updater, CommandHandler, CallbackContext, ConversationHandler, MessageHandler, Filters
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
import feedparser
import xml.etree.ElementTree as ET
import logging
import time
from time import mktime
from datetime import datetime
import sys
import os
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup as bs

sys.stdout = sys.stderr
TELEGRAM_BOT_TOKEN = "TELEGRAM_BOT_TOKEN"

latest_post_time = {}  # Keys are chat IDs, values are latest post times for each user
blocked_words_dict = {}  # Keys are chat IDs, values are sets of blocked words for each user
blocked_entries_per_chat = {}  # Keys are chat IDs, values are lists of blocked entries for each user
user_feeds = {} # Keys are chat IDS, values are RSS feeds
users_started = set()
FEED_URL = 1
REMOVE_FEED = 2
BLOCK_WORD = 3
UNBLOCK_WORD = 4

def fetch_rss(chat_id, url, latest_post_time):
    global blocked_words_dict, blocked_entries_per_chat

    blocked_words = blocked_words_dict.get(chat_id, set())

    try:
        logging.debug(f"latest_post_time in fetch_rss: type {type(latest_post_time)}, value {latest_post_time}")

        feed = feedparser.parse(url)

        entries = []
        blocked_words_used = set()
        new_latest_post_time = latest_post_time

        for entry in feed.entries:
            entry_time = entry.get('published_parsed', None)

            if entry_time is None:
                logging.debug("Skipped an entry due to lack of 'published_parsed' attribute.")
                continue

            entry_time = time.mktime(entry_time)
            new_latest_post_time = max(new_latest_post_time, entry_time)

            soup = bs(entry.description, 'html.parser')
            entry.description = soup.get_text()
            if 'summary' in entry:
                soup = bs(entry.summary, 'html.parser')
                entry.summary = soup.get_text()

            # Check if the entry should be blocked
            entry_content = entry.title.lower() + ' ' + entry.description.lower() + ' ' + entry.summary.lower()
            entry_blocked = False
            for blocked_word in blocked_words:
                if blocked_word in entry_content:
                    entry_blocked = True
                    break

            if entry_blocked and entry_time > latest_post_time:
                blocked_entries_per_chat[chat_id].append(entry)
                blocked_words_used.add(blocked_word)
            elif not entry_blocked and entry_time > latest_post_time:
                entries.append(entry)

        # Debug lines
        logging.debug(f"Entries fetched: {len(entries)}")
        logging.debug(f"New latest post time: {new_latest_post_time}")
        logging.info(f"Fetched {len(entries)} entries and blocked {len(blocked_entries_per_chat[chat_id])} entries from {url}")

        # Sort the entries by their publish times
        entries.sort(key=lambda x: x.get('published_parsed', 0))

        # Update new_latest_post_time based on the latest entry
        if entries:
            new_latest_post_time = max(new_latest_post_time, time.mktime(entries[-1].get('published_parsed', 0)))

        feed_name = feed.feed.get('title', '')  # Extract the feed name from the feed data

        return entries, new_latest_post_time, feed_name, blocked_words_used
    except Exception as e:
        logging.error(f"Error in 'fetch_rss' function: {str(e)}")
        return [], 0, None, set()

def fetch_and_send_updates(context: CallbackContext, chat_id: int):
    global latest_post_time, blocked_words_dict, user_feeds, blocked_entries_per_chat

    # Create a bot instance using the context
    bot = context.bot

    if chat_id not in user_feeds:
        user_feeds[chat_id] = []
    if chat_id not in latest_post_time:
        latest_post_time[chat_id] = {}
    if chat_id not in blocked_words_dict:
        blocked_words_dict[chat_id] = set()
    if chat_id not in blocked_entries_per_chat:
        blocked_entries_per_chat[chat_id] = []
    else:
        blocked_entries_per_chat[chat_id].clear()

    try:
        all_entries = []
        for url_dict in user_feeds[chat_id]:
            url = url_dict['xmlUrl']
            if url not in latest_post_time[chat_id]:
                latest_post_time[chat_id][url] = 0
            try:
                fetched_entries, new_latest_post_time, feed_name, blocked_words_used = fetch_rss(chat_id, url, latest_post_time[chat_id][url])
            except Exception as e:
                print(f"Error while fetching RSS or printing first entry: {str(e)}")
                continue

            entries = [entry for entry in fetched_entries if mktime(entry.published_parsed) > latest_post_time[chat_id][url]]
            all_entries.extend(entries)

            logging.info(f"Fetched {len(fetched_entries)} entries from {url}")

            latest_post_time[chat_id][url] = new_latest_post_time

        # Sort all entries by publish time
        all_entries.sort(key=lambda entry: entry.published_parsed, reverse=True)

        logging.info(f"Sending {len(all_entries)} entries to chat_id {chat_id}")

        # Send the new entries
        for entry in all_entries:
            try:
                published_time = datetime.fromtimestamp(mktime(entry.published_parsed))
                utc_timestamp = published_time.strftime("%Y-%m-%d %H:%M:%S") + " UTC"

                description = entry.description
                summary = entry.summary
                content = description if len(description) < len(summary) else summary

                # clean HTML tags from content using BeautifulSoup
                soup = bs(content, 'html.parser')
                content = soup.get_text(separator="\n")
                content = (content[:397] + '...') if len(content) > 400 else content

                message = f"<a href='{entry.link}'><b>{entry.title}</b></a>\n<code>{utc_timestamp}</code>\n\n{content}"
                bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')

            except Exception as e:
                print(f"Error while sending message: {str(e)}")

        # After sending all entries, report the blocked entries
        if blocked_entries_per_chat[chat_id]:  # Only send a report if there were blocked entries
            blocked_message = '🚫 Blocked {} new posts\n'.format(len(blocked_entries_per_chat[chat_id]))
            for entry in blocked_entries_per_chat[chat_id]:
                blocked_message += '<a href="{}">{}</a>\n'.format(entry.link, entry.title)
            bot.send_message(chat_id=chat_id, text=blocked_message, parse_mode='HTML', disable_web_page_preview=True)
            
        # Clear the list of blocked entries for this chat after the report is sent
        blocked_entries_per_chat[chat_id].clear()

    except Exception as e:
        logging.error(f"Error in 'fetch_and_send_updates' function: {str(e)}")

def send_rss_updates(context: CallbackContext):
    for chat_id in user_feeds.keys():
        try:
            fetch_and_send_updates(context, chat_id)
        except Exception as e:
            logging.error(f"Error while running 'send_rss_updates' job for chat_id {chat_id}: {str(e)}")

def refresh(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    global user_feeds, latest_post_time

    update.message.reply_text('🔍 Looking for new posts... This might take a moment!')

    if chat_id not in user_feeds or not user_feeds[chat_id]:
        update.message.reply_text('😕 You do not have any feeds. You can add new feeds using the /add command.')
    else:
        try:
            logging.info(f"Running 'refresh' for chat_id {chat_id}: {user_feeds[chat_id]}")
            fetch_and_send_updates(context, chat_id)
            update.message.reply_text('🌟 Refreshed all feeds and sent new updates!')
        except Exception as e:
            logging.error(f"Error in 'refresh' command: {str(e)}")
            update.message.reply_text('🥲 An error occurred while refreshing the feeds. Please try again.')

def load_opml(chat_id):
    opml_file = 'feeds_' + str(chat_id) + '.xml'

    # Check if file exists
    if not os.path.exists(opml_file):
        # If not, create a new XML file with basic structure
        root = ET.Element('opml')
        body = ET.SubElement(root, 'body')
        feeds_parent = ET.SubElement(body, 'outline')
        tree = ET.ElementTree(root)
        tree.write(opml_file)

    with open(opml_file) as f:
        content = f.read()
    bs_content = bs(content, "xml")

    # Look for the nested outline element
    nested_outline = bs_content.body.outline

    feeds = []
    if nested_outline:
        for outline in nested_outline.find_all('outline'):
            feed_data = {'text': outline.get('text'), 'xmlUrl': outline.get('xmlUrl')}
            feeds.append(feed_data)

    logging.debug(f"Loaded feeds for chat_id {chat_id}: {feeds}")

    return feeds

def write_opml(feeds, filename):
    root = ET.Element('opml')
    body = ET.SubElement(root, 'body')
    outline_parent = ET.SubElement(body, 'outline')

    for feed in feeds:
        outline = ET.SubElement(outline_parent, 'outline')
        outline.set('text', feed['text'])
        outline.set('xmlUrl', feed['xmlUrl'])

    indent(root)  # Apply indentation to the XML tree

    tree = ET.ElementTree(root)
    tree.write(filename, encoding='utf-8', xml_declaration=True)

def indent(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def add_start(update: Update, context: CallbackContext) -> int:
    update.message.reply_text('📄 Please send an RSS feed URL or URLs separated by commas, spaces, or line breaks.')
    return FEED_URL

def add_receive_url(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    global latest_post_time, blocked_words_dict, user_feeds, blocked_entries_per_chat

    # If this is the first time this user interacts with the bot, initialize their state variables
    if chat_id not in user_feeds:
        user_feeds[chat_id] = []
    if chat_id not in latest_post_time:
        latest_post_time[chat_id] = {}
    if chat_id not in blocked_words_dict:
        blocked_words_dict[chat_id] = set()
    if chat_id not in blocked_entries_per_chat:
        blocked_entries_per_chat[chat_id] = []

    update.message.reply_text('👨‍💻 Processing. Please wait!')

    raw_urls = update.message.text.strip()
    url_list = re.split(',|\n| ', raw_urls)  # split by commas, newlines or spaces
    url_list = [url.strip() for url in url_list if url.strip()]  # remove extra whitespaces and empty elements

    feeds = []
    all_entries = []

    for url in url_list:
        if not is_valid_url(url):
            update.message.reply_text(f'🙃 Invalid URL detected: {url}')
            continue

        if url in (feed_dict['xmlUrl'] for feed_dict in user_feeds[chat_id]):
            update.message.reply_text(f'😕 RSS feed is already added: {url}')
            continue

        try:
            entries, new_latest_post_time, feed_name, blocked_words_used = fetch_rss(chat_id, url, latest_post_time[chat_id].get(url, 0))
            feed_name = feed_name if feed_name else "Unknown Feed"
            feeds.append({'xmlUrl': url, 'text': feed_name})
            all_entries.extend(entries)
            latest_post_time[chat_id][url] = new_latest_post_time
        except Exception as e:
            logging.error(f"Error in 'fetch_rss': {str(e)}")
            update.message.reply_text(f'😤 Failed to fetch RSS feed from {url}. Please check the URL and try again.')
            continue

    # Sort all entries by publish time
    all_entries.sort(key=lambda entry: entry.published_parsed, reverse=False)

    # Inform the user that the five latest articles are being sent
    context.bot.send_message(chat_id=chat_id, text="📰 Here are the five latest items from your feeds:")

    # Select the five latest posts in descending order (newest to oldest)
    latest_entries = sorted(all_entries[-5:], key=lambda entry: entry.published_parsed, reverse=False)

    # Send the new entries
    for entry in latest_entries:
        try:
            published_time = datetime.fromtimestamp(mktime(entry.published_parsed))
            utc_timestamp = published_time.strftime("%Y-%m-%d %H:%M:%S") + " UTC"

            description = entry.description
            summary = entry.summary
            content = description if len(description) < len(summary) else summary

            # clean HTML tags from content using BeautifulSoup
            soup = bs(content, 'html.parser')
            content = soup.get_text(separator="\n")
            content = (content[:397] + '...') if len(content) > 400 else content

            message = f"<a href='{entry.link}'><b>{entry.title}</b></a>\n<code>{utc_timestamp}</code>\n\n{content}"
            context.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
        except Exception as e:
            print(f"Error while sending message: {str(e)}")

    logging.info(f"Added URL {url}, fetched {len(entries)} entries")
    joined_urls = "\n".join(url for url in url_list)
    update.message.reply_text(f'☺️ Successfully added the RSS feeds:\n{joined_urls}')

    # Inform the user about the XML/OPML file for backup
    context.bot.send_message(chat_id=chat_id, text="🗃 Here's an XML/OPML file of your feeds for backup:")

    filename = f"{chat_id}_feeds.xml"
    try:
        write_opml(feeds, filename)
        user_feeds[chat_id] = feeds
        context.bot.send_document(chat_id=chat_id, document=open(filename, 'rb'))
    except Exception as e:
        logging.error(f"Error while sending document: {str(e)}")
        update.message.reply_text(f'😥 Failed to send document for {", ".join(url for url in url_list)}')
        return FEED_URL

    return ConversationHandler.END

def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def cancel_add(update: Update, context: CallbackContext):
    update.message.reply_text('Cancelled adding a feed.')
    return ConversationHandler.END

def remove_start(update: Update, context: CallbackContext) -> int:
    chat_id = update.effective_chat.id
    global user_feeds

    if chat_id not in user_feeds or not user_feeds[chat_id]:
        message = '🤷‍♀️ You have no feeds to remove. You can add new feeds using the /add command.'
    else:
        feed_list = [(feed_dict['text'], feed_dict['xmlUrl']) for feed_dict in user_feeds[chat_id]]
        feed_list.sort(key=lambda x: x[0])
        message = "Here are your current feeds:\n\n"
        for feed_name, feed_url in feed_list:
            message += f"{feed_name}: {feed_url}\n"
        message += "\n🤔 Which feed would you like to remove? Please send the feed title or URL."

    update.message.reply_text(message)
    return REMOVE_FEED

def remove_receive_feed(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    global user_feeds

    feed = update.message.text.strip()

    if not feed:
        update.message.reply_text('☝️ You must provide a feed title or URL')
        return

    if chat_id not in user_feeds or not user_feeds[chat_id]:
        update.message.reply_text('🤷‍♀️ You have no feeds to remove.')
        return

    feed_to_remove = None
    for feed_dict in user_feeds[chat_id]:
        if feed_dict['text'] == feed or feed_dict['xmlUrl'] == feed:
            feed_to_remove = feed_dict
            break

    if not feed_to_remove:
        update.message.reply_text('🤷‍♀️ No matching feed found. Please check the feed title or URL and try again.')
        return

    user_feeds[chat_id] = [feed_dict for feed_dict in user_feeds[chat_id] if feed_dict != feed_to_remove]

    # Generate and send the updated OPML file as a document to the user
    opml_filename = f"{chat_id}_feeds.xml"
    write_opml(user_feeds[chat_id], opml_filename)
    with open(opml_filename, 'rb') as opml_file:
        context.bot.send_document(chat_id=chat_id, document=opml_file)

    update.message.reply_text(f'🧹 Removed feed: {feed_to_remove["text"]}')
    return ConversationHandler.END

def list_feeds(update: Update, context: CallbackContext):
    chat_id = update.effective_chat.id
    global user_feeds

    try:
        if chat_id not in user_feeds or not user_feeds[chat_id]:
            update.message.reply_text('🤷‍♀️ You do not have any feeds. You can add new feeds using the /add command.')
        else:
            feed_list = [feed_dict['xmlUrl'] for feed_dict in user_feeds[chat_id]]
            feed_list.sort()
            feed_list = [f"<a href='{feed_url}'>{feed_url}</a>" for feed_url in feed_list]
            update.message.reply_text('\n'.join(feed_list), parse_mode='HTML', disable_web_page_preview=True)

            # Generate and send the OPML file as a document to the user
            opml_filename = f"{chat_id}_feeds.xml"
            write_opml(user_feeds[chat_id], opml_filename)
            with open(opml_filename, 'rb') as opml_file:
                context.bot.send_document(chat_id=chat_id, document=opml_file)

    except Exception as e:
        logging.error(f"Error in 'list_feeds' command: {str(e)}")
        update.message.reply_text('😰 An error occurred while listing the feeds. Please try again.')

def block_start(update: Update, context: CallbackContext) -> int:
    update.message.reply_text('🔍 Please enter the word or phrase to block:')
    return BLOCK_WORD

def block_receive_word(update: Update, context: CallbackContext) -> int:
    chat_id = update.message.chat_id
    word = update.message.text.strip().lower()

    # If the chat doesn't have a set of blocked words yet, create one
    if chat_id not in blocked_words_dict:
        blocked_words_dict[chat_id] = set()

    if word not in blocked_words_dict[chat_id]:
        blocked_words_dict[chat_id].add(word)
        update.message.reply_text(f'🚫 Word blocked: {word}')
    else:
        update.message.reply_text(f'🤷‍♀️ Word is already blocked: {word}')
    return ConversationHandler.END

def unblock_start(update: Update, context: CallbackContext) -> int:
    chat_id = update.message.chat_id
    if chat_id in blocked_words_dict and blocked_words_dict[chat_id]:
        update.message.reply_text('🗒 Select a word or phrase to unblock:\n' + '\n'.join(blocked_words_dict[chat_id]))
        return UNBLOCK_WORD
    else:
        update.message.reply_text('🤷‍♀️ No blocked words or phrases.')
        return ConversationHandler.END

def unblock_receive_word(update: Update, context: CallbackContext) -> int:
    chat_id = update.message.chat_id
    word = update.message.text.strip().lower()
    if chat_id in blocked_words_dict and word in blocked_words_dict[chat_id]:
        blocked_words_dict[chat_id].remove(word)
        update.message.reply_text(f'🟢 Word unblocked: {word}')
    else:
        update.message.reply_text(f'❕ Word is not in the blocked list: {word}')
    return ConversationHandler.END

def list_blocked(update: Update, context: CallbackContext) -> None:
    chat_id = update.message.chat_id
    if chat_id in blocked_words_dict and blocked_words_dict[chat_id]:
        update.message.reply_text('🚫 Blocked words:\n' + '\n'.join(blocked_words_dict[chat_id]))
    else:
        update.message.reply_text(' No blocked words.')

def cancel(update: Update, context: CallbackContext) -> int:
    update.message.reply_text('🛌 Command cancelled.')
    return ConversationHandler.END

def welcome_user(update: Update, context: CallbackContext):
    user_id = update.effective_user.id

    # Check if the user has already interacted with the bot
    if user_id not in users_started:
        welcome_message = """
        I am Yuna Reada, an RSS reader bot! 📚

Here are the available commands:
/add ➕ Add feeds, send 5 latest entries
/remove ➖ Remove feeds
/block 🚫 Block words
/unblock 🟢 Unblock words
/blocked 📔 List blocked words
/list 📓 List saved feeds
/refresh 🌀 Force refresh feeds
/cancel 🛌 Cancel command

Have a Kindle? Send feed posts to your Kindle easily with @TheKindlerBot 📨

I check feeds for new entries every 15 minutes.
Send the /add command to begin! 🥰
        """
        context.bot.send_message(chat_id=update.effective_chat.id, text=welcome_message)
        users_started.add(user_id)
        context.job_queue.run_repeating(send_rss_updates, interval=900, first=0, context=context)

def main():
    try:
        global BLOCK_WORD, UNBLOCK_WORD
        updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)

        dispatcher = updater.dispatcher

        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.WARNING)

        add_handler = ConversationHandler(
            entry_points=[CommandHandler('add', add_start)],
            states={
                FEED_URL: [MessageHandler(Filters.text & ~Filters.command, add_receive_url)],
            },
            fallbacks=[CommandHandler('cancel', cancel)],
        )

        remove_handler = ConversationHandler(
            entry_points=[CommandHandler('remove', remove_start)],
            states={
                REMOVE_FEED: [MessageHandler(Filters.text & ~Filters.command, remove_receive_feed)],
            },
            fallbacks=[CommandHandler('cancel', cancel)],
        )

        block_handler = ConversationHandler(
            entry_points=[CommandHandler('block', block_start)],
            states={
                BLOCK_WORD: [MessageHandler(Filters.text & ~Filters.command, block_receive_word)],
            },
            fallbacks=[CommandHandler('cancel', cancel)],
        )

        unblock_handler = ConversationHandler(
            entry_points=[CommandHandler('unblock', unblock_start)],
            states={
                UNBLOCK_WORD: [MessageHandler(Filters.text & ~Filters.command, unblock_receive_word)],
            },
            fallbacks=[CommandHandler('cancel', cancel)],
        )

        dispatcher.add_handler(remove_handler)
        dispatcher.add_handler(add_handler)
        dispatcher.add_handler(CommandHandler('list', list_feeds))
        dispatcher.add_handler(CommandHandler('refresh', refresh))
        dispatcher.add_handler(CommandHandler('blocked', list_blocked))
        dispatcher.add_handler(unblock_handler)
        dispatcher.add_handler(block_handler)
        dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, welcome_user))

        updater.start_polling()
        updater.idle()

    except Exception as e:
        logging.error(f"An error occurred in the main function: {str(e)}")

if __name__ == '__main__':
    main()
