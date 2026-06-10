import asyncio
from datetime import datetime, timezone
import html
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup as bs
from telegram import ForceReply

from . import config
from .db import get_setting
from .scheduling import get_frequency, get_next_run


def truncate_text(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[:limit - 3].rstrip() + "..."


def force_reply(placeholder: str) -> ForceReply:
    return ForceReply(
        selective=True,
        input_field_placeholder=truncate_text(placeholder, config.FORCE_REPLY_PLACEHOLDER_LIMIT),
    )


def entry_get(entry, key: str, default=None):
    if hasattr(entry, "get"):
        return entry.get(key, default)
    return getattr(entry, key, default)


def clean_html_text(value) -> str:
    return bs(str(value or ""), "html.parser").get_text(separator=" ", strip=True)


def safe_http_link(url: str | None) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(str(url).strip())
    except ValueError:
        return ""
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.netloc:
        return ""
    return html.escape(urlunparse(parsed._replace(fragment="")), quote=True)


def entry_title(entry) -> str:
    return truncate_text(entry_get(entry, "title", "Untitled") or "Untitled", 180)


def entry_summary(entry, limit: int = 400) -> str:
    description = clean_html_text(entry_get(entry, "description", ""))
    summary = clean_html_text(entry_get(entry, "summary", ""))
    candidates = [text for text in (description, summary) if text]
    if not candidates:
        return ""
    return truncate_text(min(candidates, key=len), limit)


def entry_timestamp(entry) -> str:
    published = datetime.fromtimestamp(entry._timestamp, tz=timezone.utc)
    return published.strftime("%Y-%m-%d %H:%M:%S") + " UTC"


def entry_link_html(entry, *, bold: bool = False) -> str:
    safe_title = html.escape(entry_title(entry))
    safe_link = safe_http_link(entry_get(entry, "link", ""))
    label = f"<b>{safe_title}</b>" if bold else safe_title
    if safe_link:
        return f'<a href="{safe_link}">{label}</a>'
    return label


def format_entry_message(entry) -> str:
    content = entry_summary(entry)
    parts = [
        entry_link_html(entry, bold=True),
        f"<code>{entry_timestamp(entry)}</code>",
    ]
    if content:
        parts.append(html.escape(content))
    return "\n".join(parts[:2]) + ("\n\n" + parts[2] if len(parts) > 2 else "")


def format_digest_line(entry) -> str:
    return f"{entry_link_html(entry)} - <code>{entry_timestamp(entry)}</code>"


def feed_url_link(url: str) -> str:
    safe_link = html.escape(url, quote=True)
    safe_label = html.escape(truncate_text(url, 180))
    return f'<a href="{safe_link}">{safe_label}</a>'


def feed_display_line(name: str, url: str) -> str:
    return f"{truncate_text(name, 120)}: {truncate_text(url, 220)}"


def feed_html_line(index: int, feed: dict) -> str:
    name = html.escape(truncate_text(feed["text"], 120))
    url_link = feed_url_link(feed["xmlUrl"])
    return f"{index}. <b>{name}</b>\n{url_link}"


def feed_plain_line(index: int, feed: dict) -> str:
    return f"{index}. {feed_display_line(feed['text'], feed['xmlUrl'])}"


def format_next_run(chat_id: int) -> str:
    next_run = get_next_run(chat_id)
    if next_run is None:
        return "not scheduled yet"
    next_dt = datetime.fromtimestamp(next_run, tz=timezone.utc)
    return f"{next_dt:%Y-%m-%d %H:%M:%S} UTC"


def frequency_label(value: str) -> str:
    if value == "daily":
        return "daily at 00:00 UTC"
    return "every 15 minutes"


def format_status_lines(chat_id: int, feeds: list[dict], blocked: set[str], failures: list[dict]) -> list[str]:
    digest_enabled = get_setting(chat_id, "digest", "0") == "1"
    frequency = get_frequency(chat_id)
    lines = [
        "📊 Status",
        f"Feeds: {len(feeds)}",
        f"Hidden terms: {len(blocked)}",
        f"Digest: {'on' if digest_enabled else 'off'}",
        f"Frequency: {frequency_label(frequency)}",
        f"Next check: {format_next_run(chat_id)}",
    ]
    if not failures:
        lines.append("Paused feeds: none")
        return lines

    lines.append(f"Paused feeds: {len(failures)}")
    for failure in failures[:5]:
        retry_at = datetime.fromtimestamp(failure["next_retry"], tz=timezone.utc)
        url = truncate_text(failure["url"], 160)
        lines.append(f"- {url} (HTTP {failure['last_status']}, retry {retry_at:%Y-%m-%d %H:%M:%S} UTC)")
    if len(failures) > 5:
        lines.append(f"...and {len(failures) - 5} more")
    return lines


def format_refresh_summary(result: dict[str, int]) -> str:
    entries = result.get("entries", 0)
    blocked = result.get("blocked", 0)
    feeds = result.get("feeds", 0)
    if entries or blocked:
        parts = []
        if entries:
            parts.append(f"sent {entries} new post{'s' if entries != 1 else ''}")
        if blocked:
            parts.append(f"hid {blocked} post{'s' if blocked != 1 else ''}")
        return f"🌟 Refresh complete: {', '.join(parts)} from {feeds} feed{'s' if feeds != 1 else ''}."
    if feeds:
        return f"🌟 Refresh complete: no new posts from {feeds} feed{'s' if feeds != 1 else ''}."
    return "No feeds yet. Use /add with an RSS feed URL to get started."


def help_text() -> str:
    return (
        "Yuna Reada follows RSS feeds and sends new posts here.\n\n"
        "Start with /add and paste one or more feed URLs.\n\n"
        "Feeds\n"
        "/add - Add feeds\n"
        "/list - Show feeds and send an OPML backup\n"
        "/remove - Remove a feed by number or URL\n"
        "/refresh - Check for new posts now\n\n"
        "Preferences\n"
        "/digest - Switch between summary and individual posts\n"
        "/frequency - Choose update frequency\n"
        "/block - Hide posts containing words or phrases\n"
        "/blocked - Show hidden terms\n"
        "/unblock - Remove hidden terms\n\n"
        "/status - Show current settings and paused feeds\n"
        "/cancel - Cancel the current command"
    )


def split_line_for_limit(line: str, limit: int) -> list[str]:
    if len(line) <= limit:
        return [line]
    chunks = []
    remaining = line
    while len(remaining) > limit:
        chunks.append(remaining[:limit])
        remaining = remaining[limit:]
    if remaining:
        chunks.append(remaining)
    return chunks


def chunk_lines(lines: list[str], limit: int = config.MESSAGE_CHUNK_LIMIT) -> list[str]:
    limit = min(limit, config.TELEGRAM_MESSAGE_LIMIT)
    chunks = []
    current = []
    current_len = 0
    for raw_line in lines:
        for line in split_line_for_limit(raw_line, limit):
            line_len = len(line) + 1
            if current and current_len + line_len > limit:
                chunks.append("\n".join(current))
                current = [line]
                current_len = len(line)
            else:
                current.append(line)
                current_len += line_len
    if current:
        chunks.append("\n".join(current))
    return chunks


async def send_message_chunks(bot, chat_id: int, text: str, **kwargs):
    chunks = chunk_lines(text.splitlines() or [text])
    reply_markup = kwargs.pop("reply_markup", None)
    for index, chunk in enumerate(chunks):
        chunk_kwargs = dict(kwargs)
        if reply_markup is not None and index == len(chunks) - 1:
            chunk_kwargs["reply_markup"] = reply_markup
        await bot.send_message(chat_id=chat_id, text=chunk, **chunk_kwargs)
        await asyncio.sleep(0.25)
