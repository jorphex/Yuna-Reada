import calendar
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import ipaddress
import logging
import re
import socket
import time
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup as bs
import feedparser
import requests

from . import config
from .db import (
    clear_feed_failure,
    get_feed_failure,
    get_seen_entry_ids,
    mark_seen_entries,
    prune_seen_entries,
    record_feed_failure,
)
from .ui import truncate_text


def is_global_host(hostname: str) -> bool:
    host = hostname.rstrip(".").casefold()
    if host in {"localhost", ""} or host.endswith(".localhost"):
        return False

    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        pass

    try:
        addresses = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        return True

    for address in addresses:
        ip = address[4][0]
        try:
            if not ipaddress.ip_address(ip).is_global:
                return False
        except ValueError:
            return False
    return True


def normalize_feed_url(raw_url: str) -> str | None:
    url = raw_url.strip()
    if not url or len(url) > config.MAX_URL_LENGTH:
        return None

    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.casefold()
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None

    if scheme not in {"http", "https"} or not hostname or parsed.username or parsed.password:
        return None
    if not is_global_host(hostname):
        return None

    netloc = hostname.casefold()
    if port is not None:
        netloc = f"{netloc}:{port}"
    path = parsed.path or "/"
    return urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))


def is_valid_url(url: str) -> bool:
    return normalize_feed_url(url) is not None


def normalize_block_term(term: str) -> str:
    return truncate_text(term.casefold(), config.MAX_BLOCK_TERM_LENGTH)


def parse_block_terms(raw: str) -> list[str]:
    terms = []
    for term in raw.split(","):
        normalized = normalize_block_term(term)
        if normalized:
            terms.append(normalized)
    return terms


def blocked_term_matches(content: str, term: str) -> bool:
    if not term:
        return False
    if re.fullmatch(r"[\w-]+", term):
        return re.search(rf"(?<!\w){re.escape(term)}(?!\w)", content) is not None
    return term in content


def is_blocked(content: str, blocked_words: set[str]) -> bool:
    normalized = " ".join(content.casefold().split())
    return any(blocked_term_matches(normalized, word) for word in blocked_words)


def entry_id(entry) -> str:
    raw_id = (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or ((entry.get("title") or "") + (entry.get("summary") or ""))
    )
    raw_id = str(raw_id or "")
    if len(raw_id) > config.MAX_ENTRY_ID_LENGTH:
        return "sha256:" + hashlib.sha256(raw_id.encode("utf-8", "ignore")).hexdigest()
    return raw_id


def retry_delay_from_header(value: str | None) -> int | None:
    if not value:
        return None
    if value.isdigit():
        return int(value)
    try:
        retry_dt = parsedate_to_datetime(value)
        return int((retry_dt - datetime.now(tz=timezone.utc)).total_seconds())
    except Exception:
        return None


def fetch_rss(chat_id: int, url: str, latest_post_time: float, blocked_words: set[str]):
    try:
        normalized_url = normalize_feed_url(url)
        if not normalized_url:
            logging.warning("Skipping unsafe or invalid feed URL for chat %s: %s", chat_id, url)
            return [], [], latest_post_time, None
        request_url = normalized_url

        failure = get_feed_failure(chat_id, url)
        if failure and time.time() < failure.get("next_retry", 0):
            logging.info(
                f"Skipping fetch for {url}; retry after "
                f"{datetime.fromtimestamp(failure['next_retry'], tz=timezone.utc)}"
            )
            return [], [], latest_post_time, None

        response = requests.get(
            request_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept": "application/rss+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=(5, 20),
        )
        if response.status_code in (403, 404, 429):
            retry_delay = retry_delay_from_header(response.headers.get("Retry-After"))
            record_feed_failure(chat_id, url, response.status_code, retry_delay)
            logging.warning(f"HTTP {response.status_code} fetching RSS for {url}")
            return [], [], latest_post_time, None
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        feed_meta = getattr(feed, "feed", None) or {}
        if not feed.entries and not feed_meta:
            logging.warning("Invalid RSS payload for %s: no feed metadata or entries", url)
            return [], [], latest_post_time, None

        entries = []
        blocked_entries = []
        new_latest_post_time = latest_post_time
        clear_feed_failure(chat_id, url)

        raw_items = []
        for entry in feed.entries:
            entry_time = entry.get("published_parsed") or entry.get("updated_parsed")
            if entry_time:
                entry_timestamp = calendar.timegm(entry_time)
                has_timestamp = True
                if entry_timestamp < latest_post_time:
                    continue
            else:
                entry_timestamp = time.time()
                has_timestamp = False

            item_id = entry_id(entry)
            if not item_id:
                item_id = f"{entry_timestamp}:{entry.get('title', '')}"
            raw_items.append((entry, entry_timestamp, has_timestamp, item_id))

        seen_lookup = get_seen_entry_ids(chat_id, url, [item[3] for item in raw_items])
        seen_entry_ids = []
        for entry, entry_timestamp, has_timestamp, item_id in raw_items:
            if item_id in seen_lookup:
                continue
            entry._entry_id = item_id
            entry._timestamp = entry_timestamp
            entry._has_timestamp = has_timestamp
            seen_entry_ids.append(item_id)

            entry.title = entry.get("title") or "Untitled"
            entry.description = bs(entry.get("description", ""), "html.parser").get_text()
            entry.summary = bs(entry.get("summary", entry.description), "html.parser").get_text()

            content = entry.title + " " + entry.description + " " + entry.summary
            if is_blocked(content, blocked_words):
                blocked_entries.append(entry)
            else:
                entries.append(entry)

        if entries or blocked_entries:
            ts_candidates = [entry._timestamp for entry in entries + blocked_entries if entry._has_timestamp]
            if ts_candidates:
                new_latest_post_time = max(ts_candidates)
        feed_name = feed_meta.get("title") or request_url

        mark_seen_entries(chat_id, url, seen_entry_ids)
        prune_seen_entries(chat_id, url)

    except requests.RequestException as e:
        logging.error(f"HTTP error fetching RSS for chat {chat_id}, URL {url}: {e}", exc_info=True)
        return [], [], latest_post_time, None
    except Exception as e:
        logging.error(f"Error fetching RSS for chat {chat_id}, URL {url}: {e}", exc_info=True)
        return [], [], latest_post_time, None

    return entries, blocked_entries, new_latest_post_time, feed_name
