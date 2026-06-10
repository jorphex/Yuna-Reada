import asyncio
from types import SimpleNamespace

import pytest

from telegram.ext import ConversationHandler

from yuna import config, db, feed_handlers, opml, preference_handlers, rss, ui, updates


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "yunareada.db"))
    db.init_db()


def test_normalize_feed_url_rejects_unsafe_hosts(monkeypatch):
    monkeypatch.setattr(
        rss.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(None, None, None, None, ("93.184.216.34", 0))],
    )

    assert rss.normalize_feed_url("HTTPS://Example.COM/feed.xml#frag") == "https://example.com/feed.xml"
    assert rss.normalize_feed_url("ftp://example.com/feed.xml") is None
    assert rss.normalize_feed_url("https://user:pass@example.com/feed.xml") is None
    assert rss.normalize_feed_url("http://127.0.0.1/feed.xml") is None
    assert rss.normalize_feed_url("http://localhost/feed.xml") is None


def test_normalize_feed_url_rejects_dns_private_address(monkeypatch):
    monkeypatch.setattr(
        rss.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(None, None, None, None, ("10.0.0.5", 0))],
    )

    assert rss.normalize_feed_url("https://feeds.example.com/rss") is None


def test_blocked_word_matching_uses_word_boundaries():
    assert not rss.is_blocked("A paid subscription roundup", {"ai"})
    assert rss.is_blocked("New AI tools for readers", {"ai"})
    assert rss.is_blocked("A phrase appears here", {"phrase appears"})


def test_entry_formatting_escapes_html_and_omits_bad_links():
    entry = SimpleNamespace(
        title="<Breaking & News>",
        link="javascript:alert(1)",
        description="<p>Short & useful</p>",
        summary="<p>Longer summary that should not be selected</p>",
        _timestamp=0,
    )

    message = ui.format_entry_message(entry)

    assert "javascript:" not in message
    assert "&lt;Breaking &amp; News&gt;" in message
    assert "Short &amp; useful" in message
    assert "<code>1970-01-01 00:00:00 UTC</code>" in message
    assert len(message) < config.TELEGRAM_MESSAGE_LIMIT


def test_chunk_lines_never_exceeds_limit():
    chunks = ui.chunk_lines(["a" * 10, "b" * 10, "c" * 10], limit=15)

    assert chunks == ["a" * 10, "b" * 10, "c" * 10]
    assert all(len(chunk) <= 15 for chunk in chunks)


def test_send_message_chunks_attaches_reply_markup_to_last_chunk(monkeypatch):
    asyncio.run(_run_send_message_chunks_attaches_reply_markup_to_last_chunk(monkeypatch))


async def _run_send_message_chunks_attaches_reply_markup_to_last_chunk(monkeypatch):
    sent = []

    class Bot:
        async def send_message(self, **kwargs):
            sent.append(kwargs)

    monkeypatch.setattr(ui.asyncio, "sleep", lambda *args, **kwargs: _done())

    await ui.send_message_chunks(
        Bot(),
        1,
        ("a" * config.MESSAGE_CHUNK_LIMIT) + "\nsecond",
        reply_markup=config.MAIN_KEYBOARD,
    )

    assert len(sent) == 2
    assert "reply_markup" not in sent[0]
    assert sent[1]["reply_markup"] == config.MAIN_KEYBOARD


def test_feed_url_link_truncates_display_text():
    long_url = "https://example.com/" + "a" * 2000
    line = ui.feed_url_link(long_url)

    assert long_url in line
    assert len(line) < config.TELEGRAM_MESSAGE_LIMIT


def test_force_reply_truncates_placeholder():
    reply_markup = ui.force_reply("x" * 200)

    assert reply_markup.selective is True
    assert len(reply_markup.input_field_placeholder) <= config.FORCE_REPLY_PLACEHOLDER_LIMIT


def test_main_keyboard_exposes_common_commands():
    labels = [
        button.text
        for row in config.MAIN_KEYBOARD.keyboard
        for button in row
    ]

    assert labels == ["/add", "/list", "/refresh", "/status", "/digest", "/frequency"]


def test_build_opml_bytes_contains_feeds():
    data = opml.build_opml_bytes(
        [{"text": "Example", "xmlUrl": "https://example.com/feed.xml"}]
    )

    assert b"<?xml" in data
    assert b"Example" in data
    assert b"https://example.com/feed.xml" in data


def test_format_refresh_summary_handles_empty_and_nonempty_results():
    assert ui.format_refresh_summary({"feeds": 0, "entries": 0, "blocked": 0}).startswith("No feeds yet")
    assert ui.format_refresh_summary({"feeds": 2, "entries": 0, "blocked": 0}) == (
        "🌟 Refresh complete: no new posts from 2 feeds."
    )
    assert ui.format_refresh_summary({"feeds": 1, "entries": 1, "blocked": 2}) == (
        "🌟 Refresh complete: sent 1 new post, hid 2 posts from 1 feed."
    )
    assert ui.format_refresh_summary({"feeds": 1, "entries": 0, "blocked": 1}) == (
        "🌟 Refresh complete: hid 1 post from 1 feed."
    )


def test_format_status_lines_includes_settings_and_next_run():
    db.set_setting(1, "digest", "1")
    db.set_setting(1, "frequency", "daily")
    db.set_setting(1, "next_run", "86400")

    lines = ui.format_status_lines(1, [], {"politics"}, [])

    assert "Hidden terms: 1" in lines
    assert "Digest: on" in lines
    assert "Frequency: daily at 00:00 UTC" in lines
    assert "Next check: 1970-01-02 00:00:00 UTC" in lines
    assert "Paused feeds: none" in lines


def test_fetch_rss_rejects_non_feed_payload(monkeypatch):
    class Response:
        status_code = 200
        content = b"<html><title>Not a feed</title></html>"
        headers = {}

        def raise_for_status(self):
            return None

    monkeypatch.setattr(rss.requests, "get", lambda *args, **kwargs: Response())
    monkeypatch.setattr(rss, "is_global_host", lambda hostname: True)

    entries, blocked, latest_ts, feed_name = rss.fetch_rss(
        1,
        "https://example.com/not-feed",
        123,
        set(),
    )

    assert entries == []
    assert blocked == []
    assert latest_ts == 123
    assert feed_name is None


def test_fetch_and_send_updates_keeps_working_after_one_fetch_error(monkeypatch):
    asyncio.run(_run_fetch_and_send_updates_keeps_working_after_one_fetch_error(monkeypatch))


async def _run_fetch_and_send_updates_keeps_working_after_one_fetch_error(monkeypatch):
    db.upsert_feed(1, "https://example.com/bad.xml", "bad")
    db.upsert_feed(1, "https://example.com/good.xml", "good")

    good_entry = SimpleNamespace(
        title="Good",
        link="https://example.com/good",
        description="Good description",
        summary="Good description",
        _timestamp=10,
        _has_timestamp=True,
    )

    async def fake_fetch_with_limit(chat_id, feed, blocked_words):
        if "bad" in feed["xmlUrl"]:
            raise RuntimeError("network failure")
        return [good_entry], [], 10, "good"

    sent = []

    class Bot:
        async def send_message(self, **kwargs):
            sent.append(kwargs)

    monkeypatch.setattr(updates, "fetch_feed_with_limit", fake_fetch_with_limit)
    monkeypatch.setattr(updates.asyncio, "sleep", lambda *args, **kwargs: _done())

    await updates.fetch_and_send_updates(SimpleNamespace(bot=Bot()), 1)

    assert len(sent) == 1
    assert "Good" in sent[0]["text"]


def test_remove_receive_feed_accepts_number_choice(monkeypatch):
    asyncio.run(_run_remove_receive_feed_accepts_number_choice(monkeypatch))


async def _run_remove_receive_feed_accepts_number_choice(monkeypatch):
    db.upsert_feed(1, "https://example.com/feed.xml", "Example")
    replies = []
    documents = []

    class Message:
        text = "1"

        async def reply_text(self, text, **kwargs):
            replies.append(text)

    class Bot:
        async def send_document(self, **kwargs):
            documents.append(kwargs)

    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=1),
        message=Message(),
    )
    context = SimpleNamespace(
        bot=Bot(),
        user_data={"remove_feed_choices": {"1": "https://example.com/feed.xml"}},
    )
    result = await feed_handlers.remove_receive_feed(update, context)

    assert result == ConversationHandler.END
    assert replies == ["Removed feed: Example"]
    assert documents
    assert db.get_feeds(1) == []


def test_configure_bot_ui_sets_profile_and_menu():
    asyncio.run(_run_configure_bot_ui_sets_profile_and_menu())


async def _run_configure_bot_ui_sets_profile_and_menu():
    calls = []

    class Bot:
        async def set_my_commands(self, commands):
            calls.append(("commands", commands))

        async def set_chat_menu_button(self, *, menu_button):
            calls.append(("menu", menu_button))

        async def set_my_short_description(self, short_description):
            calls.append(("short", short_description))

        async def set_my_description(self, description):
            calls.append(("description", description))

    await preference_handlers.configure_bot_ui(Bot())

    assert calls[0] == ("commands", config.BOT_COMMANDS)
    assert calls[1][0] == "menu"
    assert calls[2] == ("short", config.BOT_SHORT_DESCRIPTION)
    assert calls[3] == ("description", config.BOT_DESCRIPTION)


async def _done():
    return None
