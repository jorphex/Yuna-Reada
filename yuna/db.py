import sqlite3
import time

from . import config


def get_db():
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=3000")
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS feeds (
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                name TEXT NOT NULL,
                latest_ts REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, url)
            );
            CREATE TABLE IF NOT EXISTS blocked_words (
                chat_id INTEGER NOT NULL,
                word TEXT NOT NULL,
                PRIMARY KEY (chat_id, word)
            );
            CREATE TABLE IF NOT EXISTS seen_entries (
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                entry_id TEXT NOT NULL,
                seen_ts REAL NOT NULL,
                PRIMARY KEY (chat_id, url, entry_id)
            );
            CREATE TABLE IF NOT EXISTS feed_failures (
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                count INTEGER NOT NULL,
                next_retry REAL NOT NULL,
                last_status INTEGER NOT NULL,
                PRIMARY KEY (chat_id, url)
            );
            CREATE TABLE IF NOT EXISTS settings (
                chat_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (chat_id, key)
            );
            CREATE INDEX IF NOT EXISTS idx_seen_entries_seen_ts
                ON seen_entries (seen_ts);
            """
        )


def get_chat_ids():
    with get_db() as conn:
        rows = conn.execute("SELECT DISTINCT chat_id FROM feeds").fetchall()
    return [row["chat_id"] for row in rows]


def get_feeds(chat_id: int):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT url, name, latest_ts FROM feeds WHERE chat_id = ? ORDER BY name",
            (chat_id,),
        ).fetchall()
    return [{"xmlUrl": row["url"], "text": row["name"], "latest_ts": row["latest_ts"]} for row in rows]


def feed_exists(chat_id: int, url: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM feeds WHERE chat_id = ? AND url = ?",
            (chat_id, url),
        ).fetchone()
    return row is not None


def upsert_feed(chat_id: int, url: str, name: str):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO feeds (chat_id, url, name, latest_ts)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(chat_id, url) DO UPDATE SET name = excluded.name
            """,
            (chat_id, url, name),
        )


def remove_feed(chat_id: int, url: str):
    with get_db() as conn:
        conn.execute("DELETE FROM feeds WHERE chat_id = ? AND url = ?", (chat_id, url))
        conn.execute("DELETE FROM seen_entries WHERE chat_id = ? AND url = ?", (chat_id, url))
        conn.execute("DELETE FROM feed_failures WHERE chat_id = ? AND url = ?", (chat_id, url))


def update_latest_ts(chat_id: int, url: str, latest_ts: float):
    with get_db() as conn:
        conn.execute(
            """
            UPDATE feeds
            SET latest_ts = CASE WHEN latest_ts < ? THEN ? ELSE latest_ts END
            WHERE chat_id = ? AND url = ?
            """,
            (latest_ts, latest_ts, chat_id, url),
        )


def get_blocked_words(chat_id: int):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT word FROM blocked_words WHERE chat_id = ?",
            (chat_id,),
        ).fetchall()
    return {row["word"] for row in rows}


def add_blocked_word(chat_id: int, word: str):
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO blocked_words (chat_id, word) VALUES (?, ?)",
            (chat_id, word),
        )


def remove_blocked_word(chat_id: int, word: str):
    with get_db() as conn:
        conn.execute(
            "DELETE FROM blocked_words WHERE chat_id = ? AND word = ?",
            (chat_id, word),
        )


def get_setting(chat_id: int, key: str, default: str | None = None) -> str | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE chat_id = ? AND key = ?",
            (chat_id, key),
        ).fetchone()
    return row["value"] if row else default


def set_setting(chat_id: int, key: str, value: str):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO settings (chat_id, key, value)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id,key) DO UPDATE SET value = excluded.value
            """,
            (chat_id, key, value),
        )


def get_feed_failures(chat_id: int):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT url, count, next_retry, last_status FROM feed_failures WHERE chat_id = ? ORDER BY next_retry",
            (chat_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_feed_failure(chat_id: int, url: str):
    with get_db() as conn:
        row = conn.execute(
            "SELECT count, next_retry, last_status FROM feed_failures WHERE chat_id = ? AND url = ?",
            (chat_id, url),
        ).fetchone()
    return dict(row) if row else None


def clear_feed_failure(chat_id: int, url: str):
    with get_db() as conn:
        conn.execute(
            "DELETE FROM feed_failures WHERE chat_id = ? AND url = ?",
            (chat_id, url),
        )


def record_feed_failure(chat_id: int, url: str, status_code: int, retry_delay: int | None):
    now = time.time()
    failure = get_feed_failure(chat_id, url) or {"count": 0, "next_retry": 0, "last_status": None}
    count = failure["count"] + 1
    if status_code == 404:
        base = 24 * 60 * 60
    elif status_code == 403:
        base = 6 * 60 * 60
    elif status_code == 429:
        base = 15 * 60
    else:
        base = 30 * 60
    delay = max(base, retry_delay or 0)
    delay = min(int(delay * (1.5 ** (count - 1))), 24 * 60 * 60)
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO feed_failures (chat_id, url, count, next_retry, last_status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, url) DO UPDATE SET
                count = excluded.count,
                next_retry = excluded.next_retry,
                last_status = excluded.last_status
            """,
            (chat_id, url, count, now + delay, status_code),
        )


def mark_seen_entries(chat_id: int, url: str, entry_ids: list[str]):
    if not entry_ids:
        return
    now = time.time()
    with get_db() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO seen_entries (chat_id, url, entry_id, seen_ts) VALUES (?, ?, ?, ?)",
            [(chat_id, url, entry_id, now) for entry_id in entry_ids],
        )


def get_seen_entry_ids(chat_id: int, url: str, entry_ids: list[str]) -> set[str]:
    if not entry_ids:
        return set()
    seen = set()
    with get_db() as conn:
        for i in range(0, len(entry_ids), 900):
            chunk = entry_ids[i:i + 900]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"""
                SELECT entry_id FROM seen_entries
                WHERE chat_id = ? AND url = ? AND entry_id IN ({placeholders})
                """,
                (chat_id, url, *chunk),
            ).fetchall()
            seen.update(row["entry_id"] for row in rows)
    return seen


def prune_seen_entries(chat_id: int, url: str, max_age_days: int = 90):
    cutoff = time.time() - (max_age_days * 24 * 60 * 60)
    with get_db() as conn:
        conn.execute(
            "DELETE FROM seen_entries WHERE chat_id = ? AND url = ? AND seen_ts < ?",
            (chat_id, url, cutoff),
        )
