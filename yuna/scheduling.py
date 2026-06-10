from datetime import datetime, timedelta, timezone

from .db import get_setting, set_setting


def next_aligned_run(now_ts: float, frequency: str) -> float:
    now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    if frequency == "daily":
        next_midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
        return next_midnight.timestamp()

    minutes_since_midnight = now.hour * 60 + now.minute
    next_slot = (minutes_since_midnight // 15 + 1) * 15
    next_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    if next_slot >= 24 * 60:
        next_day += timedelta(days=1)
        next_slot = 0
    return (next_day + timedelta(minutes=next_slot)).timestamp()


def get_frequency(chat_id: int) -> str:
    freq = get_setting(chat_id, "frequency", "15m") or "15m"
    return freq if freq in {"15m", "daily"} else "15m"


def set_next_run(chat_id: int, next_run_ts: float):
    set_setting(chat_id, "next_run", str(next_run_ts))


def get_next_run(chat_id: int) -> float | None:
    value = get_setting(chat_id, "next_run")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None
