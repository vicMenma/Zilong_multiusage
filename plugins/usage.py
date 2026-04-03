"""
plugins/usage.py
/usage — session statistics dashboard.

Shows total data uploaded/downloaded, files processed,
average speeds, uptime, and CloudConvert jobs used.
"""
from __future__ import annotations

import time
from datetime import datetime

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from services.utils import human_size, human_dur

# ── Session-level counters (reset on bot restart) ─────────────
class _Session:
    start_time:       float = time.time()
    bytes_uploaded:   int   = 0
    bytes_downloaded: int   = 0
    files_uploaded:   int   = 0
    files_downloaded: int   = 0
    cc_jobs_started:  int   = 0
    cc_jobs_done:     int   = 0
    tasks_total:      int   = 0
    tasks_failed:     int   = 0

session = _Session()


def _build_text() -> str:
    from services.task_runner import tracker, _stats_cache
    from core.bot_name import get_bot_name

    uptime  = human_dur(int(time.time() - session.start_time))
    bot_name = get_bot_name().upper()

    # Use persistent session counters (accumulated by task_runner on completion).
    # tracker.all_tasks() only holds the last 60s of finished tasks — these
    # counters survive the eviction window and reflect the full session lifetime.
    bytes_ul = session.bytes_uploaded
    bytes_dl = session.bytes_downloaded
    files_ul = session.files_uploaded
    files_dl = session.files_downloaded

    # For average speeds, fall back to tracker for tasks still in window
    done = [t for t in tracker.all_tasks() if t.state.startswith("✅")]
    ul   = [t for t in done if t.mode == "ul"]
    dl   = [t for t in done if t.mode in ("dl", "magnet")]
    avg_ul = (sum(t.total or t.done for t in ul) / max(sum(t.elapsed for t in ul), 1)) if ul else 0
    avg_dl = (sum(t.total or t.done for t in dl) / max(sum(t.elapsed for t in dl), 1)) if dl else 0

    # System
    stats    = _stats_cache
    cpu      = float(stats.get("cpu", 0.0))
    ram_pct  = float(stats.get("ram_pct", 0.0))
    disk_free= int(stats.get("disk_free", 0))

    # CC jobs from store
    cc_active = cc_done = cc_error = 0
    try:
        from services.cc_job_store import cc_job_store
        all_jobs   = cc_job_store.all_jobs()
        cc_active  = sum(1 for j in all_jobs if j.status == "processing")
        cc_done    = sum(1 for j in all_jobs if j.status == "finished")
        cc_error   = sum(1 for j in all_jobs if j.status == "error")
    except Exception:
        pass

    lines = [
        f"📊 <b>{bot_name} — Usage Stats</b>",
        "──────────────────────────",
        "",
        f"⏱  <b>Uptime</b>       <code>{uptime}</code>",
        f"🕐  <b>Since</b>        <code>{datetime.fromtimestamp(session.start_time).strftime('%Y-%m-%d %H:%M')}</code>",
        "",
        "📤  <b>Uploaded</b>",
        f"    Files   <code>{files_ul}</code>",
        f"    Data    <code>{human_size(bytes_ul)}</code>",
        f"    Avg     <code>{human_size(avg_ul)}/s</code>",
        "",
        "📥  <b>Downloaded</b>",
        f"    Files   <code>{files_dl}</code>",
        f"    Data    <code>{human_size(bytes_dl)}</code>",
        f"    Avg     <code>{human_size(avg_dl)}/s</code>",
        "",
        "☁️  <b>CloudConvert</b>",
        f"    Active  <code>{cc_active}</code>",
        f"    Done    <code>{cc_done}</code>",
        f"    Failed  <code>{cc_error}</code>",
        "",
        "──────────────────────────",
        f"💻  <b>CPU</b>   <code>{cpu:.1f}%</code>",
        f"🧠  <b>RAM</b>   <code>{ram_pct:.1f}%</code>",
        f"💾  <b>Disk</b>  <code>{human_size(disk_free)} free</code>",
    ]
    return "\n".join(lines)


_USAGE_KB = InlineKeyboardMarkup([[
    InlineKeyboardButton("🔄 Refresh", callback_data="usage|refresh"),
    InlineKeyboardButton("✖ Close",   callback_data="usage|close"),
]])


@Client.on_message(filters.private & filters.command("usage"))
async def cmd_usage(client: Client, msg: Message):
    await msg.reply(_build_text(), parse_mode=enums.ParseMode.HTML, reply_markup=_USAGE_KB)


@Client.on_callback_query(filters.regex(r"^usage\|"))
async def usage_cb(client: Client, cb: CallbackQuery):
    action = cb.data.split("|")[1]
    await cb.answer()
    if action == "refresh":
        try:
            await cb.message.edit(_build_text(),
                                   parse_mode=enums.ParseMode.HTML,
                                   reply_markup=_USAGE_KB)
        except Exception as exc:
            if "MESSAGE_NOT_MODIFIED" not in str(exc):
                pass
    elif action == "close":
        await cb.message.delete()
