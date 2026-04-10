"""
plugins/nyaa_tracker.py
Nyaa anime tracker — v3.

CHANGES v3:
  • 10 results per page (was 5)
  • Added [☁️ Seedr+CC Compress] button per result
    → Seedr downloads, then CC converts at target resolution (no hardsub)
  • /nyaa_add now supports specific date+time scheduling:
    After selecting a day, user can pick [📅 Specific Date+Time]
    which asks for "DD-MM-YYYY HH:MM" — poller then checks every 5s
    starting from that exact moment until a match is found or 2h passes
  • Interactive setup with buttons (no pipe syntax)
  • All search open to allowed users; management owner-only
  • NO auto-seedr — everything button-driven, one file at a time
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users

log = logging.getLogger(__name__)

_DATA_DIR    = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data"))
_STORE_PATH  = os.path.join(_DATA_DIR, "nyaa_watchlist.json")
_CONFIG_PATH = os.path.join(_DATA_DIR, "nyaa_config.json")

RESULTS_PER_PAGE = 10
NUM_EMOJI = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

DAYS = ("monday","tuesday","wednesday","thursday","friday","saturday","sunday")
DAY_SHORT = {d: d[:3].capitalize() for d in DAYS}

COMMON_UPLOADERS = [
    "Erai-raws", "SubsPlease", "Tsundere-Raws",
    "EMBER", "ToonsHub", "DKB", "ANi",
]


# ═════════════════════════════════════════════════════════════
# Watchlist
# ═════════════════════════════════════════════════════════════

@dataclass
class WatchlistEntry:
    id:            int
    display_name:  str
    titles:        list[str] = field(default_factory=list)
    anilist_id:    int       = 0
    day:           str       = "daily"
    uploader:      str       = ""
    quality:       str       = "1080p"
    category:      str       = "1_2"
    active:        bool      = True
    seen_hashes:   list[str] = field(default_factory=list)
    last_check:    float     = 0.0
    last_match:    float     = 0.0
    added_at:      float     = field(default_factory=time.time)
    # ── Scheduled date+time check ─────────────────────────────
    scheduled_ts:  float     = 0.0   # unix timestamp for one-shot date+time check
    schedule_done: bool      = False


class WatchlistStore:
    def __init__(self):
        self._entries: dict[int, WatchlistEntry] = {}
        self._next_id: int = 1
        self._lock = asyncio.Lock()
        self._load()

    def _load(self):
        try:
            with open(_STORE_PATH, encoding="utf-8") as f:
                raw = json.load(f)
            for d in raw.get("entries", {}).values():
                try:
                    e = WatchlistEntry(**d)
                    self._entries[e.id] = e
                except TypeError:
                    pass
            self._next_id = raw.get("next_id", max(self._entries.keys(), default=0) + 1)
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warning("[NyaaTracker] Load: %s", e)

    def _save(self):
        try:
            os.makedirs(_DATA_DIR, exist_ok=True)
            with open(_STORE_PATH, "w", encoding="utf-8") as f:
                json.dump({
                    "entries": {str(e.id): asdict(e) for e in self._entries.values()},
                    "next_id": self._next_id,
                }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.warning("[NyaaTracker] Save: %s", e)

    async def add(self, entry: WatchlistEntry) -> int:
        async with self._lock:
            entry.id = self._next_id
            self._next_id += 1
            self._entries[entry.id] = entry
            self._save()
        return entry.id

    async def remove(self, eid: int) -> bool:
        async with self._lock:
            if eid in self._entries:
                del self._entries[eid]
                self._save()
                return True
        return False

    async def update(self, eid: int, **kw) -> bool:
        async with self._lock:
            e = self._entries.get(eid)
            if not e:
                return False
            for k, v in kw.items():
                if hasattr(e, k):
                    setattr(e, k, v)
            self._save()
            return True

    async def mark_seen(self, eid: int, info_hash: str):
        async with self._lock:
            e = self._entries.get(eid)
            if e and info_hash and info_hash not in e.seen_hashes:
                e.seen_hashes.append(info_hash)
                if len(e.seen_hashes) > 300:
                    e.seen_hashes = e.seen_hashes[-300:]
                e.last_match = time.time()
                self._save()

    def get(self, eid: int) -> Optional[WatchlistEntry]:
        return self._entries.get(eid)

    def all_entries(self) -> list[WatchlistEntry]:
        return sorted(self._entries.values(), key=lambda e: e.id)

    def entries_for_day(self, day: str) -> list[WatchlistEntry]:
        day = day.lower()
        return [
            e for e in self._entries.values()
            if e.active and (e.day == day or e.day == "daily")
        ]

    def scheduled_entries(self) -> list[WatchlistEntry]:
        """Entries with a future scheduled_ts that haven't been checked yet."""
        now = time.time()
        return [
            e for e in self._entries.values()
            if e.active and e.scheduled_ts > 0 and not e.schedule_done
            and now >= e.scheduled_ts
        ]


watchlist = WatchlistStore()


# ─────────────────────────────────────────────────────────────
# Config + caches
# ─────────────────────────────────────────────────────────────

_config: dict = {"dump_channel": 0, "poll_interval": 600}


def _load_config():
    global _config
    try:
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            _config.update(json.load(f))
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("[NyaaTracker] Config: %s", e)


def _save_config():
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(_config, f, indent=2)
    except Exception as e:
        log.warning("[NyaaTracker] Config save: %s", e)


_load_config()

_search_cache: dict[str, dict] = {}
_CACHE_TTL = 1800
_magnet_cache: dict[str, str] = {}


def _cache_key(query: str) -> str:
    return hashlib.md5(f"{query}_{time.time():.0f}".encode()).hexdigest()[:8]


def _cache_put(key: str, results: list, query: str):
    now = time.time()
    for k in [k for k, v in _search_cache.items() if now - v["ts"] > _CACHE_TTL]:
        _search_cache.pop(k, None)
    _search_cache[key] = {"results": results, "query": query, "ts": now}


def _cache_get(key: str) -> Optional[dict]:
    entry = _search_cache.get(key)
    if entry and time.time() - entry["ts"] < _CACHE_TTL:
        return entry
    _search_cache.pop(key, None)
    return None


# ─────────────────────────────────────────────────────────────
# Setup state
# ─────────────────────────────────────────────────────────────

_setup: dict[str, dict] = {}
_schedule_waiting: dict[int, str] = {}  # uid → sid (waiting for date+time text)


def _sid() -> str:
    return hashlib.md5(str(time.time()).encode()).hexdigest()[:6]


def _short_date(pub: str) -> str:
    try:
        p = pub.split()
        return f"{p[2]} {p[1]} {p[4][:5]}"
    except Exception:
        return pub[:16] if pub else ""


# ═════════════════════════════════════════════════════════════
# Search rendering (10 per page)
# ═════════════════════════════════════════════════════════════

def _render_page(key: str, page: int, query: str, results: list):
    total = len(results)
    pages = max(1, (total + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE)
    page  = max(0, min(page, pages - 1))
    start = page * RESULTS_PER_PAGE
    chunk = results[start:start + RESULTS_PER_PAGE]

    lines = [
        f"📡 <b>Nyaa</b> — <code>{query[:30]}</code>",
        f"Page <b>{page+1}/{pages}</b> · {total} results",
        "",
    ]
    for i, r in enumerate(chunk):
        n = NUM_EMOJI[i] if i < len(NUM_EMOJI) else f"{i+1}."
        t = r.title[:58] + "…" if len(r.title) > 58 else r.title
        lines.append(f"{n} <code>{t}</code>")
        lines.append(f"  💾{r.size} 🌱{r.seeders} 📅{_short_date(r.pub_date)}")

    # Number buttons
    nums = [
        InlineKeyboardButton(NUM_EMOJI[i] if i < len(NUM_EMOJI) else str(i+1),
                             callback_data=f"nys|a|{key}|{start+i}")
        for i in range(len(chunk))
    ]
    # Split into 2 rows of 5
    rows = [nums[:5]]
    if len(nums) > 5:
        rows.append(nums[5:])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"nys|p|{key}|{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="nys|noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"nys|p|{key}|{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("❌ Close", callback_data="nys|x")])

    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _render_detail(key: str, idx: int, r, page: int):
    t = r.title[:65] + "…" if len(r.title) > 65 else r.title
    lines = [
        "📦 <b>Result Detail</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"<code>{t}</code>", "",
        f"💾 {r.size}  🌱 {r.seeders}  📥 {r.downloads}",
        f"📅 {_short_date(r.pub_date)}  👤 {r.uploader or '—'}",
        f"🔗 {r.link}",
    ]
    rows = [
        [InlineKeyboardButton("🧲 Magnet",       callback_data=f"nys|m|{key}|{idx}"),
         InlineKeyboardButton("📥 .torrent",     callback_data=f"nys|t|{key}|{idx}")],
        [InlineKeyboardButton("☁️ Seedr+HS",     callback_data=f"nys|sr|{key}|{idx}"),
         InlineKeyboardButton("📥 Local DL",     callback_data=f"nys|dl|{key}|{idx}")],
        [InlineKeyboardButton("☁️ Seedr+CC 🗜️",  callback_data=f"nys|sc|{key}|{idx}")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"nys|p|{key}|{page}")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# ═════════════════════════════════════════════════════════════
# /nyaa_search — open to ALL allowed users
# ═════════════════════════════════════════════════════════════

@Client.on_message(filters.private & filters.command("nyaa_search"))
async def cmd_nyaa_search(client: Client, msg: Message):
    query = " ".join(msg.command[1:])
    if not query:
        return await msg.reply(
            "📡 <b>Nyaa Search</b>\n\n"
            "<code>/nyaa_search Kujima Utaeba le Hororo 1080p</code>",
            parse_mode=enums.ParseMode.HTML)

    st = await msg.reply(f"🔍 Searching: <code>{query[:40]}</code>…",
                         parse_mode=enums.ParseMode.HTML)

    from services.nyaa import search_nyaa
    results = await search_nyaa(query, category="1_0")

    if not results:
        return await st.edit(f"❌ No results for: <code>{query}</code>",
                             parse_mode=enums.ParseMode.HTML)

    key = _cache_key(query)
    _cache_put(key, results, query)
    for i, r in enumerate(results):
        if r.magnet:
            _magnet_cache[f"{key}_{i}"] = r.magnet

    text, kb = _render_page(key, 0, query, results)
    await st.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)


# ─────────────────────────────────────────────────────────────
# Search callbacks
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nys\|"))
async def nys_cb(client: Client, cb: CallbackQuery):
    parts  = cb.data.split("|")
    action = parts[1] if len(parts) > 1 else ""

    if action == "noop":
        return await cb.answer()
    if action == "x":
        await cb.answer()
        return await cb.message.delete()
    if len(parts) < 4:
        return await cb.answer("Invalid.", show_alert=True)

    key, param = parts[2], parts[3]
    uid = cb.from_user.id

    cached = _cache_get(key)
    if not cached:
        return await cb.answer("Expired. Run /nyaa_search again.", show_alert=True)
    results, query = cached["results"], cached["query"]
    await cb.answer()

    if action == "p":
        text, kb = _render_page(key, int(param), query, results)
        try:
            await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass
        return

    if action == "a":
        idx = int(param)
        if idx >= len(results):
            return
        text, kb = _render_detail(key, idx, results[idx], idx // RESULTS_PER_PAGE)
        await cb.message.edit(text, parse_mode=enums.ParseMode.HTML,
                              reply_markup=kb, disable_web_page_preview=True)
        return

    # All action handlers below need a result
    idx = int(param)
    if idx >= len(results):
        return
    r = results[idx]
    magnet = r.magnet or _magnet_cache.get(f"{key}_{idx}", "")

    if action == "m":
        if not magnet:
            return await client.send_message(uid, "❌ No magnet available.")
        await client.send_message(uid, f"🧲 <b>Magnet</b>\n\n<code>{magnet}</code>",
                                  parse_mode=enums.ParseMode.HTML)
    elif action == "t":
        if r.torrent_url:
            await client.send_message(uid, f"📥 <b>Torrent</b>\n\n<code>{r.torrent_url}</code>",
                                      parse_mode=enums.ParseMode.HTML)
        else:
            await client.send_message(uid, "❌ No torrent URL.")
    elif action == "sr":
        if not magnet:
            return await client.send_message(uid, "❌ No magnet.")
        from plugins.url_handler import _seedr_download
        st = await client.send_message(uid, "☁️ <b>Seedr</b>\n⬆️ Submitting…",
                                       parse_mode=enums.ParseMode.HTML)
        asyncio.create_task(_seedr_download(client, st, magnet, uid))
    elif action == "dl":
        if not magnet:
            return await client.send_message(uid, "❌ No magnet.")
        from plugins.url_handler import _launch_download
        st = await client.send_message(uid, "📥 <b>Downloading…</b>",
                                       parse_mode=enums.ParseMode.HTML)
        asyncio.create_task(_launch_download(client, st, magnet, uid))
    elif action == "sc":
        # Seedr + CloudConvert Compress (no hardsub, just resolution convert)
        if not magnet:
            return await client.send_message(uid, "❌ No magnet.")
        api_key = os.environ.get("CC_API_KEY", "").strip()
        if not api_key:
            return await client.send_message(
                uid, "❌ CC_API_KEY not set.", parse_mode=enums.ParseMode.HTML)
        # Ask for target resolution
        h12 = hashlib.md5(magnet.encode()).hexdigest()[:12]
        _magnet_cache[h12] = magnet
        await client.send_message(
            uid,
            f"☁️ <b>Seedr + CC Compress</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <code>{r.title[:50]}</code>\n\n"
            f"Choose target resolution:",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔵 1080p", callback_data=f"nyc|1080|{h12}"),
                 InlineKeyboardButton("🟢 720p",  callback_data=f"nyc|720|{h12}")],
                [InlineKeyboardButton("🟡 480p",  callback_data=f"nyc|480|{h12}"),
                 InlineKeyboardButton("🎬 Original", callback_data=f"nyc|0|{h12}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="nyc|x|0")],
            ]),
        )


# ─────────────────────────────────────────────────────────────
# Seedr+CC Compress callback   nyc|<height>|<hash12>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nyc\|"))
async def nyc_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid.", show_alert=True)
    height_s, h12 = parts[1], parts[2]
    uid = cb.from_user.id
    await cb.answer()

    if height_s == "x":
        return await cb.message.delete()

    magnet = _magnet_cache.get(h12, "")
    if not magnet:
        return await cb.message.edit("❌ Magnet expired.", parse_mode=enums.ParseMode.HTML)

    height = int(height_s) if height_s.isdigit() else 0
    res_label = f"{height}p" if height else "Original"

    asyncio.create_task(
        _seedr_then_cc_compress(client, cb.message, magnet, uid, height, res_label)
    )


async def _seedr_then_cc_compress(client, st, magnet, uid, height, res_label):
    """Seedr download → CC convert (resolution compress, no hardsub)."""
    from services.seedr import download_via_seedr
    from services.utils import make_tmp, cleanup, human_size, largest_file
    from services.cloudconvert_api import submit_convert
    from services.cc_job_store import cc_job_store, CCJob
    from services.cc_sanitize import sanitize_for_cc

    tmp = make_tmp(cfg.download_dir, uid)

    try:
        await st.edit(
            f"☁️ <b>Seedr+CC Compress → {res_label}</b>\n"
            "⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        paths = await download_via_seedr(magnet, tmp, timeout_s=7200)
        if not paths:
            cleanup(tmp)
            return await st.edit("❌ Seedr: no files.", parse_mode=enums.ParseMode.HTML)

        video_path = max(paths, key=lambda p: os.path.getsize(p))
        fname = os.path.basename(video_path)

        await st.edit(
            f"☁️ <b>Seedr done — submitting to CC</b>\n"
            f"📦 <code>{fname[:40]}</code>\n"
            f"📐 → <b>{res_label}</b>\n"
            "⬆️ <i>Uploading to CloudConvert…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        api_key = os.environ.get("CC_API_KEY", "").strip()
        out_name = sanitize_for_cc(os.path.splitext(fname)[0] + f"_{res_label}.mp4")

        job_id = await submit_convert(
            api_key, video_path=video_path,
            output_name=out_name, scale_height=height,
        )

        await cc_job_store.add(CCJob(
            job_id=job_id, uid=uid,
            fname=fname, output_name=out_name,
            status="processing",
        ))

        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception:
            pass

        await st.edit(
            f"✅ <b>CC Compress submitted</b>\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📦 <code>{fname[:35]}</code> → <b>{res_label}</b>\n\n"
            "⏳ <i>Processing… auto-uploads when done.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        log.error("[Seedr+CC] %s", exc, exc_info=True)
        await st.edit(f"❌ <b>Failed</b>\n<code>{str(exc)[:200]}</code>",
                      parse_mode=enums.ParseMode.HTML)
    finally:
        cleanup(tmp)


# ═════════════════════════════════════════════════════════════
# /nyaa_add — Interactive setup with date+time scheduling
# ═════════════════════════════════════════════════════════════

@Client.on_message(filters.command("nyaa_add") & filters.user(cfg.owner_id))
async def cmd_nyaa_add(client: Client, msg: Message):
    title = " ".join(msg.command[1:])
    if not title:
        return await msg.reply(
            "📡 <b>Nyaa Tracker — Add</b>\n\n"
            "<code>/nyaa_add Oshi no Ko</code>\n\n"
            "Buttons guide you through day/uploader/quality.",
            parse_mode=enums.ParseMode.HTML)

    st = await msg.reply(f"🔍 Resolving: <code>{title}</code>…",
                         parse_mode=enums.ParseMode.HTML)

    from services.anilist import search_anime, all_titles
    all_t = [title]
    aid = 0
    try:
        res = await search_anime(title)
        if res:
            aid = res[0].get("id", 0)
            for t in all_titles(res[0]):
                if t.lower() not in {s.lower() for s in all_t}:
                    all_t.append(t)
    except Exception as exc:
        log.warning("[NyaaTracker] AniList: %s", exc)

    sid = _sid()
    _setup[sid] = {
        "uid": msg.from_user.id, "title": title, "titles": all_t,
        "anilist_id": aid, "day": None, "uploader": None,
        "quality": None, "scheduled_ts": 0, "step": "day",
    }

    preview = "\n".join(f"  • <code>{t}</code>" for t in all_t[:5])
    extra = f"\n  <i>+{len(all_t)-5} more</i>" if len(all_t) > 5 else ""

    day_rows = [
        [InlineKeyboardButton(DAY_SHORT[d], callback_data=f"nya|d|{sid}|{i}")
         for i, d in enumerate(DAYS) if i < 4],
        [InlineKeyboardButton(DAY_SHORT[d], callback_data=f"nya|d|{sid}|{i}")
         for i, d in enumerate(DAYS) if i >= 4],
        [InlineKeyboardButton("📅 Daily", callback_data=f"nya|d|{sid}|7")],
        [InlineKeyboardButton("🎯 Specific Date+Time", callback_data=f"nya|dt|{sid}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")],
    ]

    await st.edit(
        f"✅ <b>{len(all_t)} title(s) resolved</b>\n\n{preview}{extra}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>Step 1/3 — Schedule:</b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(day_rows),
    )


# ─────────────────────────────────────────────────────────────
# Setup callbacks
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nya\|"))
async def nya_cb(client: Client, cb: CallbackQuery):
    parts  = cb.data.split("|")
    action = parts[1]
    sid    = parts[2] if len(parts) > 2 else ""
    state  = _setup.get(sid)
    if not state:
        return await cb.answer("Expired. /nyaa_add again.", show_alert=True)
    await cb.answer()

    if action == "x":
        _setup.pop(sid, None)
        return await cb.message.delete()

    # ── Day selection ─────────────────────────────────────────
    if action == "d":
        idx = int(parts[3]) if len(parts) > 3 else 7
        state["day"] = DAYS[idx] if idx < 7 else "daily"
        state["step"] = "uploader"
        await _show_uploader_step(cb.message, sid, state)
        return

    # ── Date+Time input request ───────────────────────────────
    if action == "dt":
        state["step"] = "waiting_datetime"
        uid = cb.from_user.id
        _schedule_waiting[uid] = sid
        await cb.message.edit(
            "🎯 <b>Specific Date+Time</b>\n\n"
            "Send the date and time to start checking:\n\n"
            "<code>DD-MM-YYYY HH:MM</code>\n\n"
            "Example: <code>12-04-2026 12:00</code>\n\n"
            "<i>The bot will poll Nyaa every 5 seconds\n"
            "starting from that exact moment.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    # ── Uploader selection ────────────────────────────────────
    if action == "u":
        idx = int(parts[3]) if len(parts) > 3 else 99
        state["uploader"] = COMMON_UPLOADERS[idx] if idx < len(COMMON_UPLOADERS) else ""
        state["step"] = "quality"
        await _show_quality_step(cb.message, sid, state)
        return

    # ── Quality → save ────────────────────────────────────────
    if action == "q":
        quality = parts[3] if len(parts) > 3 else ""
        state["quality"] = quality
        entry = WatchlistEntry(
            id=0, display_name=state["title"], titles=state["titles"],
            anilist_id=state["anilist_id"], day=state.get("day") or "daily",
            uploader=state.get("uploader") or "", quality=quality,
            scheduled_ts=state.get("scheduled_ts", 0),
        )
        eid = await watchlist.add(entry)
        _setup.pop(sid, None)
        _ensure_poller()

        sched = ""
        if entry.scheduled_ts:
            import datetime as dt
            ts_str = dt.datetime.fromtimestamp(entry.scheduled_ts).strftime("%d-%m-%Y %H:%M")
            sched = f"\n🎯 Scheduled: <code>{ts_str}</code> (polls every 5s)"

        await cb.message.edit(
            f"✅ <b>Added #{eid}</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 <b>{entry.display_name}</b>\n"
            f"📅 {entry.day.capitalize()}  👤 {entry.uploader or 'Any'}  📐 {quality or 'Any'}\n"
            f"🔑 {len(entry.titles)} aliases{sched}",
            parse_mode=enums.ParseMode.HTML,
        )
        return


async def _show_uploader_step(msg, sid, state):
    rows = []
    row = []
    for i, u in enumerate(COMMON_UPLOADERS):
        row.append(InlineKeyboardButton(u, callback_data=f"nya|u|{sid}|{i}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔓 Any", callback_data=f"nya|u|{sid}|99")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")])

    day_s = state["day"].capitalize() if state.get("day") != "daily" else "Daily"
    await msg.edit(
        f"📅 {day_s} ✅\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>Step 2/3 — Uploader:</b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _show_quality_step(msg, sid, state):
    rows = [
        [InlineKeyboardButton("🔵 1080p", callback_data=f"nya|q|{sid}|1080p"),
         InlineKeyboardButton("🟢 720p",  callback_data=f"nya|q|{sid}|720p")],
        [InlineKeyboardButton("🟡 480p",  callback_data=f"nya|q|{sid}|480p"),
         InlineKeyboardButton("🔓 Any",   callback_data=f"nya|q|{sid}|")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")],
    ]
    day_s = state.get("day", "daily").capitalize()
    up_s  = state.get("uploader") or "Any"
    await msg.edit(
        f"📅 {day_s} ✅  👤 {up_s} ✅\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>Step 3/3 — Quality:</b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


# ─────────────────────────────────────────────────────────────
# Date+time text receiver
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.text
    & ~filters.command([
        "start","help","settings","info","status","log","restart","broadcast",
        "admin","ban_user","unban_user","banned_list","cancel",
        "show_thumb","del_thumb","json_formatter","bulk_url",
        "hardsub","botname","ccstatus","convert","resize","compress",
        "usage","captiontemplate","stream","forward",
        "createarchive","archiveddone","mergedone",
        "nyaa_add","nyaa_list","nyaa_remove","nyaa_check",
        "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",
    ]),
    group=0,
)
async def datetime_receiver(client: Client, msg: Message):
    uid = msg.from_user.id
    sid = _schedule_waiting.get(uid)
    if not sid:
        return
    state = _setup.get(sid)
    if not state or state.get("step") != "waiting_datetime":
        _schedule_waiting.pop(uid, None)
        return

    text = msg.text.strip()
    # Parse DD-MM-YYYY HH:MM
    import datetime as dt
    try:
        parsed = dt.datetime.strptime(text, "%d-%m-%Y %H:%M")
        ts = parsed.timestamp()
        if ts < time.time():
            await msg.reply("❌ That's in the past. Send a future date.",
                            parse_mode=enums.ParseMode.HTML)
            return
    except ValueError:
        await msg.reply(
            "❌ Invalid format. Use: <code>DD-MM-YYYY HH:MM</code>\n"
            "Example: <code>12-04-2026 12:00</code>",
            parse_mode=enums.ParseMode.HTML)
        return

    _schedule_waiting.pop(uid, None)
    state["scheduled_ts"] = ts
    state["day"] = "daily"  # daily ensures normal poller also picks it up
    state["step"] = "uploader"

    ts_display = parsed.strftime("%d-%m-%Y %H:%M")
    st = await msg.reply(
        f"🎯 Scheduled: <code>{ts_display}</code> ✅\n"
        f"<i>Will poll every 5s starting at that time.</i>",
        parse_mode=enums.ParseMode.HTML,
    )
    await _show_uploader_step(st, sid, state)
    msg.stop_propagation()


# ═════════════════════════════════════════════════════════════
# Management commands
# ═════════════════════════════════════════════════════════════

@Client.on_message(filters.command("nyaa_list") & filters.user(cfg.owner_id))
async def cmd_nyaa_list(client: Client, msg: Message):
    entries = watchlist.all_entries()
    if not entries:
        return await msg.reply("📡 <b>Empty</b> — /nyaa_add to start.",
                               parse_mode=enums.ParseMode.HTML)
    lines = ["📡 <b>Nyaa Watchlist</b>", "━━━━━━━━━━━━━━━━━━━━━━━━", ""]
    for e in entries:
        ico = "🟢" if e.active else "🔴"
        day = e.day.capitalize() if e.day != "daily" else "Daily"
        up  = f"[{e.uploader}]" if e.uploader else ""
        sched = ""
        if e.scheduled_ts and not e.schedule_done:
            import datetime as dt
            sched = f"\n   🎯 {dt.datetime.fromtimestamp(e.scheduled_ts).strftime('%d-%m %H:%M')}"
        lines.append(
            f"{ico} <b>#{e.id}</b> <code>{e.display_name[:25]}</code>\n"
            f"   📅{day} 📐{e.quality or 'Any'} {up}"
            f"\n   🔑{len(e.titles)} · 📦{len(e.seen_hashes)} seen{sched}"
        )
        lines.append("")
    await msg.reply("\n".join(lines)[:4000], parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_remove") & filters.user(cfg.owner_id))
async def cmd_nyaa_remove(client: Client, msg: Message):
    args = msg.command[1:]
    if not args or not args[0].isdigit():
        return await msg.reply("Usage: <code>/nyaa_remove &lt;id&gt;</code>",
                               parse_mode=enums.ParseMode.HTML)
    eid = int(args[0])
    e = watchlist.get(eid)
    if not e:
        return await msg.reply(f"❌ #{eid} not found.")
    await watchlist.remove(eid)
    await msg.reply(f"✅ Removed #{eid} — {e.display_name}", parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_toggle") & filters.user(cfg.owner_id))
async def cmd_nyaa_toggle(client: Client, msg: Message):
    args = msg.command[1:]
    if not args or not args[0].isdigit():
        return await msg.reply("Usage: <code>/nyaa_toggle &lt;id&gt;</code>",
                               parse_mode=enums.ParseMode.HTML)
    eid = int(args[0])
    e = watchlist.get(eid)
    if not e:
        return await msg.reply(f"❌ #{eid} not found.")
    new = not e.active
    await watchlist.update(eid, active=new)
    await msg.reply(f"{'🟢' if new else '🔴'} #{eid} {'Enabled' if new else 'Disabled'}",
                    parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_dump") & filters.user(cfg.owner_id))
async def cmd_nyaa_dump(client: Client, msg: Message):
    args = msg.command[1:]
    if not args:
        return await msg.reply(f"📢 Dump: <code>{_config.get('dump_channel', 0) or 'Not set'}</code>",
                               parse_mode=enums.ParseMode.HTML)
    target = args[0]
    if target == "0":
        _config["dump_channel"] = 0
        _save_config()
        return await msg.reply("📢 Disabled.")
    try:
        ch_id = int(target) if target.lstrip("-").isdigit() else (await client.get_chat(target)).id
        _config["dump_channel"] = ch_id
        _save_config()
        await msg.reply(f"✅ Dump: <code>{ch_id}</code>", parse_mode=enums.ParseMode.HTML)
    except Exception as exc:
        await msg.reply(f"❌ {exc}", parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_check") & filters.user(cfg.owner_id))
async def cmd_nyaa_check(client: Client, msg: Message):
    entries = [e for e in watchlist.all_entries() if e.active]
    if not entries:
        return await msg.reply("📡 No active entries.")
    st = await msg.reply(f"🔍 Checking {len(entries)}…", parse_mode=enums.ParseMode.HTML)
    _ensure_poller()
    found = 0
    for i, e in enumerate(entries, 1):
        try:
            n = await _check_entry(e)
            found += n
        except Exception as exc:
            log.warning("[Check] %s: %s", e.display_name, exc)
        await asyncio.sleep(2)
    await st.edit(f"✅ {len(entries)} checked, {found} new", parse_mode=enums.ParseMode.HTML)


# ═════════════════════════════════════════════════════════════
# Poller — normal + scheduled date+time (5s)
# ═════════════════════════════════════════════════════════════

_poller_task: Optional[asyncio.Task] = None


def _ensure_poller():
    global _poller_task
    if _poller_task and not _poller_task.done():
        return
    _poller_task = asyncio.create_task(_poll_loop())


async def _poll_loop():
    await asyncio.sleep(15)
    while True:
        try:
            # ── Normal day-based polling ──────────────────────
            import datetime as dt
            today = dt.datetime.now().strftime("%A").lower()
            for e in watchlist.entries_for_day(today):
                try:
                    await _check_entry(e)
                except Exception as exc:
                    log.error("[Poll] %s: %s", e.display_name, exc)
                await asyncio.sleep(3)

            # ── Scheduled date+time entries (5s precision) ────
            scheduled = watchlist.scheduled_entries()
            for e in scheduled:
                log.info("[NyaaTracker] Scheduled check triggered for '%s'", e.display_name)
                found = await _rapid_check(e, duration=7200, interval=5)
                if found:
                    log.info("[NyaaTracker] Scheduled match found for '%s'", e.display_name)
                await watchlist.update(e.id, schedule_done=True)

        except Exception as exc:
            log.error("[Poll] %s", exc, exc_info=True)

        await asyncio.sleep(_config.get("poll_interval", 600))


async def _rapid_check(entry: WatchlistEntry, duration: int = 7200, interval: int = 5) -> bool:
    """Poll every `interval` seconds for up to `duration` seconds until a match is found."""
    from core.session import get_client
    client = get_client()

    deadline = time.time() + duration
    attempt = 0

    try:
        await client.send_message(
            cfg.owner_id,
            f"🎯 <b>Scheduled check started</b>\n\n"
            f"📺 <b>{entry.display_name}</b>\n"
            f"⏱ Polling every {interval}s for up to {duration//60} min\n"
            f"<i>I'll notify you when a match is found.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception:
        pass

    while time.time() < deadline:
        attempt += 1
        try:
            n = await _check_entry(entry)
            if n > 0:
                return True
        except Exception as exc:
            log.warning("[RapidCheck] %s attempt %d: %s", entry.display_name, attempt, exc)
        await asyncio.sleep(interval)

    try:
        await client.send_message(
            cfg.owner_id,
            f"⏰ <b>Scheduled check expired</b>\n\n"
            f"📺 {entry.display_name}\n"
            f"Checked {attempt} times over {duration//60} min — no new match.",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception:
        pass
    return False


async def _check_entry(entry: WatchlistEntry) -> int:
    from services.nyaa import search_nyaa, match_title, extract_episode

    terms = [entry.display_name]
    for t in entry.titles:
        if t.lower().strip() not in [s.lower().strip() for s in terms]:
            terms.append(t)
            if len(terms) >= 3:
                break

    all_r = []
    seen_h: set = set()
    for term in terms:
        for r in await search_nyaa(term, category=entry.category):
            if r.info_hash and r.info_hash not in seen_h:
                seen_h.add(r.info_hash)
                all_r.append(r)
        await asyncio.sleep(1)

    await watchlist.update(entry.id, last_check=time.time())

    matched = [
        r for r in all_r
        if r.info_hash not in entry.seen_hashes
        and match_title(r.title, entry.titles, entry.uploader, entry.quality)
    ]
    if not matched:
        return 0

    from core.session import get_client
    client  = get_client()
    dump_ch = _config.get("dump_channel", 0)

    for r in matched:
        ep = extract_episode(r.title)
        ep_s = f"Ep {ep}" if ep else "Batch"
        h12 = r.info_hash[:12] if r.info_hash else hashlib.md5(r.title.encode()).hexdigest()[:12]
        if r.magnet:
            _magnet_cache[h12] = r.magnet

        text = (
            f"🔔 <b>Nyaa Match</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 <b>{entry.display_name}</b> ({ep_s})\n"
            f"📦 <code>{r.title[:65]}</code>\n\n"
            f"💾{r.size} 🌱{r.seeders} 📅{_short_date(r.pub_date)} 👤{r.uploader or '—'}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("☁️ Seedr+HS", callback_data=f"nyt|sr|{entry.id}|{h12}"),
             InlineKeyboardButton("📥 DL",       callback_data=f"nyt|dl|{entry.id}|{h12}")],
            [InlineKeyboardButton("☁️ Seedr+CC🗜️", callback_data=f"nyt|sc|{entry.id}|{h12}"),
             InlineKeyboardButton("🧲 Magnet",   callback_data=f"nyt|mg|{entry.id}|{h12}")],
            [InlineKeyboardButton("❌ Skip",     callback_data=f"nyt|sk|{entry.id}|{h12}")],
        ])

        if dump_ch:
            try:
                await client.send_message(dump_ch, text, parse_mode=enums.ParseMode.HTML,
                                          reply_markup=kb, disable_web_page_preview=True)
            except Exception as exc:
                log.warning("[Dump] %s", exc)
        try:
            await client.send_message(cfg.owner_id, text, parse_mode=enums.ParseMode.HTML,
                                      reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            pass
        await watchlist.mark_seen(entry.id, r.info_hash)

    return len(matched)


# ─────────────────────────────────────────────────────────────
# Poller match callbacks
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nyt\|"))
async def nyt_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 4:
        return await cb.answer("Invalid.", show_alert=True)
    action, h12 = parts[1], parts[3]
    uid = cb.from_user.id
    await cb.answer()

    if action == "sk":
        try:
            await cb.message.edit(cb.message.text + "\n\n❌ <b>Skipped</b>",
                                  parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass
        return

    magnet = _magnet_cache.get(h12, "")

    if action == "mg":
        if not magnet:
            return await client.send_message(uid, "❌ Magnet expired.")
        await client.send_message(uid, f"🧲\n\n<code>{magnet}</code>",
                                  parse_mode=enums.ParseMode.HTML)
        return

    if not magnet:
        return await client.send_message(uid, "❌ Magnet expired.")

    if action == "sr":
        from plugins.url_handler import _seedr_download
        st = await client.send_message(uid, "☁️ Seedr…", parse_mode=enums.ParseMode.HTML)
        asyncio.create_task(_seedr_download(client, st, magnet, uid))
        try:
            await cb.message.edit(cb.message.text + "\n\n☁️ <b>→ Seedr</b>",
                                  parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass

    elif action == "dl":
        from plugins.url_handler import _launch_download
        st = await client.send_message(uid, "📥 Downloading…", parse_mode=enums.ParseMode.HTML)
        asyncio.create_task(_launch_download(client, st, magnet, uid))
        try:
            await cb.message.edit(cb.message.text + "\n\n📥 <b>DL started</b>",
                                  parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass

    elif action == "sc":
        api_key = os.environ.get("CC_API_KEY", "").strip()
        if not api_key:
            return await client.send_message(uid, "❌ CC_API_KEY not set.")
        _magnet_cache[h12] = magnet
        await client.send_message(
            uid,
            "☁️ <b>Seedr+CC Compress</b>\nChoose resolution:",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔵 1080p", callback_data=f"nyc|1080|{h12}"),
                 InlineKeyboardButton("🟢 720p",  callback_data=f"nyc|720|{h12}")],
                [InlineKeyboardButton("🟡 480p",  callback_data=f"nyc|480|{h12}"),
                 InlineKeyboardButton("🎬 Original", callback_data=f"nyc|0|{h12}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="nyc|x|0")],
            ]),
        )


# ═════════════════════════════════════════════════════════════
# Auto-start
# ═════════════════════════════════════════════════════════════

def start_nyaa_poller():
    if any(e.active for e in watchlist.all_entries()):
        _ensure_poller()
