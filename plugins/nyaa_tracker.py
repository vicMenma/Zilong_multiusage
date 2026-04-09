"""
plugins/nyaa_tracker.py
Nyaa anime tracker — paginated search, interactive add, button-driven workflow.

═══════════════════════════════════════════════════════════════════
COMMANDS:
  /nyaa_search <query>  — paginated search with magnet/torrent/seedr buttons
  /nyaa_add <title>     — interactive setup (day → uploader → quality)
  /nyaa_list            — show tracked anime
  /nyaa_remove <id>     — remove entry
  /nyaa_check           — manual poll now
  /nyaa_dump <channel>  — set dump channel
  /nyaa_toggle <id>     — enable/disable entry

DESIGN:
  • NO auto-seedr — everything is button-driven, one file at a time
  • Paginated search (5/page) with [🧲 Magnet] [☁️ Seedr] [📥 DL] per result
  • Interactive /nyaa_add: title → AniList resolve → day → uploader → quality
  • Poller: finds matches → sends to dump channel + owner with action buttons
  • /nyaa_search open to all allowed users; management commands owner-only
═══════════════════════════════════════════════════════════════════
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

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────

_DATA_DIR    = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data"))
_STORE_PATH  = os.path.join(_DATA_DIR, "nyaa_watchlist.json")
_CONFIG_PATH = os.path.join(_DATA_DIR, "nyaa_config.json")

RESULTS_PER_PAGE = 5
NUM_EMOJI = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣"]

DAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
DAY_SHORT = {"monday": "Mon", "tuesday": "Tue", "wednesday": "Wed",
             "thursday": "Thu", "friday": "Fri", "saturday": "Sat", "sunday": "Sun"}

COMMON_UPLOADERS = [
    "Erai-raws", "SubsPlease", "Tsundere-Raws",
    "EMBER", "ToonsHub", "DKB",
]


# ─────────────────────────────────────────────────────────────
# Watchlist store
# ─────────────────────────────────────────────────────────────

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
            log.warning("[NyaaTracker] Load error: %s", e)

    def _save(self):
        try:
            os.makedirs(_DATA_DIR, exist_ok=True)
            with open(_STORE_PATH, "w", encoding="utf-8") as f:
                json.dump({
                    "entries": {str(e.id): asdict(e) for e in self._entries.values()},
                    "next_id": self._next_id,
                }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.warning("[NyaaTracker] Save error: %s", e)

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

    async def mark_seen(self, eid: int, info_hash: str) -> None:
        async with self._lock:
            e = self._entries.get(eid)
            if e and info_hash and info_hash not in e.seen_hashes:
                e.seen_hashes.append(info_hash)
                if len(e.seen_hashes) > 200:
                    e.seen_hashes = e.seen_hashes[-200:]
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


watchlist = WatchlistStore()


# ─────────────────────────────────────────────────────────────
# Config
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
        log.warning("[NyaaTracker] Config load: %s", e)


def _save_config():
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(_config, f, indent=2)
    except Exception as e:
        log.warning("[NyaaTracker] Config save: %s", e)


_load_config()


# ─────────────────────────────────────────────────────────────
# Search results cache (for pagination)
# ─────────────────────────────────────────────────────────────

_search_cache: dict[str, dict] = {}  # key → {results, query, ts}
_CACHE_TTL = 1800  # 30 min


def _cache_key(query: str) -> str:
    return hashlib.md5(f"{query}_{time.time():.0f}".encode()).hexdigest()[:8]


def _cache_put(key: str, results: list, query: str) -> None:
    # Evict old entries
    now = time.time()
    dead = [k for k, v in _search_cache.items() if now - v["ts"] > _CACHE_TTL]
    for k in dead:
        _search_cache.pop(k, None)
    _search_cache[key] = {"results": results, "query": query, "ts": now}


def _cache_get(key: str) -> Optional[dict]:
    entry = _search_cache.get(key)
    if entry and time.time() - entry["ts"] < _CACHE_TTL:
        return entry
    _search_cache.pop(key, None)
    return None


# ─────────────────────────────────────────────────────────────
# Magnet cache for callbacks
# ─────────────────────────────────────────────────────────────

_magnet_cache: dict[str, str] = {}  # hash_short → magnet


# ─────────────────────────────────────────────────────────────
# Interactive /nyaa_add setup state
# ─────────────────────────────────────────────────────────────

_setup: dict[str, dict] = {}  # setup_id → {uid, title, titles, anilist_id, step, ...}


def _setup_id() -> str:
    return hashlib.md5(str(time.time()).encode()).hexdigest()[:6]


# ─────────────────────────────────────────────────────────────
# Render helpers
# ─────────────────────────────────────────────────────────────

def _short_date(pub_date: str) -> str:
    try:
        parts = pub_date.split()
        return f"{parts[2]} {parts[1]} {parts[4][:5]}"
    except Exception:
        return pub_date[:16] if pub_date else ""


def _render_search_page(key: str, page: int, query: str, results: list) -> tuple[str, InlineKeyboardMarkup]:
    total   = len(results)
    pages   = max(1, (total + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE)
    page    = max(0, min(page, pages - 1))
    start   = page * RESULTS_PER_PAGE
    end     = min(start + RESULTS_PER_PAGE, total)
    chunk   = results[start:end]

    lines = [
        f"📡 <b>Nyaa Search</b> — <code>{query[:35]}</code>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Page <b>{page + 1}/{pages}</b>  ·  {total} results",
        "",
    ]

    for i, r in enumerate(chunk):
        idx   = start + i
        num   = NUM_EMOJI[i] if i < len(NUM_EMOJI) else f"{i+1}."
        title = r.title[:65] + "…" if len(r.title) > 65 else r.title
        date  = _short_date(r.pub_date)
        lines.append(f"{num} <code>{title}</code>")
        lines.append(f"   💾 {r.size}  ·  🌱 {r.seeders}  ·  📅 {date}")
        lines.append("")

    lines += ["━━━━━━━━━━━━━━━━━━━━━━━━", "<i>Tap a number to see options:</i>"]

    # Number buttons row
    num_btns = []
    for i in range(len(chunk)):
        idx = start + i
        num_btns.append(InlineKeyboardButton(
            NUM_EMOJI[i] if i < len(NUM_EMOJI) else str(i+1),
            callback_data=f"nys|a|{key}|{idx}",
        ))

    rows = [num_btns]

    # Navigation row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"nys|p|{key}|{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="nys|noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"nys|p|{key}|{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("❌ Close", callback_data="nys|x")])

    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _render_result_detail(key: str, idx: int, r, page: int) -> tuple[str, InlineKeyboardMarkup]:
    title = r.title[:70] + "…" if len(r.title) > 70 else r.title
    date  = _short_date(r.pub_date)
    lines = [
        "📦 <b>Selected Result</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"<code>{title}</code>",
        "",
        f"💾 <b>Size:</b> {r.size}",
        f"🌱 <b>Seeders:</b> {r.seeders}  ·  <b>Leechers:</b> {r.leechers}",
        f"📥 <b>Downloads:</b> {r.downloads}",
        f"📅 <b>Date:</b> {date}",
        f"👤 <b>Uploader:</b> {r.uploader or '—'}",
        f"🔗 {r.link}",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    rows = [
        [InlineKeyboardButton("🧲 Get Magnet",  callback_data=f"nys|m|{key}|{idx}"),
         InlineKeyboardButton("📥 .torrent",    callback_data=f"nys|t|{key}|{idx}")],
        [InlineKeyboardButton("☁️ Seedr+HS",    callback_data=f"nys|sr|{key}|{idx}"),
         InlineKeyboardButton("📥 Local DL",    callback_data=f"nys|dl|{key}|{idx}")],
        [InlineKeyboardButton("🔙 Back to list", callback_data=f"nys|p|{key}|{page}")],
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
            "Usage: <code>/nyaa_search Kujima Utaeba le Hororo</code>\n\n"
            "Searches Nyaa.si anime category.\n"
            "Results include magnet links and torrent downloads.",
            parse_mode=enums.ParseMode.HTML,
        )

    st = await msg.reply(
        f"🔍 Searching Nyaa for: <code>{query[:40]}</code>…",
        parse_mode=enums.ParseMode.HTML,
    )

    from services.nyaa import search_nyaa
    results = await search_nyaa(query, category="1_0", filter_=0)

    if not results:
        return await st.edit(
            f"❌ No results found for: <code>{query}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    key = _cache_key(query)
    _cache_put(key, results, query)

    # Store magnets for callback access
    for i, r in enumerate(results):
        if r.magnet:
            _magnet_cache[f"{key}_{i}"] = r.magnet

    text, kb = _render_search_page(key, 0, query, results)
    await st.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)


# ─────────────────────────────────────────────────────────────
# Search callbacks  nys|<action>|<key>|<param>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nys\|"))
async def nys_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    action = parts[1] if len(parts) > 1 else ""

    if action == "noop":
        return await cb.answer()

    if action == "x":
        await cb.answer()
        return await cb.message.delete()

    if len(parts) < 4:
        return await cb.answer("Invalid.", show_alert=True)

    key   = parts[2]
    param = parts[3]
    uid   = cb.from_user.id

    cached = _cache_get(key)
    if not cached:
        await cb.answer("Search expired. Run /nyaa_search again.", show_alert=True)
        return

    results = cached["results"]
    query   = cached["query"]

    await cb.answer()

    # ── Page navigation ───────────────────────────────────────
    if action == "p":
        page = int(param)
        text, kb = _render_search_page(key, page, query, results)
        try:
            await cb.message.edit(text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass
        return

    # ── Show result detail ────────────────────────────────────
    if action == "a":
        idx = int(param)
        if idx >= len(results):
            return
        r    = results[idx]
        page = idx // RESULTS_PER_PAGE
        text, kb = _render_result_detail(key, idx, r, page)
        await cb.message.edit(text, parse_mode=enums.ParseMode.HTML,
                              reply_markup=kb, disable_web_page_preview=True)
        return

    # ── Get magnet link ───────────────────────────────────────
    if action == "m":
        idx = int(param)
        if idx >= len(results):
            return
        r = results[idx]
        magnet = r.magnet or _magnet_cache.get(f"{key}_{idx}", "")
        if not magnet:
            return await client.send_message(uid, "❌ No magnet link available.")
        # Send as copyable code block
        await client.send_message(
            uid,
            f"🧲 <b>Magnet Link</b>\n\n<code>{magnet}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    # ── Torrent download link ─────────────────────────────────
    if action == "t":
        idx = int(param)
        if idx >= len(results):
            return
        r = results[idx]
        if r.torrent_url:
            await client.send_message(
                uid,
                f"📥 <b>Torrent File</b>\n\n<code>{r.torrent_url}</code>\n\n"
                f"<i>Paste this URL to download the .torrent file.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
        else:
            await client.send_message(uid, "❌ No torrent URL available.")
        return

    # ── Seedr + Hardsub ───────────────────────────────────────
    if action == "sr":
        idx = int(param)
        if idx >= len(results):
            return
        r = results[idx]
        magnet = r.magnet or _magnet_cache.get(f"{key}_{idx}", "")
        if not magnet:
            return await client.send_message(uid, "❌ No magnet link available.")

        username = os.environ.get("SEEDR_USERNAME", "").strip()
        if not username:
            return await client.send_message(
                uid, "❌ Seedr credentials not configured. Add SEEDR_USERNAME/PASSWORD to .env",
                parse_mode=enums.ParseMode.HTML,
            )

        from plugins.url_handler import _seedr_download
        st = await client.send_message(
            uid,
            f"☁️ <b>Seedr Download</b>\n──────────────────────\n\n"
            f"📦 <code>{r.title[:50]}</code>\n\n"
            f"⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_seedr_download(client, st, magnet, uid))
        return

    # ── Local download ────────────────────────────────────────
    if action == "dl":
        idx = int(param)
        if idx >= len(results):
            return
        r = results[idx]
        magnet = r.magnet or _magnet_cache.get(f"{key}_{idx}", "")
        if not magnet:
            return await client.send_message(uid, "❌ No magnet link available.")

        from plugins.url_handler import _launch_download
        st = await client.send_message(
            uid,
            f"📥 <b>Downloading…</b>\n<code>{r.title[:50]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_launch_download(client, st, magnet, uid))
        return


# ═════════════════════════════════════════════════════════════
# /nyaa_add — Interactive setup (owner only)
# ═════════════════════════════════════════════════════════════

@Client.on_message(filters.command("nyaa_add") & filters.user(cfg.owner_id))
async def cmd_nyaa_add(client: Client, msg: Message):
    title = " ".join(msg.command[1:])
    if not title:
        return await msg.reply(
            "📡 <b>Nyaa Tracker — Add Anime</b>\n\n"
            "Usage: <code>/nyaa_add Kujima Utaeba le Hororo</code>\n\n"
            "Just the title — I'll guide you through the rest with buttons.",
            parse_mode=enums.ParseMode.HTML,
        )

    st = await msg.reply(
        f"🔍 Resolving: <code>{title}</code>\n<i>Querying AniList…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    from services.anilist import search_anime, all_titles

    all_title_list = [title]
    anilist_id = 0

    try:
        results = await search_anime(title)
        if results:
            best = results[0]
            anilist_id = best.get("id", 0)
            resolved = all_titles(best)
            seen_lower = {title.lower()}
            for t in resolved:
                if t.lower() not in seen_lower:
                    seen_lower.add(t.lower())
                    all_title_list.append(t)
    except Exception as exc:
        log.warning("[NyaaTracker] AniList failed: %s", exc)

    sid = _setup_id()
    _setup[sid] = {
        "uid": msg.from_user.id,
        "title": title,
        "titles": all_title_list,
        "anilist_id": anilist_id,
        "day": None,
        "uploader": None,
        "quality": None,
        "step": "day",
    }

    title_preview = "\n".join(f"  • <code>{t}</code>" for t in all_title_list[:6])
    extra = f"\n  <i>…+{len(all_title_list)-6} more</i>" if len(all_title_list) > 6 else ""

    day_rows = [
        [InlineKeyboardButton(DAY_SHORT[d], callback_data=f"nya|d|{sid}|{i}")
         for i, d in enumerate(DAYS) if i < 4],
        [InlineKeyboardButton(DAY_SHORT[d], callback_data=f"nya|d|{sid}|{i}")
         for i, d in enumerate(DAYS) if i >= 4],
        [InlineKeyboardButton("📅 Daily (check every day)", callback_data=f"nya|d|{sid}|7")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")],
    ]

    await st.edit(
        f"✅ <b>Resolved {len(all_title_list)} title(s)</b>\n\n"
        f"{title_preview}{extra}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Step 1/3 — Choose schedule:</b>\n"
        f"<i>Which day does this anime air?</i>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(day_rows),
    )


# ─────────────────────────────────────────────────────────────
# /nyaa_add interactive callbacks   nya|<action>|<sid>|<param>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nya\|"))
async def nya_cb(client: Client, cb: CallbackQuery):
    parts  = cb.data.split("|")
    action = parts[1]
    sid    = parts[2] if len(parts) > 2 else ""

    state = _setup.get(sid)
    if not state:
        return await cb.answer("Session expired. Use /nyaa_add again.", show_alert=True)
    await cb.answer()

    # ── Cancel ────────────────────────────────────────────────
    if action == "x":
        _setup.pop(sid, None)
        return await cb.message.delete()

    # ── Day selection ─────────────────────────────────────────
    if action == "d":
        day_idx = int(parts[3]) if len(parts) > 3 else 7
        if day_idx < 7:
            state["day"] = DAYS[day_idx]
        else:
            state["day"] = "daily"
        state["step"] = "uploader"

        up_rows = []
        row = []
        for i, up in enumerate(COMMON_UPLOADERS):
            row.append(InlineKeyboardButton(up, callback_data=f"nya|u|{sid}|{i}"))
            if len(row) == 3:
                up_rows.append(row)
                row = []
        if row:
            up_rows.append(row)
        up_rows.append([InlineKeyboardButton("🔓 Any uploader", callback_data=f"nya|u|{sid}|99")])
        up_rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")])

        day_display = state["day"].capitalize()
        await cb.message.edit(
            f"📅 Schedule: <b>{day_display}</b> ✅\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Step 2/3 — Preferred uploader:</b>\n"
            f"<i>Filter results by release group</i>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(up_rows),
        )
        return

    # ── Uploader selection ────────────────────────────────────
    if action == "u":
        up_idx = int(parts[3]) if len(parts) > 3 else 99
        if up_idx < len(COMMON_UPLOADERS):
            state["uploader"] = COMMON_UPLOADERS[up_idx]
        else:
            state["uploader"] = ""
        state["step"] = "quality"

        q_rows = [
            [InlineKeyboardButton("🔵 1080p", callback_data=f"nya|q|{sid}|1080p"),
             InlineKeyboardButton("🟢 720p",  callback_data=f"nya|q|{sid}|720p")],
            [InlineKeyboardButton("🟡 480p",  callback_data=f"nya|q|{sid}|480p"),
             InlineKeyboardButton("🔓 Any",   callback_data=f"nya|q|{sid}|")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")],
        ]

        day_display = state["day"].capitalize()
        up_display  = state["uploader"] or "Any"
        await cb.message.edit(
            f"📅 Schedule: <b>{day_display}</b> ✅\n"
            f"👤 Uploader: <b>{up_display}</b> ✅\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Step 3/3 — Quality preference:</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(q_rows),
        )
        return

    # ── Quality selection → save entry ────────────────────────
    if action == "q":
        quality = parts[3] if len(parts) > 3 else ""
        state["quality"] = quality

        entry = WatchlistEntry(
            id=0,
            display_name=state["title"],
            titles=state["titles"],
            anilist_id=state["anilist_id"],
            day=state["day"],
            uploader=state["uploader"],
            quality=quality,
        )
        eid = await watchlist.add(entry)
        _setup.pop(sid, None)
        _ensure_poller()

        day_display = entry.day.capitalize()
        await cb.message.edit(
            f"✅ <b>Added to Nyaa Watchlist</b>  (#{eid})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 <b>{entry.display_name}</b>\n"
            f"📅 {day_display}\n"
            f"👤 Uploader: <code>{entry.uploader or 'Any'}</code>\n"
            f"📐 Quality: <code>{quality or 'Any'}</code>\n"
            f"🔑 {len(entry.titles)} title aliases\n\n"
            f"<i>The poller checks Nyaa every 10 min.\n"
            f"Use /nyaa_check to trigger now.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        return


# ═════════════════════════════════════════════════════════════
# /nyaa_list, /nyaa_remove, /nyaa_toggle, /nyaa_dump
# ═════════════════════════════════════════════════════════════

@Client.on_message(filters.command("nyaa_list") & filters.user(cfg.owner_id))
async def cmd_nyaa_list(client: Client, msg: Message):
    entries = watchlist.all_entries()
    if not entries:
        return await msg.reply(
            "📡 <b>Nyaa Watchlist — Empty</b>\n\n"
            "Use <code>/nyaa_add Title</code> to start tracking.",
            parse_mode=enums.ParseMode.HTML,
        )

    lines = ["📡 <b>Nyaa Watchlist</b>", "━━━━━━━━━━━━━━━━━━━━━━━━", ""]
    for e in entries:
        icon  = "🟢" if e.active else "🔴"
        day_s = e.day.capitalize() if e.day != "daily" else "Daily"
        up_s  = f"[{e.uploader}]" if e.uploader else ""
        lines.append(
            f"{icon} <b>#{e.id}</b>  <code>{e.display_name[:28]}</code>\n"
            f"   📅 {day_s}  📐 {e.quality or 'Any'}  {up_s}\n"
            f"   🔑 {len(e.titles)} aliases  ·  📦 {len(e.seen_hashes)} seen"
        )
        lines.append("")

    dump = _config.get("dump_channel", 0)
    lines.append(f"📢 Dump: <code>{dump or 'Not set'}</code>")
    await msg.reply("\n".join(lines)[:4000], parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_remove") & filters.user(cfg.owner_id))
async def cmd_nyaa_remove(client: Client, msg: Message):
    args = msg.command[1:]
    if not args or not args[0].isdigit():
        return await msg.reply("Usage: <code>/nyaa_remove &lt;id&gt;</code>",
                               parse_mode=enums.ParseMode.HTML)
    eid = int(args[0])
    entry = watchlist.get(eid)
    if not entry:
        return await msg.reply(f"❌ Entry #{eid} not found.")
    await watchlist.remove(eid)
    await msg.reply(
        f"✅ Removed <b>#{eid}</b> — <code>{entry.display_name}</code>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("nyaa_toggle") & filters.user(cfg.owner_id))
async def cmd_nyaa_toggle(client: Client, msg: Message):
    args = msg.command[1:]
    if not args or not args[0].isdigit():
        return await msg.reply("Usage: <code>/nyaa_toggle &lt;id&gt;</code>",
                               parse_mode=enums.ParseMode.HTML)
    eid = int(args[0])
    entry = watchlist.get(eid)
    if not entry:
        return await msg.reply(f"❌ #{eid} not found.")
    new = not entry.active
    await watchlist.update(eid, active=new)
    icon = "🟢" if new else "🔴"
    await msg.reply(f"{icon} #{eid} {entry.display_name} — {'Enabled' if new else 'Disabled'}",
                    parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_dump") & filters.user(cfg.owner_id))
async def cmd_nyaa_dump(client: Client, msg: Message):
    args = msg.command[1:]
    if not args:
        cur = _config.get("dump_channel", 0)
        return await msg.reply(
            f"📢 <b>Dump Channel:</b> <code>{cur or 'Not set'}</code>\n\n"
            f"Set: <code>/nyaa_dump @channel</code>\n"
            f"Disable: <code>/nyaa_dump 0</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    target = args[0]
    if target == "0":
        _config["dump_channel"] = 0
        _save_config()
        return await msg.reply("📢 Dump channel disabled.")
    try:
        if target.lstrip("-").isdigit():
            ch_id = int(target)
        else:
            chat = await client.get_chat(target)
            ch_id = chat.id
        _config["dump_channel"] = ch_id
        _save_config()
        await msg.reply(f"✅ Dump channel: <code>{ch_id}</code>", parse_mode=enums.ParseMode.HTML)
    except Exception as exc:
        await msg.reply(f"❌ {exc}", parse_mode=enums.ParseMode.HTML)


# ═════════════════════════════════════════════════════════════
# /nyaa_check — manual poll (owner only)
# ═════════════════════════════════════════════════════════════

@Client.on_message(filters.command("nyaa_check") & filters.user(cfg.owner_id))
async def cmd_nyaa_check(client: Client, msg: Message):
    entries = [e for e in watchlist.all_entries() if e.active]
    if not entries:
        return await msg.reply("📡 No active entries.")

    st = await msg.reply(f"🔍 Checking {len(entries)} entries…", parse_mode=enums.ParseMode.HTML)
    _ensure_poller()

    found = 0
    for i, entry in enumerate(entries, 1):
        try:
            n = await _check_entry(entry)
            found += n
            await st.edit(
                f"🔍 {i}/{len(entries)}…  {found} new match(es)",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as exc:
            log.warning("[NyaaTracker] Check %s: %s", entry.display_name, exc)
        await asyncio.sleep(2)

    await st.edit(
        f"✅ Done — {len(entries)} checked, {found} new matches",
        parse_mode=enums.ParseMode.HTML,
    )


# ═════════════════════════════════════════════════════════════
# Poller
# ═════════════════════════════════════════════════════════════

_poller_task: Optional[asyncio.Task] = None


def _ensure_poller():
    global _poller_task
    if _poller_task and not _poller_task.done():
        return
    _poller_task = asyncio.create_task(_poll_loop())
    log.info("[NyaaTracker] Poller started")


async def _poll_loop():
    await asyncio.sleep(30)
    while True:
        try:
            import datetime
            today = datetime.datetime.now().strftime("%A").lower()
            entries = watchlist.entries_for_day(today)
            for entry in entries:
                try:
                    await _check_entry(entry)
                except Exception as exc:
                    log.error("[NyaaTracker] Poll %s: %s", entry.display_name, exc)
                await asyncio.sleep(3)
        except Exception as exc:
            log.error("[NyaaTracker] Poll loop: %s", exc, exc_info=True)
        await asyncio.sleep(_config.get("poll_interval", 600))


async def _check_entry(entry: WatchlistEntry) -> int:
    """Check one entry against Nyaa. Returns count of new matches."""
    from services.nyaa import search_nyaa, match_title, extract_episode

    search_terms = [entry.display_name]
    for t in entry.titles:
        if t.lower().strip() not in [s.lower().strip() for s in search_terms]:
            search_terms.append(t)
            if len(search_terms) >= 3:
                break

    all_results = []
    seen_h: set = set()

    for term in search_terms:
        results = await search_nyaa(term, category=entry.category)
        for r in results:
            if r.info_hash and r.info_hash not in seen_h:
                seen_h.add(r.info_hash)
                all_results.append(r)
        await asyncio.sleep(1)

    await watchlist.update(entry.id, last_check=time.time())

    matched = [
        r for r in all_results
        if r.info_hash not in entry.seen_hashes
        and match_title(r.title, entry.titles, entry.uploader, entry.quality)
    ]

    if not matched:
        return 0

    log.info("[NyaaTracker] %d new match(es) for '%s'", len(matched), entry.display_name)

    from core.session import get_client
    client  = get_client()
    dump_ch = _config.get("dump_channel", 0)

    for r in matched:
        ep   = extract_episode(r.title)
        ep_s = f"Ep {ep}" if ep else "Batch"
        h12  = r.info_hash[:12] if r.info_hash else hashlib.md5(r.title.encode()).hexdigest()[:12]

        # Store magnet for callback
        if r.magnet:
            _magnet_cache[h12] = r.magnet

        text = (
            f"🔔 <b>Nyaa Match</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 <b>{entry.display_name}</b>  ({ep_s})\n"
            f"📦 <code>{r.title[:70]}</code>\n\n"
            f"💾 {r.size}  ·  🌱 {r.seeders}  ·  📥 {r.downloads}\n"
            f"👤 {r.uploader or '—'}  ·  📅 {_short_date(r.pub_date)}"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("☁️ Seedr+Hardsub", callback_data=f"nyt|sr|{entry.id}|{h12}"),
             InlineKeyboardButton("📥 Download",      callback_data=f"nyt|dl|{entry.id}|{h12}")],
            [InlineKeyboardButton("🧲 Magnet",        callback_data=f"nyt|mg|{entry.id}|{h12}"),
             InlineKeyboardButton("❌ Skip",           callback_data=f"nyt|sk|{entry.id}|{h12}")],
        ])

        # Send to dump channel
        if dump_ch:
            try:
                await client.send_message(dump_ch, text, parse_mode=enums.ParseMode.HTML,
                                          reply_markup=kb, disable_web_page_preview=True)
            except Exception as exc:
                log.warning("[NyaaTracker] Dump send: %s", exc)

        # Notify owner
        try:
            await client.send_message(cfg.owner_id, text, parse_mode=enums.ParseMode.HTML,
                                      reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            pass

        await watchlist.mark_seen(entry.id, r.info_hash)

    return len(matched)


# ─────────────────────────────────────────────────────────────
# Poller match callbacks   nyt|<action>|<eid>|<hash12>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nyt\|"))
async def nyt_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 4:
        return await cb.answer("Invalid.", show_alert=True)

    action = parts[1]
    h12    = parts[3]
    uid    = cb.from_user.id
    await cb.answer()

    if action == "sk":
        try:
            await cb.message.edit(
                cb.message.text + "\n\n❌ <b>Skipped</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    magnet = _magnet_cache.get(h12, "")

    if action == "mg":
        if not magnet:
            return await client.send_message(uid, "❌ Magnet expired.")
        await client.send_message(
            uid,
            f"🧲 <b>Magnet Link</b>\n\n<code>{magnet}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    if not magnet:
        return await client.send_message(uid, "❌ Magnet expired — resend link manually.")

    if action == "sr":
        username = os.environ.get("SEEDR_USERNAME", "").strip()
        if not username:
            return await client.send_message(
                uid, "❌ Seedr not configured. Add SEEDR_USERNAME/PASSWORD to .env",
                parse_mode=enums.ParseMode.HTML,
            )
        from plugins.url_handler import _seedr_download
        st = await client.send_message(
            uid,
            "☁️ <b>Seedr Download</b>\n──────────────────────\n\n"
            "⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_seedr_download(client, st, magnet, uid))

        try:
            await cb.message.edit(
                cb.message.text + "\n\n☁️ <b>Sent to Seedr</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    if action == "dl":
        from plugins.url_handler import _launch_download
        st = await client.send_message(
            uid,
            "📥 <b>Downloading…</b>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_launch_download(client, st, magnet, uid))

        try:
            await cb.message.edit(
                cb.message.text + "\n\n📥 <b>Download started</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return


# ═════════════════════════════════════════════════════════════
# Auto-start
# ═════════════════════════════════════════════════════════════

def start_nyaa_poller():
    if any(e.active for e in watchlist.all_entries()):
        _ensure_poller()
        log.info("[NyaaTracker] Poller auto-started (%d active entries)",
                 sum(1 for e in watchlist.all_entries() if e.active))
