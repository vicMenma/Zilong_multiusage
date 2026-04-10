"""
colab_leecher/nyaa_tracker.py
Nyaa anime search + tracker for Zilong bot.

FEATURES:
  /nyaa_search <query>  — 10 results/page, paginated, magnets + Seedr+CC
  /nyaa_add <title>     — interactive setup with date+time scheduling
  /nyaa_list            — watchlist
  /nyaa_remove <id>     — remove
  /nyaa_check           — poll now
  /nyaa_snipe <query> <datetime>  — snipe mode: poll every 5s at specific time
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
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote_plus

import aiohttp
from pyrogram import filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from colab_leecher import colab_bot, OWNER

log = logging.getLogger(__name__)

_DATA_DIR    = "/content/zilong/data"
_STORE_PATH  = os.path.join(_DATA_DIR, "nyaa_watchlist.json")
_CACHE_TTL   = 1800
PER_PAGE     = 10

NUM_EMOJI = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

DAYS = ("monday","tuesday","wednesday","thursday","friday","saturday","sunday")
DAY_SHORT = {"monday":"Mon","tuesday":"Tue","wednesday":"Wed",
             "thursday":"Thu","friday":"Fri","saturday":"Sat","sunday":"Sun"}

UPLOADERS = ["Erai-raws","SubsPlease","Tsundere-Raws","EMBER","ToonsHub","DKB","Judas","ASW"]

_NYAA_RSS = "https://nyaa.si/?page=rss"
_NYAA_NS  = {"nyaa": "https://nyaa.si/xmlns/nyaa"}
_TRACKERS = (
    "http://nyaa.tracker.wf:7777/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://exodus.desync.com:6969/announce",
)

# ═════════════════════════════════════════════════════════════
# Nyaa RSS parser
# ═════════════════════════════════════════════════════════════

@dataclass
class NyaaEntry:
    title: str; link: str; magnet: str = ""
    torrent_url: str = ""; size: str = ""
    seeders: int = 0; leechers: int = 0; downloads: int = 0
    pub_date: str = ""; info_hash: str = ""; uploader: str = ""

    def __post_init__(self):
        m = re.match(r'^\[([^\]]+)\]', self.title)
        if m and not self.uploader: self.uploader = m.group(1).strip()
        if not self.torrent_url and self.link:
            nid = re.search(r'/view/(\d+)', self.link)
            if nid: self.torrent_url = f"https://nyaa.si/download/{nid.group(1)}.torrent"
        if self.magnet and not self.info_hash:
            ih = re.search(r'btih:([a-fA-F0-9]{40}|[A-Za-z2-7]{32})', self.magnet)
            if ih: self.info_hash = ih.group(1).upper()
        if not self.magnet and self.info_hash:
            dn = quote_plus(self.title)
            trs = "&".join(f"tr={quote_plus(t)}" for t in _TRACKERS)
            self.magnet = f"magnet:?xt=urn:btih:{self.info_hash}&dn={dn}&{trs}"


def _parse_rss(xml_text: str) -> list[NyaaEntry]:
    import xml.etree.ElementTree as ET
    entries = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    for item in root.findall(".//item"):
        title     = (item.findtext("title") or "").strip()
        link      = (item.findtext("link") or "").strip()
        guid      = (item.findtext("guid") or "").strip()
        seeders   = int(item.findtext("nyaa:seeders",   "0", _NYAA_NS) or 0)
        leechers  = int(item.findtext("nyaa:leechers",  "0", _NYAA_NS) or 0)
        downloads = int(item.findtext("nyaa:downloads",  "0", _NYAA_NS) or 0)
        size      = (item.findtext("nyaa:size", "", _NYAA_NS) or "").strip()
        info_hash = (item.findtext("nyaa:infoHash", "", _NYAA_NS) or "").strip()
        pub_date  = (item.findtext("pubDate") or "").strip()
        magnet    = guid if guid.startswith("magnet:") else ""
        entries.append(NyaaEntry(
            title=title, link=link, magnet=magnet, size=size,
            seeders=seeders, leechers=leechers, downloads=downloads,
            pub_date=pub_date, info_hash=info_hash.upper() if info_hash else "",
        ))
    return entries


async def search_nyaa(query: str, category: str = "1_0", timeout: int = 15) -> list[NyaaEntry]:
    url = f"{_NYAA_RSS}&q={quote_plus(query)}&c={category}&f=0&s=id&o=desc"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"User-Agent": "ZilongBot/2.0"},
        ) as sess:
            async with sess.get(url) as resp:
                if resp.status != 200: return []
                return _parse_rss(await resp.text())
    except Exception as e:
        log.warning("[Nyaa] %s", e)
        return []

def _short_date(d: str) -> str:
    try:
        parts = d.split()
        return f"{parts[2]} {parts[1]} {parts[4][:5]}"
    except Exception:
        return d[:16]

# ═════════════════════════════════════════════════════════════
# Search cache + magnet cache
# ═════════════════════════════════════════════════════════════

_cache: dict[str, dict] = {}
_magnets: dict[str, str] = {}

def _ck(q): return hashlib.md5(f"{q}_{time.time():.0f}".encode()).hexdigest()[:8]
def _cp(k, r, q): _cache[k] = {"r": r, "q": q, "t": time.time()}
def _cg(k):
    e = _cache.get(k)
    if e and time.time() - e["t"] < _CACHE_TTL: return e
    _cache.pop(k, None); return None

# ═════════════════════════════════════════════════════════════
# Render helpers
# ═════════════════════════════════════════════════════════════

def _render_page(key, page, query, results):
    total = len(results)
    pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page  = max(0, min(page, pages - 1))
    start = page * PER_PAGE
    chunk = results[start:start + PER_PAGE]

    lines = [
        f"📡 <b>Nyaa Search</b> — <code>{query[:30]}</code>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Page <b>{page+1}/{pages}</b>  ·  {total} results",
        "",
    ]
    for i, r in enumerate(chunk):
        num = NUM_EMOJI[i] if i < len(NUM_EMOJI) else f"{i+1}."
        title = r.title[:60] + "…" if len(r.title) > 60 else r.title
        lines.append(f"{num} <code>{title}</code>")
        lines.append(f"   💾 {r.size}  🌱 {r.seeders}  📅 {_short_date(r.pub_date)}")
        lines.append("")

    # Number buttons (2 rows of 5)
    nums1 = [InlineKeyboardButton(NUM_EMOJI[i], callback_data=f"nys|a|{key}|{start+i}")
             for i in range(min(5, len(chunk)))]
    nums2 = [InlineKeyboardButton(NUM_EMOJI[i], callback_data=f"nys|a|{key}|{start+i}")
             for i in range(5, len(chunk))]

    rows = []
    if nums1: rows.append(nums1)
    if nums2: rows.append(nums2)

    nav = []
    if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"nys|p|{key}|{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="nys|noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("▶️", callback_data=f"nys|p|{key}|{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("❌ Close", callback_data="nys|x")])

    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _render_detail(key, idx, r, page):
    title = r.title[:65] + "…" if len(r.title) > 65 else r.title
    lines = [
        "📦 <b>Selected</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"<code>{title}</code>", "",
        f"💾 {r.size}  ·  🌱 {r.seeders}  ·  📥 {r.downloads}",
        f"👤 {r.uploader or '—'}  ·  📅 {_short_date(r.pub_date)}",
        f"🔗 {r.link}", "",
    ]
    h = r.info_hash[:12] if r.info_hash else hashlib.md5(r.title.encode()).hexdigest()[:12]
    rows = [
        [InlineKeyboardButton("🧲 Magnet",        callback_data=f"nys|m|{key}|{idx}"),
         InlineKeyboardButton("📥 .torrent",      callback_data=f"nys|t|{key}|{idx}")],
        [InlineKeyboardButton("☁️ Seedr+Hardsub", callback_data=f"nys|sr|{key}|{idx}"),
         InlineKeyboardButton("☁️ Seedr+CC 🗜",   callback_data=f"nys|sc|{key}|{idx}")],
        [InlineKeyboardButton("📥 Local DL",      callback_data=f"nys|dl|{key}|{idx}")],
        [InlineKeyboardButton("🔙 Back",          callback_data=f"nys|p|{key}|{page}")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# ═════════════════════════════════════════════════════════════
# /nyaa_search
# ═════════════════════════════════════════════════════════════

@colab_bot.on_message(filters.command("nyaa_search") & filters.private)
async def cmd_nyaa_search(client, message):
    query = " ".join(message.command[1:])
    if not query:
        return await message.reply_text(
            "📡 <b>Nyaa Search</b>\n\n"
            "Usage: <code>/nyaa_search Kujima Utaeba</code>\n\n"
            "10 results per page with magnet, torrent,\n"
            "Seedr+Hardsub and Seedr+CC Compress buttons."
        )
    st = await message.reply_text(f"🔍 Searching: <code>{query[:40]}</code>…")
    results = await search_nyaa(query)
    if not results:
        return await st.edit_text(f"❌ No results for: <code>{query}</code>")

    key = _ck(query)
    _cp(key, results, query)
    for i, r in enumerate(results):
        if r.magnet: _magnets[f"{key}_{i}"] = r.magnet

    text, kb = _render_page(key, 0, query, results)
    await st.edit_text(text, reply_markup=kb, disable_web_page_preview=True)


# ═════════════════════════════════════════════════════════════
# Search callbacks
# ═════════════════════════════════════════════════════════════

@colab_bot.on_callback_query(filters.regex(r"^nys\|"))
async def nys_cb(client, cq):
    parts = cq.data.split("|")
    action = parts[1]
    uid = cq.message.chat.id

    if action == "noop": return await cq.answer()
    if action == "x": await cq.answer(); return await cq.message.delete()

    if len(parts) < 4: return await cq.answer("Invalid.", show_alert=True)
    key, param = parts[2], parts[3]
    cached = _cg(key)
    if not cached:
        return await cq.answer("Expired. /nyaa_search again.", show_alert=True)
    results, query = cached["r"], cached["q"]
    await cq.answer()

    if action == "p":
        text, kb = _render_page(key, int(param), query, results)
        try: await cq.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        except Exception: pass
        return

    if action == "a":
        idx = int(param)
        if idx >= len(results): return
        text, kb = _render_detail(key, idx, results[idx], idx // PER_PAGE)
        await cq.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
        return

    idx = int(param)
    if idx >= len(results): return
    r = results[idx]
    magnet = r.magnet or _magnets.get(f"{key}_{idx}", "")

    if action == "m":
        if not magnet: return await client.send_message(uid, "❌ No magnet.")
        await client.send_message(uid, f"🧲 <b>Magnet</b>\n\n<code>{magnet}</code>")
        return

    if action == "t":
        if r.torrent_url:
            await client.send_message(uid, f"📥 <b>Torrent</b>\n\n<code>{r.torrent_url}</code>")
        return

    if action == "dl":
        if not magnet: return await client.send_message(uid, "❌ No magnet.")
        # Feed magnet into the bot's normal link handler
        from colab_leecher.utility.variables import BOT
        BOT.SOURCE = [magnet]
        BOT.Mode.ytdl = False; BOT.Mode.mode = "leech"
        BOT.State.started = True
        await cq.message.edit_text(
            f"📥 <b>Download queued</b>\n<code>{r.title[:50]}</code>\n\n"
            "Choose mode:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📄 Normal", callback_data="normal"),
                 InlineKeyboardButton("🗜 Compress", callback_data="zip")],
                [InlineKeyboardButton("📂 Extract", callback_data="unzip")],
            ])
        )
        return

    if action == "sr":
        if not magnet: return await client.send_message(uid, "❌ No magnet.")
        await client.send_message(
            uid,
            f"☁️ <b>Seedr+Hardsub</b>\n\n"
            f"📦 <code>{r.title[:50]}</code>\n\n"
            f"🧲 <code>{magnet[:80]}…</code>\n\n"
            f"<i>Paste this magnet in the bot to use Seedr pipeline\n"
            f"(the full Seedr+Hardsub workflow from Zilong Multiusage).</i>"
        )
        return

    if action == "sc":
        if not magnet: return await client.send_message(uid, "❌ No magnet.")
        await client.send_message(
            uid,
            f"☁️ <b>Seedr + CloudConvert Compress</b>\n\n"
            f"📦 <code>{r.title[:50]}</code>\n\n"
            f"🧲 <code>{magnet[:80]}…</code>\n\n"
            f"<b>Why Seedr+CC Compress?</b>\n"
            f"Seedr downloads at datacenter speed → CC re-encodes\n"
            f"to target resolution/size → auto-uploads to Telegram.\n\n"
            f"<i>Available in Zilong Multiusage (full version).\n"
            f"Use /nyaa_search in Multiusage for the full pipeline.</i>"
        )
        return


# ═════════════════════════════════════════════════════════════
# Watchlist store
# ═════════════════════════════════════════════════════════════

@dataclass
class WatchEntry:
    id: int; name: str; titles: list = field(default_factory=list)
    day: str = "daily"; uploader: str = ""; quality: str = "1080p"
    active: bool = True; seen: list = field(default_factory=list)
    # Snipe mode: specific date+time, poll every 5s
    snipe_at: str = ""  # ISO format: "2026-04-12T12:00:00"
    snipe_done: bool = False


class _WL:
    _entries: dict = {}; _nid: int = 1

    @classmethod
    def _load(cls):
        try:
            with open(_STORE_PATH) as f: raw = json.load(f)
            for d in raw.get("e", {}).values():
                try:
                    e = WatchEntry(**d); cls._entries[e.id] = e
                except TypeError: pass
            cls._nid = raw.get("n", max(cls._entries.keys(), default=0) + 1)
        except FileNotFoundError: pass
        except Exception as e: log.warning("[NyaaWL] %s", e)

    @classmethod
    def _save(cls):
        os.makedirs(_DATA_DIR, exist_ok=True)
        with open(_STORE_PATH, "w") as f:
            json.dump({"e": {str(e.id): asdict(e) for e in cls._entries.values()}, "n": cls._nid}, f, indent=2)

    @classmethod
    def add(cls, e): e.id = cls._nid; cls._nid += 1; cls._entries[e.id] = e; cls._save(); return e.id

    @classmethod
    def remove(cls, eid):
        if eid in cls._entries: del cls._entries[eid]; cls._save(); return True
        return False

    @classmethod
    def get(cls, eid): return cls._entries.get(eid)
    @classmethod
    def all(cls): return sorted(cls._entries.values(), key=lambda e: e.id)
    @classmethod
    def for_day(cls, day):
        return [e for e in cls._entries.values() if e.active and (e.day == day or e.day == "daily")]
    @classmethod
    def snipers(cls):
        return [e for e in cls._entries.values() if e.active and e.snipe_at and not e.snipe_done]
    @classmethod
    def mark_seen(cls, eid, h):
        e = cls._entries.get(eid)
        if e and h not in e.seen: e.seen.append(h); cls._save()
    @classmethod
    def update(cls, eid, **kw):
        e = cls._entries.get(eid)
        if not e: return
        for k, v in kw.items():
            if hasattr(e, k): setattr(e, k, v)
        cls._save()

_WL._load()


# ═════════════════════════════════════════════════════════════
# /nyaa_add — interactive setup
# ═════════════════════════════════════════════════════════════

_setup: dict = {}

@colab_bot.on_message(filters.command("nyaa_add") & filters.private)
async def cmd_nyaa_add(client, message):
    if message.chat.id != OWNER: return
    title = " ".join(message.command[1:])
    if not title:
        return await message.reply_text(
            "📡 <b>Nyaa Tracker — Add</b>\n\n"
            "<code>/nyaa_add Kujima Utaeba le Hororo</code>\n\n"
            "I'll guide you through day/uploader/quality with buttons."
        )

    # Resolve titles via AniList (lightweight, no dep needed)
    titles = [title]
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as s:
            q = '{ Page(page:1,perPage:3) { media(search:"%s",type:ANIME) { title { romaji english native } synonyms } } }' % title.replace('"', '\\"')
            async with s.post("https://graphql.anilist.co", json={"query": q}) as r:
                data = await r.json()
            for m in (data.get("data") or {}).get("Page", {}).get("media", [])[:1]:
                t = m.get("title") or {}
                for v in (t.get("romaji"), t.get("english"), t.get("native")):
                    if v and v.lower() not in [x.lower() for x in titles]:
                        titles.append(v)
                for syn in (m.get("synonyms") or [])[:3]:
                    if syn and syn.lower() not in [x.lower() for x in titles]:
                        titles.append(syn)
    except Exception: pass

    sid = hashlib.md5(str(time.time()).encode()).hexdigest()[:6]
    _setup[sid] = {"title": title, "titles": titles}

    preview = "\n".join(f"  · <code>{t}</code>" for t in titles[:6])

    rows = [
        [InlineKeyboardButton(DAY_SHORT[d], callback_data=f"nya|d|{sid}|{i}")
         for i, d in enumerate(DAYS) if i < 4],
        [InlineKeyboardButton(DAY_SHORT[d], callback_data=f"nya|d|{sid}|{i}")
         for i, d in enumerate(DAYS) if i >= 4],
        [InlineKeyboardButton("📅 Daily", callback_data=f"nya|d|{sid}|7"),
         InlineKeyboardButton("🎯 Snipe (date+time)", callback_data=f"nya|d|{sid}|8")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")],
    ]
    await message.reply_text(
        f"✅ <b>{len(titles)} title(s) resolved</b>\n\n{preview}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>Step 1/3 — Schedule:</b>",
        reply_markup=InlineKeyboardMarkup(rows),
    )


# Snipe datetime input state
_snipe_waiting: dict = {}  # uid → sid


@colab_bot.on_callback_query(filters.regex(r"^nya\|"))
async def nya_cb(client, cq):
    parts = cq.data.split("|")
    action, sid = parts[1], parts[2]
    state = _setup.get(sid)
    if not state: return await cq.answer("Expired.", show_alert=True)
    await cq.answer()

    if action == "x":
        _setup.pop(sid, None)
        return await cq.message.delete()

    if action == "d":
        idx = int(parts[3])
        if idx == 8:
            # Snipe mode — ask for date+time
            _snipe_waiting[cq.message.chat.id] = sid
            state["day"] = "snipe"
            await cq.message.edit_text(
                "🎯 <b>Snipe Mode</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Send the exact date and time to start polling:\n\n"
                "<code>12-04-2026 12:00</code>\n\n"
                "Format: <code>DD-MM-YYYY HH:MM</code>\n\n"
                "<i>The bot will poll Nyaa every 5 seconds\n"
                "starting at this time until a match is found.</i>"
            )
            return
        elif idx < 7:
            state["day"] = DAYS[idx]
        else:
            state["day"] = "daily"

        # Step 2: uploader
        rows = []
        row = []
        for i, up in enumerate(UPLOADERS):
            row.append(InlineKeyboardButton(up, callback_data=f"nya|u|{sid}|{i}"))
            if len(row) == 3: rows.append(row); row = []
        if row: rows.append(row)
        rows.append([InlineKeyboardButton("🔓 Any", callback_data=f"nya|u|{sid}|99")])
        rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")])

        await cq.message.edit_text(
            f"📅 {state['day'].capitalize()} ✅\n\n"
            "<b>Step 2/3 — Uploader:</b>",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if action == "u":
        idx = int(parts[3])
        state["uploader"] = UPLOADERS[idx] if idx < len(UPLOADERS) else ""

        rows = [
            [InlineKeyboardButton("🔵 1080p", callback_data=f"nya|q|{sid}|1080p"),
             InlineKeyboardButton("🟢 720p",  callback_data=f"nya|q|{sid}|720p")],
            [InlineKeyboardButton("🟡 480p",  callback_data=f"nya|q|{sid}|480p"),
             InlineKeyboardButton("🔓 Any",   callback_data=f"nya|q|{sid}|")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"nya|x|{sid}")],
        ]
        await cq.message.edit_text(
            f"📅 {state['day'].capitalize()} ✅\n"
            f"👤 {state['uploader'] or 'Any'} ✅\n\n"
            "<b>Step 3/3 — Quality:</b>",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if action == "q":
        quality = parts[3]
        entry = WatchEntry(
            id=0, name=state["title"], titles=state["titles"],
            day=state["day"], uploader=state.get("uploader", ""),
            quality=quality, snipe_at=state.get("snipe_at", ""),
        )
        eid = _WL.add(entry)
        _setup.pop(sid, None)
        _ensure_poller()

        extra = ""
        if entry.snipe_at:
            extra = f"\n🎯 Snipe at: <code>{entry.snipe_at}</code>\n   <i>Polling every 5 seconds</i>"

        await cq.message.edit_text(
            f"✅ <b>Added #{eid}</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 <b>{entry.name}</b>\n"
            f"📅 {entry.day.capitalize()}\n"
            f"👤 {entry.uploader or 'Any'}\n"
            f"📐 {quality or 'Any'}\n"
            f"🔑 {len(entry.titles)} aliases{extra}"
        )


# Handle snipe datetime input
@colab_bot.on_message(filters.text & filters.private & ~filters.command(
    ["start","help","settings","status","stats","ping","cancel","stop",
     "setname","rename","zipaswd","unzipaswd",
     "nyaa_search","nyaa_add","nyaa_list","nyaa_remove","nyaa_check","nyaa_snipe"]
))
async def snipe_input(client, message):
    uid = message.chat.id
    sid = _snipe_waiting.get(uid)
    if not sid or sid not in _setup:
        return
    text = message.text.strip()
    # Parse DD-MM-YYYY HH:MM
    try:
        dt = datetime.strptime(text, "%d-%m-%Y %H:%M")
        iso = dt.isoformat()
    except ValueError:
        try:
            dt = datetime.strptime(text, "%Y-%m-%d %H:%M")
            iso = dt.isoformat()
        except ValueError:
            return await message.reply_text("❌ Invalid format. Use: <code>DD-MM-YYYY HH:MM</code>")

    _setup[sid]["snipe_at"] = iso
    _snipe_waiting.pop(uid, None)

    # Continue to step 2 (uploader)
    state = _setup[sid]
    rows = []
    row = []
    for i, up in enumerate(UPLOADERS):
        row.append(InlineKeyboardButton(up, callback_data=f"nya|u|{sid}|{i}"))
        if len(row) == 3: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("🔓 Any", callback_data=f"nya|u|{sid}|99")])

    await message.reply_text(
        f"🎯 Snipe: <code>{text}</code> ✅\n\n"
        "<b>Step 2/3 — Uploader:</b>",
        reply_markup=InlineKeyboardMarkup(rows),
    )


# ═════════════════════════════════════════════════════════════
# /nyaa_list, /nyaa_remove, /nyaa_check
# ═════════════════════════════════════════════════════════════

@colab_bot.on_message(filters.command("nyaa_list") & filters.private)
async def cmd_list(client, message):
    if message.chat.id != OWNER: return
    entries = _WL.all()
    if not entries:
        return await message.reply_text("📡 <b>Watchlist empty.</b>\nUse /nyaa_add")
    lines = ["📡 <b>Nyaa Watchlist</b>", "━━━━━━━━━━━━━━━━━━━━━━━━", ""]
    for e in entries:
        icon = "🟢" if e.active else "🔴"
        snipe = f" 🎯 {e.snipe_at[5:16]}" if e.snipe_at else ""
        lines.append(
            f"{icon} <b>#{e.id}</b>  <code>{e.name[:25]}</code>\n"
            f"   📅 {e.day.capitalize()}  📐 {e.quality}  [{e.uploader or 'Any'}]{snipe}"
        )
        lines.append("")
    await message.reply_text("\n".join(lines)[:4000])


@colab_bot.on_message(filters.command("nyaa_remove") & filters.private)
async def cmd_remove(client, message):
    if message.chat.id != OWNER: return
    args = message.command[1:]
    if not args or not args[0].isdigit():
        return await message.reply_text("Usage: <code>/nyaa_remove 1</code>")
    eid = int(args[0])
    e = _WL.get(eid)
    if not e: return await message.reply_text(f"❌ #{eid} not found.")
    _WL.remove(eid)
    await message.reply_text(f"✅ Removed #{eid} — {e.name}")


@colab_bot.on_message(filters.command("nyaa_check") & filters.private)
async def cmd_check(client, message):
    if message.chat.id != OWNER: return
    entries = [e for e in _WL.all() if e.active]
    if not entries: return await message.reply_text("No active entries.")
    st = await message.reply_text(f"🔍 Checking {len(entries)} entries…")
    found = 0
    for e in entries:
        n = await _check_entry(e)
        found += n
        await asyncio.sleep(2)
    await st.edit_text(f"✅ Done — {found} new matches")


# ═════════════════════════════════════════════════════════════
# Poller (daily + snipe)
# ═════════════════════════════════════════════════════════════

_poller_task = None

def _ensure_poller():
    global _poller_task
    if _poller_task and not _poller_task.done(): return
    _poller_task = asyncio.get_event_loop().create_task(_poll_loop())

async def _poll_loop():
    await asyncio.sleep(30)
    while True:
        # ── Snipe mode: check entries with specific datetime every 5s ──
        for e in _WL.snipers():
            try:
                target = datetime.fromisoformat(e.snipe_at)
                now = datetime.now()
                if now >= target:
                    log.info("[Nyaa] Snipe active for '%s' — polling every 5s", e.name)
                    # Poll aggressively for 10 minutes
                    deadline = now + timedelta(minutes=10)
                    while datetime.now() < deadline:
                        n = await _check_entry(e)
                        if n > 0:
                            log.info("[Nyaa] Snipe hit for '%s'!", e.name)
                            _WL.update(e.id, snipe_done=True)
                            break
                        await asyncio.sleep(5)
                    else:
                        _WL.update(e.id, snipe_done=True)
                        try:
                            await colab_bot.send_message(
                                OWNER,
                                f"🎯 <b>Snipe expired</b> — {e.name}\n"
                                f"No match found within 10 min of {e.snipe_at}",
                            )
                        except Exception: pass
            except Exception as exc:
                log.warning("[Nyaa] Snipe error: %s", exc)

        # ── Daily entries ─────────────────────────────────────
        try:
            today = datetime.now().strftime("%A").lower()
            for e in _WL.for_day(today):
                try: await _check_entry(e)
                except Exception as exc: log.warning("[Nyaa] %s: %s", e.name, exc)
                await asyncio.sleep(3)
        except Exception as exc:
            log.error("[Nyaa] Poll: %s", exc)

        await asyncio.sleep(600)


async def _check_entry(entry: WatchEntry) -> int:
    results = await search_nyaa(entry.name)
    matched = []
    for r in results:
        if r.info_hash in entry.seen: continue
        # Title match
        nt = r.title.lower()
        if entry.uploader and entry.uploader.lower() not in nt: continue
        if entry.quality and entry.quality.lower() not in nt: continue
        name_match = any(t.lower() in nt for t in entry.titles if len(t) >= 3)
        if not name_match: continue
        matched.append(r)

    for r in matched:
        h = r.info_hash[:12] or hashlib.md5(r.title.encode()).hexdigest()[:12]
        if r.magnet: _magnets[h] = r.magnet

        text = (
            f"🔔 <b>Nyaa Match</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📺 <b>{entry.name}</b>\n"
            f"📦 <code>{r.title[:65]}</code>\n\n"
            f"💾 {r.size}  🌱 {r.seeders}  👤 {r.uploader or '—'}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🧲 Magnet", callback_data=f"nyt|m|{h}"),
             InlineKeyboardButton("📥 DL",     callback_data=f"nyt|dl|{h}")],
        ])
        try:
            await colab_bot.send_message(OWNER, text, reply_markup=kb, disable_web_page_preview=True)
        except Exception: pass
        _WL.mark_seen(entry.id, r.info_hash)

    return len(matched)


# Match notification callbacks
@colab_bot.on_callback_query(filters.regex(r"^nyt\|"))
async def nyt_cb(client, cq):
    parts = cq.data.split("|")
    action, h = parts[1], parts[2]
    uid = cq.message.chat.id
    await cq.answer()

    magnet = _magnets.get(h, "")
    if not magnet:
        return await client.send_message(uid, "❌ Magnet expired.")

    if action == "m":
        await client.send_message(uid, f"🧲 <code>{magnet}</code>")
    elif action == "dl":
        from colab_leecher.utility.variables import BOT
        BOT.SOURCE = [magnet]; BOT.Mode.ytdl = False; BOT.State.started = True
        await cq.message.edit_text(
            cq.message.text + "\n\n📥 <b>Queued for download</b>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📄 Normal", callback_data="normal"),
            ]])
        )


# Auto-start poller
if any(e.active for e in _WL.all()):
    try: _ensure_poller()
    except Exception: pass
