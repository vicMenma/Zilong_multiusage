"""
plugins/nyaa_tracker.py
Nyaa anime tracker — weekly calendar with auto-scrape + Seedr+Hardsub pipeline.

═══════════════════════════════════════════════════════════════════
FEATURES:
  • /nyaa_add <title> [day] [uploader] [quality]  — add anime to watchlist
  • /nyaa_list                                     — show tracked anime
  • /nyaa_remove <id>                              — remove from watchlist
  • /nyaa_check                                    — manual check NOW
  • /nyaa_search <query>                           — one-shot Nyaa search
  • /nyaa_dump <channel_id>                        — set dump channel for raw results
  • /nyaa_toggle <id>                              — enable/disable an entry
  • /nyaa_edit <id> <field> <value>                — edit entry field

FLOW:
  1. User adds anime via /nyaa_add with day-of-week schedule
  2. Poller runs every 10 min, checks anime scheduled for today
  3. New Nyaa results → forwarded to dump channel (all matches)
  4. If entry has auto_seedr=True + uploader matches → magnet sent to Seedr
  5. Seedr downloads → auto-hardsub pipeline (existing code)
  6. Result uploaded to user chat

TITLE MATCHING:
  When user adds an anime by English name, the bot queries AniList
  to get romaji + native (Japanese) + synonyms. ALL are stored and
  used for matching against Nyaa titles (which may be in any language).

DUPLICATE PREVENTION:
  Seen info_hashes are stored per-entry. A torrent is only processed once.
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
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
# Persistent data
# ─────────────────────────────────────────────────────────────

_DATA_DIR  = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data"))
_STORE_PATH = os.path.join(_DATA_DIR, "nyaa_watchlist.json")
_CONFIG_PATH = os.path.join(_DATA_DIR, "nyaa_config.json")

DAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
DAY_ALIASES = {
    "mon": "monday", "tue": "tuesday", "wed": "wednesday",
    "thu": "thursday", "fri": "friday", "sat": "saturday", "sun": "sunday",
    "mo": "monday", "tu": "tuesday", "we": "wednesday",
    "th": "thursday", "fr": "friday", "sa": "saturday", "su": "sunday",
}


@dataclass
class WatchlistEntry:
    id:             int
    display_name:   str                    # user-friendly name for display
    titles:         list[str] = field(default_factory=list)  # all known titles (EN + JP + romaji)
    anilist_id:     int       = 0
    day:            str       = ""         # "monday" .. "sunday" or "" for daily
    uploader:       str       = ""         # filter: "Tsundere Raws", "SubsPlease", etc.
    quality:        str       = "1080p"    # filter: "1080p", "720p", ""
    category:       str       = "1_2"      # Nyaa category: 1_2=English, 1_4=Raw, 1_0=All
    auto_seedr:     bool      = True       # auto-send matching magnets to Seedr
    auto_hardsub:   bool      = False      # auto-hardsub after Seedr (requires sub)
    active:         bool      = True
    seen_hashes:    list[str] = field(default_factory=list)  # already-processed info_hashes
    last_check:     float     = 0.0
    last_match:     float     = 0.0
    added_at:       float     = field(default_factory=time.time)


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
            for eid_s, d in raw.get("entries", {}).items():
                try:
                    entry = WatchlistEntry(**d)
                    self._entries[entry.id] = entry
                except TypeError:
                    pass
            self._next_id = raw.get("next_id", max(self._entries.keys(), default=0) + 1)
            log.info("[NyaaTracker] Loaded %d watchlist entries", len(self._entries))
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
                # Keep only last 200 hashes per entry
                if len(e.seen_hashes) > 200:
                    e.seen_hashes = e.seen_hashes[-200:]
                e.last_match = time.time()
                self._save()

    def get(self, eid: int) -> Optional[WatchlistEntry]:
        return self._entries.get(eid)

    def all_entries(self) -> list[WatchlistEntry]:
        return sorted(self._entries.values(), key=lambda e: e.id)

    def entries_for_day(self, day: str) -> list[WatchlistEntry]:
        """Return active entries scheduled for the given day (or daily entries)."""
        day = day.lower()
        return [
            e for e in self._entries.values()
            if e.active and (e.day == day or e.day == "" or e.day == "daily")
        ]


watchlist = WatchlistStore()


# ─────────────────────────────────────────────────────────────
# Config (dump channel, poll interval)
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
        log.warning("[NyaaTracker] Config load error: %s", e)


def _save_config():
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(_config, f, indent=2)
    except Exception as e:
        log.warning("[NyaaTracker] Config save error: %s", e)


_load_config()


# ─────────────────────────────────────────────────────────────
# Poller
# ─────────────────────────────────────────────────────────────

_poller_task: Optional[asyncio.Task] = None


def _ensure_poller():
    global _poller_task
    if _poller_task and not _poller_task.done():
        return
    _poller_task = asyncio.create_task(_poll_loop())
    log.info("[NyaaTracker] Poller started (interval=%ds)", _config.get("poll_interval", 600))


async def _poll_loop():
    """Main polling loop — runs every poll_interval seconds."""
    # Initial delay to let the bot fully start
    await asyncio.sleep(30)

    while True:
        try:
            await _check_all_today()
        except Exception as exc:
            log.error("[NyaaTracker] Poll error: %s", exc, exc_info=True)

        interval = _config.get("poll_interval", 600)
        await asyncio.sleep(interval)


async def _check_all_today():
    """Check all anime scheduled for today."""
    import datetime
    today = datetime.datetime.now().strftime("%A").lower()
    entries = watchlist.entries_for_day(today)

    if not entries:
        return

    log.info("[NyaaTracker] Checking %d entries for %s", len(entries), today)

    for entry in entries:
        try:
            await _check_entry(entry)
        except Exception as exc:
            log.error("[NyaaTracker] Check failed for '%s': %s",
                      entry.display_name, exc, exc_info=True)
        # Small delay between entries to avoid hammering Nyaa
        await asyncio.sleep(3)


async def _check_entry(entry: WatchlistEntry):
    """Check a single watchlist entry against Nyaa."""
    from services.nyaa import search_nyaa, match_title, extract_episode

    # Search using the primary display name first
    search_terms = [entry.display_name]
    # Also try romaji if different from display name
    for t in entry.titles:
        norm_t = t.lower().strip()
        if norm_t not in [s.lower().strip() for s in search_terms]:
            search_terms.append(t)
            if len(search_terms) >= 3:
                break

    all_results = []
    seen_hashes: set = set()

    for term in search_terms:
        results = await search_nyaa(
            query=term,
            category=entry.category,
            filter_=0,
            sort="id",
            order="desc",
        )
        for r in results:
            if r.info_hash and r.info_hash not in seen_hashes:
                seen_hashes.add(r.info_hash)
                all_results.append(r)
        await asyncio.sleep(1)  # Nyaa rate limit courtesy

    await watchlist.update(entry.id, last_check=time.time())

    if not all_results:
        return

    # Filter: match title + uploader + quality
    matched = []
    for r in all_results:
        # Skip already-seen torrents
        if r.info_hash in entry.seen_hashes:
            continue
        if match_title(r.title, entry.titles, entry.uploader, entry.quality):
            matched.append(r)

    if not matched:
        return

    log.info("[NyaaTracker] %d new match(es) for '%s'", len(matched), entry.display_name)

    from core.session import get_client
    client = get_client()
    dump_ch = _config.get("dump_channel", 0)

    for r in matched:
        ep = extract_episode(r.title)
        ep_s = f"Ep {ep}" if ep else "Batch/Unknown"

        # ── Send to dump channel ──────────────────────────────
        if dump_ch:
            try:
                dump_text = (
                    f"🔔 <b>Nyaa Match</b>\n"
                    f"──────────────────────\n\n"
                    f"📺 <b>{entry.display_name}</b>  ({ep_s})\n"
                    f"📦 <code>{r.title[:80]}</code>\n\n"
                    f"💾 {r.size}  ·  🌱 {r.seeders}  ·  📥 {r.downloads}\n"
                    f"👤 {r.uploader or 'Unknown'}\n"
                    f"🔗 {r.link}\n\n"
                    f"{'🟢 Auto-Seedr ON' if entry.auto_seedr else '⚪ Manual'}"
                )
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        "🔥 Send to Seedr+Hardsub",
                        callback_data=f"nyt|seedr|{entry.id}|{r.info_hash[:16]}",
                    )],
                    [InlineKeyboardButton(
                        "📥 Download Locally",
                        callback_data=f"nyt|local|{entry.id}|{r.info_hash[:16]}",
                    )],
                    [InlineKeyboardButton(
                        "❌ Skip",
                        callback_data=f"nyt|skip|{entry.id}|{r.info_hash[:16]}",
                    )],
                ])
                await client.send_message(
                    dump_ch, dump_text,
                    parse_mode=enums.ParseMode.HTML,
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
            except Exception as exc:
                log.warning("[NyaaTracker] Dump channel send failed: %s", exc)

        # ── Also notify owner in private ──────────────────────
        try:
            owner_text = (
                f"🔔 <b>New episode detected!</b>\n\n"
                f"📺 <b>{entry.display_name}</b>  ({ep_s})\n"
                f"📦 <code>{r.title[:60]}</code>\n"
                f"💾 {r.size}  🌱 {r.seeders}\n"
                f"👤 {r.uploader or '?'}\n\n"
            )
            if entry.auto_seedr:
                owner_text += "🟢 <i>Auto-sending to Seedr…</i>"
            else:
                owner_text += "⚪ <i>Manual mode — use buttons in dump channel.</i>"

            await client.send_message(
                cfg.owner_id, owner_text,
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

        # ── Auto-Seedr pipeline ───────────────────────────────
        if entry.auto_seedr and r.magnet:
            # Store magnet temporarily for callback access
            _pending_magnets[r.info_hash[:16]] = r.magnet
            asyncio.create_task(_auto_seedr_pipeline(client, entry, r))

        # Mark as seen
        await watchlist.mark_seen(entry.id, r.info_hash)


# Temporary magnet storage for callback buttons
_pending_magnets: dict[str, str] = {}


async def _auto_seedr_pipeline(client, entry: WatchlistEntry, nyaa_entry):
    """Auto-send to Seedr and optionally trigger hardsub."""
    from services.nyaa import NyaaEntry

    log.info("[NyaaTracker] Auto-Seedr: %s → %s",
             entry.display_name, nyaa_entry.title[:50])

    try:
        # Check Seedr credentials
        username = os.environ.get("SEEDR_USERNAME", "").strip()
        password = os.environ.get("SEEDR_PASSWORD", "").strip()
        if not username or not password:
            await client.send_message(
                cfg.owner_id,
                f"⚠️ <b>Seedr credentials missing</b>\n"
                f"Cannot auto-download: {entry.display_name}",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # Send status to owner
        st = await client.send_message(
            cfg.owner_id,
            f"☁️ <b>Auto-Seedr Download</b>\n"
            f"──────────────────────\n\n"
            f"📺 <b>{entry.display_name}</b>\n"
            f"📦 <code>{nyaa_entry.title[:50]}</code>\n\n"
            f"⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        # Use the Seedr download pipeline from url_handler
        from services.seedr import download_via_seedr
        from services.utils import make_tmp, cleanup, human_size
        from services.uploader import upload_file

        tmp = make_tmp(cfg.download_dir, cfg.owner_id)

        async def _progress(stage, pct, detail):
            icons = {"adding": "⬆️", "waiting": "⏳", "downloading": "☁️",
                     "fetching": "🔗", "dl_file": "⬇️"}
            icon = icons.get(stage, "⏳")
            bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
            try:
                await st.edit(
                    f"☁️ <b>Auto-Seedr: {entry.display_name}</b>\n"
                    f"──────────────────────\n\n"
                    f"{icon} <i>{detail}</i>\n"
                    f"<code>[{bar}]</code>  <b>{pct:.0f}%</b>",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception:
                pass

        local_paths = await download_via_seedr(
            nyaa_entry.magnet, tmp,
            progress_cb=_progress, timeout_s=7200,
        )

        if not local_paths:
            await st.edit(
                f"❌ <b>Seedr: no files downloaded</b>\n{entry.display_name}",
                parse_mode=enums.ParseMode.HTML,
            )
            cleanup(tmp)
            return

        # Upload all files
        for i, fpath in enumerate(local_paths, 1):
            fsize = os.path.getsize(fpath)
            if fsize > cfg.file_limit_b:
                await client.send_message(
                    cfg.owner_id,
                    f"⚠️ Skipped (too large): <code>{os.path.basename(fpath)}</code>\n"
                    f"<code>{human_size(fsize)}</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
                continue

            upload_st = await client.send_message(
                cfg.owner_id,
                f"📤 <b>Uploading {i}/{len(local_paths)}</b>\n"
                f"<code>{os.path.basename(fpath)[:50]}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            await upload_file(client, upload_st, fpath, user_id=cfg.owner_id)

        try:
            await st.delete()
        except Exception:
            pass

        cleanup(tmp)
        log.info("[NyaaTracker] Auto-Seedr complete: %s", entry.display_name)

    except Exception as exc:
        log.error("[NyaaTracker] Auto-Seedr failed: %s", exc, exc_info=True)
        try:
            await client.send_message(
                cfg.owner_id,
                f"❌ <b>Auto-Seedr failed</b>\n"
                f"{entry.display_name}\n\n"
                f"<code>{str(exc)[:200]}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.command("nyaa_add") & filters.user(cfg.owner_id))
async def cmd_nyaa_add(client: Client, msg: Message):
    """
    /nyaa_add <title> | [day] | [uploader] | [quality]

    Examples:
      /nyaa_add Oshi no Ko | wednesday | SubsPlease | 1080p
      /nyaa_add Dandadan | thursday
      /nyaa_add 推しの子 | wed | Tsundere Raws
      /nyaa_add One Piece
    """
    text = msg.text.split(None, 1)
    if len(text) < 2:
        return await msg.reply(
            "📡 <b>Nyaa Tracker — Add Anime</b>\n\n"
            "Usage:\n"
            "<code>/nyaa_add Title | day | uploader | quality</code>\n\n"
            "Examples:\n"
            "<code>/nyaa_add Oshi no Ko | wednesday | SubsPlease | 1080p</code>\n"
            "<code>/nyaa_add Dandadan | thu</code>\n"
            "<code>/nyaa_add 推しの子 | wed | Tsundere Raws</code>\n\n"
            "Only title is required. Day defaults to daily.\n"
            "Uploader filters by [Group] tag in Nyaa titles.",
            parse_mode=enums.ParseMode.HTML,
        )

    parts = text[1].split("|")
    title    = parts[0].strip()
    day      = parts[1].strip().lower() if len(parts) > 1 else ""
    uploader = parts[2].strip() if len(parts) > 2 else ""
    quality  = parts[3].strip() if len(parts) > 3 else "1080p"

    # Normalize day
    if day in DAY_ALIASES:
        day = DAY_ALIASES[day]
    elif day and day not in DAYS and day != "daily":
        return await msg.reply(
            f"❌ Invalid day: <code>{day}</code>\n\n"
            f"Valid: {', '.join(DAYS)} or daily",
            parse_mode=enums.ParseMode.HTML,
        )

    st = await msg.reply(
        f"🔍 <b>Resolving titles for:</b> <code>{title}</code>\n"
        f"<i>Querying AniList for all known names…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # ── Resolve all titles via AniList ────────────────────────
    from services.anilist import search_anime, all_titles

    all_title_list = [title]  # always include user's input
    anilist_id = 0

    try:
        results = await search_anime(title)
        if results:
            best = results[0]
            anilist_id = best.get("id", 0)
            resolved = all_titles(best)
            # Merge with user input (dedup)
            seen_lower = {title.lower()}
            for t in resolved:
                if t.lower() not in seen_lower:
                    seen_lower.add(t.lower())
                    all_title_list.append(t)

            title_preview = "\n".join(f"  • <code>{t}</code>" for t in all_title_list[:8])
            await st.edit(
                f"✅ <b>AniList resolved {len(all_title_list)} title(s)</b>\n\n"
                f"{title_preview}\n\n"
                f"<i>Adding to watchlist…</i>",
                parse_mode=enums.ParseMode.HTML,
            )
        else:
            await st.edit(
                f"⚠️ <b>AniList returned no results</b>\n"
                f"Using only: <code>{title}</code>\n\n"
                f"<i>Tip: try the official English or romaji title.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
    except Exception as exc:
        log.warning("[NyaaTracker] AniList lookup failed: %s", exc)
        await st.edit(
            f"⚠️ <b>AniList lookup failed</b> — using title as-is.\n"
            f"<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    # ── Create entry ──────────────────────────────────────────
    entry = WatchlistEntry(
        id=0,
        display_name=title,
        titles=all_title_list,
        anilist_id=anilist_id,
        day=day or "daily",
        uploader=uploader,
        quality=quality,
        category="1_2",
        auto_seedr=True,
    )
    eid = await watchlist.add(entry)

    _ensure_poller()

    day_display = entry.day.capitalize() if entry.day != "daily" else "Daily"
    await st.edit(
        f"✅ <b>Added to Nyaa Watchlist</b>  (#{eid})\n"
        f"──────────────────────\n\n"
        f"📺 <b>{title}</b>\n"
        f"📅 {day_display}\n"
        f"👤 Uploader: <code>{uploader or 'Any'}</code>\n"
        f"📐 Quality: <code>{quality or 'Any'}</code>\n"
        f"🔑 Titles: {len(all_title_list)} known aliases\n"
        f"☁️ Auto-Seedr: ✅\n\n"
        f"<i>The poller will check Nyaa automatically.\n"
        f"Use /nyaa_check to trigger now.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("nyaa_list") & filters.user(cfg.owner_id))
async def cmd_nyaa_list(client: Client, msg: Message):
    entries = watchlist.all_entries()
    if not entries:
        return await msg.reply(
            "📡 <b>Nyaa Watchlist</b>\n\n<i>Empty — use /nyaa_add to start tracking.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    lines = ["📡 <b>Nyaa Watchlist</b>", "──────────────────────", ""]
    for e in entries:
        status = "🟢" if e.active else "🔴"
        day_s  = e.day.capitalize() if e.day != "daily" else "Daily"
        seedr  = "☁️" if e.auto_seedr else ""
        up_s   = f"[{e.uploader}]" if e.uploader else ""
        lines.append(
            f"{status} <b>#{e.id}</b>  <code>{e.display_name[:30]}</code>\n"
            f"   📅 {day_s}  📐 {e.quality}  {up_s} {seedr}\n"
            f"   🔑 {len(e.titles)} aliases  📦 {len(e.seen_hashes)} seen"
        )
        lines.append("")

    dump_ch = _config.get("dump_channel", 0)
    lines.append(f"📢 Dump channel: <code>{dump_ch or 'Not set'}</code>")
    lines.append(f"⏱ Poll interval: <code>{_config.get('poll_interval', 600)}s</code>")

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
    ok = await watchlist.remove(eid)
    if ok:
        await msg.reply(
            f"✅ Removed <b>#{eid}</b> — <code>{entry.display_name}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        await msg.reply("❌ Failed to remove.")


@Client.on_message(filters.command("nyaa_toggle") & filters.user(cfg.owner_id))
async def cmd_nyaa_toggle(client: Client, msg: Message):
    args = msg.command[1:]
    if not args or not args[0].isdigit():
        return await msg.reply("Usage: <code>/nyaa_toggle &lt;id&gt;</code>",
                               parse_mode=enums.ParseMode.HTML)
    eid = int(args[0])
    entry = watchlist.get(eid)
    if not entry:
        return await msg.reply(f"❌ Entry #{eid} not found.")
    new_state = not entry.active
    await watchlist.update(eid, active=new_state)
    icon = "🟢" if new_state else "🔴"
    await msg.reply(
        f"{icon} <b>#{eid}</b> {entry.display_name} — "
        f"{'Enabled' if new_state else 'Disabled'}",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("nyaa_dump") & filters.user(cfg.owner_id))
async def cmd_nyaa_dump(client: Client, msg: Message):
    """
    /nyaa_dump <channel_id_or_username>
    /nyaa_dump 0  — disable dump channel
    """
    args = msg.command[1:]
    if not args:
        cur = _config.get("dump_channel", 0)
        return await msg.reply(
            f"📢 <b>Dump Channel</b>\n\n"
            f"Current: <code>{cur or 'Not set'}</code>\n\n"
            f"Set with: <code>/nyaa_dump @channel</code> or <code>/nyaa_dump -100123456</code>\n"
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
        await msg.reply(
            f"✅ Dump channel set to <code>{ch_id}</code>\n"
            f"All Nyaa matches will be forwarded there.",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        await msg.reply(f"❌ Could not resolve channel: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_check") & filters.user(cfg.owner_id))
async def cmd_nyaa_check(client: Client, msg: Message):
    """Manual trigger — check ALL active entries now."""
    entries = [e for e in watchlist.all_entries() if e.active]
    if not entries:
        return await msg.reply("📡 No active entries to check.")

    st = await msg.reply(
        f"🔍 <b>Checking {len(entries)} entries…</b>",
        parse_mode=enums.ParseMode.HTML,
    )

    _ensure_poller()

    found = 0
    for i, entry in enumerate(entries, 1):
        try:
            from services.nyaa import search_nyaa, match_title

            # Search with primary title
            results = await search_nyaa(entry.display_name, category=entry.category)
            new_matches = []
            for r in results:
                if r.info_hash in entry.seen_hashes:
                    continue
                if match_title(r.title, entry.titles, entry.uploader, entry.quality):
                    new_matches.append(r)

            if new_matches:
                found += len(new_matches)
                # Process through normal pipeline
                for r in new_matches:
                    _pending_magnets[r.info_hash[:16]] = r.magnet
                await _check_entry(entry)

            try:
                await st.edit(
                    f"🔍 Checking {i}/{len(entries)}…  Found: {found}",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception:
                pass

        except Exception as exc:
            log.warning("[NyaaTracker] Check failed for %s: %s", entry.display_name, exc)

        await asyncio.sleep(2)

    await st.edit(
        f"✅ <b>Check complete</b>\n\n"
        f"Entries checked: {len(entries)}\n"
        f"New matches: {found}",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command("nyaa_search") & filters.user(cfg.owner_id))
async def cmd_nyaa_search(client: Client, msg: Message):
    """One-shot Nyaa search — /nyaa_search <query>"""
    query = " ".join(msg.command[1:])
    if not query:
        return await msg.reply("Usage: <code>/nyaa_search Oshi no Ko 1080p</code>",
                               parse_mode=enums.ParseMode.HTML)

    st = await msg.reply(f"🔍 Searching Nyaa for: <code>{query}</code>…",
                         parse_mode=enums.ParseMode.HTML)

    from services.nyaa import search_nyaa

    results = await search_nyaa(query, category="1_0")  # All anime
    if not results:
        return await st.edit("❌ No results found.")

    lines = [f"📡 <b>Nyaa Search: {query[:30]}</b>", "──────────────────────", ""]
    for i, r in enumerate(results[:15], 1):
        lines.append(
            f"<b>{i}.</b> <code>{r.title[:55]}</code>\n"
            f"   💾 {r.size}  🌱 {r.seeders}  📥 {r.downloads}  👤 {r.uploader or '?'}"
        )
        lines.append("")

    if len(results) > 15:
        lines.append(f"<i>…and {len(results) - 15} more</i>")

    await st.edit("\n".join(lines)[:4000], parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("nyaa_edit") & filters.user(cfg.owner_id))
async def cmd_nyaa_edit(client: Client, msg: Message):
    """
    /nyaa_edit <id> <field> <value>
    Fields: day, uploader, quality, category, auto_seedr
    """
    args = msg.command[1:]
    if len(args) < 3:
        return await msg.reply(
            "Usage: <code>/nyaa_edit &lt;id&gt; &lt;field&gt; &lt;value&gt;</code>\n\n"
            "Fields: day, uploader, quality, category, auto_seedr\n\n"
            "Examples:\n"
            "<code>/nyaa_edit 1 day thursday</code>\n"
            "<code>/nyaa_edit 2 uploader Tsundere Raws</code>\n"
            "<code>/nyaa_edit 3 auto_seedr true</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    eid = int(args[0]) if args[0].isdigit() else 0
    field_name = args[1].lower()
    value = " ".join(args[2:])

    entry = watchlist.get(eid)
    if not entry:
        return await msg.reply(f"❌ Entry #{eid} not found.")

    valid_fields = {"day", "uploader", "quality", "category", "auto_seedr", "auto_hardsub"}
    if field_name not in valid_fields:
        return await msg.reply(f"❌ Unknown field: {field_name}\nValid: {', '.join(valid_fields)}")

    # Type conversion
    if field_name == "auto_seedr" or field_name == "auto_hardsub":
        value = value.lower() in ("true", "1", "yes", "on")
    elif field_name == "day":
        value = value.lower()
        if value in DAY_ALIASES:
            value = DAY_ALIASES[value]
        if value not in DAYS and value != "daily":
            return await msg.reply(f"❌ Invalid day: {value}")

    await watchlist.update(eid, **{field_name: value})
    await msg.reply(
        f"✅ <b>#{eid}</b> updated: <code>{field_name} = {value}</code>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# Callback handler for dump channel buttons
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^nyt\|"))
async def nyaa_tracker_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 4:
        return await cb.answer("Invalid data.", show_alert=True)

    _, action, eid_s, hash_short = parts
    uid = cb.from_user.id
    await cb.answer()

    if action == "skip":
        try:
            await cb.message.edit(
                cb.message.text.html + "\n\n❌ <b>Skipped</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    magnet = _pending_magnets.get(hash_short, "")
    if not magnet:
        return await cb.message.edit(
            cb.message.text.html + "\n\n❌ <b>Magnet expired — resend link manually.</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    if action == "seedr":
        try:
            await cb.message.edit(
                cb.message.text.html + "\n\n☁️ <b>Sending to Seedr…</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

        # Trigger the Seedr pipeline via url_handler
        from plugins.url_handler import _seedr_download, _store
        token = _store(magnet)
        st = await client.send_message(
            uid,
            "☁️ <b>Seedr Cloud Download</b>\n──────────────────────\n\n"
            "⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_seedr_download(client, st, magnet, uid))

    elif action == "local":
        try:
            await cb.message.edit(
                cb.message.text.html + "\n\n📥 <b>Starting local download…</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

        from plugins.url_handler import _launch_download
        st = await client.send_message(
            uid,
            "📥 <b>Downloading…</b>\n<i>Local aria2c download</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_launch_download(client, st, magnet, uid))


# ─────────────────────────────────────────────────────────────
# Auto-start poller if watchlist has entries
# ─────────────────────────────────────────────────────────────

if watchlist.all_entries():
    try:
        # Deferred start — poller will be created when event loop is running
        import atexit
        _poller_scheduled = True
    except Exception:
        pass


# Called from main.py after bot starts
def start_nyaa_poller():
    """Called from main.py to start the poller if there are active entries."""
    if any(e.active for e in watchlist.all_entries()):
        _ensure_poller()
        log.info("[NyaaTracker] Poller auto-started (%d active entries)",
                 sum(1 for e in watchlist.all_entries() if e.active))
