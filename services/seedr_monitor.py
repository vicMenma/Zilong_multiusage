"""
services/seedr_monitor.py
Background monitor — polls Seedr for newly completed folders and
automatically initiates the CloudConvert hardsub pipeline.

FLOW
────
1.  Poll Seedr root every POLL_ACTIVE (60 s) or POLL_IDLE (300 s).
2.  Compare folder list against known IDs persisted to
    data/seedr_known_folders.json — only NEW folders are processed.
3.  For each new folder:
    a.  Download first 50 MB (probe portion) via aiohttp.
    b.  Probe streams with ffprobe.
    c.  French text subtitle found →
          extract from full CDN URL → _auto_hardsub_url() (seedr_hardsub.py)
    d.  No suitable French sub →
          send notification WITH inline buttons:
            [🔥 I'll send a subtitle]  [📤 Upload as-is]  [❌ Skip]
          • "I'll send a subtitle" → populates _WAITING_SUB so existing
            seedr_hardsub subtitle-file/URL handlers fire transparently.
          • "Upload as-is" → downloads full file, uploads to DM, no hardsub.
          • "Skip" → marks folder as processed, cleans up tmp.

PERSISTENCE
───────────
data/seedr_known_folders.json  →  {"known": [...], "processed": [...]}

CALLBACK REGISTRATION
─────────────────────
The inline button callbacks (smon|*) are registered in
plugins/seedr_hardsub.py (auto-loaded by Pyrogram) so no manual wiring
in main.py is needed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

log = logging.getLogger(__name__)

# ── Paths & intervals ────────────────────────────────────────────────────────
_DATA_DIR   = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data"))
_STORE_PATH = os.path.join(_DATA_DIR, "seedr_known_folders.json")

POLL_ACTIVE = 60    # s — used for 10 min after any new folder
POLL_IDLE   = 300   # s — when idle

# ── File types & subtitle codec sets ────────────────────────────────────────
_VIDEO_EXTS = frozenset({
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
    ".ts", ".m2ts", ".wmv", ".m4v",
})
_FRENCH_CODES      = frozenset({"fr", "fra", "fre"})
_TEXT_SUB_CODECS   = frozenset({
    "ass", "ssa", "subrip", "srt", "webvtt", "vtt",
    "mov_text", "text", "microdvd",
})
_BITMAP_SUB_CODECS = frozenset({
    "hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle",
    "pgssub", "dvdsub",
})

# ── Module state ─────────────────────────────────────────────────────────────
_monitor_task:  Optional[asyncio.Task] = None
_known_ids:     set = set()
_processed_ids: set = set()

# Pending "upload-as-is" jobs: folder_id → {video_url, fname, fsize, tmp, user, pwd}
_pending_upload: dict[int, dict] = {}


# ══════════════════════════════════════════════════════════════════════════════
# Persistence helpers
# ══════════════════════════════════════════════════════════════════════════════

def _load_store() -> None:
    global _known_ids, _processed_ids
    try:
        with open(_STORE_PATH, encoding="utf-8") as f:
            d = json.load(f)
        _known_ids     = set(d.get("known",     []))
        _processed_ids = set(d.get("processed", []))
        log.info("[SeedrMon] Loaded %d known / %d processed",
                 len(_known_ids), len(_processed_ids))
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning("[SeedrMon] Store load: %s", exc)


def _save_store() -> None:
    try:
        os.makedirs(_DATA_DIR, exist_ok=True)
        with open(_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "known":     sorted(_known_ids),
                "processed": sorted(_processed_ids),
            }, f, indent=2)
    except Exception as exc:
        log.warning("[SeedrMon] Store save: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# Message helper
# ══════════════════════════════════════════════════════════════════════════════

async def _edit_or_new(msg, uid: int, text: str, reply_markup=None) -> object:
    """Edit `msg` if possible, else send fresh message to uid."""
    from pyrogram import enums as _pe
    kwargs = {"parse_mode": _pe.ParseMode.HTML}
    if reply_markup:
        kwargs["reply_markup"] = reply_markup
    try:
        await msg.edit(text, **kwargs)
        return msg
    except Exception:
        pass
    try:
        from core.session import get_client
        return await get_client().send_message(uid, text, **kwargs)
    except Exception as exc:
        log.error("[SeedrMon] Cannot deliver message uid=%d: %s", uid, exc)
        return msg


# ══════════════════════════════════════════════════════════════════════════════
# Public: "upload as-is" handler (called by the callback button)
# ══════════════════════════════════════════════════════════════════════════════

async def handle_upload_as_is(folder_id: int, uid: int, client) -> None:
    """
    Download the full Seedr CDN file and upload it to the user's DM.
    Called when the user clicks "📤 Upload as-is" on the monitor notification.
    """
    from pyrogram import enums as _pe
    from services.downloader import download_direct
    from services.uploader   import upload_file
    from services.utils      import make_tmp, cleanup, human_size
    from core.config         import cfg

    info = _pending_upload.pop(folder_id, None)
    if not info:
        try:
            await client.send_message(
                uid,
                "⚠️ <b>Session expired</b> — folder data is no longer available.",
                parse_mode=_pe.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    video_url = info["video_url"]
    fname     = info["fname"]
    fsize     = info["fsize"]
    tmp       = info["tmp"]
    folder_id_int = info.get("folder_id", folder_id)
    seedr_user    = info.get("seedr_user", "")
    seedr_pwd     = info.get("seedr_pwd",  "")

    try:
        st = await client.send_message(
            uid,
            f"⬇️ <b>Downloading from Seedr CDN…</b>\n\n"
            f"📁 <code>{fname[:45]}</code>\n"
            f"💾 <code>{human_size(fsize)}</code>",
            parse_mode=_pe.ParseMode.HTML,
        )
        local_path = await download_direct(video_url, tmp)
        await upload_file(client, st, local_path, user_id=uid)
    except Exception as exc:
        log.error("[SeedrMon] upload_as_is failed: %s", exc)
        try:
            await client.send_message(
                uid,
                f"❌ <b>Upload failed</b>\n<code>{str(exc)[:200]}</code>",
                parse_mode=_pe.ParseMode.HTML,
            )
        except Exception:
            pass
    finally:
        cleanup(tmp)
        # Clean up the Seedr folder to reclaim quota
        if seedr_user and folder_id_int:
            try:
                from services.seedr import _del_folder
                await _del_folder(seedr_user, seedr_pwd, folder_id_int)
                log.info("[SeedrMon] Seedr folder %d cleaned", folder_id_int)
            except Exception as exc:
                log.warning("[SeedrMon] Seedr cleanup: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# Core: process one new folder
# ══════════════════════════════════════════════════════════════════════════════

async def _process_new_folder(
    folder:     dict,
    seedr_user: str,
    seedr_pwd:  str,
    owner_uid:  int,
) -> None:
    import aiohttp as _ah
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    from core.config    import cfg
    from services       import ffmpeg as FF
    from services.seedr import _collect_files
    from services.utils import human_size, lang_flag, lang_name, make_tmp, cleanup

    folder_id   = folder["id"]
    folder_name = folder.get("name", f"folder_{folder_id}")
    log.info("[SeedrMon] Processing '%s' (id=%d)", folder_name, folder_id)

    # ── Collect CDN URLs ──────────────────────────────────────────────────────
    try:
        files = await _collect_files(seedr_user, seedr_pwd, folder_id)
    except Exception as exc:
        log.error("[SeedrMon] collect_files: %s", exc)
        return

    if not files:
        log.warning("[SeedrMon] No files in folder %d", folder_id)
        return

    video_files = [f for f in files
                   if os.path.splitext(f["name"])[1].lower() in _VIDEO_EXTS]
    target = max(video_files or files, key=lambda f: f.get("size", 0))

    fname     = target.get("clean_name") or target["name"]
    fsize     = target.get("size", 0)
    video_url = target["url"]

    # ── Send initial notification ─────────────────────────────────────────────
    from core.session import get_client
    from pyrogram import enums as _pe

    try:
        notify_msg = await get_client().send_message(
            owner_uid,
            f"🔍 <b>New Seedr folder detected</b>\n"
            "──────────────────────\n\n"
            f"📁 <b>{folder_name}</b>\n"
            f"🎬 <code>{fname[:45]}</code>\n"
            f"💾 <code>{human_size(fsize)}</code>\n\n"
            "<i>Probing for French subtitle…</i>",
            parse_mode=_pe.ParseMode.HTML,
        )
    except Exception as exc:
        log.error("[SeedrMon] Initial notify: %s", exc)
        return

    # ── Partial download (50 MB) for ffprobe ─────────────────────────────────
    tmp        = make_tmp(cfg.download_dir, owner_uid)
    probe_path = video_url   # fallback: probe URL directly

    try:
        _dst     = os.path.join(tmp, fname)
        _MAX     = 50 * 1024 * 1024
        _timeout = _ah.ClientTimeout(total=300)
        async with _ah.ClientSession(timeout=_timeout) as sess:
            async with sess.get(
                video_url,
                headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True,
            ) as resp:
                resp.raise_for_status()
                _done = 0
                with open(_dst, "wb") as fh:
                    async for chunk in resp.content.iter_chunked(1 * 1024 * 1024):
                        fh.write(chunk)
                        _done += len(chunk)
                        if _done >= _MAX:
                            break
        probe_path = _dst
        log.info("[SeedrMon] Probe: %s bytes → %s", _done, fname)
    except Exception as exc:
        log.warning("[SeedrMon] Partial probe failed (%s) — probing URL", exc)

    # ── Probe streams ─────────────────────────────────────────────────────────
    try:
        sd = await FF.probe_streams(probe_path)
    except Exception as exc:
        cleanup(tmp)
        await _edit_or_new(
            notify_msg, owner_uid,
            f"❌ <b>Stream probe failed</b>\n\n<code>{exc}</code>",
        )
        return

    all_subs      = sd.get("subtitle", [])
    french_text   = [
        s for s in all_subs
        if (s.get("tags") or {}).get("language", "und").lower() in _FRENCH_CODES
        and (s.get("codec_name") or "").lower() in _TEXT_SUB_CODECS
    ]
    french_bitmap = [
        s for s in all_subs
        if (s.get("tags") or {}).get("language", "und").lower() in _FRENCH_CODES
        and (s.get("codec_name") or "").lower() in _BITMAP_SUB_CODECS
    ]

    log.info("[SeedrMon] Streams: %d video / %d audio / %d sub "
             "(%d FR-text, %d FR-bitmap)",
             len(sd.get("video", [])), len(sd.get("audio", [])), len(all_subs),
             len(french_text), len(french_bitmap))

    # ── French text sub found → auto-hardsub ─────────────────────────────────
    if french_text:
        best_sub = french_text[0]
        codec    = (best_sub.get("codec_name") or "ass").upper()
        tags     = best_sub.get("tags") or {}
        detail   = f"#{best_sub.get('index','?')} {codec}"
        if tags.get("title"):
            detail += f" — {tags['title']}"

        await _edit_or_new(
            notify_msg, owner_uid,
            f"✅ <b>French subtitle found — auto-hardsub</b>\n"
            "──────────────────────\n\n"
            f"🎬 <code>{fname[:45]}</code>\n"
            f"💬 <code>{detail}</code>\n\n"
            "<i>Extracting and submitting to CloudConvert…</i>",
        )

        try:
            from plugins.seedr_hardsub import _auto_hardsub_url
            await _auto_hardsub_url(
                get_client(), notify_msg,
                probe_path,    # local partial file — sub extraction only
                video_url,     # full Seedr CDN URL sent directly to CC
                fname, fsize, best_sub, tmp,
                owner_uid, folder_id, seedr_user, seedr_pwd,
            )
        except Exception as exc:
            log.error("[SeedrMon] auto_hardsub_url: %s", exc, exc_info=True)
            cleanup(tmp)
            await _edit_or_new(
                notify_msg, owner_uid,
                f"❌ <b>Auto-hardsub failed</b>\n\n<code>{str(exc)[:300]}</code>",
            )
            return

        _processed_ids.add(folder_id)
        _save_store()
        return

    # ── No suitable French sub → ask user via BUTTONS ────────────────────────
    # Store context so buttons can trigger the right action
    _pending_upload[folder_id] = {
        "video_url":  video_url,
        "fname":      fname,
        "fsize":      fsize,
        "tmp":        tmp,
        "folder_id":  folder_id,
        "seedr_user": seedr_user,
        "seedr_pwd":  seedr_pwd,
        "_created":   time.time(),
    }

    # Also populate _WAITING_SUB so existing subtitle file/URL handlers fire
    try:
        from plugins.seedr_hardsub import _WAITING_SUB, _evict_waiting_subs
        _evict_waiting_subs()
        _WAITING_SUB[owner_uid] = {
            "video_url":   video_url,
            "video_path":  probe_path if os.path.isfile(str(probe_path)) else None,
            "fname":       fname,
            "fsize":       fsize,
            "tmp":         tmp,
            "folder_id":   folder_id,
            "seedr_user":  seedr_user,
            "seedr_pwd":   seedr_pwd,
            "_created":    time.time(),
        }
    except Exception as exc:
        log.error("[SeedrMon] _WAITING_SUB: %s", exc)
        cleanup(tmp)
        return

    # Build subtitle list
    sub_lines = []
    for s in all_subs:
        tags   = s.get("tags") or {}
        lang   = (tags.get("language") or "und").lower()
        codec  = (s.get("codec_name") or "?").upper()
        idx    = s.get("index", "?")
        flag   = lang_flag(lang)
        lname  = lang_name(lang)
        forced = " ⚡Forced" if tags.get("forced") else ""
        sub_lines.append(f"  #{idx} {flag} {lname} [{codec}]{forced}")
    sub_info = "\n".join(sub_lines) if sub_lines else "  <i>No subtitle tracks found</i>"

    if french_bitmap:
        b      = french_bitmap[0]
        notice = (
            f"⚠️ French subtitle found but it is bitmap "
            f"({(b.get('codec_name') or 'PGS').upper()}) — need a text format.\n\n"
        )
    else:
        notice = "⚠️ No French subtitle found automatically.\n\n"

    # ── Inline keyboard: three choices ────────────────────────────────────────
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "🔥 I'll send a subtitle (.ass/.srt)",
            callback_data=f"smon|sub|{folder_id}",
        )],
        [InlineKeyboardButton(
            "📤 Upload as-is (no hardsub)",
            callback_data=f"smon|upload|{folder_id}",
        )],
        [InlineKeyboardButton(
            "❌ Skip this folder",
            callback_data=f"smon|skip|{folder_id}",
        )],
    ])

    await _edit_or_new(
        notify_msg, owner_uid,
        f"📁 <b>New Seedr folder: {folder_name}</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{fname[:45]}</code>\n"
        f"💾 <code>{human_size(fsize)}</code>\n\n"
        f"{notice}"
        f"<b>Available subtitle tracks:</b>\n{sub_info}\n\n"
        "──────────────────────\n"
        "<i>Choose an action below, or send a .ass/.srt file / URL directly.</i>",
        reply_markup=kb,
    )

    _processed_ids.add(folder_id)
    _save_store()


# ══════════════════════════════════════════════════════════════════════════════
# Background poll loop
# ══════════════════════════════════════════════════════════════════════════════

async def _poll_loop(owner_uid: int) -> None:
    global _known_ids

    if not os.environ.get("SEEDR_USERNAME", "").strip():
        log.warning("[SeedrMon] SEEDR_USERNAME not set — monitor disabled")
        return

    from services.seedr import _root, _accounts

    log.info("[SeedrMon] Starting (uid=%d)", owner_uid)

    # Bootstrap: snapshot existing folders to avoid re-processing
    if not _known_ids:
        try:
            accs       = _accounts()
            user, pwd  = accs[0]
            root_data  = await _root(user, pwd)
            _known_ids = {f["id"] for f in root_data.get("folders", [])}
            _save_store()
            log.info("[SeedrMon] Bootstrapped with %d folder(s)", len(_known_ids))
        except Exception as exc:
            log.warning("[SeedrMon] Bootstrap snapshot: %s", exc)

    last_new = 0.0

    while True:
        interval = POLL_ACTIVE if (time.time() - last_new < 600) else POLL_IDLE
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            log.info("[SeedrMon] Cancelled")
            return

        cc_key = os.environ.get("CC_API_KEY", "").strip()
        if not cc_key:
            log.debug("[SeedrMon] CC_API_KEY not set — skipping cycle")
            continue

        try:
            accs      = _accounts()
            user, pwd = accs[0]
            root_data = await _root(user, pwd)
            folders   = root_data.get("folders", [])

            current_ids = {f["id"] for f in folders}
            new_ids     = current_ids - _known_ids - _processed_ids

            if new_ids:
                last_new = time.time()
                log.info("[SeedrMon] %d new folder(s): %s", len(new_ids), new_ids)
                for folder in folders:
                    if folder["id"] not in new_ids:
                        continue
                    _known_ids.add(folder["id"])
                    _save_store()
                    asyncio.create_task(
                        _process_new_folder(folder, user, pwd, owner_uid)
                    )
            else:
                _known_ids = current_ids | _processed_ids

        except asyncio.CancelledError:
            return
        except Exception as exc:
            log.warning("[SeedrMon] Poll error: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def start_monitor(owner_uid: int) -> None:
    """Start the background Seedr monitor. Call from main.py after bot starts."""
    global _monitor_task
    _load_store()
    if _monitor_task and not _monitor_task.done():
        return
    try:
        _monitor_task = asyncio.get_running_loop().create_task(
            _poll_loop(owner_uid)
        )
        log.info("[SeedrMon] Task created")
    except RuntimeError as exc:
        log.warning("[SeedrMon] Start failed: %s", exc)


def stop_monitor() -> None:
    global _monitor_task
    if _monitor_task and not _monitor_task.done():
        _monitor_task.cancel()
        log.info("[SeedrMon] Stopped")
