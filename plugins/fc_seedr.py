"""
plugins/fc_seedr.py
Seedr → FreeConvert pipeline (convert and hardsub).

NEW BUTTONS (added to magnet menu in url_handler.py):
  🔄 Seedr+FC Convert   → sfc|convert|{token}
  🔥 Seedr+FC Hardsub   → sfc|hardsub|{token}

FLOWS
─────
Seedr+FC Convert:
  1. Seedr downloads magnet at datacenter speed → local file
  2. Resolution picker shown to user
  3. Video uploaded to FreeConvert → convert job → poll
  4. Result downloaded → uploaded to Telegram

Seedr+FC Hardsub:
  1. Seedr downloads magnet → local file
  2. ffprobe auto-detects French subtitle track
  3. If found: extract → upload video + sub to FC → hardsub job → poll
  4. If not found: ask user for subtitle file/URL, then FC hardsub
  5. Result downloaded → uploaded to Telegram

FIX: Uses FreeConvert (no CC_API_KEY needed) — only FC_API_KEY required.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import urllib.parse as _up

import aiohttp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from services.utils import cleanup, human_size, lang_flag, lang_name, make_tmp, safe_edit

log = logging.getLogger(__name__)

# ── French subtitle detection (copied from seedr_hardsub.py) ──
_FRENCH_CODES    = frozenset({"fr", "fra", "fre"})
_TEXT_SUB_CODECS = frozenset({"ass", "ssa", "subrip", "srt", "webvtt", "vtt",
                               "mov_text", "text", "microdvd"})
_BITMAP_SUB_CODECS = frozenset({"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle",
                                 "pgssub", "dvdsub"})
_SUB_EXTS  = frozenset({".ass", ".srt", ".vtt", ".ssa", ".sub", ".txt"})
_VIDEO_EXTS = frozenset({".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
                          ".ts", ".m2ts", ".wmv", ".m4v"})

# ── Per-user state for Seedr+FC flows ────────────────────────
# uid → {"video_path", "fname", "tmp", "mode": "hardsub"|"convert", "_created"}
_SFC_STATE: dict[int, dict] = {}
_SFC_TTL = 1800  # 30 min


def _clear_sfc(uid: int) -> None:
    state = _SFC_STATE.pop(uid, None)
    if state and state.get("tmp"):
        cleanup(state["tmp"])


def _evict_sfc() -> None:
    now  = time.time()
    dead = [u for u, s in list(_SFC_STATE.items())
            if now - s.get("_created", 0) > _SFC_TTL]
    for u in dead:
        _clear_sfc(u)


# ─────────────────────────────────────────────────────────────
# FC API key helper
# ─────────────────────────────────────────────────────────────

def _get_fc_keys() -> list[str]:
    from services.freeconvert_api import parse_fc_keys
    raw  = os.environ.get("FC_API_KEY", "").strip()
    keys = parse_fc_keys(raw)
    for i in range(2, 10):
        extra = os.environ.get(f"FC_API_KEY_{i}", "").strip()
        if extra:
            keys.extend(parse_fc_keys(extra))
    return keys


# ─────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────

def _sfc_resolution_kb(token: str, mode: str) -> InlineKeyboardMarkup:
    """Resolution picker for Seedr+FC Convert."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔵 1080p", callback_data=f"sfc|res|1080|{token}"),
         InlineKeyboardButton("🟢 720p",  callback_data=f"sfc|res|720|{token}")],
        [InlineKeyboardButton("🟡 480p",  callback_data=f"sfc|res|480|{token}"),
         InlineKeyboardButton("🟠 360p",  callback_data=f"sfc|res|360|{token}")],
        [InlineKeyboardButton("🎬 Original (no resize)", callback_data=f"sfc|res|0|{token}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"sfc|cancel|{token}")],
    ])


# ─────────────────────────────────────────────────────────────
# Main entry point — sfc|<action>|<token>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^sfc\|"))
async def sfc_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    action = parts[1]
    uid    = cb.from_user.id
    await cb.answer()

    if action == "cancel":
        _clear_sfc(uid)
        try:
            await cb.message.delete()
        except Exception:
            pass
        return

    # ── Seedr+FC Convert — show resolution picker ─────────────
    if action == "convert":
        token = parts[2]
        keys  = _get_fc_keys()
        if not keys:
            return await safe_edit(
                cb.message,
                "❌ <b>FreeConvert not configured</b>\n\n"
                "Add <code>FC_API_KEY=your_key</code> to .env or Colab secrets.\n"
                "Get a free key at <b>freeconvert.com → Account → API Keys</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        username = os.environ.get("SEEDR_USERNAME", "").strip()
        password = os.environ.get("SEEDR_PASSWORD", "").strip()
        if not username or not password:
            return await safe_edit(
                cb.message,
                "❌ <b>Seedr not configured</b>\n\n"
                "Add to .env:\n"
                "<code>SEEDR_USERNAME=your@email.com</code>\n"
                "<code>SEEDR_PASSWORD=yourpassword</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        await safe_edit(
            cb.message,
            "🔄 <b>Seedr+FC Convert</b>\n"
            "──────────────────────\n\n"
            "Choose target resolution:\n"
            "<i>Seedr will download first, then FreeConvert converts.</i>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=_sfc_resolution_kb(token, "convert"),
        )
        return

    # ── Seedr+FC Hardsub — start pipeline ────────────────────
    if action == "hardsub":
        token = parts[2]
        keys  = _get_fc_keys()
        if not keys:
            return await safe_edit(
                cb.message,
                "❌ <b>FreeConvert not configured</b>\n\n"
                "Add <code>FC_API_KEY=your_key</code> to .env.",
                parse_mode=enums.ParseMode.HTML,
            )
        username = os.environ.get("SEEDR_USERNAME", "").strip()
        password = os.environ.get("SEEDR_PASSWORD", "").strip()
        if not username or not password:
            return await safe_edit(
                cb.message,
                "❌ <b>Seedr not configured</b>\n\n"
                "Add <code>SEEDR_USERNAME</code> + <code>SEEDR_PASSWORD</code> to .env.",
                parse_mode=enums.ParseMode.HTML,
            )
        try:
            from plugins.url_handler import _get
            url = _get(token)
        except ImportError:
            url = ""
        if not url:
            return await safe_edit(
                cb.message,
                "❌ Session expired. Resend the magnet link.",
                parse_mode=enums.ParseMode.HTML,
            )
        st = await cb.message.edit(
            "🔥 <b>Seedr+FC Hardsub</b>\n"
            "──────────────────────\n\n"
            "⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_sfc_hardsub_pipeline(client, st, url, uid))
        return

    # ── Resolution selected → start Seedr+FC Convert ─────────
    if action == "res":
        if len(parts) < 4:
            return await cb.answer("Invalid.", show_alert=True)
        height_s = parts[2]
        token    = parts[3]
        height   = int(height_s) if height_s.isdigit() else 0
        try:
            from plugins.url_handler import _get
            url = _get(token)
        except ImportError:
            url = ""
        if not url:
            return await safe_edit(
                cb.message,
                "❌ Session expired. Resend the magnet link.",
                parse_mode=enums.ParseMode.HTML,
            )
        res_label = f"{height}p" if height else "Original"
        st = await cb.message.edit(
            f"🔄 <b>Seedr+FC Convert → {res_label}</b>\n"
            "──────────────────────\n\n"
            "⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_sfc_convert_pipeline(client, st, url, uid, height))
        return


# ─────────────────────────────────────────────────────────────
# Shared: Seedr CDN URL fetcher (no local download)
# ─────────────────────────────────────────────────────────────

async def _seedr_fetch_urls_for_fc(
    st, magnet: str, uid: int, label: str
) -> tuple[dict | None, int | None, str | None, str | None]:
    """
    Submit magnet to Seedr and wait for it to finish, then return the
    CDN URL of the largest video file WITHOUT downloading it locally.

    Returns (file_dict, folder_id, seedr_user, seedr_pwd) on success,
    or (None, None, None, None) on failure.
    file_dict = {name, url, size, clean_name}

    The Seedr folder is kept alive so CloudConvert/FreeConvert can pull
    the file via URL. Caller must call _seedr_cleanup(folder_id, user, pwd)
    after the conversion job completes.
    """
    from services.seedr import fetch_urls_via_seedr
    from services.utils import human_dur

    start = time.time()

    async def _progress(stage: str, pct: float, detail: str) -> None:
        now     = time.time()
        elapsed = human_dur(int(now - start))
        icons   = {"selecting": "🔍", "adding": "⬆️", "waiting": "⏳",
                   "downloading": "☁️", "fetching": "🔗", "submitting": "⬆️"}
        icon    = icons.get(stage, "⏳")
        bar     = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        await safe_edit(
            st,
            f"☁️ <b>{label}</b>\n"
            "──────────────────────\n\n"
            f"{icon} <i>{detail}</i>\n\n"
            f"<code>[{bar}]</code>  <b>{pct:.0f}%</b>  ·  ⏱ <i>{elapsed}</i>\n\n"
            "<i>Seedr fetches at datacenter speed.\n"
            "Link will be sent directly to the converter.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    try:
        files, folder_id, seedr_user, seedr_pwd = await fetch_urls_via_seedr(
            magnet, progress_cb=_progress, timeout_s=7200,
        )
    except Exception as exc:
        await safe_edit(
            st,
            f"❌ <b>Seedr failed</b>\n\n<code>{str(exc)[:300]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return None, None, None, None

    if not files:
        await safe_edit(st, "❌ <b>Seedr: no files found.</b>",
                        parse_mode=enums.ParseMode.HTML)
        return None, None, None, None

    # Pick the largest video file
    video_files = [f for f in files
                   if os.path.splitext(f["name"])[1].lower() in _VIDEO_EXTS]
    if not video_files:
        video_files = files

    best = max(video_files, key=lambda f: f.get("size", 0))
    return best, folder_id, seedr_user, seedr_pwd


async def _seedr_cleanup(folder_id: int, user: str, pwd: str) -> None:
    """Delete the Seedr folder after conversion is done (reclaim quota)."""
    try:
        from services.seedr import _del_folder  # noqa: internal use
        await _del_folder(user, pwd, folder_id)
        log.info("[SFC] Seedr folder %d cleaned up.", folder_id)
    except Exception as exc:
        log.warning("[SFC] Seedr cleanup (non-fatal): %s", exc)


# ─────────────────────────────────────────────────────────────
# Seedr+FC Convert pipeline
# ─────────────────────────────────────────────────────────────

async def _sfc_convert_pipeline(
    client: Client, st, magnet: str, uid: int, scale_height: int,
) -> None:
    """
    1. Seedr fetches torrent → CDN URL (no local download)
    2. Pass CDN URL directly to FreeConvert → convert/resize job
    3. Poll → download result → upload to Telegram
    """
    from services.freeconvert_api import (
        pick_best_fc_key, submit_convert,
        run_fc_job,
    )
    from services.uploader import upload_file
    from services.utils import make_tmp, human_size

    keys      = _get_fc_keys()
    tmp       = make_tmp(cfg.download_dir, uid)
    res_label = f"{scale_height}p" if scale_height else "Original"

    # Step 1: Seedr → CDN URL (no local download)
    file_info, folder_id, seedr_user, seedr_pwd = await _seedr_fetch_urls_for_fc(
        st, magnet, uid, f"Seedr+FC Convert → {res_label}"
    )
    if not file_info:
        cleanup(tmp)
        return

    fname     = file_info["clean_name"]
    fsize     = file_info.get("size", 0)
    video_url = file_info["url"]
    name_base = os.path.splitext(fname)[0]
    out_name  = f"{name_base}_{res_label}.mp4"

    await safe_edit(
        st,
        f"🔄 <b>Seedr+FC Convert → {res_label}</b>\n"
        "──────────────────────\n\n"
        f"✅ Seedr ready — sending link to FreeConvert\n"
        f"📁 <code>{fname[:40]}</code>  <code>{human_size(fsize)}</code>\n\n"
        "☁️ <i>Selecting best FC key…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # Step 2: Submit CDN URL directly to FreeConvert (no upload needed!)
    try:
        key, mins = await pick_best_fc_key(keys)

        await safe_edit(
            st,
            f"🔄 <b>Seedr+FC Convert → {res_label}</b>\n"
            "──────────────────────\n\n"
            f"📁 <code>{fname[:40]}</code>\n"
            f"📐 → <b>{res_label}</b>\n\n"
            "🔗 <i>Submitting Seedr CDN link to FreeConvert…\n"
            "(no upload — FreeConvert pulls directly!)</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        job_id = await submit_convert(
            key,
            video_url=video_url,       # ← CDN URL, not a local file
            scale_height=scale_height,
            crf=23,
            output_name=out_name,
        )

        await safe_edit(
            st,
            f"🔄 <b>Seedr+FC Convert → {res_label}</b>\n"
            "──────────────────────\n\n"
            f"📁 <code>{fname[:40]}</code>\n"
            f"🆔 <code>{job_id}</code>\n\n"
            "⏳ <i>FreeConvert is converting…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        async def _prog(pct: float, detail: str) -> None:
            bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
            await safe_edit(
                st,
                f"🔄 <b>Seedr+FC Convert → {res_label}</b>\n"
                "──────────────────────\n\n"
                f"📁 <code>{fname[:38]}</code>\n"
                f"<code>[{bar}]</code>  <b>{pct:.0f}%</b>\n\n"
                f"<i>{detail}</i>",
                parse_mode=enums.ParseMode.HTML,
            )

        result_path = await run_fc_job(
            key, job_id, tmp,
            output_name=out_name,
            progress_cb=_prog,
        )

    except Exception as exc:
        log.error("[SFC-Convert] FC failed: %s", exc, exc_info=True)
        cleanup(tmp)
        return await safe_edit(
            st,
            f"❌ <b>FreeConvert conversion failed</b>\n\n<code>{str(exc)[:300]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    finally:
        # Always clean up Seedr folder once FC has the file (or on failure)
        await _seedr_cleanup(folder_id, seedr_user, seedr_pwd)

    # Step 3: Download result → Upload to Telegram
    result_size = os.path.getsize(result_path)
    try:
        await st.delete()
    except Exception:
        pass

    try:
        upload_st = await client.send_message(
            uid,
            f"🔄 <b>Convert done!</b>  {res_label}\n"
            f"<code>{out_name}</code>  <code>{human_size(result_size)}</code>\n"
            "⬆️ Uploading…",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        log.error("[SFC-Convert] send_message failed: %s", exc)
        cleanup(tmp)
        return

    try:
        await upload_file(client, upload_st, result_path, user_id=uid)
    except Exception as exc:
        log.error("[SFC-Convert] upload_file failed: %s", exc, exc_info=True)
        await safe_edit(
            upload_st,
            f"❌ <b>Upload failed</b>\n\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    finally:
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Seedr+FC Hardsub pipeline
# ─────────────────────────────────────────────────────────────

async def _sfc_hardsub_pipeline(
    client: Client, st, magnet: str, uid: int,
) -> None:
    """
    1. Seedr fetches torrent → CDN URL (no local download)
    2. Pass CDN URL directly to FreeConvert for hardsub
       (subtitle must be provided by user since ffprobe needs local file)
    NOTE: If the MKV contains embedded French subs, we must download it
    locally only for ffprobe + sub extraction, then pass the video URL
    and the extracted sub to FC. We avoid re-downloading the full video.
    """
    from services import ffmpeg as FF
    from services.utils import make_tmp, human_size
    from services.downloader import download_direct

    tmp = make_tmp(cfg.download_dir, uid)

    # Step 1: Seedr → CDN URL (no local video download yet)
    file_info, folder_id, seedr_user, seedr_pwd = await _seedr_fetch_urls_for_fc(
        st, magnet, uid, "Seedr+FC Hardsub"
    )
    if not file_info:
        cleanup(tmp)
        return

    fname     = file_info["clean_name"]
    fsize     = file_info.get("size", 0)
    video_url = file_info["url"]

    await safe_edit(
        st,
        f"🔥 <b>Seedr+FC Hardsub</b>\n"
        "──────────────────────\n\n"
        f"✅ Seedr ready\n"
        f"📁 <code>{fname[:45]}</code>  <code>{human_size(fsize)}</code>\n\n"
        "🔍 <i>Probing streams for French subtitle…\n"
        "(downloading just enough to read the header)</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # Download only what's needed for ffprobe (first ~10 MB is enough for headers)
    # We use a partial download trick — download the file but only keep the header portion
    # Download only what's needed for ffprobe (first 50 MB covers MKV headers + subtitle tracks)
    probe_path = os.path.join(tmp, fname)
    try:
        _probe_fpath = os.path.join(tmp, fname)
        _timeout     = aiohttp.ClientTimeout(total=300)
        _MAX         = 50 * 1024 * 1024  # 50 MB cap
        async with aiohttp.ClientSession(timeout=_timeout) as _sess:
            async with _sess.get(video_url, headers={"User-Agent": "Mozilla/5.0"},
                                  allow_redirects=True) as _resp:
                _resp.raise_for_status()
                _done = 0
                with open(_probe_fpath, "wb") as _fh:
                    async for _chunk in _resp.content.iter_chunked(1024 * 1024):
                        _fh.write(_chunk)
                        _done += len(_chunk)
                        if _done >= _MAX:
                            break
        probe_path = _probe_fpath
        log.info("[SFC-Hardsub] Partial probe: %s bytes -> %s", _done, _probe_fpath)
    except Exception as exc:
        log.warning("[SFC-Hardsub] Partial probe failed: %s — probing URL directly", exc)
        probe_path = video_url

    try:
        sd = await FF.probe_streams(probe_path)
    except Exception as exc:
        cleanup(tmp)
        await _seedr_cleanup(folder_id, seedr_user, seedr_pwd)
        return await safe_edit(
            st,
            f"❌ <b>Stream probe failed</b>\n\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    all_subs     = sd.get("subtitle", [])
    french_text  = [
        s for s in all_subs
        if (s.get("tags", {}) or {}).get("language", "und").lower() in _FRENCH_CODES
        and (s.get("codec_name") or "").lower() in _TEXT_SUB_CODECS
    ]
    french_bitmap = [
        s for s in all_subs
        if (s.get("tags", {}) or {}).get("language", "und").lower() in _FRENCH_CODES
        and (s.get("codec_name") or "").lower() in _BITMAP_SUB_CODECS
    ]

    if french_text:
        # We have the probe file locally — extract the sub from it
        # Then submit video_url + extracted sub to FC (video is NOT re-uploaded)
        await _sfc_auto_hardsub_url(
            client, st, probe_path, video_url, fname, fsize,
            french_text[0], tmp, uid, folder_id, seedr_user, seedr_pwd
        )
        return

    # No text sub found — ask user to provide one
    # Store video_url for later use (no local video needed)
    sub_info_lines = []
    for s in all_subs:
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()
        codec = (s.get("codec_name") or "?").upper()
        idx   = s.get("index", "?")
        flag  = lang_flag(lang)
        lname = lang_name(lang)
        sub_info_lines.append(f"  #{idx} {flag} {lname} [{codec}]")

    sub_info = "\n".join(sub_info_lines) if sub_info_lines else "  <i>None found</i>"

    if french_bitmap:
        b = french_bitmap[0]
        notice = (
            f"⚠️ French sub found but it's bitmap ({(b.get('codec_name') or 'PGS').upper()})\n"
            "FreeConvert needs text-based subtitles.\n\n"
        )
    else:
        notice = "⚠️ No French subtitle found automatically.\n\n"

    _evict_sfc()
    _SFC_STATE[uid] = {
        "video_url":   video_url,       # ← CDN URL, not local path
        "video_path":  probe_path if os.path.isfile(probe_path) else None,
        "fname":       fname,
        "fsize":       fsize,
        "tmp":         tmp,
        "mode":        "hardsub",
        "folder_id":   folder_id,
        "seedr_user":  seedr_user,
        "seedr_pwd":   seedr_pwd,
        "_created":    time.time(),
    }

    await safe_edit(
        st,
        f"🔥 <b>Seedr+FC Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:40]}</code>\n\n"
        f"{notice}"
        f"<b>Available subtitles:</b>\n{sub_info}\n\n"
        "──────────────────────\n\n"
        "Send me a <b>subtitle</b>:\n"
        "• A <b>.ass / .srt / .vtt file</b>\n"
        "• A <b>URL</b> to a subtitle file\n\n"
        "<i>Video is on Seedr — no re-download needed.\n"
        "Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


async def _sfc_auto_hardsub_url(
    client: Client, st,
    probe_path: str,        # local partial file (for sub extraction only)
    video_url: str,         # full Seedr CDN URL (sent directly to FC)
    fname: str,
    fsize: int,
    sub_stream: dict,
    tmp: str,
    uid: int,
    folder_id: int,
    seedr_user: str,
    seedr_pwd: str,
) -> None:
    """Extract French sub from local probe file, then submit video_url + sub to FC."""
    from services import ffmpeg as FF

    idx   = sub_stream.get("index", 0)
    codec = (sub_stream.get("codec_name") or "ass").lower()
    ext   = FF.subtitle_ext(codec)
    tags  = sub_stream.get("tags", {}) or {}

    sub_path  = os.path.join(tmp, f"french_sub{ext}")
    sub_fname = os.path.basename(sub_path)

    detail_s = f"#{idx} {codec.upper()}"
    if tags.get("title"):
        detail_s += f" — {tags['title']}"

    await safe_edit(
        st,
        f"🔥 <b>Seedr+FC Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:40]}</code>\n"
        f"✅ French sub: <code>{detail_s}</code>\n\n"
        "📤 <i>Extracting subtitle…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        await FF.stream_op(probe_path, sub_path, ["-map", f"0:{idx}", "-c", "copy"])
    except Exception as exc:
        cleanup(tmp)
        await _seedr_cleanup(folder_id, seedr_user, seedr_pwd)
        return await safe_edit(
            st,
            f"❌ <b>Subtitle extraction failed</b>\n\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    await _submit_fc_hardsub_url(
        client, st,
        video_url, fname, fsize,
        sub_path, sub_fname, detail_s, tmp, uid,
        folder_id, seedr_user, seedr_pwd,
    )


async def _submit_fc_hardsub_url(
    client, st,
    video_url: str,         # ← Seedr CDN URL sent directly to FC
    video_fname: str,
    fsize: int,
    sub_path: str,
    sub_fname: str,
    sub_detail: str,
    tmp: str,
    uid: int,
    folder_id: int,
    seedr_user: str,
    seedr_pwd: str,
) -> None:
    """Submit video CDN URL + local subtitle file to FreeConvert for hardsub."""
    from services.freeconvert_api import (
        pick_best_fc_key, submit_hardsub, run_fc_job,
    )
    from services.uploader import upload_file
    from services.utils import human_size
    from services.fc_job_store import fc_job_store, FCJob
    from services.cc_sanitize import build_cc_output_name

    keys        = _get_fc_keys()
    output_name = build_cc_output_name(video_fname, "VOSTFR")

    await safe_edit(
        st,
        f"🔥 <b>Seedr+FC Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{video_fname[:38]}</code>\n"
        f"💬 <code>{sub_detail[:38]}</code>\n"
        f"📦 → <code>{output_name[:38]}</code>\n\n"
        "☁️ <i>Submitting to FreeConvert…\n"
        "Video URL sent directly — only subtitle uploaded!</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        key, mins = await pick_best_fc_key(keys)

        # video_url goes as import/url (no upload); subtitle is uploaded as a small file
        job_id = await submit_hardsub(
            key,
            video_url=video_url,        # ← CDN URL, FreeConvert pulls directly
            subtitle_path=sub_path,     # ← small subtitle file uploaded
            output_name=output_name,
            crf=20,
            preset="medium",
        )

        await fc_job_store.add(FCJob(
            job_id=job_id,
            uid=uid,
            fname=video_fname,
            sub_fname=sub_fname,
            output_name=output_name,
            status="processing",
            job_type="hardsub",
            api_key=key,
        ))

        await safe_edit(
            st,
            f"✅ <b>Seedr+FC Hardsub — Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📁 <code>{video_fname[:36]}</code>\n"
            f"💬 <code>{sub_detail[:36]}</code>\n"
            f"📦 → <code>{output_name[:36]}</code>\n\n"
            "⏳ <i>FreeConvert is processing…\n"
            "The hardsubbed MP4 will auto-upload\n"
            "when ready (~3-5 min).</i>\n\n"
            "📋 Use /ccstatus to track progress.",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        log.error("[SFC-Hardsub] FC submit failed: %s", exc, exc_info=True)
        await safe_edit(
            st,
            f"❌ <b>FreeConvert submission failed</b>\n\n<code>{str(exc)[:250]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    finally:
        # Clean up Seedr folder — FC has the URL, no longer needed
        await _seedr_cleanup(folder_id, seedr_user, seedr_pwd)
        cleanup(tmp)


# Keep old name as alias for manual sub flow (file/URL handlers below)
async def _submit_fc_hardsub(
    client, st,
    video_path: str, video_fname: str,
    sub_path: str, sub_fname: str,
    sub_detail: str, tmp: str, uid: int,
) -> None:
    """Legacy path: video downloaded locally (used only when video_url is unavailable)."""
    from services.freeconvert_api import (
        pick_best_fc_key, submit_hardsub, run_fc_job,
    )
    from services.uploader import upload_file
    from services.utils import human_size
    from services.fc_job_store import fc_job_store, FCJob
    from services.cc_sanitize import build_cc_output_name

    keys        = _get_fc_keys()
    output_name = build_cc_output_name(video_fname, "VOSTFR")

    await safe_edit(
        st,
        f"🔥 <b>Seedr+FC Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{video_fname[:38]}</code>\n"
        f"💬 <code>{sub_detail[:38]}</code>\n"
        f"📦 → <code>{output_name[:38]}</code>\n\n"
        "☁️ <i>Uploading to FreeConvert…\n"
        "(video + subtitle files)</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        key, mins = await pick_best_fc_key(keys)

        job_id = await submit_hardsub(
            key,
            video_path=video_path,
            subtitle_path=sub_path,
            output_name=output_name,
            crf=20,
            preset="medium",
        )

        await fc_job_store.add(FCJob(
            job_id=job_id,
            uid=uid,
            fname=video_fname,
            sub_fname=sub_fname,
            output_name=output_name,
            status="processing",
            job_type="hardsub",
            api_key=key,
        ))

        await safe_edit(
            st,
            f"✅ <b>Seedr+FC Hardsub — Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📁 <code>{video_fname[:36]}</code>\n"
            f"💬 <code>{sub_detail[:36]}</code>\n"
            f"📦 → <code>{output_name[:36]}</code>\n\n"
            "⏳ <i>FreeConvert is processing…\n"
            "The hardsubbed MP4 will auto-upload\n"
            "when ready (~3-5 min).</i>\n\n"
            "📋 Use /ccstatus to track progress.",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        log.error("[SFC-Hardsub] FC submit failed: %s", exc, exc_info=True)
        await safe_edit(
            st,
            f"❌ <b>FreeConvert submission failed</b>\n\n<code>{str(exc)[:250]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Manual subtitle receivers (file + URL)
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.document,
    group=-3,
)
async def sfc_manual_sub_file(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _SFC_STATE.get(uid)
    if not state or state.get("mode") != "hardsub":
        return

    media     = msg.document
    doc_fname = getattr(media, "file_name", None) or "subtitle.ass"
    ext       = os.path.splitext(doc_fname)[1].lower()
    if ext not in _SUB_EXTS:
        return

    tmp         = state["tmp"]
    video_path  = state.get("video_path")
    video_url   = state.get("video_url")
    video_fname = state["fname"]
    folder_id   = state.get("folder_id")
    seedr_user  = state.get("seedr_user")
    seedr_pwd   = state.get("seedr_pwd")

    st = await msg.reply("⬇️ Downloading subtitle…")
    try:
        sub_path = await client.download_media(
            media, file_name=os.path.join(tmp, doc_fname),
        )
    except Exception as exc:
        return await safe_edit(
            st,
            f"❌ Subtitle download failed: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    _SFC_STATE.pop(uid, None)
    sub_fname = os.path.basename(sub_path)

    if video_url and folder_id and seedr_user and seedr_pwd:
        # Preferred path: send Seedr CDN URL to FC directly (no video upload)
        await _submit_fc_hardsub_url(
            client, st,
            video_url, video_fname, state.get("fsize", 0),
            sub_path, sub_fname, sub_fname, tmp, uid,
            folder_id, seedr_user, seedr_pwd,
        )
    else:
        # Fallback: video was downloaded locally
        await _submit_fc_hardsub(client, st, video_path, video_fname,
                                  sub_path, sub_fname, sub_fname, tmp, uid)
    msg.stop_propagation()


@Client.on_message(
    filters.private & filters.text
    & ~filters.command([
        "start", "help", "settings", "cancel",
        "hardsub", "ccstatus", "convert", "resize", "compress",
    ]),
    group=-3,
)
async def sfc_manual_sub_url(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _SFC_STATE.get(uid)
    if not state or state.get("mode") != "hardsub":
        return

    text = msg.text.strip()
    if not text.startswith("http"):
        return

    tmp         = state["tmp"]
    video_path  = state.get("video_path")
    video_url   = state.get("video_url")
    video_fname = state["fname"]
    folder_id   = state.get("folder_id")
    seedr_user  = state.get("seedr_user")
    seedr_pwd   = state.get("seedr_pwd")

    st = await msg.reply(f"⬇️ Downloading subtitle…\n<code>{text[:60]}</code>",
                         parse_mode=enums.ParseMode.HTML)
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(text, headers={"User-Agent": "Mozilla/5.0"},
                                 allow_redirects=True) as resp:
                resp.raise_for_status()
                content = await resp.read()
                cd  = resp.headers.get("Content-Disposition", "")
                raw = (cd.split("filename=")[-1].strip().strip('"').strip("'")
                       if "filename=" in cd else "")
        if not raw:
            raw = os.path.basename(_up.urlparse(text).path)
            raw = _up.unquote_plus(raw) if raw else "subtitle.ass"
        ext = os.path.splitext(raw)[1].lower()
        if ext not in _SUB_EXTS:
            raw += ".ass"
        raw      = re.sub(r'[\\/:*?"<>|]', "_", raw)
        sub_path = os.path.join(tmp, raw)
        with open(sub_path, "wb") as f:
            f.write(content)
    except Exception as exc:
        return await safe_edit(
            st,
            f"❌ Subtitle download failed:\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    _SFC_STATE.pop(uid, None)
    sub_fname = os.path.basename(sub_path)

    if video_url and folder_id and seedr_user and seedr_pwd:
        # Preferred path: send Seedr CDN URL to FC directly (no video upload)
        await _submit_fc_hardsub_url(
            client, st,
            video_url, video_fname, state.get("fsize", 0),
            sub_path, sub_fname, sub_fname, tmp, uid,
            folder_id, seedr_user, seedr_pwd,
        )
    else:
        # Fallback: video was downloaded locally
        await _submit_fc_hardsub(client, st, video_path, video_fname,
                                  sub_path, sub_fname, sub_fname, tmp, uid)
    msg.stop_propagation()


@Client.on_message(filters.private & filters.command("cancel"), group=-3)
async def sfc_cancel(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _SFC_STATE:
        return
    _clear_sfc(uid)
    await msg.reply("❌ Seedr+FC pipeline cancelled.")
    msg.stop_propagation()
