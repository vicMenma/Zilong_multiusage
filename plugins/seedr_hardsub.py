"""
plugins/seedr_hardsub.py
Seedr → Auto-Hardsub pipeline.

WHAT THIS DOES:
  1. User sends magnet → clicks "🔥 Seedr+Hardsub"
  2. Seedr downloads torrent at datacenter speed
  3. Bot downloads video from Seedr to local
  4. ffprobe auto-detects French subtitle track (fra/fre/fr)
  5. Extracts subtitle to .ass/.srt
  6. Submits video + subtitle to CloudConvert for hardsub
  7. CC processes → ccstatus poller auto-uploads result

FIX C-05 (audit v3): _WAITING_SUB now has 30-minute TTL eviction.
Previously, if a user abandoned the manual-subtitle flow (no French sub found),
the cached video file (1-2 GB) persisted forever in the temp directory.
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
from services.utils import (
    cleanup, human_size, lang_flag, lang_name, make_tmp, safe_edit,
)

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# French subtitle detection
# ─────────────────────────────────────────────────────────────

_FRENCH_CODES = frozenset({"fr", "fra", "fre"})

_TEXT_SUB_CODECS = frozenset({
    "ass", "ssa", "subrip", "srt", "webvtt", "vtt",
    "mov_text", "text", "microdvd",
})

_BITMAP_SUB_CODECS = frozenset({
    "hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle",
    "pgssub", "dvdsub",
})

_SUB_EXTS = frozenset({".ass", ".srt", ".vtt", ".ssa", ".sub", ".txt"})


def _find_french_subs(subtitle_streams: list[dict]) -> tuple[list[dict], list[dict]]:
    french_text: list[dict]   = []
    french_bitmap: list[dict] = []

    for s in subtitle_streams:
        tags   = s.get("tags", {}) or {}
        lang   = (tags.get("language") or "und").lower()
        codec  = (s.get("codec_name") or "").lower()
        forced = str(tags.get("forced", "0")).lower() in ("1", "true", "yes")

        if lang not in _FRENCH_CODES:
            continue

        if codec in _BITMAP_SUB_CODECS:
            french_bitmap.append(s)
            continue

        s["_is_forced"] = forced
        french_text.append(s)

    def _priority(s: dict) -> tuple[int, int]:
        forced_score = 1 if s.get("_is_forced") else 0
        codec = (s.get("codec_name") or "").lower()
        if codec in ("ass", "ssa"):
            codec_score = 0
        elif codec in ("subrip", "srt"):
            codec_score = 1
        else:
            codec_score = 2
        return (forced_score, codec_score)

    french_text.sort(key=_priority)
    return french_text, french_bitmap


# ─────────────────────────────────────────────────────────────
# Per-user state for manual subtitle fallback
# FIX C-05 (audit v3): TTL eviction added
# ─────────────────────────────────────────────────────────────

_WAITING_SUB: dict[int, dict] = {}
_WAITING_SUB_TTL = 1800  # 30 min — auto-cleanup cached video files


def _clear_waiting(uid: int) -> None:
    state = _WAITING_SUB.pop(uid, None)
    if state and state.get("tmp"):
        cleanup(state["tmp"])


def _evict_waiting_subs() -> None:
    """FIX C-05: evict stale entries holding multi-GB video files."""
    now = time.time()
    dead = [uid for uid, s in _WAITING_SUB.items()
            if now - s.get("_created", 0) > _WAITING_SUB_TTL]
    for uid in dead:
        _clear_waiting(uid)


# ─────────────────────────────────────────────────────────────
# Entry point — callback from magnet menu
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^shs\|"))
async def seedr_hardsub_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    action = parts[1]
    uid    = cb.from_user.id
    await cb.answer()

    if action == "cancel":
        _clear_waiting(uid)
        try:
            await cb.message.delete()
        except Exception:
            pass
        return

    if action == "start":
        token = parts[2]

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

        api_key = os.environ.get("CC_API_KEY", "").strip()
        if not api_key:
            return await safe_edit(
                cb.message,
                "❌ <b>CloudConvert API key not set</b>\n\n"
                "Add <code>CC_API_KEY=your_key</code> to .env",
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

        st = await cb.message.edit(
            "🔥 <b>Seedr → Hardsub</b>\n"
            "──────────────────────\n\n"
            "⬆️ <i>Submitting to Seedr…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_seedr_hardsub_pipeline(client, st, url, uid))
        return


# ─────────────────────────────────────────────────────────────
# The pipeline
# ─────────────────────────────────────────────────────────────

_VIDEO_EXTS = frozenset({
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
    ".ts", ".m2ts", ".wmv", ".m4v",
})


async def _seedr_hardsub_pipeline(
    client: Client, st, magnet: str, uid: int,
) -> None:
    from services.seedr import fetch_urls_via_seedr
    from services import ffmpeg as FF

    tmp = make_tmp(cfg.download_dir, uid)
    start = time.time()

    async def _progress(stage: str, pct: float, detail: str) -> None:
        from services.utils import human_dur
        elapsed = human_dur(int(time.time() - start))
        icons = {
            "selecting": "🔍", "submitting": "⬆️", "waiting": "⏳",
            "downloading": "☁️", "fetching": "🔗",
        }
        icon   = icons.get(stage, "⏳")
        filled = int(pct / 10)
        bar    = "█" * filled + "░" * (10 - filled)
        await safe_edit(
            st,
            f"🔥 <b>Seedr → Hardsub</b>\n"
            "──────────────────────\n\n"
            f"{icon} <i>{detail}</i>\n\n"
            f"<code>[{bar}]</code>  <b>{pct:.0f}%</b>  ·  ⏱ <i>{elapsed}</i>\n\n"
            "<i>Seedr fetches at datacenter speed.\n"
            "CDN link will be sent directly to CloudConvert.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    try:
        files, folder_id, seedr_user, seedr_pwd = await fetch_urls_via_seedr(
            magnet, progress_cb=_progress, timeout_s=7200,
        )
    except Exception as exc:
        cleanup(tmp)
        log.error("[SeedrHS] Seedr failed: %s", exc, exc_info=True)
        return await safe_edit(
            st,
            f"❌ <b>Seedr failed</b>\n\n<code>{str(exc)[:300]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    if not files:
        cleanup(tmp)
        return await safe_edit(
            st,
            "❌ <b>Seedr: no files found.</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    # Pick the largest video file
    video_files = [f for f in files
                   if os.path.splitext(f["name"])[1].lower() in _VIDEO_EXTS]
    if not video_files:
        video_files = files

    best      = max(video_files, key=lambda f: f.get("size", 0))
    fname     = best["clean_name"]
    fsize     = best.get("size", 0)
    video_url = best["url"]

    log.info("[SeedrHS] Phase 2: video=%s (%s) — probing via partial download", fname, human_size(fsize))

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"✅ Seedr ready — CDN link obtained\n"
        f"📁 <code>{fname[:45]}</code>  <code>{human_size(fsize)}</code>\n\n"
        "🔍 <i>Probing streams for French subtitle…\n"
        "(downloading first 50 MB for stream analysis)</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # Download only what's needed for ffprobe (first 50 MB is enough for MKV headers + sub tracks)
    probe_path = video_url  # fallback: probe directly by URL
    try:
        import aiohttp as _aiohttp
        _probe_fname = fname
        _probe_fpath = os.path.join(tmp, _probe_fname)
        _timeout     = _aiohttp.ClientTimeout(total=300)
        async with _aiohttp.ClientSession(timeout=_timeout) as _sess:
            async with _sess.get(video_url, headers={"User-Agent": "Mozilla/5.0"},
                                  allow_redirects=True) as _resp:
                _resp.raise_for_status()
                _MAX = 50 * 1024 * 1024  # 50 MB cap
                _done = 0
                with open(_probe_fpath, "wb") as _fh:
                    async for _chunk in _resp.content.iter_chunked(1024 * 1024):
                        _fh.write(_chunk)
                        _done += len(_chunk)
                        if _done >= _MAX:
                            break  # enough for stream headers
        probe_path = _probe_fpath
        log.info("[SeedrHS] Partial download for probe: %s bytes -> %s", _done, _probe_fpath)
    except Exception as exc:
        log.warning("[SeedrHS] Partial download for probe failed: %s — probing URL directly", exc)

    try:
        sd = await FF.probe_streams(probe_path)
    except Exception as exc:
        cleanup(tmp)
        try:
            from services.seedr import _del_folder
            await _del_folder(seedr_user, seedr_pwd, folder_id)
        except Exception:
            pass
        return await safe_edit(
            st,
            f"❌ <b>Stream probe failed</b>\n\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    all_subs = sd.get("subtitle", [])
    french_text, french_bitmap = _find_french_subs(all_subs)

    v_count = len(sd.get("video", []))
    a_count = len(sd.get("audio", []))
    s_count = len(all_subs)

    log.info(
        "[SeedrHS] Streams: %d video, %d audio, %d sub "
        "(%d French text, %d French bitmap)",
        v_count, a_count, s_count,
        len(french_text), len(french_bitmap),
    )

    # Case A: French text subtitle found — extract from probe file, then send video_url to CC
    if french_text:
        best = french_text[0]
        await _auto_hardsub_url(client, st, probe_path, video_url, fname, fsize,
                                 best, tmp, uid, folder_id, seedr_user, seedr_pwd)
        return

    # Case B: French bitmap subtitle (PGS/DVB) — can't use
    if french_bitmap:
        b = french_bitmap[0]
        b_codec = (b.get("codec_name") or "PGS").upper()
        b_idx   = b.get("index", "?")

        _evict_waiting_subs()
        _WAITING_SUB[uid] = {
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

        return await safe_edit(
            st,
            f"⚠️ <b>French subtitle found — but bitmap</b>\n"
            "──────────────────────\n\n"
            f"📁 <code>{fname[:40]}</code>\n"
            f"💬 French sub: <code>#{b_idx} {b_codec}</code>\n\n"
            f"<b>{b_codec}</b> is a bitmap format (image-based).\n"
            "CloudConvert needs <b>text-based</b> subtitles\n"
            "(.ass / .srt) for the FFmpeg subtitles filter.\n\n"
            "──────────────────────\n\n"
            "Send me a <b>French text subtitle</b>:\n"
            "• A <b>.ass / .srt / .vtt file</b>\n"
            "• A <b>URL</b> to a subtitle file\n\n"
            "<i>Video is on Seedr — no re-download needed.\n"
            "Send /cancel to abort.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    # Case C: No French subtitle at all
    sub_lines = []
    for s in all_subs:
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()
        codec = (s.get("codec_name") or "?").upper()
        idx   = s.get("index", "?")
        title = tags.get("title", "")
        flag  = lang_flag(lang)
        lname = lang_name(lang)
        forced_s = " ⚡Forced" if str(tags.get("forced", "0")).lower() in ("1", "true", "yes") else ""
        line = f"  #{idx} {flag} {lname} [{codec}]{forced_s}"
        if title:
            line += f" — {title}"
        sub_lines.append(line)

    sub_info = "\n".join(sub_lines) if sub_lines else "  <i>No subtitle tracks found</i>"

    _evict_waiting_subs()
    _WAITING_SUB[uid] = {
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

    await safe_edit(
        st,
        f"⚠️ <b>No French subtitle found</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:40]}</code>\n"
        f"💾 <code>{human_size(fsize)}</code>\n"
        f"🎬 {v_count} video · 🎵 {a_count} audio · 💬 {s_count} sub\n\n"
        f"<b>Available subtitles:</b>\n{sub_info}\n\n"
        "──────────────────────\n\n"
        "Send me a <b>French subtitle</b>:\n"
        "• A <b>.ass / .srt / .vtt file</b>\n"
        "• A <b>URL</b> to a subtitle file\n\n"
        "<i>Video is on Seedr — no re-download needed.\n"
        "Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# Auto-hardsub URL: extract sub from probe file, submit video_url to CC
# ─────────────────────────────────────────────────────────────

async def _auto_hardsub_url(
    client: Client, st,
    probe_path: str,        # local partial file used only for sub extraction
    video_url: str,         # Seedr CDN URL sent directly to CloudConvert
    fname: str,
    fsize: int,
    sub_stream: dict,
    tmp: str,
    uid: int,
    folder_id: int,
    seedr_user: str,
    seedr_pwd: str,
) -> None:
    from services import ffmpeg as FF

    idx    = sub_stream.get("index", 0)
    codec  = (sub_stream.get("codec_name") or "ass").lower()
    ext    = FF.subtitle_ext(codec)
    tags   = sub_stream.get("tags", {}) or {}
    title  = tags.get("title", "")
    forced = sub_stream.get("_is_forced", False)

    sub_path  = os.path.join(tmp, f"french_sub{ext}")
    sub_fname = os.path.basename(sub_path)

    forced_s = " (Forced)" if forced else ""
    detail_s = f"#{idx} {codec.upper()}{forced_s}"
    if title:
        detail_s += f" — {title}"

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:40]}</code>\n"
        f"✅ French sub: <code>{detail_s}</code>\n\n"
        "📤 <i>Extracting subtitle from probe file…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        await FF.stream_op(probe_path, sub_path, [
            "-map", f"0:{idx}", "-c", "copy",
        ])
    except Exception as exc:
        cleanup(tmp)
        try:
            from services.seedr import _del_folder
            await _del_folder(seedr_user, seedr_pwd, folder_id)
        except Exception:
            pass
        log.error("[SeedrHS] Sub extraction failed: %s", exc)
        return await safe_edit(
            st,
            f"❌ <b>Subtitle extraction failed</b>\n\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    if not os.path.isfile(sub_path) or os.path.getsize(sub_path) < 10:
        cleanup(tmp)
        try:
            from services.seedr import _del_folder
            await _del_folder(seedr_user, seedr_pwd, folder_id)
        except Exception:
            pass
        return await safe_edit(
            st,
            "❌ <b>Extracted subtitle is empty.</b>\n\n"
            "Try sending a subtitle file manually.",
            parse_mode=enums.ParseMode.HTML,
        )

    sub_size = os.path.getsize(sub_path)
    log.info("[SeedrHS] Extracted: %s (%s) — submitting via CDN URL", sub_fname, human_size(sub_size))

    await _submit_to_cc_url(
        client, st, video_url, fname, sub_path, sub_fname,
        detail_s, tmp, uid, folder_id, seedr_user, seedr_pwd,
    )


# ─────────────────────────────────────────────────────────────
# Submit CDN URL + subtitle to CloudConvert (URL-based, no video upload)
# ─────────────────────────────────────────────────────────────

async def _submit_to_cc_url(
    client: Client, st,
    video_url: str,         # ← Seedr CDN URL sent directly to CloudConvert
    video_fname: str,
    sub_path: str,
    sub_fname: str,
    sub_detail: str,
    tmp: str,
    uid: int,
    folder_id: int,
    seedr_user: str,
    seedr_pwd: str,
) -> None:
    from services.cloudconvert_api import submit_hardsub, parse_api_keys, pick_best_key
    from services.cc_job_store import cc_job_store, CCJob
    from services.cc_sanitize import build_cc_output_name
    from services.task_runner import tracker, TaskRecord
    from services.utils import human_size
    import time as _time

    api_key     = os.environ.get("CC_API_KEY", "").strip()
    output_name = build_cc_output_name(video_fname, "VOSTFR")

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{video_fname[:38]}</code>\n"
        f"💬 <code>{sub_detail[:38]}</code>\n"
        f"📦 → <code>{output_name[:38]}</code>\n\n"
        "☁️ <i>Submitting to CloudConvert…\n"
        "Video URL sent directly — only subtitle uploaded!</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    ul_tid = None
    try:
        ul_tid = tracker.new_tid()
        ul_rec = TaskRecord(
            tid=ul_tid, user_id=uid,
            label=f"CC↑ {video_fname} (sub only)",
            fname=video_fname,
            mode="ul", engine="http",
            state="☁️ Uploading subtitle to CC",
            total=os.path.getsize(sub_path),
        )
        await tracker.register(ul_rec)

        sub_size  = os.path.getsize(sub_path)
        _ul_start = _time.time()

        async def _upload_progress(phase: str, done: int, total: int) -> None:
            elapsed = _time.time() - _ul_start
            speed   = done / elapsed if elapsed else 0.0
            eta     = int((sub_size - done) / speed) if (speed and sub_size > done) else 0
            await tracker.update(
                ul_tid,
                state=f"☁️ Sub {human_size(done)}/{human_size(sub_size)}",
                done=done, total=sub_size,
                speed=speed, eta=eta, elapsed=elapsed,
            )

        keys = parse_api_keys(api_key)
        selected, credits = await pick_best_key(keys)
        key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits)"

        job_id = await submit_hardsub(
            api_key,
            video_url=video_url,        # ← CDN URL, CloudConvert pulls directly
            subtitle_path=sub_path,     # ← small subtitle file uploaded
            output_name=output_name,
            upload_progress_cb=_upload_progress,
        )

        await tracker.finish(ul_tid, success=True)

        await cc_job_store.add(CCJob(
            job_id=job_id,
            uid=uid,
            fname=video_fname,
            sub_fname=sub_fname,
            output_name=output_name,
            status="processing",
        ))

        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception:
            pass

        log.info(
            "[SeedrHS] CC job submitted via URL: %s  video=%s  sub=%s  out=%s",
            job_id, video_fname, sub_fname, output_name,
        )

        await safe_edit(
            st,
            f"✅ <b>Seedr → Hardsub — Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📁 <code>{video_fname[:36]}</code>\n"
            f"💬 <code>{sub_detail[:36]}</code>\n"
            f"📦 → <code>{output_name[:36]}</code>\n"
            f"{key_info}\n\n"
            "⏳ <i>CloudConvert is processing…\n"
            "The hardsubbed MP4 will auto-upload\n"
            "to this chat when ready (~3-5 min).</i>\n\n"
            "📋 Use /ccstatus to track progress.",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        if ul_tid is not None:
            try:
                await tracker.finish(ul_tid, success=False, msg=str(exc)[:60])
            except Exception:
                pass
        log.error("[SeedrHS] CC submit failed: %s", exc, exc_info=True)
        await safe_edit(
            st,
            f"❌ <b>CloudConvert submission failed</b>\n\n"
            f"<code>{str(exc)[:250]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
    finally:
        # Clean up Seedr folder — CloudConvert has the URL
        try:
            from services.seedr import _del_folder
            await _del_folder(seedr_user, seedr_pwd, folder_id)
            log.info("[SeedrHS] Seedr folder %d cleaned up.", folder_id)
        except Exception as e:
            log.warning("[SeedrHS] Seedr cleanup (non-fatal): %s", e)
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Legacy: Submit video + subtitle to CloudConvert (local file upload)
# Kept for backward compatibility; prefer _submit_to_cc_url
# ─────────────────────────────────────────────────────────────

async def _submit_to_cc(
    client: Client, st,
    video_path: str, video_fname: str,
    sub_path: str, sub_fname: str,
    sub_detail: str,
    tmp: str, uid: int,
) -> None:
    from services.cloudconvert_api import submit_hardsub, parse_api_keys, pick_best_key
    from services.cc_job_store import cc_job_store, CCJob
    from services.cc_sanitize import build_cc_output_name
    from services.task_runner import tracker, TaskRecord
    from services.utils import human_size
    import time as _time

    api_key     = os.environ.get("CC_API_KEY", "").strip()
    output_name = build_cc_output_name(video_fname, "VOSTFR")

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{video_fname[:38]}</code>\n"
        f"💬 <code>{sub_detail[:38]}</code>\n"
        f"📦 → <code>{output_name[:38]}</code>\n\n"
        "☁️ <i>Uploading to CloudConvert…\n"
        "(video + subtitle files)</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    ul_tid = None
    try:
        ul_tid = tracker.new_tid()
        ul_rec = TaskRecord(
            tid=ul_tid, user_id=uid,
            label=f"CC↑ {video_fname}",
            fname=video_fname,
            mode="ul", engine="http",
            state="☁️ Uploading to CC",
            total=os.path.getsize(video_path) + os.path.getsize(sub_path),
        )
        await tracker.register(ul_rec)

        sub_size   = os.path.getsize(sub_path)
        vid_size   = os.path.getsize(video_path)
        _ul_start  = _time.time()

        async def _upload_progress(phase: str, done: int, total: int) -> None:
            if phase == "sub":
                pct_label = f"📄 Sub {human_size(done)}/{human_size(sub_size)}"
                ul_done = done
                ul_total = sub_size + vid_size
            else:
                pct_label = f"🎬 Video {human_size(done)}/{human_size(vid_size)}"
                ul_done = sub_size + done
                ul_total = sub_size + vid_size
            elapsed = _time.time() - _ul_start
            speed   = ul_done / elapsed if elapsed else 0.0
            eta     = int((ul_total - ul_done) / speed) if (speed and ul_total > ul_done) else 0
            await tracker.update(
                ul_tid,
                state=f"☁️ {pct_label}",
                done=ul_done, total=ul_total,
                speed=speed, eta=eta, elapsed=elapsed,
            )

        keys = parse_api_keys(api_key)
        if len(keys) > 1:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits)"
        else:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 {credits} credits remaining"

        job_id = await submit_hardsub(
            api_key,
            video_path=video_path,
            subtitle_path=sub_path,
            output_name=output_name,
            upload_progress_cb=_upload_progress,
        )

        await tracker.finish(ul_tid, success=True)

        await cc_job_store.add(CCJob(
            job_id=job_id,
            uid=uid,
            fname=video_fname,
            sub_fname=sub_fname,
            output_name=output_name,
            status="processing",
        ))

        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception:
            pass

        log.info(
            "[SeedrHS] CC job submitted: %s  video=%s  sub=%s  out=%s",
            job_id, video_fname, sub_fname, output_name,
        )

        await safe_edit(
            st,
            f"✅ <b>Seedr → Hardsub — Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📁 <code>{video_fname[:36]}</code>\n"
            f"💬 <code>{sub_detail[:36]}</code>\n"
            f"📦 → <code>{output_name[:36]}</code>\n"
            f"{key_info}\n\n"
            "⏳ <i>CloudConvert is processing…\n"
            "The hardsubbed MP4 will auto-upload\n"
            "to this chat when ready (~3-5 min).</i>\n\n"
            "📋 Use /ccstatus to track progress.",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        if ul_tid is not None:
            try:
                await tracker.finish(ul_tid, success=False, msg=str(exc)[:60])
            except Exception:
                pass
        log.error("[SeedrHS] CC submit failed: %s", exc, exc_info=True)
        await safe_edit(
            st,
            f"❌ <b>CloudConvert submission failed</b>\n\n"
            f"<code>{str(exc)[:250]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Manual subtitle receivers (file + URL)
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.document,
    group=-2,
)
async def shs_manual_sub_file(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _WAITING_SUB.get(uid)
    if not state:
        return

    media = msg.document
    if not media:
        return

    doc_fname = getattr(media, "file_name", None) or "subtitle.ass"
    ext = os.path.splitext(doc_fname)[1].lower()

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

    _WAITING_SUB.pop(uid, None)

    sub_fname = os.path.basename(sub_path)
    log.info("[SeedrHS] Manual sub received: %s", sub_fname)

    if video_url and folder_id and seedr_user and seedr_pwd:
        await _submit_to_cc_url(
            client, st, video_url, video_fname,
            sub_path, sub_fname, sub_fname,
            tmp, uid, folder_id, seedr_user, seedr_pwd,
        )
    else:
        await _submit_to_cc(
            client, st,
            video_path, video_fname,
            sub_path, sub_fname, sub_fname,
            tmp, uid,
        )
    msg.stop_propagation()


@Client.on_message(
    filters.private & filters.text
    & ~filters.command([
        "start", "help", "settings", "info", "status", "log", "restart",
        "broadcast", "admin", "ban_user", "unban_user", "banned_list",
        "cancel", "show_thumb", "del_thumb", "json_formatter", "bulk_url",
        "hardsub", "botname", "ccstatus", "convert", "resize", "compress",
        "usage", "captiontemplate", "stream", "forward",
        "createarchive", "archiveddone", "mergedone",
    ]),
    group=-2,
)
async def shs_manual_sub_url(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _WAITING_SUB.get(uid)
    if not state:
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

    st = await msg.reply(
        f"⬇️ Downloading subtitle…\n<code>{text[:60]}</code>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        timeout = aiohttp.ClientTimeout(total=60)
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(text, headers=headers, allow_redirects=True) as resp:
                resp.raise_for_status()
                content = await resp.read()

                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    raw = cd.split("filename=")[-1].strip().strip('"').strip("'")
                    raw = _up.unquote_plus(raw) if raw else ""
                else:
                    raw = ""

        if not raw:
            raw = os.path.basename(_up.urlparse(text).path)
            raw = _up.unquote_plus(raw) if raw else "subtitle.ass"

        ext = os.path.splitext(raw)[1].lower()
        if ext not in _SUB_EXTS:
            raw = raw + ".ass"

        raw = re.sub(r'[\\/:*?"<>|]', "_", raw)

        sub_path = os.path.join(tmp, raw)
        with open(sub_path, "wb") as f:
            f.write(content)

        if len(content) > 10_000_000:
            return await safe_edit(st, "❌ File too large — not a subtitle.")

    except Exception as exc:
        return await safe_edit(
            st,
            f"❌ Subtitle download failed:\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    _WAITING_SUB.pop(uid, None)

    sub_fname = os.path.basename(sub_path)
    log.info("[SeedrHS] Manual sub from URL: %s", sub_fname)

    if video_url and folder_id and seedr_user and seedr_pwd:
        await _submit_to_cc_url(
            client, st, video_url, video_fname,
            sub_path, sub_fname, sub_fname,
            tmp, uid, folder_id, seedr_user, seedr_pwd,
        )
    else:
        await _submit_to_cc(
            client, st,
            video_path, video_fname,
            sub_path, sub_fname, sub_fname,
            tmp, uid,
        )
    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# /cancel handler for Seedr+Hardsub flow
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("cancel"), group=-2)
async def shs_cancel(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _WAITING_SUB:
        return
    _clear_waiting(uid)
    await msg.reply("❌ Seedr+Hardsub cancelled. Video files cleaned up.")
    msg.stop_propagation()
