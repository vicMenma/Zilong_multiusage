"""
plugins/seedr_hardsub.py
Seedr → Auto-Hardsub pipeline.

PATCH v2 — direct import/url to CloudConvert, no local video download
───────────────────────────────────────────────────────────────────────
Steps 6+7 previously:
  6. Download full video from Seedr CDN locally (hundreds of MB, minutes)
  7. Upload that local file to CloudConvert via import/upload

Steps 6+7 now:
  6. Submit Seedr CDN URL directly to CloudConvert via import/url
     (CC fetches at datacenter speed, no local copy needed)
  7. Fallback: only if CC rejects the URL (403 / network error) do we
     fall back to the old local-download path

This works because:
  • Seedr CDN URLs are token+expiry signed, NOT IP-restricted
  • _collect_files() (seedr.py v6) already renamed the file to a clean
    name before returning the URL, so CC's ffmpeg parser sees no brackets
    or spaces in the path

FIX C-05 (audit v3): _WAITING_SUB now has 30-minute TTL eviction.
MAIN-06 addition: smon| callback handlers for Seedr monitor buttons.
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
_WAITING_SUB_TTL = 1800  # 30 min


def _clear_waiting(uid: int) -> None:
    state = _WAITING_SUB.pop(uid, None)
    if not state:
        return
    if state.get("tmp"):
        cleanup(state["tmp"])
    folder_id  = state.get("folder_id", 0)
    seedr_user = state.get("seedr_user", "")
    seedr_pwd  = state.get("seedr_pwd", "")
    if folder_id and seedr_user:
        async def _deferred_del():
            try:
                from services.seedr import _del_folder
                await _del_folder(seedr_user, seedr_pwd, folder_id)
                log.info("[SeedrHS] Seedr folder %d cleaned on cancel/evict", folder_id)
            except Exception as e:
                log.warning("[SeedrHS] Seedr cleanup on cancel (non-fatal): %s", e)
        try:
            asyncio.get_event_loop().create_task(_deferred_del())
        except RuntimeError:
            pass


def _evict_waiting_subs() -> None:
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
    """
    Full pipeline — v3: no probe download, use embedded subtitle directly.

    OLD flow:
      1. Seedr → CDN URL
      2. Download 50 MB probe
      3. ffprobe on local probe file → find French sub index
      4. Extract subtitle to .ass via ffmpeg
      5. Upload .ass to CC + pass video CDN URL
      6. CC burns external .ass onto video

    NEW flow (v3):
      1. Seedr → CDN URL  (renamed to clean name in seedr.py v6)
      2. ffprobe on CDN URL directly (no download — reads only headers)
      3. Find French subtitle stream index (si = position within subtitle tracks)
      4. Submit CDN URL to CC with embedded_sub_si=si
         → CC burns the embedded subtitle track directly
         → No subtitle extraction, no subtitle upload
      5. Fall back to extract+upload only if no embedded French subtitle
         (user must provide .ass/.srt manually)

    Benefits vs old flow:
      • No 50 MB probe download
      • No subtitle extraction step
      • No subtitle upload to CC (~20 KB saved, but more importantly no
        silent-fail where ffmpeg skips a broken external subtitle file)
      • CC reads the subtitle from its native container stream = reliable
    """
    from services.seedr import fetch_urls_via_seedr, _del_folder
    from services import ffmpeg as FF
    from services.cloudconvert_api import (
        submit_hardsub, parse_api_keys, pick_best_key,
    )
    from services.cc_job_store import cc_job_store, CCJob
    from services.cc_sanitize import build_cc_output_name

    tmp = make_tmp(cfg.download_dir, uid)

    # ── Step 1: Seedr ─────────────────────────────────────────────────────────
    async def _seedr_progress(stage: str, pct: float, detail: str) -> None:
        icons  = {"selecting": "🔍", "submitting": "⬆️",
                  "waiting": "⏳", "downloading": "☁️", "fetching": "🔗"}
        filled = int(pct / 10)
        bar    = "█" * filled + "░" * (10 - filled)
        await safe_edit(
            st,
            f"🔥 <b>Seedr → Hardsub</b>\n"
            "──────────────────────\n\n"
            f"{icons.get(stage, '⏳')} <i>{detail}</i>\n\n"
            f"<code>[{bar}]</code>  <b>{pct:.0f}%</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    try:
        files, folder_id, seedr_user, seedr_pwd = await fetch_urls_via_seedr(
            magnet, progress_cb=_seedr_progress, timeout_s=7200,
        )
    except Exception as exc:
        cleanup(tmp)
        log.error("[SeedrHS] Seedr failed: %s", exc, exc_info=True)
        return await safe_edit(
            st, f"❌ <b>Seedr failed</b>\n\n<code>{str(exc)[:300]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    video_files = [f for f in files
                   if os.path.splitext(f.get("name",""))[1].lower() in _VIDEO_EXTS]
    best      = max(video_files or files, key=lambda f: f.get("size", 0))
    fname     = best.get("clean_name") or best["name"]
    fsize     = best.get("size", 0)
    video_url = best["url"]
    log.info("[SeedrHS] CDN URL: %s (%s)", fname, human_size(fsize))

    # ── Step 2: ffprobe on CDN URL directly — no download ────────────────────
    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"✅ CDN link obtained\n"
        f"📁 <code>{fname[:45]}</code>  <i>{human_size(fsize)}</i>\n\n"
        "🔍 <i>Probing subtitle streams…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        sd = await FF.probe_streams(video_url)
    except Exception as exc:
        cleanup(tmp)
        await _del_folder(seedr_user, seedr_pwd, folder_id)
        return await safe_edit(
            st, f"❌ <b>Stream probe failed</b>\n\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    all_subs = sd.get("subtitle", [])
    french_text, french_bitmap = _find_french_subs(all_subs)
    log.info("[SeedrHS] %dv %da %ds | %d FR text, %d FR bitmap",
             len(sd.get("video",[])), len(sd.get("audio",[])), len(all_subs),
             len(french_text), len(french_bitmap))

    # ── Step 3: No French text sub → ask user ─────────────────────────────────
    if not french_text:
        if french_bitmap:
            b_codec = (french_bitmap[0].get("codec_name") or "PGS").upper()
            msg = (
                f"⚠️ <b>French subtitle is bitmap ({b_codec})</b>\n"
                "──────────────────────\n\n"
                f"📁 <code>{fname[:40]}</code>\n\n"
                f"<b>{b_codec}</b> is image-based — CC needs text (.ass/.srt).\n\n"
                "Send a <b>French .ass / .srt</b> file or URL.\n"
                "<i>Send /cancel to abort.</i>"
            )
        else:
            sub_info = "\n".join(
                f"  #{s.get('index','?')} "
                f"{lang_flag((s.get('tags',{}) or {}).get('language','und'))} "
                f"[{(s.get('codec_name','?')).upper()}]"
                for s in all_subs
            ) or "  <i>none</i>"
            msg = (
                f"⚠️ <b>No French subtitle found</b>\n"
                "──────────────────────\n\n"
                f"📁 <code>{fname[:40]}</code>\n"
                f"🎬 {len(sd.get('video',[]))}v · 🎵 {len(sd.get('audio',[]))}a "
                f"· 💬 {len(all_subs)} sub\n\n"
                f"<b>Available:</b>\n{sub_info}\n\n"
                "Send a <b>French .ass / .srt</b> file or URL.\n"
                "<i>Send /cancel to abort.</i>"
            )
        _evict_waiting_subs()
        _WAITING_SUB[uid] = {
            "video_url":  video_url,
            "video_path": None,
            "fname": fname, "fsize": fsize, "tmp": tmp,
            "folder_id": folder_id, "seedr_user": seedr_user,
            "seedr_pwd": seedr_pwd, "_created": time.time(),
        }
        return await safe_edit(st, msg, parse_mode=enums.ParseMode.HTML)

    # ── Step 4: French text sub found → get subtitle-stream index (si) ────────
    # si = position within subtitle tracks only (0 = first subtitle track)
    # This is what ffmpeg subtitles filter expects for :si=N
    sub_stream = french_text[0]
    sub_si     = all_subs.index(sub_stream)  # 0-based within subtitle streams

    tags   = sub_stream.get("tags", {}) or {}
    codec  = (sub_stream.get("codec_name") or "ass").upper()
    detail = f"#{sub_stream.get('index','?')} {codec}  (si={sub_si})"
    if sub_stream.get("_is_forced"): detail += " Forced"
    if tags.get("title"):            detail += f" — {tags['title']}"

    output_name = build_cc_output_name(fname, "VOSTFR")

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:40]}</code>\n"
        f"💬 French sub: <code>{detail}</code>\n"
        f"📦 → <code>{output_name[:38]}</code>\n\n"
        "☁️ <i>Submitting to CloudConvert…\n"
        "CC burns the embedded subtitle directly — no upload needed</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # ── Step 5: Submit CDN URL + embedded subtitle index to CC ───────────────
    try:
        api_key         = os.environ.get("CC_API_KEY", "").strip()
        keys            = parse_api_keys(api_key)
        selected, creds = await pick_best_key(keys)
        key_info        = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({creds} credits)"

        job_id = await submit_hardsub(
            selected,
            video_url=video_url,         # ← Seedr CDN URL, no local copy
            subtitle_path="",            # ← not used when embedded_sub_si set
            output_name=output_name,
            embedded_sub_si=sub_si,      # ← burn embedded track directly
        )

        await cc_job_store.add(CCJob(
            job_id=job_id, uid=uid,
            fname=fname, sub_fname=f"embedded si={sub_si}", output_name=output_name,
            status="processing",
            seedr_folder_id=folder_id,
            seedr_user=seedr_user, seedr_pwd=seedr_pwd,
        ))
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception:
            pass

        log.info("[SeedrHS] CC job submitted: %s (embedded si=%d)", job_id, sub_si)
        await safe_edit(
            st,
            f"✅ <b>Submitted to CloudConvert!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📁 <code>{fname[:36]}</code>\n"
            f"💬 <code>{detail[:36]}</code>\n"
            f"📦 → <code>{output_name[:36]}</code>\n"
            f"{key_info}\n\n"
            "⏳ <i>Processing (~3-5 min)…\n"
            "Hardsubbed MP4 will be sent here when ready.</i>\n\n"
            "📋 /ccstatus to check progress.",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        log.error("[SeedrHS] CC submit failed: %s", exc, exc_info=True)
        await safe_edit(
            st,
            f"❌ <b>CloudConvert submission failed</b>\n\n<code>{str(exc)[:250]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    cleanup(tmp)



async def _submit_to_cc_url_with_fallback(
    client:     Client,
    st,
    video_url:  str,
    video_fname: str,
    fsize:      int,
    sub_path:   str,
    sub_fname:  str,
    sub_detail: str,
    tmp:        str,
    uid:        int,
    folder_id:  int,
    seedr_user: str,
    seedr_pwd:  str,
) -> None:
    """
    Submit Seedr CDN URL + subtitle to CloudConvert via import/url.

    Only the subtitle file is uploaded (typically ~20 KB).
    CC fetches the video directly from Seedr — no local download needed.

    Falls back to local download + import/upload if CC returns an error
    fetching the video URL (network issue, token expired, etc.).
    """
    from services.cloudconvert_api import (
        submit_hardsub, parse_api_keys, pick_best_key,
    )
    from services.cc_job_store import cc_job_store, CCJob
    from services.cc_sanitize import build_cc_output_name
    from services.task_runner import tracker, TaskRecord
    import time as _t

    api_key     = os.environ.get("CC_API_KEY", "").strip()
    output_name = build_cc_output_name(video_fname, "VOSTFR")

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{video_fname[:38]}</code>  <i>{human_size(fsize)}</i>\n"
        f"💬 <code>{sub_detail[:38]}</code>\n"
        f"📦 → <code>{output_name[:38]}</code>\n\n"
        "☁️ <i>Submitting Seedr URL to CloudConvert…\n"
        "Only the subtitle is uploaded (~20 KB)</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    ul_tid = None
    job_id = None

    try:
        keys            = parse_api_keys(api_key)
        selected, creds = await pick_best_key(keys)
        key_info        = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({creds} credits)"

        # Track subtitle upload only
        ul_tid   = tracker.new_tid()
        sub_size = os.path.getsize(sub_path)
        _ul_start = _t.time()
        await tracker.register(TaskRecord(
            tid=ul_tid, user_id=uid,
            label=f"CC↑ sub {sub_fname}",
            fname=sub_fname, mode="ul", engine="http",
            state="☁️ Uploading subtitle to CC",
            total=sub_size,
        ))

        async def _ul_progress(phase: str, done: int, total: int) -> None:
            elapsed = _t.time() - _ul_start
            speed   = done / elapsed if elapsed else 0.0
            await tracker.update(
                ul_tid,
                state=f"☁️ Sub {human_size(done)}/{human_size(sub_size)}",
                done=done, total=sub_size,
                speed=speed,
                eta=int((sub_size - done) / speed) if speed and done < sub_size else 0,
                elapsed=elapsed,
            )

        job_id = await submit_hardsub(
            selected,
            video_url=video_url,        # ← Seedr CDN URL, CC fetches directly
            subtitle_path=sub_path,     # ← subtitle uploaded (~20 KB)
            output_name=output_name,
            upload_progress_cb=_ul_progress,
        )
        await tracker.finish(ul_tid, success=True)
        ul_tid = None

        await cc_job_store.add(CCJob(
            job_id=job_id, uid=uid,
            fname=video_fname, sub_fname=sub_fname, output_name=output_name,
            status="processing",
            seedr_folder_id=folder_id,
            seedr_user=seedr_user, seedr_pwd=seedr_pwd,
        ))
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception:
            pass

        log.info("[SeedrHS] CC job submitted via import/url: %s → %s", job_id, output_name)
        await safe_edit(
            st,
            f"✅ <b>Submitted to CloudConvert!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📁 <code>{video_fname[:36]}</code>\n"
            f"💬 <code>{sub_detail[:36]}</code>\n"
            f"📦 → <code>{output_name[:36]}</code>\n"
            f"{key_info}\n\n"
            "⏳ <i>Processing (~3-5 min)…\n"
            "Hardsubbed MP4 will be sent here when ready.</i>\n\n"
            "📋 /ccstatus to check progress.",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        if ul_tid is not None:
            try:
                await tracker.finish(ul_tid, success=False, msg=str(exc)[:60])
            except Exception:
                pass

        log.warning("[SeedrHS] import/url failed (%s) — falling back to local download", exc)

        # ── Fallback: download video locally then upload to CC ────────────────
        await safe_edit(
            st,
            f"⚠️ <b>Direct URL failed — downloading locally…</b>\n"
            "──────────────────────\n\n"
            f"📁 <code>{video_fname[:40]}</code>  <i>{human_size(fsize)}</i>\n\n"
            f"<i>Error: {str(exc)[:120]}</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        video_path = os.path.join(tmp, video_fname)
        try:
            _dl_start = time.time()
            _timeout  = aiohttp.ClientTimeout(total=3600)
            async with aiohttp.ClientSession(timeout=_timeout) as _sess:
                async with _sess.get(
                    video_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    allow_redirects=True,
                ) as _resp:
                    _resp.raise_for_status()
                    _total = int(_resp.headers.get("Content-Length", fsize) or fsize)
                    _done  = 0
                    _last_edit = 0.0
                    with open(video_path, "wb") as _fh:
                        async for _chunk in _resp.content.iter_chunked(2 * 1024 * 1024):
                            _fh.write(_chunk)
                            _done += len(_chunk)
                            _now   = time.time()
                            if _now - _last_edit >= 4:
                                _last_edit = _now
                                _elapsed = _now - _dl_start
                                _speed   = _done / _elapsed if _elapsed else 0
                                _pct     = (_done / _total * 100) if _total else 0
                                _eta     = int((_total - _done) / _speed) if _speed else 0
                                from services.utils import human_dur
                                await safe_edit(
                                    st,
                                    f"🔥 <b>Seedr → Hardsub</b>\n"
                                    "──────────────────────\n\n"
                                    f"📁 <code>{video_fname[:40]}</code>\n\n"
                                    f"⬇️ <b>{human_size(_done)}</b> / {human_size(_total)}"
                                    f"  <i>({_pct:.0f}%)</i>\n"
                                    f"🚀 {human_size(int(_speed))}/s  "
                                    f"⏱ ETA {human_dur(_eta)}",
                                    parse_mode=enums.ParseMode.HTML,
                                )
        except Exception as dl_exc:
            cleanup(tmp)
            await _del_folder(seedr_user, seedr_pwd, folder_id)
            log.error("[SeedrHS] Fallback download also failed: %s", dl_exc)
            return await safe_edit(
                st,
                f"❌ <b>Both direct URL and local download failed</b>\n\n"
                f"URL error: <code>{str(exc)[:150]}</code>\n"
                f"Download error: <code>{str(dl_exc)[:150]}</code>",
                parse_mode=enums.ParseMode.HTML,
            )

        # Upload locally-downloaded video to CC
        await _submit_to_cc(
            client, st,
            video_path, video_fname,
            sub_path, sub_fname, sub_detail,
            tmp, uid,
            folder_id=folder_id,
            seedr_user=seedr_user, seedr_pwd=seedr_pwd,
        )
        return

    cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Submit CDN URL + subtitle to CloudConvert (legacy local-file path)
# ─────────────────────────────────────────────────────────────

async def _submit_to_cc_url(
    client: Client, st,
    video_url: str,
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
    """Kept for backward compat — now delegates to _submit_to_cc_url_with_fallback."""
    await _submit_to_cc_url_with_fallback(
        client, st,
        video_url, video_fname, 0,
        sub_path, sub_fname, sub_detail,
        tmp, uid, folder_id, seedr_user, seedr_pwd,
    )


async def _submit_to_cc(
    client: Client, st,
    video_path: str, video_fname: str,
    sub_path: str, sub_fname: str,
    sub_detail: str,
    tmp: str, uid: int,
    folder_id: int = 0,
    seedr_user: str = "",
    seedr_pwd:  str = "",
) -> None:
    """Local-file upload to CloudConvert (fallback path)."""
    from services.cloudconvert_api import submit_hardsub, parse_api_keys, pick_best_key
    from services.cc_job_store import cc_job_store, CCJob
    from services.cc_sanitize import build_cc_output_name
    from services.task_runner import tracker, TaskRecord
    import time as _t

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
        _ul_start  = _t.time()

        async def _upload_progress(phase: str, done: int, total: int) -> None:
            if phase == "sub":
                pct_label = f"📄 Sub {human_size(done)}/{human_size(sub_size)}"
                ul_done = done
                ul_total = sub_size + vid_size
            else:
                pct_label = f"🎬 Video {human_size(done)}/{human_size(vid_size)}"
                ul_done = sub_size + done
                ul_total = sub_size + vid_size
            elapsed = _t.time() - _ul_start
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
            selected,
            video_path=video_path,
            subtitle_path=sub_path,
            output_name=output_name,
            upload_progress_cb=_upload_progress,
        )

        await tracker.finish(ul_tid, success=True)
        ul_tid = None

        await cc_job_store.add(CCJob(
            job_id=job_id,
            uid=uid,
            fname=video_fname,
            sub_fname=sub_fname,
            output_name=output_name,
            status="processing",
            seedr_folder_id=folder_id,
            seedr_user=seedr_user,
            seedr_pwd=seedr_pwd,
        ))

        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception:
            pass

        log.info("[SeedrHS] CC job submitted (local): %s  video=%s  sub=%s  out=%s",
                 job_id, video_fname, sub_fname, output_name)

        await safe_edit(
            st,
            f"✅ <b>Seedr → Hardsub — Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"📁 <code>{video_fname[:36]}</code>\n"
            f"💬 <code>{sub_detail[:36]}</code>\n"
            f"📦 → <code>{output_name[:36]}</code>\n"
            f"{key_info}\n\n"
            "⏳ <i>Processing (~3-5 min)…\n"
            "The hardsubbed MP4 will auto-upload\n"
            "to this chat when ready.</i>\n\n"
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
    video_url   = state.get("video_url")
    video_fname = state["fname"]
    fsize       = state.get("fsize", 0)
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
        await _submit_to_cc_url_with_fallback(
            client, st, video_url, video_fname, fsize,
            sub_path, sub_fname, sub_fname,
            tmp, uid, folder_id, seedr_user, seedr_pwd,
        )
    else:
        video_path = state.get("video_path")
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
    video_url   = state.get("video_url")
    video_fname = state["fname"]
    fsize       = state.get("fsize", 0)
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
                cl = int(resp.headers.get("Content-Length", 0) or 0)
                if cl > 10_000_000:
                    return await safe_edit(st, "❌ File too large — not a subtitle.")
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
        await _submit_to_cc_url_with_fallback(
            client, st, video_url, video_fname, fsize,
            sub_path, sub_fname, sub_fname,
            tmp, uid, folder_id, seedr_user, seedr_pwd,
        )
    else:
        video_path = state.get("video_path")
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


# ─────────────────────────────────────────────────────────────
# Seedr Monitor button callbacks
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^smon\|"))
async def seedr_monitor_cb(client: Client, cb: CallbackQuery) -> None:
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, action, folder_id_str = parts[:3]
    try:
        folder_id = int(folder_id_str)
    except ValueError:
        return await cb.answer("Invalid folder ID.", show_alert=True)

    uid = cb.from_user.id
    await cb.answer()

    if action == "sub":
        try:
            await cb.message.edit(
                cb.message.text + "\n\n"
                "💬 <b>Waiting for your subtitle file or URL…</b>\n"
                "<i>Send .ass / .srt / .vtt or a direct URL.\n"
                "Send /cancel to abort.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    if action == "upload":
        _WAITING_SUB.pop(uid, None)
        try:
            await cb.message.edit(
                "📤 <b>Uploading original file…</b>\n"
                "<i>Downloading from Seedr CDN — this may take a few minutes.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        asyncio.create_task(_do_upload_as_is(client, cb.message, folder_id, uid))
        return

    if action == "skip":
        _WAITING_SUB.pop(uid, None)
        try:
            from services.seedr_monitor import _pending_upload
            info = _pending_upload.pop(folder_id, None)
            if info and info.get("tmp"):
                cleanup(info["tmp"])
            s_user = (info or {}).get("seedr_user", "")
            s_pwd  = (info or {}).get("seedr_pwd",  "")
            if s_user and folder_id:
                asyncio.create_task(_deferred_seedr_del(s_user, s_pwd, folder_id))
        except Exception:
            pass
        try:
            await cb.message.edit(
                cb.message.text + "\n\n❌ <b>Folder skipped.</b>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return


async def _do_upload_as_is(
    client: Client, status_msg, folder_id: int, uid: int,
) -> None:
    try:
        from services.seedr_monitor import handle_upload_as_is
        await handle_upload_as_is(folder_id, uid, client)
    except Exception as exc:
        log.error("[SeedrMon-CB] upload_as_is: %s", exc)
        try:
            await status_msg.edit(
                f"❌ <b>Upload failed</b>\n<code>{str(exc)[:200]}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass


async def _deferred_seedr_del(user: str, pwd: str, folder_id: int) -> None:
    try:
        from services.seedr import _del_folder
        await _del_folder(user, pwd, folder_id)
        log.info("[SeedrMon-CB] Seedr folder %d cleaned on skip", folder_id)
    except Exception as exc:
        log.warning("[SeedrMon-CB] Seedr cleanup (non-fatal): %s", exc)


# ─────────────────────────────────────────────────────────────
# Public auto-hardsub entry point (called by seedr_monitor.py)
# ─────────────────────────────────────────────────────────────

async def _auto_hardsub_url(
    client:      Client,
    st,
    probe_path:  str,
    video_url:   str,
    fname:       str,
    fsize:       int,
    sub_stream:  dict,
    tmp:         str,
    uid:         int,
    folder_id:   int,
    seedr_user:  str,
    seedr_pwd:   str,
) -> None:
    """
    Called by seedr_monitor when a new folder has a French text subtitle.
    Extracts the sub from probe_path, then submits video_url + sub to CC.
    """
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
        f"🔥 <b>Seedr → Hardsub</b>\n"
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
        from services.seedr import _del_folder
        await _del_folder(seedr_user, seedr_pwd, folder_id)
        return await safe_edit(
            st,
            f"❌ <b>Subtitle extraction failed</b>\n\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    await _submit_to_cc_url_with_fallback(
        client, st,
        video_url, fname, fsize,
        sub_path, sub_fname, detail_s,
        tmp, uid, folder_id, seedr_user, seedr_pwd,
    )
