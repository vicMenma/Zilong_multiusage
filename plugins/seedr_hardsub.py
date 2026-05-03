"""
plugins/seedr_hardsub.py
Seedr → Auto-Hardsub pipeline.

[original docstring preserved — see repo for full description]

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
        import asyncio
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
    Full pipeline (linear, no intermediate state):
      magnet → Seedr CDN URL → 50 MB probe → detect French sub
           → extract sub from probe locally → submit CDN URL + sub to CC
    CloudConvert pulls the video directly from Seedr's CDN.
    Only the subtitle file (~20 KB) is uploaded.
    """
    from services.seedr import fetch_urls_via_seedr, _del_folder
    from services import ffmpeg as FF
    from services.cloudconvert_api import submit_hardsub, parse_api_keys, pick_best_key
    from services.cc_job_store import cc_job_store, CCJob
    from services.cc_sanitize import build_cc_output_name

    tmp   = make_tmp(cfg.download_dir, uid)
    start = time.time()

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
    fname     = best["clean_name"]
    fsize     = best.get("size", 0)
    video_url = best["url"]
    log.info("[SeedrHS] CDN URL: %s (%s)", fname, human_size(fsize))

    # ── Step 2: Download 50 MB probe ─────────────────────────────────────────
    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"✅ CDN link obtained\n"
        f"📁 <code>{fname[:45]}</code>  <i>{human_size(fsize)}</i>\n\n"
        "🔍 <i>Downloading 50 MB probe for subtitle detection…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    probe_path: object = video_url   # fallback to CDN URL if download fails
    try:
        import aiohttp as _aiohttp
        _probe_fpath = os.path.join(tmp, fname)
        _timeout     = _aiohttp.ClientTimeout(total=300)
        async with _aiohttp.ClientSession(timeout=_timeout) as _sess:
            async with _sess.get(
                video_url, headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True,
            ) as _resp:
                _resp.raise_for_status()
                _done = 0
                with open(_probe_fpath, "wb") as _fh:
                    async for _chunk in _resp.content.iter_chunked(1024 * 1024):
                        _fh.write(_chunk)
                        _done += len(_chunk)
                        if _done >= 50 * 1024 * 1024:
                            break
        probe_path = _probe_fpath
        log.info("[SeedrHS] Probe saved: %s", human_size(_done))
    except Exception as exc:
        log.warning("[SeedrHS] Probe download failed (%s) — using CDN URL for probe", exc)

    # ── Step 3: Detect subtitle streams ──────────────────────────────────────
    try:
        sd = await FF.probe_streams(str(probe_path))
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

    # ── Step 4: No French text sub → ask user for manual subtitle ─────────────
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
            "video_url": video_url,
            "video_path": probe_path if os.path.isfile(str(probe_path)) else None,
            "fname": fname, "fsize": fsize, "tmp": tmp,
            "folder_id": folder_id, "seedr_user": seedr_user,
            "seedr_pwd": seedr_pwd, "_created": time.time(),
        }
        return await safe_edit(st, msg, parse_mode=enums.ParseMode.HTML)

    # ── Step 5: Extract French subtitle from local probe file ─────────────────
    sub_stream = french_text[0]
    idx    = sub_stream.get("index", 0)
    codec  = (sub_stream.get("codec_name") or "ass").lower()
    ext    = FF.subtitle_ext(codec)
    tags   = sub_stream.get("tags", {}) or {}
    detail = f"#{idx} {codec.upper()}"
    if sub_stream.get("_is_forced"): detail += " (Forced)"
    if tags.get("title"):            detail += f" — {tags['title']}"

    sub_path  = os.path.join(tmp, f"french_sub{ext}")
    sub_fname = os.path.basename(sub_path)

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:40]}</code>\n"
        f"💬 French sub: <code>{detail}</code>\n\n"
        "📤 <i>Extracting subtitle…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # Extract from local probe — never from CDN URL (that streams the whole file)
    extract_src = str(probe_path) if os.path.isfile(str(probe_path)) else video_url
    try:
        await asyncio.wait_for(
            FF.stream_op(extract_src, sub_path, ["-map", f"0:{idx}", "-c", "copy"]),
            timeout=120,
        )
    except Exception as exc:
        cleanup(tmp)
        await _del_folder(seedr_user, seedr_pwd, folder_id)
        log.error("[SeedrHS] Sub extraction failed: %s", exc)
        return await safe_edit(
            st,
            f"❌ <b>Subtitle extraction failed</b>\n\n<code>{exc}</code>\n\n"
            "Send a .ass / .srt file manually.",
            parse_mode=enums.ParseMode.HTML,
        )

    if not os.path.isfile(sub_path) or os.path.getsize(sub_path) < 10:
        cleanup(tmp)
        await _del_folder(seedr_user, seedr_pwd, folder_id)
        return await safe_edit(
            st, "❌ <b>Extracted subtitle is empty.</b>\n\nSend a .ass/.srt manually.",
            parse_mode=enums.ParseMode.HTML,
        )
    log.info("[SeedrHS] Sub extracted: %s (%s)", sub_fname,
             human_size(os.path.getsize(sub_path)))

    # ── Step 6: Submit CDN URL + subtitle to CloudConvert ─────────────────────
    api_key     = os.environ.get("CC_API_KEY", "").strip()
    output_name = build_cc_output_name(fname, "VOSTFR")

    # Seedr CDN URLs often embed the original filename in the path, which can
    # contain spaces, brackets and other characters that CloudConvert rejects
    # as "url format is invalid".  Percent-encode only the unsafe characters
    # without touching the scheme, host, query string, or already-encoded parts.
    import re as _re
    from urllib.parse import urlparse as _urlparse, urlunparse as _urlunparse, quote as _quote
    def _safe_url(u: str) -> str:
        try:
            p = _urlparse(u)
            # Encode path: allow normal URL path chars, encode everything else
            safe_path  = _quote(p.path,  safe="/:@!$&'()*+,;=~.-_")
            # Encode query: allow key=value&... chars
            safe_query = _quote(p.query, safe="=&+%~.-_:@!$'()*,;/?")
            return _urlunparse(p._replace(path=safe_path, query=safe_query))
        except Exception:
            # Fallback: just encode the most problematic chars inline
            return _re.sub(r'[ \[\]{}|\\^`<>"]',
                           lambda m: f"%{ord(m.group()):02X}", u)

    safe_video_url = _safe_url(video_url)
    if safe_video_url != video_url:
        log.info("[SeedrHS] URL encoded for CC: %s → %s",
                 video_url[:80], safe_video_url[:80])

    await safe_edit(
        st,
        f"🔥 <b>Seedr → Hardsub</b>\n"
        "──────────────────────\n\n"
        f"📁 <code>{fname[:38]}</code>\n"
        f"💬 <code>{detail[:38]}</code>\n"
        f"📦 → <code>{output_name[:38]}</code>\n\n"
        "☁️ <i>Submitting to CloudConvert…\n"
        "Video URL sent directly — only subtitle uploaded.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        keys             = parse_api_keys(api_key)
        selected, creds  = await pick_best_key(keys)
        key_info         = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({creds} credits)"

        job_id = await submit_hardsub(
            selected,
            video_url=safe_video_url,
            subtitle_path=sub_path,
            output_name=output_name,
        )

        from services.cc_job_store import cc_job_store, CCJob
        await cc_job_store.add(CCJob(
            job_id=job_id, uid=uid,
            fname=fname, sub_fname=sub_fname, output_name=output_name,
            status="processing",
            seedr_folder_id=folder_id,
            seedr_user=seedr_user, seedr_pwd=seedr_pwd,
        ))
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
        except Exception:
            pass

        log.info("[SeedrHS] CC job submitted: %s → %s", job_id, output_name)
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
    finally:
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Submit CDN URL + subtitle to CloudConvert
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
            selected,
            video_url=video_url,
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
            seedr_folder_id=folder_id,
            seedr_user=seedr_user,
            seedr_pwd=seedr_pwd,
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
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Legacy: local-file upload to CloudConvert (kept for compatibility)
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
            selected,
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


# ─────────────────────────────────────────────────────────────
# Seedr Monitor button callbacks  (smon|sub|  smon|upload|  smon|skip|)
# Handles the three inline buttons shown by services/seedr_monitor.py
# when a new Seedr folder has no French subtitle.
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^smon\|"))
async def seedr_monitor_cb(client: Client, cb: CallbackQuery) -> None:
    """
    smon|sub|<folder_id>    — keep _WAITING_SUB active, remind user to send a sub
    smon|upload|<folder_id> — download the full video from Seedr CDN, upload to DM
    smon|skip|<folder_id>   — discard pending state and do nothing
    """
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

    # ── "I'll send a subtitle" — _WAITING_SUB already populated ─────────────
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

    # ── "Upload as-is" — download full file, upload to DM, no hardsub ───────
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

    # ── "Skip" — clean up and move on ────────────────────────────────────────
    if action == "skip":
        _WAITING_SUB.pop(uid, None)
        try:
            from services.seedr_monitor import _pending_upload
            info = _pending_upload.pop(folder_id, None)
            if info and info.get("tmp"):
                cleanup(info["tmp"])
            # Best-effort Seedr folder cleanup
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
    """Download the full Seedr CDN file and upload it to the user's DM."""
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
