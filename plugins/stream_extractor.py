"""
plugins/stream_extractor.py
Complete Stream Extractor — three sources:
  1. URL (YouTube / any yt-dlp site)
  2. Magnet / Torrent
  3. Uploaded Video (Telegram file)

DOWNLOAD-FIRST IMPROVEMENT (direct URLs):
  Previously: ffprobe probed the URL directly
    → Single-use CDN auth tokens get consumed by the probe
    → Incomplete metadata (some containers store stream info mid-file)
    → Then extraction had to re-download anyway
  Now: Download the full file first, probe locally, extract locally
    → 100% accurate stream detection
    → No token consumption issues
    → File is already local for instant extraction
    → Creates a real FileSession so ALL existing se_fext_cb callbacks
       work seamlessly without any change
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import urllib.parse as _urlparse
from dataclasses import dataclass
from typing import Optional

import aiohttp
import aria2p
import yt_dlp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import sessions
from services import ffmpeg as FF
from services.downloader import classify, download_ytdlp, download_aria2
from services.uploader import upload_file
from services.utils import cleanup, human_size, lang_flag, lang_name, make_tmp, safe_edit

log = logging.getLogger(__name__)

_flag      = lang_flag
_lang_name = lang_name


# ─────────────────────────────────────────────────────────────
# Quality buckets
# ─────────────────────────────────────────────────────────────

_QUALITY_ORDER = ["4K","1440p","1080p","720p","480p","360p","240p","144p","Audio"]

_QUALITY_ICON: dict[str, str] = {
    "4K":"🔷","1440p":"🟣","1080p":"🔵","720p":"🟢",
    "480p":"🟡","360p":"🟠","240p":"🔴","144p":"⚫","Audio":"🎵",
}


def _quality_bucket(height: int, width: int = 0) -> str:
    if height >= 2160 or width >= 3840: return "4K"
    if height >= 1440: return "1440p"
    if height >= 1080: return "1080p"
    if height >= 720:  return "720p"
    if height >= 480:  return "480p"
    if height >= 360:  return "360p"
    if height >= 240:  return "240p"
    if height > 0:     return "144p"
    return "Audio"


def _bucket_from_note(note: str) -> str:
    note = (note or "").strip()
    if re.search(r"2160|4k", note, re.I): return "4K"
    if re.search(r"1440",    note, re.I): return "1440p"
    if re.search(r"1080",    note, re.I): return "1080p"
    if re.search(r"720",     note, re.I): return "720p"
    if re.search(r"480",     note, re.I): return "480p"
    if re.search(r"360",     note, re.I): return "360p"
    if re.search(r"240",     note, re.I): return "240p"
    if re.search(r"144",     note, re.I): return "144p"
    return "Audio"


# ─────────────────────────────────────────────────────────────
# Format descriptor
# ─────────────────────────────────────────────────────────────

@dataclass
class StreamFormat:
    fmt_id:    str
    bucket:    str
    label:     str
    detail:    str
    filesize:  int   = 0
    is_audio:  bool  = False
    has_audio: bool  = True
    hdr:       bool  = False
    fps:       float = 0.0
    tbr:       float = 0.0


def _parse_yt_formats(info: dict) -> dict[str, list[StreamFormat]]:
    groups: dict[str, list[StreamFormat]] = {b: [] for b in _QUALITY_ORDER}
    seen: set = set()

    for f in reversed(info.get("formats", [])):
        fid    = f.get("format_id", "")
        vcodec = f.get("vcodec", "none") or "none"
        acodec = f.get("acodec", "none") or "none"
        ext    = f.get("ext", "?")
        note   = f.get("format_note") or f.get("resolution") or ""
        height = int(f.get("height") or 0)
        width  = int(f.get("width")  or 0)
        fps    = float(f.get("fps")  or 0)
        tbr    = float(f.get("tbr")  or 0)
        abr    = float(f.get("abr")  or 0)
        fsz    = int(f.get("filesize") or f.get("filesize_approx") or 0)
        dyn_range = (f.get("dynamic_range") or "").upper()
        hdr    = "HDR" in dyn_range or "HLG" in dyn_range

        if not fid:
            continue

        is_audio_only = (vcodec == "none")
        has_audio     = (acodec != "none")

        if is_audio_only:
            bucket = "Audio"
            acodec_clean = acodec.split(".")[0].upper()
            br_s = f"{int(abr)}kbps" if abr else (f"{int(tbr)}kbps" if tbr else "")
            dedup_key = f"audio_{acodec_clean}_{br_s}"
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            label  = f"🎵 {acodec_clean} {br_s}"
            detail = f"{acodec_clean} · {ext} · {br_s}"
            if fsz: detail += f" · {human_size(fsz)}"
            groups["Audio"].append(StreamFormat(
                fmt_id=fid, bucket="Audio", label=label, detail=detail,
                filesize=fsz, is_audio=True, has_audio=True, tbr=tbr,
            ))
            continue

        bucket = _bucket_from_note(note) if note else _quality_bucket(height, width)
        vcodec_clean = vcodec.split(".")[0].upper()
        fps_s  = f"{int(fps)}fps" if fps and fps not in (24, 25, 30) else ""
        hdr_s  = " HDR" if hdr else ""
        sz_s   = human_size(fsz) if fsz else ""
        tbr_s  = f"{int(tbr)}kbps" if tbr else ""
        audio_s = "" if has_audio else " 🔇"
        res_s = f"{height}p" if height else note
        dedup_key = f"{bucket}_{vcodec_clean}_{has_audio}_{fps_s}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        icon  = _QUALITY_ICON.get(bucket, "📦")
        label = f"{icon} {res_s}{fps_s} {vcodec_clean}{hdr_s}{audio_s}"
        if sz_s: label += f" [{sz_s}]"

        detail_parts = [f"{res_s}", vcodec_clean, ext]
        if fps_s:  detail_parts.append(fps_s)
        if hdr_s:  detail_parts.append("HDR")
        if tbr_s:  detail_parts.append(tbr_s)
        if sz_s:   detail_parts.append(sz_s)
        if not has_audio: detail_parts.append("no audio")
        detail = " · ".join(detail_parts)

        groups[bucket].append(StreamFormat(
            fmt_id=fid, bucket=bucket, label=label, detail=detail,
            filesize=fsz, is_audio=False, has_audio=has_audio,
            hdr=hdr, fps=fps, tbr=tbr,
        ))

    for b in groups:
        groups[b].sort(key=lambda x: (not x.has_audio, -x.tbr))

    return {b: v for b, v in groups.items() if v}


# ─────────────────────────────────────────────────────────────
# Token cache
# ─────────────────────────────────────────────────────────────

_cache: dict[str, str] = {}
_CACHE_MAX = 1000


def _tok(data: str) -> str:
    token = hashlib.md5(data.encode()).hexdigest()[:12]
    if len(_cache) >= _CACHE_MAX:
        try:
            del _cache[next(iter(_cache))]
        except StopIteration:
            pass
    _cache[token] = data
    return token


def _untok(token: str) -> str:
    return _cache.get(token, "")


# ─────────────────────────────────────────────────────────────
# /stream command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("stream"))
async def cmd_stream(client: Client, msg: Message):
    await msg.reply(
        "📡 <b>Stream Extractor</b>\n\n"
        "Send me one of:\n"
        "• A <b>URL</b> (YouTube, Instagram, TikTok, direct link…)\n"
        "• A <b>magnet link</b> — I'll list files inside\n"
        "• A <b>video / audio file</b> — I'll show all streams\n\n"
        "<i>Or just paste a URL directly in chat — no command needed.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# Entry from video menu  se_file|<session_key>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^se_file\|"))
async def se_file_cb(client: Client, cb: CallbackQuery):
    _, key = cb.data.split("|", 1)
    user_id = cb.from_user.id
    session = sessions.get(key)
    if not session:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    async with session.lock:
        st   = await cb.message.edit("⬇️ Downloading file for stream analysis…")
        from plugins.video import _ensure
        path = await _ensure(client, session, st)
        if not path:
            return

    await _analyse_file_streams(client, st, path, key, user_id)


# ─────────────────────────────────────────────────────────────
# Source 1: URL / yt-dlp
# ─────────────────────────────────────────────────────────────

async def _ffprobe_url(url: str) -> dict | None:
    """
    Probe a URL directly via ffprobe.
    Kept as fallback only — primary path now downloads first.
    """
    cmd = [
        "ffprobe", "-v", "quiet",
        "-allowed_extensions", "ALL",
        "-analyzeduration", "20000000",
        "-probesize", "50000000",
        "-print_format", "json",
        "-show_format", "-show_streams",
        url,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode != 0 or not out.strip():
            return None
        data = json.loads(out.decode(errors="replace"))
        streams = data.get("streams", [])
        if any(s.get("codec_type") in ("video", "audio", "subtitle") for s in streams):
            return data
        return None
    except Exception as exc:
        log.debug("ffprobe direct probe failed: %s", exc)
        return None


def _build_session_from_ffprobe(data: dict, url: str) -> dict:
    streams  = data.get("streams", [])
    fmt      = data.get("format", {})
    duration = float(fmt.get("duration") or 0)
    title    = (fmt.get("tags") or {}).get("title") or url.split("/")[-1].split("?")[0][:60]

    videos, audios, subs_out = [], [], []

    for s in streams:
        ctype  = s.get("codec_type", "")
        codec  = s.get("codec_name", "?")
        idx    = s.get("index", 0)
        tags   = s.get("tags") or {}
        lang   = (tags.get("language") or "und").lower()
        title_tag = tags.get("title", "")

        if ctype == "video":
            w = s.get("width", 0) or 0
            h = s.get("height", 0) or 0
            fr = s.get("r_frame_rate", "0/1")
            try:
                fn2, fd2 = fr.split("/")
                fps = round(float(fn2) / max(float(fd2), 1))
            except Exception:
                fps = 0
            br  = int(s.get("bit_rate") or 0)
            sz  = int(br * duration / 8) if br and duration else 0
            res = f"{h}p" if h else f"{w}x{h}"
            fps_s = f" {fps}fps" if fps > 30 else ""
            label = f"🎬  {res}{fps_s}  [{codec.upper()}]"
            if sz: label += f"  ~{human_size(sz)}"
            if title_tag: label += f"  {title_tag}"
            videos.append({
                "stream_idx": idx, "label": label,
                "h": h, "fps": fps, "sz": sz,
                "lang": lang, "codec": codec,
                "ext": "mkv", "source": "ffprobe",
            })

        elif ctype == "audio":
            ch  = s.get("channels", 0) or 0
            br  = int(s.get("bit_rate") or 0)
            sz  = int(br * duration / 8) if br and duration else 0
            ch_s = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
            br_s = f"{br//1000}kbps" if br else ""
            flag = _flag(lang)
            lname = _lang_name(lang)
            label = f"{flag}  {lname}  [{codec.upper()}]  {ch_s}  {br_s}"
            if title_tag: label += f"  {title_tag}"
            audios.append({
                "stream_idx": idx, "label": label.strip(),
                "abr": br // 1000 if br else 0,
                "sz": sz, "lang": lang,
                "ext": FF.audio_ext(codec).lstrip(".") or "mka",
                "source": "ffprobe",
            })

        elif ctype == "subtitle":
            flag  = _flag(lang)
            lname = _lang_name(lang)
            forced = " ⚡Forced" if tags.get("forced") else ""
            label  = f"{flag}  {lname}  [{codec.upper()}]{forced}"
            if title_tag: label += f"  ({title_tag})"
            subs_out.append({
                "stream_idx": idx, "label": label,
                "lang": lang,
                "ext": FF.subtitle_ext(codec).lstrip(".") or "srt",
                "source": "ffprobe",
            })

    return {
        "url":    url,
        "title":  title,
        "video":  videos,
        "audio":  audios,
        "subs":   subs_out,
        "source": "ffprobe",
        "duration": duration,
    }


async def extract_url_streams(
    client: Client, msg_or_st, url: str, uid: int, edit: bool = True,
) -> None:
    if edit:
        st = await msg_or_st.edit("📡 Fetching stream list…")
    else:
        st = msg_or_st

    kind = classify(url)

    # ── DOWNLOAD-FIRST for direct URLs ────────────────────────
    # Previously: ffprobe on URL → burned auth tokens, missed streams
    # Now: download full file → probe locally → extract locally
    # Creates a real FileSession so ALL existing se_fext_cb callbacks
    # work without modification — _ensure() finds local_path and returns it.
    if kind == "direct":
        tmp = make_tmp(cfg.download_dir, uid)
        await safe_edit(
            st,
            "⬇️ <b>Downloading for stream analysis…</b>\n"
            "──────────────────────\n\n"
            "<i>Full download gives 100% accurate stream detection\n"
            "and the file is ready for instant extraction.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        try:
            from services.downloader import download_direct as _dl_direct
            path = await _dl_direct(url, tmp)
        except Exception as exc:
            cleanup(tmp)
            log.warning("Direct download failed for stream analysis (%s) — falling back to URL probe", exc)
            # Fallback: try ffprobe on URL (may still work for simple public URLs)
            raw = await _ffprobe_url(url)
            if raw:
                sess_data = _build_session_from_ffprobe(raw, url)
                if sess_data["video"] or sess_data["audio"] or sess_data["subs"]:
                    await _show_ffprobe_streams(client, st, sess_data, uid)
                    return
            return await safe_edit(
                st,
                f"❌ <b>Could not analyse streams</b>\n\n"
                f"Download failed: <code>{exc}</code>\n\n"
                f"<i>Try downloading the file first, then send it to the bot.</i>",
                parse_mode=enums.ParseMode.HTML,
            )

        fname  = os.path.basename(path)
        ext    = os.path.splitext(fname)[1] or ".mkv"
        fsize  = os.path.getsize(path)

        # Create a real FileSession — se_fext_cb reuses it seamlessly
        # _ensure() checks is_downloaded() → finds local_path → returns instantly
        file_session = await sessions.create(
            user_id=uid, file_id="__direct__",
            fname=fname, fsize=fsize, ext=ext, tmp_dir=tmp,
        )
        file_session.local_path = path  # mark as already downloaded

        log.info("[StreamExtractor] Downloaded %s (%s) for local analysis",
                 fname, human_size(fsize))
        await _analyse_file_streams(client, st, path, file_session.key, uid)
        return

    # ── yt-dlp: format list (no download needed for listing) ──
    try:
        ydl_opts = {
            "quiet":       True,
            "no_warnings": True,
            "noplaylist":  False,
            "extract_flat":"in_playlist",
        }
        loop = asyncio.get_running_loop()
        def _extract() -> dict:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        info = await loop.run_in_executor(None, _extract)
    except Exception as exc:
        return await safe_edit(st,
            f"❌ Could not fetch streams:\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)

    if info.get("_type") == "playlist":
        entries = info.get("entries", [])
        if not entries:
            return await safe_edit(st, "❌ Playlist is empty.")
        await _show_playlist(client, st, url, info, entries, uid)
        return

    await _show_url_streams(client, st, url, info, uid)


async def _show_ffprobe_streams(client, st, session: dict, uid: int) -> None:
    title    = session["title"]
    duration = session.get("duration", 0)
    videos   = session["video"]
    audios   = session["audio"]
    subs     = session["subs"]

    lines = [
        "📡 <b>Stream Extractor</b>  <i>(Direct Link)</i>",
        f"<code>{title[:55]}</code>",
        f"⏱ <code>{_fmt_dur(duration)}</code>",
        "──────────────────────",
    ]
    if videos:
        lines.append(f"🎬 <b>Video</b>  ({len(videos)} track)")
        for v in videos: lines.append(f"  {v['label']}")
    if audios:
        lines.append(f"🎵 <b>Audio</b>  ({len(audios)} track{'s' if len(audios)>1 else ''})")
        for a in audios: lines.append(f"  {a['label']}")
    if subs:
        lines.append(f"💬 <b>Subtitles</b>  ({len(subs)} track{'s' if len(subs)>1 else ''})")
        for s in subs: lines.append(f"  {s['label']}")
    if not any([videos, audios, subs]):
        lines.append("⚠️ <i>No streams detected.</i>")
    lines += ["──────────────────────", "<i>Tap a stream to extract it:</i>"]

    sess_tok = _tok(session["url"])
    _cache[f"ffprobe_session|{sess_tok}"] = session

    rows: list = []
    for v in videos:
        rows.append([InlineKeyboardButton(
            v["label"][:58],
            callback_data=f"se_fp|v|{sess_tok}|{v['stream_idx']}|{uid}",
        )])
    for a in audios:
        rows.append([InlineKeyboardButton(
            a["label"][:58],
            callback_data=f"se_fp|a|{sess_tok}|{a['stream_idx']}|{uid}",
        )])
    for s in subs:
        rows.append([InlineKeyboardButton(
            s["label"][:58],
            callback_data=f"se_fp|s|{sess_tok}|{s['stream_idx']}|{uid}",
        )])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}")])

    await safe_edit(st, "\n".join(l for l in lines if l is not None),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows))


@Client.on_callback_query(filters.regex(r"^se_fp\|"))
async def se_fp_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 5:
        return await cb.answer("Invalid data.", show_alert=True)
    _, stype, sess_tok, idx_str, uid_str = parts[:5]
    user_id = int(uid_str) if uid_str.isdigit() else cb.from_user.id
    session = _cache.get(f"ffprobe_session|{sess_tok}")
    if not session:
        return await cb.answer("Session expired. Resend the link.", show_alert=True)
    await cb.answer()

    url = session["url"]
    st  = await cb.message.edit(f"⬇️ Extracting stream #{idx_str}…")
    tmp = make_tmp(cfg.download_dir, user_id)

    try:
        idx   = int(idx_str)
        all_s = session["video"] + session["audio"] + session["subs"]
        rec   = next((s for s in all_s if s.get("stream_idx") == idx), None)
        if not rec:
            return await safe_edit(st, f"❌ Stream #{idx} not found in session.")

        ext = rec.get("ext", "mkv")
        _stype = {"v": "video", "a": "audio", "s": "subtitle"}.get(stype, "video")
        out    = _stream_fname(tmp, _stype, rec.get("lang", "und"), idx, f".{ext}")

        await safe_edit(st, f"⬇️ Downloading & extracting stream #{idx} via ffmpeg…")

        await FF.stream_op(url, out, [
            "-map", f"0:{idx}",
            "-c", "copy",
        ])

        caption_type = {"v": "video", "a": "audio", "s": "subtitle"}.get(stype, "stream")
        lang = rec.get("lang", "und")
        caption = (
            f"{'🎬' if stype=='v' else '🎵' if stype=='a' else '💬'} "
            f"<b>{caption_type.capitalize()} #{idx}</b>"
            + (f"  {_flag(lang)} {_lang_name(lang)}" if lang and lang != "und" else "")
            + f"\n<code>{os.path.basename(out)}</code>"
        )
        await upload_file(client, st, out, caption=caption, force_document=(stype != "v"))
        cleanup(tmp)

    except Exception as exc:
        log.error("se_fp extraction failed: %s", exc, exc_info=True)
        cleanup(tmp)
        await safe_edit(st,
            f"❌ Extraction failed: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)


async def _show_playlist(
    client: Client, st, url: str, info: dict, entries: list, uid: int,
) -> None:
    title   = info.get("title", "Playlist")[:50]
    channel = info.get("uploader", "")
    total   = len(entries)
    pl_tok  = _tok(f"playlist|{url}")
    _cache[f"pl_entries|{pl_tok}"] = entries

    lines = [
        f"📋 <b>Playlist: {title}</b>",
        f"👤 {channel}" if channel else "",
        f"📦 {total} video(s)",
        "──────────────────────",
        "<i>Select a video:</i>",
    ]
    rows: list = []
    for i, entry in enumerate(entries[:20]):
        et    = (entry.get("title") or f"Video {i+1}")[:40]
        e_url = entry.get("url") or entry.get("webpage_url") or ""
        if not e_url:
            continue
        t2 = _tok(e_url)
        rows.append([InlineKeyboardButton(
            f"{i+1}. {et}",
            callback_data=f"se_url|single|{t2}|{uid}",
        )])
    if total > 20:
        lines.append(f"\n<i>Showing first 20 of {total}</i>")
    rows.append([
        InlineKeyboardButton("🟢 Download All (best quality)",
                             callback_data=f"se_url|dl_all|{pl_tok}|{uid}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}"),
    ])

    await safe_edit(st, "\n".join(l for l in lines if l),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows))


async def _show_url_streams(client, st, url, info, uid):
    title    = (info.get("title") or "")[:55]
    uploader = info.get("uploader") or info.get("channel") or ""
    dur      = info.get("duration", 0)
    views    = info.get("view_count", 0)

    groups = _parse_yt_formats(info)
    subs   = _parse_yt_subtitles(info)

    dur_s = _fmt_dur(dur)
    lines = [
        "📡 <b>Stream Extractor</b>",
        f"<b>{title}</b>" if title else "",
        f"👤 {uploader}" if uploader else "",
        f"⏱ {dur_s}" + (f"  👁 {views:,}" if views else ""),
        "──────────────────────",
    ]

    if not groups:
        lines.append("⚠️ No downloadable streams found.")
    else:
        for bucket in _QUALITY_ORDER:
            fmts = groups.get(bucket, [])
            if not fmts:
                continue
            icon = _QUALITY_ICON.get(bucket, "📦")
            lines.append(f"{icon} <b>{bucket}</b> — {len(fmts)} format(s)")

    if subs:
        sub_flags = " ".join(_flag(lang) for lang in list(subs.keys())[:8])
        lines.append(f"💬 Subtitles: {sub_flags}")
    lines.append("──────────────────────")
    lines.append("<i>Choose quality:</i>")

    url_tok = _tok(url)
    _cache[f"info|{url_tok}"] = info

    rows: list = []
    for bucket in _QUALITY_ORDER:
        fmts = groups.get(bucket, [])
        if not fmts:
            continue
        icon  = _QUALITY_ICON.get(bucket, "📦")
        label = f"{icon} {bucket}  ({len(fmts)} option{'s' if len(fmts)>1 else ''})"
        rows.append([InlineKeyboardButton(
            label, callback_data=f"se_url|bucket|{url_tok}|{bucket}",
        )])
    if subs:
        rows.append([
            InlineKeyboardButton(f"💬 Subtitles ({len(subs)} lang)",
                                 callback_data=f"se_url|subs|{url_tok}|{uid}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}"),
        ])
    else:
        rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}")])

    await safe_edit(st, "\n".join(l for l in lines if l),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows))


def _parse_yt_subtitles(info: dict) -> dict[str, list[dict]]:
    out: dict = {}
    for src in ("subtitles", "automatic_captions"):
        for lang, fmts in (info.get(src) or {}).items():
            if lang not in out:
                out[lang] = []
            for f in (fmts or []):
                out[lang].append({
                    "url":  f.get("url",""),
                    "ext":  f.get("ext","vtt"),
                    "auto": src == "automatic_captions",
                })
    return out


# ─────────────────────────────────────────────────────────────
# Source 2: Magnet
# ─────────────────────────────────────────────────────────────

async def extract_magnet_streams(client, st, magnet: str, uid: int) -> None:
    await safe_edit(st,
        "🧲 <b>Fetching torrent metadata…</b>\n<i>This may take up to 60 seconds.</i>",
        parse_mode=enums.ParseMode.HTML)

    file_list: list[dict] = []
    torrent_name = ""
    try:
        file_list, torrent_name = await _aria2_file_list(magnet)
    except Exception as exc:
        log.warning("aria2 file list failed: %s", exc)

    if not file_list:
        import re as _re
        import urllib.parse as _up
        name_m = _re.search(r"[&?]dn=([^&]+)", magnet)
        torrent_name = _up.unquote_plus(name_m.group(1)) if name_m else "Unknown torrent"
        xl_m = _re.search(r"[&?]xl=([^&]+)", magnet)
        size  = int(xl_m.group(1)) if xl_m else 0
        xt_m  = _re.search(r"xt=urn:btih:([a-fA-F0-9]{40}|[A-Za-z2-7]{32})", magnet)
        ih    = xt_m.group(1).upper() if xt_m else "—"
        trs   = _re.findall(r"tr=([^&]+)", magnet)
        lines = [
            "🧲 <b>Magnet Info</b>", "──────────────────────",
            f"📄 <code>{torrent_name[:60]}</code>",
            f"🔑 <code>{ih}</code>",
        ]
        if size:
            lines.append(f"💾 <code>{human_size(size)}</code>")
        lines += [
            f"📡 Trackers: <code>{len(trs)}</code>",
            "",
            "⚠️ <i>aria2c not running — cannot list files.</i>",
            "<i>Start aria2c and try again, or download the whole torrent.</i>",
        ]
        mg_tok = _tok(magnet)
        await safe_edit(st, "\n".join(lines),
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🟢 Download whole torrent",
                                     callback_data=f"se_mag|all|{mg_tok}|{uid}"),
                 InlineKeyboardButton("❌ Cancel", callback_data=f"se_mag|cancel||{uid}")],
            ]))
        return

    await _show_magnet_files(client, st, magnet, torrent_name, file_list, uid)


async def _aria2_file_list(magnet: str) -> tuple[list[dict], str]:
    api = aria2p.API(aria2p.Client(
        host=cfg.aria2_host, port=cfg.aria2_port, secret=cfg.aria2_secret,
    ))
    opts = {
        "dir":              "/tmp/aria2_meta",
        "seed-time":        "0",
        "pause":            "true",
        "bt-metadata-only": "true",
        "follow-torrent":   "mem",
    }
    dl = api.add_magnet(magnet, options=opts)
    torrent_name = ""
    file_list: list[dict] = []
    try:
        for _ in range(60):
            await asyncio.sleep(1)
            try:
                dl = api.get_download(dl.gid)
            except Exception:
                continue
            if dl.error_message:
                raise RuntimeError(dl.error_message)
            if dl.name and dl.name != "Unknown":
                torrent_name = dl.name
                break
        try:
            dl = api.get_download(dl.gid)
            for f in (dl.files or []):
                file_list.append({
                    "index": f.index,
                    "path":  os.path.basename(f.path) if f.path else f"File {f.index}",
                    "size":  f.length or 0,
                })
        except Exception as exc:
            log.warning("aria2 file collection: %s", exc)
    finally:
        try:
            api.remove([dl])
        except Exception:
            pass
    return file_list, torrent_name


async def _show_magnet_files(client, st, magnet, name, files, uid):
    total_size = sum(f.get("size", 0) for f in files)
    lines = [
        "🧲 <b>Torrent File List</b>",
        f"📁 <code>{name[:55]}</code>",
        f"💾 Total: <code>{human_size(total_size)}</code>",
        f"📦 {len(files)} file(s)",
        "──────────────────────",
        "<i>Select file(s) to download:</i>",
    ]
    mg_tok = _tok(magnet)
    _cache[f"mag_files|{mg_tok}"] = files

    rows: list = []
    for f in files[:20]:
        idx   = f["index"]
        fname = f["path"][:40]
        sz    = human_size(f["size"]) if f["size"] else "?"
        ext   = os.path.splitext(f["path"])[1].lower()
        icon  = _file_icon(ext)
        rows.append([InlineKeyboardButton(
            f"{icon} {fname} [{sz}]",
            callback_data=f"se_mag|file|{mg_tok}|{idx}",
        )])
    if len(files) > 20:
        lines.append(f"\n<i>Showing first 20 of {len(files)} files</i>")
    rows += [
        [InlineKeyboardButton("🟢 Download ALL files",
                              callback_data=f"se_mag|all|{mg_tok}|{uid}"),
         InlineKeyboardButton("❌ Cancel",
                              callback_data=f"se_mag|cancel||{uid}")],
    ]
    await safe_edit(st, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows))


def _file_icon(ext: str) -> str:
    video = {".mp4",".mkv",".avi",".mov",".webm",".flv",".ts",".m2ts"}
    audio = {".mp3",".aac",".flac",".ogg",".m4a",".opus",".wav"}
    subs  = {".srt",".ass",".vtt",".sup",".sub"}
    arch  = {".zip",".rar",".7z",".tar",".gz"}
    if ext in video: return "🎬"
    if ext in audio: return "🎵"
    if ext in subs:  return "💬"
    if ext in arch:  return "📦"
    return "📄"


# ─────────────────────────────────────────────────────────────
# Source 3: Uploaded file
# ─────────────────────────────────────────────────────────────

async def _analyse_file_streams(
    client: Client, st, path: str, session_key: str, uid: int,
) -> None:
    await safe_edit(st, "🔍 Analysing streams…")
    try:
        sd, dur = await asyncio.gather(
            FF.probe_streams(path),
            FF.probe_duration(path),
        )
    except Exception as exc:
        return await safe_edit(st,
            f"❌ Could not probe file: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)

    fname = os.path.basename(path)
    fsize = os.path.getsize(path)
    v_streams = sd.get("video",    [])
    a_streams = sd.get("audio",    [])
    s_streams = sd.get("subtitle", [])

    lines = [
        "📡 <b>Stream Extractor</b>",
        f"📄 <code>{fname[:50]}</code>",
        f"💾 <code>{human_size(fsize)}</code>  ⏱ <code>{_fmt_dur(dur)}</code>",
        "──────────────────────",
    ]
    if v_streams:
        lines.append(f"🎬 <b>Video  ({len(v_streams)} track)</b>")
        for s in v_streams:
            lines.append("  " + _describe_video_stream(s))
    if a_streams:
        lines.append(f"🎵 <b>Audio  ({len(a_streams)} track{'s' if len(a_streams)>1 else ''})</b>")
        for s in a_streams:
            lines.append("  " + _describe_audio_stream(s))
    if s_streams:
        lines.append(f"💬 <b>Subtitles  ({len(s_streams)} track{'s' if len(s_streams)>1 else ''})</b>")
        for s in s_streams:
            lines.append("  " + _describe_sub_stream(s))
    if not any([v_streams, a_streams, s_streams]):
        lines.append("⚠️ <i>No streams detected — file may be corrupted or unsupported.</i>")
    lines += ["──────────────────────", "<i>Tap a stream to extract it:</i>"]

    rows: list = []
    for s in v_streams:
        idx   = s.get("index", 0)
        codec = (s.get("codec_name") or "video").upper()
        w, h  = s.get("width",0), s.get("height",0)
        rows.append([InlineKeyboardButton(
            f"🎬 Video #{idx}  {codec}  {w}x{h}",
            callback_data=f"se_fext|{session_key}|{idx}|video|{uid}",
        )])
    for s in a_streams:
        idx   = s.get("index", 0)
        codec = (s.get("codec_name") or "audio").upper()
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()
        flag  = _flag(lang)
        ch    = s.get("channels", 0)
        ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
        rows.append([InlineKeyboardButton(
            f"🎵 Audio #{idx}  {flag} {codec}  {ch_s}",
            callback_data=f"se_fext|{session_key}|{idx}|audio|{uid}",
        )])
    for s in s_streams:
        idx   = s.get("index", 0)
        codec = (s.get("codec_name") or "sub").upper()
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()
        flag  = _flag(lang)
        lname = _lang_name(lang)
        rows.append([InlineKeyboardButton(
            f"💬 Sub #{idx}  {flag} {lname}  {codec}",
            callback_data=f"se_fext|{session_key}|{idx}|sub|{uid}",
        )])
    if len(a_streams) > 1:
        rows.append([InlineKeyboardButton(
            "🎵 Extract ALL audio tracks",
            callback_data=f"se_fext|{session_key}|all|audio|{uid}",
        )])
    if len(s_streams) > 1:
        rows.append([InlineKeyboardButton(
            "💬 Extract ALL subtitle tracks",
            callback_data=f"se_fext|{session_key}|all|sub|{uid}",
        )])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"vid|back|{session_key}")])

    await safe_edit(st, "\n".join(l for l in lines if l is not None),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows))


def _describe_video_stream(s: dict) -> str:
    codec   = (s.get("codec_name") or "?").upper()
    profile = s.get("profile") or ""
    w, h    = s.get("width",0), s.get("height",0)
    fr      = s.get("r_frame_rate","0/1")
    try:
        fn2, fd2 = fr.split("/")
        fps = f"{float(fn2)/max(float(fd2),1):.2f}fps"
    except Exception:
        fps = ""
    pix = s.get("pix_fmt","")
    br  = s.get("bit_rate","")
    br_s = f"  {int(int(br))//1000}kbps" if br and str(br).isdigit() else ""
    hdr_s = " HDR" if "10" in pix else ""
    prof_s = f"@{profile}" if profile and "High" in profile else ""
    return f"<code>{codec}{prof_s}  {w}x{h}  {fps}{hdr_s}{br_s}</code>"


def _describe_audio_stream(s: dict) -> str:
    codec  = (s.get("codec_name") or "?").upper()
    ch     = s.get("channels", 0)
    ch_s   = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
    sr     = s.get("sample_rate","")
    sr_s   = f"  {int(sr)//1000}kHz" if sr else ""
    br     = s.get("bit_rate","")
    br_s   = f"  {int(int(br))//1000}kbps" if br and str(br).isdigit() else ""
    tags   = s.get("tags", {}) or {}
    lang   = (tags.get("language") or "und").lower()
    flag   = _flag(lang)
    lname  = _lang_name(lang)
    title  = tags.get("title","") or ""
    title_s = f" — {title}" if title else ""
    return f"{flag} <code>{codec}  {ch_s}{sr_s}{br_s}</code>  {lname}{title_s}"


def _describe_sub_stream(s: dict) -> str:
    codec  = (s.get("codec_name") or "?").upper()
    tags   = s.get("tags", {}) or {}
    lang   = (tags.get("language") or "und").lower()
    flag   = _flag(lang)
    lname  = _lang_name(lang)
    title  = tags.get("title","") or ""
    title_s = f" ({title})" if title else ""
    forced = " ⚡Forced" if tags.get("forced") else ""
    return f"{flag} <code>{codec}</code>  {lname}{title_s}{forced}"


# ─────────────────────────────────────────────────────────────
# File stream extraction callback  se_fext|<key>|<idx>|<type>|<uid>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^se_fext\|"))
async def se_fext_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 5:
        return await cb.answer("Invalid data.", show_alert=True)
    _, key, idx_str, stream_type, uid_str = parts[:5]
    user_id = int(uid_str) if uid_str.isdigit() else cb.from_user.id
    session = sessions.get(key)
    if not session:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    async with session.lock:
        st   = await cb.message.edit("⬇️ Preparing…")
        from plugins.video import _ensure
        # For direct-downloaded sessions, _ensure finds local_path immediately
        # No re-download needed — the file is already on disk
        path = await _ensure(client, session, st)
        if not path:
            return

    tmp  = session.tmp_dir
    base = os.path.splitext(os.path.basename(path))[0]
    await safe_edit(st, f"📤 Extracting stream #{idx_str}…")

    try:
        if idx_str == "all":
            await _extract_all_streams(client, st, path, tmp, base, stream_type, user_id)
        else:
            await _extract_single_stream(client, st, path, tmp, base, idx_str, stream_type, session, user_id)
    except Exception as exc:
        log.error("se_fext extraction failed: %s", exc, exc_info=True)
        await safe_edit(st,
            f"❌ Extraction failed: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)


async def _extract_single_stream(
    client, st, path, tmp, base, idx_str, stream_type, session, user_id: int,
):
    sd = await FF.probe_streams(path)
    all_streams = sd["video"] + sd["audio"] + sd["subtitle"]
    target = next((s for s in all_streams if str(s.get("index")) == idx_str), None)

    if not target:
        return await safe_edit(st, f"❌ Stream #{idx_str} not found.")

    stype = target.get("codec_type", stream_type)
    codec = (target.get("codec_name") or "").lower()

    if stype == "subtitle":
        out_ext = FF.subtitle_ext(codec)
        tags    = target.get("tags", {}) or {}
        lang    = (tags.get("language") or "und").lower()
        out     = _stream_fname(tmp, "subtitle", lang, idx_str, out_ext)
    elif stype == "audio":
        out_ext = FF.audio_ext(codec)
        tags    = target.get("tags", {}) or {}
        lang    = (tags.get("language") or "und").lower()
        out     = _stream_fname(tmp, "audio", lang, idx_str, out_ext)
    else:
        out_ext = session.ext or os.path.splitext(path)[1] or ".mp4"
        out     = _stream_fname(tmp, "video", "", idx_str, out_ext)

    await FF.stream_op(path, out, ["-map", f"0:{idx_str}", "-c", "copy"])

    caption = _stream_caption(target, stype, codec)
    await upload_file(client, st, out, caption=caption, force_document=True)


async def _extract_all_streams(
    client, st, path, tmp, base, stream_type, user_id: int,
):
    sd = await FF.probe_streams(path)
    if stream_type == "audio":
        streams = sd.get("audio", [])
        stype   = "audio"
    else:
        streams = sd.get("subtitle", [])
        stype   = "subtitle"

    if not streams:
        return await safe_edit(st, f"❌ No {stype} streams found.")

    await safe_edit(st, f"📤 Extracting {len(streams)} {stype} track(s)…")

    for s in streams:
        idx   = s.get("index", 0)
        codec = (s.get("codec_name") or "").lower()
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()

        if stype == "subtitle":
            out_ext = FF.subtitle_ext(codec)
        else:
            out_ext = FF.audio_ext(codec)

        out = _stream_fname(tmp, stype, lang, idx, out_ext)
        try:
            await FF.stream_op(path, out, ["-map", f"0:{idx}", "-c", "copy"])
            caption = _stream_caption(s, stype, codec)
            await client.send_document(
                user_id, out, caption=caption,
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as exc:
            log.warning("Extract stream %d failed: %s", idx, exc)

    await st.delete()


def _stream_fname(tmp: str, stype: str, lang: str, idx, ext: str) -> str:
    lang = (lang or "und").strip().lower()
    if stype == "subtitle":
        prefix = f"sub_{lang}"
    elif stype == "audio":
        prefix = f"audio_{lang}"
    else:
        prefix = f"video_{idx}"

    base_name = os.path.join(tmp, f"{prefix}{ext}")
    if not os.path.exists(base_name):
        return base_name
    counter = 2
    while True:
        candidate = os.path.join(tmp, f"{prefix}_{counter}{ext}")
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def _stream_caption(s: dict, stype: str, codec: str) -> str:
    tags   = s.get("tags", {}) or {}
    lang   = (tags.get("language") or "und").lower()
    flag   = _flag(lang)
    lname  = _lang_name(lang)
    title  = tags.get("title","") or ""
    idx    = s.get("index","?")
    codec_up = codec.upper()
    if stype == "subtitle":
        return (f"💬 <b>Subtitle #{idx}</b>  {flag} {lname}\n"
                f"<code>{codec_up}</code>" + (f" — {title}" if title else ""))
    if stype == "audio":
        ch  = s.get("channels", 0)
        ch_s = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
        return (f"🎵 <b>Audio #{idx}</b>  {flag} {lname}\n"
                f"<code>{codec_up}  {ch_s}</code>" + (f" — {title}" if title else ""))
    w, h = s.get("width",0), s.get("height",0)
    return f"🎬 <b>Video #{idx}</b>\n<code>{codec_up}  {w}x{h}</code>"


# ─────────────────────────────────────────────────────────────
# URL bucket/format callbacks  se_url|<action>|<tok>|<extra>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^se_url\|"))
async def se_url_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 4:
        return await cb.answer("Invalid data.", show_alert=True)
    _, action, tok, extra = parts[0], parts[1], parts[2], parts[3]
    user_id = cb.from_user.id
    await cb.answer()

    if action == "cancel":
        return await cb.message.delete()

    if action == "single":
        url = _untok(tok)
        if not url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit("📡 Fetching stream list…")
        await extract_url_streams(client, st, url, user_id, edit=False)
        return

    if action == "bucket":
        bucket = extra
        url    = _untok(tok)
        info   = _cache.get(f"info|{tok}")
        if not url or not info:
            return await safe_edit(cb.message, "❌ Session expired. Resend the URL.")
        await _show_bucket_formats(client, cb.message, url, tok, info, bucket, user_id)
        return

    if action == "subs":
        url  = _untok(tok)
        info = _cache.get(f"info|{tok}")
        if not info:
            return await safe_edit(cb.message, "❌ Session expired.")
        await _show_subtitle_picker(client, cb.message, url, tok, info, user_id)
        return

    if action == "dl_fmt":
        fmt_data = _untok(extra)
        if "|||" in fmt_data:
            url, fmt_id = fmt_data.split("|||", 1)
        else:
            url    = _untok(tok)
            fmt_id = fmt_data
        if not url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit("⬇️ Downloading stream…")
        await _download_url_fmt(client, st, url, fmt_id, user_id)
        return

    if action == "dl_audio":
        url = _untok(tok)
        if not url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit("⬇️ Downloading best audio…")
        await _download_url_fmt(client, st, url, None, user_id, audio_only=True)
        return

    if action == "dl_sub":
        sub_url = _untok(extra)
        lang    = tok
        if not sub_url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit("⬇️ Downloading subtitle…")
        await _download_subtitle(client, st, sub_url, lang, user_id)
        return

    if action == "dl_all":
        url = _untok(tok)
        if not url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit("⬇️ Downloading…")
        await _download_url_fmt(client, st, url, None, user_id)
        return

    if action == "back_main":
        url  = _untok(tok)
        info = _cache.get(f"info|{tok}")
        if not url or not info:
            return await safe_edit(cb.message, "❌ Session expired. Resend the URL.")
        await _show_url_streams(client, cb.message, url, info, user_id)
        return


async def _show_bucket_formats(client, msg, url, url_tok, info, bucket, uid):
    groups = _parse_yt_formats(info)
    fmts   = groups.get(bucket, [])
    if not fmts:
        return await safe_edit(msg, f"❌ No {bucket} formats available.")

    icon  = _QUALITY_ICON.get(bucket, "📦")
    lines = [
        f"{icon} <b>{bucket} — {len(fmts)} format(s)</b>",
        "──────────────────────",
    ]
    for f in fmts:
        lines.append(f"• <code>{f.detail}</code>")
    lines += ["──────────────────────", "<i>Select format to download:</i>"]

    rows: list = []
    for f in fmts:
        fmt_tok = _tok(f"{url}|||{f.fmt_id}")
        rows.append([InlineKeyboardButton(
            f.label, callback_data=f"se_url|dl_fmt|{url_tok}|{fmt_tok}",
        )])
    rows += [
        [InlineKeyboardButton("🔙 Back to qualities",
                              callback_data=f"se_url|back_main|{url_tok}|{uid}"),
         InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}")],
    ]
    await safe_edit(msg, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows))


async def _show_subtitle_picker(client, msg, url, url_tok, info, uid):
    subs = _parse_yt_subtitles(info)
    if not subs:
        return await safe_edit(msg, "❌ No subtitles available.")

    lines = ["💬 <b>Available Subtitles</b>", "──────────────────────"]
    rows: list = []
    for lang, fmts in sorted(subs.items()):
        flag    = _flag(lang)
        lname   = _lang_name(lang)
        is_auto = any(f.get("auto") for f in fmts)
        auto_s  = " (Auto)" if is_auto else ""
        best = (next((f for f in fmts if f.get("ext") == "vtt"), None)
                or next((f for f in fmts if f.get("ext") == "srt"), None)
                or fmts[0])
        sub_url_tok = _tok(best.get("url",""))
        ext_s = best.get("ext","vtt")
        lines.append(f"{flag} <b>{lname}</b>{auto_s}  [{ext_s}]")
        rows.append([InlineKeyboardButton(
            f"{flag} {lname}{auto_s}",
            callback_data=f"se_url|dl_sub|{lang}|{sub_url_tok}",
        )])
    rows += [
        [InlineKeyboardButton("🔙 Back", callback_data=f"se_url|back_main|{url_tok}|{uid}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}")],
    ]
    await safe_edit(msg, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows))


async def _download_url_fmt(
    client, st, url: str, fmt_id: Optional[str],
    uid: int, audio_only: bool = False,
) -> None:
    tmp   = make_tmp(cfg.download_dir, uid)
    start = time.time()
    last  = [start]

    async def _progress(done: int, total: int, speed: float, eta: int) -> None:
        now = time.time()
        if now - last[0] < 3.0:
            return
        last[0] = now

    try:
        path = await download_ytdlp(
            url, tmp,
            audio_only=audio_only,
            fmt_id=fmt_id,
            progress=_progress,
        )
    except Exception as exc:
        cleanup(tmp)
        return await safe_edit(st,
            f"❌ Download failed: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)

    fsize = os.path.getsize(path)
    if fsize > cfg.file_limit_b:
        cleanup(tmp)
        return await safe_edit(st,
            f"❌ File too large: <code>{human_size(fsize)}</code>\n"
            f"Limit: <code>{human_size(cfg.file_limit_b)}</code>",
            parse_mode=enums.ParseMode.HTML)

    await upload_file(client, st, path)
    cleanup(tmp)


async def _download_subtitle(client, st, sub_url: str, lang: str, uid: int) -> None:
    flag  = _flag(lang)
    lname = _lang_name(lang)
    tmp   = make_tmp(cfg.download_dir, uid)
    ext_m = re.search(r"\.(vtt|srt|ass|ttml|srv\d?)(\?|$)", sub_url, re.I)
    ext   = f".{ext_m.group(1)}" if ext_m else ".vtt"
    fname = f"subtitle_{lang}{ext}"
    fpath = os.path.join(tmp, fname)
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(sub_url) as resp:
                resp.raise_for_status()
                content = await resp.read()
        with open(fpath, "wb") as f:
            f.write(content)
    except Exception as exc:
        cleanup(tmp)
        return await safe_edit(st,
            f"❌ Subtitle download failed: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)
    caption = f"💬 <b>Subtitle</b>  {flag} {lname}\n<code>{fname}</code>"
    await upload_file(client, st, fpath, caption=caption, force_document=True)
    cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Magnet callbacks  se_mag|<action>|<tok>|<extra>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^se_mag\|"))
async def se_mag_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 4:
        return await cb.answer("Invalid data.", show_alert=True)
    _, action, tok, extra = parts[0], parts[1], parts[2], parts[3]
    user_id = cb.from_user.id
    await cb.answer()

    if action == "cancel":
        return await cb.message.delete()

    magnet = _untok(tok)
    if not magnet:
        return await safe_edit(cb.message, "❌ Session expired.")

    uid = user_id

    if action == "all":
        st  = await cb.message.edit("🧲 Starting full torrent download…")
        tmp = make_tmp(cfg.download_dir, uid)
        start = time.time(); last = [start]

        async def _mag_prog(done: int, total: int, speed: float, eta: int) -> None:
            now = time.time()
            if now - last[0] < 3.0: return
            last[0] = now

        try:
            path = await download_aria2(magnet, tmp, is_file=False, progress=_mag_prog)
        except Exception as exc:
            cleanup(tmp)
            return await safe_edit(st,
                f"❌ Download failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML)

        fsize = os.path.getsize(path)
        if fsize > cfg.file_limit_b:
            cleanup(tmp)
            return await safe_edit(st,
                f"❌ File too large: <code>{human_size(fsize)}</code>",
                parse_mode=enums.ParseMode.HTML)

        await upload_file(client, st, path)
        cleanup(tmp)
        return

    if action == "file":
        file_idx = extra
        files    = _cache.get(f"mag_files|{tok}", [])
        selected = next((f for f in files if str(f.get("index")) == str(file_idx)), None)
        fname    = selected["path"] if selected else f"file_{file_idx}"

        st  = await cb.message.edit(
            f"🧲 Downloading <code>{fname[:50]}</code>…",
            parse_mode=enums.ParseMode.HTML,
        )
        tmp = make_tmp(cfg.download_dir, uid)
        start = time.time(); last = [start]

        async def _file_prog(done: int, total: int, speed: float, eta: int) -> None:
            now = time.time()
            if now - last[0] < 3.0: return
            last[0] = now

        try:
            api = aria2p.API(aria2p.Client(
                host=cfg.aria2_host, port=cfg.aria2_port, secret=cfg.aria2_secret,
            ))
            opts = {
                "dir":           tmp,
                "seed-time":     "0",
                "select-file":   str(file_idx),
                "follow-torrent":"mem",
            }
            dl = api.add_magnet(magnet, options=opts)
            start_dl = time.time()
            while True:
                await asyncio.sleep(3)
                try:
                    dl = api.get_download(dl.gid)
                except Exception:
                    continue
                if dl.error_message:
                    raise RuntimeError(dl.error_message)
                if dl.is_complete:
                    break
                done_b  = dl.completed_length or 0
                total_b = dl.total_length     or 0
                speed   = dl.download_speed   or 0.0
                eta_val = int((total_b - done_b) / speed) if speed else 0
                await _file_prog(done_b, total_b, speed, eta_val)
                if time.time() - start_dl > 3600 * 6:
                    raise TimeoutError("Torrent timeout (6h)")

            from services.utils import largest_file
            path = largest_file(tmp)
            if not path:
                raise FileNotFoundError("No output file found")

        except Exception as exc:
            cleanup(tmp)
            return await safe_edit(st,
                f"❌ Download failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML)

        await upload_file(client, st, path)
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _fmt_dur(s) -> str:
    if not s:
        return "—"
    try:
        s = int(float(s))
    except Exception:
        return "—"
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
