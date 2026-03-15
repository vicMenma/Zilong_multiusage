"""
plugins/stream_extractor.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Complete Stream Extractor — three sources:

  1. URL (YouTube / any yt-dlp site)
     - Fetches all formats via yt-dlp
     - Groups by quality bucket: 4K / 1080p / 720p / 480p / 360p / Audio
     - Shows: codec · container · size · bitrate · fps · HDR flag
     - Subtitle tracks with language flags
     - Playlist support (pick episode)
     - HLS / DASH adaptive streams supported

  2. Magnet / Torrent
     - Parses magnet URI without downloading
     - Lists all files in the torrent via aria2c metadata fetch
     - User picks which file(s) to download

  3. Uploaded Video (Telegram file)
     - ffprobe all streams (video / audio / subtitle)
     - Detailed per-stream info panel
     - Extract any stream as a file with one tap
     - Correct extension per codec (srt/ass/vtt/sup/aac/dts/flac…)

Entry points:
  /stream          → prompt user to send URL or file
  Inline button    → "📡 Stream Extractor" from video menu or URL menu
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import time
import urllib.parse as _urlparse
from dataclasses import dataclass, field
from typing import Optional

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import sessions
from services import ffmpeg as FF
from services.downloader import classify, download_ytdlp, download_aria2
from services.tg_download import tg_download
from services.uploader import upload_file
from services.utils import cleanup, human_size, make_tmp, safe_edit

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Language flags & names
# ─────────────────────────────────────────────────────────────

_LANG_FLAG: dict[str, str] = {
    "eng": "🇬🇧", "en":  "🇬🇧",
    "jpn": "🇯🇵", "ja":  "🇯🇵",
    "fra": "🇫🇷", "fre": "🇫🇷", "fr": "🇫🇷",
    "deu": "🇩🇪", "ger": "🇩🇪", "de": "🇩🇪",
    "spa": "🇪🇸", "es":  "🇪🇸",
    "por": "🇧🇷", "pt":  "🇧🇷",
    "ita": "🇮🇹", "it":  "🇮🇹",
    "kor": "🇰🇷", "ko":  "🇰🇷",
    "chi": "🇨🇳", "zho": "🇨🇳", "zh": "🇨🇳",
    "rus": "🇷🇺", "ru":  "🇷🇺",
    "ara": "🇸🇦", "ar":  "🇸🇦",
    "hin": "🇮🇳", "hi":  "🇮🇳",
    "tha": "🇹🇭", "th":  "🇹🇭",
    "vie": "🇻🇳", "vi":  "🇻🇳",
    "ind": "🇮🇩", "id":  "🇮🇩",
    "msa": "🇲🇾", "ms":  "🇲🇾",
    "tur": "🇹🇷", "tr":  "🇹🇷",
    "pol": "🇵🇱", "pl":  "🇵🇱",
    "nld": "🇳🇱", "nl":  "🇳🇱",
    "swe": "🇸🇪", "sv":  "🇸🇪",
    "nor": "🇳🇴", "no":  "🇳🇴",
    "dan": "🇩🇰", "da":  "🇩🇰",
    "fin": "🇫🇮", "fi":  "🇫🇮",
    "heb": "🇮🇱", "he":  "🇮🇱",
    "ces": "🇨🇿", "cze": "🇨🇿",
    "ron": "🇷🇴", "rum": "🇷🇴",
    "hun": "🇭🇺", "hu":  "🇭🇺",
    "bul": "🇧🇬", "bg":  "🇧🇬",
    "ukr": "🇺🇦", "uk":  "🇺🇦",
    "und": "🌐",
}

_LANG_NAME: dict[str, str] = {
    "eng":"English","en":"English",
    "jpn":"Japanese","ja":"Japanese",
    "fra":"French","fre":"French","fr":"French",
    "deu":"German","ger":"German","de":"German",
    "spa":"Spanish","es":"Spanish",
    "por":"Portuguese","pt":"Portuguese",
    "ita":"Italian","it":"Italian",
    "kor":"Korean","ko":"Korean",
    "chi":"Chinese","zho":"Chinese","zh":"Chinese",
    "rus":"Russian","ru":"Russian",
    "ara":"Arabic","ar":"Arabic",
    "hin":"Hindi","hi":"Hindi",
    "tha":"Thai","th":"Thai",
    "vie":"Vietnamese","vi":"Vietnamese",
    "ind":"Indonesian","id":"Indonesian",
    "msa":"Malay","ms":"Malay",
    "tur":"Turkish","tr":"Turkish",
    "pol":"Polish","pl":"Polish",
    "nld":"Dutch","nl":"Dutch",
    "swe":"Swedish","sv":"Swedish",
    "nor":"Norwegian","no":"Norwegian",
    "dan":"Danish","da":"Danish",
    "fin":"Finnish","fi":"Finnish",
    "heb":"Hebrew","he":"Hebrew",
    "ces":"Czech","cze":"Czech",
    "ron":"Romanian","rum":"Romanian",
    "hun":"Hungarian","hu":"Hungarian",
    "bul":"Bulgarian","bg":"Bulgarian",
    "ukr":"Ukrainian","uk":"Ukrainian",
    "und":"Unknown",
}


def _flag(lang: str) -> str:
    return _LANG_FLAG.get(lang.lower(), "🌐")


def _lang_name(lang: str) -> str:
    return _LANG_NAME.get(lang.lower(), lang.upper())


# ─────────────────────────────────────────────────────────────
# Quality buckets
# ─────────────────────────────────────────────────────────────

_QUALITY_ORDER = ["4K", "1440p", "1080p", "720p", "480p", "360p", "240p", "144p", "Audio"]

_QUALITY_ICON: dict[str, str] = {
    "4K":    "🔷",
    "1440p": "🟣",
    "1080p": "🔵",
    "720p":  "🟢",
    "480p":  "🟡",
    "360p":  "🟠",
    "240p":  "🔴",
    "144p":  "⚫",
    "Audio": "🎵",
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
    """Derive bucket from yt-dlp format_note string like '1080p60', '4K', 'tiny'."""
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
# Format descriptors
# ─────────────────────────────────────────────────────────────

@dataclass
class StreamFormat:
    fmt_id:    str
    bucket:    str
    label:     str          # shown on the button
    detail:    str          # shown in the info panel
    filesize:  int  = 0
    is_audio:  bool = False
    has_audio: bool = True
    hdr:       bool = False
    fps:       float = 0.0
    tbr:       float = 0.0


def _parse_yt_formats(info: dict) -> dict[str, list[StreamFormat]]:
    """
    Parse yt-dlp info dict into {bucket: [StreamFormat]} grouped dict.
    Deduplicates by (height, vcodec_family, has_audio).
    """
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

        # Video (with or without audio)
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

        icon   = _QUALITY_ICON.get(bucket, "📦")
        label  = f"{icon} {res_s}{fps_s} {vcodec_clean}{hdr_s}{audio_s}"
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

    # Sort each bucket: prefer has_audio, then by tbr desc
    for b in groups:
        groups[b].sort(key=lambda x: (not x.has_audio, -x.tbr))

    return {b: v for b, v in groups.items() if v}


# ─────────────────────────────────────────────────────────────
# Token cache (url ↔ short token for callback_data)
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
# Session state for multi-step operations
# ─────────────────────────────────────────────────────────────

# {uid: {"url": str, "info": dict, "tmp": str, ...}}
_SE_STATE: dict[int, dict] = {}


# ─────────────────────────────────────────────────────────────
# /stream command  — entry point
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("stream"))
async def cmd_stream(client: Client, msg: Message):
    uid = msg.from_user.id
    _SE_STATE[uid] = {"step": "waiting_input"}
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
# Trigger from video menu button  "📡 Stream Extractor"
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^se_file\|"))
async def se_file_cb(client: Client, cb: CallbackQuery):
    """
    Entry from the video processing menu.
    callback_data = se_file|<session_key>
    """
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
# ── Source 1: URL / yt-dlp ───────────────────────────────────
# ─────────────────────────────────────────────────────────────

async def extract_url_streams(
    client: Client,
    msg_or_cb,
    url: str,
    uid: int,
    edit: bool = True,
) -> None:
    """
    Fetch all streams from a yt-dlp-supported URL.
    Shows bucketed quality selector + subtitle tracks + playlist.
    """
    st = await msg_or_cb.edit("📡 Fetching stream list…") if edit else \
         await msg_or_cb.reply("📡 Fetching stream list…")

    try:
        import yt_dlp

        ydl_opts = {
            "quiet":        True,
            "no_warnings":  True,
            "noplaylist":   False,   # allow playlist detection
            "extract_flat": "in_playlist",
        }

        loop = asyncio.get_event_loop()

        def _extract() -> dict:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)

        info = await loop.run_in_executor(None, _extract)

    except Exception as exc:
        return await safe_edit(st,
            f"❌ Could not fetch streams:\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)

    # ── Playlist ──────────────────────────────────────────────
    if info.get("_type") == "playlist":
        entries = info.get("entries", [])
        if not entries:
            return await safe_edit(st, "❌ Playlist is empty.")
        await _show_playlist(client, st, url, info, entries, uid)
        return

    # ── Single video ──────────────────────────────────────────
    await _show_url_streams(client, st, url, info, uid)


async def _show_playlist(
    client: Client, st, url: str, info: dict, entries: list, uid: int
):
    title   = info.get("title", "Playlist")[:50]
    channel = info.get("uploader", "")
    total   = len(entries)

    # Store playlist state
    pl_tok = _tok(f"playlist|{url}")
    _cache[f"pl_entries|{pl_tok}"] = entries   # type: ignore[assignment]

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

    rows.append([InlineKeyboardButton(
        "⬇️ Download All (best quality)",
        callback_data=f"se_url|dl_all|{pl_tok}|{uid}",
    )])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}")])

    await safe_edit(st,
        "\n".join(l for l in lines if l),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _show_url_streams(
    client: Client, st, url: str, info: dict, uid: int
):
    title    = (info.get("title") or "")[:55]
    uploader = info.get("uploader") or info.get("channel") or ""
    dur      = info.get("duration", 0)
    views    = info.get("view_count", 0)
    thumb    = info.get("thumbnail") or ""

    # Parse formats into buckets
    groups = _parse_yt_formats(info)

    # Parse subtitle tracks
    subs   = _parse_yt_subtitles(info)

    # Build info header
    dur_s  = _fmt_dur(dur)
    lines  = [
        f"📡 <b>Stream Extractor</b>",
        f"<b>{title}</b>" if title else "",
        f"👤 {uploader}" if uploader else "",
        f"⏱ {dur_s}" + (f"  👁 {views:,}" if views else ""),
        "──────────────────────",
    ]

    # List available buckets
    bucket_count = sum(len(v) for v in groups.values())
    if bucket_count == 0:
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

    # Build keyboard: one row per non-empty bucket
    url_tok = _tok(url)
    # Store full info dict for later use
    _cache[f"info|{url_tok}"] = info   # type: ignore[assignment]

    rows: list = []
    for bucket in _QUALITY_ORDER:
        fmts = groups.get(bucket, [])
        if not fmts:
            continue
        icon  = _QUALITY_ICON.get(bucket, "📦")
        label = f"{icon} {bucket}  ({len(fmts)} option{'s' if len(fmts)>1 else ''})"
        rows.append([InlineKeyboardButton(
            label,
            callback_data=f"se_url|bucket|{url_tok}|{bucket}",
        )])

    # Subtitle row
    if subs:
        sub_tok = _tok(f"subs|{url}")
        rows.append([InlineKeyboardButton(
            f"💬 Subtitles ({len(subs)} lang)",
            callback_data=f"se_url|subs|{url_tok}|{uid}",
        )])

    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}")])

    await safe_edit(st,
        "\n".join(l for l in lines if l),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


def _parse_yt_subtitles(info: dict) -> dict[str, list[dict]]:
    """Return {lang_code: [sub_format, ...]} from yt-dlp info."""
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
# ── Source 2: Magnet / Torrent ───────────────────────────────
# ─────────────────────────────────────────────────────────────

async def extract_magnet_streams(
    client: Client, st, magnet: str, uid: int
) -> None:
    """
    List files inside a magnet without fully downloading.
    Uses aria2c to fetch metadata only (seed-time=0, file selection).
    Falls back to magnet URI parsing if aria2c is not available.
    """
    await safe_edit(st,
        "🧲 <b>Fetching torrent metadata…</b>\n"
        "<i>This may take up to 60 seconds.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # Try aria2c metadata fetch
    file_list: list[dict] = []
    torrent_name = ""
    try:
        file_list, torrent_name = await _aria2_file_list(magnet)
    except Exception as exc:
        log.warning("aria2 file list failed: %s", exc)

    # Fallback: parse magnet URI directly
    if not file_list:
        name_m = re.search(r"[&?]dn=([^&]+)", magnet)
        torrent_name = _urlparse.unquote_plus(name_m.group(1)) if name_m else "Unknown torrent"
        xl_m = re.search(r"[&?]xl=([^&]+)", magnet)
        size = int(xl_m.group(1)) if xl_m else 0
        xt_m = re.search(r"xt=urn:btih:([a-fA-F0-9]{40}|[A-Za-z2-7]{32})", magnet)
        ih   = xt_m.group(1).upper() if xt_m else "—"
        trs  = re.findall(r"tr=([^&]+)", magnet)

        lines = [
            "🧲 <b>Magnet Info</b>",
            "──────────────────────",
            f"📄 <code>{torrent_name[:60]}</code>",
            f"🔑 <code>{ih}</code>",
        ]
        if size:
            lines.append(f"💾 <code>{human_size(size)}</code>")
        lines.append(f"📡 Trackers: <code>{len(trs)}</code>")
        lines.append("")
        lines.append("⚠️ <i>aria2c not running — cannot list files.</i>")
        lines.append("<i>Start aria2c and try again, or download the whole torrent.</i>")

        mg_tok = _tok(magnet)
        await safe_edit(st, "\n".join(lines),
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬇️ Download whole torrent",
                                     callback_data=f"se_mag|all|{mg_tok}|{uid}")],
                [InlineKeyboardButton("❌ Cancel",
                                     callback_data=f"se_mag|cancel||{uid}")],
            ]))
        return

    # Show file list
    await _show_magnet_files(client, st, magnet, torrent_name, file_list, uid)


async def _aria2_file_list(magnet: str) -> tuple[list[dict], str]:
    """
    Use aria2c RPC to fetch torrent metadata.
    Adds magnet with file-allocation=none, seed-time=0,
    waits for metadata (files become known), then removes the download.
    Returns (file_list, torrent_name).
    """
    import aria2p

    api = aria2p.API(aria2p.Client(
        host=cfg.aria2_host, port=cfg.aria2_port, secret=cfg.aria2_secret,
    ))

    opts = {
        "dir":           "/tmp/aria2_meta",
        "seed-time":     "0",
        "pause":         "true",
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

        # Collect files
        try:
            dl = api.get_download(dl.gid)
            for f in (dl.files or []):
                file_list.append({
                    "index": f.index,
                    "path":  os.path.basename(f.path) if f.path else f"File {f.index}",
                    "size":  f.length or 0,
                })
        except Exception as exc:
            log.warning("aria2 file collection error: %s", exc)

    finally:
        try:
            api.remove([dl])
        except Exception:
            pass

    return file_list, torrent_name


async def _show_magnet_files(
    client: Client, st, magnet: str,
    name: str, files: list[dict], uid: int,
):
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
    _cache[f"mag_files|{mg_tok}"] = files   # type: ignore[assignment]

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
        [InlineKeyboardButton("⬇️ Download ALL files",
                              callback_data=f"se_mag|all|{mg_tok}|{uid}")],
        [InlineKeyboardButton("❌ Cancel",
                              callback_data=f"se_mag|cancel||{uid}")],
    ]

    await safe_edit(st, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


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
# ── Source 3: Uploaded video file ───────────────────────────
# ─────────────────────────────────────────────────────────────

async def _analyse_file_streams(
    client: Client, st, path: str, session_key: str, uid: int
) -> None:
    """
    Probe all streams in a local file and show a detailed selector.
    """
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

    # Video streams
    if v_streams:
        lines.append(f"🎬 <b>Video  ({len(v_streams)} track)</b>")
        for s in v_streams:
            lines.append("  " + _describe_video_stream(s))

    # Audio streams
    if a_streams:
        lines.append(f"🎵 <b>Audio  ({len(a_streams)} track{'s' if len(a_streams)>1 else ''})</b>")
        for s in a_streams:
            lines.append("  " + _describe_audio_stream(s))

    # Subtitle streams
    if s_streams:
        lines.append(f"💬 <b>Subtitles  ({len(s_streams)} track{'s' if len(s_streams)>1 else ''})</b>")
        for s in s_streams:
            lines.append("  " + _describe_sub_stream(s))

    if not any([v_streams, a_streams, s_streams]):
        lines.append("⚠️ <i>No streams detected — file may be corrupted or unsupported.</i>")

    lines.append("──────────────────────")
    lines.append("<i>Tap a stream to extract it:</i>")

    # Build extraction keyboard
    rows: list = []

    for s in v_streams:
        idx   = s.get("index", 0)
        codec = (s.get("codec_name") or "video").upper()
        w, h  = s.get("width",0), s.get("height",0)
        label = f"🎬 Video #{idx}  {codec}  {w}x{h}"
        rows.append([InlineKeyboardButton(
            label, callback_data=f"se_fext|{session_key}|{idx}|video",
        )])

    for s in a_streams:
        idx   = s.get("index", 0)
        codec = (s.get("codec_name") or "audio").upper()
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()
        flag  = _flag(lang)
        ch    = s.get("channels", 0)
        ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
        label = f"🎵 Audio #{idx}  {flag} {codec}  {ch_s}"
        rows.append([InlineKeyboardButton(
            label, callback_data=f"se_fext|{session_key}|{idx}|audio",
        )])

    for s in s_streams:
        idx   = s.get("index", 0)
        codec = (s.get("codec_name") or "sub").upper()
        tags  = s.get("tags", {}) or {}
        lang  = (tags.get("language") or "und").lower()
        flag  = _flag(lang)
        lname = _lang_name(lang)
        label = f"💬 Sub #{idx}  {flag} {lname}  {codec}"
        rows.append([InlineKeyboardButton(
            label, callback_data=f"se_fext|{session_key}|{idx}|sub",
        )])

    # Extract all audio
    if len(a_streams) > 1:
        rows.append([InlineKeyboardButton(
            "🎵 Extract ALL audio tracks",
            callback_data=f"se_fext|{session_key}|all|audio",
        )])
    # Extract all subs
    if len(s_streams) > 1:
        rows.append([InlineKeyboardButton(
            "💬 Extract ALL subtitle tracks",
            callback_data=f"se_fext|{session_key}|all|sub",
        )])

    rows.append([InlineKeyboardButton(
        "🔙 Back", callback_data=f"vid|back|{session_key}",
    )])

    await safe_edit(st,
        "\n".join(l for l in lines if l),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


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
# File stream extraction callback  se_fext|<key>|<idx>|<type>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^se_fext\|"))
async def se_fext_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 4:
        return await cb.answer("Invalid data.", show_alert=True)
    _, key, idx_str, stream_type = parts[:4]
    user_id = cb.from_user.id
    session = sessions.get(key)
    if not session:
        return await cb.answer("Session expired.", show_alert=True)
    await cb.answer()

    async with session.lock:
        st   = await cb.message.edit("⬇️ Downloading…")
        from plugins.video import _ensure
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
            await _extract_single_stream(client, st, path, tmp, base, idx_str, stream_type, session)
    except Exception as exc:
        log.error("se_fext extraction failed: %s", exc)
        await safe_edit(st,
            f"❌ Extraction failed: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)


async def _extract_single_stream(
    client, st, path, tmp, base, idx_str, stream_type, session
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
        out     = os.path.join(tmp, f"{base}_sub_{idx_str}_{lang}{out_ext}")
    elif stype == "audio":
        out_ext = FF.audio_ext(codec)
        tags    = target.get("tags", {}) or {}
        lang    = (tags.get("language") or "und").lower()
        out     = os.path.join(tmp, f"{base}_audio_{idx_str}_{lang}{out_ext}")
    else:
        out_ext = session.ext or os.path.splitext(path)[1] or ".mp4"
        out     = os.path.join(tmp, f"{base}_video_{idx_str}{out_ext}")

    await FF.stream_op(path, out, ["-map", f"0:{idx_str}", "-c", "copy"])

    # Register as a tracker task (instantaneous, so mark done immediately)
    from services.task_runner import tracker, TaskRecord
    tid = tracker.new_tid()
    rec = TaskRecord(tid=tid, user_id=user_id, label=f"Extract stream #{idx_str}",
                     mode="proc", engine="ffmpeg",
                     fname=os.path.basename(out), state="✅ Done")
    await tracker.register(rec)

    # Build a nice caption
    caption = _stream_caption(target, stype, codec)
    await upload_file(client, st, out, caption=caption, force_document=True)


async def _extract_all_streams(
    client, st, path, tmp, base, stream_type, user_id
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

        out = os.path.join(tmp, f"{base}_{stype}_{idx}_{lang}{out_ext}")
        try:
            await FF.stream_op(path, out, ["-map", f"0:{idx}", "-c", "copy"])
            caption = _stream_caption(s, stype, codec)
            await client.send_document(user_id, out, caption=caption,
                                       parse_mode=enums.ParseMode.HTML)
        except Exception as exc:
            log.warning("Extract stream %d failed: %s", idx, exc)

    await st.delete()


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
                f"<code>{codec_up}</code>"
                + (f" — {title}" if title else ""))
    if stype == "audio":
        ch  = s.get("channels", 0)
        ch_s = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
        return (f"🎵 <b>Audio #{idx}</b>  {flag} {lname}\n"
                f"<code>{codec_up}  {ch_s}</code>"
                + (f" — {title}" if title else ""))
    w, h = s.get("width",0), s.get("height",0)
    return f"🎬 <b>Video #{idx}</b>\n<code>{codec_up}  {w}x{h}</code>"


# ─────────────────────────────────────────────────────────────
# URL bucket callback  se_url|bucket|<url_tok>|<bucket>
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
        await cb.message.delete()
        return

    if action == "single":
        # extra = uid, tok = url_token
        url = _untok(tok)
        if not url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit("📡 Fetching stream list…")
        await extract_url_streams(client, st, url, user_id, edit=False)
        return

    if action == "bucket":
        # extra = bucket name, tok = url_token
        bucket = extra
        url    = _untok(tok)
        info   = _cache.get(f"info|{tok}")
        if not url or not info:
            return await safe_edit(cb.message, "❌ Session expired. Resend the URL.")
        await _show_bucket_formats(client, cb.message, url, tok, info, bucket, user_id)
        return

    if action == "subs":
        # Show subtitle language picker
        url  = _untok(tok)
        info = _cache.get(f"info|{tok}")
        if not info:
            return await safe_edit(cb.message, "❌ Session expired.")
        await _show_subtitle_picker(client, cb.message, url, tok, info, user_id)
        return

    if action == "dl_fmt":
        # extra = fmt_id (possibly with |||url embedded)
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
        # extra = sub_url_token
        sub_url = _untok(extra)
        lang    = tok   # reused field
        if not sub_url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit(f"⬇️ Downloading subtitle…")
        await _download_subtitle(client, st, sub_url, lang, user_id)
        return

    if action == "dl_all":
        # Download all with best quality
        url = _untok(tok)
        if not url:
            return await safe_edit(cb.message, "❌ Session expired.")
        st = await cb.message.edit("⬇️ Downloading…")
        await _download_url_fmt(client, st, url, None, user_id)
        return


async def _show_bucket_formats(
    client, msg, url: str, url_tok: str, info: dict, bucket: str, uid: int
):
    groups = _parse_yt_formats(info)
    fmts   = groups.get(bucket, [])
    if not fmts:
        return await safe_edit(msg, f"❌ No {bucket} formats available.",
                               parse_mode=enums.ParseMode.HTML)

    icon  = _QUALITY_ICON.get(bucket, "📦")
    lines = [
        f"{icon} <b>{bucket} — {len(fmts)} format(s)</b>",
        "──────────────────────",
    ]
    for f in fmts:
        lines.append(f"• <code>{f.detail}</code>")

    lines.append("──────────────────────")
    lines.append("<i>Select format to download:</i>")

    rows: list = []
    for f in fmts:
        fmt_tok = _tok(f"{url}|||{f.fmt_id}")
        rows.append([InlineKeyboardButton(
            f.label,
            callback_data=f"se_url|dl_fmt|{url_tok}|{fmt_tok}",
        )])

    rows += [
        [InlineKeyboardButton("🔙 Back to qualities",
                              callback_data=f"se_url|back_main|{url_tok}|{uid}")],
        [InlineKeyboardButton("❌ Cancel",
                              callback_data=f"se_url|cancel||{uid}")],
    ]

    await safe_edit(msg, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _show_subtitle_picker(
    client, msg, url: str, url_tok: str, info: dict, uid: int
):
    subs = _parse_yt_subtitles(info)
    if not subs:
        return await safe_edit(msg, "❌ No subtitles available.")

    lines = [
        "💬 <b>Available Subtitles</b>",
        "──────────────────────",
    ]
    rows: list = []

    for lang, fmts in sorted(subs.items()):
        flag   = _flag(lang)
        lname  = _lang_name(lang)
        is_auto = any(f.get("auto") for f in fmts)
        auto_s  = " (Auto)" if is_auto else ""

        # Pick best format: prefer vtt, then srt
        best = next((f for f in fmts if f.get("ext") == "vtt"), None) \
            or next((f for f in fmts if f.get("ext") == "srt"), None) \
            or fmts[0]

        sub_url_tok = _tok(best.get("url",""))
        ext_s = best.get("ext","vtt")

        lines.append(f"{flag} <b>{lname}</b>{auto_s}  [{ext_s}]")
        rows.append([InlineKeyboardButton(
            f"{flag} {lname}{auto_s}",
            callback_data=f"se_url|dl_sub|{lang}|{sub_url_tok}",
        )])

    rows += [
        [InlineKeyboardButton("🔙 Back",   callback_data=f"se_url|back_main|{url_tok}|{uid}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"se_url|cancel||{uid}")],
    ]

    await safe_edit(msg, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _download_url_fmt(
    client, st, url: str, fmt_id: Optional[str],
    uid: int, audio_only: bool = False,
) -> None:
    tmp   = make_tmp(cfg.download_dir, uid)
    start = time.time()
    last  = [start]

    async def _progress(done: int, total: int, speed: float, eta: int) -> None:
        from services.utils import progress_panel
        now = time.time()
        if now - last[0] < 3.0:
            return
        last[0] = now
        panel = progress_panel(
            mode="dl", done=done, total=total,
            speed=speed, eta=eta,
            elapsed=now - start,
            engine="ytdlp",
        )
        await safe_edit(st, panel, parse_mode=enums.ParseMode.HTML)

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


async def _download_subtitle(
    client, st, sub_url: str, lang: str, uid: int
) -> None:
    """Download a subtitle file directly from its URL."""
    import aiohttp
    flag  = _flag(lang)
    lname = _lang_name(lang)
    tmp   = make_tmp(cfg.download_dir, uid)

    # Determine extension from URL
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
# Magnet download callbacks  se_mag|<action>|<tok>|<extra>
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
        # Download entire torrent
        st  = await cb.message.edit("🧲 Starting full torrent download…")
        tmp = make_tmp(cfg.download_dir, uid)
        start = time.time(); last = [start]

        async def _mag_prog(done: int, total: int, speed: float, eta: int) -> None:
            from services.utils import progress_panel
            now = time.time()
            if now - last[0] < 3.0: return
            last[0] = now
            panel = progress_panel(
                mode="magnet", done=done, total=total,
                speed=speed, eta=eta, elapsed=now - start, engine="aria2",
            )
            await safe_edit(st, panel, parse_mode=enums.ParseMode.HTML)

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
        # Download specific file by index — aria2c file-selection
        file_idx = extra
        files    = _cache.get(f"mag_files|{tok}", [])
        selected = next((f for f in files if str(f.get("index")) == str(file_idx)), None)
        fname    = selected["path"] if selected else f"file_{file_idx}"
        fsize    = selected["size"] if selected else 0

        st  = await cb.message.edit(
            f"🧲 Downloading <code>{fname[:50]}</code>…",
            parse_mode=enums.ParseMode.HTML,
        )
        tmp = make_tmp(cfg.download_dir, uid)
        start = time.time(); last = [start]

        async def _file_prog(done: int, total: int, speed: float, eta: int) -> None:
            from services.utils import progress_panel
            now = time.time()
            if now - last[0] < 3.0: return
            last[0] = now
            panel = progress_panel(
                mode="magnet", fname=fname, done=done,
                total=total or fsize, speed=speed, eta=eta,
                elapsed=now - start, engine="aria2",
            )
            await safe_edit(st, panel, parse_mode=enums.ParseMode.HTML)

        try:
            # aria2c file selection: --select-file=<index>
            import aria2p
            api = aria2p.API(aria2p.Client(
                host=cfg.aria2_host, port=cfg.aria2_port, secret=cfg.aria2_secret,
            ))
            opts = {
                "dir":           tmp,
                "seed-time":     "0",
                "select-file":   str(file_idx),
                "follow-torrent": "mem",
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
                eta     = int((total_b - done_b) / speed) if speed else 0
                await _file_prog(done_b, total_b, speed, eta)
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
# Back to main quality view
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^se_url\|back_main\|"))
async def se_url_back_main(client: Client, cb: CallbackQuery):
    parts   = cb.data.split("|")
    url_tok = parts[2] if len(parts) > 2 else ""
    uid     = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else cb.from_user.id
    url     = _untok(url_tok)
    info    = _cache.get(f"info|{url_tok}")
    await cb.answer()
    if not url or not info:
        return await safe_edit(cb.message, "❌ Session expired. Resend the URL.")
    st = cb.message
    await _show_url_streams(client, st, url, info, uid)


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
