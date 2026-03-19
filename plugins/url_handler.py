"""
plugins/url_handler.py
Handles URL messages and torrent files.

Changes:
- Added "📊 Media Info" and "📡 Stream Extractor" buttons for magnets
- Magnet Media Info: downloads torrent, probes the largest file, shows streams
- Magnet Stream Extractor: lets user pick and extract a specific stream track
- _launch_download no longer attaches a live panel; progress is silent
  and only visible via /status
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import tempfile
import time
import urllib.parse as _up

import aiohttp
import aria2p
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.downloader import classify, smart_download, download_ytdlp
from services.tg_download import tg_download
from services.uploader import upload_file
from services.utils import cleanup, human_size, lang_flag, lang_name, largest_file, make_tmp, safe_edit

log = logging.getLogger(__name__)

# Convenience aliases
_flag  = lang_flag
_lname = lang_name

URL_RE = re.compile(r"https?://\S+|magnet:\?\S+", re.I)

_cache: dict[str, str] = {}
_CACHE_MAX = 500

# Store magnet probe sessions: tok → {"path": str, "tmp": str, "streams": dict}
_magnet_probe: dict[str, dict] = {}


def _store(url: str) -> str:
    token = hashlib.md5(url.encode()).hexdigest()[:10]
    if len(_cache) >= _CACHE_MAX:
        try:
            del _cache[next(iter(_cache))]
        except StopIteration:
            pass
    _cache[token] = url
    return token


def _get(token: str) -> str:
    return _cache.get(token, "")


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


# ─────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────

def _url_kb(token: str, kind: str) -> InlineKeyboardMarkup:
    rows: list = []
    if kind == "ytdlp":
        rows += [
            [InlineKeyboardButton("🎬 Download Video",   callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("🎵 Download Audio",   callback_data=f"dl|audio|{token}")],
            [InlineKeyboardButton("📡 Stream Extractor", callback_data=f"dl|stream|{token}"),
             InlineKeyboardButton("📊 Media Info",       callback_data=f"dl|info|{token}")],
            [InlineKeyboardButton("🖼️ Thumbnail",         callback_data=f"dl|thumb|{token}")],
        ]
    elif kind in ("magnet", "torrent"):
        rows += [
            [InlineKeyboardButton("🧲 Download",         callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("📊 Media Info",       callback_data=f"dl|info|{token}")],
            [InlineKeyboardButton("📡 Stream Extractor", callback_data=f"dl|magnet_stream|{token}")],
        ]
    elif kind == "gdrive":
        rows += [
            [InlineKeyboardButton("☁️ Download",         callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("🎵 Audio Only",       callback_data=f"dl|audio|{token}")],
            [InlineKeyboardButton("📡 Stream Extractor", callback_data=f"dl|stream|{token}")],
        ]
    elif kind == "mediafire":
        rows += [
            [InlineKeyboardButton("📁 Download", callback_data=f"dl|video|{token}")],
        ]
    else:
        rows += [
            [InlineKeyboardButton("🎬 Download File",    callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("📊 Media Info",       callback_data=f"dl|info|{token}")],
            [InlineKeyboardButton("📡 Stream Extractor", callback_data=f"dl|stream|{token}")],
        ]
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"dl|cancel|{token}")])
    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────
# URL message handler
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.text, group=5)
async def url_handler(client: Client, msg: Message):
    text = msg.text.strip()
    if text.startswith("/"):
        return

    m = URL_RE.search(text)
    if not m:
        return

    uid = msg.from_user.id
    await users.register(uid, msg.from_user.first_name or "")

    url   = m.group(0)
    kind  = classify(url)
    token = _store(url)

    icons  = {"magnet":"🧲","torrent":"📦","gdrive":"☁️","mediafire":"📁","ytdlp":"▶️","direct":"🔗"}
    labels = {"magnet":"Magnet Link","torrent":"Torrent","gdrive":"Google Drive",
              "mediafire":"Mediafire","ytdlp":"Video Site","direct":"Direct Link"}

    await msg.reply(
        f"<b>{icons.get(kind,'🔗')} {labels.get(kind,'Link')} detected</b>\n\n"
        f"<code>{url[:80]}</code>\n\n<i>Choose an action:</i>",
        reply_markup=_url_kb(token, kind),
        parse_mode=enums.ParseMode.HTML,
        disable_web_page_preview=True,
    )


# ─────────────────────────────────────────────────────────────
# Torrent file (from media_router)
# ─────────────────────────────────────────────────────────────

async def handle_torrent_file(client: Client, msg: Message, media, uid: int) -> None:
    try:
        await msg.delete()
    except Exception:
        pass
    tmp = make_tmp(cfg.download_dir, uid)
    from types import SimpleNamespace
    _dummy = SimpleNamespace(edit=lambda *a, **kw: asyncio.sleep(0),
                             delete=lambda: asyncio.sleep(0))
    try:
        tp = await tg_download(
            client, media.file_id,
            os.path.join(tmp, "dl.torrent"), _dummy,
            fname="dl.torrent",
            fsize=getattr(media, "file_size", 0) or 0,
            user_id=uid,
        )
        from services.downloader import download_aria2
        result = await download_aria2(tp, tmp, is_file=True)
    except Exception as exc:
        cleanup(tmp)
        try:
            from core.session import get_client
            await get_client().send_message(
                uid, f"❌ Torrent failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return
    from types import SimpleNamespace
    _up_dummy = SimpleNamespace(
        edit=lambda *a, **kw: asyncio.sleep(0),
        delete=lambda: asyncio.sleep(0),
        chat=SimpleNamespace(id=uid),
    )
    asyncio.create_task(_upload_and_cleanup(client, _up_dummy, result, tmp))


# ─────────────────────────────────────────────────────────────
# Magnet: probe the file content via aria2c metadata fetch
# ─────────────────────────────────────────────────────────────

async def _probe_magnet_file(magnet: str, uid: int, st) -> tuple[str | None, str | None, dict]:
    """
    Fetches torrent metadata via aria2c, then downloads enough of the
    largest media file to probe its streams.
    Returns (file_path, tmp_dir, stream_data). On failure returns (None, None, {}).
    """
    from services import ffmpeg as FF
    from services.task_runner import tracker, TaskRecord, runner

    tmp = make_tmp(cfg.download_dir, uid)
    await safe_edit(st,
        "🧲 <b>Magnet Probe</b>\n\n"
        "<i>Connecting to aria2c and fetching torrent metadata…</i>\n"
        "<i>This may take up to 90 seconds depending on tracker response.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        api = aria2p.API(aria2p.Client(
            host=cfg.aria2_host, port=cfg.aria2_port, secret=cfg.aria2_secret,
        ))
        # Quick ping to confirm aria2c is alive
        api.get_stats()
    except Exception as exc:
        await safe_edit(st,
            f"❌ <b>aria2c not reachable</b>\n<code>{exc}</code>\n\n"
            "<i>Make sure aria2c is running with --enable-rpc.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        cleanup(tmp)
        return None, None, {}

    # ── Phase 1: fetch metadata only ─────────────────────────
    # Standard options — no bt-metadata-only (that's not a real aria2c flag).
    # We add the magnet, wait until aria2c resolves the name and file list,
    # then remove it before any actual data is downloaded.
    meta_opts = {
        "dir":                        tmp,
        "seed-time":                  "0",
        "max-connection-per-server":  "8",
        "follow-torrent":             "mem",
        "bt-max-peers":               "100",
    }

    try:
        dl = api.add_magnet(magnet, options=meta_opts)
    except Exception as exc:
        await safe_edit(st, f"❌ aria2c rejected magnet: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        cleanup(tmp)
        return None, None, {}

    torrent_name = ""
    file_list: list[dict] = []
    t_start = time.time()

    for tick in range(120):
        await asyncio.sleep(1)
        try:
            dl = api.get_download(dl.gid)
        except Exception:
            continue

        if dl.error_message:
            try: api.remove([dl])
            except Exception: pass
            await safe_edit(st,
                f"❌ <b>Metadata fetch failed</b>\n<code>{dl.error_message}</code>\n\n"
                "<i>The magnet link could not be resolved. Try a magnet with more trackers.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
            cleanup(tmp)
            return None, None, {}

        # Metadata is ready when we have a name and file list
        files = dl.files or []
        if dl.name and dl.name not in ("Unknown", "") and files and any(f.path for f in files):
            torrent_name = dl.name
            for f in files:
                file_list.append({
                    "index": f.index,
                    "path":  str(f.path or ""),
                    "size":  f.length or 0,
                })
            break

        # Update status every 10s
        if tick % 10 == 0 and tick > 0:
            elapsed = int(time.time() - t_start)
            await safe_edit(st,
                f"🧲 <b>Waiting for metadata…</b>  <code>{elapsed}s</code>\n\n"
                "<i>Contacting trackers to resolve file list…</i>",
                parse_mode=enums.ParseMode.HTML,
            )

        if time.time() - t_start > 90:
            break

    # Remove the metadata download — we don't want it to keep downloading
    try: api.remove([dl])
    except Exception: pass

    if not file_list:
        await safe_edit(st,
            "❌ <b>Could not resolve torrent metadata.</b>\n\n"
            "<i>No trackers responded in time. The magnet link may be dead, "
            "or all trackers are offline. Try a different magnet source.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        cleanup(tmp)
        return None, None, {}

    # ── Pick the best file to probe ───────────────────────────
    _video_exts = {".mp4",".mkv",".avi",".mov",".webm",".flv",".ts",".m2ts",".wmv",".m4v",".rmvb",".mpg",".mpeg"}
    _audio_exts = {".mp3",".aac",".m4a",".opus",".ogg",".flac",".wav",".wma",".ac3",".mka"}

    def _priority(f: dict) -> int:
        ext = os.path.splitext(f["path"])[1].lower()
        if ext in _video_exts: return 2
        if ext in _audio_exts: return 1
        return 0

    best = sorted(file_list, key=lambda f: (_priority(f), f["size"]), reverse=True)
    if not best:
        cleanup(tmp)
        return None, None, {}

    target_file = best[0]
    fname       = os.path.basename(target_file["path"]) or f"file_{target_file['index']}"
    file_total  = target_file["size"] or 0

    await safe_edit(st,
        f"✅ <b>Metadata resolved!</b>\n\n"
        f"📁 <b>{torrent_name[:50]}</b>\n"
        f"🎯 Probing: <code>{fname[:48]}</code>\n"
        f"💾 Size: <code>{human_size(file_total)}</code>\n\n"
        f"⬇️ <i>Downloading first 30 MB for stream analysis…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    # ── Phase 2: download the selected file with progress ─────
    # Register a TaskRecord so the panel shows this download
    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=uid,
        label=fname,
        mode="dl", engine="magnet",
        fname=fname, total=file_total,
        state="📥 Downloading",
    )
    await tracker.register(record)

    dl_opts = {
        "dir":                        tmp,
        "seed-time":                  "0",
        "select-file":                str(target_file["index"]),
        "follow-torrent":             "mem",
        "max-connection-per-server":  "16",
        "split":                      "16",
        "bt-max-peers":               "200",
    }

    try:
        dl2 = api.add_magnet(magnet, options=dl_opts)
    except Exception as exc:
        record.update(state="❌ Failed")
        await safe_edit(st, f"❌ Download start failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        cleanup(tmp)
        return None, None, {}

    t_start      = time.time()
    PROBE_ENOUGH = 30 * 1024 * 1024   # 30 MB is enough for ffprobe

    while True:
        await asyncio.sleep(2)
        try:
            dl2 = api.get_download(dl2.gid)
        except Exception:
            await asyncio.sleep(3)
            continue

        if dl2.error_message:
            try: api.remove([dl2])
            except Exception: pass
            record.update(state="❌ Failed")
            await safe_edit(st, f"❌ Download error: <code>{dl2.error_message}</code>",
                            parse_mode=enums.ParseMode.HTML)
            cleanup(tmp)
            return None, None, {}

        done  = dl2.completed_length or 0
        total = dl2.total_length     or file_total or 0
        speed = float(dl2.download_speed or 0)
        eta   = int((total - done) / speed) if speed and total > done else 0

        record.update(
            done=done, total=total, speed=speed, eta=eta,
            elapsed=time.time() - t_start,
            state="📥 Downloading",
        )
        runner._wake_panel(uid)

        if dl2.is_complete or done >= PROBE_ENOUGH:
            if not dl2.is_complete:
                try: api.pause([dl2])
                except Exception: pass
            break

        if time.time() - t_start > 600:
            try: api.remove([dl2])
            except Exception: pass
            record.update(state="❌ Timed out")
            await safe_edit(st, "❌ Download timed out (10 min).",
                            parse_mode=enums.ParseMode.HTML)
            cleanup(tmp)
            return None, None, {}

    # Remove the partial download from aria2c
    try: api.remove([dl2])
    except Exception: pass

    record.update(state="✅ Done", done=done)

    # Find the downloaded file
    path = largest_file(tmp)
    if not path:
        cleanup(tmp)
        await safe_edit(st, "❌ No file found after download.",
                        parse_mode=enums.ParseMode.HTML)
        return None, None, {}

    # ── Probe streams ─────────────────────────────────────────
    await safe_edit(st, "🔍 <b>Probing streams…</b>", parse_mode=enums.ParseMode.HTML)
    try:
        sd, dur = await asyncio.gather(
            FF.probe_streams(path),
            FF.probe_duration(path),
        )
        return path, tmp, {"streams": sd, "duration": dur, "fname": fname}
    except Exception as exc:
        await safe_edit(st, f"❌ Stream probe failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        cleanup(tmp)
        return None, None, {}


# ─────────────────────────────────────────────────────────────
# Download callback
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^dl\|"))
async def dl_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    mode  = parts[1]
    token = parts[2]

    if mode == "cancel":
        _cache.pop(token, None)
        await cb.message.delete()
        return await cb.answer()

    url = _get(token)
    if not url:
        return await cb.answer("Session expired. Resend the link.", show_alert=True)

    uid = cb.from_user.id
    await cb.answer()

    # ── Thumbnail ─────────────────────────────────────────────
    if mode == "thumb":
        st = await cb.message.edit("🖼️ Fetching thumbnail…")
        try:
            import yt_dlp
            with yt_dlp.YoutubeDL({"quiet":True,"skip_download":True}) as ydl:
                info = ydl.extract_info(url, download=False)
            tu = info.get("thumbnail")
            if not tu:
                return await safe_edit(st, "❌ No thumbnail found.")
            await client.send_photo(
                cb.message.chat.id, tu,
                caption=f"🖼️ <b>{info.get('title','')[:60]}</b>",
                parse_mode=enums.ParseMode.HTML,
            )
            await st.delete()
        except Exception as exc:
            await safe_edit(st, f"❌ Thumbnail fetch failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        _cache.pop(token, None)
        return

    # ── Info ──────────────────────────────────────────────────
    if mode == "info":
        kind_i = classify(url)
        if kind_i in ("magnet", "torrent"):
            await _handle_magnet_info(client, cb, url, token)
        else:
            await _handle_info(client, cb, url, token)
        return

    # ── Magnet Stream Extractor ───────────────────────────────
    if mode == "magnet_stream":
        st = await cb.message.edit("🧲 Preparing magnet stream extractor…")
        path, tmp, probe = await _probe_magnet_file(url, uid, st)
        if not path:
            return
        sd   = probe.get("streams", {})
        dur  = probe.get("duration", 0)
        fname = probe.get("fname", os.path.basename(path))

        # Store session for stream extraction callbacks
        sess_tok = hashlib.md5(path.encode()).hexdigest()[:10]
        _magnet_probe[sess_tok] = {"path": path, "tmp": tmp, "streams": sd, "fname": fname}

        await _show_magnet_streams(client, st, sess_tok, sd, dur, fname, uid)
        return

    # ── Stream selector (non-magnet) ─────────────────────────
    if mode == "stream":
        kind_s = classify(url)
        if kind_s in ("magnet", "torrent"):
            st = await cb.message.edit("🧲 Fetching torrent file list…")
            from plugins.stream_extractor import extract_magnet_streams
            await extract_magnet_streams(client, st, url, uid)
        else:
            from plugins.stream_extractor import extract_url_streams
            st = await cb.message.edit("📡 Fetching streams…")
            await extract_url_streams(client, st, url, uid, edit=False)
        return

    # ── Stream download (specific format ID) ─────────────────
    if mode == "stream_dl":
        raw = _get(token)
        if "|||" in raw:
            url2, fmt_id = raw.split("|||", 1)
        else:
            url2   = url
            fmt_id = raw or None
        _cache.pop(token, None)
        asyncio.create_task(
            _launch_download(client, cb.message, url2, uid, fmt_id=fmt_id)
        )
        return

    # ── Standard download ─────────────────────────────────────
    if mode in ("video", "audio"):
        audio_only = (mode == "audio")
        _cache.pop(token, None)
        asyncio.create_task(
            _launch_download(client, cb.message, url, uid, audio_only=audio_only)
        )


# ─────────────────────────────────────────────────────────────
# Magnet stream display + extraction
# ─────────────────────────────────────────────────────────────


async def _show_magnet_streams(
    client: Client, st, sess_tok: str,
    sd: dict, dur: int, fname: str, uid: int,
) -> None:
    from services.utils import human_size, fmt_hms

    v_streams = sd.get("video",    [])
    a_streams = sd.get("audio",    [])
    s_streams = sd.get("subtitle", [])

    lines = [
        "📡 <b>Magnet Stream Extractor</b>",
        f"📄 <code>{fname[:50]}</code>",
        f"⏱ <code>{fmt_hms(dur)}</code>",
        "──────────────────────",
    ]

    if v_streams:
        lines.append(f"🎬 <b>Video</b>  ({len(v_streams)} track)")
        for s in v_streams:
            codec = s.get("codec_name","?").upper()
            w, h  = s.get("width",0), s.get("height",0)
            fr    = s.get("r_frame_rate","0/1")
            try:
                n2, d2 = fr.split("/")
                fps = f"{float(n2)/max(float(d2),1):.0f}fps"
            except Exception:
                fps = ""
            lines.append(f"  • <code>{codec}  {w}x{h}  {fps}</code>")

    if a_streams:
        lines.append(f"🎵 <b>Audio</b>  ({len(a_streams)} track{'s' if len(a_streams)>1 else ''})")
        for s in a_streams:
            codec = s.get("codec_name","?").upper()
            tags  = s.get("tags",{}) or {}
            lang  = (tags.get("language","und")).lower()
            ch    = s.get("channels",0)
            ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
            lines.append(f"  • {_flag(lang)} <code>{codec}  {ch_s}</code>  {_lname(lang)}")

    if s_streams:
        lines.append(f"💬 <b>Subtitles</b>  ({len(s_streams)} track{'s' if len(s_streams)>1 else ''})")
        for s in s_streams:
            codec = s.get("codec_name","?").upper()
            tags  = s.get("tags",{}) or {}
            lang  = (tags.get("language","und")).lower()
            lines.append(f"  • {_flag(lang)} <code>{codec}</code>  {_lname(lang)}")

    if not any([v_streams, a_streams, s_streams]):
        lines.append("⚠️ <i>No streams detected in this file.</i>")

    lines += ["──────────────────────", "<i>Tap a stream to extract it:</i>"]

    rows: list = []
    for s in v_streams:
        idx   = s.get("index", 0)
        codec = s.get("codec_name","?").upper()
        w, h  = s.get("width",0), s.get("height",0)
        rows.append([InlineKeyboardButton(
            f"🎬 Video #{idx}  {codec}  {w}x{h}",
            callback_data=f"mse|v|{sess_tok}|{idx}|{uid}",
        )])
    for s in a_streams:
        idx   = s.get("index", 0)
        codec = s.get("codec_name","?").upper()
        tags  = s.get("tags",{}) or {}
        lang  = (tags.get("language","und")).lower()
        ch    = s.get("channels",0)
        ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
        rows.append([InlineKeyboardButton(
            f"🎵 Audio #{idx}  {_flag(lang)}  {codec}  {ch_s}",
            callback_data=f"mse|a|{sess_tok}|{idx}|{uid}",
        )])
    for s in s_streams:
        idx   = s.get("index", 0)
        codec = s.get("codec_name","?").upper()
        tags  = s.get("tags",{}) or {}
        lang  = (tags.get("language","und")).lower()
        rows.append([InlineKeyboardButton(
            f"💬 Sub #{idx}  {_flag(lang)}  {_lname(lang)}  {codec}",
            callback_data=f"mse|s|{sess_tok}|{idx}|{uid}",
        )])

    if len(a_streams) > 1:
        rows.append([InlineKeyboardButton(
            "🎵 Extract ALL audio tracks",
            callback_data=f"mse|a_all|{sess_tok}|all|{uid}",
        )])
    if len(s_streams) > 1:
        rows.append([InlineKeyboardButton(
            "💬 Extract ALL subtitle tracks",
            callback_data=f"mse|s_all|{sess_tok}|all|{uid}",
        )])

    rows.append([InlineKeyboardButton("❌ Close", callback_data=f"mse|cancel|{sess_tok}|0|{uid}")])

    await safe_edit(st, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


@Client.on_callback_query(filters.regex(r"^mse\|"))
async def mse_cb(client: Client, cb: CallbackQuery):
    """Handle magnet stream extraction callbacks."""
    parts = cb.data.split("|")
    if len(parts) < 5:
        return await cb.answer("Invalid data.", show_alert=True)

    _, stype, sess_tok, idx_str, uid_str = parts[:5]
    user_id = int(uid_str) if uid_str.isdigit() else cb.from_user.id
    await cb.answer()

    if stype == "cancel":
        sess = _magnet_probe.pop(sess_tok, None)
        if sess:
            cleanup(sess["tmp"])
        return await cb.message.delete()

    sess = _magnet_probe.get(sess_tok)
    if not sess:
        return await safe_edit(cb.message, "❌ Session expired. Re-run Stream Extractor.",
                               parse_mode=enums.ParseMode.HTML)

    path  = sess["path"]
    tmp   = sess["tmp"]
    sd    = sess["streams"]
    fname = sess.get("fname", os.path.basename(path))
    base  = os.path.splitext(fname)[0]

    from services import ffmpeg as FF
    from services.uploader import upload_file as _upload

    st = await cb.message.edit(f"📤 Extracting stream…")

    try:
        if stype in ("a_all", "s_all"):
            # Extract all tracks of a type
            stream_list = sd.get("audio" if stype == "a_all" else "subtitle", [])
            if not stream_list:
                return await safe_edit(st, "❌ No tracks found.")

            await safe_edit(st, f"📤 Extracting {len(stream_list)} track(s)…")
            for s in stream_list:
                idx   = s.get("index", 0)
                codec = (s.get("codec_name") or "").lower()
                tags  = s.get("tags", {}) or {}
                lang  = (tags.get("language") or "und").lower()

                if stype == "a_all":
                    out_ext = FF.audio_ext(codec)
                    out = os.path.join(tmp, f"{base}_audio_{idx}_{lang}{out_ext}")
                    caption = f"🎵 <b>Audio #{idx}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"
                else:
                    out_ext = FF.subtitle_ext(codec)
                    out = os.path.join(tmp, f"{base}_sub_{idx}_{lang}{out_ext}")
                    caption = f"💬 <b>Subtitle #{idx}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"

                try:
                    await FF.stream_op(path, out, ["-map", f"0:{idx}", "-c", "copy"])
                    await client.send_document(
                        user_id, out, caption=caption,
                        parse_mode=enums.ParseMode.HTML,
                    )
                except Exception as exc:
                    log.warning("mse all extract idx=%d: %s", idx, exc)

            await st.delete()

        else:
            # Single stream extraction
            all_streams = sd.get("video",[]) + sd.get("audio",[]) + sd.get("subtitle",[])
            target = next((s for s in all_streams if str(s.get("index")) == idx_str), None)

            if not target:
                return await safe_edit(st, f"❌ Stream #{idx_str} not found.")

            codec     = (target.get("codec_name") or "").lower()
            codec_type = target.get("codec_type", "")
            tags      = target.get("tags", {}) or {}
            lang      = (tags.get("language") or "und").lower()

            if codec_type == "subtitle" or stype == "s":
                out_ext = FF.subtitle_ext(codec)
                out = os.path.join(tmp, f"{base}_sub_{idx_str}_{lang}{out_ext}")
                caption = f"💬 <b>Subtitle #{idx_str}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"
                force_doc = True
            elif codec_type == "audio" or stype == "a":
                out_ext = FF.audio_ext(codec)
                out = os.path.join(tmp, f"{base}_audio_{idx_str}_{lang}{out_ext}")
                caption = f"🎵 <b>Audio #{idx_str}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"
                force_doc = False
            else:
                ext = os.path.splitext(path)[1] or ".mp4"
                out = os.path.join(tmp, f"{base}_video_{idx_str}{ext}")
                w   = target.get("width", 0)
                h   = target.get("height", 0)
                caption = f"🎬 <b>Video #{idx_str}</b>  <code>{codec.upper()}  {w}x{h}</code>"
                force_doc = False

            await safe_edit(st, f"📤 Extracting stream #{idx_str}…")
            await FF.stream_op(path, out, ["-map", f"0:{idx_str}", "-c", "copy"])
            await _upload(client, st, out, caption=caption, force_document=force_doc)

    except Exception as exc:
        log.error("mse extraction failed: %s", exc, exc_info=True)
        await safe_edit(st,
            f"❌ Extraction failed: <code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML,
        )


# ─────────────────────────────────────────────────────────────
# Magnet media info (file list + largest file stream info)
# ─────────────────────────────────────────────────────────────

async def _handle_magnet_info(client: Client, cb: CallbackQuery, url: str, token: str) -> None:
    st  = await cb.message.edit("🧲 Probing magnet content…")
    uid = cb.from_user.id

    path, tmp, probe = await _probe_magnet_file(url, uid, st)
    if not path:
        return

    sd    = probe.get("streams", {})
    dur   = probe.get("duration", 0)
    fname = probe.get("fname", os.path.basename(path))
    fsize = os.path.getsize(path) if os.path.exists(path) else 0

    from services.utils import human_size, fmt_hms
    from services import ffmpeg as FF

    v_streams = sd.get("video",    [])
    a_streams = sd.get("audio",    [])
    s_streams = sd.get("subtitle", [])

    lines = [
        "📊 <b>Magnet Media Info</b>",
        "──────────────────────",
        f"📄 <code>{fname[:50]}</code>",
        f"💾 <code>{human_size(fsize)}</code>  ⏱ <code>{fmt_hms(dur)}</code>",
        "──────────────────────",
    ]

    for s in v_streams:
        codec = s.get("codec_name","?").upper()
        w, h  = s.get("width",0), s.get("height",0)
        fr    = s.get("r_frame_rate","0/1")
        try:
            n2, d2 = fr.split("/")
            fps = f"{float(n2)/max(float(d2),1):.3f}fps"
        except Exception:
            fps = "?"
        pix = s.get("pix_fmt","")
        hdr_s = " HDR" if "10" in pix else ""
        lines.append(f"🎬 <code>{codec}  {w}x{h}  {fps}{hdr_s}</code>")

    for s in a_streams:
        codec = s.get("codec_name","?").upper()
        ch    = s.get("channels",0)
        ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
        tags  = s.get("tags",{}) or {}
        lang  = (tags.get("language","und")).lower()
        lines.append(f"🎵 <code>{codec}  {ch_s}</code>  {_flag(lang)} {_lname(lang)}")

    for s in s_streams[:6]:
        codec = s.get("codec_name","?").upper()
        tags  = s.get("tags",{}) or {}
        lang  = (tags.get("language","und")).lower()
        lines.append(f"💬 <code>{codec}</code>  {_flag(lang)} {_lname(lang)}")

    if not any([v_streams, a_streams, s_streams]):
        lines.append("⚠️ <i>No media streams detected.</i>")

    # Try Telegraph for full mediainfo
    kb_rows: list = []
    try:
        raw = await FF.get_mediainfo(path)
        from services.telegraph import post_mediainfo
        tph = await post_mediainfo(fname, raw)
        kb_rows.append([InlineKeyboardButton("📋 Full MediaInfo →", url=tph)])
    except Exception:
        pass

    kb_rows += [
        [InlineKeyboardButton("🧲 Download File", callback_data=f"dl|video|{token}")],
        [InlineKeyboardButton("❌ Close",         callback_data=f"dl|cancel|{token}")],
    ]

    cleanup(tmp)
    await safe_edit(st, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb_rows),
    )


# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# Upload helper — runs as independent task
# ─────────────────────────────────────────────────────────────


async def _upload_and_cleanup(client, msg, path: str, tmp: str) -> None:
    """Run upload serialised through the global upload semaphore.
    This ensures only _UPLOAD_CONCURRENCY uploads run at once even when
    multiple downloads finish at the same time.
    """
    from services.task_runner import runner as _r
    sem = _r._get_upload_sem()
    async with sem:
        try:
            await upload_file(client, msg, path)
        except Exception as exc:
            log.error("Upload failed for %s: %s", path, exc)
        finally:
            cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Safe delete helper
# ─────────────────────────────────────────────────────────────

async def _safe_delete(msg) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Download launcher — registers task BEFORE download so panel shows it
# ─────────────────────────────────────────────────────────────

async def _launch_download(
    client: Client,
    panel_msg,
    url: str,
    uid: int,
    audio_only: bool = False,
    fmt_id: str | None = None,
) -> None:
    tmp = make_tmp(cfg.download_dir, uid)

    # Fix E: extract a clean human label from the magnet/URL.
    kind = classify(url)
    if kind in ("magnet", "torrent"):
        dn_match = re.search(r"[&?]dn=([^&]+)", url)
        if dn_match:
            label = _up.unquote_plus(dn_match.group(1))[:50]
        else:
            ih_match = re.search(r"xt=urn:btih:([a-fA-F0-9]{6,}|[A-Za-z2-7]{6,})", url)
            label = f"Magnet {ih_match.group(1)[:12].upper()}" if ih_match else "Magnet Download"
    else:
        # Always URL-decode the path segment so "%20" becomes spaces etc.
        raw = url.split("/")[-1].split("?")[0]
        label = _up.unquote_plus(raw)[:50] or "Download"

    # Silently delete the keyboard message — the panel appears automatically
    asyncio.create_task(_safe_delete(panel_msg))

    try:
        path = await smart_download(
            url, tmp,
            audio_only=audio_only,
            fmt_id=fmt_id,
            user_id=uid,
            label=label,
        )
    except Exception as exc:
        log.error("_launch_download uid=%d failed: %s", uid, exc, exc_info=True)
        cleanup(tmp)
        asyncio.create_task(_safe_delete(panel_msg))
        try:
            from core.session import get_client
            await get_client().send_message(
                uid,
                f"❌ <b>Download failed</b>\n\n<code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as send_exc:
            log.warning("_launch_download: could not notify uid=%d: %s", uid, send_exc)
        return

    # Delete the "starting…" status message — the panel takes over from here
    asyncio.create_task(_safe_delete(panel_msg))

    if os.path.isdir(path):
        resolved = largest_file(path)
        if resolved:
            path = resolved

    if not os.path.isfile(path):
        log.error("_launch_download uid=%d: path not a file after download: %s", uid, path)
        cleanup(tmp)
        try:
            from core.session import get_client
            await get_client().send_message(
                uid,
                "❌ <b>Download failed</b>\n\n<code>Output file not found</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    fsize = os.path.getsize(path)
    if fsize > cfg.file_limit_b:
        cleanup(tmp)
        try:
            from core.session import get_client
            await get_client().send_message(
                uid,
                f"❌ <b>File too large</b>\n"
                f"Size: <code>{human_size(fsize)}</code>\n"
                f"Limit: <code>{human_size(cfg.file_limit_b)}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    from types import SimpleNamespace
    _up_dummy = SimpleNamespace(
        edit=lambda *a, **kw: asyncio.sleep(0),
        delete=lambda: asyncio.sleep(0),
        chat=SimpleNamespace(id=uid),
    )

    # ── Smart clean + prefix/suffix ───────────────────────────────────────────
    from core.session import settings as _settings
    from services.utils import smart_clean_filename
    s = await _settings.get(uid)

    fname    = os.path.basename(path)
    cleaned  = smart_clean_filename(fname)          # strip technical noise
    name, ext = os.path.splitext(cleaned)

    prefix = s.get("prefix", "").strip()
    suffix = s.get("suffix", "").strip()

    final_name = f"{prefix}{name}{suffix}{ext}"

    if final_name != fname:
        new_path = os.path.join(os.path.dirname(path), final_name)
        try:
            os.rename(path, new_path)
            path = new_path
            log.info("Renamed: %s → %s", fname, final_name)
        except OSError as _re:
            log.warning("Rename failed: %s", _re)
    # ─────────────────────────────────────────────────────────────────────────

    asyncio.create_task(_upload_and_cleanup(client, _up_dummy, path, tmp))


# ─────────────────────────────────────────────────────────────
# Info handler (non-magnet)
# ─────────────────────────────────────────────────────────────

async def _handle_info(client: Client, cb: CallbackQuery, url: str, token: str) -> None:
    st   = await cb.message.edit("📊 Fetching info…")
    kind = classify(url)

    if kind == "ytdlp":
        try:
            import yt_dlp
            with yt_dlp.YoutubeDL({"quiet":True,"skip_download":True,"noplaylist":True}) as ydl:
                info = ydl.extract_info(url, download=False)
            dur   = info.get("duration", 0)
            title = info.get("title","N/A")
            lines = [
                "📊 <b>Media Info</b>", "──────────────────",
                f"🎬 <b>{title[:55]}</b>",
                f"👤 {info.get('uploader','N/A')}",
                f"⏱ {_fmt_dur(dur)}",
            ]
            if info.get("view_count"):
                lines.append(f"👁 {info['view_count']:,} views")
            lines.append("──────────────────")
            seen: set = set()
            count = 0
            for f in reversed(info.get("formats", [])):
                note  = f.get("format_note") or f.get("resolution","")
                vc    = f.get("vcodec","none")
                ext_f = f.get("ext","?")
                tbr   = int(f.get("tbr") or 0)
                if note and note not in seen and vc != "none":
                    seen.add(note)
                    count += 1
                    lines.append(f"📦 <code>{note}</code> [{ext_f}] {tbr}kbps")
                if count >= 6:
                    break
            await safe_edit(st, "\n".join(lines),
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎬 Download Video", callback_data=f"dl|video|{token}"),
                     InlineKeyboardButton("🎵 Download Audio", callback_data=f"dl|audio|{token}")],
                    [InlineKeyboardButton("❌ Close",          callback_data=f"dl|cancel|{token}")],
                ]))
        except Exception as exc:
            await safe_edit(st, f"❌ Info failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        return

    # Direct link
    try:
        from services.ffmpeg import probe_streams, probe_duration, get_mediainfo
        headers = {"User-Agent":"Mozilla/5.0","Range":"bytes=0-5242880"}
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, allow_redirects=True) as resp:
                cd = resp.headers.get("Content-Disposition","")
                fn = ""
                if "filename=" in cd:
                    fn = cd.split("filename=")[-1].strip().strip('"')
                if not fn:
                    from pathlib import Path as _P
                    fn = _P(url.split("?")[0]).name or "file"
                cr    = resp.headers.get("Content-Range","").split("/")[-1]
                total = int(cr) if cr.isdigit() else int(resp.headers.get("Content-Length",0))
                chunk = await resp.content.read(5*1024*1024)

        with tempfile.NamedTemporaryFile(
            suffix=os.path.splitext(fn)[1] or ".tmp", delete=False,
        ) as tf:
            tf.write(chunk)
            tmp_path = tf.name

        raw, sd, dur = await asyncio.gather(
            get_mediainfo(tmp_path),
            probe_streams(tmp_path),
            probe_duration(tmp_path),
        )
        os.unlink(tmp_path)

        lines = [
            "📊 <b>Media Info (Direct)</b>", "──────────────────",
            f"📄 <code>{fn[:50]}</code>",
            f"💾 <code>{human_size(total) if total else '—'}</code>  ⏱ <code>{_fmt_dur(dur)}</code>",
            "──────────────────",
        ]
        for s in sd.get("video", []):
            codec = s.get("codec_name","?").upper()
            w, h  = s.get("width",0), s.get("height",0)
            try:
                n2, d2 = s.get("r_frame_rate","0/1").split("/")
                fps = f"{float(n2)/max(float(d2),1):.2f}"
            except Exception:
                fps = "?"
            lines.append(f"🎬 <code>{codec} {w}x{h} @ {fps}fps</code>")
        for s in sd.get("audio", []):
            codec = s.get("codec_name","?").upper()
            ch    = s.get("channels",0)
            ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch,f"{ch}ch") if ch else ""
            lines.append(f"🎵 <code>{codec} {ch_s}</code>")

        kb = [
            [InlineKeyboardButton("🎬 Download", callback_data=f"dl|video|{token}")],
            [InlineKeyboardButton("❌ Close",    callback_data=f"dl|cancel|{token}")],
        ]
        try:
            from services.telegraph import post_mediainfo
            tph = await post_mediainfo(fn, raw)
            kb.insert(0, [InlineKeyboardButton("📋 Full MediaInfo →", url=tph)])
        except Exception:
            pass
        await safe_edit(st, "\n".join(lines),
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(kb))
    except Exception as exc:
        await safe_edit(st, f"❌ Could not probe: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🎬 Download", callback_data=f"dl|video|{token}")],
                            [InlineKeyboardButton("❌ Close",    callback_data=f"dl|cancel|{token}")],
                        ]))
