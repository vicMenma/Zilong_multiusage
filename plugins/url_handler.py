"""
plugins/url_handler.py
Handles URL messages and torrent files.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import time

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.downloader import classify, smart_download, download_ytdlp
from services.tg_download import tg_download
from services.uploader import upload_file
from services.utils import cleanup, human_size, largest_file, make_tmp, safe_edit

log = logging.getLogger(__name__)

URL_RE = re.compile(r"https?://\S+|magnet:\?\S+", re.I)

# Simple LRU token cache
_cache: dict[str, str] = {}
_CACHE_MAX = 500


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
    elif kind in ("magnet","torrent"):
        rows += [
            [InlineKeyboardButton("🧲 Download",    callback_data=f"dl|video|{token}")],
            [InlineKeyboardButton("📊 Magnet Info", callback_data=f"dl|info|{token}")],
        ]
    elif kind == "gdrive":
        rows += [
            [InlineKeyboardButton("☁️ Download",    callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("🎵 Audio Only",  callback_data=f"dl|audio|{token}")],
        ]
    elif kind == "mediafire":
        rows += [
            [InlineKeyboardButton("📁 Download",    callback_data=f"dl|video|{token}")],
        ]
    else:
        rows += [
            [InlineKeyboardButton("🎬 Download File", callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("📊 Media Info",    callback_data=f"dl|info|{token}")],
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
    await users.register(uid)

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

async def handle_torrent_file(client: Client, msg: Message, media, uid: int):
    st  = await msg.reply("🌊 Torrent received. Starting aria2c…")
    tmp = make_tmp(cfg.download_dir, uid)
    try:
        tp = await tg_download(client, media.file_id,
                               os.path.join(tmp, "dl.torrent"), st, fname="dl.torrent")
        from services.downloader import download_aria2
        result = await download_aria2(tp, tmp, is_file=True)
    except Exception as exc:
        cleanup(tmp)
        return await safe_edit(st, f"❌ Torrent failed: <code>{exc}</code>",
                               parse_mode=enums.ParseMode.HTML)
    await upload_file(client, st, result)
    cleanup(tmp)


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

    # ── Thumbnail (yt-dlp) ────────────────────────────────────
    if mode == "thumb":
        st = await cb.message.edit("🖼️ Fetching thumbnail…")
        try:
            import yt_dlp
            with yt_dlp.YoutubeDL({"quiet":True,"skip_download":True}) as ydl:
                info = ydl.extract_info(url, download=False)
            tu = info.get("thumbnail")
            if not tu:
                return await safe_edit(st, "❌ No thumbnail found.")
            await client.send_photo(cb.message.chat.id, tu,
                caption=f"🖼️ <b>{info.get('title','')[:60]}</b>",
                parse_mode=enums.ParseMode.HTML)
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
            from plugins.stream_extractor import extract_magnet_streams
            st = await cb.message.edit("🧲 Fetching torrent info…")
            await extract_magnet_streams(client, st, url, uid)
        else:
            await _handle_info(client, cb, url, token)
        return

    # ── Stream selector — delegate to full stream extractor ───
    if mode == "stream":
        from plugins.stream_extractor import extract_url_streams
        st = await cb.message.edit("📡 Fetching streams…")
        await extract_url_streams(client, st, url, uid, edit=False)
        return

    # ── Stream download (specific format ID) ─────────────────
    if mode == "stream_dl":
        fmt_id = parts[3] if len(parts) > 3 else None
        await _launch_download(client, cb.message, url, uid, fmt_id=fmt_id)
        _cache.pop(token, None)
        return

    # ── Standard download ─────────────────────────────────────
    if mode in ("video","audio"):
        audio_only = (mode == "audio")
        await _launch_download(client, cb.message, url, uid, audio_only=audio_only)
        _cache.pop(token, None)


# ─────────────────────────────────────────────────────────────
# Download launcher
# ─────────────────────────────────────────────────────────────

async def _launch_download(
    client: Client,
    panel_msg,
    url: str,
    uid: int,
    audio_only: bool = False,
    fmt_id: str | None = None,
):
    tmp  = make_tmp(cfg.download_dir, uid)
    st   = panel_msg
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
            engine=classify(url),
        )
        await safe_edit(st, panel, parse_mode=enums.ParseMode.HTML)

    await safe_edit(st, "📥 Starting download…")

    try:
        path = await smart_download(
            url, tmp,
            audio_only=audio_only,
            fmt_id=fmt_id,
            progress=_progress,
        )
    except Exception as exc:
        cleanup(tmp)
        return await safe_edit(st,
            f"❌ <b>Download failed</b>\n\n<code>{exc}</code>",
            parse_mode=enums.ParseMode.HTML)

    if os.path.isdir(path):
        resolved = largest_file(path)
        if resolved:
            path = resolved

    if not os.path.isfile(path):
        cleanup(tmp)
        return await safe_edit(st, "❌ File not found after download.",
                               parse_mode=enums.ParseMode.HTML)

    fsize = os.path.getsize(path)
    if fsize > cfg.file_limit_b:
        cleanup(tmp)
        return await safe_edit(st,
            f"❌ <b>File too large</b>\n"
            f"Size: <code>{human_size(fsize)}</code>\n"
            f"Limit: <code>{human_size(cfg.file_limit_b)}</code>",
            parse_mode=enums.ParseMode.HTML)

    await upload_file(client, st, path)
    cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Info handler
# ─────────────────────────────────────────────────────────────

async def _handle_info(client: Client, cb: CallbackQuery, url: str, token: str):
    st   = await cb.message.edit("📊 Fetching info…")
    kind = classify(url)

    if kind in ("magnet","torrent"):
        import urllib.parse as _up
        dn  = re.search(r"[&?]dn=([^&]+)", url)
        xt  = re.search(r"xt=urn:btih:([a-fA-F0-9]{40}|[A-Za-z2-7]{32})", url)
        xl  = re.search(r"[&?]xl=([^&]+)", url)
        trs = re.findall(r"tr=([^&]+)", url)
        name = _up.unquote_plus(dn.group(1)) if dn else "Unknown"
        h    = xt.group(1).upper() if xt else "—"
        size = int(xl.group(1)) if xl else 0
        lines = [
            "🧲 <b>Magnet Info</b>","──────────────────",
            f"📄 <code>{name[:60]}</code>",
            f"🔑 <code>{h}</code>",
        ]
        if size:
            lines.append(f"💾 <code>{human_size(size)}</code>")
        lines.append(f"📡 <b>Trackers:</b> <code>{len(trs)}</code>")
        await safe_edit(st, "\n".join(lines), parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🧲 Download", callback_data=f"dl|video|{token}")],
                [InlineKeyboardButton("❌ Close",    callback_data=f"dl|cancel|{token}")],
            ]))
        return

    if kind == "ytdlp":
        try:
            import yt_dlp
            with yt_dlp.YoutubeDL({"quiet":True,"skip_download":True,"noplaylist":True}) as ydl:
                info = ydl.extract_info(url, download=False)
            dur   = info.get("duration", 0)
            title = info.get("title","N/A")
            lines = [
                "📊 <b>Media Info</b>","──────────────────",
                f"🎬 <b>{title[:55]}</b>",
                f"👤 {info.get('uploader','N/A')}",
                f"⏱ {_fmt_dur(dur)}",
            ]
            if info.get("view_count"):
                lines.append(f"👁 {info['view_count']:,} views")
            lines.append("──────────────────")
            seen: set = set(); count = 0
            for f in reversed(info.get("formats",[])):
                note = f.get("format_note") or f.get("resolution","")
                vc   = f.get("vcodec","none"); ext_f = f.get("ext","?")
                tbr  = int(f.get("tbr") or 0)
                if note and note not in seen and vc != "none":
                    seen.add(note); count += 1
                    lines.append(f"📦 <code>{note}</code> [{ext_f}] {tbr}kbps")
                if count >= 6: break
            await safe_edit(st, "\n".join(lines), parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎬 Download Video", callback_data=f"dl|video|{token}"),
                     InlineKeyboardButton("🎵 Download Audio", callback_data=f"dl|audio|{token}")],
                    [InlineKeyboardButton("❌ Close",          callback_data=f"dl|cancel|{token}")],
                ]))
        except Exception as exc:
            await safe_edit(st, f"❌ Info failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        return

    # Direct link — probe first 5MB
    try:
        import aiohttp, tempfile
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
            tf.write(chunk); tmp_path = tf.name

        raw, sd, dur = await asyncio.gather(
            get_mediainfo(tmp_path),
            probe_streams(tmp_path),
            probe_duration(tmp_path),
        )
        os.unlink(tmp_path)

        lines = [
            "📊 <b>Media Info (Direct)</b>","──────────────────",
            f"📄 <code>{fn[:50]}</code>",
            f"💾 <code>{human_size(total) if total else '—'}</code>  ⏱ <code>{_fmt_dur(dur)}</code>",
            "──────────────────",
        ]
        for s in sd.get("video",[]):
            codec=s.get("codec_name","?").upper(); w=s.get("width",0); h=s.get("height",0)
            try:
                n2,d2=s.get("r_frame_rate","0/1").split("/")
                fps=f"{float(n2)/max(float(d2),1):.2f}"
            except Exception: fps="?"
            lines.append(f"🎬 <code>{codec} {w}x{h} @ {fps}fps</code>")
        for s in sd.get("audio",[]):
            codec=s.get("codec_name","?").upper(); ch=s.get("channels",0)
            ch_s={1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch,f"{ch}ch") if ch else ""
            lines.append(f"🎵 <code>{codec} {ch_s}</code>")

        kb = [[InlineKeyboardButton("🎬 Download", callback_data=f"dl|video|{token}")],
              [InlineKeyboardButton("❌ Close",    callback_data=f"dl|cancel|{token}")]]
        try:
            from services.telegraph import post_mediainfo
            tph = await post_mediainfo(fn, raw)
            kb.insert(0,[InlineKeyboardButton("📋 Full MediaInfo →", url=tph)])
        except Exception:
            pass
        await safe_edit(st, "\n".join(lines), parse_mode=enums.ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(kb))
    except Exception as exc:
        await safe_edit(st, f"❌ Could not probe: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🎬 Download", callback_data=f"dl|video|{token}")],
                            [InlineKeyboardButton("❌ Close",    callback_data=f"dl|cancel|{token}")],
                        ]))


# ─────────────────────────────────────────────────────────────
# Stream selector (yt-dlp formats)
# ─────────────────────────────────────────────────────────────

async def _handle_stream_select(
    client: Client, cb: CallbackQuery, url: str, token: str, uid: int
):
    st = await cb.message.edit("📡 Fetching available streams…")
    try:
        import yt_dlp
        with yt_dlp.YoutubeDL({"quiet":True,"skip_download":True,"noplaylist":True}) as ydl:
            info = ydl.extract_info(url, download=False)

        combined: list = []; video_only: list = []; seen: set = set()
        for f in reversed(info.get("formats",[])):
            fid   = f.get("format_id","")
            note  = f.get("format_note") or f.get("resolution","")
            vc    = f.get("vcodec","none"); ac = f.get("acodec","none")
            ext_f = f.get("ext","?"); tbr = int(f.get("tbr") or 0)
            fsz   = f.get("filesize") or f.get("filesize_approx") or 0
            sz_s  = f" [{human_size(fsz)}]" if fsz else ""
            if not note or not fid or vc == "none": continue
            k2 = f"{note}_{ext_f}"
            if k2 in seen: continue
            seen.add(k2)
            if ac not in ("none",""):
                combined.append((fid, f"🎬 {note} [{ext_f}]{sz_s}"))
            else:
                video_only.append((fid, f"🎞 {note} [{ext_f}]{sz_s} (no audio)"))

        def _rows(items, max_n=8):
            rows = []
            for fid, label in items[:max_n]:
                t2 = _store(f"{url}|||{fid}")
                rows.append([InlineKeyboardButton(label, callback_data=f"dl|stream_dl|{t2}")])
            return rows

        buttons = _rows(combined, 8) + _rows(video_only, 4)
        if not buttons:
            return await safe_edit(st, "❌ No streams found. Try Download Video instead.",
                                   parse_mode=enums.ParseMode.HTML)
        buttons.append([InlineKeyboardButton("🎵 Best Audio Only", callback_data=f"dl|audio|{token}")])
        buttons.append([InlineKeyboardButton("❌ Cancel",           callback_data=f"dl|cancel|{token}")])
        title = (info.get("title") or "")[:50]
        await safe_edit(st,
            f"📡 <b>Stream Extractor</b>\n<b>{title}</b>\n\n"
            f"<i>{len(combined)} combined, {len(video_only)} video-only</i>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons))

    except Exception as exc:
        await safe_edit(st, f"❌ Stream fetch failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
