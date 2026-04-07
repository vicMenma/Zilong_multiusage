"""
plugins/url_handler.py
Handles URL messages and torrent files.

DOWNLOAD-FIRST IMPROVEMENTS (applied directly):

PATCH 1 — _probe_magnet_file(): full download instead of 30 MB cap
PATCH 2 — ccv_resolution_cb(): always download-first for CC convert
Other fixes (unchanged from prior audit):
  - _launch_download: live progress panel before smart_download
  - _handle_info (direct URLs): ffprobe on URL, zero bytes downloaded
  - _launch_download double-delete: only one _safe_delete call
"""
from __future__ import annotations

import asyncio
import hashlib
import json as _json
import logging
import os
import re
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
from services.downloader import classify, smart_download
from services.tg_download import tg_download
from services.uploader import upload_file
from services.utils import (
    cleanup, human_size, lang_flag, lang_name,
    largest_file, make_tmp, progress_panel, safe_edit,
    smart_clean_filename,
)

log = logging.getLogger(__name__)

_flag  = lang_flag
_lname = lang_name

URL_RE = re.compile(r"https?://\S+|magnet:\?\S+", re.I)

_cache: dict[str, str] = {}
_CACHE_MAX = 500

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


def _evict_magnet_probes() -> None:
    now  = time.time()
    dead = [k for k, v in list(_magnet_probe.items())
            if now - v.get("created", 0) > 1800]
    for k in dead:
        sess = _magnet_probe.pop(k, None)
        if sess:
            cleanup(sess.get("tmp", ""))


# ─────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────

def _url_kb(token: str, kind: str) -> InlineKeyboardMarkup:
    if kind == "ytdlp":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 Download Video",    callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("🎵 Download Audio",    callback_data=f"dl|audio|{token}")],
            [InlineKeyboardButton("🔵 Stream Extractor",  callback_data=f"dl|stream|{token}"),
             InlineKeyboardButton("🖼️ Thumbnail",          callback_data=f"dl|thumb|{token}")],
            [InlineKeyboardButton("📊 Media Info",         callback_data=f"dl|info|{token}"),
             InlineKeyboardButton("🟡 Convert",            callback_data=f"dl|convert|{token}")],
            [InlineKeyboardButton("🔥 Hardsub",           callback_data=f"dl|hardsub|{token}"),
             InlineKeyboardButton("❌ Cancel",             callback_data=f"dl|cancel|{token}")],
        ])
    elif kind in ("magnet", "torrent"):
        seedr_ready = bool(
            os.environ.get("SEEDR_USERNAME") and os.environ.get("SEEDR_PASSWORD")
        )
        rows = [
            [InlineKeyboardButton("🟢 Download (local)",    callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("🔵 Stream Extractor",    callback_data=f"dl|magnet_stream|{token}")],
            [InlineKeyboardButton("📊 Media Info",          callback_data=f"dl|info|{token}"),
             InlineKeyboardButton("🔥 Hardsub",            callback_data=f"dl|hardsub|{token}")],
            [InlineKeyboardButton("🟡 Convert",             callback_data=f"dl|convert|{token}"),
             InlineKeyboardButton("❌ Cancel",              callback_data=f"dl|cancel|{token}")],
        ]
        if seedr_ready:
            rows.insert(0, [
                InlineKeyboardButton(
                    "☁️ Download via Seedr",
                    callback_data=f"dl|seedr|{token}",
                )
            ])
        return InlineKeyboardMarkup(rows)
    elif kind == "gdrive":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 Download",           callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("🎵 Audio Only",         callback_data=f"dl|audio|{token}")],
            [InlineKeyboardButton("🔵 Stream Extractor",   callback_data=f"dl|stream|{token}"),
             InlineKeyboardButton("🔥 Hardsub",           callback_data=f"dl|hardsub|{token}")],
            [InlineKeyboardButton("🟡 Convert",            callback_data=f"dl|convert|{token}"),
             InlineKeyboardButton("❌ Cancel",             callback_data=f"dl|cancel|{token}")],
        ])
    elif kind == "mediafire":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 Download",           callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("❌ Cancel",             callback_data=f"dl|cancel|{token}")],
        ])
    else:  # direct / http
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 Download File",      callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("🔵 Stream Extractor",   callback_data=f"dl|stream|{token}")],
            [InlineKeyboardButton("📊 Media Info",         callback_data=f"dl|info|{token}"),
             InlineKeyboardButton("🔥 Hardsub",           callback_data=f"dl|hardsub|{token}")],
            [InlineKeyboardButton("🟡 Convert",            callback_data=f"dl|convert|{token}"),
             InlineKeyboardButton("❌ Cancel",             callback_data=f"dl|cancel|{token}")],
        ])


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

    uid  = msg.from_user.id
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
    _dummy = SimpleNamespace(
        edit=lambda *a, **kw: asyncio.sleep(0),
        delete=lambda: asyncio.sleep(0),
    )
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
    asyncio.create_task(_upload_and_cleanup(client, uid, result, tmp))


# ─────────────────────────────────────────────────────────────
# Magnet probe
# ─────────────────────────────────────────────────────────────

async def _probe_magnet_file(magnet: str, uid: int, st) -> tuple[str | None, str | None, dict]:
    from services import ffmpeg as FF
    from services.downloader import smart_download as _smart_dl

    tmp = make_tmp(cfg.download_dir, uid)

    await safe_edit(st,
        "🧲 <b>Magnet — Downloading Complete File</b>\n"
        "──────────────────────\n\n"
        "<i>Using aria2c to download the full file.\n"
        "Stream analysis and mediainfo will be 100% accurate.\n"
        "File is cached for instant hardsub/convert after this.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        path_or_dir = await _smart_dl(
            magnet, tmp,
            user_id=uid,
            label="Magnet Probe",
            msg=st,
        )

        if os.path.isdir(path_or_dir):
            path = largest_file(path_or_dir)
            if not path:
                from services.utils import all_video_files as _avf
                files = _avf(path_or_dir, min_bytes=0)
                path = files[0] if files else None
        else:
            path = path_or_dir

        if not path or not os.path.isfile(path):
            await safe_edit(st,
                "❌ <b>No file found after download.</b>\n\n"
                "<i>The torrent may be empty or all files were filtered out.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
            cleanup(tmp)
            return None, None, {}

        fname = os.path.basename(path)
        fsize = os.path.getsize(path)
        log.info("[MagnetProbe] Downloaded %s (%s) — probing streams",
                 fname, human_size(fsize))

        await safe_edit(st,
            f"🔍 <b>Probing streams…</b>\n\n"
            f"📄 <code>{fname[:50]}</code>\n"
            f"💾 <code>{human_size(fsize)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

        sd, dur = await asyncio.gather(
            FF.probe_streams(path),
            FF.probe_duration(path),
        )
        return path, tmp, {"streams": sd, "duration": dur, "fname": fname}

    except Exception as exc:
        log.error("[MagnetProbe] Failed: %s", exc, exc_info=True)
        await safe_edit(st,
            f"❌ <b>Magnet download failed</b>\n\n"
            f"<code>{str(exc)[:300]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        cleanup(tmp)
        return None, None, {}


# ─────────────────────────────────────────────────────────────
# Background magnet download for hardsub flow
# ─────────────────────────────────────────────────────────────

async def _hardsub_magnet_dl(st, url: str, uid: int, tmp: str, fname: str) -> None:
    from plugins.hardsub import _STATE, _clear
    try:
        path = await smart_download(url, tmp, user_id=uid, label=fname)
        if os.path.isdir(path):
            resolved = largest_file(path)
            if resolved:
                path = resolved
        if not os.path.isfile(path):
            raise FileNotFoundError("No output file found")
        fname_dl = os.path.basename(path)
        _STATE[uid]["videos"][0] = {
            "path": path, "url": None,
            "fname": fname_dl, "resolution": 0,
        }
        _STATE[uid]["step"] = "waiting_subtitle"
        await safe_edit(
            st,
            "🔥 <b>Hardsub</b>\n"
            "──────────────────────\n\n"
            f"✅ Downloaded: <code>{fname_dl[:45]}</code>\n\n"
            "Now send the <b>subtitle</b>:\n"
            "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
            "• A <b>URL</b> to a subtitle file\n\n"
            "<i>Send /cancel to abort.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        _clear(uid)
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)


# ─────────────────────────────────────────────────────────────
# yt-dlp quality picker  (NEW)
# ─────────────────────────────────────────────────────────────

async def _show_ytdlp_quality_picker(
    client: Client, st, url: str, token: str, uid: int,
) -> None:
    """
    Fetch yt-dlp format info and display quality-bucket buttons inline.
    Reuses _parse_yt_formats / _QUALITY_ORDER / _QUALITY_ICON from
    stream_extractor so there is zero duplication of parsing logic.
    Falls back to best-quality download if format fetch fails.
    """
    import yt_dlp as _yt_dlp
    from plugins.stream_extractor import _parse_yt_formats, _QUALITY_ORDER, _QUALITY_ICON

    try:
        ydl_opts = {"quiet": True, "no_warnings": True, "noplaylist": True}
        loop = asyncio.get_running_loop()

        def _extract() -> dict:
            with _yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)

        info = await loop.run_in_executor(None, _extract)
    except Exception as exc:
        await safe_edit(
            st,
            f"❌ <b>Could not fetch formats</b>\n\n<code>{exc}</code>\n\n"
            "<i>Falling back to best quality…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_launch_download(client, st, url, uid))
        return

    groups = _parse_yt_formats(info)
    if not groups:
        await safe_edit(st, "❌ No downloadable formats found.")
        return

    title    = (info.get("title") or "")[:50]
    uploader = info.get("uploader") or info.get("channel") or ""
    dur      = info.get("duration", 0)
    h, rem   = divmod(int(dur or 0), 3600)
    m, s     = divmod(rem, 60)
    dur_s    = (f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}") if dur else ""

    lines = [
        "📥 <b>Choose Quality</b>",
        "──────────────────────",
        f"<b>{title}</b>" if title else "",
        f"👤 {uploader}" if uploader else "",
        f"⏱ {dur_s}" if dur_s else "",
        "──────────────────────",
        "<i>Tap a quality to see formats:</i>",
    ]

    # Serialise format groups into cache so bucket callback can read without re-fetch
    _cache[f"ytinfo|{token}"] = _json.dumps({
        "url":   url,
        "title": title,
        "groups": {
            bucket: [
                {
                    "fmt_id":   f.fmt_id,
                    "label":    f.label,
                    "detail":   f.detail,
                    "filesize": f.filesize,
                }
                for f in fmts
            ]
            for bucket, fmts in groups.items()
        },
    })

    rows = []
    for bucket in _QUALITY_ORDER:
        fmts = groups.get(bucket, [])
        if not fmts:
            continue
        icon  = _QUALITY_ICON.get(bucket, "📦")
        count = len(fmts)
        rows.append([InlineKeyboardButton(
            f"{icon} {bucket}  ({count} option{'s' if count > 1 else ''})",
            callback_data=f"dlq|bucket|{token}|{bucket}",
        )])

    rows.append([
        InlineKeyboardButton(
            "⚡ Best quality (auto)",
            callback_data=f"dlq|best|{token}|",
        ),
        InlineKeyboardButton(
            "🎵 Audio only",
            callback_data=f"dlq|audio|{token}|",
        ),
    ])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"dl|cancel|{token}")])

    await safe_edit(
        st,
        "\n".join(l for l in lines if l),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


# ─────────────────────────────────────────────────────────────
# Download callback
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# Seedr cloud download pipeline  (NEW)
# ─────────────────────────────────────────────────────────────

async def _seedr_download(client, st, magnet: str, uid: int) -> None:
    """
    Full Seedr pipeline with live status panel.
    Seedr downloads the torrent on their servers at datacenter speed,
    then the bot downloads the resulting files via HTTPS and uploads to Telegram.
    Files are auto-deleted from Seedr after download to stay within 2 GB free tier.
    """
    from services.seedr import download_via_seedr
    from services.utils import make_tmp, cleanup, human_size
    from core.session import settings as _settings

    tmp = make_tmp(cfg.download_dir, uid)

    async def _progress(stage: str, pct: float, detail: str) -> None:
        icons = {
            "adding":      "⬆️",
            "waiting":     "⏳",
            "downloading": "☁️",
            "fetching":    "🔗",
            "dl_file":     "⬇️",
        }
        icon = icons.get(stage, "⏳")
        bar  = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        await safe_edit(
            st,
            f"☁️ <b>Seedr Cloud Download</b>\n"
            "──────────────────────\n\n"
            f"{icon} <i>{detail}</i>\n\n"
            f"<code>[{bar}]</code>  <b>{pct:.0f}%</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    try:
        local_paths = await download_via_seedr(
            magnet, tmp, progress_cb=_progress, timeout_s=7200,
        )
    except Exception as exc:
        log.error("[Seedr] Pipeline failed: %s", exc, exc_info=True)
        cleanup(tmp)
        return await safe_edit(
            st,
            f"❌ <b>Seedr download failed</b>\n\n<code>{str(exc)[:300]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    if not local_paths:
        cleanup(tmp)
        return await safe_edit(
            st, "❌ <b>Seedr: no files downloaded.</b>",
            parse_mode=enums.ParseMode.HTML,
        )

    total = len(local_paths)
    if total > 1:
        await safe_edit(
            st,
            f"✅ <b>Seedr done — {total} files</b>\n📤 <i>Uploading…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    s      = await _settings.get(uid)
    prefix = s.get("prefix", "").strip()
    suffix_s = s.get("suffix", "").strip()

    try:
        for i, fpath in enumerate(local_paths, 1):
            fsize = os.path.getsize(fpath)
            if fsize > cfg.file_limit_b:
                await client.send_message(
                    uid,
                    f"⚠️ <b>Skipped ({i}/{total})</b>\n"
                    f"<code>{os.path.basename(fpath)}</code>\n"
                    f"<code>{human_size(fsize)}</code> exceeds limit",
                    parse_mode=enums.ParseMode.HTML,
                )
                continue

            fname     = os.path.basename(fpath)
            name, ext = os.path.splitext(fname)
            final     = f"{prefix}{name}{suffix_s}{ext}"
            if final != fname:
                new_path = os.path.join(os.path.dirname(fpath), final)
                try:
                    os.rename(fpath, new_path)
                    fpath = new_path
                except OSError:
                    pass

            upload_st = await client.send_message(
                uid,
                f"📤 <b>Uploading {i}/{total}</b>\n"
                f"<code>{os.path.basename(fpath)}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            await upload_file(client, upload_st, fpath, user_id=uid)

            if i < total:
                await asyncio.sleep(2)

        try:
            await st.delete()
        except Exception:
            pass

    finally:
        cleanup(tmp)

@Client.on_callback_query(filters.regex(r"^dl\|"))
async def dl_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    mode  = parts[1]
    token = parts[2]

    if mode == "cancel":
        _cache.pop(token, None)
        _cache.pop(f"ytinfo|{token}", None)
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
        st   = await cb.message.edit("🧲 Preparing magnet stream extractor…")
        path, tmp, probe = await _probe_magnet_file(url, uid, st)
        if not path:
            return
        sd    = probe.get("streams", {})
        dur   = probe.get("duration", 0)
        fname = probe.get("fname", os.path.basename(path))
        sess_tok = hashlib.md5(path.encode()).hexdigest()[:10]
        _magnet_probe[sess_tok] = {
            "path": path, "tmp": tmp, "streams": sd,
            "fname": fname, "created": time.time(),
        }
        _evict_magnet_probes()
        await _show_magnet_streams(client, st, sess_tok, sd, dur, fname, uid)
        return

    # ── Seedr cloud download ──────────────────────────────────
    if mode == "seedr":
        username = os.environ.get("SEEDR_USERNAME", "").strip()
        password = os.environ.get("SEEDR_PASSWORD", "").strip()
        if not username or not password:
            return await safe_edit(
                cb.message,
                "❌ <b>Seedr not configured</b>\n\n"
                "Add to your .env:\n"
                "<code>SEEDR_USERNAME=your@email.com</code>\n"
                "<code>SEEDR_PASSWORD=yourpassword</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        st = await cb.message.edit(
            "☁️ <b>Seedr Cloud Download</b>\n"
            "──────────────────────\n\n"
            "⬆️ <i>Submitting to Seedr servers…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(_seedr_download(client, st, url, uid))
        return

    # ── Hardsub ───────────────────────────────────────────────
    if mode == "hardsub":
        api_key = os.environ.get("CC_API_KEY", "").strip()
        if not api_key:
            return await safe_edit(cb.message,
                "❌ <b>CloudConvert API key not set</b>\n\n"
                "Add <code>CC_API_KEY=your_key</code> to your .env or Colab secrets.",
                parse_mode=enums.ParseMode.HTML,
            )
        from plugins.hardsub import _STATE, _clear, start_hardsub_for_url
        _clear(uid)
        tmp    = make_tmp(cfg.download_dir, uid)
        kind_h = classify(url)
        if kind_h == "direct":
            raw_name = url.split("/")[-1].split("?")[0]
            fname    = _up.unquote_plus(raw_name)[:50] or "video.mkv"
        elif kind_h in ("magnet", "torrent"):
            dn_match = re.search(r"[&?]dn=([^&]+)", url)
            fname    = _up.unquote_plus(dn_match.group(1))[:50] if dn_match else "video.mkv"
        else:
            fname = "video.mkv"

        if kind_h in ("magnet", "torrent"):
            _STATE[uid] = {
                "step": "_downloading_for_hardsub", "tmp": tmp,
                "videos": [{"path": None, "url": None, "fname": fname, "resolution": 0}],
                "sub_path": None, "sub_fname": None, "_res_idx": 0,
            }
            st = await cb.message.edit(
                f"🔥 <b>Hardsub</b>\n\n"
                f"⬇️ Downloading video via {kind_h}…\n"
                "<i>I'll ask for the subtitle once done.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
            asyncio.create_task(_hardsub_magnet_dl(st, url, uid, tmp, fname))
            return

        st = await cb.message.edit(
            f"🔥 <b>Hardsub</b>\n──────────────────────\n\n"
            f"📁 <code>{fname[:45]}</code>\n\n"
            "<i>Starting download…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        asyncio.create_task(start_hardsub_for_url(client, st, uid, url, fname))
        return

    # ── Convert ───────────────────────────────────────────────
    if mode == "convert":
        api_key = os.environ.get("CC_API_KEY", "").strip()
        if not api_key:
            return await safe_edit(cb.message,
                "❌ <b>CloudConvert API key not set</b>\n\n"
                "Add <code>CC_API_KEY=your_key</code> to your .env or Colab secrets.",
                parse_mode=enums.ParseMode.HTML,
            )
        kind_c = classify(url)
        if kind_c == "direct":
            fname = _up.unquote_plus(url.split("/")[-1].split("?")[0])[:50] or "video.mkv"
        elif kind_c in ("magnet", "torrent"):
            dn_match = re.search(r"[&?]dn=([^&]+)", url)
            fname    = _up.unquote_plus(dn_match.group(1))[:50] if dn_match else "video.mkv"
        else:
            fname = "video.mkv"

        await cb.message.edit(
            f"🔄 <b>CloudConvert — Convert</b>\n──────────────────────\n\n"
            f"🎬 <code>{fname[:45]}</code>\n\nChoose target resolution:",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎬 Original",  callback_data=f"ccv|0|{token}"),
                 InlineKeyboardButton("🔵 1080p",     callback_data=f"ccv|1080|{token}")],
                [InlineKeyboardButton("🟢 720p",      callback_data=f"ccv|720|{token}"),
                 InlineKeyboardButton("🟡 480p",      callback_data=f"ccv|480|{token}")],
                [InlineKeyboardButton("🟠 360p",      callback_data=f"ccv|360|{token}"),
                 InlineKeyboardButton("❌ Cancel",     callback_data=f"dl|cancel|{token}")],
            ]),
        )
        return

    # ── Stream selector ───────────────────────────────────────
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

    # ── Stream download ───────────────────────────────────────
    if mode == "stream_dl":
        raw = _get(token)
        if "|||" in raw:
            url2, fmt_id = raw.split("|||", 1)
        else:
            url2   = url
            fmt_id = raw or None
        _cache.pop(token, None)
        asyncio.create_task(_launch_download(client, cb.message, url2, uid, fmt_id=fmt_id))
        return

    # ── Standard download ─────────────────────────────────────
    if mode in ("video", "audio"):
        audio_only = (mode == "audio")

        # For yt-dlp sites in video mode: show resolution/format picker first
        if not audio_only and classify(url) == "ytdlp":
            st = await cb.message.edit("📡 Fetching available resolutions…")
            await _show_ytdlp_quality_picker(client, st, url, token, uid)
            return

        # Audio mode or non-ytdlp: download immediately (best quality / best audio)
        _cache.pop(token, None)
        asyncio.create_task(
            _launch_download(client, cb.message, url, uid, audio_only=audio_only)
        )


# ─────────────────────────────────────────────────────────────
# yt-dlp quality / format picker callbacks  (NEW)
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^dlq\|"))
async def dl_quality_cb(client: Client, cb: CallbackQuery):
    """
    Handles all quality/format picker interactions for yt-dlp video downloads.

    Callback data patterns:
      dlq|bucket|<token>|<bucket>   → show formats for that quality bucket
      dlq|best|<token>|             → download best quality (auto)
      dlq|audio|<token>|            → download best audio only
      dlq|fmt|<token>|<fmt_id>      → download a specific format id
      dlq|back|<token>|             → return to the quality bucket list
    """
    parts = cb.data.split("|", 3)
    if len(parts) < 4:
        return await cb.answer("Invalid data.", show_alert=True)

    _, action, token, extra = parts
    uid = cb.from_user.id
    await cb.answer()

    url = _get(token)
    if not url:
        return await safe_edit(
            cb.message,
            "❌ Session expired. Resend the link.",
            parse_mode=enums.ParseMode.HTML,
        )

    # ── Shortcuts: best quality or audio-only ─────────────────
    if action in ("best", "audio"):
        audio_only = (action == "audio")
        _cache.pop(token, None)
        _cache.pop(f"ytinfo|{token}", None)
        asyncio.create_task(
            _launch_download(client, cb.message, url, uid, audio_only=audio_only)
        )
        return

    # ── Specific format chosen → start download ───────────────
    if action == "fmt":
        fmt_id = extra or None
        _cache.pop(token, None)
        _cache.pop(f"ytinfo|{token}", None)
        asyncio.create_task(
            _launch_download(client, cb.message, url, uid, fmt_id=fmt_id)
        )
        return

    # ── Back → re-display quality buckets ─────────────────────
    if action == "back":
        st = await cb.message.edit("📡 Loading quality list…")
        await _show_ytdlp_quality_picker(client, st, url, token, uid)
        return

    # ── Bucket chosen → list individual formats ───────────────
    if action == "bucket":
        bucket = extra
        raw    = _cache.get(f"ytinfo|{token}")

        if not raw:
            # Info expired — re-fetch transparently
            st = await cb.message.edit("📡 Re-fetching formats…")
            await _show_ytdlp_quality_picker(client, st, url, token, uid)
            return

        try:
            data  = _json.loads(raw)
            fmts  = data.get("groups", {}).get(bucket, [])
            title = data.get("title", "")
        except Exception:
            fmts  = []
            title = ""

        if not fmts:
            await safe_edit(cb.message, f"❌ No formats available for {bucket}.")
            return

        lines = [
            f"📥 <b>{bucket} — select format</b>",
            "──────────────────────",
            f"<code>{title[:50]}</code>" if title else "",
            "──────────────────────",
        ]

        rows = []
        for f in fmts:
            rows.append([InlineKeyboardButton(
                f["label"][:56],
                callback_data=f"dlq|fmt|{token}|{f['fmt_id']}",
            )])

        rows.append([
            InlineKeyboardButton(
                "🔙 Back to qualities",
                callback_data=f"dlq|back|{token}|",
            ),
            InlineKeyboardButton("❌ Cancel", callback_data=f"dl|cancel|{token}"),
        ])

        await safe_edit(
            cb.message,
            "\n".join(l for l in lines if l),
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    await cb.answer("Unknown action.", show_alert=True)


# ─────────────────────────────────────────────────────────────
# Convert resolution picker
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^ccv\|"))
async def ccv_resolution_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, height_str, token = parts[:3]
    uid = cb.from_user.id
    await cb.answer()

    url = _get(token)
    if not url:
        return await safe_edit(cb.message, "❌ Session expired. Resend the link.")

    scale_height = int(height_str) if height_str.isdigit() else 0
    res_label    = f"{scale_height}p" if scale_height else "Original"
    kind         = classify(url)

    if kind == "direct":
        fname = _up.unquote_plus(url.split("/")[-1].split("?")[0])[:50] or "video.mkv"
    elif kind in ("magnet", "torrent"):
        dn_match = re.search(r"[&?]dn=([^&]+)", url)
        fname    = _up.unquote_plus(dn_match.group(1))[:50] if dn_match else "video.mkv"
    else:
        fname = "video.mkv"

    name_base   = os.path.splitext(fname)[0]
    res_tag     = f" [{scale_height}p]" if scale_height else ""
    output_name = re.sub(r'[^\w\s\-\[\]()]', '_', name_base).strip() + f"{res_tag}.mp4"

    api_key = os.environ.get("CC_API_KEY", "").strip()

    tmp_conv = make_tmp(cfg.download_dir, uid)
    await safe_edit(cb.message,
        f"⬇️ <b>Downloading for Convert…</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{fname[:40]}</code>\n"
        f"📐 → <b>{res_label}</b>\n\n"
        "<i>Download-first ensures reliable CC file upload.</i>",
        parse_mode=enums.ParseMode.HTML)

    try:
        video_path_raw = await smart_download(url, tmp_conv, user_id=uid, label=fname)
        if os.path.isdir(video_path_raw):
            resolved = largest_file(video_path_raw)
            if resolved:
                video_path_raw = resolved
        if not os.path.isfile(video_path_raw):
            raise FileNotFoundError("No output file found after download")
        video_path = video_path_raw
        fname      = os.path.basename(video_path)
        log.info("[Convert] Downloaded %s (%s) for CC job",
                 fname, human_size(os.path.getsize(video_path)))
    except Exception as exc:
        cleanup(tmp_conv)
        return await safe_edit(cb.message,
            f"❌ <b>Download failed</b>\n\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML)

    try:
        from services.cloudconvert_api import parse_api_keys, pick_best_key, submit_convert
        keys = parse_api_keys(api_key)
        if len(keys) > 1:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits left)"
        else:
            key_info = "🔑 1 API key"

        await safe_edit(cb.message,
            f"☁️ <b>Submitting Convert job…</b>\n"
            "──────────────────────\n\n"
            f"🎬 <code>{fname[:40]}</code>\n"
            f"📐 → <b>{res_label}</b>\n\n"
            "<i>Checking API keys…</i>",
            parse_mode=enums.ParseMode.HTML)

        job_id = await submit_convert(
            api_key,
            video_path=video_path,
            video_url=None,
            output_name=output_name,
            scale_height=scale_height,
        )
        mode_s = "📤 File upload (download-first)"

        await safe_edit(cb.message,
            f"✅ <b>Convert Job Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"🎬 <code>{fname[:38]}</code>\n"
            f"📐 → <b>{res_label}</b>\n"
            f"📦 → <code>{output_name[:40]}</code>\n"
            f"⚙️ {mode_s}\n{key_info}\n\n"
            "⏳ <i>CloudConvert is processing…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

    except Exception as exc:
        log.error("[Convert] Failed: %s", exc, exc_info=True)
        cleanup(tmp_conv)
        await safe_edit(cb.message,
            f"❌ <b>Convert failed</b>\n\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )


# ─────────────────────────────────────────────────────────────
# Magnet stream display + extraction
# ─────────────────────────────────────────────────────────────

async def _show_magnet_streams(
    client: Client, st, sess_tok: str,
    sd: dict, dur: int, fname: str, uid: int,
) -> None:
    from services.utils import fmt_hms

    v_streams = sd.get("video",    [])
    a_streams = sd.get("audio",    [])
    s_streams = sd.get("subtitle", [])

    lines = [
        "📡 <b>Magnet Stream Extractor</b>",
        f"📄 <code>{fname[:50]}</code>",
        f"⏱ <code>{fmt_hms(dur)}</code>",
        "──────────────────────",
    ]

    for s in v_streams:
        codec = s.get("codec_name","?").upper()
        w, h  = s.get("width",0), s.get("height",0)
        fr    = s.get("r_frame_rate","0/1")
        try:
            n2, d2 = fr.split("/")
            fps = f"{float(n2)/max(float(d2),1):.0f}fps"
        except Exception:
            fps = ""
        lines.append(f"  🎬 <code>{codec}  {w}x{h}  {fps}</code>")

    for s in a_streams:
        codec = s.get("codec_name","?").upper()
        tags  = s.get("tags",{}) or {}
        lang  = (tags.get("language","und")).lower()
        ch    = s.get("channels",0)
        ch_s  = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch") if ch else ""
        lines.append(f"  🎵 {_flag(lang)} <code>{codec}  {ch_s}</code>  {_lname(lang)}")

    for s in s_streams:
        codec = s.get("codec_name","?").upper()
        tags  = s.get("tags",{}) or {}
        lang  = (tags.get("language","und")).lower()
        lines.append(f"  💬 {_flag(lang)} <code>{codec}</code>  {_lname(lang)}")

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
    st = await cb.message.edit("📤 Extracting stream…")

    try:
        if stype in ("a_all", "s_all"):
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
                    out     = os.path.join(tmp, f"{base}_audio_{idx}_{lang}{out_ext}")
                    caption = f"🎵 <b>Audio #{idx}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"
                else:
                    out_ext = FF.subtitle_ext(codec)
                    out     = os.path.join(tmp, f"{base}_sub_{idx}_{lang}{out_ext}")
                    caption = f"💬 <b>Subtitle #{idx}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"
                try:
                    await FF.stream_op(path, out, ["-map", f"0:{idx}", "-c", "copy"])
                    await client.send_document(
                        user_id, out, caption=caption, parse_mode=enums.ParseMode.HTML,
                    )
                except Exception as exc:
                    log.warning("mse all extract idx=%d: %s", idx, exc)
            await st.delete()

        else:
            all_streams = sd.get("video",[]) + sd.get("audio",[]) + sd.get("subtitle",[])
            target = next((s for s in all_streams if str(s.get("index")) == idx_str), None)
            if not target:
                return await safe_edit(st, f"❌ Stream #{idx_str} not found.")

            codec      = (target.get("codec_name") or "").lower()
            codec_type = target.get("codec_type", "")
            tags       = target.get("tags", {}) or {}
            lang       = (tags.get("language") or "und").lower()

            if codec_type == "subtitle" or stype == "s":
                out_ext   = FF.subtitle_ext(codec)
                out       = os.path.join(tmp, f"{base}_sub_{idx_str}_{lang}{out_ext}")
                caption   = f"💬 <b>Subtitle #{idx_str}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"
                force_doc = True
            elif codec_type == "audio" or stype == "a":
                out_ext   = FF.audio_ext(codec)
                out       = os.path.join(tmp, f"{base}_audio_{idx_str}_{lang}{out_ext}")
                caption   = f"🎵 <b>Audio #{idx_str}</b>  {_flag(lang)} {_lname(lang)}\n<code>{codec.upper()}</code>"
                force_doc = False
            else:
                ext       = os.path.splitext(path)[1] or ".mp4"
                out       = os.path.join(tmp, f"{base}_video_{idx_str}{ext}")
                w         = target.get("width", 0)
                h         = target.get("height", 0)
                caption   = f"🎬 <b>Video #{idx_str}</b>  <code>{codec.upper()}  {w}x{h}</code>"
                force_doc = False

            await safe_edit(st, f"📤 Extracting stream #{idx_str}…")
            await FF.stream_op(path, out, ["-map", f"0:{idx_str}", "-c", "copy"])
            await upload_file(client, st, out, caption=caption, force_document=force_doc, user_id=user_id)

    except Exception as exc:
        log.error("mse extraction failed: %s", exc, exc_info=True)
        await safe_edit(st, f"❌ Extraction failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)


# ─────────────────────────────────────────────────────────────
# Magnet media info
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

    from services.utils import fmt_hms
    from services import ffmpeg as FF

    v_streams = sd.get("video",    [])
    a_streams = sd.get("audio",    [])
    s_streams = sd.get("subtitle", [])

    lines = [
        "📊 <b>Magnet Media Info</b>", "──────────────────────",
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
        pix   = s.get("pix_fmt","")
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

    kb_rows: list = []
    try:
        raw = await FF.get_mediainfo(path)
        from services.telegraph import post_mediainfo
        tph = await post_mediainfo(fname, raw)
        kb_rows.append([InlineKeyboardButton("📋 Full MediaInfo →", url=tph)])
    except Exception:
        pass
    kb_rows += [
        [InlineKeyboardButton("🟢 Download File", callback_data=f"dl|video|{token}"),
         InlineKeyboardButton("❌ Close",         callback_data=f"dl|cancel|{token}")],
    ]
    cleanup(tmp)
    await safe_edit(st, "\n".join(lines),
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb_rows),
    )


# ─────────────────────────────────────────────────────────────
# Upload + cleanup helper  (used by handle_torrent_file)
# ─────────────────────────────────────────────────────────────

async def _upload_and_cleanup(client, uid: int, path: str, tmp: str) -> None:
    from services.utils import all_video_files as _avf, smart_clean_filename
    from core.session import settings as _settings
    from core.config import cfg

    if os.path.isdir(path):
        all_files = _avf(path)
        if not all_files:
            resolved = largest_file(path)
            all_files = [resolved] if resolved else []
    elif os.path.isfile(path):
        all_files = [path]
    else:
        all_files = []

    if not all_files:
        try:
            await client.send_message(
                uid, "❌ <b>Upload failed</b>\n\n<code>No output files found</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        cleanup(tmp)
        return

    total_files = len(all_files)

    if total_files > 1:
        try:
            await client.send_message(
                uid,
                f"✅ <b>Download complete — {total_files} files</b>\n"
                f"📤 <i>Starting batch upload…</i>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

    s      = await _settings.get(uid)
    prefix = s.get("prefix", "").strip()
    suffix = s.get("suffix", "").strip()

    try:
        for i, fpath in enumerate(all_files, 1):
            fsize = os.path.getsize(fpath)

            if fsize > cfg.file_limit_b:
                log.warning("Skipping %s — exceeds file limit", fpath)
                try:
                    await client.send_message(
                        uid,
                        f"⚠️ <b>Skipped ({i}/{total_files})</b>\n"
                        f"<code>{os.path.basename(fpath)}</code>\n"
                        f"Size <code>{human_size(fsize)}</code> exceeds "
                        f"limit <code>{human_size(cfg.file_limit_b)}</code>",
                        parse_mode=enums.ParseMode.HTML,
                    )
                except Exception:
                    pass
                continue

            fname     = os.path.basename(fpath)
            cleaned   = smart_clean_filename(fname)
            name, ext = os.path.splitext(cleaned)
            final_name = f"{prefix}{name}{suffix}{ext}"
            if final_name != fname:
                new_path = os.path.join(os.path.dirname(fpath), final_name)
                try:
                    os.rename(fpath, new_path)
                    fpath = new_path
                except OSError as rename_err:
                    log.warning("Rename failed: %s", rename_err)

            progress_label = (
                f"📤 <b>Uploading {i}/{total_files}</b>\n"
                f"<code>{os.path.basename(fpath)}</code>"
            )
            st = await client.send_message(
                uid, progress_label,
                parse_mode=enums.ParseMode.HTML,
            )
            try:
                await upload_file(client, st, fpath, user_id=uid)
            except Exception as exc:
                log.error("Upload failed for %s: %s", fpath, exc)

            if i < total_files:
                await asyncio.sleep(2)

    finally:
        cleanup(tmp)


async def _safe_delete(msg) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# _launch_download — live download panel
# ─────────────────────────────────────────────────────────────

async def _launch_download(
    client: Client,
    panel_msg,
    url: str,
    uid: int,
    audio_only: bool = False,
    fmt_id: str | None = None,
) -> None:
    tmp  = make_tmp(cfg.download_dir, uid)
    kind = classify(url)

    if kind in ("magnet", "torrent"):
        dn_match = re.search(r"[&?]dn=([^&]+)", url)
        if dn_match:
            label = _up.unquote_plus(dn_match.group(1))[:50]
        else:
            ih_match = re.search(r"xt=urn:btih:([a-fA-F0-9]{6,}|[A-Za-z2-7]{6,})", url)
            label = (
                f"Magnet {ih_match.group(1)[:12].upper()}"
                if ih_match else "Magnet Download"
            )
        engine = "magnet"
        mode   = "magnet"
    else:
        raw    = url.split("/")[-1].split("?")[0]
        label  = _up.unquote_plus(raw)[:50] or "Download"
        engine = "ytdlp" if kind == "ytdlp" else kind
        mode   = "dl"

    try:
        await panel_msg.delete()
    except Exception:
        pass

    st = await client.send_message(
        uid,
        progress_panel(
            mode=mode, fname=label, done=0, total=0,
            engine=engine, link_label=label[:24],
        ),
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        path = await smart_download(
            url, tmp,
            audio_only=audio_only,
            fmt_id=fmt_id,
            user_id=uid,
            label=label,
            msg=st,
        )
    except Exception as exc:
        log.error("_launch_download uid=%d failed: %s", uid, exc, exc_info=True)
        cleanup(tmp)
        try:
            await st.edit(
                f"❌ <b>Download failed</b>\n\n<code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    from services.utils import all_video_files as _all_videos
    if os.path.isdir(path):
        all_files = _all_videos(path)
        if not all_files:
            resolved = largest_file(path)
            all_files = [resolved] if resolved else []
    elif os.path.isfile(path):
        all_files = [path]
    else:
        all_files = []

    if not all_files:
        log.error("_launch_download uid=%d: no output files in %s", uid, tmp)
        cleanup(tmp)
        try:
            await st.edit(
                "❌ <b>Download failed</b>\n\n<code>Output file not found</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass
        return

    total_files = len(all_files)
    if total_files > 1:
        try:
            await st.edit(
                f"✅ <b>Download complete — {total_files} files</b>\n"
                f"📤 <i>Starting batch upload…</i>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

    from core.session import settings as _settings
    s      = await _settings.get(uid)
    prefix = s.get("prefix", "").strip()
    suffix = s.get("suffix", "").strip()

    try:
        for i, fpath in enumerate(all_files, 1):
            fsize = os.path.getsize(fpath)

            if fsize > cfg.file_limit_b:
                log.warning("Skipping %s — exceeds file limit", fpath)
                try:
                    await client.send_message(
                        uid,
                        f"⚠️ <b>Skipped ({i}/{total_files})</b>\n"
                        f"<code>{os.path.basename(fpath)}</code>\n"
                        f"Size <code>{human_size(fsize)}</code> exceeds "
                        f"limit <code>{human_size(cfg.file_limit_b)}</code>",
                        parse_mode=enums.ParseMode.HTML,
                    )
                except Exception:
                    pass
                continue

            fname     = os.path.basename(fpath)
            cleaned   = smart_clean_filename(fname)
            name, ext = os.path.splitext(cleaned)
            final_name = f"{prefix}{name}{suffix}{ext}"
            if final_name != fname:
                new_path = os.path.join(os.path.dirname(fpath), final_name)
                try:
                    os.rename(fpath, new_path)
                    fpath = new_path
                except OSError as rename_err:
                    log.warning("Rename failed: %s", rename_err)

            upload_st = await client.send_message(
                uid,
                f"📤 <b>Uploading {i}/{total_files}</b>\n"
                f"<code>{os.path.basename(fpath)}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            try:
                await upload_file(client, upload_st, fpath, user_id=uid)
            except Exception as exc:
                log.error("Upload failed for %s: %s", fpath, exc)

            if i < total_files:
                await asyncio.sleep(2)

        try:
            await st.delete()
        except Exception:
            pass
    finally:
        cleanup(tmp)


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
                    [InlineKeyboardButton("🟢 Download Video", callback_data=f"dl|video|{token}"),
                     InlineKeyboardButton("🎵 Download Audio", callback_data=f"dl|audio|{token}")],
                    [InlineKeyboardButton("❌ Close",          callback_data=f"dl|cancel|{token}")],
                ]))
        except Exception as exc:
            await safe_edit(st, f"❌ Info failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        return

    # Direct URL — ffprobe on URL, zero bytes downloaded
    try:
        cmd = [
            "ffprobe", "-v", "quiet",
            "-allowed_extensions", "ALL",
            "-analyzeduration", "20000000",
            "-probesize", "50000000",
            "-print_format", "json",
            "-show_format", "-show_streams",
            url,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            try: proc.kill()
            except Exception: pass
            raise RuntimeError("ffprobe timed out (30s)")

        data    = _json.loads(out.decode(errors="replace") or "{}")
        streams = data.get("streams", [])
        fmt     = data.get("format", {})

        from pathlib import Path as _P
        fn    = _P(url.split("?")[0]).name or "file"
        fn    = fn[:50]
        total = int(fmt.get("size", 0) or 0)

        if not total:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.head(url, allow_redirects=True,
                                         timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        total = int(resp.headers.get("Content-Length", 0))
                        cd = resp.headers.get("Content-Disposition", "")
                        if "filename=" in cd:
                            fn_cd = cd.split("filename=")[-1].strip().strip('"').strip("'")
                            if fn_cd:
                                fn = fn_cd[:50]
            except Exception:
                pass

        dur_s = float(fmt.get("duration", 0) or 0)
        sd: dict = {"video": [], "audio": [], "subtitle": []}
        for s in streams:
            t = s.get("codec_type", "")
            if t in sd:
                sd[t].append(s)

        lines = [
            "📊 <b>Media Info (Direct)</b>", "──────────────────",
            f"📄 <code>{fn}</code>",
            f"💾 <code>{human_size(total) if total else '—'}</code>  "
            f"⏱ <code>{_fmt_dur(int(dur_s))}</code>",
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
            tags  = s.get("tags", {}) or {}
            lang  = (tags.get("language","und") or "und").lower()
            lines.append(f"🎵 <code>{codec} {ch_s}</code>  {_flag(lang)} {_lname(lang)}")
        for s in sd.get("subtitle", [])[:4]:
            codec = s.get("codec_name","?").upper()
            tags  = s.get("tags", {}) or {}
            lang  = (tags.get("language","und") or "und").lower()
            lines.append(f"💬 <code>{codec}</code>  {_flag(lang)} {_lname(lang)}")

        if not any(sd.get(t) for t in ("video", "audio")):
            lines.append("⚠️ <i>ffprobe could not read streams.</i>")

        kb = [
            [InlineKeyboardButton("🟢 Download", callback_data=f"dl|video|{token}"),
             InlineKeyboardButton("❌ Close",    callback_data=f"dl|cancel|{token}")],
        ]
        try:
            from services.telegraph import post_mediainfo
            mi_lines = [f"File: {fn}"]
            if total:
                mi_lines.append(f"Size: {human_size(total)}")
            if dur_s:
                mi_lines.append(f"Duration: {_fmt_dur(int(dur_s))}")
            for s in sd.get("video", []):
                mi_lines.append(
                    f"Video: {s.get('codec_name','?').upper()} "
                    f"{s.get('width',0)}x{s.get('height',0)}"
                )
            for s in sd.get("audio", []):
                tags = s.get("tags", {}) or {}
                lang = tags.get("language", "und")
                mi_lines.append(f"Audio: {s.get('codec_name','?').upper()} [{lang}]")
            tph = await post_mediainfo(fn, "\n".join(mi_lines))
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
                            [InlineKeyboardButton("🟢 Download", callback_data=f"dl|video|{token}"),
                             InlineKeyboardButton("❌ Close",    callback_data=f"dl|cancel|{token}")],
                        ]))
