"""
plugins/hardsub.py
CloudConvert-powered hardsubbing — burn subtitles into video via
CloudConvert's FFmpeg engine (much faster than Colab's free GPU).

Usage:
  /hardsub  →  bot asks for video (file or URL) → then subtitle file or URL
  CloudConvert does the heavy lifting → webhook auto-uploads result

Supports:
  Video input:
  - Telegram video file
  - Direct URL (CloudConvert imports directly — fastest)
  - Magnet / torrent / yt-dlp / gdrive / mediafire

  Subtitle input:
  - Telegram file: .ass .srt .vtt .ssa .sub .txt
  - URL to subtitle: https://example.com/subs/ep01.ass
  - URL to .txt subtitle file
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import urllib.parse as _urlparse

import aiohttp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.utils import human_size, make_tmp, cleanup, safe_edit

log = logging.getLogger(__name__)

# ── Accepted subtitle extensions ──────────────────────────────
_SUB_EXTS = {".ass", ".srt", ".vtt", ".ssa", ".sub", ".txt"}

# ── Per-user state machine ────────────────────────────────────
_STATE: dict[int, dict] = {}


def _user_state(uid: int) -> dict | None:
    return _STATE.get(uid)


def _clear(uid: int) -> None:
    s = _STATE.pop(uid, None)
    if s and s.get("tmp"):
        cleanup(s["tmp"])


# ─────────────────────────────────────────────────────────────
# Shared: submit hardsub to CloudConvert
# ─────────────────────────────────────────────────────────────

async def _submit_to_cloudconvert(st, state: dict, sub_fname: str, uid: int) -> None:
    """Shared submission logic — called after subtitle is ready."""
    video_fname = state.get("video_fname", "video.mkv")
    name_base = os.path.splitext(video_fname)[0]
    output_name = re.sub(r'[^\w\s\-\[\]()]', '_', name_base).strip() + " [VOSTFR].mp4"

    await safe_edit(st,
        "☁️ <b>Submitting to CloudConvert…</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{video_fname[:45]}</code>\n"
        f"💬 <code>{sub_fname[:45]}</code>\n"
        f"📤 → <code>{output_name[:45]}</code>\n\n"
        "<i>CloudConvert will burn the subtitles and the webhook\n"
        "will auto-upload the result when ready.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        api_key = os.environ.get("CC_API_KEY", "").strip()
        from services.cloudconvert_api import submit_hardsub

        job_id = await submit_hardsub(
            api_key,
            video_path=state.get("video_path"),
            video_url=state.get("video_url"),
            subtitle_path=state["sub_path"],
            output_name=output_name,
        )

        if state.get("video_url"):
            mode_s = "☁️ URL import (no upload needed)"
        else:
            vsize = os.path.getsize(state["video_path"]) if state.get("video_path") else 0
            mode_s = f"📤 Uploaded {human_size(vsize)}"

        await safe_edit(st,
            "✅ <b>Hardsub Job Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"🎬 <code>{video_fname[:40]}</code>\n"
            f"💬 <code>{sub_fname[:40]}</code>\n"
            f"📦 → <code>{output_name[:40]}</code>\n"
            f"⚙️ {mode_s}\n\n"
            "⏳ <i>CloudConvert is processing…\n"
            "The webhook will auto-upload the result to this chat.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        log.info("[Hardsub] Job %s submitted for uid=%d: %s + %s → %s",
                 job_id, uid, video_fname, sub_fname, output_name)

    except Exception as exc:
        log.error("[Hardsub] Submit failed: %s", exc, exc_info=True)
        await safe_edit(st,
            f"❌ <b>CloudConvert submission failed</b>\n\n"
            f"<code>{str(exc)[:200]}</code>\n\n"
            "<i>Check your CC_API_KEY and try again.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    finally:
        _clear(uid)


# ─────────────────────────────────────────────────────────────
# /hardsub command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("hardsub"))
async def cmd_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    await users.register(uid, msg.from_user.first_name or "")

    api_key = os.environ.get("CC_API_KEY", "").strip()
    if not api_key:
        return await msg.reply(
            "❌ <b>CloudConvert API key not set</b>\n\n"
            "Add <code>CC_API_KEY=your_key</code> to your .env or Colab secrets.\n\n"
            "Get a key at: cloudconvert.com → Dashboard → API → API Keys",
            parse_mode=enums.ParseMode.HTML,
        )

    _clear(uid)

    tmp = make_tmp(cfg.download_dir, uid)
    _STATE[uid] = {
        "step": "waiting_video",
        "tmp": tmp,
        "video_path": None,
        "video_url": None,
        "sub_path": None,
    }

    await msg.reply(
        "🔥 <b>CloudConvert Hardsub</b>\n"
        "──────────────────────\n\n"
        "Send me the <b>video</b>:\n"
        "• A <b>video file</b> (upload from Telegram)\n"
        "• A <b>direct URL</b> (HTTP link to .mkv/.mp4)\n"
        "• A <b>magnet link</b> (downloaded via aria2 first)\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.private & filters.command("cancel"), group=4)
async def cmd_cancel_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid in _STATE:
        _clear(uid)
        await msg.reply("❌ Hardsub cancelled.")
        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 1: Receive video (file or URL)
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & (filters.video | filters.document),
    group=1,
)
async def hardsub_video_file(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_video":
        return

    media = msg.video or msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "video.mkv"
    ext = os.path.splitext(fname)[1].lower()

    _VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".m2ts", ".wmv", ".m4v"}
    if ext not in _VIDEO_EXTS and not msg.video:
        return

    fsize = getattr(media, "file_size", 0) or 0
    st = await msg.reply(
        f"⬇️ Downloading <code>{fname[:40]}</code>…",
        parse_mode=enums.ParseMode.HTML,
    )

    tmp = state["tmp"]
    try:
        from services.tg_download import tg_download
        path = await tg_download(
            client, media.file_id,
            os.path.join(tmp, fname), st,
            fname=fname, fsize=fsize, user_id=uid,
        )
        state["video_path"] = path
        state["video_fname"] = os.path.basename(path)
        state["step"] = "waiting_subtitle"

        await safe_edit(st,
            f"✅ Video received: <code>{fname[:40]}</code>\n"
            f"💾 <code>{human_size(fsize)}</code>\n\n"
            "Now send the <b>subtitle</b>:\n"
            "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
            "• A <b>URL</b> to a subtitle file",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid)

    msg.stop_propagation()


# Handle URL/magnet input (for both video step and subtitle step)
@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start", "help", "settings", "info", "status", "log", "restart",
         "broadcast", "admin", "ban_user", "unban_user", "banned_list",
         "cancel", "show_thumb", "del_thumb", "json_formatter", "bulk_url",
         "hardsub", "stream", "forward", "createarchive", "archiveddone",
         "mergedone"]
    ),
    group=1,
)
async def hardsub_url_handler(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state:
        return
    if state["step"] not in ("waiting_video", "waiting_subtitle"):
        return

    text = msg.text.strip()

    url_re = re.compile(r"^(https?://\S+|magnet:\?\S+)$", re.I)
    if not url_re.match(text):
        return

    # ── Subtitle URL ──────────────────────────────────────────
    if state["step"] == "waiting_subtitle":
        await _handle_subtitle_url(msg, state, text, uid)
        msg.stop_propagation()
        return

    # ── Video URL ─────────────────────────────────────────────
    from services.downloader import classify
    kind = classify(text)

    if kind == "direct":
        state["video_url"] = text
        raw_name = text.split("/")[-1].split("?")[0]
        state["video_fname"] = _urlparse.unquote_plus(raw_name)[:50] or "video.mkv"
        state["step"] = "waiting_subtitle"

        await msg.reply(
            f"✅ Video URL received\n"
            f"<code>{text[:60]}</code>\n\n"
            "☁️ <i>CloudConvert will fetch this directly — no local download needed!</i>\n\n"
            "Now send the <b>subtitle</b>:\n"
            "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
            "• A <b>URL</b> to a subtitle file",
            parse_mode=enums.ParseMode.HTML,
        )
        msg.stop_propagation()

    elif kind in ("magnet", "torrent", "ytdlp", "gdrive", "mediafire"):
        st = await msg.reply(
            f"⬇️ Downloading video via {kind}…\n"
            "<i>This may take a while for magnets.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        tmp = state["tmp"]
        try:
            from services.downloader import smart_download
            from services.utils import largest_file
            path = await smart_download(
                text, tmp,
                user_id=uid,
                label=f"Hardsub DL",
            )
            if os.path.isdir(path):
                resolved = largest_file(path)
                if resolved:
                    path = resolved

            if not os.path.isfile(path):
                raise FileNotFoundError("No output file found")

            state["video_path"] = path
            state["video_fname"] = os.path.basename(path)
            state["step"] = "waiting_subtitle"

            fsize = os.path.getsize(path)
            await safe_edit(st,
                f"✅ Video downloaded: <code>{os.path.basename(path)[:40]}</code>\n"
                f"💾 <code>{human_size(fsize)}</code>\n\n"
                "Now send the <b>subtitle</b>:\n"
                "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
                "• A <b>URL</b> to a subtitle file",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as exc:
            await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
            _clear(uid)

        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2a: Receive subtitle FILE (.ass .srt .vtt .ssa .sub .txt)
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.document,
    group=0,
)
async def hardsub_subtitle_file(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_subtitle":
        return

    media = msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "subtitle.ass"
    ext = os.path.splitext(fname)[1].lower()

    if ext not in _SUB_EXTS:
        return  # Not a subtitle — let other handlers take it

    tmp = state["tmp"]
    st = await msg.reply("⬇️ Downloading subtitle…")

    try:
        sub_path = await client.download_media(
            media, file_name=os.path.join(tmp, fname)
        )
        state["sub_path"] = sub_path
        state["sub_fname"] = os.path.basename(sub_path)
    except Exception as exc:
        await safe_edit(st, f"❌ Subtitle download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid)
        msg.stop_propagation()
        return

    # Submit to CloudConvert
    await _submit_to_cloudconvert(st, state, fname, uid)

    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2b: Receive subtitle URL (any URL during waiting_subtitle)
# ─────────────────────────────────────────────────────────────

async def _handle_subtitle_url(msg: Message, state: dict, url: str, uid: int) -> None:
    """Download subtitle from URL and submit to CloudConvert."""
    tmp = state["tmp"]

    # Extract filename from URL
    parsed_path = _urlparse.urlparse(url).path
    raw_fname = os.path.basename(parsed_path)
    fname = _urlparse.unquote_plus(raw_fname) if raw_fname else "subtitle.ass"

    # If no extension or not a sub extension, default to .ass
    ext = os.path.splitext(fname)[1].lower()
    if ext not in _SUB_EXTS:
        fname = fname + ".ass" if fname else "subtitle.ass"

    # Clean filename for filesystem
    fname = re.sub(r'[\\/:*?"<>|]', "_", fname)

    st = await msg.reply(
        f"⬇️ Downloading subtitle from URL…\n"
        f"<code>{url[:60]}</code>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        sub_path = os.path.join(tmp, fname)
        headers = {"User-Agent": "Mozilla/5.0"}

        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, allow_redirects=True) as resp:
                resp.raise_for_status()

                # Try to get better filename from Content-Disposition header
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    cd_fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
                    if cd_fname:
                        cd_fname = _urlparse.unquote_plus(cd_fname)
                        cd_ext = os.path.splitext(cd_fname)[1].lower()
                        if cd_ext in _SUB_EXTS:
                            fname = re.sub(r'[\\/:*?"<>|]', "_", cd_fname)
                            sub_path = os.path.join(tmp, fname)

                content = await resp.read()

        # Sanity check — subtitle files shouldn't be >10MB
        if len(content) > 10_000_000:
            await safe_edit(st, "❌ File too large — doesn't look like a subtitle file.",
                            parse_mode=enums.ParseMode.HTML)
            _clear(uid)
            return

        with open(sub_path, "wb") as f:
            f.write(content)

        fsize = os.path.getsize(sub_path)
        state["sub_path"] = sub_path
        state["sub_fname"] = fname

        log.info("[Hardsub] Subtitle downloaded from URL: %s (%s)",
                 fname, human_size(fsize))

    except Exception as exc:
        log.error("[Hardsub] Subtitle URL download failed: %s", exc)
        await safe_edit(st,
            f"❌ Subtitle download failed:\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        _clear(uid)
        return

    # Submit to CloudConvert
    await _submit_to_cloudconvert(st, state, fname, uid)
