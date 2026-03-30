"""
plugins/resize.py
Local FFmpeg resize and compression commands.

/resize  — downscale video to 1080p / 720p / 480p / 360p for free
           (no CloudConvert credits used)
/compress — re-encode to hit a target file size in MB

Both work on:
  • A previously uploaded video (via the video menu → Resize / Compress)
  • A direct URL (paste after the command)
"""
from __future__ import annotations

import logging
import os
import re

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import sessions, users
from services.utils import human_size, make_tmp, cleanup, safe_edit

log = logging.getLogger(__name__)

# ── State dicts ───────────────────────────────────────────────
_RESIZE_STATE:   dict[int, dict] = {}   # uid → {path, tmp, fname}
_COMPRESS_STATE: dict[int, dict] = {}   # uid → {path, tmp, fname}


# ─────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────

def _resize_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔵 1080p", callback_data=f"rsz|1080|{uid}"),
         InlineKeyboardButton("🟢 720p",  callback_data=f"rsz|720|{uid}")],
        [InlineKeyboardButton("🟡 480p",  callback_data=f"rsz|480|{uid}"),
         InlineKeyboardButton("🟠 360p",  callback_data=f"rsz|360|{uid}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"rsz|cancel|{uid}")],
    ])


# ─────────────────────────────────────────────────────────────
# /resize command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("resize"))
async def cmd_resize(client: Client, msg: Message):
    uid = msg.from_user.id
    await users.register(uid)

    # If a URL is passed directly: /resize https://...
    args = msg.command[1:]
    if args and args[0].startswith("http"):
        url   = args[0]
        fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
        tmp   = make_tmp(cfg.download_dir, uid)
        _RESIZE_STATE[uid] = {"path": None, "url": url, "fname": fname, "tmp": tmp}
        await msg.reply(
            f"🔄 <b>Resize — URL mode</b>\n\n"
            f"<code>{fname}</code>\n\n"
            f"Choose target resolution:",
            reply_markup=_resize_kb(uid),
            parse_mode=enums.ParseMode.HTML,
        )
        return

    # No URL — prompt to send a file
    _RESIZE_STATE[uid] = {"path": None, "url": None, "fname": "", "tmp": None}
    await msg.reply(
        "🔄 <b>Resize Video</b>\n\n"
        "Send me a <b>video file</b> or paste a <b>direct URL</b>.\n\n"
        "<i>Resized locally with FFmpeg — no CloudConvert credits.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# /compress command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("compress"))
async def cmd_compress(client: Client, msg: Message):
    uid  = msg.from_user.id
    await users.register(uid)
    args = msg.command[1:]

    # Usage: /compress 50  or  /compress https://... 50
    url        = None
    target_mb  = None

    for a in args:
        if a.startswith("http"):
            url = a
        elif re.match(r"^\d+(\.\d+)?$", a):
            target_mb = float(a)

    if url and target_mb:
        fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
        tmp   = make_tmp(cfg.download_dir, uid)
        _COMPRESS_STATE[uid] = {
            "path": None, "url": url, "fname": fname,
            "tmp": tmp, "target_mb": target_mb,
        }
        st = await msg.reply(
            f"🗜️ <b>Compress</b>  →  <b>{target_mb:.0f} MB</b>\n\n"
            f"<code>{fname}</code>\n\n"
            f"⬇️ <i>Downloading…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        await _run_compress(client, st, uid)
        return

    _COMPRESS_STATE[uid] = {"path": None, "url": None, "fname": "", "tmp": None, "target_mb": None}
    await msg.reply(
        "🗜️ <b>Compress Video</b>\n\n"
        "Usage:\n"
        "  <code>/compress 50</code>  — send a file then specify size\n"
        "  <code>/compress https://… 50</code>  — URL + target MB\n\n"
        "<i>Send a video file to start, then reply with target MB.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# Receive video file for resize / compress flow
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & (filters.video | filters.document),
    group=11,
)
async def resize_compress_file_receiver(client: Client, msg: Message):
    uid   = msg.from_user.id
    in_rs = uid in _RESIZE_STATE   and _RESIZE_STATE[uid].get("path")   is None and _RESIZE_STATE[uid].get("url") is None
    in_cp = uid in _COMPRESS_STATE and _COMPRESS_STATE[uid].get("path") is None and _COMPRESS_STATE[uid].get("url") is None

    if not in_rs and not in_cp:
        return

    media = msg.video or msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "video.mkv"
    fsize = getattr(media, "file_size", 0) or 0
    tmp   = make_tmp(cfg.download_dir, uid)
    st    = await msg.reply(f"⬇️ Downloading <code>{fname[:40]}</code>…",
                            parse_mode=enums.ParseMode.HTML)

    try:
        from services.tg_download import tg_download
        path = await tg_download(client, media.file_id,
                                 os.path.join(tmp, fname), st,
                                 fname=fname, fsize=fsize, user_id=uid)
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        cleanup(tmp)
        return

    if in_rs:
        _RESIZE_STATE[uid].update(path=path, fname=fname, tmp=tmp)
        await safe_edit(
            st,
            f"✅ <b>{fname[:40]}</b> received.\n\nChoose target resolution:",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=_resize_kb(uid),
        )
    else:
        _COMPRESS_STATE[uid].update(path=path, fname=fname, tmp=tmp)
        await safe_edit(
            st,
            f"✅ <b>{fname[:40]}</b> received.\n\n"
            f"Reply with target size in <b>MB</b>:\n<code>50</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    msg.stop_propagation()


# Receive target MB text for compress flow
@Client.on_message(
    filters.private & filters.text
    & ~filters.command(["resize", "compress", "cancel"]),
    group=11,
)
async def compress_mb_receiver(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _COMPRESS_STATE.get(uid)
    if not state or not state.get("path") or state.get("target_mb"):
        return

    text = msg.text.strip()
    if not re.match(r"^\d+(\.\d+)?$", text):
        return

    state["target_mb"] = float(text)
    st = await msg.reply(
        f"🗜️ <b>Compressing to {state['target_mb']:.0f} MB…</b>",
        parse_mode=enums.ParseMode.HTML,
    )
    await _run_compress(client, st, uid)
    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Resize callback
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^rsz\|"))
async def rsz_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid.", show_alert=True)
    _, height_s, uid_s = parts
    uid = int(uid_s)
    await cb.answer()

    if height_s == "cancel":
        state = _RESIZE_STATE.pop(uid, None)
        if state and state.get("tmp"):
            cleanup(state["tmp"])
        await cb.message.delete()
        return

    state = _RESIZE_STATE.pop(uid, None)
    if not state:
        return await safe_edit(cb.message, "❌ Session expired. Use /resize again.")

    height = int(height_s)
    path   = state.get("path")
    url    = state.get("url")
    fname  = state.get("fname", "video.mkv")
    tmp    = state.get("tmp") or make_tmp(cfg.download_dir, uid)

    await safe_edit(
        cb.message,
        f"🔄 <b>Resizing to {height}p…</b>\n<code>{fname[:40]}</code>\n\n"
        f"<i>This may take a few minutes.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        # Download if URL-mode
        if url and not path:
            from services.downloader import download_direct
            path = await download_direct(url, tmp)

        name_base   = os.path.splitext(os.path.basename(path))[0]
        out_fname   = f"{name_base}_{height}p.mp4"
        out         = os.path.join(tmp, out_fname)

        from services import ffmpeg as FF
        await FF.resize_video(path, out, height)

        fsize = os.path.getsize(out)
        await safe_edit(
            cb.message,
            f"✅ <b>Resized to {height}p</b>\n"
            f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\n\n"
            f"⬆️ <i>Uploading…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        from services.uploader import upload_file
        await upload_file(client, cb.message, out)
    except Exception as exc:
        log.error("[Resize] %s", exc, exc_info=True)
        await safe_edit(cb.message,
                        f"❌ <b>Resize failed</b>\n<code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
    finally:
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Compress runner
# ─────────────────────────────────────────────────────────────

async def _run_compress(client, st, uid: int) -> None:
    state = _COMPRESS_STATE.pop(uid, None)
    if not state:
        return await safe_edit(st, "❌ Session expired.")

    path      = state.get("path")
    url       = state.get("url")
    fname     = state.get("fname", "video.mkv")
    tmp       = state.get("tmp") or make_tmp(cfg.download_dir, uid)
    target_mb = state.get("target_mb", 50.0)

    try:
        if url and not path:
            from services.downloader import download_direct
            await safe_edit(st, f"⬇️ Downloading…\n<code>{fname[:40]}</code>",
                            parse_mode=enums.ParseMode.HTML)
            path = await download_direct(url, tmp)

        name_base = os.path.splitext(os.path.basename(path))[0]
        out_fname = f"{name_base}_{int(target_mb)}MB.mp4"
        out       = os.path.join(tmp, out_fname)

        await safe_edit(
            st,
            f"🗜️ <b>Compressing to {target_mb:.0f} MB…</b>\n"
            f"<code>{fname[:40]}</code>\n\n"
            f"<i>2-pass encoding — takes 2× real-time.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        from services import ffmpeg as FF
        await FF.compress_to_size(path, out, target_mb)

        fsize = os.path.getsize(out)
        await safe_edit(
            st,
            f"✅ <b>Compressed</b>\n"
            f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\n\n"
            f"⬆️ <i>Uploading…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        from services.uploader import upload_file
        await upload_file(client, st, out)
    except Exception as exc:
        log.error("[Compress] %s", exc, exc_info=True)
        await safe_edit(st, f"❌ <b>Compress failed</b>\n<code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
    finally:
        cleanup(tmp)
