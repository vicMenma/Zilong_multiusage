"""
plugins/resize.py
Resize and compress video — integrated directly into existing menus.

Entry points (no state machine needed):
  • Video menu   → 📐 Resize  /  🗜️ Compress  buttons on uploaded video
  • URL menu     → 📐 Resize  button on direct links
  • /resize      → prompts to send file or URL
  • /compress    → prompts to send file or URL or reply to video

All processing progress goes through the unified task panel.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.utils import human_size, make_tmp, cleanup, safe_edit

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────

def resize_resolution_kb(source_token: str) -> InlineKeyboardMarkup:
    """Resolution picker. source_token is opaque — passed straight to callback."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔵 1080p", callback_data=f"rsz|1080|{source_token}"),
         InlineKeyboardButton("🟢 720p",  callback_data=f"rsz|720|{source_token}")],
        [InlineKeyboardButton("🟡 480p",  callback_data=f"rsz|480|{source_token}"),
         InlineKeyboardButton("🟠 360p",  callback_data=f"rsz|360|{source_token}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"rsz|cancel|{source_token}")],
    ])


# ─────────────────────────────────────────────────────────────
# Token store  (session_key or url → stored info)
# ─────────────────────────────────────────────────────────────

import hashlib, time as _time
_token_store: dict[str, dict] = {}   # token → {"kind": "session"|"url", ...}
_TOKEN_TTL = 1800                     # 30 min


def _store_session(session_key: str) -> str:
    tok = session_key
    _token_store[tok] = {"kind": "session", "key": session_key, "ts": _time.time()}
    return tok


def _store_url(url: str, fname: str) -> str:
    tok = hashlib.md5(url.encode()).hexdigest()[:12]
    _token_store[tok] = {"kind": "url", "url": url, "fname": fname, "ts": _time.time()}
    return tok


def _get_token(tok: str) -> dict | None:
    entry = _token_store.get(tok)
    if not entry:
        return None
    if _time.time() - entry["ts"] > _TOKEN_TTL:
        _token_store.pop(tok, None)
        return None
    return entry


# ─────────────────────────────────────────────────────────────
# /resize command  (entry point for manual use)
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("resize"))
async def cmd_resize(client: Client, msg: Message):
    uid = msg.from_user.id
    await users.register(uid)

    args = msg.command[1:]
    # /resize <url>  — direct URL
    if args and args[0].startswith("http"):
        url   = args[0]
        fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
        tok   = _store_url(url, fname)
        await msg.reply(
            f"📐 <b>Resize</b>\n<code>{fname[:45]}</code>\n\nChoose resolution:",
            reply_markup=resize_resolution_kb(tok),
            parse_mode=enums.ParseMode.HTML,
        )
        return

    # /resize with no args — tell user to use the video menu or send a URL
    await msg.reply(
        "📐 <b>Resize Video</b>\n\n"
        "Two ways to use this:\n\n"
        "1️⃣  <b>Send a video file</b> → tap <b>📐 Resize</b> in the menu\n\n"
        "2️⃣  <b>Direct URL:</b>\n"
        "   <code>/resize https://example.com/video.mkv</code>\n\n"
        "<i>Resized locally with FFmpeg — no CloudConvert credits.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# /compress command
# ─────────────────────────────────────────────────────────────

_compress_waiting: dict[int, dict] = {}   # uid → pending compress info


@Client.on_message(filters.private & filters.command("compress"))
async def cmd_compress(client: Client, msg: Message):
    uid  = msg.from_user.id
    await users.register(uid)
    args = msg.command[1:]

    url       = next((a for a in args if a.startswith("http")), None)
    target_mb = next((float(a) for a in args if re.match(r"^\d+(\.\d+)?$", a)), None)

    # Reply to a video with /compress 200
    reply = msg.reply_to_message
    if reply and target_mb and not url:
        media = reply.video or reply.document
        if media:
            fname = getattr(media, "file_name", None) or "video.mkv"
            fsize = getattr(media, "file_size", 0) or 0
            tmp   = make_tmp(cfg.download_dir, uid)
            st    = await msg.reply(
                f"⬇️ Downloading <code>{fname[:40]}</code>…",
                parse_mode=enums.ParseMode.HTML,
            )
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
            await _do_compress(client, st, path, fname, tmp, target_mb, uid)
            return

    # /compress <url> <mb>
    if url and target_mb:
        fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
        tmp   = make_tmp(cfg.download_dir, uid)
        st    = await msg.reply(
            f"🗜️ <b>Compress</b> → <b>{target_mb:.0f} MB</b>\n"
            f"<code>{fname}</code>\n⬇️ Downloading…",
            parse_mode=enums.ParseMode.HTML,
        )
        try:
            from services.downloader import download_direct
            path = await download_direct(url, tmp)
        except Exception as exc:
            await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
            cleanup(tmp)
            return
        await _do_compress(client, st, path, os.path.basename(path), tmp, target_mb, uid)
        return

    # No valid args — prompt
    await msg.reply(
        "🗜️ <b>Compress Video</b>\n\n"
        "Ways to use:\n\n"
        "1️⃣  <b>Send a video file</b> → tap <b>🗜️ Compress</b> in the menu\n\n"
        "2️⃣  <b>URL + target size:</b>\n"
        "   <code>/compress https://… 85</code>\n\n"
        "3️⃣  <b>Reply to a video:</b>\n"
        "   Reply with <code>/compress 200</code>\n\n"
        "<i>2-pass FFmpeg — no CloudConvert credits.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# Entry from VIDEO MENU  (vid|resize|key  /  vid|compress_ask|key)
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^vid\|resize\|"))
async def vid_resize_cb(client: Client, cb: CallbackQuery):
    session_key = cb.data.split("|", 2)[2]
    await cb.answer()
    tok = _store_session(session_key)
    await cb.message.edit(
        "📐 <b>Resize Video</b>\n\nChoose target resolution:",
        reply_markup=resize_resolution_kb(tok),
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_callback_query(filters.regex(r"^vid\|compress_ask\|"))
async def vid_compress_ask_cb(client: Client, cb: CallbackQuery):
    session_key = cb.data.split("|", 2)[2]
    uid = cb.from_user.id
    await cb.answer()
    _compress_waiting[uid] = {"session_key": session_key, "msg_id": cb.message.id}
    await cb.message.edit(
        "🗜️ <b>Compress Video</b>\n\n"
        "Send target size in <b>MB</b>:\n"
        "<code>85</code>  or  <code>200</code>\n\n"
        "<i>2-pass FFmpeg encode — takes ~2× real-time.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# Receive target MB for compress-from-video-menu flow
@Client.on_message(
    filters.private & filters.text
    & ~filters.command(["resize", "compress", "cancel"]),
    group=1,
)
async def compress_mb_receiver(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _compress_waiting:
        return
    text = msg.text.strip()
    if not re.match(r"^\d+(\.\d+)?$", text):
        return

    target_mb   = float(text)
    info        = _compress_waiting.pop(uid)
    session_key = info["session_key"]

    from core.session import sessions
    session = sessions.get(session_key)
    if not session:
        await msg.reply("❌ Session expired. Send the video again.")
        return

    st = await msg.reply(
        f"🗜️ <b>Compress</b> → <b>{target_mb:.0f} MB</b>\n"
        f"<code>{session.fname[:40]}</code>\n⬇️ Downloading…",
        parse_mode=enums.ParseMode.HTML,
    )

    tmp = make_tmp(cfg.download_dir, uid)
    try:
        from plugins.video import _ensure
        async with session.lock:
            path = await _ensure(client, session, st)
        if not path:
            cleanup(tmp)
            return
        await _do_compress(client, st, path, session.fname, tmp, target_mb, uid)
    except Exception as exc:
        cleanup(tmp)
        await safe_edit(st, f"❌ Failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)

    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Entry from URL MENU  (dl|resize|token)
# Called from url_handler.py when user clicks 📐 Resize
# ─────────────────────────────────────────────────────────────

async def handle_url_resize(client: Client, cb: CallbackQuery, url: str, token: str) -> None:
    """Called by url_handler dl_cb when mode=='resize'."""
    fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
    tok   = _store_url(url, fname)
    await cb.message.edit(
        f"📐 <b>Resize</b>\n<code>{fname[:45]}</code>\n\nChoose resolution:",
        reply_markup=resize_resolution_kb(tok),
        parse_mode=enums.ParseMode.HTML,
    )


async def handle_url_compress(client: Client, cb: CallbackQuery, url: str, uid: int) -> None:
    """Called by url_handler dl_cb when mode=='compress_url'."""
    fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
    _compress_waiting[uid] = {"url": url, "fname": fname, "msg": cb.message}
    await cb.message.edit(
        f"🗜️ <b>Compress</b>\n<code>{fname[:45]}</code>\n\n"
        "Send target size in <b>MB</b>:\n<code>85</code>",
        parse_mode=enums.ParseMode.HTML,
    )


# Receive target MB for compress-from-url flow
@Client.on_message(
    filters.private & filters.text
    & ~filters.command(["resize", "compress", "cancel"]),
    group=1,
)
async def compress_url_mb_receiver(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _compress_waiting:
        return
    info = _compress_waiting.get(uid)
    if not info or "url" not in info or "session_key" in info:
        return
    text = msg.text.strip()
    if not re.match(r"^\d+(\.\d+)?$", text):
        return

    target_mb = float(text)
    info      = _compress_waiting.pop(uid)
    url       = info["url"]
    fname     = info["fname"]
    st_msg    = info.get("msg")

    tmp = make_tmp(cfg.download_dir, uid)
    st  = await msg.reply(
        f"🗜️ <b>Compress</b> → <b>{target_mb:.0f} MB</b>\n"
        f"<code>{fname}</code>\n⬇️ Downloading…",
        parse_mode=enums.ParseMode.HTML,
    )
    try:
        if st_msg:
            try: await st_msg.delete()
            except Exception: pass
        from services.downloader import download_direct
        path = await download_direct(url, tmp)
        await _do_compress(client, st, path, os.path.basename(path), tmp, target_mb, uid)
    except Exception as exc:
        cleanup(tmp)
        await safe_edit(st, f"❌ Failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Resize resolution callback  rsz|<height>|<token>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^rsz\|"))
async def rsz_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3:
        return await cb.answer("Invalid.", show_alert=True)
    _, height_s, tok = parts
    uid = cb.from_user.id
    await cb.answer()

    if height_s == "cancel":
        _token_store.pop(tok, None)
        await cb.message.delete()
        return

    entry = _get_token(tok)
    if not entry:
        return await safe_edit(cb.message, "❌ Session expired. Try again.",
                               parse_mode=enums.ParseMode.HTML)

    height = int(height_s)
    await safe_edit(
        cb.message,
        f"📐 <b>Resizing to {height}p…</b>\n"
        f"<i>Downloading & encoding with FFmpeg…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    tmp = make_tmp(cfg.download_dir, uid)
    try:
        if entry["kind"] == "url":
            url   = entry["url"]
            fname = entry["fname"]
            from services.downloader import download_direct
            await safe_edit(cb.message,
                f"📐 <b>Resize → {height}p</b>\n"
                f"<code>{fname[:40]}</code>\n⬇️ Downloading…",
                parse_mode=enums.ParseMode.HTML)
            path = await download_direct(url, tmp)
            fname = os.path.basename(path)

        else:  # session
            from core.session import sessions
            from plugins.video import _ensure
            session = sessions.get(entry["key"])
            if not session:
                await safe_edit(cb.message, "❌ Session expired. Send the video again.")
                cleanup(tmp)
                return
            fname = session.fname
            async with session.lock:
                path = await _ensure(client, session, cb.message)
            if not path:
                cleanup(tmp)
                return

        await _do_resize(client, cb.message, path, fname, tmp, height, uid)

    except Exception as exc:
        log.error("[Resize] %s", exc, exc_info=True)
        cleanup(tmp)
        await safe_edit(cb.message,
                        f"❌ <b>Resize failed</b>\n<code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)


# ─────────────────────────────────────────────────────────────
# Core processing — routes through unified task panel
# ─────────────────────────────────────────────────────────────

async def _do_resize(
    client, msg, path: str, fname: str, tmp: str, height: int, uid: int
) -> None:
    """Run FFmpeg resize and upload — all progress via task panel."""
    from services.task_runner import tracker, TaskRecord, runner
    from services import ffmpeg as FF
    from services.uploader import upload_file

    name_base = os.path.splitext(fname)[0]
    out_fname = f"{name_base}_{height}p.mp4"
    out       = os.path.join(tmp, out_fname)

    # Register as a processing task so it shows in the panel
    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=uid,
        label=f"Resize → {height}p",
        mode="proc", engine="ffmpeg",
        fname=fname, state=f"📐 Resizing to {height}p…",
    )
    await tracker.register(record)

    try:
        await FF.resize_video(path, out, height)
        record.update(state="✅ Done")
        runner._wake_panel(uid, immediate=True)
    except Exception as exc:
        record.update(state="❌ Failed")
        runner._wake_panel(uid, immediate=True)
        raise exc

    fsize = os.path.getsize(out)
    log.info("[Resize] Done: %s → %dp  (%s)", fname, height, human_size(fsize))

    # Delete the "resizing…" status message — panel takes over
    try:
        await msg.delete()
    except Exception:
        pass

    # Fresh upload status message
    st = await client.send_message(
        uid,
        f"📐 <b>Resize done!</b>  {height}p\n"
        f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\n"
        f"⬆️ Uploading…",
        parse_mode=enums.ParseMode.HTML,
    )
    await upload_file(client, st, out)
    cleanup(tmp)


async def _do_compress(
    client, msg, path: str, fname: str, tmp: str, target_mb: float, uid: int
) -> None:
    """Run 2-pass FFmpeg compress and upload — all progress via task panel."""
    from services.task_runner import tracker, TaskRecord, runner
    from services import ffmpeg as FF
    from services.uploader import upload_file

    name_base = os.path.splitext(fname)[0]
    out_fname = f"{name_base}_{int(target_mb)}MB.mp4"
    out       = os.path.join(tmp, out_fname)

    # Register as a processing task
    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=uid,
        label=f"Compress → {target_mb:.0f} MB",
        mode="proc", engine="ffmpeg",
        fname=fname, state=f"🗜️ Compressing to {target_mb:.0f} MB…",
    )
    await tracker.register(record)

    try:
        record.update(state="🗜️ Pass 1/2…")
        runner._wake_panel(uid)
        await FF.compress_to_size(path, out, target_mb)
        record.update(state="✅ Done")
        runner._wake_panel(uid, immediate=True)
    except Exception as exc:
        record.update(state="❌ Failed")
        runner._wake_panel(uid, immediate=True)
        raise exc

    fsize = os.path.getsize(out)
    log.info("[Compress] Done: %s → %.0f MB actual %s",
             fname, target_mb, human_size(fsize))

    try:
        await msg.delete()
    except Exception:
        pass

    st = await client.send_message(
        uid,
        f"🗜️ <b>Compress done!</b>  {human_size(fsize)}\n"
        f"<code>{out_fname}</code>\n"
        f"⬆️ Uploading…",
        parse_mode=enums.ParseMode.HTML,
    )
    await upload_file(client, st, out)
    cleanup(tmp)
