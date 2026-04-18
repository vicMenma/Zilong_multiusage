"""
plugins/resize.py
Resize and compress video — unified, all entry paths work.

FIXED:
  - Removed all runner._wake_panel() calls (method no longer exists)
  - _do_resize / _do_compress no longer crash mid-execution
  - All four entry paths now work for both operations:
      1. /resize url              → direct URL mode
      2. /resize (no args)        → interactive: send file or URL
      3. URL menu → Resize button → resolution picker
      4. Video menu → Resize button → resize session file
  - Same for compress (all 4 paths)
  - vid_resize_cb and vid_compress_ask_cb keep stop_propagation() to
    prevent video_cb from double-firing

FIX H-05 (audit v3): _interactive_mode now stores (mode_str, timestamp) tuples
  with a 10-minute TTL. Previously, if a user started /resize or /compress
  interactively and then abandoned the flow, the state persisted forever.
  _compress_waiting is cleaned alongside _interactive_mode on eviction.
"""
from __future__ import annotations

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
from services.utils import human_size, make_tmp, cleanup, safe_edit

import shutil as _shutil
_FFMPEG_OK = bool(_shutil.which("ffmpeg"))

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Platform helpers
# ─────────────────────────────────────────────────────────────

def _has_cc()    -> bool: return bool(os.environ.get("CC_API_KEY", "").strip())
def _has_fc()    -> bool: return bool(os.environ.get("FC_API_KEY", "").strip())
def _has_local() -> bool: return _FFMPEG_OK


def _default_platform() -> str:
    if _has_fc():    return "fc"
    if _has_cc():    return "cc"
    if _has_local(): return "local"
    return "local"


def _platform_label(p: str) -> str:
    return {"cc": "☁️ CloudConvert",
            "fc": "🆓 FreeConvert",
            "local": "🖥 Local FFmpeg"}.get(p, p)


def _platform_row(tok: str, current: str, kind: str) -> list:
    """Build platform toggle row. kind='rzp' or 'czp' for resize/compress."""
    row = []
    if _has_cc():
        tick = " ✓" if current == "cc" else ""
        row.append(InlineKeyboardButton(f"☁️ CC{tick}", callback_data=f"{kind}|cc|{tok}"))
    if _has_fc():
        tick = " ✓" if current == "fc" else ""
        row.append(InlineKeyboardButton(f"🆓 FC{tick}", callback_data=f"{kind}|fc|{tok}"))
    if _has_local():
        tick = " ✓" if current == "local" else ""
        row.append(InlineKeyboardButton(f"🖥 Local{tick}", callback_data=f"{kind}|local|{tok}"))
    return row


# ─────────────────────────────────────────────────────────────
# Keyboard
# ─────────────────────────────────────────────────────────────

def resize_resolution_kb(source_token: str, platform: str | None = None) -> InlineKeyboardMarkup:
    platform = platform or _default_platform()
    # Update stored platform on keyboard build (keeps token in sync with current selection)
    entry = _token_store.get(source_token)
    if entry:
        entry["platform"] = platform

    rows = []
    plat_row = _platform_row(source_token, platform, "rzp")
    if len(plat_row) >= 2:
        rows.append(plat_row)
    rows += [
        [InlineKeyboardButton("🔵 1080p", callback_data=f"rsz|1080|{source_token}"),
         InlineKeyboardButton("🟢 720p",  callback_data=f"rsz|720|{source_token}")],
        [InlineKeyboardButton("🟡 480p",  callback_data=f"rsz|480|{source_token}"),
         InlineKeyboardButton("🟠 360p",  callback_data=f"rsz|360|{source_token}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"rsz|cancel|{source_token}")],
    ]
    return InlineKeyboardMarkup(rows)


def compress_platform_kb(tok: str, platform: str | None = None) -> InlineKeyboardMarkup:
    platform = platform or _default_platform()
    rows = []
    plat_row = _platform_row(tok, platform, "czp")
    if plat_row:
        rows.append(plat_row)
    rows.append([InlineKeyboardButton("🚀 Go", callback_data=f"czp|go|{tok}"),
                 InlineKeyboardButton("❌ Cancel", callback_data=f"czp|cancel|{tok}")])
    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────
# Token store  (maps short tokens to source descriptors)
# ─────────────────────────────────────────────────────────────

_token_store: dict[str, dict] = {}
_TOKEN_TTL = 1800   # 30 min


def _store_session(session_key: str) -> str:
    tok = session_key
    _token_store[tok] = {
        "kind": "session", "key": session_key, "ts": time.time(),
        "platform": _default_platform(),
    }
    return tok


def _store_url(url: str, fname: str) -> str:
    tok = hashlib.md5(url.encode()).hexdigest()[:12]
    _token_store[tok] = {
        "kind": "url", "url": url, "fname": fname, "ts": time.time(),
        "platform": _default_platform(),
    }
    return tok


def _store_tg_file(file_id: str, fname: str, fsize: int) -> str:
    tok = hashlib.md5(file_id.encode()).hexdigest()[:12]
    _token_store[tok] = {
        "kind": "tg_file", "file_id": file_id,
        "fname": fname, "fsize": fsize, "ts": time.time(),
        "platform": _default_platform(),
    }
    return tok


def _get_token(tok: str) -> dict | None:
    entry = _token_store.get(tok)
    if not entry:
        return None
    if time.time() - entry["ts"] > _TOKEN_TTL:
        _token_store.pop(tok, None)
        return None
    return entry


# ─────────────────────────────────────────────────────────────
# Interactive state
# FIX H-05 (audit v3): tuples with timestamps + TTL eviction
# ─────────────────────────────────────────────────────────────

_interactive_mode:  dict[int, tuple[str, float]]  = {}  # uid → ("resize"|"compress", ts)
_compress_waiting:  dict[int, dict] = {}   # uid → source info dict
_RESIZE_STATE_TTL = 600  # 10 min


def _evict_resize_states() -> None:
    """FIX H-05 (audit v3): clean stale interactive states."""
    now = time.time()
    dead = [uid for uid, (_, ts) in _interactive_mode.items()
            if now - ts > _RESIZE_STATE_TTL]
    for uid in dead:
        _interactive_mode.pop(uid, None)
        _compress_waiting.pop(uid, None)


# ─────────────────────────────────────────────────────────────
# /resize command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("resize"))
async def cmd_resize(client: Client, msg: Message):
    uid  = msg.from_user.id
    await users.register(uid)
    args = msg.command[1:]

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

    _evict_resize_states()  # FIX H-05
    _interactive_mode[uid] = ("resize", time.time())
    _compress_waiting.pop(uid, None)
    await msg.reply(
        "📐 <b>Resize Video</b>\n\n"
        "Send me a <b>video file</b> or a <b>direct URL</b>.\n\n"
        "<i>Send /cancel to abort.</i>",
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

    url       = next((a for a in args if a.startswith("http")), None)
    target_mb = next((float(a) for a in args if re.match(r"^\d+(\.\d+)?$", a)), None)

    async def _stage_cmd(st, path, fname, tmp):
        tok = hashlib.md5(f"{uid}_{time.time()}".encode()).hexdigest()[:12]
        _cz_pending[tok] = {
            "target_mb": target_mb, "uid": uid, "tmp": tmp,
            "path": path, "fname": fname,
            "platform": _default_platform(),
        }
        await safe_edit(
            st,
            f"🗜️ <b>Compress</b> → <b>{target_mb:.0f} MB</b>\n"
            f"<code>{fname[:45]}</code>\n"
            f"<code>{human_size(os.path.getsize(path))}</code>\n\n"
            "Pick backend and tap 🚀 Go:",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=compress_platform_kb(tok, _default_platform()),
        )

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
        await _stage_cmd(st, path, os.path.basename(path), tmp)
        return

    reply     = msg.reply_to_message
    if reply and target_mb:
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
            await _stage_cmd(st, path, fname, tmp)
            return

    _evict_resize_states()  # FIX H-05
    _interactive_mode[uid] = ("compress", time.time())
    _compress_waiting.pop(uid, None)
    await msg.reply(
        "🗜️ <b>Compress Video</b>\n\n"
        "Send me a <b>video file</b> or a <b>direct URL</b>.\n\n"
        "I'll ask for the target size (MB) after.\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# /cancel  (clears resize/compress interactive state)
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("cancel"), group=4)
async def cmd_cancel_resize(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid in _interactive_mode or uid in _compress_waiting:
        _interactive_mode.pop(uid, None)
        _compress_waiting.pop(uid, None)
        await msg.reply("❌ Cancelled.")
        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Entry from VIDEO MENU  (vid|resize|key  /  vid|compress_ask|key)
# stop_propagation() is critical — prevents video_cb from also firing
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
    cb.stop_propagation()


@Client.on_callback_query(filters.regex(r"^vid\|compress_ask\|"))
async def vid_compress_ask_cb(client: Client, cb: CallbackQuery):
    session_key = cb.data.split("|", 2)[2]
    uid = cb.from_user.id
    await cb.answer()
    _compress_waiting[uid] = {"session_key": session_key, "msg_id": cb.message.id}
    _interactive_mode.pop(uid, None)
    await cb.message.edit(
        "🗜️ <b>Compress Video</b>\n\n"
        "Send target size in <b>MB</b>:\n"
        "<code>85</code>  or  <code>200</code>\n\n"
        "<i>2-pass FFmpeg encode — takes ~2× real-time.</i>",
        parse_mode=enums.ParseMode.HTML,
    )
    cb.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Entry from URL MENU  (called from url_handler.py)
# ─────────────────────────────────────────────────────────────

async def handle_url_resize(client: Client, cb: CallbackQuery, url: str, token: str) -> None:
    fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
    tok   = _store_url(url, fname)
    await cb.message.edit(
        f"📐 <b>Resize</b>\n<code>{fname[:45]}</code>\n\nChoose resolution:",
        reply_markup=resize_resolution_kb(tok),
        parse_mode=enums.ParseMode.HTML,
    )


async def handle_url_compress(client: Client, cb: CallbackQuery, url: str, uid: int) -> None:
    fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
    _compress_waiting[uid] = {"url": url, "fname": fname, "msg": cb.message}
    _interactive_mode.pop(uid, None)
    await cb.message.edit(
        f"🗜️ <b>Compress</b>\n<code>{fname[:45]}</code>\n\n"
        "Send target size in <b>MB</b>:\n<code>85</code>",
        parse_mode=enums.ParseMode.HTML,
    )


# ─────────────────────────────────────────────────────────────
# Interactive file/URL receiver  (group=1)
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & (filters.video | filters.document),
    group=1,
)
async def resize_compress_file_receiver(client: Client, msg: Message):
    uid  = msg.from_user.id
    # FIX H-05: extract mode from (mode_str, timestamp) tuple
    _entry = _interactive_mode.get(uid)
    if not _entry:
        return
    mode = _entry[0]

    media = msg.video or msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "video.mkv"
    ext   = os.path.splitext(fname)[1].lower()
    if ext not in {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
                   ".ts", ".m2ts", ".wmv", ".m4v"} and not msg.video:
        return

    fsize = getattr(media, "file_size", 0) or 0
    _interactive_mode.pop(uid, None)

    if mode == "resize":
        tok = _store_tg_file(media.file_id, fname, fsize)
        await msg.reply(
            f"📐 <b>Resize</b>\n<code>{fname[:45]}</code>  "
            f"<code>{human_size(fsize)}</code>\n\nChoose resolution:",
            reply_markup=resize_resolution_kb(tok),
            parse_mode=enums.ParseMode.HTML,
        )
        msg.stop_propagation()

    elif mode == "compress":
        tok = _store_tg_file(media.file_id, fname, fsize)
        _compress_waiting[uid] = {"tg_tok": tok, "fname": fname}
        await msg.reply(
            f"🗜️ <b>Compress</b>  <code>{fname[:40]}</code>\n\n"
            "Send target size in <b>MB</b>:\n<code>85</code>  or  <code>200</code>\n\n"
            "<i>Send /cancel to abort.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# URL receiver for /resize interactive mode  (group=1)
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.text
    & ~filters.command(["start", "help", "settings", "cancel", "resize", "compress",
                        "nyaa_add", "nyaa_list", "nyaa_remove", "nyaa_check",
                        "nyaa_search", "nyaa_dump", "nyaa_toggle", "nyaa_edit"]),
    group=1,
)
async def resize_url_receiver(client: Client, msg: Message):
    uid  = msg.from_user.id
    # FIX H-05: extract mode from (mode_str, timestamp) tuple
    _entry = _interactive_mode.get(uid)
    if not _entry or _entry[0] != "resize":
        return
    mode = _entry[0]

    text = msg.text.strip()
    if not text.startswith("http"):
        return

    _interactive_mode.pop(uid, None)
    url   = text
    fname = url.split("/")[-1].split("?")[0][:50] or "video.mkv"
    tok   = _store_url(url, fname)
    await msg.reply(
        f"📐 <b>Resize</b>\n<code>{fname[:45]}</code>\n\nChoose resolution:",
        reply_markup=resize_resolution_kb(tok),
        parse_mode=enums.ParseMode.HTML,
    )
    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# MB value receiver — handles ALL compress sources  (group=1)
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.text
    & ~filters.command([
        "start", "help", "settings", "info", "status", "log", "restart",
        "broadcast", "admin", "ban_user", "unban_user", "banned_list",
        "cancel", "show_thumb", "del_thumb", "json_formatter", "bulk_url",
        "hardsub", "botname", "ccstatus", "convert", "resize", "compress",
        "usage", "captiontemplate",
        "nyaa_add", "nyaa_list", "nyaa_remove", "nyaa_check",
        "nyaa_search", "nyaa_dump", "nyaa_toggle", "nyaa_edit",
    ]),
    group=1,
)
async def compress_mb_receiver(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _compress_waiting:
        return

    text = msg.text.strip()
    if not re.match(r"^\d+(\.\d+)?$", text):
        return

    target_mb = float(text)
    info      = _compress_waiting.pop(uid)

    async def _stage_platform_pick(st, path, fname, tmp):
        """Download is done; stage platform picker."""
        tok = hashlib.md5(f"{uid}_{time.time()}".encode()).hexdigest()[:12]
        _cz_pending[tok] = {
            "target_mb": target_mb, "uid": uid, "tmp": tmp,
            "path": path, "fname": fname,
            "platform": _default_platform(),
        }
        await safe_edit(
            st,
            f"🗜️ <b>Compress</b> → <b>{target_mb:.0f} MB</b>\n"
            f"<code>{fname[:45]}</code>\n"
            f"<code>{human_size(os.path.getsize(path))}</code>\n\n"
            "Pick backend and tap 🚀 Go:",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=compress_platform_kb(tok, _default_platform()),
        )

    # ── Source: video menu session ────────────────────────────
    if "session_key" in info:
        from core.session import sessions
        session = sessions.get(info["session_key"])
        if not session:
            await msg.reply("❌ Session expired. Send the video again.")
            msg.stop_propagation()
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
                msg.stop_propagation()
                return
            await _stage_platform_pick(st, path, session.fname, tmp)
        except Exception as exc:
            cleanup(tmp)
            await safe_edit(st, f"❌ Failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        msg.stop_propagation()
        return

    # ── Source: URL (from URL menu button) ───────────────────
    if "url" in info:
        url    = info["url"]
        fname  = info["fname"]
        st_msg = info.get("msg")
        tmp    = make_tmp(cfg.download_dir, uid)
        st     = await msg.reply(
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
            await _stage_platform_pick(st, path, os.path.basename(path), tmp)
        except Exception as exc:
            cleanup(tmp)
            await safe_edit(st, f"❌ Failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        msg.stop_propagation()
        return

    # ── Source: interactive mode file token ───────────────────
    if "tg_tok" in info:
        tok   = info["tg_tok"]
        fname = info["fname"]
        entry = _get_token(tok)
        if not entry:
            await msg.reply("❌ Session expired. Send the video again.")
            msg.stop_propagation()
            return

        tmp = make_tmp(cfg.download_dir, uid)
        st  = await msg.reply(
            f"🗜️ <b>Compress</b> → <b>{target_mb:.0f} MB</b>\n"
            f"<code>{fname[:40]}</code>\n⬇️ Downloading…",
            parse_mode=enums.ParseMode.HTML,
        )
        try:
            from services.tg_download import tg_download
            path = await tg_download(
                client, entry["file_id"],
                os.path.join(tmp, fname), st,
                fname=fname, fsize=entry.get("fsize", 0), user_id=uid,
            )
            await _stage_platform_pick(st, path, fname, tmp)
        except Exception as exc:
            cleanup(tmp)
            await safe_edit(st, f"❌ Failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
        msg.stop_propagation()
        return

    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Resize platform toggle  rzp|<platform>|<token>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^rzp\|"))
async def rzp_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3: return await cb.answer("Invalid.", show_alert=True)
    _, plat, tok = parts
    await cb.answer()
    if plat not in ("cc", "fc", "local"): return
    entry = _get_token(tok)
    if not entry:
        return await safe_edit(cb.message, "❌ Session expired.")
    entry["platform"] = plat
    try:
        await cb.message.edit_reply_markup(reply_markup=resize_resolution_kb(tok, plat))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Compress platform picker + go  czp|<platform>|<tok>
# ─────────────────────────────────────────────────────────────

# Stores: tok → {target_mb, client, msg, path/entry info, tmp, uid, fname, platform}
_cz_pending: dict[str, dict] = {}


@Client.on_callback_query(filters.regex(r"^czp\|"))
async def czp_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|", 2)
    if len(parts) < 3: return await cb.answer("Invalid.", show_alert=True)
    _, action, tok = parts
    await cb.answer()
    pending = _cz_pending.get(tok)
    if not pending:
        return await safe_edit(cb.message, "❌ Session expired.", parse_mode=enums.ParseMode.HTML)

    if action == "cancel":
        _cz_pending.pop(tok, None)
        try: await cb.message.delete()
        except Exception: pass
        return

    if action in ("cc", "fc", "local"):
        pending["platform"] = action
        try:
            await cb.message.edit_reply_markup(
                reply_markup=compress_platform_kb(tok, action)
            )
        except Exception:
            pass
        return

    if action == "go":
        platform  = pending.get("platform", _default_platform())
        target_mb = pending["target_mb"]
        uid       = pending["uid"]
        tmp       = pending["tmp"]
        path      = pending["path"]
        fname     = pending["fname"]
        st        = cb.message
        _cz_pending.pop(tok, None)
        try:
            await _do_compress(client, st, path, fname, tmp, target_mb, uid, platform=platform)
        except Exception as exc:
            cleanup(tmp)
            await safe_edit(st, f"❌ <b>Compress failed</b>\n<code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)


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
    tmp    = make_tmp(cfg.download_dir, uid)

    try:
        if entry["kind"] == "url":
            url   = entry["url"]
            fname = entry["fname"]
            await safe_edit(cb.message,
                f"📐 <b>Resize → {height}p</b>\n"
                f"<code>{fname[:40]}</code>\n⬇️ Downloading…",
                parse_mode=enums.ParseMode.HTML)
            from services.downloader import download_direct
            path  = await download_direct(url, tmp)
            fname = os.path.basename(path)

        elif entry["kind"] == "tg_file":
            fname = entry["fname"]
            await safe_edit(cb.message,
                f"📐 <b>Resize → {height}p</b>\n"
                f"<code>{fname[:40]}</code>\n⬇️ Downloading…",
                parse_mode=enums.ParseMode.HTML)
            from services.tg_download import tg_download
            path = await tg_download(
                client, entry["file_id"],
                os.path.join(tmp, fname), cb.message,
                fname=fname, fsize=entry.get("fsize", 0), user_id=uid,
            )

        else:  # session
            from core.session import sessions
            from plugins.video import _ensure
            session = sessions.get(entry["key"])
            if not session:
                await safe_edit(cb.message, "❌ Session expired. Send the video again.")
                cleanup(tmp)
                return
            fname = session.fname
            await safe_edit(cb.message,
                f"📐 <b>Resize → {height}p</b>\n"
                f"<code>{fname[:40]}</code>\n⬇️ Downloading…",
                parse_mode=enums.ParseMode.HTML)
            async with session.lock:
                path = await _ensure(client, session, cb.message)
            if not path:
                cleanup(tmp)
                return

        platform = entry.get("platform", _default_platform())
        await _do_resize(client, cb.message, path, fname, tmp, height, uid, platform=platform)

    except Exception as exc:
        log.error("[Resize] %s", exc, exc_info=True)
        cleanup(tmp)
        await safe_edit(cb.message,
                        f"❌ <b>Resize failed</b>\n<code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)


# ─────────────────────────────────────────────────────────────
# Core processing — resize
# ─────────────────────────────────────────────────────────────

async def _do_resize(
    client, msg, path: str, fname: str, tmp: str, height: int, uid: int,
    platform: str | None = None,
) -> None:
    """
    Resize video to `height` px on the chosen backend (cc | fc | local).
    If the chosen backend is unavailable, falls back to next-best.
    """
    from services.task_runner import tracker, TaskRecord
    from services import ffmpeg as FF
    from services.uploader import upload_file
    from services.freeconvert_api import parse_fc_keys

    platform = platform or _default_platform()
    name_base = os.path.splitext(fname)[0]
    out_fname = f"{name_base}_{height}p.mp4"
    out       = os.path.join(tmp, out_fname)

    # ── CloudConvert branch ───────────────────────────────────
    if platform == "cc" and _has_cc():
        try:
            from services.cloudconvert_api import (
                parse_api_keys, pick_best_key, submit_convert, run_cc_job,
            )
            cc_keys = parse_api_keys(os.environ.get("CC_API_KEY", "").strip())
            cc_key, _credits = await pick_best_key(cc_keys) if len(cc_keys) > 1 else (cc_keys[0], 0)

            tid    = tracker.new_tid()
            record = TaskRecord(
                tid=tid, user_id=uid,
                label=f"CC Resize → {height}p",
                mode="proc", engine="cloudconvert",
                fname=fname, state="☁️ Uploading to CC…",
            )
            await tracker.register(record)

            await safe_edit(msg,
                f"☁️ <b>CC Resize → {height}p</b>\n"
                f"<code>{fname[:40]}</code>\n⬆️ Uploading to CloudConvert…",
                parse_mode=enums.ParseMode.HTML)

            job_id = await submit_convert(
                cc_key, video_path=path,
                scale_height=height, crf=23, output_name=out_fname,
            )
            await safe_edit(msg,
                f"⏳ <b>CC Resize → {height}p</b>\n"
                f"🆔 <code>{job_id}</code>\n<i>CloudConvert processing…</i>",
                parse_mode=enums.ParseMode.HTML)

            result_path = await run_cc_job(cc_key, job_id, tmp, output_name=out_fname)
            record.update(state="✅ Done")
            fsize = os.path.getsize(result_path)

            try: await msg.delete()
            except Exception: pass
            st = await client.send_message(
                uid,
                f"📐 <b>CC Resize done!</b>  {height}p\n"
                f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\n"
                f"⬆️ Uploading…",
                parse_mode=enums.ParseMode.HTML,
            )
            try:
                await upload_file(client, st, result_path, user_id=uid)
            finally:
                cleanup(tmp)
            return
        except Exception as exc:
            log.warning("[CC-resize] failed: %s", exc)
            await safe_edit(msg,
                f"⚠️ CC failed — falling back to local FFmpeg\n<code>{str(exc)[:150]}</code>",
                parse_mode=enums.ParseMode.HTML)
            platform = "local"

    # ── FreeConvert branch ────────────────────────────────────
    if platform == "fc" and _has_fc():
        fc_raw  = os.environ.get("FC_API_KEY", "").strip()
        fc_keys = parse_fc_keys(fc_raw)
        for _ii in range(2, 10):
            _xtra = os.environ.get(f"FC_API_KEY_{_ii}", "").strip()
            if _xtra:
                fc_keys.extend(parse_fc_keys(_xtra))

        if fc_keys:
            try:
                from services.freeconvert_api import pick_best_fc_key, submit_convert, run_fc_job
                tid    = tracker.new_tid()
                record = TaskRecord(
                    tid=tid, user_id=uid,
                    label=f"FC Resize → {height}p",
                    mode="proc", engine="freeconvert",
                    fname=fname, state="☁️ Uploading to FC…",
                )
                await tracker.register(record)
                await safe_edit(msg,
                    f"☁️ <b>FC Resize → {height}p</b>\n"
                    f"<code>{fname[:40]}</code>\n⬆️ Uploading to FreeConvert…",
                    parse_mode=enums.ParseMode.HTML)
                key, _mins = await pick_best_fc_key(fc_keys)
                job_id = await submit_convert(
                    key, video_path=path, scale_height=height, crf=23, output_name=out_fname,
                )
                await safe_edit(msg,
                    f"⏳ <b>FC Resize → {height}p</b>\n"
                    f"🆔 <code>{job_id}</code>\n<i>FreeConvert processing…</i>",
                    parse_mode=enums.ParseMode.HTML)
                result_path = await run_fc_job(key, job_id, tmp, output_name=out_fname)
                record.update(state="✅ Done")
                fsize = os.path.getsize(result_path)
                try: await msg.delete()
                except Exception: pass
                st = await client.send_message(
                    uid,
                    f"📐 <b>FC Resize done!</b>  {height}p\n"
                    f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\n"
                    f"⬆️ Uploading…",
                    parse_mode=enums.ParseMode.HTML,
                )
                try:
                    await upload_file(client, st, result_path, user_id=uid)
                finally:
                    cleanup(tmp)
                return
            except Exception as exc:
                log.warning("[FC-resize] FC failed, fallback: %s", exc)
                await safe_edit(msg,
                    f"⚠️ FC failed — using local FFmpeg\n<code>{str(exc)[:100]}</code>",
                    parse_mode=enums.ParseMode.HTML)

    # ── Local FFmpeg ──────────────────────────────────────────
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
    except Exception as exc:
        record.update(state="❌ Failed")
        await safe_edit(msg, f"❌ <b>Resize failed</b>\n<code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        cleanup(tmp)
        return

    fsize = os.path.getsize(out)
    log.info("[Resize] Done: %s → %dp  (%s)", fname, height, human_size(fsize))

    try:
        await msg.delete()
    except Exception:
        pass

    st = await client.send_message(
        uid,
        f"📐 <b>Resize done!</b>  {height}p\n"
        f"<code>{out_fname}</code>  <code>{human_size(fsize)}</code>\n"
        f"⬆️ Uploading…",
        parse_mode=enums.ParseMode.HTML,
    )
    try:
        await upload_file(client, st, out, user_id=uid)
    finally:
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Core processing — compress
# ─────────────────────────────────────────────────────────────

async def _do_compress(
    client, msg, path: str, fname: str, tmp: str, target_mb: float, uid: int,
    platform: str | None = None,
) -> None:
    from services.task_runner import tracker, TaskRecord
    from services import ffmpeg as FF
    from services.uploader import upload_file

    platform  = platform or _default_platform()
    name_base = os.path.splitext(fname)[0]
    out_fname = f"{name_base}_{int(target_mb)}MB.mp4"
    out       = os.path.join(tmp, out_fname)

    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=uid,
        label=f"Compress → {target_mb:.0f} MB",
        mode="proc", engine=platform,
        fname=fname, state=f"🗜️ Compressing to {target_mb:.0f} MB…",
    )
    await tracker.register(record)

    # ── CloudConvert branch ───────────────────────────────────
    if platform == "cc" and _has_cc():
        try:
            from services.cloudconvert_api import (
                parse_api_keys, pick_best_key, submit_compress, run_cc_job,
            )
            cc_keys = parse_api_keys(os.environ.get("CC_API_KEY", "").strip())
            cc_key, _credits = await pick_best_key(cc_keys) if len(cc_keys) > 1 else (cc_keys[0], 0)

            record.update(state="☁️ Uploading to CC…")
            await safe_edit(msg,
                f"☁️ <b>CC Compress → {target_mb:.0f} MB</b>\n"
                f"<code>{fname[:40]}</code>\n⬆️ Uploading to CloudConvert…",
                parse_mode=enums.ParseMode.HTML)
            job_id = await submit_compress(
                cc_key, video_path=path, target_mb=target_mb, output_name=out_fname,
            )
            await safe_edit(msg,
                f"⏳ <b>CC Compress → {target_mb:.0f} MB</b>\n"
                f"🆔 <code>{job_id}</code>\n<i>CloudConvert processing…</i>",
                parse_mode=enums.ParseMode.HTML)
            result_c = await run_cc_job(cc_key, job_id, tmp, output_name=out_fname)
            record.update(state="✅ Done")
            fsize_c = os.path.getsize(result_c)
            log.info("[Compress] CC done: %s → %.0f MB actual %s",
                     fname, target_mb, human_size(fsize_c))
            try: await msg.delete()
            except Exception: pass
            st_c = await client.send_message(
                uid,
                f"🗜️ <b>CC Compress done!</b>  {human_size(fsize_c)}\n"
                f"<code>{out_fname}</code>\n⬆️ Uploading…",
                parse_mode=enums.ParseMode.HTML)
            try:
                await upload_file(client, st_c, result_c, user_id=uid)
            finally:
                cleanup(tmp)
            return
        except Exception as exc:
            record.update(state="⚠️ CC failed")
            log.warning("[CC-compress] failed, fallback: %s", exc)
            await safe_edit(msg,
                f"⚠️ CC failed — using local FFmpeg\n<code>{str(exc)[:150]}</code>",
                parse_mode=enums.ParseMode.HTML)
            platform = "local"

    # ── FreeConvert branch ────────────────────────────────────
    if platform == "fc" and _has_fc():
        from services.freeconvert_api import parse_fc_keys
        fc_raw2  = os.environ.get("FC_API_KEY", "").strip()
        fc_keys2 = parse_fc_keys(fc_raw2)
        for _jj in range(2, 10):
            _xtra2 = os.environ.get(f"FC_API_KEY_{_jj}", "").strip()
            if _xtra2:
                fc_keys2.extend(parse_fc_keys(_xtra2))

        if fc_keys2:
            try:
                from services.freeconvert_api import pick_best_fc_key, submit_compress, run_fc_job
                record.update(state="☁️ Uploading to FC…")
                await safe_edit(msg,
                    f"🗜️ <b>FC Compress → {target_mb:.0f} MB</b>\n"
                    f"<code>{fname[:40]}</code>\n⬆️ Uploading to FreeConvert…",
                    parse_mode=enums.ParseMode.HTML)
                key2c, _mc = await pick_best_fc_key(fc_keys2)
                job_idc = await submit_compress(key2c, video_path=path, target_mb=target_mb, output_name=out_fname)
                await safe_edit(msg,
                    f"⏳ <b>FC Compress → {target_mb:.0f} MB</b>\n"
                    f"🆔 <code>{job_idc}</code>\n<i>FreeConvert processing…</i>",
                    parse_mode=enums.ParseMode.HTML)
                result_c = await run_fc_job(key2c, job_idc, tmp, output_name=out_fname)
                record.update(state="✅ Done")
                fsize_c = os.path.getsize(result_c)
                log.info("[Compress] FC done: %s → %.0f MB actual %s", fname, target_mb, human_size(fsize_c))
                try: await msg.delete()
                except Exception: pass
                st_c = await client.send_message(
                    uid,
                    f"🗜️ <b>FC Compress done!</b>  {human_size(fsize_c)}\n"
                    f"<code>{out_fname}</code>\n⬆️ Uploading…",
                    parse_mode=enums.ParseMode.HTML)
                try:
                    await upload_file(client, st_c, result_c, user_id=uid)
                finally:
                    cleanup(tmp)
                return
            except Exception as exc:
                record.update(state="⚠️ FC failed")
                log.warning("[FC-compress] FC failed, fallback: %s", exc)
                await safe_edit(msg,
                    f"⚠️ FC failed — using local FFmpeg 2-pass\n<code>{str(exc)[:100]}</code>",
                    parse_mode=enums.ParseMode.HTML)

    # ── Local FFmpeg 2-pass ───────────────────────────────────
    try:
        record.update(state="🗜️ Pass 1/2…")
        await FF.compress_to_size(path, out, target_mb)
        record.update(state="✅ Done")
    except Exception as exc:
        record.update(state="❌ Failed")
        await safe_edit(msg, f"❌ <b>Compress failed</b>\n<code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        cleanup(tmp)
        return

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
    try:
        await upload_file(client, st, out, user_id=uid)
    finally:
        cleanup(tmp)
