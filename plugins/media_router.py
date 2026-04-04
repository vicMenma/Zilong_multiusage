"""
plugins/media_router.py
Receives every file/photo sent in private and dispatches cleanly.

Change: removed runner.ensure_panel() call — panels are only shown on
explicit /status command. All background progress is tracked silently.
"""
import os
import logging

from pyrogram import Client, filters, enums
from pyrogram.types import Message

from core.config import cfg
from core.session import sessions, settings, users
from services.utils import human_size, make_tmp

log = logging.getLogger(__name__)

_VIDEO_EXTS   = {".mp4",".mkv",".avi",".mov",".webm",".flv",
                 ".ts",".m2ts",".wmv",".3gp",".m4v",".rmvb",".mpg",".mpeg"}
_AUDIO_EXTS   = {".mp3",".aac",".m4a",".opus",".ogg",".flac",".wav",".wma",".ac3",".mka"}
_ARCHIVE_EXTS = {".zip",".rar",".7z",".tar",".gz",".bz2",".xz",".cbr",".cb7"}


@Client.on_message(
    filters.private & (filters.video | filters.audio | filters.document | filters.photo),
    group=3,
)
async def media_router(client: Client, msg: Message):
    uid = msg.from_user.id if msg.from_user else None
    if not uid:
        return

    await users.register(uid, msg.from_user.first_name or "")

    # ── Check size limit ──────────────────────────────────────
    media = msg.video or msg.audio or msg.document
    fsize = getattr(media, "file_size", 0) or 0

    if fsize > cfg.file_limit_b and not msg.photo:
        await msg.reply(
            f"❌ <b>File too large</b>\n"
            f"Size: <code>{human_size(fsize)}</code>\n"
            f"Limit: <code>{human_size(cfg.file_limit_b)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    # ── Photo → save as thumbnail ─────────────────────────────
    if msg.photo:
        await settings.update(uid, {"thumb_id": msg.photo.file_id})
        await msg.reply("🖼️ Thumbnail saved!\nUse /settings → Clear Thumbnail to remove.")
        msg.stop_propagation()
        return

    # ── Check if a session is waiting for a secondary file ────
    waiting_session = sessions.waiting_session(uid)
    if waiting_session and waiting_session.waiting in (
        "merge_av", "merge_vs", "burn_sub", "merge_vids"
    ):
        from plugins.video import handle_secondary_file
        await handle_secondary_file(client, msg, waiting_session)
        msg.stop_propagation()
        return

    # ── Forward flow ──────────────────────────────────────────
    from plugins.forwarder import _FWD_STATE
    fwd_state = _FWD_STATE.get(uid)
    if fwd_state and fwd_state.get("step") == "waiting_file":
        from plugins.forwarder import handle_fwd_file
        await handle_fwd_file(client, msg, uid)
        msg.stop_propagation()
        return

    # ── Archive creation collect flow ─────────────────────────
    from plugins.archive import _CREATE_STATE
    if uid in _CREATE_STATE:
        from plugins.archive import handle_archive_collect
        await handle_archive_collect(client, msg, uid)
        msg.stop_propagation()
        return

    if not media:
        return

    fname = getattr(media, "file_name", None) or ""
    ext   = os.path.splitext(fname)[1].lower()

    # ── Torrent file ──────────────────────────────────────────
    if ext == ".torrent":
        from plugins.url_handler import handle_torrent_file
        await handle_torrent_file(client, msg, media, uid)
        msg.stop_propagation()
        return

    # ── Archive ───────────────────────────────────────────────
    if ext in _ARCHIVE_EXTS:
        from plugins.archive import handle_archive_file
        await handle_archive_file(client, msg, media, fname, fsize, uid)
        msg.stop_propagation()
        return

    # ── Video ─────────────────────────────────────────────────
    if msg.video or ext in _VIDEO_EXTS:
        if not fname:
            fname = f"video_{media.file_id[:8]}.mp4"
            ext   = ".mp4"
        await _open_video_session(client, msg, media, fname, fsize, ext, uid)
        msg.stop_propagation()
        return

    # ── Audio ─────────────────────────────────────────────────
    if msg.audio or ext in _AUDIO_EXTS:
        if not fname:
            fname = f"audio_{media.file_id[:8]}{ext or '.mp3'}"
        await _open_audio_session(client, msg, media, fname, fsize, ext, uid)
        msg.stop_propagation()
        return

    # ── Unknown document — offer download ─────────────────────
    await msg.reply(
        f"<b>{fname or 'File'}</b>  <code>{human_size(fsize)}</code>\n"
        "<i>File type not directly supported. Sending as document.</i>",
        parse_mode=enums.ParseMode.HTML,
    )
    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Session builders
# ─────────────────────────────────────────────────────────────

async def _open_video_session(client, msg, media, fname, fsize, ext, uid):
    from plugins.video import video_menu_kb
    tmp     = make_tmp(cfg.download_dir, uid)
    session = await sessions.create(
        user_id=uid, file_id=media.file_id,
        fname=fname, fsize=fsize, ext=ext, tmp_dir=tmp,
    )
    await msg.reply(
        f"<b>{fname}</b>\n<code>{human_size(fsize)}</code>\n\n<i>Choose an action:</i>",
        reply_markup=video_menu_kb(session.key),
        parse_mode=enums.ParseMode.HTML,
    )


async def _open_audio_session(client, msg, media, fname, fsize, ext, uid):
    from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    tmp     = make_tmp(cfg.download_dir, uid)
    session = await sessions.create(
        user_id=uid, file_id=media.file_id,
        fname=fname, fsize=fsize, ext=ext, tmp_dir=tmp,
    )
    key = session.key
    await msg.reply(
        f"<b>{fname}</b>\n<code>{human_size(fsize)}</code>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🟡 Convert Audio", callback_data=f"vaud|mp3|{key}"),
             InlineKeyboardButton("📊 Media Info",    callback_data=f"vid|mediainfo|{key}")],
            [InlineKeyboardButton("🎵 Extract Audio", callback_data=f"vid|to_audio|{key}"),
             InlineKeyboardButton("❌ Cancel",        callback_data=f"vid|cancel|{key}")],
        ]),
    )
