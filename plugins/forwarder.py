"""
plugins/forwarder.py
Forward files to any channel/group without the "forwarded from" tag.

FIX H-06 (audit v3): _FWD_STATE now has TTL eviction so abandoned
/forward sessions (and their downloaded files) are cleaned up.
"""
import os
import time as _time
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from core.config import cfg
from services.utils import make_tmp, cleanup

_FWD_STATE: dict = {}
_FWD_TTL = 600  # 10 min
_VIDEO_EXTS = {".mp4",".mkv",".avi",".mov",".webm",".flv"}
_AUDIO_EXTS = {".mp3",".aac",".flac",".m4a",".opus",".ogg"}


def _evict_fwd_states() -> None:
    """FIX H-06: clean stale forward sessions and their temp files."""
    now = _time.time()
    dead = [uid for uid, s in _FWD_STATE.items()
            if now - s.get("_ts", 0) > _FWD_TTL]
    for uid in dead:
        s = _FWD_STATE.pop(uid, {})
        if s.get("tmp"):
            cleanup(s["tmp"])


@Client.on_message(filters.private & filters.command("forward"))
async def cmd_forward(client: Client, msg: Message):
    uid = msg.from_user.id
    _evict_fwd_states()
    _FWD_STATE[uid] = {"step": "waiting_file", "_ts": _time.time()}
    await msg.reply(
        "📨 <b>Media Forwarder</b> (no forward tag)\n\n"
        "Send me the file you want to forward.\n"
        "The bot must be <b>admin</b> in the target channel/group.",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.private & filters.text, group=7)
async def fwd_target(client: Client, msg: Message):
    uid   = msg.from_user.id
    state = _FWD_STATE.get(uid)
    if not state or state.get("step") != "waiting_target":
        return
    if msg.text.startswith("/"):
        return
    target = msg.text.strip()
    path   = state.get("path")
    tmp    = state.get("tmp")
    if not path or not os.path.exists(path):
        _FWD_STATE.pop(uid, None)
        return await msg.reply("❌ File not found. Use /forward to start again.")
    st = await msg.reply(f"📨 Forwarding to <code>{target}</code>…",
                         parse_mode=enums.ParseMode.HTML)
    try:
        ext = os.path.splitext(path)[1].lower()
        if ext in _VIDEO_EXTS:    await client.send_video(target, path)
        elif ext in _AUDIO_EXTS:  await client.send_audio(target, path)
        else:                      await client.send_document(target, path)
        await st.edit(f"✅ Forwarded to <code>{target}</code> without forward tag!",
                      parse_mode=enums.ParseMode.HTML)
    except Exception as exc:
        await st.edit(f"❌ Failed: <code>{exc}</code>\nBot must be admin in <code>{target}</code>.",
                      parse_mode=enums.ParseMode.HTML)
    if tmp:
        cleanup(tmp)
    _FWD_STATE.pop(uid, None)


async def handle_fwd_file(client: Client, msg: Message, uid: int):
    """Called from media_router when the fwd step == 'waiting_file'."""
    media = msg.video or msg.audio or msg.document or msg.photo
    if not media:
        return
    tmp = make_tmp(cfg.download_dir, uid)
    try:
        path = await client.download_media(media, file_name=os.path.join(tmp, "fwd_file"))
    except Exception as exc:
        await msg.reply(f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        return
    _FWD_STATE[uid] = {"step": "waiting_target", "path": path, "tmp": tmp, "_ts": _time.time()}
    await msg.reply(
        "✅ File received!\n\nSend the channel username or ID:\n"
        "<code>@mychannel</code> or <code>-100123456789</code>\n\n"
        "<i>Bot must be admin there.</i>",
        parse_mode=enums.ParseMode.HTML,
    )
