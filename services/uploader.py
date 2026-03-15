"""
services/uploader.py
Upload a local file to Telegram.
- Auto-detects send method (video / audio / document)
- Generates thumbnail if missing
- Reports progress via TaskRecord
- Forwards to LOG_CHANNEL if set
- Retries once on FloodWait ≤ 30s
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from pyrogram import Client, enums
from pyrogram.errors import FloodWait

from core.config import cfg
from services.ffmpeg import video_meta
from services.utils import human_size, progress_panel, safe_edit

log = logging.getLogger(__name__)

_AUDIO_EXTS = {".mp3",".aac",".flac",".ogg",".m4a",".opus",".wav",".wma",".ac3",".mka"}

# ALL of these are sent as video (Telegram streams them all fine)
# Use force_document=True to override per-call
_VIDEO_EXTS = {
    ".mp4",".mov",".webm",".m4v",".mkv",".avi",".flv",
    ".ts",".m2ts",".wmv",".3gp",".rmvb",".mpg",".mpeg",
}


def _chat_id(msg) -> int:
    if msg.chat and msg.chat.id:
        return msg.chat.id
    if msg.from_user and msg.from_user.id:
        return msg.from_user.id
    return 0


async def upload_file(
    client:         Client,
    msg,                         # progress message to edit / delete after upload
    path:           str,
    caption:        str  = "",
    thumb:          str | None = None,
    force_document: bool = False,
    task_record     = None,      # Optional TaskRecord from task_runner
) -> None:
    """
    Upload `path` to the chat of `msg`.
    On success, deletes `msg`. On failure, edits `msg` with the error.
    """
    if not os.path.isfile(path):
        await safe_edit(msg,
            f"❌ File not found: <code>{os.path.basename(path)}</code>",
            parse_mode=enums.ParseMode.HTML)
        return

    chat_id   = _chat_id(msg)
    if not chat_id:
        log.error("upload_file: cannot determine chat_id")
        return

    file_size = os.path.getsize(path)
    fname     = os.path.basename(path)
    ext       = os.path.splitext(fname)[1].lower()

    if not caption:
        caption = f"<code>{fname}</code>"

    # Determine upload method
    if force_document:
        method = "document"
    elif ext in _AUDIO_EXTS:
        method = "audio"
    elif ext in _VIDEO_EXTS:
        method = "video"      # ALL video formats sent as video by default
    else:
        method = "document"

    # Get video metadata + thumbnail for video/document-video files
    vid_meta: dict = {"duration": 0, "width": 0, "height": 0, "thumb": None}
    auto_thumb: str | None = None

    if ext in _VIDEO_EXTS and method in ("video", "document"):
        try:
            vid_meta  = await video_meta(path)
            if not thumb and vid_meta.get("thumb"):
                auto_thumb = vid_meta["thumb"]
                thumb      = auto_thumb
        except Exception as exc:
            log.warning("video_meta failed for %s: %s", fname, exc)

    from services.task_runner import tracker, TaskRecord
    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=chat_id,
        label=fname, mode="ul", engine="telegram",
        fname=fname, total=file_size,
    )
    await tracker.register(record)

    start = time.time()
    last  = [start]

    async def _progress(current: int, total: int) -> None:
        now = time.time()
        if now - last[0] < 2.5:
            return
        last[0]  = now
        elapsed  = now - start
        speed    = current / elapsed if elapsed else 0
        eta      = int((total - current) / speed) if speed else 0
        panel    = progress_panel(
            mode="ul", fname=fname,
            done=current, total=total,
            speed=speed, eta=eta, elapsed=elapsed,
            engine="telegram",
        )
        await safe_edit(msg, panel, parse_mode=enums.ParseMode.HTML)
        record.update(
            done=current, total=total,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📤 Uploading",
        )
        if task_record:
            task_record.update(
                fname=fname, done=current, total=total,
                speed=speed, eta=eta, elapsed=elapsed,
                state="📤 Uploading",
            )

    async def _send() -> None:
        if method == "video":
            sent = await client.send_video(
                chat_id, path, caption=caption,
                duration=vid_meta["duration"],
                width=vid_meta["width"],
                height=vid_meta["height"],
                thumb=thumb,
                supports_streaming=True,
                parse_mode=enums.ParseMode.HTML,
                progress=_progress,
            )
        elif method == "audio":
            sent = await client.send_audio(
                chat_id, path, caption=caption,
                thumb=thumb,
                parse_mode=enums.ParseMode.HTML,
                progress=_progress,
            )
        else:
            sent = await client.send_document(
                chat_id, path, caption=caption,
                thumb=thumb,
                force_document=True,
                parse_mode=enums.ParseMode.HTML,
                progress=_progress,
            )

        try:
            await msg.delete()
        except Exception:
            pass

        if cfg.log_channel and sent:
            try:
                await sent.forward(cfg.log_channel)
            except Exception:
                pass

        record.update(state="✅ Done", done=file_size, total=file_size)

    # Send with one FloodWait retry
    try:
        await _send()
    except FloodWait as fw:
        if fw.value <= 30:
            log.warning("FloodWait %ds — waiting", fw.value)
            await asyncio.sleep(fw.value)
            await _send()
        else:
            raise
    except Exception as exc:
        err = str(exc)
        if "MESSAGE_NOT_MODIFIED" not in err:
            await safe_edit(msg,
                f"❌ Upload failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML)
        raise
    finally:
        # Clean up auto-generated thumbnail
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
