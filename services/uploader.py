"""
services/uploader.py
Upload a local file to Telegram.

- Metadata + thumbnail via services/ffmpeg.video_meta() (correct probe flags)
- Shows "🔍 Analyzing…" state during ffprobe so user knows it's not frozen
- Global upload semaphore (1 slot) prevents Pyrogram deadlock when two large
  send_video() calls run simultaneously on the same client
- FloodWait retry preserved
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from pyrogram import Client, enums
from pyrogram.errors import FloodWait

from core.config import cfg
from services.utils import human_size, safe_edit

log = logging.getLogger(__name__)

_AUDIO_EXTS = {".mp3",".aac",".flac",".ogg",".m4a",".opus",".wav",".wma",".ac3",".mka"}
_VIDEO_EXTS = {
    ".mp4",".mov",".webm",".m4v",".mkv",".avi",".flv",
    ".ts",".m2ts",".wmv",".3gp",".rmvb",".mpg",".mpeg",
}


def _chat_id(msg) -> int:
    try:
        if hasattr(msg, "chat") and msg.chat and msg.chat.id:
            return msg.chat.id
    except Exception:
        pass
    try:
        if hasattr(msg, "from_user") and msg.from_user and msg.from_user.id:
            return msg.from_user.id
    except Exception:
        pass
    return 0


async def upload_file(
    client:         Client,
    msg,                         # legacy: may be _up_dummy or a real message
    path:           str,
    caption:        str  = "",
    thumb:          str | None = None,
    force_document: bool = False,
    task_record     = None,
    status_msg      = None,      # accepted for API compatibility, not used
) -> None:
    """Upload `path` to Telegram."""
    if not os.path.isfile(path):
        await safe_edit(msg,
            f"❌ File not found: <code>{os.path.basename(path)}</code>",
            parse_mode=enums.ParseMode.HTML)
        return

    chat_id = _chat_id(msg)
    if not chat_id:
        log.error("upload_file: cannot determine chat_id")
        return

    file_size = os.path.getsize(path)
    fname     = os.path.basename(path)
    ext       = os.path.splitext(fname)[1].lower()

    if not caption:
        caption = f"<code>{fname}</code>"

    if force_document:
        method = "document"
    elif ext in _AUDIO_EXTS:
        method = "audio"
    elif ext in _VIDEO_EXTS:
        method = "video"
    else:
        method = "document"

    vid_meta: dict = {"duration": 0, "width": 0, "height": 0, "thumb": None}
    auto_thumb: str | None = None
    user_thumb: str | None = None  # downloaded from Telegram, needs cleanup after upload

    # ── Load user's saved thumbnail from settings ────────────────────────────
    # This is the thumbnail set via /settings → Set Thumbnail.
    # It takes priority over the auto-generated ffprobe thumbnail.
    if not thumb:
        try:
            from core.session import settings as _settings
            _s = await _settings.get(chat_id)
            _thumb_id = _s.get("thumb_id")
            if _thumb_id:
                import tempfile
                from core.session import get_client as _get_client
                _cl = _get_client()
                _tmp_thumb = tempfile.NamedTemporaryFile(
                    suffix=".jpg", dir=cfg.download_dir, delete=False
                )
                _tmp_thumb.close()
                await _cl.download_media(_thumb_id, file_name=_tmp_thumb.name)
                if os.path.isfile(_tmp_thumb.name) and os.path.getsize(_tmp_thumb.name) > 0:
                    thumb      = _tmp_thumb.name
                    user_thumb = _tmp_thumb.name
                    log.info("Using user thumbnail: %s", _thumb_id[:16])
                else:
                    os.remove(_tmp_thumb.name)
        except Exception as _te:
            log.warning("Could not load user thumbnail: %s", _te)
    # ─────────────────────────────────────────────────────────────────────────

    if ext in _VIDEO_EXTS and method in ("video", "document"):
        # Show "Analyzing…" state while ffprobe runs (can take 10–30s for large files)
        # so the user knows the bot is working, not frozen at 0%.
        if task_record is not None:
            task_record.update(state="🔍 Analyzing…", fname=fname)
        try:
            from services.task_runner import tracker as _tracker, TaskRecord as _TR, runner as _runner
            _runner._wake_panel(chat_id, immediate=True)
        except Exception:
            pass

        try:
            from services.ffmpeg import video_meta
            vid_meta = await video_meta(path)
        except Exception as exc:
            log.warning("video_meta failed for %s: %s", fname, exc)

        if not thumb:
            # No user thumbnail — try the auto-generated ffprobe one
            thumb = vid_meta.get("thumb")
            if thumb and os.path.isfile(thumb):
                auto_thumb = thumb
            else:
                thumb = None

        log.info(
            "Video meta: duration=%ds  %dx%d  thumb=%s",
            vid_meta.get("duration", 0),
            vid_meta.get("width", 0),
            vid_meta.get("height", 0),
            "yes" if thumb else "no",
        )

    # ── TaskRecord ─────────────────────────────────────────────
    from services.task_runner import tracker, TaskRecord, runner

    if task_record is None:
        tid    = tracker.new_tid()
        record = TaskRecord(
            tid=tid, user_id=chat_id,
            label=f"Upload {fname}", mode="ul", engine="telegram",
            fname=fname, total=file_size,
            state="📤 Uploading",
        )
        await tracker.register(record)
    else:
        record = task_record
        record.update(mode="ul", engine="telegram", total=file_size, fname=fname)

    start      = time.time()
    last_panel = [start]
    # Throttle: only update the TaskRecord/panel at most once per second.
    # pyrogram fires _progress on every 512 KiB chunk — at 50 MB/s that is
    # ~100 calls/sec.  Each call would hit record.update() + asyncio event
    # dispatch, adding measurable overhead. With this guard we drop 99% of
    # those calls without losing any visible accuracy on the progress bar.
    _PROGRESS_INTERVAL = 1.0  # seconds between panel refreshes

    async def _progress(current: int, total: int) -> None:
        now = time.time()
        if now - last_panel[0] < _PROGRESS_INTERVAL:
            return                  # skip — too soon, no work needed
        last_panel[0] = now
        elapsed = now - start
        speed   = current / elapsed if elapsed else 0
        eta     = int((total - current) / speed) if speed else 0
        record.update(
            done=current, total=total,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📤 Uploading",
        )
        runner._wake_panel(chat_id)

    async def _send() -> None:
        common = dict(
            caption=caption,
            thumb=thumb,
            parse_mode=enums.ParseMode.HTML,
            progress=_progress,
        )

        if method == "video":
            sent = await client.send_video(
                chat_id, path,
                duration=vid_meta.get("duration", 0),
                width=vid_meta.get("width", 0),
                height=vid_meta.get("height", 0),
                supports_streaming=True,
                **common,
            )
        elif method == "audio":
            sent = await client.send_audio(chat_id, path, **common)
        else:
            sent = await client.send_document(
                chat_id, path,
                force_document=True,
                **common,
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
        runner._wake_panel(chat_id)

    # Acquire the global upload semaphore BEFORE calling send_video/send_audio/
    # send_document.  Pyrogram deadlocks when two large uploads run concurrently
    # on the same client (both wait for a connection the other holds).
    # One upload at a time — matches the reference bot's sequential loop.
    from services.task_runner import runner as _runner_ul
    upload_sem = _runner_ul._get_upload_sem()

    try:
        async with upload_sem:
            record.update(state="📤 Uploading")
            _runner_ul._wake_panel(chat_id, immediate=True)
            await _send()
    except FloodWait as fw:
        if fw.value <= 60:
            log.warning("FloodWait %ds — waiting", fw.value)
            record.update(state=f"⏳ FloodWait {fw.value}s")
            await asyncio.sleep(fw.value)
            async with upload_sem:
                await _send()
        else:
            raise
    except Exception as exc:
        err = str(exc)
        if "MESSAGE_NOT_MODIFIED" not in err:
            record.update(state=f"❌ {str(exc)[:60]}")
            runner._wake_panel(chat_id)
            await safe_edit(msg,
                f"❌ Upload failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML)
        raise
    finally:
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
        if user_thumb and os.path.isfile(user_thumb):
            try:
                os.remove(user_thumb)
            except OSError:
                pass
