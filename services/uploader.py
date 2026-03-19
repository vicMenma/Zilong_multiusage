"""
services/uploader.py
Upload a local file to Telegram.

Reference-bot pattern adopted:
- upload_file() accepts an optional `status_msg` — the live panel message.
- During upload the progress callback edits that message directly
  (throttled to 3 seconds, exactly like the reference bot's isTimeOver()).
- This gives seamless download→upload in a SINGLE message, no gap.
- TaskRecord is still updated for /status and multi-user tracking.
- FloodWait retry preserved.
- Metadata via services/ffmpeg.video_meta() (correct -analyzeduration flags).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from pyrogram import Client, enums
from pyrogram.errors import FloodWait

from core.config import cfg
from services.utils import human_size, human_dur, safe_edit

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


def _spd_icon(bps: float) -> str:
    mib = bps / (1024 * 1024)
    if mib >= 50: return "🚀"
    if mib >= 10: return "⚡"
    if mib >= 1:  return "🏃"
    return "🐢"


def _bar(pct: float, w: int = 12) -> str:
    pct    = min(max(pct, 0), 100)
    filled = int(pct / 100 * w)
    return "█" * filled + "░" * (w - filled)


async def upload_file(
    client:         Client,
    msg,                         # legacy: may be _up_dummy or a real message
    path:           str,
    caption:        str  = "",
    thumb:          str | None = None,
    force_document: bool = False,
    task_record     = None,
    status_msg      = None,      # reference-bot pattern: the live panel message
) -> None:
    """
    Upload `path` to Telegram.

    status_msg — if provided, this message is edited directly during upload
                 (reference bot pattern). Throttled to once every 3 seconds.
    """
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

    if ext in _VIDEO_EXTS and method in ("video", "document"):
        try:
            from services.ffmpeg import video_meta
            vid_meta = await video_meta(path)
        except Exception as exc:
            log.warning("video_meta failed for %s: %s", fname, exc)

        if not thumb:
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
    last_edit  = [0.0]   # throttle: edit status_msg at most once every 3s
    last_panel = [start]

    async def _progress(current: int, total: int) -> None:
        now     = time.time()
        elapsed = now - start
        speed   = current / elapsed if elapsed else 0
        eta     = int((total - current) / speed) if speed else 0
        pct     = min(current / total * 100, 100) if total else 0

        # Update TaskRecord for /status panel
        record.update(
            done=current, total=total,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📤 Uploading",
        )
        if now - last_panel[0] >= 1.0:
            last_panel[0] = now
            runner._wake_panel(chat_id)

        # Direct status_msg edit — reference bot pattern, 3s throttle
        if status_msg is not None and now - last_edit[0] >= 3.0:
            last_edit[0] = now
            bar     = _bar(pct, 12)
            spd_s   = human_size(speed) + "/s"
            eta_s   = human_dur(eta) if eta > 0 else "—"
            el_s    = human_dur(int(elapsed))
            text = (
                f"📤 <b>UPLOADING</b>\n\n"
                f"<code>{(fname[:48] + '…') if len(fname) > 48 else fname}</code>\n\n"
                f"<code>[{bar}]</code>  <b>{pct:.1f}%</b>\n"
                f"──────────────────\n"
                f"{_spd_icon(speed)}  <b>Speed</b>    <code>{spd_s}</code>\n"
                f"⚙️  <b>Engine</b>   <code>Pyrofork</code>\n"
                f"⏳  <b>ETA</b>      <code>{eta_s}</code>\n"
                f"🕰  <b>Elapsed</b>  <code>{el_s}</code>\n"
                f"✅  <b>Done</b>     <code>{human_size(current)}</code>\n"
                f"📦  <b>Total</b>    <code>{human_size(total)}</code>"
            )
            try:
                await status_msg.edit(text, parse_mode=enums.ParseMode.HTML)
            except Exception:
                pass

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

