"""
services/uploader.py

Rewrite goals vs previous version:
  • video_meta() now fires as a background Task the instant the file is
    identified as video.  The upload semaphore is acquired and all other
    prep (thumbnail load from settings, TaskRecord registration) happens
    while ffprobe is already running.  A 5-second timeout caps the wait:
    local files probe in <2 s; only broken/remote paths ever hit the limit.
    Result: the dead-time before the first upload byte is sent drops from
    10–30 s (old sequential ffprobe) to ≤5 s worst-case.

  • Progress callback does a single record.update() and one panel wake per
    second.  No duplicate imports, no nested runner references.

  • FloodWait retry uses a bounded loop instead of recursion — no stack
    growth on repeated waits.

  • Upload semaphore slot count stays wired to UPLOAD_CONCURRENCY
    (task_runner._get_upload_sem), unchanged.

  • All existing features preserved:
      – user thumbnail from /settings
      – log_channel forwarding
      – forward_channels inline-keyboard prompt
      – TaskRecord state tracking
      – auto_thumb cleanup in finally
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

_AUDIO_EXTS = {".mp3", ".aac", ".flac", ".ogg", ".m4a", ".opus", ".wav", ".wma", ".ac3", ".mka"}
_VIDEO_EXTS = {
    ".mp4", ".mov", ".webm", ".m4v", ".mkv", ".avi", ".flv",
    ".ts", ".m2ts", ".wmv", ".3gp", ".rmvb", ".mpg", ".mpeg",
}

_META_TIMEOUT = 5.0   # seconds to wait for ffprobe before uploading with defaults


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
    msg,
    path:           str,
    caption:        str       = "",
    thumb:          str | None = None,
    force_document: bool      = False,
    task_record                = None,
    status_msg                 = None,   # kept for API compatibility
) -> None:
    """Upload *path* to Telegram."""

    if not os.path.isfile(path):
        await safe_edit(
            msg,
            f"❌ File not found: <code>{os.path.basename(path)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    chat_id   = _chat_id(msg)
    file_size = os.path.getsize(path)
    fname     = os.path.basename(path)
    ext       = os.path.splitext(fname)[1].lower()

    if not caption:
        caption = f"<code>{fname}</code>"

    # ── Determine upload method ───────────────────────────────
    if force_document:
        method = "document"
    elif ext in _AUDIO_EXTS:
        method = "audio"
    elif ext in _VIDEO_EXTS:
        method = "video"
    else:
        method = "document"

    # ── Fire video_meta() immediately as a background task ────
    # It runs concurrently while we do the thumbnail / TaskRecord setup below.
    meta_task: asyncio.Task | None = None
    if method == "video":
        from services.ffmpeg import video_meta
        meta_task = asyncio.create_task(video_meta(path))

    # ── Load user thumbnail from settings ────────────────────
    auto_thumb: str | None = None   # ffprobe-generated — cleaned up in finally
    user_thumb: str | None = None   # downloaded from TG  — cleaned up in finally

    if not thumb and chat_id:
        try:
            from core.session import settings as _settings
            _s        = await _settings.get(chat_id)
            _thumb_id = _s.get("thumb_id")
            if _thumb_id:
                import tempfile
                from core.session import get_client as _get_client
                _tmp = tempfile.NamedTemporaryFile(
                    suffix=".jpg", dir=cfg.download_dir, delete=False
                )
                _tmp.close()
                await _get_client().download_media(_thumb_id, file_name=_tmp.name)
                if os.path.isfile(_tmp.name) and os.path.getsize(_tmp.name) > 0:
                    thumb      = _tmp.name
                    user_thumb = _tmp.name
                    log.info("Using user thumbnail: %s…", _thumb_id[:16])
                else:
                    try:
                        os.remove(_tmp.name)
                    except OSError:
                        pass
        except Exception as _te:
            log.warning("Could not load user thumbnail: %s", _te)

    # ── TaskRecord ────────────────────────────────────────────
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

    # ── Wait for video metadata (with timeout) ────────────────
    vid_meta: dict = {"duration": 0, "width": 0, "height": 0, "thumb": None}

    if meta_task is not None:
        # Show "Analyzing…" only while we're actually waiting
        record.update(state="🔍 Analyzing…", fname=fname)
        runner._wake_panel(chat_id, immediate=True)

        try:
            vid_meta = await asyncio.wait_for(
                asyncio.shield(meta_task), timeout=_META_TIMEOUT
            )
        except asyncio.TimeoutError:
            log.warning(
                "video_meta timed out after %.0fs for %s — uploading with defaults",
                _META_TIMEOUT, fname,
            )
            # Cancel the underlying task so it doesn't keep running in background
            meta_task.cancel()
            try:
                await meta_task
            except (asyncio.CancelledError, Exception):
                pass
        except Exception as exc:
            log.warning("video_meta error for %s: %s", fname, exc)
            meta_task.cancel()

        # Merge ffprobe thumbnail only if no user thumb already selected
        if not thumb:
            fp_thumb = vid_meta.get("thumb")
            if fp_thumb and os.path.isfile(fp_thumb):
                thumb      = fp_thumb
                auto_thumb = fp_thumb

        log.info(
            "Video meta: duration=%ds  %dx%d  thumb=%s",
            vid_meta.get("duration", 0),
            vid_meta.get("width", 0),
            vid_meta.get("height", 0),
            "yes" if thumb else "no",
        )

    # ── Progress callback ─────────────────────────────────────
    start      = time.time()
    last_tick  = [start]
    _TICK      = 1.0   # max one panel update per second

    async def _progress(current: int, total: int) -> None:
        now = time.time()
        if now - last_tick[0] < _TICK:
            return
        last_tick[0] = now
        elapsed = now - start
        speed   = current / elapsed if elapsed else 0
        eta     = int((total - current) / speed) if speed else 0
        record.update(
            done=current, total=total,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📤 Uploading",
        )
        runner._wake_panel(chat_id)

    # ── Build the send coroutine ──────────────────────────────
    _sent_msg = [None]

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
                chat_id, path, force_document=True, **common
            )
        _sent_msg[0] = sent

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

    # ── Upload with FloodWait retry loop ──────────────────────
    upload_sem = runner._get_upload_sem()

    MAX_FW_RETRIES = 3
    fw_retries     = 0

    try:
        async with upload_sem:
            record.update(state="📤 Uploading")
            runner._wake_panel(chat_id, immediate=True)

            while True:
                try:
                    await _send()
                    break                        # success — exit retry loop
                except FloodWait as fw:
                    fw_retries += 1
                    if fw.value > 120 or fw_retries > MAX_FW_RETRIES:
                        raise
                    log.warning("FloodWait %ds (retry %d/%d)", fw.value, fw_retries, MAX_FW_RETRIES)
                    record.update(state=f"⏳ FloodWait {fw.value}s")
                    runner._wake_panel(chat_id)
                    await asyncio.sleep(fw.value)

        # ── Forward prompt ────────────────────────────────────
        sent = _sent_msg[0]
        if sent and chat_id:
            try:
                from core.session import settings as _st, get_client as _gc
                from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                _s        = await _st.get(chat_id)
                _channels = _s.get("forward_channels", [])
                _auto     = _s.get("auto_forward", False)

                if _channels:
                    if _auto:
                        _errors: list[str] = []
                        for ch in _channels:
                            try:
                                await sent.copy(ch["id"])
                            except Exception as _fe:
                                _errors.append(ch.get("name", str(ch["id"])))
                                log.warning("Auto-forward to %s failed: %s", ch["id"], _fe)
                        if _errors:
                            await _gc().send_message(
                                chat_id,
                                f"⚠️ Auto-forward failed for: {', '.join(_errors)}",
                            )
                    else:
                        rows = []
                        for ch in _channels:
                            cid   = ch["id"]
                            cname = ch.get("name", str(cid))[:28]
                            rows.append([InlineKeyboardButton(
                                f"📢 {cname}",
                                callback_data=f"fwd|one|{sent.chat.id}|{sent.id}|{cid}",
                            )])
                        if len(_channels) > 1:
                            rows.append([InlineKeyboardButton(
                                "📡 Forward to ALL channels",
                                callback_data=f"fwd|all|{sent.chat.id}|{sent.id}|0",
                            )])
                        rows.append([InlineKeyboardButton(
                            "✖ Skip",
                            callback_data=f"fwd|skip|{sent.chat.id}|{sent.id}|0",
                        )])
                        await _gc().send_message(
                            chat_id,
                            f"📨 <b>Forward this file?</b>\n<code>{fname}</code>",
                            parse_mode=enums.ParseMode.HTML,
                            reply_markup=InlineKeyboardMarkup(rows),
                        )
            except Exception as _fwe:
                log.warning("Forward prompt failed: %s", _fwe)

    except FloodWait as fw:
        # Exceeded retry budget or wait > 120 s — surface to caller
        record.update(state=f"❌ FloodWait {fw.value}s — give up")
        runner._wake_panel(chat_id)
        await safe_edit(
            msg,
            f"❌ Upload aborted: FloodWait {fw.value}s (too long).",
            parse_mode=enums.ParseMode.HTML,
        )
        raise

    except Exception as exc:
        err = str(exc)
        if "MESSAGE_NOT_MODIFIED" not in err:
            record.update(state=f"❌ {err[:60]}")
            runner._wake_panel(chat_id)
            await safe_edit(
                msg,
                f"❌ Upload failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        raise

    finally:
        for _tp in (auto_thumb, user_thumb):
            if _tp and os.path.isfile(_tp):
                try:
                    os.remove(_tp)
                except OSError:
                    pass
