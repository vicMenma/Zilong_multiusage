"""
services/uploader.py
Upload a local file to Telegram.

SPEED FIXES APPLIED:
  1. video_meta() runs in parallel with upload start (no more 15-45s blocking)
  2. User thumbnail cached to disk per-session (no re-download per file)
  3. Auto-forward runs as fire-and-forget task (no serial blocking)
  4. Thumbnail extraction reduced to 2 attempts max
  5. Progress callback kept at 3.0s — PATCH: raised from 1.0s to cut
     API calls during upload (each msg.edit is a round-trip that competes
     with MTProto upload streams and can trigger FloodWait stalls).
  6. PATCH: skip progress callbacks entirely for files < 50 MB — they
     finish before the first edit would fire anyway.
  7. PATCH: ffprobe head-start reduced 2.0s → 0.3s so MTProto connections
     open immediately; ffprobe keeps running in the background.
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

# ── User thumbnail cache (avoids re-downloading same thumb per file) ──────────
_thumb_cache: dict[int, str] = {}


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


def _apply_caption_style(fname: str, style: str) -> str:
    """Wrap filename in the user's chosen Telegram HTML style."""
    s = style or "Monospace"
    if s == "Monospace":   return f"<code>{fname}</code>"
    if s == "Bold":        return f"<b>{fname}</b>"
    if s == "Italic":      return f"<i>{fname}</i>"
    if s == "Bold Italic": return f"<b><i>{fname}</i></b>"
    return fname   # Plain


async def _get_cached_user_thumb(chat_id: int) -> str | None:
    """Return cached user thumbnail path, or download + cache it once."""
    cached = _thumb_cache.get(chat_id)
    if cached and os.path.isfile(cached) and os.path.getsize(cached) > 0:
        return cached

    try:
        from core.session import settings as _settings, get_client as _get_client
        _s = await _settings.get(chat_id)
        _thumb_id = _s.get("thumb_id")
        if not _thumb_id:
            return None

        import tempfile
        _cl = _get_client()
        _tmp_thumb = tempfile.NamedTemporaryFile(
            suffix=".jpg", dir=cfg.download_dir, delete=False
        )
        _tmp_thumb.close()
        await _cl.download_media(_thumb_id, file_name=_tmp_thumb.name)
        if os.path.isfile(_tmp_thumb.name) and os.path.getsize(_tmp_thumb.name) > 0:
            _thumb_cache[chat_id] = _tmp_thumb.name
            log.info("User thumbnail cached for uid=%d: %s", chat_id, _thumb_id[:16])
            return _tmp_thumb.name
        else:
            os.remove(_tmp_thumb.name)
    except Exception as _te:
        log.warning("Could not load user thumbnail: %s", _te)
    return None


async def _fire_and_forget_forward(sent, chat_id: int, channels: list, fname: str) -> None:
    """Auto-forward to channels without blocking the upload pipeline."""
    try:
        from core.session import get_client as _gc
        _errors: list[str] = []
        for ch in channels:
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
    except Exception as exc:
        log.warning("Auto-forward task failed: %s", exc)


async def upload_file(
    client:         Client,
    msg,
    path:           str,
    caption:        str  = "",
    thumb:          str | None = None,
    force_document: bool = False,
    task_record     = None,
    status_msg      = None,
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
        from core.session import settings as _settings
        s     = await _settings.get(chat_id) if chat_id else {}
        style = s.get("caption_style", "Monospace")
        caption = _apply_caption_style(fname, style)

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

    # ── Load user's saved thumbnail (CACHED — not re-downloaded per file) ─
    if not thumb:
        cached_thumb = await _get_cached_user_thumb(chat_id)
        if cached_thumb:
            thumb = cached_thumb

    # ── Parallel metadata probe ────────────────────────────────────────────
    meta_task: asyncio.Task | None = None

    if ext in _VIDEO_EXTS and method in ("video", "document"):
        if task_record is not None:
            task_record.update(state="🔍 Analyzing…", fname=fname)
        try:
            from services.task_runner import runner as _runner
            _runner._wake_panel(chat_id, immediate=True)
        except Exception:
            pass

        async def _probe_meta():
            try:
                from services.ffmpeg import video_meta
                return await video_meta(path)
            except Exception as exc:
                log.warning("video_meta failed for %s: %s", fname, exc)
                return {"duration": 0, "width": 0, "height": 0, "thumb": None}

        meta_task = asyncio.create_task(_probe_meta())

        # PATCH: reduced head-start 2.0s → 0.3s — MTProto connections open
        # immediately; ffprobe keeps running in parallel behind the scenes.
        try:
            vid_meta = await asyncio.wait_for(asyncio.shield(meta_task), timeout=0.3)
            meta_task = None
        except asyncio.TimeoutError:
            log.info("video_meta still probing after 0.3s — uploading with placeholder metadata")

        if not thumb:
            t = vid_meta.get("thumb")
            if t and os.path.isfile(t):
                thumb = t
                auto_thumb = t

        log.info(
            "Video meta (initial): duration=%ds  %dx%d  thumb=%s",
            vid_meta.get("duration", 0),
            vid_meta.get("width", 0),
            vid_meta.get("height", 0),
            "yes" if thumb else "pending",
        )

    # ── TaskRecord ────────────────────────────────────────────────────────
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

    # PATCH: raised from 1.0s → 3.0s — fewer msg.edit() calls means fewer
    # Telegram API round-trips competing with MTProto upload streams.
    # A FloodWait triggered during upload pauses the *entire* session.
    _PROGRESS_INTERVAL = 3.0

    # PATCH: files under 50 MB finish in < 5s — progress callbacks add
    # pure overhead with zero visible benefit for the user.
    _SMALL_FILE = 50 * 1024 * 1024  # 50 MB

    async def _progress(current: int, total: int) -> None:
        if file_size < _SMALL_FILE:
            return  # skip entirely — file done before first edit anyway
        now = time.time()
        if now - last_panel[0] < _PROGRESS_INTERVAL:
            return
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

    _sent_msg = [None]

    async def _send() -> None:
        nonlocal vid_meta, thumb, auto_thumb
        if meta_task is not None and not meta_task.done():
            try:
                vid_meta = await asyncio.wait_for(asyncio.shield(meta_task), timeout=1.0)
            except asyncio.TimeoutError:
                pass
        elif meta_task is not None and meta_task.done():
            try:
                vid_meta = meta_task.result()
            except Exception:
                pass

        if not thumb and vid_meta.get("thumb") and os.path.isfile(vid_meta["thumb"]):
            thumb = vid_meta["thumb"]
            auto_thumb = vid_meta["thumb"]

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

    from services.task_runner import runner as _runner_ul

    try:
        record.update(state="📤 Uploading")
        _runner_ul._wake_panel(chat_id, immediate=True)
        await _send()

        sent = _sent_msg[0]
        if sent:
            try:
                from core.session import settings as _st, get_client as _gc
                from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                _s        = await _st.get(chat_id)
                _channels = _s.get("forward_channels", [])
                _auto     = _s.get("auto_forward", False)

                if _channels:
                    if _auto:
                        asyncio.create_task(
                            _fire_and_forget_forward(sent, chat_id, _channels, fname)
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
                            f"📨 <b>Forward this file?</b>\n"
                            f"<code>{fname}</code>",
                            parse_mode=enums.ParseMode.HTML,
                            reply_markup=InlineKeyboardMarkup(rows),
                        )
            except Exception as _fwe:
                log.warning("Forward prompt failed: %s", _fwe)

    except FloodWait as fw:
        if fw.value <= 60:
            log.warning("FloodWait %ds — waiting", fw.value)
            record.update(state=f"⏳ FloodWait {fw.value}s")
            await asyncio.sleep(fw.value)
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
        if meta_task is not None and not meta_task.done():
            meta_task.cancel()
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
