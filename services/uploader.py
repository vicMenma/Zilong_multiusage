"""
services/uploader.py
Upload a local file to Telegram.

Progress is now routed through the unified task panel (same as downloads):
  - Creates a TaskRecord(mode="ul", engine="telegram")
  - tracker.register() → auto_panel() → render_panel() handles all display
  - `msg` (the "📤 Uploading…" placeholder) is dismissed at the very start
    so the panel is the single source of truth for upload status
  - FloodWait handled with a retry loop (no recursion, no stack growth)
  - On error a fresh client.send_message() is used (msg is already gone)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

from pyrogram import Client, enums
from pyrogram.errors import FloodWait

from core.config import cfg
from services.utils import human_size, safe_edit

log = logging.getLogger(__name__)

_AUDIO_EXTS = {".mp3", ".aac", ".flac", ".ogg", ".m4a", ".opus",
               ".wav", ".wma", ".ac3", ".mka"}
_VIDEO_EXTS = {".mp4", ".mov", ".webm", ".m4v", ".mkv", ".avi", ".flv",
               ".ts",  ".m2ts", ".wmv",  ".3gp", ".rmvb", ".mpg", ".mpeg"}
_PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}


# ─────────────────────────────────────────────────────────────
# Tiny helpers
# ─────────────────────────────────────────────────────────────

def _chat_id(msg) -> int:
    try:
        return msg.chat.id
    except Exception:
        pass
    try:
        return msg.from_user.id
    except Exception:
        return 0


def _file_type(path: str, force_document: bool) -> str:
    if force_document:
        return "document"
    ext = os.path.splitext(path)[1].lower()
    if ext in _PHOTO_EXTS:
        return "photo"
    if ext in _AUDIO_EXTS:
        return "audio"
    if ext in _VIDEO_EXTS:
        return "video"
    return "document"


def _build_caption(fname: str, custom: str, is_last: bool) -> str:
    if custom:
        return custom
    label = f"✅ Done · {fname}" if is_last else fname
    return f"<code>{label}</code>"


# ─────────────────────────────────────────────────────────────
# Wait for aria2 to finish writing
# ─────────────────────────────────────────────────────────────

async def _wait_stable(path: str, timeout: int = 30) -> None:
    """
    Wait for aria2 to finish writing before uploading.

    Fast path: if no .aria2 sidecar exists the file is already complete
    (FFmpeg, yt-dlp, and direct downloads all close the file before we
    reach upload_file). Skip the stability loop entirely — saves ≥1s
    per upload call.

    Slow path: .aria2 exists → poll until it disappears (aria2 removes
    it when the download is complete), then do one size-stability check.
    """
    aria = path + ".aria2"
    if not os.path.exists(aria):
        return  # fast path: already complete, no wait needed

    # Slow path: aria2 is still writing
    for _ in range(timeout):
        if not os.path.exists(aria):
            # sidecar gone → wait one extra second for OS to flush
            await asyncio.sleep(1)
            return
        await asyncio.sleep(1)


# ─────────────────────────────────────────────────────────────
# Video metadata — single ffprobe call
# ─────────────────────────────────────────────────────────────

async def _video_meta(path: str) -> dict:
    meta = {"duration": 0, "width": 0, "height": 0}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-show_streams", "-show_format", "-of", "json", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        data = json.loads(out.decode(errors="replace") or "{}")

        for s in data.get("streams", []):
            if s.get("codec_type") != "video":
                continue
            meta["width"]  = int(s.get("width",  0) or 0)
            meta["height"] = int(s.get("height", 0) or 0)
            try:
                meta["duration"] = int(float(s.get("duration", 0) or 0))
            except Exception:
                pass
            # MKV/HEVC keep duration in tags
            if not meta["duration"]:
                for key in ("DURATION", "duration", "DURATION-eng", "DURATION-jpn"):
                    raw = (s.get("tags") or {}).get(key, "")
                    if raw and ":" in str(raw):
                        try:
                            p = str(raw).split(":")
                            meta["duration"] = (
                                int(float(p[0])) * 3600
                                + int(float(p[1])) * 60
                                + int(float(str(p[2]).split(".")[0]))
                            )
                        except Exception:
                            pass
                    if meta["duration"]:
                        break
            break  # only first video stream needed

        if not meta["duration"]:
            try:
                meta["duration"] = int(float(
                    data.get("format", {}).get("duration") or 0
                ))
            except Exception:
                pass

    except Exception as exc:
        log.debug("_video_meta: %s", exc)
    return meta


# ─────────────────────────────────────────────────────────────
# Thumbnail — ffmpeg seek to 20 % of duration
# ─────────────────────────────────────────────────────────────

async def _make_thumb(path: str, duration: int) -> tuple[str | None, bool]:
    """Returns (thumb_path, is_temp). Caller deletes if is_temp."""
    out = path + "_zt.jpg"
    ts  = max(1, int(duration * 0.2)) if duration > 5 else 1
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-ss", str(ts), "-i", path,
            "-frames:v", "1", "-vf", "scale=320:-2", "-q:v", "2",
            out,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.communicate()
        if os.path.exists(out) and os.path.getsize(out) > 500:
            return out, True
    except Exception as exc:
        log.debug("_make_thumb: %s", exc)
    return None, False


# ─────────────────────────────────────────────────────────────
# upload_file — the one function the rest of the repo calls
# ─────────────────────────────────────────────────────────────

async def upload_file(
    client:         Client,
    msg,
    path:           str,
    caption:        str        = "",
    thumb:          str | None = None,
    force_document: bool       = False,
    task_record                = None,   # kept for API compat — not used
    is_last:        bool       = False,
) -> None:
    """
    Upload one file through the unified progress panel.

    The panel (render_panel) shows upload progress identically to downloads:
      📤 Uploading  |  Name  |  Progress bar  |  Speed  |  ETA

    Flow:
      1. Dismiss the placeholder `msg` immediately (panel takes over)
      2. Register a TaskRecord(mode="ul") → triggers auto_panel()
      3. Pyrogram progress callback updates the record → panel refreshes
      4. On FloodWait: retry loop (no recursion)
      5. On error: send a fresh message to chat_id (msg is already deleted)
    """
    if not os.path.isfile(path):
        await safe_edit(
            msg,
            f"❌ File not found: <code>{os.path.basename(path)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    await _wait_stable(path)

    chat_id   = _chat_id(msg)
    fname     = os.path.basename(path)
    file_size = os.path.getsize(path)
    ftype     = _file_type(path, force_document)
    cap       = _build_caption(fname, caption, is_last)

    # ── Video: width / height / duration ──────────────────────
    meta       = {"duration": 0, "width": 0, "height": 0}
    auto_thumb: str | None = None

    if ftype == "video":
        meta = await _video_meta(path)

    # ── Thumbnail ──────────────────────────────────────────────
    if ftype in ("video", "document") and not thumb:
        t, is_temp = await _make_thumb(path, meta["duration"])
        if t:
            thumb = t
            if is_temp:
                auto_thumb = t

    # ── Dismiss placeholder — panel takes over display ─────────
    try:
        await msg.delete()
    except Exception:
        pass

    # ── Register upload in unified task tracker ────────────────
    from services.task_runner import tracker, TaskRecord, runner

    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=chat_id,
        label=fname, mode="ul", engine="telegram",
        fname=fname, total=file_size,
        state="📤 Uploading",
    )
    await tracker.register(record)

    task_start  = time.time()
    _last_prog  = [task_start]          # throttle: update at most every 0.5s
    _PROG_MIN   = 0.5                   # seconds between progress updates

    async def progress(current: int, total: int) -> None:
        now = time.time()
        if now - _last_prog[0] < _PROG_MIN:
            return
        _last_prog[0] = now
        elapsed = now - task_start
        speed   = current / elapsed if elapsed > 0 else 0
        eta     = int((total - current) / speed) if speed > 0 else 0
        record.update(
            done=current, total=total or file_size,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📤 Uploading",
        )
        runner._wake_panel(chat_id)

    log.info("⬆ %s  [%s]  %s  dur=%ds  %dx%d",
             fname, ftype, human_size(file_size),
             meta["duration"], meta["width"], meta["height"])

    # ── Send — retry loop for FloodWait (no recursion) ────────
    sent  = None
    error = None

    for attempt in range(4):
        try:
            common = dict(
                caption    = cap,
                thumb      = thumb,
                parse_mode = enums.ParseMode.HTML,
                progress   = progress,
            )

            if ftype == "video":
                sent = await client.send_video(
                    chat_id, path,
                    supports_streaming = True,
                    width              = meta["width"],
                    height             = meta["height"],
                    duration           = meta["duration"],
                    **common,
                )
            elif ftype == "audio":
                sent = await client.send_audio(chat_id, path, **common)
            elif ftype == "photo":
                sent = await client.send_photo(
                    chat_id, path,
                    caption    = cap,
                    parse_mode = enums.ParseMode.HTML,
                    progress   = progress,
                )
            else:
                sent = await client.send_document(
                    chat_id, path,
                    force_document = True,
                    **common,
                )

            error = None
            break  # success

        except FloodWait as fw:
            wait = min(fw.value + 5, 120)
            log.warning("FloodWait %ds on upload attempt %d — sleeping", wait, attempt + 1)
            await asyncio.sleep(wait)

        except Exception as exc:
            error = exc
            break

    # ── Post-send bookkeeping ──────────────────────────────────
    if error is not None:
        record.update(state="❌ Failed")
        runner._wake_panel(chat_id, immediate=True)
        if "MESSAGE_NOT_MODIFIED" not in str(error):
            log.error("Upload error %s: %s", fname, error)
            try:
                await client.send_message(
                    chat_id,
                    f"❌ Upload failed: <code>{error}</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception:
                pass
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
        raise error

    record.update(state="✅ Done", done=file_size, total=file_size)
    runner._wake_panel(chat_id, immediate=True)

    # Forward to log channel if configured
    if cfg.log_channel and sent:
        try:
            await sent.forward(cfg.log_channel)
        except Exception:
            pass

    elapsed = time.time() - task_start
    log.info("✅ Done: %s  %s/s  %.1fs",
             fname,
             human_size(file_size / elapsed) if elapsed else "—",
             elapsed)

    if auto_thumb and os.path.isfile(auto_thumb):
        try:
            os.remove(auto_thumb)
        except OSError:
            pass
