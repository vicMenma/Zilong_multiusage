"""
services/uploader.py (REWRITTEN)
Upload a local file to Telegram.

═══════════════════════════════════════════════════════════════════
Style: Matches zilong-leech's colab_leecher/uploader/telegram.py
  • Simple progress_bar() callback — no task_runner integration
  • Direct client.send_video / send_audio / send_document
  • FloodWait retry with recursive call (same pattern as leech bot)
  • Thumbnail extraction via ffmpeg → moviepy fallback
  • Actual upload speed logged after each file

The REAL speed comes from main.py's build_client():
  max_concurrent_transmissions=12 → 50-80 MB/s on Colab
This file just sends the file and tracks progress cleanly.
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
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


# ─────────────────────────────────────────────────────────────
# Thumbnail extraction (same as zilong-leech's thumbMaintainer)
# ─────────────────────────────────────────────────────────────

async def _extract_thumb(path: str, out_path: str) -> bool:
    """Extract thumbnail via ffmpeg. Returns True on success."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-show_streams", "-show_format", "-of", "json", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out_b, _ = await proc.communicate()
        data = json.loads(out_b.decode(errors="replace") or "{}")

        duration = 0
        for s in data.get("streams", []):
            if s.get("codec_type") == "video":
                try:
                    duration = int(float(s.get("duration", 0) or 0))
                except Exception:
                    pass
                if not duration:
                    for k in ("DURATION", "duration", "DURATION-eng"):
                        v = (s.get("tags") or {}).get(k, "")
                        if v and ":" in str(v):
                            try:
                                p = str(v).split(":")
                                duration = (int(float(p[0])) * 3600 +
                                            int(float(p[1])) * 60 +
                                            int(float(p[2].split(".")[0])))
                            except Exception:
                                pass
                        if duration:
                            break
                break

        if not duration:
            try:
                duration = int(float(data.get("format", {}).get("duration") or 0))
            except Exception:
                pass

        ts = max(1, int(duration * 0.2)) if duration > 5 else 1

        proc2 = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-ss", str(ts), "-i", path,
            "-frames:v", "1", "-vf", "scale=320:-2", "-q:v", "2", out_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc2.communicate()
        return os.path.exists(out_path) and os.path.getsize(out_path) > 500
    except Exception as exc:
        log.debug("ffmpeg thumb failed: %s", exc)
        return False


async def _extract_thumb_moviepy(path: str, out_path: str) -> bool:
    """Fallback thumbnail via moviepy."""
    try:
        loop = asyncio.get_event_loop()

        def _do():
            from moviepy.video.io.VideoFileClip import VideoFileClip
            with VideoFileClip(path) as clip:
                t = max(1, math.floor(clip.duration * 0.2)) if clip.duration > 5 else 1
                clip.save_frame(out_path, t=t)
            return os.path.exists(out_path) and os.path.getsize(out_path) > 500

        return await loop.run_in_executor(None, _do)
    except Exception as exc:
        log.debug("moviepy thumb failed: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────
# Video metadata (duration, width, height) — single ffprobe call
# ─────────────────────────────────────────────────────────────

async def _get_video_meta(path: str) -> dict:
    meta = {"duration": 0, "width": 0, "height": 0}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-show_streams", "-show_format", "-of", "json", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out_b, _ = await proc.communicate()
        data = json.loads(out_b.decode(errors="replace") or "{}")

        for s in data.get("streams", []):
            if s.get("codec_type") == "video":
                meta["width"]  = int(s.get("width", 0) or 0)
                meta["height"] = int(s.get("height", 0) or 0)
                try:
                    meta["duration"] = int(float(s.get("duration", 0) or 0))
                except Exception:
                    pass
                if not meta["duration"]:
                    for k in ("DURATION", "duration", "DURATION-eng"):
                        v = (s.get("tags") or {}).get(k, "")
                        if v and ":" in str(v):
                            try:
                                p = str(v).split(":")
                                meta["duration"] = (int(float(p[0])) * 3600 +
                                                    int(float(p[1])) * 60 +
                                                    int(float(p[2].split(".")[0])))
                            except Exception:
                                pass
                        if meta["duration"]:
                            break
                break

        if not meta["duration"]:
            fmt_dur = data.get("format", {}).get("duration")
            if fmt_dur:
                try:
                    meta["duration"] = int(float(fmt_dur))
                except Exception:
                    pass
    except Exception as exc:
        log.debug("ffprobe meta failed: %s", exc)

    # Fallback to moviepy if ffprobe missed something
    if not meta["duration"] or not meta["width"]:
        try:
            loop = asyncio.get_event_loop()

            def _mv():
                from moviepy.video.io.VideoFileClip import VideoFileClip
                with VideoFileClip(path) as clip:
                    return {
                        "duration": int(clip.duration or 0),
                        "width":    int(clip.size[0]) if clip.size else 0,
                        "height":   int(clip.size[1]) if clip.size else 0,
                    }

            mv = await loop.run_in_executor(None, _mv)
            if not meta["duration"]: meta["duration"] = mv["duration"]
            if not meta["width"]:    meta["width"]    = mv["width"]
            if not meta["height"]:   meta["height"]   = mv["height"]
        except Exception as exc:
            log.debug("moviepy meta failed: %s", exc)

    return meta


# ─────────────────────────────────────────────────────────────
# Simple progress bar — same style as zilong-leech
# ─────────────────────────────────────────────────────────────

def _make_progress_bar(file_size: int, start_time: float):
    """
    Returns a simple progress callback matching zilong-leech's style.
    Logs speed — no task_runner, no panel editing overhead.
    """
    last = [start_time]

    async def progress_bar(current: int, total: int) -> None:
        now = time.time()
        if now - last[0] < 0.3:
            return
        last[0] = now
        elapsed = now - start_time
        if current > 0 and elapsed > 0:
            speed = current / elapsed
            eta   = (total - current) / speed if speed else 0
            log.debug(
                "📤 %s/%s  %s/s  ETA %ds",
                human_size(current), human_size(total),
                human_size(speed), int(eta),
            )

    return progress_bar


# ═══════════════════════════════════════════════════════════════
# Main upload function — SIMPLE, DIRECT (zilong-leech style)
# ═══════════════════════════════════════════════════════════════

async def upload_file(
    client:         Client,
    msg,
    path:           str,
    caption:        str  = "",
    thumb:          str | None = None,
    force_document: bool = False,
    task_record     = None,   # kept for API compat, not used
) -> None:
    """
    Upload one file to the chat.

    Simple and direct — matches zilong-leech's upload_file() pattern:
      1. Detect file type (video / audio / document)
      2. Extract thumbnail if video
      3. Send with progress callback
      4. Delete status message
      5. FloodWait retry
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

    # ── Determine send method ─────────────────────────────────
    if force_document:
        method = "document"
    elif ext in _AUDIO_EXTS:
        method = "audio"
    elif ext in _VIDEO_EXTS:
        method = "video"
    else:
        method = "document"

    # ── Video metadata + thumbnail ────────────────────────────
    vid_meta:   dict       = {"duration": 0, "width": 0, "height": 0}
    auto_thumb: str | None = None

    if ext in _VIDEO_EXTS and method in ("video", "document"):
        vid_meta = await _get_video_meta(path)

        if not thumb:
            thumb_path = path + "_thumb.jpg"
            ok = await _extract_thumb(path, thumb_path)
            if not ok:
                ok = await _extract_thumb_moviepy(path, thumb_path)
            if ok:
                auto_thumb = thumb_path
                thumb      = auto_thumb

    # ── Progress callback ─────────────────────────────────────
    start_time   = time.time()
    progress_bar = _make_progress_bar(file_size, start_time)

    # ── Send ──────────────────────────────────────────────────
    try:
        if method == "video":
            sent = await client.send_video(
                chat_id=chat_id,
                video=path,
                supports_streaming=True,
                width=vid_meta["width"],
                height=vid_meta["height"],
                caption=caption,
                thumb=thumb,
                duration=vid_meta["duration"],
                parse_mode=enums.ParseMode.HTML,
                progress=progress_bar,
            )

        elif method == "audio":
            sent = await client.send_audio(
                chat_id=chat_id,
                audio=path,
                caption=caption,
                thumb=thumb,
                parse_mode=enums.ParseMode.HTML,
                progress=progress_bar,
            )

        else:  # document
            sent = await client.send_document(
                chat_id=chat_id,
                document=path,
                caption=caption,
                thumb=thumb,
                force_document=True,
                parse_mode=enums.ParseMode.HTML,
                progress=progress_bar,
            )

        # ── Post-upload ───────────────────────────────────────
        # Delete the status/progress message
        try:
            await msg.delete()
        except Exception:
            pass

        # Forward to log channel if configured
        if cfg.log_channel and sent:
            try:
                await sent.forward(cfg.log_channel)
            except Exception:
                pass

        # ── Log actual speed ──────────────────────────────────
        elapsed = time.time() - start_time
        if elapsed > 0:
            actual_speed = file_size / elapsed
            log.info(
                "📤 Upload complete: %s  %s  speed=%s/s  time=%.1fs",
                fname, human_size(file_size),
                human_size(actual_speed), elapsed,
            )

    except FloodWait as e:
        # Same retry pattern as zilong-leech
        logging.warning(f"FloodWait {e.value}s")
        await asyncio.sleep(e.value)
        await upload_file(client, msg, path, caption, thumb, force_document, task_record)

    except Exception as e:
        err = str(e)
        if "MESSAGE_NOT_MODIFIED" not in err:
            logging.error(f"Upload error: {e}")
            await safe_edit(msg,
                f"❌ Upload failed: <code>{e}</code>",
                parse_mode=enums.ParseMode.HTML)

    finally:
        # Clean up auto-generated thumbnail
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
