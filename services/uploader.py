"""
services/uploader.py  (COMBINED — best of all three)
Upload a local file to Telegram.

═══════════════════════════════════════════════════════════════════
What this file combines:

  From uploader.py (Optimized):
    • TaskRecord / task_runner integration for full state tracking
    • _wait_file_stable() guard (aria2 safety)
    • FloodWait cap at 120 s (matches client sleep_threshold)
    • ffmpeg → moviepy fallback for both thumb AND metadata
    • Multi-tag duration parsing (DURATION-eng, DURATION-jpn …)
    • Auto-thumbnail cleanup in finally block
    • cfg.log_channel forwarding
    • Actual upload speed logged after completion

  From telegram.py (colab_leecher):
    • User-facing Telegram status message progress bar (%, speed, ETA)
    • Photo file type support
    • is_last flag — "✅ Done · name" caption + status message delete
    • stream_upload / prefix / suffix caption options via BOT.Options
    • thumbMaintainer() / videoExtFix() used when available

  From uploader_1.py (Rewritten):
    • Clean _make_progress_bar() factory — no global state
    • task_record kept for API compat but optional
    • _chat_id() resilient helper
    • Multi-user support (not hardcoded to OWNER)

  Speed still comes from build_client():
    max_concurrent_transmissions=12  →  50-80 MB/s on Colab
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
    ".ts",  ".m2ts", ".wmv",  ".3gp", ".rmvb", ".mpg", ".mpeg",
}
_PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}


# ── Helpers ────────────────────────────────────────────────────

def _chat_id(msg) -> int:
    """Resolve chat_id from a Pyrogram Message object."""
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


def _get_caption(fname: str, caption: str, is_last: bool) -> str:
    """
    Build caption with optional BOT prefix/suffix and is_last marker.

    Falls back gracefully if BOT.Options is not available.
    """
    try:
        from colab_leecher.utility.variables import BOT
        prefix = getattr(BOT.Setting, "prefix", "") or ""
        suffix = getattr(BOT.Setting, "suffix", "") or ""
        tag    = getattr(BOT.Options, "caption", "b") or "b"
        name_part = f"{prefix} {fname} {suffix}".strip()
        label     = f"✅ Done · {name_part}" if is_last else name_part
        return f"<{tag}>{label}</{tag}>"
    except Exception:
        pass

    # Fallback: plain HTML caption
    if caption:
        return caption
    label = f"✅ Done · {fname}" if is_last else fname
    return f"<code>{label}</code>"


# ── File-stability guard ───────────────────────────────────────

async def _wait_file_stable(path: str, timeout: int = 30) -> bool:
    """
    Wait until the file is no longer growing (aria2 / partial download guard).
    Returns True when stable, False on timeout.
    """
    aria_file = path + ".aria2"
    prev_size = -1
    for _ in range(timeout):
        if os.path.exists(aria_file):
            await asyncio.sleep(1)
            continue
        try:
            curr_size = os.path.getsize(path)
        except OSError:
            await asyncio.sleep(1)
            continue
        if curr_size == prev_size and curr_size > 0:
            return True
        prev_size = curr_size
        await asyncio.sleep(1)
    return False


# ── Thumbnail extraction ───────────────────────────────────────

async def _extract_thumb_ffmpeg(path: str, out_path: str) -> bool:
    """Extract thumbnail via ffmpeg (fast path). Returns True on success."""
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
                    for k in ("DURATION", "duration", "DURATION-eng", "DURATION-jpn"):
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
    """Fallback thumbnail extraction via moviepy."""
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


async def _resolve_thumb(path: str, thumb: str | None, file_type: str) -> tuple[str | None, str | None]:
    """
    Resolve the thumbnail path for a file.

    Returns (thumb_path, auto_thumb_path).
    auto_thumb_path is set only if we created a temp file that needs cleanup.
    Falls back to thumbMaintainer() from colab_leecher if available.
    """
    if thumb:
        return thumb, None

    # Try colab_leecher's thumbMaintainer first
    try:
        from colab_leecher.utility.helper import thumbMaintainer
        from colab_leecher.utility.variables import Paths
        thmb_path, _ = thumbMaintainer(path)
        if thmb_path and os.path.exists(thmb_path):
            return thmb_path, None
        # For non-video, fall back to a static thumb if it exists
        if file_type != "video" and os.path.exists(Paths.THMB_PATH):
            return Paths.THMB_PATH, None
    except Exception:
        pass

    # ffmpeg → moviepy fallback
    if file_type == "video":
        thumb_path = path + "_thumb.jpg"
        ok = await _extract_thumb_ffmpeg(path, thumb_path)
        if not ok:
            ok = await _extract_thumb_moviepy(path, thumb_path)
        if ok:
            return thumb_path, thumb_path   # second value = auto-generated, needs cleanup

    return None, None


# ── Video metadata ─────────────────────────────────────────────

async def _get_video_meta(path: str) -> dict:
    """
    Single ffprobe call for duration / width / height.
    Falls back to moviepy if ffprobe misses anything.
    """
    await _wait_file_stable(path)
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
                meta["width"]  = int(s.get("width",  0) or 0)
                meta["height"] = int(s.get("height", 0) or 0)
                try:
                    meta["duration"] = int(float(s.get("duration", 0) or 0))
                except Exception:
                    pass
                if not meta["duration"]:
                    for k in ("DURATION", "duration", "DURATION-eng", "DURATION-jpn"):
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

    # Fallback to moviepy
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


# ── Progress callback ──────────────────────────────────────────

def _make_progress_callback(
    file_size:   int,
    start_time:  float,
    status_msg,              # Pyrogram Message to edit with live progress
    record       = None,     # Optional TaskRecord for panel integration
    chat_id:     int = 0,
    runner       = None,     # Optional task runner for _wake_panel
):
    """
    Returns an async progress callback that:
      • Updates a Telegram status message (telegram.py style) with %, speed, ETA
      • Updates the TaskRecord / panel system (uploader.py style) if provided
      • Throttled to one update every 0.3 s to avoid FloodWait
    """
    last = [start_time]

    async def _progress(current: int, total: int) -> None:
        now = time.time()
        if now - last[0] < 0.3:
            return
        last[0] = now

        elapsed = now - start_time
        speed   = current / elapsed if elapsed > 0 else 0
        eta     = int((total - current) / speed) if speed > 0 else 0

        # ── User-facing Telegram status bar ───────────────────
        if status_msg is not None:
            try:
                pct   = current / total * 100 if total else 0
                done  = human_size(current)
                total_h = human_size(total)
                spd_h = human_size(speed)
                # Build a compact progress bar: [████░░░░░░] 42%
                filled = int(pct / 10)
                bar    = "█" * filled + "░" * (10 - filled)
                text = (
                    f"📤 <b>Uploading…</b>\n"
                    f"[{bar}] <b>{pct:.1f}%</b>\n"
                    f"<b>Speed:</b> {spd_h}/s  |  <b>ETA:</b> {eta}s\n"
                    f"<b>Done:</b> {done} / {total_h}"
                )
                await safe_edit(status_msg, text, parse_mode=enums.ParseMode.HTML)
            except Exception:
                pass

        # ── TaskRecord / panel update ──────────────────────────
        if record is not None:
            record.update(
                done=current, total=total,
                speed=speed, eta=eta, elapsed=elapsed,
                state="📤 Uploading",
            )
            if runner is not None and chat_id:
                try:
                    runner._wake_panel(chat_id)
                except Exception:
                    pass

    return _progress


# ═══════════════════════════════════════════════════════════════
# Main upload function
# ═══════════════════════════════════════════════════════════════

async def upload_file(
    client:         Client,
    msg,                        # Pyrogram Message — used as status display + chat resolver
    path:           str,
    caption:        str   = "",
    thumb:          str | None = None,
    force_document: bool  = False,
    task_record           = None,   # Optional TaskRecord from task_runner
    is_last:        bool  = False,  # Mark final file in a batch (✅ Done caption)
) -> None:
    """
    Upload one file to the chat.

    Combines:
      • Full task_runner / panel integration  (uploader.py)
      • Live Telegram progress bar            (telegram.py)
      • Photo support                         (telegram.py)
      • is_last batch marker                  (telegram.py)
      • File stability guard                  (uploader.py)
      • ffmpeg → moviepy thumb + meta         (all three)
      • Multi-user support                    (uploader_1.py)
      • cfg.log_channel forwarding            (uploader.py)
      • FloodWait cap 120 s                   (uploader.py)
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

    # ── Determine file type ────────────────────────────────────
    if force_document:
        file_type = "document"
    elif ext in _PHOTO_EXTS:
        file_type = "photo"
    elif ext in _AUDIO_EXTS:
        file_type = "audio"
    elif ext in _VIDEO_EXTS:
        # Respect stream_upload option from colab_leecher if present
        try:
            from colab_leecher.utility.variables import BOT
            file_type = "video" if getattr(BOT.Options, "stream_upload", True) else "document"
        except Exception:
            file_type = "video"
    else:
        file_type = "document"

    # ── Caption ────────────────────────────────────────────────
    final_caption = _get_caption(fname, caption, is_last)

    # ── Video: fix extension + fetch metadata ──────────────────
    vid_meta: dict = {"duration": 0, "width": 0, "height": 0}
    if file_type in ("video", "document") and ext in _VIDEO_EXTS:
        # videoExtFix from colab_leecher remuxes if needed
        if file_type == "document":
            try:
                from colab_leecher.utility.helper import videoExtFix
                path = videoExtFix(path)
            except Exception:
                pass

        vid_meta = await _get_video_meta(path)

    # ── Thumbnail ──────────────────────────────────────────────
    thumb, auto_thumb = await _resolve_thumb(path, thumb, file_type)

    log.info(
        "Upload starting: %s  type=%s  size=%s  thumb=%s",
        fname, file_type, human_size(file_size), "yes" if thumb else "no",
    )
    if file_type == "video":
        log.info(
            "Video meta: duration=%ds  %dx%d",
            vid_meta["duration"], vid_meta["width"], vid_meta["height"],
        )

    # ── Task tracking (optional) ───────────────────────────────
    try:
        from services.task_runner import tracker, TaskRecord, runner as _runner
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
    except Exception:
        record  = None
        _runner = None

    # ── Progress callback ──────────────────────────────────────
    start_time = time.time()
    _progress  = _make_progress_callback(
        file_size  = file_size,
        start_time = start_time,
        status_msg = msg,
        record     = record,
        chat_id    = chat_id,
        runner     = _runner if record else None,
    )

    # ── Send helper ────────────────────────────────────────────
    async def _send() -> None:
        common = dict(
            caption    = final_caption,
            thumb      = thumb,
            parse_mode = enums.ParseMode.HTML,
            progress   = _progress,
        )

        if file_type == "video":
            sent = await client.send_video(
                chat_id, path,
                supports_streaming = True,
                width              = vid_meta["width"],
                height             = vid_meta["height"],
                duration           = vid_meta["duration"],
                **common,
            )
        elif file_type == "audio":
            sent = await client.send_audio(chat_id, path, **common)

        elif file_type == "photo":
            # Photos don't support thumb
            sent = await client.send_photo(
                chat_id, path,
                caption    = final_caption,
                parse_mode = enums.ParseMode.HTML,
                progress   = _progress,
            )

        else:  # document
            sent = await client.send_document(
                chat_id, path,
                force_document = True,
                **common,
            )

        # ── Post-send housekeeping ─────────────────────────────
        # Delete status / progress message
        # On is_last, telegram.py also does this — now unified
        try:
            await msg.delete()
        except Exception:
            pass

        # Forward to log channel
        if cfg.log_channel and sent:
            try:
                await sent.forward(cfg.log_channel)
            except Exception:
                pass

        # Update task record to done
        if record is not None:
            record.update(state="✅ Done", done=file_size, total=file_size)
            if _runner is not None:
                try:
                    _runner._wake_panel(chat_id)
                except Exception:
                    pass

        # Log actual upload speed
        elapsed = time.time() - start_time
        if elapsed > 0:
            log.info(
                "📤 Upload complete: %s  %s  speed=%s/s  time=%.1fs",
                fname, human_size(file_size),
                human_size(file_size / elapsed), elapsed,
            )

    # ── Execute with FloodWait handling ───────────────────────
    try:
        await _send()

    except FloodWait as fw:
        # Cap at 120 s (matches client sleep_threshold in build_client)
        if fw.value <= 120:
            log.warning("FloodWait %ds — waiting", fw.value)
            if record is not None:
                record.update(state=f"⏳ FloodWait {fw.value}s")
            await asyncio.sleep(fw.value)
            await _send()
        else:
            log.error("FloodWait too long (%ds) — giving up", fw.value)
            raise

    except Exception as exc:
        err = str(exc)
        if "MESSAGE_NOT_MODIFIED" not in err:
            log.error("Upload error: %s", exc)
            if record is not None:
                record.update(state=f"❌ {err[:60]}")
                if _runner is not None:
                    try:
                        _runner._wake_panel(chat_id)
                    except Exception:
                        pass
            await safe_edit(
                msg,
                f"❌ Upload failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        raise

    finally:
        # Clean up any auto-generated thumbnail temp file
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
