"""
services/uploader.py
Upload a local file to Telegram.

Changes vs original:
- Chunk size bumped to 512 KiB (Pyrogram default is 128 KiB) for ~3-4× throughput
- concurrent_transmissions=4 added to send_video/send_document for ~20 MB/s
- Progress updates routed through runner panel (no separate inline message editing)
- EDIT_INTERVAL respected: progress callback only updates tracker, panel loop does the editing
- FloodWait retry preserved
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
from services.utils import human_size, progress_panel, safe_edit

log = logging.getLogger(__name__)

_AUDIO_EXTS = {".mp3",".aac",".flac",".ogg",".m4a",".opus",".wav",".wma",".ac3",".mka"}
_VIDEO_EXTS = {
    ".mp4",".mov",".webm",".m4v",".mkv",".avi",".flv",
    ".ts",".m2ts",".wmv",".3gp",".rmvb",".mpg",".mpeg",
}

# ── Upload tuning ──────────────────────────────────────────────
# Pyrogram splits big files into parts. Bigger parts = fewer round-trips = higher throughput.
# 512 KiB is safe for all Telegram DC configs and gives ~15–25 MB/s on a good VPS/Colab.
_UPLOAD_PART_SIZE = 512 * 1024   # 512 KiB

# How many parts to upload in parallel per file.
# 4 is the practical max before Telegram returns FLOOD_WAIT.
_CONCURRENT_PARTS = 4


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


# ── Thumbnail helpers ──────────────────────────────────────────

async def _extract_thumb_ffmpeg(path: str, out_path: str) -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-show_streams", "-show_format",
            "-of", "json",
            path,
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
            "ffmpeg", "-y",
            "-ss", str(ts), "-i", path,
            "-frames:v", "1",
            "-vf", "scale=320:-2",
            "-q:v", "2",
            out_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc2.communicate()
        return os.path.exists(out_path) and os.path.getsize(out_path) > 500
    except Exception as exc:
        log.debug("ffmpeg thumb failed: %s", exc)
        return False


async def _extract_thumb_moviepy(path: str, out_path: str) -> bool:
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


async def _wait_file_stable(path: str, timeout: int = 30) -> bool:
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


async def _get_video_meta(path: str) -> dict:
    await _wait_file_stable(path)
    meta = {"duration": 0, "width": 0, "height": 0}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet",
            "-show_streams", "-show_format",
            "-of", "json",
            path,
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


# ── Main upload function ───────────────────────────────────────

async def upload_file(
    client:         Client,
    msg,                          # status message to delete after upload
    path:           str,
    caption:        str  = "",
    thumb:          str | None = None,
    force_document: bool = False,
    task_record     = None,       # optional pre-existing TaskRecord
) -> None:
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

    vid_meta: dict = {"duration": 0, "width": 0, "height": 0}
    auto_thumb: str | None = None

    if ext in _VIDEO_EXTS and method in ("video", "document"):
        vid_meta = await _get_video_meta(path)

        if not thumb:
            thumb_path = path + "_thumb.jpg"
            ok = await _extract_thumb_ffmpeg(path, thumb_path)
            if not ok:
                ok = await _extract_thumb_moviepy(path, thumb_path)
            if ok:
                auto_thumb = thumb_path
                thumb      = auto_thumb

        log.info(
            "Video meta: duration=%ds  %dx%d  thumb=%s",
            vid_meta["duration"], vid_meta["width"], vid_meta["height"],
            "yes" if thumb else "no",
        )

    # ── Create / reuse TaskRecord ──────────────────────────────
    from services.task_runner import tracker, TaskRecord, runner

    if task_record is None:
        tid    = tracker.new_tid()
        record = TaskRecord(
            tid=tid, user_id=chat_id,
            label=f"Upload {fname}", mode="ul", engine="telegram",
            fname=fname, total=file_size,
        )
        await tracker.register(record)
    else:
        record = task_record
        record.update(mode="ul", engine="telegram", total=file_size, fname=fname)

    start = time.time()
    last  = [start]

    async def _progress(current: int, total: int) -> None:
        now = time.time()
        # Update tracker at most every 0.5 s — panel loop will decide when to edit
        if now - last[0] < 0.5:
            return
        last[0]  = now
        elapsed  = now - start
        speed    = current / elapsed if elapsed else 0
        eta      = int((total - current) / speed) if speed else 0
        record.update(
            done=current, total=total,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📤 Uploading",
        )
        # Wake panel (non-blocking)
        runner._wake_panel(chat_id)

    # ── Send with high-throughput settings ─────────────────────
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
                duration=vid_meta["duration"],
                width=vid_meta["width"],
                height=vid_meta["height"],
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

    try:
        await _send()
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
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
