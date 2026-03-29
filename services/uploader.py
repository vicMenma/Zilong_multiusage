"""
services/uploader.py
Upload a local file to Telegram.

Style mirrors telegram.py exactly:
  - progress() defined inline with %, speed, ETA bar
  - direct send_video / send_audio / send_photo / send_document call
  - FloodWait → sleep → recursive retry
  - no semaphores, no upload concurrency gates, no task tracking
  - thumbnail: ffprobe duration → ffmpeg seek → jpg
  - video width / height / duration from one ffprobe pass
  - file-stability guard (.aria2 sentinel)
  - cfg.log_channel forward after send
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
# Wait for aria2 to finish writing (aria2 keeps a .aria2 sidecar)
# ─────────────────────────────────────────────────────────────

async def _wait_stable(path: str, timeout: int = 30) -> None:
    aria = path + ".aria2"
    prev = -1
    for _ in range(timeout):
        if os.path.exists(aria):
            await asyncio.sleep(1)
            continue
        try:
            cur = os.path.getsize(path)
        except OSError:
            await asyncio.sleep(1)
            continue
        if cur == prev and cur > 0:
            return
        prev = cur
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
    task_record                = None,   # accepted but ignored — kept for API compat
    is_last:        bool       = False,
) -> None:
    """
    Upload one file.  Mirrors telegram.py style:
      - inline progress bar (%, speed, ETA)
      - direct send_* per type
      - FloodWait → sleep → retry (recursive)
      - no semaphores, no concurrency gates
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

    # ── Video: get width / height / duration ───────────────────
    meta       = {"duration": 0, "width": 0, "height": 0}
    auto_thumb : str | None = None

    if ftype == "video":
        meta = await _video_meta(path)

    # ── Thumbnail ──────────────────────────────────────────────
    if ftype in ("video", "document") and not thumb:
        t, is_temp = await _make_thumb(path, meta["duration"])
        if t:
            thumb = t
            if is_temp:
                auto_thumb = t

    log.info("⬆ %s  [%s]  %s  dur=%ds  %dx%d",
             fname, ftype, human_size(file_size),
             meta["duration"], meta["width"], meta["height"])

    # ── Progress callback — telegram.py style ─────────────────
    task_start = time.time()
    last_edit  = [task_start]

    async def progress(current: int, total: int) -> None:
        now = time.time()
        if now - last_edit[0] < 0.5:
            return
        last_edit[0] = now

        elapsed = now - task_start
        speed   = current / elapsed if elapsed > 0 else 0
        eta     = int((total - current) / speed) if speed > 0 else 0
        pct     = current / total * 100 if total else 0
        filled  = int(pct / 10)
        bar     = "█" * filled + "░" * (10 - filled)

        await safe_edit(
            msg,
            f"📤 <b>Uploading…</b>\n"
            f"<code>[{bar}]</code> <b>{pct:.1f}%</b>\n"
            f"🔥 <code>{human_size(speed)}/s</code>   "
            f"⏳ <code>{eta}s</code>\n"
            f"✅ <code>{human_size(current)}</code> / "
            f"<code>{human_size(total)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

    # ── Send — one branch per type, mirrors telegram.py ───────
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

        else:  # document
            sent = await client.send_document(
                chat_id, path,
                force_document = True,
                **common,
            )

        # Delete the progress status message
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

        elapsed = time.time() - task_start
        log.info("✅ Done: %s  %s/s  %.1fs",
                 fname,
                 human_size(file_size / elapsed) if elapsed else "—",
                 elapsed)

    except FloodWait as fw:
        # Honour Telegram's wait, cap at 120 s — then retry exactly like telegram.py
        wait = min(fw.value, 120)
        log.warning("FloodWait %ds — sleeping", wait)
        await asyncio.sleep(wait)
        await upload_file(
            client, msg, path,
            caption=caption, thumb=thumb,
            force_document=force_document, is_last=is_last,
        )

    except Exception as exc:
        if "MESSAGE_NOT_MODIFIED" not in str(exc):
            log.error("Upload error %s: %s", fname, exc)
            await safe_edit(
                msg,
                f"❌ Upload failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        raise

    finally:
        if auto_thumb and os.path.isfile(auto_thumb):
            try:
                os.remove(auto_thumb)
            except OSError:
                pass
