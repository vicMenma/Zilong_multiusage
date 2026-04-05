"""
services/uploader.py
Upload a local file to Telegram with inline per-task progress.

REWRITE:
  - Per-task inline progress using new progress_panel() design
  - No dummy message objects — caller always passes a real message
  - Thumbnail generated only for video files (not documents)
  - Auto-split for files > 1.9 GB
  - FloodWait retry (up to 4 attempts)
  - LOG_CHANNEL forward after successful upload

THUMBNAIL FIXES in _make_thumb():
  1. flags=lanczos+accurate_rnd  — sharpest downscale (was missing; FFmpeg
     was defaulting to bicubic despite comment claiming lanczos)
  2. format=yuv420p BEFORE scale — correct 10-bit HEVC pixel format
     conversion before the scale filter; prevents mid-pipeline softness
  3. unsharp=lx=5:ly=5:la=0.8:cx=5:cy=5:ca=0.0 — recovers micro-contrast
     lost during downscale; luma-only (ca=0.0) prevents color fringing
  4. -q:v 1 — maximum JPEG quality (was 2; scale is 1-31, lower=better)
  5. Fixed seek overshoot: fine_seek = ts - pre_seek
     (was always hardcoded to 3, so ts=1 → extracted frame at second 3)
  6. HDR detection + tone-mapping chain via ffmpeg.py._is_hdr()
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
from core.session import settings
from services.utils import human_size, progress_panel, safe_edit

log = logging.getLogger(__name__)

_AUDIO_EXTS = frozenset({
    ".mp3", ".aac", ".flac", ".ogg", ".m4a", ".opus",
    ".wav", ".wma", ".ac3", ".mka",
})
_VIDEO_EXTS = frozenset({
    ".mp4", ".mov", ".webm", ".m4v", ".mkv", ".avi", ".flv",
    ".ts", ".m2ts", ".wmv", ".3gp", ".rmvb", ".mpg", ".mpeg",
})
_PHOTO_EXTS = frozenset({".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"})

TG_MAX_BYTES = 1_900 * 1024 * 1024   # 1.9 GiB

# ─────────────────────────────────────────────────────────────
# Crystal-clear thumbnail VF chains (mirrors ffmpeg.py)
# ─────────────────────────────────────────────────────────────

# SDR content (most MP4/MKV)
_VF_SHARP_SDR = (
    "format=yuv420p,"                                   # normalize pixel format FIRST (handles 10-bit HEVC)
    "scale=w=320:h=320:"
    "flags=lanczos+accurate_rnd:"                       # lanczos: sharpest downscale, accurate_rnd: sub-pixel precision
    "force_original_aspect_ratio=decrease,"
    "unsharp=lx=5:ly=5:la=0.8:cx=5:cy=5:ca=0.0"       # sharpen luma only (ca=0.0 = no chroma → no color fringing)
)

# HDR10 / HLG — tone-map to SDR before scale+sharpen
_VF_SHARP_HDR = (
    "zscale=transfer=linear:npl=100,"
    "format=gbrpf32le,"
    "zscale=primaries=bt709,"
    "tonemap=hable:desat=0,"
    "zscale=transfer=bt709:matrix=bt709:range=tv,"
    "format=yuv420p,"
    "scale=w=320:h=320:"
    "flags=lanczos+accurate_rnd:"
    "force_original_aspect_ratio=decrease,"
    "unsharp=lx=5:ly=5:la=0.8:cx=5:cy=5:ca=0.0"
)


# ─────────────────────────────────────────────────────────────
# Helpers
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


def _ftype(path: str, force_document: bool) -> str:
    if force_document:
        return "document"
    ext = os.path.splitext(path)[1].lower()
    if ext in _PHOTO_EXTS:  return "photo"
    if ext in _AUDIO_EXTS:  return "audio"
    if ext in _VIDEO_EXTS:  return "video"
    return "document"


async def _wait_aria2(path: str, timeout: int = 30) -> None:
    """Block until aria2's .aria2 control file disappears."""
    ctrl = path + ".aria2"
    if not os.path.exists(ctrl):
        return
    for _ in range(timeout):
        await asyncio.sleep(1)
        if not os.path.exists(ctrl):
            return


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
            break
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


async def _make_thumb(path: str, duration: int) -> tuple[str | None, bool]:
    """
    Extract a crystal-clear thumbnail from a video file.

    Strategy:
      - Two-stage seek (fast keyframe seek + accurate frame decode)
      - lanczos+accurate_rnd downscaling (sharpest algorithm)
      - format=yuv420p before scale (handles 10-bit HEVC correctly)
      - unsharp mask after scale (recovers micro-contrast, luma only)
      - Maximum JPEG quality (-q:v 1)
      - HDR detection + tone-mapping (prevents washed-out HDR frames)
      - Fixed seek math: fine_seek = ts - pre_seek (never overshoots)
    """
    out = path + "_zt.jpg"

    candidates = (
        [max(1, int(duration * p)) for p in (0.20, 0.30, 0.10)]
        if duration > 5 else [1]
    )

    # Detect HDR once — avoids re-probing on every candidate timestamp
    hdr = False
    try:
        from services.ffmpeg import _is_hdr
        hdr = await _is_hdr(path)
        if hdr:
            log.debug("_make_thumb: HDR detected for %s — using tone-map chain",
                      os.path.basename(path))
    except Exception:
        pass  # _is_hdr failure is non-fatal — fall back to SDR chain

    vf_chain = _VF_SHARP_HDR if hdr else _VF_SHARP_SDR

    for ts in candidates:
        # FIX: correct two-stage seek math
        # Old code had fine_seek hardcoded to 3, causing:
        #   ts=1 → pre_seek=0, fine_seek=3 → extracted frame at second 3 (wrong!)
        # New code: fine_seek = ts - pre_seek, always in [0, 3], never overshoots
        pre_seek  = max(0, ts - 3)
        fine_seek = ts - pre_seek   # = min(3, ts)

        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                # Stage 1: fast keyframe seek (lands within a few frames of ts)
                "-ss", str(pre_seek),
                "-i", path,
                # Stage 2: accurate frame-level decode from that keyframe
                "-ss", str(fine_seek),
                "-frames:v", "1",
                "-vf", vf_chain,
                "-q:v", "1",            # FIX: max JPEG quality (was 2; scale 1-31 lower=better)
                out,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            # HDR fallback: if zscale/libzimg not available, retry with SDR chain
            if hdr and (not os.path.exists(out) or os.path.getsize(out) < 1000):
                err_txt = stderr.decode(errors="replace")
                if "zscale" in err_txt or "libzimg" in err_txt:
                    log.warning(
                        "_make_thumb: zscale unavailable — falling back to SDR chain"
                    )
                    proc2 = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-y",
                        "-ss", str(pre_seek), "-i", path,
                        "-ss", str(fine_seek),
                        "-frames:v", "1",
                        "-vf", _VF_SHARP_SDR,
                        "-q:v", "1",
                        out,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await proc2.communicate()

            if os.path.exists(out) and os.path.getsize(out) > 1000:
                return out, True

        except Exception as exc:
            log.debug("_make_thumb ts=%d: %s", ts, exc)

    return None, False


# ─────────────────────────────────────────────────────────────
# Splitting for oversized files
# ─────────────────────────────────────────────────────────────

async def _split_video(path: str, tmp_dir: str, chunk_bytes: int) -> list[str]:
    from services import ffmpeg as FF

    try:
        dur = await FF.probe_duration(path)
        if not dur:
            raise ValueError("Unknown duration")

        fsize          = os.path.getsize(path)
        secs_per_chunk = max(10, int(dur * chunk_bytes / fsize))
        base    = os.path.splitext(os.path.basename(path))[0]
        ext     = os.path.splitext(path)[1] or ".mp4"
        pattern = os.path.join(tmp_dir, f"{base}.part%03d{ext}")

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", path,
            "-c", "copy", "-f", "segment",
            "-segment_time", str(secs_per_chunk),
            "-reset_timestamps", "1",
            pattern,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(stderr.decode(errors="replace")[-300:])

        parts = sorted(
            os.path.join(tmp_dir, p)
            for p in os.listdir(tmp_dir)
            if f"{base}.part" in p and os.path.isfile(os.path.join(tmp_dir, p))
        )
        if parts:
            return parts
        raise RuntimeError("FFmpeg produced no output files")
    except Exception as exc:
        log.warning("_split_video failed (%s) — binary split fallback", exc)
        return await _split_binary(path, tmp_dir, chunk_bytes)


async def _split_binary(path: str, tmp_dir: str, chunk_bytes: int) -> list[str]:
    base  = os.path.splitext(os.path.basename(path))[0]
    ext   = os.path.splitext(path)[1]
    parts = []
    idx   = 1
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_bytes)
            if not chunk:
                break
            pp = os.path.join(tmp_dir, f"{base}.part{idx:03d}{ext}")
            with open(pp, "wb") as out:
                out.write(chunk)
            parts.append(pp)
            idx += 1
    return parts


# ─────────────────────────────────────────────────────────────
# Core single-file upload
# ─────────────────────────────────────────────────────────────

async def _upload_single(
    client:         Client,
    msg,
    path:           str,
    caption:        str        = "",
    thumb:          str | None = None,
    force_document: bool       = False,
    user_id:        int        = 0,
) -> None:
    from services.task_runner import _stats_cache

    chat_id     = _chat_id(msg)
    fname       = os.path.basename(path)
    file_size   = os.path.getsize(path)
    ftype       = _ftype(path, force_document)
    cap         = caption or f"<code>{fname}</code>"

    user_cfg    = await settings.get(user_id)
    panel_style = user_cfg.get("progress_style", "B")

    # Video metadata + thumbnail
    meta:       dict = {"duration": 0, "width": 0, "height": 0}
    auto_thumb: str | None = None

    if ftype == "video":
        meta = await _video_meta(path)
        if not thumb:
            t, is_temp = await _make_thumb(path, meta["duration"])
            if t:
                thumb = t
                if is_temp:
                    auto_thumb = t

    # Delete placeholder message; panel is now the upload progress message
    try:
        await msg.delete()
    except Exception:
        pass

    start        = time.time()
    last_edit    = [start - 4.0]

    async def progress(current: int, total: int) -> None:
        now = time.time()
        if now - last_edit[0] < 4.0:
            return
        last_edit[0] = now
        elapsed = now - start
        speed   = current / elapsed if elapsed else 0.0
        eta     = int((total - current) / speed) if (speed and total > current) else 0
        s = _stats_cache
        text = progress_panel(
            mode       = "ul",
            fname      = fname,
            done       = current,
            total      = total or file_size,
            speed      = speed,
            eta        = eta,
            elapsed    = elapsed,
            engine     = "telegram",
            link_label = "Telegram",
            cpu        = float(s.get("cpu", 0)),
            ram_used   = int(s.get("ram_used", 0)),
            disk_free  = int(s.get("disk_free", 0)),
            style      = panel_style,
        )
        await safe_edit(status_msg, text, parse_mode=enums.ParseMode.HTML)

    # Send progress message
    status_msg = await client.send_message(
        chat_id,
        progress_panel(
            mode="ul", fname=fname,
            done=0, total=file_size,
            engine="telegram", link_label="Telegram",
            style=panel_style,
        ),
        parse_mode=enums.ParseMode.HTML,
    )

    common = dict(
        caption    = cap,
        thumb      = thumb,
        parse_mode = enums.ParseMode.HTML,
        progress   = progress,
    )

    sent  = None
    error = None

    for attempt in range(4):
        try:
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
                    chat_id, path, force_document=True, **common,
                )
            error = None
            break
        except FloodWait as fw:
            log.warning("FloodWait %ds on upload (attempt %d/4)", fw.value, attempt + 1)
            await asyncio.sleep(fw.value + 2)
            continue
        except Exception as exc:
            error = exc
            break

    elapsed = time.time() - start
    speed   = file_size / elapsed if elapsed else 0.0

    try:
        if error is None:
            await status_msg.delete()
        else:
            await status_msg.edit(
                f"❌ <b>Upload failed</b>\n<code>{fname}</code>\n"
                f"<code>{str(error)[:200]}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
    except Exception:
        pass

    if auto_thumb and os.path.isfile(auto_thumb):
        try:
            os.remove(auto_thumb)
        except OSError:
            pass

    if error is not None:
        if "MESSAGE_NOT_MODIFIED" not in str(error):
            log.error("Upload error %s: %s", fname, error)
        raise error

    # Forward to log channel
    if cfg.log_channel and sent:
        try:
            await sent.forward(cfg.log_channel)
        except Exception:
            pass

    # Auto-forward to user's saved channels (if enabled in Settings)
    if sent and user_id:
        try:
            s = await settings.get(user_id)
            if s.get("auto_forward") and s.get("forward_channels"):
                for ch in s["forward_channels"]:
                    ch_id = ch.get("id")
                    if not ch_id:
                        continue
                    try:
                        from plugins.caption_templates import (
                            build_caption, has_custom_template,
                        )
                        if has_custom_template(ch_id):
                            cap_fwd = await build_caption(path, ch_id)
                            await sent.copy(ch_id, caption=cap_fwd,
                                            parse_mode=enums.ParseMode.HTML)
                        else:
                            await sent.copy(ch_id)
                    except Exception as fwd_exc:
                        log.warning("Auto-forward to %s failed: %s", ch_id, fwd_exc)
        except Exception as af_exc:
            log.warning("Auto-forward error: %s", af_exc)

    log.info("✅ %s  %s/s  %.1fs", fname, human_size(speed), elapsed)


# ─────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────

async def upload_file(
    client:         Client,
    msg,
    path:           str,
    caption:        str        = "",
    thumb:          str | None = None,
    force_document: bool       = False,
    task_record                = None,   # kept for API compat, unused
    is_last:        bool       = False,
    user_id:        int        = 0,
) -> None:
    if not os.path.isfile(path):
        await safe_edit(
            msg,
            f"❌ File not found: <code>{os.path.basename(path)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    await _wait_aria2(path)

    file_size      = os.path.getsize(path)
    original_fname = os.path.basename(path)

    if file_size <= TG_MAX_BYTES:
        await _upload_single(client, msg, path,
                             caption=caption, thumb=thumb,
                             force_document=force_document,
                             user_id=user_id)
        return

    log.info("File %s (%s) > 1.9 GiB — splitting", original_fname, human_size(file_size))

    try:
        await msg.delete()
    except Exception:
        pass

    import tempfile
    tmp_dir = tempfile.mkdtemp(prefix="tg_split_")
    chat_id = _chat_id(msg)

    try:
        ext = os.path.splitext(path)[1].lower()
        if ext in _VIDEO_EXTS and not force_document:
            parts = await _split_video(path, tmp_dir, TG_MAX_BYTES)
        else:
            parts = await _split_binary(path, tmp_dir, TG_MAX_BYTES)

        total_parts = len(parts)

        try:
            await client.send_message(
                chat_id,
                f"✂️ <b>Splitting into {total_parts} parts</b>\n"
                f"<code>{original_fname}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

        for i, pp in enumerate(parts, 1):
            part_size = os.path.getsize(pp)
            part_cap  = (
                f"<code>{original_fname}</code>\n"
                f"📦 <b>Part {i}/{total_parts}</b>  <code>{human_size(part_size)}</code>"
            )
            await asyncio.sleep(1.5)
            ph = await client.send_message(
                chat_id,
                f"📤 Part {i}/{total_parts}…\n<code>{os.path.basename(pp)}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            try:
                await _upload_single(client, ph, pp, caption=part_cap,
                                     force_document=force_document,
                                     user_id=user_id)
            except Exception as exc:
                log.error("Part %d/%d failed: %s", i, total_parts, exc)
                try:
                    await client.send_message(
                        chat_id,
                        f"❌ Part {i}/{total_parts} failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML,
                    )
                except Exception:
                    pass

        try:
            await client.send_message(
                chat_id,
                f"✅ <b>All {total_parts} parts uploaded</b>\n"
                f"<code>{original_fname}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception:
            pass

    finally:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
