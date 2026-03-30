"""
services/uploader.py
Upload a local file to Telegram.

HIGH FIX: _upload_single was calling _make_thumb for ftype == "document"
as well as "video". For non-video documents (zip, rar, srt, json, etc.)
ffmpeg runs, tries 4 timestamps, fails silently every time, and wastes
1-3 seconds on each document upload.

Fix: only call _make_thumb when ftype == "video".
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
    aria = path + ".aria2"
    if not os.path.exists(aria):
        return  # fast path: already complete

    for _ in range(timeout):
        if not os.path.exists(aria):
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


# ─────────────────────────────────────────────────────────────
# Thumbnail — ffmpeg seek to 20 % of duration
# ─────────────────────────────────────────────────────────────

async def _make_thumb(path: str, duration: int) -> tuple[str | None, bool]:
    out = path + "_zt.jpg"
    candidates = (
        [max(1, int(duration * 0.20)),
         max(1, int(duration * 0.30)),
         max(1, int(duration * 0.10))]
        if duration > 5 else [1]
    )
    for ts in candidates:
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                "-ss", str(ts), "-i", path,
                "-frames:v", "1",
                "-vf", "scale='min(1280,iw)':-2",
                "-q:v", "1",
                out,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.communicate()
            if os.path.exists(out) and os.path.getsize(out) > 1000:
                return out, True
        except Exception as exc:
            log.debug("_make_thumb ts=%d: %s", ts, exc)
    return None, False


# ─────────────────────────────────────────────────────────────
# Auto-split for files > Telegram's 2 GB bot limit
# ─────────────────────────────────────────────────────────────

TG_MAX_BYTES = 1900 * 1024 * 1024   # 1.9 GB


async def _split_binary(path: str, tmp_dir: str, chunk_bytes: int) -> list[str]:
    fname  = os.path.basename(path)
    base   = os.path.splitext(fname)[0]
    ext    = os.path.splitext(fname)[1]
    parts  = []
    idx    = 1

    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_bytes)
            if not chunk:
                break
            part_path = os.path.join(tmp_dir, f"{base}.part{idx:03d}{ext}")
            with open(part_path, "wb") as out:
                out.write(chunk)
            parts.append(part_path)
            idx += 1

    return parts


async def _split_video_by_size(path: str, tmp_dir: str, chunk_bytes: int) -> list[str]:
    from services import ffmpeg as FF

    try:
        dur = await FF.probe_duration(path)
        if not dur:
            raise ValueError("Unknown duration")

        fsize          = os.path.getsize(path)
        secs_per_chunk = int(dur * chunk_bytes / fsize)
        if secs_per_chunk < 10:
            secs_per_chunk = 10

        base    = os.path.splitext(os.path.basename(path))[0]
        ext     = os.path.splitext(path)[1] or ".mp4"
        pattern = os.path.join(tmp_dir, f"{base}.part%03d{ext}")

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", path,
            "-c", "copy",
            "-f", "segment",
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
            f for f in (
                os.path.join(tmp_dir, p)
                for p in os.listdir(tmp_dir)
                if f"{base}.part" in p
            )
            if os.path.isfile(f)
        )
        if parts:
            return parts
        raise RuntimeError("FFmpeg produced no output files")

    except Exception as exc:
        log.warning("_split_video_by_size FFmpeg failed (%s) — binary split fallback", exc)
        return await _split_binary(path, tmp_dir, chunk_bytes)


async def _upload_parts(
    client,
    msg,
    parts:          list[str],
    original_fname: str,
    force_document: bool,
    thumb:          str | None,
) -> None:
    total_parts = len(parts)
    chat_id     = _chat_id(msg)

    try:
        await client.send_message(
            chat_id,
            f"✂️ <b>File split into {total_parts} parts</b>\n"
            f"<code>{original_fname}</code>\n"
            f"<i>Each part ≤ 1.9 GB — uploading now…</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception:
        pass

    for i, part_path in enumerate(parts, 1):
        part_fname = os.path.basename(part_path)
        part_size  = os.path.getsize(part_path)
        part_cap   = (
            f"<code>{original_fname}</code>\n"
            f"📦 <b>Part {i} / {total_parts}</b>  "
            f"<code>{human_size(part_size)}</code>"
        )

        part_st = await client.send_message(
            chat_id,
            f"📤 Uploading Part {i}/{total_parts}…\n"
            f"<code>{part_fname}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

        try:
            await _upload_single(
                client, part_st, part_path,
                caption=part_cap,
                thumb=thumb,
                force_document=force_document,
            )
        except Exception as exc:
            log.error("Part %d/%d upload failed: %s", i, total_parts, exc)
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
    task_record                = None,
    is_last:        bool       = False,
) -> None:
    if not os.path.isfile(path):
        await safe_edit(
            msg,
            f"❌ File not found: <code>{os.path.basename(path)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    await _wait_stable(path)

    file_size      = os.path.getsize(path)
    original_fname = os.path.basename(path)

    if file_size <= TG_MAX_BYTES:
        await _upload_single(client, msg, path,
                             caption=caption, thumb=thumb,
                             force_document=force_document)
        return

    log.info(
        "File %s is %s — exceeds 1.9 GB limit, splitting",
        original_fname, human_size(file_size),
    )

    try:
        await msg.delete()
    except Exception:
        pass

    import tempfile
    tmp_dir = tempfile.mkdtemp(prefix="tg_split_")
    try:
        ext = os.path.splitext(path)[1].lower()
        if ext in _VIDEO_EXTS and not force_document:
            parts = await _split_video_by_size(path, tmp_dir, TG_MAX_BYTES)
        else:
            parts = await _split_binary(path, tmp_dir, TG_MAX_BYTES)

        await _upload_parts(
            client, msg, parts,
            original_fname=original_fname,
            force_document=force_document,
            thumb=thumb,
        )
    finally:
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


async def _upload_single(
    client:         Client,
    msg,
    path:           str,
    caption:        str        = "",
    thumb:          str | None = None,
    force_document: bool       = False,
) -> None:
    chat_id   = _chat_id(msg)
    fname     = os.path.basename(path)
    file_size = os.path.getsize(path)
    ftype     = _file_type(path, force_document)
    cap       = caption if caption else _build_caption(fname, "", False)

    # ── Video: width / height / duration ──────────────────────
    meta       = {"duration": 0, "width": 0, "height": 0}
    auto_thumb: str | None = None

    if ftype == "video":
        meta = await _video_meta(path)

    # FIX (HIGH): only generate thumbnail for video files.
    # Previously this also ran for ftype="document" (zip, rar, srt, etc.)
    # causing ffmpeg to try 4 timestamps and fail silently for each one,
    # wasting 1-3 seconds per non-video document upload.
    if ftype == "video" and not thumb:
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
    _last_prog  = [task_start]
    _PROG_MIN   = 0.5

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
            break

        except FloodWait as fw:
            wait = min(fw.value + 5, 120)
            log.warning("FloodWait %ds on upload attempt %d — sleeping", wait, attempt + 1)
            await asyncio.sleep(wait)

        except Exception as exc:
            error = exc
            break

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
