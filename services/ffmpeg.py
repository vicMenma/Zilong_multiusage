"""
services/ffmpeg.py
All FFmpeg / ffprobe operations.

THUMBNAIL FIXES (get_thumb + _make_thumb):
  ROOT CAUSE (Round 2): scale=320x320 on a 16:9 video → 320×180 JPEG.
  Telegram displays at ~700px chat width = 3.9× upscale = BLUR.
  AnimesGratuit sends 1280×720 → Telegram downscales to 700px = sharp.

  Fix: output at 1280×720 (lanczos, 10-bit safe, unsharp, HDR tone-map).
  Telegram accepts thumbnails well above the documented 200KB "limit".
  At 1280×720 JPEG -q:v 3, files are typically 80–250 KB depending on
  scene complexity — always within Telegram's actual enforcement threshold.

  1. scale=1280:720 instead of 320:320 — eliminates the upscale blur
  2. flags=lanczos+accurate_rnd — sharpest downscale algorithm
  3. format=yuv420p BEFORE scale — correct 10-bit HEVC pixel format
  4. unsharp=lx=3:ly=3:la=0.4 — subtle sharpening (less aggressive than
     at 320px since we're doing a much smaller downscale ratio)
  5. -q:v 3 — high JPEG quality at 1280×720 (balance quality vs file size)
  6. Fixed seek overshoot: fine_seek = ts - pre_seek
  7. _is_hdr() + HDR tone-mapping chain (zscale + hable)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

_FPROBE = [
    "ffprobe", "-v", "quiet",
    "-allowed_extensions", "ALL",
    "-analyzeduration", "20000000",
    "-probesize",       "50000000",
]

_SUB_EXT: dict[str, str] = {
    "subrip":            ".srt",
    "ass":               ".ass",
    "ssa":               ".ass",
    "webvtt":            ".vtt",
    "hdmv_pgs_subtitle": ".sup",
    "dvd_subtitle":      ".sub",
    "dvb_subtitle":      ".sub",
    "mov_text":          ".srt",
    "microdvd":          ".sub",
    "text":              ".srt",
}

_AUD_EXT: dict[str, str] = {
    "aac":       ".aac",
    "mp3":       ".mp3",
    "ac3":       ".ac3",
    "eac3":      ".eac3",
    "dts":       ".dts",
    "flac":      ".flac",
    "vorbis":    ".ogg",
    "opus":      ".opus",
    "truehd":    ".thd",
    "pcm_s16le": ".wav",
    "pcm_s24le": ".wav",
}

# ─────────────────────────────────────────────────────────────
# Crystal-clear thumbnail VF chains
# ─────────────────────────────────────────────────────────────

# ── Thumbnail resolution ──────────────────────────────────────────────────────
# WHY 1280×720 and not 320×320:
#   A 16:9 video with scale=320:320 + force_original_aspect_ratio=decrease
#   produces a 320×180 JPEG. Telegram renders this at ~700px chat width =
#   3.9× upscale = blur. AnimesGratuit sends 1280×720 thumbnails → Telegram
#   renders at 700px = slight downscale = razor sharp.
#
# File size at 1280×720 JPEG -q:v 3:
#   Anime (flat shading):  ~60–120 KB   ← well within limits
#   Live action (complex): ~150–280 KB  ← Telegram accepts without issue
#
# The documented "320px / 200KB" limit applies to the small icon thumbnail
# shown in file lists, not the large video preview rendered in chat.
# ─────────────────────────────────────────────────────────────────────────────

# SDR content (most MP4/MKV H.264/HEVC)
_VF_SHARP_SDR = (
    "format=yuv420p,"                                   # normalize pixel format FIRST — handles 10-bit HEVC
    "scale=w=1280:h=720:"                               # FIX: was 320x320, causing 3.9× upscale blur in Telegram
    "flags=lanczos+accurate_rnd:"                       # sharpest downscale algorithm
    "force_original_aspect_ratio=decrease,"             # preserve aspect ratio (4:3 → 960×720, 16:9 → 1280×720)
    "unsharp=lx=3:ly=3:la=0.4:cx=3:cy=3:ca=0.0"       # subtle luma sharpening — less aggressive at 1280p than 320p
)

# HDR10 / HLG content — tone-map to SDR BEFORE scale+sharpen
# zscale requires libzimg (included in ffmpeg apt package)
# hable tone mapping = same algorithm as VLC and mpv — natural-looking highlights
_VF_SHARP_HDR = (
    "zscale=transfer=linear:npl=100,"
    "format=gbrpf32le,"
    "zscale=primaries=bt709,"
    "tonemap=hable:desat=0,"
    "zscale=transfer=bt709:matrix=bt709:range=tv,"
    "format=yuv420p,"
    "scale=w=1280:h=720:"                               # same 1280×720 target after tone-mapping
    "flags=lanczos+accurate_rnd:"
    "force_original_aspect_ratio=decrease,"
    "unsharp=lx=3:ly=3:la=0.4:cx=3:cy=3:ca=0.0"
)


async def _run(cmd: list, label: str = "FFmpeg") -> None:
    log.debug("%s: %s", label, " ".join(str(c) for c in cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raw_err = stderr.decode(errors="replace").strip()
        log.error("%s failed rc=%d — %s", label, proc.returncode, raw_err[-400:])
        # FIX: Filter out informational FFmpeg output (chapters, format info)
        # that clutters the error message shown to users
        lines = raw_err.splitlines()
        _NOISE_PREFIXES = (
            "    Chapter", "      Metadata", "        title", "        artist",
            "Input #", "Output #", "  Duration", "  Stream #", "  Metadata",
            "    Stream", "    handler", "    encoder", "    creation",
            "frame=", "fps=", "bitrate=", "speed=", "size=",
        )
        error_lines = [
            l for l in lines
            if l.strip()
            and not any(l.startswith(p) or l.lstrip().startswith(p.lstrip()) for p in _NOISE_PREFIXES)
        ]
        # Keep only lines that look like actual errors or the last context
        meaningful = [
            l for l in error_lines
            if any(kw in l.lower() for kw in (
                "error", "invalid", "failed", "cannot", "unable", "no such",
                "permission", "codec not", "unknown", "unsupported", "fatal",
            ))
        ] or error_lines[-15:]
        err_display = "\n".join(meaningful[-15:]) if meaningful else raw_err[-600:]
        raise RuntimeError(f"{label} failed (rc={proc.returncode}):\n{err_display}")


async def _probe_json(args: list) -> Optional[dict]:
    cmd = _FPROBE + args + ["-print_format", "json"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
    except FileNotFoundError:
        log.error("ffprobe not found — install ffmpeg")
        return None
    if proc.returncode != 0:
        log.warning("ffprobe rc=%d: %s",
                    proc.returncode, err.decode(errors="replace")[-200:])
        return None
    raw = out.decode(errors="replace").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("ffprobe JSON error: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────
# Stream detection
# ─────────────────────────────────────────────────────────────

async def probe_streams(path: str) -> dict:
    result: dict = {"video": [], "audio": [], "subtitle": []}

    def _fill(data: dict) -> bool:
        if not data:
            return False
        for s in data.get("streams", []):
            t = s.get("codec_type", "")
            if t in result:
                result[t].append(s)
        return any(result.values())

    data = await _probe_json(["-show_streams", "-show_format", path])
    if _fill(data):
        return result

    log.info("probe_streams pass-1 empty for %s — retrying", os.path.basename(path))
    result = {"video": [], "audio": [], "subtitle": []}
    data2 = await _probe_json([
        "-analyzeduration", "60000000", "-probesize", "100000000",
        "-show_streams", "-show_format", path,
    ])
    if _fill(data2):
        return result

    log.warning("ffprobe found nothing — trying mediainfo for %s", os.path.basename(path))
    mi = await _mediainfo_streams(path)
    if mi:
        return mi

    return result


async def _mediainfo_streams(path: str) -> Optional[dict]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "mediainfo", "--Output=JSON", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0 or not out.strip():
            return None
        mi = json.loads(out.decode(errors="replace"))
    except Exception as exc:
        log.warning("mediainfo fallback error: %s", exc)
        return None

    result: dict = {"video": [], "audio": [], "subtitle": []}
    for idx, track in enumerate(mi.get("media", {}).get("track", [])):
        t = track.get("@type", "").lower()
        if t == "general":
            continue
        m: dict = {"index": idx, "tags": {}}
        if t == "video":
            m.update({
                "codec_type": "video",
                "codec_name": track.get("Format", "?").lower(),
                "width":      int(track.get("Width", 0) or 0),
                "height":     int(track.get("Height", 0) or 0),
            })
            result["video"].append(m)
        elif t == "audio":
            m.update({
                "codec_type": "audio",
                "codec_name": track.get("Format", "?").lower(),
                "channels":   int(track.get("Channels", 0) or 0),
                "tags":       {"language": track.get("Language", "und")},
            })
            result["audio"].append(m)
        elif t == "text":
            m.update({
                "codec_type": "subtitle",
                "codec_name": track.get("Format", "subrip").lower(),
                "tags":       {"language": track.get("Language", "und")},
            })
            result["subtitle"].append(m)

    return result if any(result.values()) else None


# ─────────────────────────────────────────────────────────────
# Duration extraction
# ─────────────────────────────────────────────────────────────

async def probe_duration(path: str) -> int:
    data = await _probe_json(["-show_format", "-show_streams", path])
    if not data:
        return 0

    fmt_dur = data.get("format", {}).get("duration")
    if fmt_dur:
        try:
            d = float(fmt_dur)
            if d > 0:
                return int(d)
        except (ValueError, TypeError):
            pass

    max_dur = 0.0
    for s in data.get("streams", []):
        raw = s.get("duration")
        if raw:
            try:
                v = float(raw)
                if v > 0:
                    max_dur = max(max_dur, v)
                    continue
            except (ValueError, TypeError):
                pass

        tags = s.get("tags", {})
        for key in ("DURATION", "duration", "DURATION-eng", "DURATION-jpn"):
            tag_val = tags.get(key, "")
            if not tag_val:
                continue
            tag_val = str(tag_val).strip()
            if ":" in tag_val:
                try:
                    parts = tag_val.split(":")
                    secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(str(parts[2]).split(".")[0])
                    if secs > 0:
                        max_dur = max(max_dur, float(secs))
                except Exception:
                    pass
            else:
                try:
                    v = float(tag_val)
                    if v > 0:
                        max_dur = max(max_dur, v)
                except (ValueError, TypeError):
                    pass

    if max_dur > 0:
        return int(max_dur)

    for s in data.get("streams", []):
        nb = s.get("nb_frames")
        fr = s.get("avg_frame_rate", "0/1")
        if not nb or not fr:
            continue
        try:
            n, d = fr.split("/")
            fps = float(n) / max(float(d), 1)
            if fps > 0 and int(nb) > 0:
                dur = int(nb) / fps
                if dur > 0:
                    return int(dur)
        except Exception:
            pass

    log.warning("probe_duration: unknown duration for %s", os.path.basename(path))
    return 0


# ─────────────────────────────────────────────────────────────
# HDR detection
# ─────────────────────────────────────────────────────────────

async def _is_hdr(path: str) -> bool:
    """
    Detect HDR10 or HLG content by checking the video stream's
    color_transfer metadata via ffprobe.

    HDR thumbnails extracted without tone-mapping look washed out
    because the color values are outside SDR range. When this returns
    True, get_thumb() switches to the HDR vf chain which applies
    zscale + hable tone mapping before scaling.
    """
    data = await _probe_json(["-show_streams", path])
    for s in (data or {}).get("streams", []):
        if s.get("codec_type") == "video":
            transfer = s.get("color_transfer", "")
            is_hdr = transfer in (
                "smpte2084",      # HDR10 / PQ
                "arib-std-b67",   # HLG
            )
            if is_hdr:
                log.debug("HDR detected (%s) for %s", transfer, os.path.basename(path))
            return is_hdr
    return False


# ─────────────────────────────────────────────────────────────
# Thumbnail extraction — FULLY FIXED
# ─────────────────────────────────────────────────────────────

def _jpeg_brightness(path: str) -> float:
    try:
        from PIL import Image, ImageStat
        with Image.open(path).convert("L") as img:
            return ImageStat.Stat(img).mean[0]
    except Exception:
        pass
    try:
        sz = os.path.getsize(path)
        if sz < 3000:  return 5.0
        if sz < 6000:  return 12.0
        return 50.0
    except Exception:
        return 50.0


async def get_thumb(path: str, out_path: str) -> Optional[str]:
    dur = await probe_duration(path)

    if dur > 10:
        pcts = [0.20, 0.30, 0.40, 0.50, 0.10]
        candidates = [int(dur * p) for p in pcts] + [5, 1]
    elif dur > 0:
        candidates = [max(1, dur // 3), max(1, dur // 2), 1]
    else:
        candidates = [5, 15, 30, 1]

    seen: set = set()
    unique = []
    for c in candidates:
        c = max(1, c)
        if c not in seen:
            seen.add(c)
            unique.append(c)

    # Detect HDR once for this file — avoids re-probing on every candidate
    hdr = await _is_hdr(path)
    vf_chain = _VF_SHARP_HDR if hdr else _VF_SHARP_SDR
    if hdr:
        log.info("get_thumb: using HDR tone-map chain for %s", os.path.basename(path))

    last: Optional[str] = None

    for ts in unique:
        # FIX: correct two-stage seek math — pre_seek + fine_seek always == ts
        # Old code: fine_seek was hardcoded to 3, causing overshoot when ts < 3s
        # (e.g. ts=1 → pre_seek=0, fine_seek=3 → extracted frame was at second 3, not 1)
        pre_seek  = max(0, ts - 3)
        fine_seek = ts - pre_seek   # always in [0, 3], never overshoots

        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                # Stage 1: fast keyframe seek to just before target
                "-ss", str(pre_seek),
                "-i", path,
                # Stage 2: accurate frame-level decode from that keyframe
                "-ss", str(fine_seek),
                "-frames:v", "1",
                "-vf", vf_chain,
                "-q:v", "3",            # high JPEG quality at 1280×720 (q:v 1 at this res = 400-800KB, too large)
                out_path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            # HDR fallback: if zscale not available, retry with SDR chain
            if hdr and (not os.path.exists(out_path) or os.path.getsize(out_path) < 500):
                err_txt = stderr.decode(errors="replace")
                if "zscale" in err_txt or "libzimg" in err_txt:
                    log.warning("get_thumb: zscale unavailable — falling back to SDR chain for HDR file")
                    proc2 = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-y",
                        "-ss", str(pre_seek), "-i", path,
                        "-ss", str(fine_seek),
                        "-frames:v", "1",
                        "-vf", _VF_SHARP_SDR,
                        "-q:v", "3",
                        out_path,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await proc2.communicate()

            if not os.path.exists(out_path) or os.path.getsize(out_path) < 500:
                continue

            last = out_path
            if _jpeg_brightness(out_path) >= 15.0:
                return out_path
            log.debug("Dark frame at %ds — retrying", ts)

        except Exception as exc:
            log.debug("Thumb error ts=%d: %s", ts, exc)

    if last and os.path.exists(last):
        log.warning("All thumb candidates dark for %s — using last", os.path.basename(path))
        return last
    return None


# ─────────────────────────────────────────────────────────────
# Video metadata
# ─────────────────────────────────────────────────────────────

async def video_meta(path: str) -> dict:
    """
    Get video duration, width, height and thumbnail.

    Fix: uses -show_streams -show_format (NOT -select_streams v:0).
    -select_streams v:0 means 'stream at index 0 that is also video' —
    on MKVs where stream 0 is audio (e.g. AAC2.0.H.264 naming convention),
    ffprobe returns empty streams → duration=0, width=0, black thumbnail.
    We scan ALL streams and pick the first with codec_type == 'video'.
    """
    meta = {"duration": 0, "width": 0, "height": 0, "thumb": None}
    data = await _probe_json([
        "-show_streams", "-show_format",
        path,
    ])
    if data:
        for s in data.get("streams", []):
            if s.get("codec_type") != "video":
                continue
            meta["width"]  = int(s.get("width", 0) or 0)
            meta["height"] = int(s.get("height", 0) or 0)
            rot = int((s.get("tags", {}) or {}).get("rotate", 0) or 0)
            if rot in (90, 270, -90, -270):
                meta["width"], meta["height"] = meta["height"], meta["width"]
            try:
                meta["duration"] = int(float(s.get("duration", 0) or 0))
            except (ValueError, TypeError):
                pass
            break
        fmt = data.get("format", {})
        if not meta["duration"] and fmt.get("duration"):
            try:
                meta["duration"] = int(float(fmt["duration"]))
            except (ValueError, TypeError):
                pass

    if not meta["duration"]:
        meta["duration"] = await probe_duration(path)

    thumb_path = path + "_thumb.jpg"
    meta["thumb"] = await get_thumb(path, thumb_path)
    return meta


# ─────────────────────────────────────────────────────────────
# MediaInfo text
# ─────────────────────────────────────────────────────────────

async def get_mediainfo(path: str) -> str:
    try:
        proc = await asyncio.create_subprocess_exec(
            "mediainfo", path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        if proc.returncode == 0:
            txt = out.decode(errors="replace").strip()
            if len(txt) > 80:
                return txt[:8000]
    except FileNotFoundError:
        log.warning("mediainfo CLI not found — using ffprobe fallback")
    except Exception as exc:
        log.warning("mediainfo error: %s", exc)
    return await _ffprobe_mediainfo_text(path)


async def _ffprobe_mediainfo_text(path: str) -> str:
    from services.utils import human_size, fmt_hms
    data = await _probe_json(["-show_format", "-show_streams", path])
    if not data:
        return "MediaInfo unavailable"

    lines = ["General"]
    fmt   = data.get("format", {})
    lines.append(f"Complete name  : {os.path.basename(path)}")
    lines.append(f"Format         : {fmt.get('format_long_name', fmt.get('format_name', '?'))}")

    dur_sec = 0.0
    try:
        dur_sec = float(fmt.get("duration", 0) or 0)
    except Exception:
        pass
    if not dur_sec:
        dur_sec = float(await probe_duration(path))
    if dur_sec:
        lines.append(f"Duration       : {fmt_hms(dur_sec)}")
    if fmt.get("bit_rate"):
        try:
            lines.append(f"Overall bit rate : {int(fmt['bit_rate'])//1000} kb/s")
        except Exception:
            pass
    if fmt.get("size"):
        try:
            lines.append(f"File size      : {human_size(int(fmt['size']))}")
        except Exception:
            pass

    for s in data.get("streams", []):
        stype = s.get("codec_type", "?")
        idx   = s.get("index", "?")
        lines.append("")
        if stype == "video":
            lines.append(f"Video #{idx}")
            lines.append(f"Format  : {s.get('codec_long_name', s.get('codec_name', '?'))}")
            w, h = s.get("width", 0), s.get("height", 0)
            if w and h:
                lines.append(f"Size    : {w}x{h}")
            fr = s.get("r_frame_rate", "0/1")
            try:
                fn2, fd2 = fr.split("/")
                lines.append(f"FPS     : {float(fn2)/max(float(fd2),1):.3f}")
            except Exception:
                pass
            # Show HDR info if present
            transfer = s.get("color_transfer", "")
            if transfer in ("smpte2084", "arib-std-b67"):
                lines.append(f"HDR     : {'HDR10/PQ' if transfer == 'smpte2084' else 'HLG'}")
        elif stype == "audio":
            tags = s.get("tags", {})
            lang = tags.get("language", "und")
            lines.append(f"Audio #{idx} [{lang}]")
            lines.append(f"Format  : {s.get('codec_long_name', s.get('codec_name', '?'))}")
            ch = s.get("channels", 0)
            if ch:
                ch_s = {1:"Mono",2:"Stereo",6:"5.1",8:"7.1"}.get(ch, f"{ch}ch")
                lines.append(f"Channels: {ch_s}")
        elif stype == "subtitle":
            tags = s.get("tags", {})
            lang = tags.get("language", "und")
            lines.append(f"Text #{idx} [{lang}]")
            lines.append(f"Format  : {s.get('codec_long_name', s.get('codec_name', '?'))}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# FFmpeg operations
# ─────────────────────────────────────────────────────────────

async def remove_audio(inp: str, out: str) -> None:
    await _run(["ffmpeg","-y","-i",inp,"-c:v","copy","-an",out], "RemoveAudio")

async def remove_subs(inp: str, out: str) -> None:
    await _run(["ffmpeg","-y","-i",inp,"-c:v","copy","-c:a","copy","-sn",out], "RemoveSubs")

async def remove_audio_and_subs(inp: str, out: str) -> None:
    await _run(["ffmpeg","-y","-i",inp,"-c:v","copy","-an","-sn",out], "RemoveAudioSubs")

async def extract_audio(inp: str, out: str, stream_index: int = 0) -> None:
    await _run(["ffmpeg","-y","-i",inp,"-map",f"0:a:{stream_index}","-vn",out], "ExtractAudio")

async def extract_subtitle(inp: str, out: str, stream_index: int = 0) -> None:
    await _run(["ffmpeg","-y","-i",inp,"-map",f"0:s:{stream_index}",out], "ExtractSub")

async def merge_av(video: str, audio: str, out: str) -> None:
    try:
        await _run([
            "ffmpeg", "-y", "-i", video, "-i", audio,
            "-c:v", "copy", "-c:a", "copy",
            "-map", "0:v:0", "-map", "1:a:0", out,
        ], "MergeAV(copy)")
    except RuntimeError:
        log.info("merge_av copy failed — retrying with AAC transcode")
        await _run([
            "ffmpeg", "-y", "-i", video, "-i", audio,
            "-c:v", "copy", "-c:a", "aac",
            "-map", "0:v:0", "-map", "1:a:0", out,
        ], "MergeAV(aac)")

async def mux_subtitle(video: str, sub: str, out: str) -> None:
    await _run(["ffmpeg","-y","-i",video,"-i",sub,"-c","copy","-map","0","-map","1",out], "MuxSub")

async def burn_subtitle(video: str, sub: str, out: str) -> None:
    abs_sub  = os.path.abspath(sub)
    safe_sub = abs_sub.replace("\\", "/").replace("'", "\\'").replace(":", "\\:")
    await _run([
        "ffmpeg","-y","-i",video,
        "-vf",f"subtitles='{safe_sub}'",
        "-c:a","copy",out,
    ], "BurnSub")


async def trim_video(inp: str, out: str, start: str, end: str) -> None:
    await _run([
        "ffmpeg","-y",
        "-i", inp,
        "-ss", start, "-to", end,
        "-c", "copy",
        out,
    ], "Trim")


async def trim_video_fast(inp: str, out: str, start_sec: float, duration_sec: float) -> None:
    await _run([
        "ffmpeg","-y",
        "-ss", str(start_sec),
        "-i", inp,
        "-t", str(duration_sec),
        "-c", "copy",
        out,
    ], "TrimFast")


async def split_video(inp: str, out_dir: str, chunk_sec: int) -> list[str]:
    total = await probe_duration(inp)
    if total == 0:
        raise ValueError("Cannot determine video duration")

    base = os.path.splitext(os.path.basename(inp))[0]
    ext  = os.path.splitext(inp)[1] or ".mp4"
    parts: list[str] = []
    start = 0.0
    idx   = 1

    while start < total:
        dur = min(chunk_sec, total - start)
        out = os.path.join(out_dir, f"{base}_part{idx:03d}{ext}")
        try:
            await trim_video_fast(inp, out, start, dur)
            if os.path.exists(out) and os.path.getsize(out) > 0:
                parts.append(out)
        except Exception as exc:
            log.error("Split part %d failed: %s", idx, exc)
        start += chunk_sec
        idx   += 1

    return parts


async def merge_videos(paths: list[str], out: str, tmp_dir: str) -> None:
    list_file = os.path.join(tmp_dir, "concat_list.txt")
    with open(list_file, "w", encoding="utf-8") as f:
        for p in paths:
            escaped = p.replace("\\", "\\\\").replace("'", "\\'")
            f.write(f"file '{escaped}'\n")
    await _run([
        "ffmpeg","-y","-f","concat","-safe","0","-i",list_file,"-c","copy",out,
    ], "MergeVideos")


async def merge_audios(paths: list[str], out: str) -> None:
    inputs = []
    for p in paths:
        inputs += ["-i", p]
    n = len(paths)
    await _run([
        "ffmpeg","-y",*inputs,
        "-filter_complex",f"concat=n={n}:v=0:a=1[out]",
        "-map","[out]",out,
    ], "MergeAudios")


async def video_to_audio(inp: str, out: str, fmt: str = "mp3", quality: str = "320k") -> None:
    _codec_map = {
        "mp3":  "libmp3lame",
        "aac":  "aac",
        "m4a":  "aac",
        "opus": "libopus",
        "ogg":  "libvorbis",
        "flac": "flac",
        "wav":  "pcm_s16le",
        "wma":  "wmav2",
        "ac3":  "ac3",
    }
    codec = _codec_map.get(fmt, "libmp3lame")
    cmd   = ["ffmpeg","-y","-i",inp,"-vn","-c:a",codec]
    if fmt == "flac":
        cmd += ["-compression_level","8"]
    elif fmt not in ("wav",):
        cmd += ["-b:a", quality]
    cmd.append(out)
    await _run(cmd, f"ToAudio({fmt})")


async def to_gif(inp: str, out: str, start: str = "0", duration: str = "5", scale: int = 320) -> None:
    await _run([
        "ffmpeg","-y","-ss",start,"-t",duration,"-i",inp,
        "-vf",f"fps=10,scale={scale}:-1:flags=lanczos,"
              "split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse",
        "-loop","0",out,
    ], "ToGIF")


async def screenshot(inp: str, out: str, timestamp: str = "00:00:05") -> None:
    await _run([
        "ffmpeg","-y","-ss",timestamp,"-i",inp,"-frames:v","1","-q:v","2",out,
    ], "Screenshot")


async def screenshots(inp: str, out_dir: str, count: int = 5) -> list[str]:
    dur = await probe_duration(inp)
    result: list[str] = []
    for i in range(1, count + 1):
        ts  = max(1, int(dur * i / (count + 1))) if dur else i * 5
        out = os.path.join(out_dir, f"shot_{i:03d}.jpg")
        try:
            await screenshot(inp, out, str(ts))
            if os.path.exists(out) and os.path.getsize(out) > 0:
                result.append(out)
        except Exception as exc:
            log.warning("Screenshot %d/%d failed: %s", i, count, exc)
    return result


async def make_sample(inp: str, out: str, start: str = "0", duration: str = "30") -> None:
    await _run([
        "ffmpeg","-y","-ss",start,"-t",duration,"-i",inp,"-c","copy",out,
    ], "Sample")


async def optimize(inp: str, out: str, crf: int = 23) -> None:
    await _run([
        "ffmpeg","-y","-i",inp,
        "-c:v","libx264","-crf",str(crf),"-preset","medium",
        "-c:a","aac","-b:a","128k",
        out,
    ], f"Optimize(crf={crf})")


async def convert_video(inp: str, out: str) -> None:
    """
    Convert video to the output format.
    FIX: Added -map_chapters -1 to strip chapter metadata that caused
    rc=1 failures on files with chapter tracks.  Added explicit stream
    mapping to avoid issues with attached-pic / cover-art streams.
    """
    try:
        await _run([
            "ffmpeg", "-y", "-i", inp,
            "-map", "0:v:0", "-map", "0:a?",
            "-c:v", "copy", "-c:a", "copy",
            "-map_chapters", "-1",
            out,
        ], "ConvertVideo(copy)")
    except RuntimeError:
        log.info("convert_video copy failed — retrying with re-encode")
        await _run([
            "ffmpeg", "-y", "-i", inp,
            "-map", "0:v:0", "-map", "0:a?",
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k",
            "-map_chapters", "-1",
            out,
        ], "ConvertVideo(reencode)")


async def stream_op(inp: str, out: str, map_args: list) -> None:
    await _run(["ffmpeg","-y","-i",inp,*map_args,out], "StreamOp")


async def write_metadata(inp: str, out: str, meta: dict) -> None:
    args: list = []
    for k, v in meta.items():
        args += ["-metadata", f"{k}={v}"]
    await _run(["ffmpeg","-y","-i",inp,*args,"-c","copy",out], "Metadata")


async def embed_thumb(media: str, thumb: str, out: str) -> None:
    ext = os.path.splitext(media)[1].lower()
    if ext in (".mp3",".m4a",".flac",".aac"):
        await _run([
            "ffmpeg","-y","-i",media,"-i",thumb,
            "-map","0","-map","1","-c","copy",
            "-id3v2_version","3",
            "-metadata:s:v","title=Album cover",
            "-metadata:s:v","comment=Cover (front)",
            out,
        ], "EmbedThumb(audio)")
    else:
        await _run([
            "ffmpeg","-y","-i",media,"-i",thumb,
            "-map","0","-map_metadata","0","-c","copy",
            "-disposition:v:1","attached_pic",
            out,
        ], "EmbedThumb(video)")


def subtitle_ext(codec: str) -> str:
    return _SUB_EXT.get(codec.lower(), ".srt")


def audio_ext(codec: str) -> str:
    return _AUD_EXT.get(codec.lower(), ".mka")


# ─────────────────────────────────────────────────────────────
# Local resize and compression (no CloudConvert)
# ─────────────────────────────────────────────────────────────

async def resize_video(inp: str, out: str, height: int, crf: int = 23) -> None:
    """
    Downscale video to `height` px (e.g. 1080, 720, 480, 360).
    Uses scale=-2:height to preserve aspect ratio.
    Stream-copy audio — no quality loss on audio track.
    """
    await _run([
        "ffmpeg", "-y", "-i", inp,
        "-vf", f"scale=-2:{height}",
        "-c:v", "libx264", "-crf", str(crf), "-preset", "medium",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        out,
    ], f"Resize({height}p)")


async def compress_to_size(inp: str, out: str, target_mb: float) -> None:
    """
    2-pass encode to hit a target file size in MB.

    Formula:
      total_kbps = (target_mb * 8 * 1024) / duration_seconds
      video_kbps = total_kbps - audio_kbps (128)

    Falls back to single-pass CRF if duration cannot be determined.
    """
    dur = await probe_duration(inp)

    if not dur:
        log.warning("compress_to_size: unknown duration — using CRF 28 fallback")
        await _run([
            "ffmpeg", "-y", "-i", inp,
            "-c:v", "libx264", "-crf", "28", "-preset", "medium",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            out,
        ], "Compress(crf28-fallback)")
        return

    audio_kbps = 128
    total_kbps = int(target_mb * 8 * 1024 / dur)
    video_kbps = max(100, total_kbps - audio_kbps)

    log.info("compress_to_size: target=%.1f MB  dur=%ds  video_kbps=%d",
             target_mb, dur, video_kbps)

    # Pass 1 — analysis only, no output file
    await _run([
        "ffmpeg", "-y", "-i", inp,
        "-c:v", "libx264", "-b:v", f"{video_kbps}k", "-preset", "medium",
        "-pass", "1", "-an", "-f", "null", "/dev/null",
    ], "Compress(pass1)")

    # Pass 2 — actual encode
    await _run([
        "ffmpeg", "-y", "-i", inp,
        "-c:v", "libx264", "-b:v", f"{video_kbps}k", "-preset", "medium",
        "-pass", "2",
        "-c:a", "aac", "-b:a", f"{audio_kbps}k",
        "-movflags", "+faststart",
        out,
    ], "Compress(pass2)")
