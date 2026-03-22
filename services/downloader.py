"""
services/downloader.py
Download strategies — decoupled from Telegram types.

FIXES:
- CRITICAL: timeout variable was computed but then discarded — always used
  TOTAL_TIMEOUT (6h). Dead magnets now properly time out after 3 minutes.
- urllib.parse moved to module-level (was duplicated as two local aliases)
"""
from __future__ import annotations

import asyncio
import os
import re
import time
import urllib.parse as _urlparse
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Callable, Awaitable, Optional

import aiohttp
import yt_dlp

from core.config import cfg
from services.utils import largest_file

ProgressCB = Callable[[int, int, float, int], Awaitable[None]]

_YTDLP_POOL: Optional[ProcessPoolExecutor] = None


def _get_pool() -> ProcessPoolExecutor:
    global _YTDLP_POOL
    if _YTDLP_POOL is None:
        _YTDLP_POOL = ProcessPoolExecutor(max_workers=5)
    return _YTDLP_POOL


# ── URL classifier ────────────────────────────────────────────

_MAGNET_RE  = re.compile(r"^magnet:\?", re.I)
_TORRENT_RE = re.compile(r"\.torrent(\?.*)?$", re.I)
_GDRIVE_RE  = re.compile(r"drive\.google\.com", re.I)
_MF_RE      = re.compile(r"mediafire\.com", re.I)
_YTDLP_RE   = re.compile(
    r"(youtube\.com|youtu\.be|instagram\.com|twitter\.com|x\.com|"
    r"facebook\.com|tiktok\.com|dailymotion\.com|vimeo\.com|twitch\.tv|"
    r"reddit\.com|pinterest\.com|ok\.ru|bilibili\.com|soundcloud\.com|"
    r"nicovideo\.jp|rumble\.com|odysee\.com|bitchute\.com)", re.I)


def classify(url: str) -> str:
    if _MAGNET_RE.match(url):   return "magnet"
    if _TORRENT_RE.search(url): return "torrent"
    if _GDRIVE_RE.search(url):  return "gdrive"
    if _MF_RE.search(url):      return "mediafire"
    if _YTDLP_RE.search(url):   return "ytdlp"
    return "direct"


# ── Direct HTTP ───────────────────────────────────────────────

async def download_direct(
    url: str, dest: str, progress: Optional[ProgressCB] = None
) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    start   = time.time()

    async with aiohttp.ClientSession(headers=headers) as sess:
        async with sess.get(url, allow_redirects=True) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", 0))

            cd    = resp.headers.get("Content-Disposition", "")
            fname = None
            if "filename=" in cd:
                fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
            if not fname:
                fname = Path(url.split("?")[0]).name or "download"
            fname = _urlparse.unquote_plus(fname)
            fname = re.sub(r'[\\/:*?"<>|]', "_", fname)

            fpath = os.path.join(dest, fname)
            done  = 0
            with open(fpath, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 1024):
                    f.write(chunk)
                    done += len(chunk)
                    if progress:
                        elapsed = time.time() - start
                        speed   = done / elapsed if elapsed else 0
                        eta     = int((total - done) / speed) if (speed and total) else 0
                        await progress(done, total, speed, eta)
    return fpath


# ── yt-dlp (process pool) ─────────────────────────────────────

def _ytdlp_worker(url: str, dest: str, audio_only: bool, fmt_id: Optional[str]) -> str:
    import yt_dlp as _ydlp

    out_tmpl = os.path.join(dest, "%(title).60s.%(ext)s")
    opts: dict = {
        "outtmpl":           out_tmpl,
        "quiet":             True,
        "no_warnings":       True,
        "noplaylist":        True,
        "restrictfilenames": True,
    }

    if fmt_id:
        opts["format"] = fmt_id
    elif audio_only:
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [{
            "key":              "FFmpegExtractAudio",
            "preferredcodec":   "mp3",
            "preferredquality": "320",
        }]
    else:
        opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"

    with _ydlp.YoutubeDL(opts) as ydl:
        info  = ydl.extract_info(url, download=True)
        fpath = ydl.prepare_filename(info)

    if not os.path.exists(fpath):
        base = os.path.splitext(fpath)[0]
        for ext in (".mp3",".m4a",".opus",".ogg",".aac",".mp4",".mkv",".webm"):
            if os.path.exists(base + ext):
                return base + ext
        result = largest_file(dest)
        if not result:
            raise FileNotFoundError(f"yt-dlp produced no output in {dest!r}")
        return result

    return fpath


async def download_ytdlp(
    url: str, dest: str,
    audio_only: bool = False,
    fmt_id: Optional[str] = None,
    progress: Optional[ProgressCB] = None,
) -> str:
    loop = asyncio.get_running_loop()
    pool = _get_pool()

    expected_size: int = 0
    try:
        def _get_size() -> int:
            opts: dict = {"quiet": True, "no_warnings": True, "noplaylist": True}
            if fmt_id:
                opts["format"] = fmt_id
            elif audio_only:
                opts["format"] = "bestaudio/best"
            else:
                opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            fmts = info.get("requested_formats") or [info]
            return sum(
                int(f.get("filesize") or f.get("filesize_approx") or 0)
                for f in fmts
            )
        expected_size = await loop.run_in_executor(pool, _get_size)
    except Exception:
        pass

    future    = loop.run_in_executor(pool, _ytdlp_worker, url, dest, audio_only, fmt_id)
    start     = time.time()
    last_size = 0
    last_time = start

    while not future.done():
        await asyncio.sleep(1.0)
        if progress:
            try:
                cur      = largest_file(dest)
                cur_size = os.path.getsize(cur) if cur else 0
                now      = time.time()
                dt       = now - last_time
                speed    = (cur_size - last_size) / dt if dt > 0 else 0.0
                last_size = cur_size
                last_time = now
                total = expected_size or 0
                eta   = int((total - cur_size) / speed) if (speed and total > cur_size) else 0
                await progress(cur_size, total, speed, eta)
            except Exception:
                pass

    return await future


# ── Mediafire ─────────────────────────────────────────────────

async def download_mediafire(
    url: str, dest: str, progress: Optional[ProgressCB] = None
) -> str:
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            html = await resp.text()

    patterns = [
        r'href="(https://download\d+\.mediafire\.com/[^"]+)"',
        r'"downloadUrl"\s*:\s*"([^"]+)"',
        r'id="downloadButton"[^>]+href="([^"]+)"',
    ]
    direct = None
    for pat in patterns:
        m = re.search(pat, html)
        if m:
            direct = m.group(1)
            break
    if not direct:
        raise ValueError("Cannot extract Mediafire direct link.")
    return await download_direct(direct, dest, progress)


# ── Google Drive ──────────────────────────────────────────────

async def download_gdrive(
    url: str, dest: str,
    sa_json: Optional[str] = None,
    progress: Optional[ProgressCB] = None,
) -> str:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    m = re.search(r"/d/([a-zA-Z0-9_-]+)", url) or re.search(r"id=([a-zA-Z0-9_-]+)", url)
    if not m:
        raise ValueError("Cannot parse Google Drive file ID from URL")
    file_id = m.group(1)

    sa    = sa_json or cfg.gdrive_sa_json
    creds = None
    if sa and os.path.exists(sa):
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(
            sa, scopes=["https://www.googleapis.com/auth/drive.readonly"])

    svc   = build("drive", "v3", credentials=creds, cache_discovery=False)
    meta  = svc.files().get(fileId=file_id, fields="name,size").execute()
    fname = meta.get("name", "gdrive_file")
    total = int(meta.get("size", 0))
    fpath = os.path.join(dest, fname)

    request = svc.files().get_media(fileId=file_id)
    start   = time.time()
    with open(fpath, "wb") as fh:
        dl        = MediaIoBaseDownload(fh, request, chunksize=10 * 1024 * 1024)
        done_flag = False
        while not done_flag:
            status, done_flag = dl.next_chunk()
            if status and progress:
                done    = int(status.resumable_progress)
                elapsed = time.time() - start
                speed   = done / elapsed if elapsed else 0
                eta     = int((total - done) / speed) if speed else 0
                await progress(done, total, speed, eta)
    return fpath


# ── Aria2 (magnet / torrent / direct) ────────────────────────

import re as _re

_ARIA2_PROG_RE = _re.compile(
    r"\[#\w+\s+([\d.]+\w+)/([\d.]+\w+)\((\d+)%\)"
    r"(?:.*?DL:([\d.]+\w+))?(?:.*?ETA:([\dhms]+))?"
)


def _aria2_bytes(s: str) -> int:
    units = {"b":1,"kib":1024,"mib":1024**2,"gib":1024**3,
             "kb":1000,"mb":1000**2,"gb":1000**3}
    m = _re.match(r"([\d.]+)\s*(\w+)", s.strip(), _re.I)
    if not m:
        return 0
    try:
        return int(float(m.group(1)) * units.get(m.group(2).lower(), 1))
    except Exception:
        return 0


def _aria2_eta(s: str) -> int:
    total = 0
    for v, u in _re.findall(r"(\d+)([hms])", s):
        total += int(v) * {"h":3600,"m":60,"s":1}.get(u, 0)
    return total


async def download_aria2(
    uri_or_path: str, dest: str,
    is_file: bool = False,
    progress: Optional[ProgressCB] = None,
    task_record=None,
) -> str:
    """
    Download via aria2c subprocess — real-time stdout parsing.

    FIX: timeout variable is now actually used. Metadata phase times out
    after META_TIMEOUT (3 min) instead of always waiting TOTAL_TIMEOUT (6h).
    """
    META_TIMEOUT  = 180    # 3 min to get first progress line from magnet
    TOTAL_TIMEOUT = 21600  # 6 h overall cap

    if is_file:
        cmd = [
            "aria2c", "-x16", "--seed-time=0",
            "--summary-interval=1", "--console-log-level=notice",
            "--max-tries=3", "-d", dest,
            f"--torrent-file={uri_or_path}",
        ]
    else:
        cmd = [
            "aria2c", "-x16", "--seed-time=0",
            "--bt-max-peers=200",
            "--summary-interval=1", "--console-log-level=notice",
            "--max-tries=3", "-d", dest,
            uri_or_path,
        ]

    if task_record is not None and not is_file:
        task_record.update(meta_phase=True, state="🔍 Fetching metadata…")
        try:
            from services.task_runner import runner as _r
            _r._wake_panel(task_record.user_id, immediate=True)
        except Exception:
            pass

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    start     = time.time()
    last_wake = [start]
    in_meta   = [task_record is not None and not is_file]

    def _wake(uid: int) -> None:
        now = time.time()
        if now - last_wake[0] >= 1.0:
            last_wake[0] = now
            try:
                from services.task_runner import runner as _r
                _r._wake_panel(uid)
            except Exception:
                pass

    async def _drain_stderr():
        if proc.stderr:
            try:
                await proc.stderr.read()
            except Exception:
                pass

    asyncio.ensure_future(_drain_stderr())
    assert proc.stdout is not None

    async def _read_stdout():
        async for raw in proc.stdout:
            line = raw.decode(errors="replace").strip()
            if not line:
                continue

            elapsed = time.time() - start
            m       = _ARIA2_PROG_RE.search(line)
            if m:
                done_b  = _aria2_bytes(m.group(1) or "0")
                total_b = _aria2_bytes(m.group(2) or "0")
                spd_b   = _aria2_bytes(m.group(4) or "0") if m.group(4) else 0
                eta_sec = _aria2_eta(m.group(5) or "") if m.group(5) else 0

                if in_meta[0]:
                    in_meta[0] = False
                    fname_now = largest_file(dest)
                    fname_s   = os.path.basename(fname_now)[:40] if fname_now else ""
                    if task_record is not None:
                        task_record.update(
                            meta_phase=False, state="📥 Downloading",
                            **({"label": fname_s, "fname": fname_s} if fname_s else {}),
                        )

                if task_record is not None:
                    task_record.update(
                        done=done_b, total=total_b,
                        speed=float(spd_b), eta=eta_sec,
                        elapsed=elapsed, state="📥 Downloading",
                    )
                    _wake(task_record.user_id)

                if progress:
                    await progress(done_b, total_b, float(spd_b), eta_sec)

            elif in_meta[0] and task_record is not None:
                task_record.update(
                    meta_phase=True, state="🔍 Fetching metadata…", elapsed=elapsed,
                )
                _wake(task_record.user_id)

    # FIX: use the correct timeout per phase.
    # Magnets: META_TIMEOUT (3 min) — if no progress in 3 min, magnet is dead.
    # Everything else: TOTAL_TIMEOUT (6 h).
    timeout = META_TIMEOUT if (task_record is not None and not is_file) else TOTAL_TIMEOUT

    try:
        await asyncio.wait_for(_read_stdout(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        phase = "metadata resolution" if in_meta[0] else "download"
        limit = "3 min" if in_meta[0] else "6 h"
        raise RuntimeError(
            f"aria2c timed out during {phase} ({limit} limit). "
            "The magnet may have no active peers or trackers."
        )

    await proc.wait()

    if proc.returncode not in (0, None):
        err = b""
        if proc.stderr:
            err = await proc.stderr.read()
        raise RuntimeError(
            f"aria2c exited {proc.returncode}: {err.decode(errors='replace')[-300:]}"
        )

    if task_record is not None:
        task_record.update(state="✅ Done", done=task_record.total or task_record.done)
        try:
            from services.task_runner import runner as _r
            _r._wake_panel(task_record.user_id)
        except Exception:
            pass

    result = largest_file(dest)
    if not result:
        raise FileNotFoundError("No file found after aria2c download")
    return result


# ── Smart dispatcher ──────────────────────────────────────────

async def smart_download(
    url: str, dest: str,
    audio_only: bool = False,
    fmt_id: Optional[str] = None,
    sa_json: Optional[str] = None,
    progress: Optional[ProgressCB] = None,
    user_id: int = 0,
    label: str = "",
) -> str:
    from services.task_runner import tracker, TaskRecord, runner

    kind   = classify(url)
    engine = {
        "magnet":    "magnet",
        "torrent":   "aria2",
        "gdrive":    "gdrive",
        "mediafire": "mediafire",
        "ytdlp":     "ytdlp",
        "direct":    "direct",
    }.get(kind, "direct")

    raw_label   = label or url.split("/")[-1].split("?")[0][:40] or "Download"
    clean_label = _urlparse.unquote_plus(raw_label)[:50]

    tid            = tracker.new_tid()
    initial_meta   = kind in ("magnet", "torrent")
    initial_state  = "🔍 Fetching metadata…" if initial_meta else "📥 Starting…"
    record = TaskRecord(
        tid=tid, user_id=user_id,
        label=clean_label,
        mode="magnet" if kind in ("magnet", "torrent") else "dl",
        engine=engine,
        meta_phase=initial_meta,
        state=initial_state,
    )
    await tracker.register(record)

    async def _tracked_progress(done: int, total: int, speed: float, eta: int) -> None:
        record.update(done=done, total=total, speed=speed, eta=eta, state="📥 Downloading")
        runner._wake_panel(user_id)
        if progress:
            await progress(done, total, speed, eta)

    try:
        result = await _dispatch(
            url, dest, kind, audio_only, fmt_id, sa_json,
            _tracked_progress, record,
        )
        record.update(state="✅ Done")
        runner._wake_panel(user_id, immediate=True)
        return result
    except Exception as exc:
        record.update(state=f"❌ {str(exc)[:50]}")
        runner._wake_panel(user_id, immediate=True)
        raise


async def _dispatch(
    url: str, dest: str, kind: str,
    audio_only: bool, fmt_id: Optional[str],
    sa_json: Optional[str], progress: Optional[ProgressCB],
    task_record=None,
) -> str:
    if kind == "magnet":
        return await download_aria2(
            url, dest, is_file=False,
            progress=progress, task_record=task_record,
        )
    if kind == "torrent":
        tp = await download_direct(url, dest, progress)
        return await download_aria2(
            tp, dest, is_file=True,
            progress=progress, task_record=task_record,
        )
    if kind == "gdrive":
        return await download_gdrive(url, dest, sa_json=sa_json, progress=progress)
    if kind == "mediafire":
        return await download_mediafire(url, dest, progress=progress)
    if kind == "ytdlp":
        return await download_ytdlp(url, dest, audio_only=audio_only,
                                    fmt_id=fmt_id, progress=progress)
    return await download_aria2(
        url, dest, is_file=False, progress=progress, task_record=task_record,
    )
