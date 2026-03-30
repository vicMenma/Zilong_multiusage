"""
services/downloader.py (OPTIMIZED)
Download strategies — decoupled from Telegram types.

═══════════════════════════════════════════════════════════════════
FIXES in this version:
  - download_aria2 now uses proper TWO-PHASE timeout for magnets:
      Phase 1: up to 3 min to resolve metadata (no peers → fail fast)
      Phase 2: once download starts, full 6h allowed
    Previously, ALL magnets used a single 3-min timeout, meaning a
    1.4 GB file downloading at 8 MB/s would fail after 3 min with
    the wrong error message "6 h limit".
  - Error messages now accurately reflect which phase timed out.

OPTIMIZATIONS:
  1. aiohttp chunk size: 4 MB (reduces syscall overhead)
  2. Connection pooling: reusable TCPConnector with limit=32
  3. Parallel range download for direct HTTP (8 segments)
  4. aria2c: --file-allocation=none for instant start
  5. yt-dlp: concurrent_fragment_downloads=8
═══════════════════════════════════════════════════════════════════
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

_CONNECTOR: Optional[aiohttp.TCPConnector] = None
_DL_CHUNK_SIZE = 4 * 1024 * 1024
_DL_TIMEOUT = aiohttp.ClientTimeout(
    total=8 * 3600,
    connect=30,
    sock_read=300,
)


def _get_connector() -> aiohttp.TCPConnector:
    global _CONNECTOR
    if _CONNECTOR is None or _CONNECTOR.closed:
        _CONNECTOR = aiohttp.TCPConnector(
            limit=32,
            limit_per_host=16,
            ttl_dns_cache=600,
            enable_cleanup_closed=True,
            force_close=False,
        )
    return _CONNECTOR


def _get_session(**kwargs) -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        connector=_get_connector(),
        connector_owner=False,
        timeout=_DL_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ZilongBot/2.0)"},
        **kwargs,
    )


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


# ── Direct HTTP download ──────────────────────────────────────

async def download_direct(
    url: str, dest: str, progress: Optional[ProgressCB] = None
) -> str:
    start = time.time()

    async with _get_session() as sess:
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
                async for chunk in resp.content.iter_chunked(_DL_CHUNK_SIZE):
                    f.write(chunk)
                    done += len(chunk)
                    if progress:
                        elapsed = time.time() - start
                        speed   = done / elapsed if elapsed else 0
                        eta     = int((total - done) / speed) if (speed and total) else 0
                        await progress(done, total, speed, eta)
    return fpath


# ── Parallel range download ───────────────────────────────────

async def download_parallel(
    url: str, dest: str,
    num_segments: int = 8,
    progress: Optional[ProgressCB] = None,
) -> str:
    async with _get_session() as sess:
        async with sess.head(url, allow_redirects=True) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            accepts_range = resp.headers.get("Accept-Ranges", "").lower() == "bytes"

            cd    = resp.headers.get("Content-Disposition", "")
            fname = None
            if "filename=" in cd:
                fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
            if not fname:
                fname = Path(url.split("?")[0]).name or "download"
            fname = _urlparse.unquote_plus(fname)
            fname = re.sub(r'[\\/:*?"<>|]', "_", fname)

    if not accepts_range or total < 10 * 1024 * 1024 or total == 0:
        return await download_direct(url, dest, progress)

    fpath        = os.path.join(dest, fname)
    segment_size = total // num_segments
    done_bytes   = [0]
    start        = time.time()
    lock         = asyncio.Lock()

    async def _download_segment(seg_idx: int, start_byte: int, end_byte: int) -> bytes:
        headers = {"Range": f"bytes={start_byte}-{end_byte}"}
        data = bytearray()
        async with _get_session() as sess:
            async with sess.get(url, headers=headers, allow_redirects=True) as resp:
                async for chunk in resp.content.iter_chunked(_DL_CHUNK_SIZE):
                    data.extend(chunk)
                    async with lock:
                        done_bytes[0] += len(chunk)
                    if progress:
                        elapsed = time.time() - start
                        speed   = done_bytes[0] / elapsed if elapsed else 0
                        eta     = int((total - done_bytes[0]) / speed) if speed else 0
                        await progress(done_bytes[0], total, speed, eta)
        return bytes(data)

    tasks = []
    for i in range(num_segments):
        s = i * segment_size
        e = (i + 1) * segment_size - 1 if i < num_segments - 1 else total - 1
        tasks.append(_download_segment(i, s, e))

    segments = await asyncio.gather(*tasks)

    with open(fpath, "wb") as f:
        for seg in segments:
            f.write(seg)

    return fpath


# ── yt-dlp (process pool) ─────────────────────────────────────

def _ytdlp_worker(url: str, dest: str, audio_only: bool, fmt_id: Optional[str]) -> str:
    import yt_dlp as _ydlp

    out_tmpl = os.path.join(dest, "%(title).60s.%(ext)s")
    opts: dict = {
        "outtmpl":                       out_tmpl,
        "quiet":                         True,
        "no_warnings":                   True,
        "noplaylist":                    True,
        "restrictfilenames":             True,
        "concurrent_fragment_downloads": 8,
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
    async with _get_session() as sess:
        async with sess.get(url) as resp:
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


# ── Aria2 ─────────────────────────────────────────────────────

import re as _re

_ARIA2_PROG_RE = _re.compile(
    r"\[#\w+\s+([\d.]+\w+)/([\d.]+\w+)\((\d+)%\)"
    r"(?:.*?DL:([\d.]+\w+))?(?:.*?ETA:([\dhms]+))?"
)

META_TIMEOUT  = 180      # 3 min — how long to wait for FIRST progress line
TOTAL_TIMEOUT = 21600    # 6 h   — max time for the full download phase


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
    Download via aria2c subprocess.

    TWO-PHASE TIMEOUT FOR MAGNETS (FIX):
      Phase 1 (metadata): poll every second for up to META_TIMEOUT (3 min).
                          If aria2c never outputs a progress line (no peers),
                          we kill it and raise "metadata timeout (3 min)".
      Phase 2 (download): once first progress arrives, give it TOTAL_TIMEOUT (6h).

    Non-magnets and .torrent files use a single TOTAL_TIMEOUT.
    This prevents the old bug where large magnet downloads were killed at 3 min
    with the wrong message "6 h limit".
    """
    is_magnet = _MAGNET_RE.match(uri_or_path) is not None

    if is_file:
        cmd = [
            "aria2c",
            "-x16", "--split=16",
            "--min-split-size=1M",
            "--file-allocation=none",
            "--seed-time=0",
            "--summary-interval=1", "--console-log-level=notice",
            "--max-tries=3", "-d", dest,
            f"--torrent-file={uri_or_path}",
        ]
    else:
        cmd = [
            "aria2c",
            "-x16", "--split=16",
            "--min-split-size=1M",
            "--file-allocation=none",
            "--seed-time=0",
            "--bt-max-peers=200",
            "--summary-interval=1", "--console-log-level=notice",
            "--max-tries=3", "-d", dest,
            uri_or_path,
        ]

    in_meta = [is_magnet and task_record is not None and not is_file]

    if in_meta[0] and task_record is not None:
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
        """Read aria2c stdout and update progress. Sets in_meta[0]=False on first progress line."""
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
                    # First progress line — metadata resolved, now in download phase
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

    # ── TWO-PHASE HANDLING FOR MAGNETS ───────────────────────
    if is_magnet and not is_file:
        # Start the stdout reader as a background task
        read_task = asyncio.create_task(_read_stdout())

        # Phase 1: Poll until metadata resolves (first progress line) or timeout
        meta_deadline = time.time() + META_TIMEOUT
        while in_meta[0]:
            if read_task.done():
                break
            if time.time() >= meta_deadline:
                # No peers / dead magnet — kill and fail fast
                try:
                    proc.kill()
                except Exception:
                    pass
                read_task.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(read_task), timeout=5)
                except Exception:
                    pass
                if task_record is not None:
                    task_record.update(state="❌ Dead magnet")
                raise RuntimeError(
                    "aria2c timed out during metadata resolution (3 min limit). "
                    "The magnet may have no active peers or trackers."
                )
            await asyncio.sleep(1)

        # Phase 2: Metadata resolved — let download run for the full 6h
        if not read_task.done():
            try:
                remaining = TOTAL_TIMEOUT - int(time.time() - start)
                await asyncio.wait_for(read_task, timeout=max(remaining, 60))
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except Exception:
                    pass
                raise RuntimeError(
                    "aria2c timed out during download (6 h limit). "
                    "The download stalled."
                )

        # Re-raise any exception from the read task
        if not read_task.cancelled():
            exc = read_task.exception()
            if exc is not None:
                raise exc

    else:
        # Non-magnet (.torrent file, direct, etc.): single TOTAL_TIMEOUT
        try:
            await asyncio.wait_for(_read_stdout(), timeout=TOTAL_TIMEOUT)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            raise RuntimeError(
                "aria2c timed out during download (6 h limit). "
                "The download stalled or took too long."
            )

    await proc.wait()

    if proc.returncode not in (0, None):
        err = b""
        if proc.stderr:
            try:
                err = await asyncio.wait_for(proc.stderr.read(), timeout=3)
            except Exception:
                pass
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

    tid           = tracker.new_tid()
    initial_meta  = kind in ("magnet", "torrent")
    initial_state = "🔍 Fetching metadata…" if initial_meta else "📥 Starting…"
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
    import logging as _log
    _dlog = _log.getLogger(__name__)

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

    # direct — try parallel, then aria2c, then single stream
    try:
        return await download_parallel(url, dest, num_segments=8, progress=progress)
    except Exception as par_exc:
        _dlog.debug("[Downloader] Parallel range failed (%s) — trying aria2c", par_exc)

    try:
        return await download_aria2(
            url, dest, is_file=False,
            progress=progress, task_record=task_record,
        )
    except Exception as aria2_exc:
        _dlog.warning(
            "[Downloader] aria2c failed for direct URL (%s) — falling back to aiohttp",
            aria2_exc,
        )
        if task_record is not None:
            task_record.update(
                state="📥 Downloading", engine="direct", meta_phase=False,
            )
        return await download_direct(url, dest, progress=progress)


async def cleanup_connections() -> None:
    global _CONNECTOR
    if _CONNECTOR and not _CONNECTOR.closed:
        await _CONNECTOR.close()
        _CONNECTOR = None
