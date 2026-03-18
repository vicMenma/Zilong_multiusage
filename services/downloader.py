"""
services/downloader.py
Download strategies — decoupled from Telegram types.

Change vs original:
- _tracked_progress inside smart_download now calls runner._wake_panel(user_id)
  so the live panel refreshes immediately on every progress tick instead of
  waiting for the 1.5s EDIT_INTERVAL timer.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Callable, Awaitable, Optional

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

_MAGNET_RE    = re.compile(r"^magnet:\?", re.I)
_TORRENT_RE   = re.compile(r"\.torrent(\?.*)?$", re.I)
_GDRIVE_RE    = re.compile(r"drive\.google\.com", re.I)
_MF_RE        = re.compile(r"mediafire\.com", re.I)
_YTDLP_RE     = re.compile(
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
    import aiohttp
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

def _ytdlp_worker(
    url: str,
    dest: str,
    audio_only: bool,
    fmt_id: Optional[str],
) -> str:
    import yt_dlp

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

    with yt_dlp.YoutubeDL(opts) as ydl:
        info  = ydl.extract_info(url, download=True)
        fpath = ydl.prepare_filename(info)

    if not os.path.exists(fpath):
        base = os.path.splitext(fpath)[0]
        for ext in (".mp3", ".m4a", ".opus", ".ogg", ".aac", ".mp4", ".mkv", ".webm"):
            candidate = base + ext
            if os.path.exists(candidate):
                return candidate
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
    loop = asyncio.get_event_loop()
    pool = _get_pool()

    future = loop.run_in_executor(
        pool,
        _ytdlp_worker,
        url, dest, audio_only, fmt_id,
    )

    start     = time.time()
    last_size = 0
    last_time = start

    while not future.done():
        await asyncio.sleep(1.0)
        if progress:
            try:
                cur = largest_file(dest)
                cur_size = os.path.getsize(cur) if cur else 0
                now      = time.time()
                dt       = now - last_time
                speed    = (cur_size - last_size) / dt if dt > 0 else 0.0
                last_size = cur_size
                last_time = now
                eta = 0
                await progress(cur_size, 0, speed, eta)
            except Exception:
                pass

    return await future


# ── Mediafire ─────────────────────────────────────────────────

async def download_mediafire(
    url: str, dest: str, progress: Optional[ProgressCB] = None
) -> str:
    import aiohttp
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
        raise ValueError("Cannot extract Mediafire direct link. Page may require login.")
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

    sa = sa_json or cfg.gdrive_sa_json
    creds = None
    if sa and os.path.exists(sa):
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(
            sa, scopes=["https://www.googleapis.com/auth/drive.readonly"])

    svc  = build("drive", "v3", credentials=creds, cache_discovery=False)
    meta = svc.files().get(fileId=file_id, fields="name,size").execute()
    fname = meta.get("name", "gdrive_file")
    total = int(meta.get("size", 0))
    fpath = os.path.join(dest, fname)

    request = svc.files().get_media(fileId=file_id)
    start   = time.time()
    with open(fpath, "wb") as fh:
        dl = MediaIoBaseDownload(fh, request, chunksize=10 * 1024 * 1024)
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


# ── Aria2 (magnet / torrent) ──────────────────────────────────

async def download_aria2(
    uri_or_path: str, dest: str,
    is_file: bool = False,
    progress: Optional[ProgressCB] = None,
    task_record=None,
) -> str:
    import aria2p

    api = aria2p.API(aria2p.Client(
        host=cfg.aria2_host, port=cfg.aria2_port, secret=cfg.aria2_secret
    ))

    aria_opts = {
        "dir":                        dest,
        "seed-time":                  "0",
        "max-connection-per-server":  "16",
        "split":                      "16",
        "min-split-size":             "1M",
        "bt-max-peers":               "200",
        "follow-torrent":             "mem",
    }

    if is_file:
        import base64
        with open(uri_or_path, "rb") as f:
            data = f.read()
        dl = api.add_torrent(base64.b64encode(data).decode(), options=aria_opts)
    else:
        dl = api.add_magnet(uri_or_path, options=aria_opts)

    # ── Phase 1: metadata fetch ───────────────────────────────
    if task_record is not None:
        task_record.update(meta_phase=True, state="🔍 Fetching metadata…")

    meta_start = time.time()
    for _ in range(120):
        await asyncio.sleep(1)
        try:
            dl = api.get_download(dl.gid)
        except Exception:
            continue
        if dl.error_message:
            raise RuntimeError(f"aria2c: {dl.error_message}")
        if task_record is not None:
            task_record.update(
                meta_phase=True,
                state="🔍 Metadata…",
                elapsed=time.time() - meta_start,
            )
        if dl.name and dl.name != "Unknown":
            break

    if task_record is not None:
        task_record.update(
            meta_phase=False,
            state="📥 Downloading",
            label=dl.name[:40] if dl.name else task_record.label,
            fname=dl.name[:40] if dl.name else task_record.fname,
        )

    # ── Phase 2: actual download ──────────────────────────────
    dl_start = time.time()
    while True:
        await asyncio.sleep(2)
        try:
            dl = api.get_download(dl.gid)
        except Exception:
            await asyncio.sleep(5)
            continue
        if dl.error_message:
            raise RuntimeError(f"aria2c: {dl.error_message}")
        if dl.is_complete:
            break

        total    = dl.total_length     or 0
        done     = dl.completed_length or 0
        speed    = dl.download_speed   or 0.0
        eta      = int((total - done) / speed) if speed else 0
        seeds    = getattr(dl, "num_seeders", 0) or 0
        elapsed  = time.time() - dl_start

        if task_record is not None:
            task_record.update(
                done=done, total=total,
                speed=speed, eta=eta, seeds=seeds,
                elapsed=elapsed,
                state="📥 Downloading",
            )
            # Wake the panel immediately on each update
            try:
                from services.task_runner import runner
                runner._wake_panel(task_record.user_id)
            except Exception:
                pass

        if progress:
            await progress(done, total, speed, eta)

        if time.time() - dl_start > 3600 * 6:
            raise TimeoutError("Torrent download timed out (6h)")

    result = largest_file(dl.dir or dest) or largest_file(dest)
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

    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=user_id,
        label=label or url.split("/")[-1].split("?")[0][:40] or "Download",
        mode="magnet" if kind in ("magnet", "torrent") else "dl",
        engine=engine,
    )
    await tracker.register(record)

    async def _tracked_progress(done: int, total: int, speed: float, eta: int) -> None:
        record.update(done=done, total=total, speed=speed, eta=eta, state="📥 Downloading")
        # Wake the panel on every progress tick — no waiting for 1.5s timer
        runner._wake_panel(user_id)
        if progress:
            await progress(done, total, speed, eta)

    try:
        result = await _dispatch(
            url, dest, kind, audio_only, fmt_id, sa_json,
            _tracked_progress, record,
        )
        record.update(state="✅ Done")
        runner._wake_panel(user_id)
        return result
    except Exception as exc:
        record.update(state=f"❌ {str(exc)[:50]}")
        runner._wake_panel(user_id)
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
    return await download_direct(url, dest, progress)
