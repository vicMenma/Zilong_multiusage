"""
services/seedr.py
Seedr.cc cloud torrent client — uses the official REST API v1.

═══════════════════════════════════════════════════════════════════
OFFICIAL SEEDR REST API v1 (from https://www.seedr.cc/docs/api/rest/v1/)
  Authentication: HTTP Basic Auth (email:password) — NO OAuth tokens needed

  POST /rest/transfer/magnet   data: magnet={magnet_link}  ← add magnet
  POST /rest/transfer/url      data: url={url}             ← add URL
  GET  /rest/folder            → list root folder
  GET  /rest/folder/{id}       → list subfolder
  GET  /rest/file/{id}         → download file (follow redirects)
  GET  /rest/user              → account info / storage
  DELETE /rest/folder/{id}     → delete folder

SETUP (.env):
    SEEDR_USERNAME=your@email.com
    SEEDR_PASSWORD=yourpassword

  Optional proxy (for cloud IPs like Google Colab):
    SEEDR_PROXY=http://user:pass@host:port
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import urllib.parse as _up
from typing import Optional

import httpx

log = logging.getLogger(__name__)

_BASE = "https://www.seedr.cc/rest"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _creds() -> tuple[str, str]:
    u = os.environ.get("SEEDR_USERNAME", "").strip()
    p = os.environ.get("SEEDR_PASSWORD", "").strip()
    if not u or not p:
        raise RuntimeError(
            "Seedr credentials not configured.\n"
            "Add to your .env:\n"
            "  SEEDR_USERNAME=your@email.com\n"
            "  SEEDR_PASSWORD=yourpassword"
        )
    return u, p


def _get_proxy() -> Optional[str]:
    return os.environ.get("SEEDR_PROXY", "").strip() or None


def _client() -> httpx.AsyncClient:
    """Return an httpx client pre-configured with auth + optional proxy."""
    username, password = _creds()
    proxy = _get_proxy()
    if proxy:
        log.info("[Seedr] Using proxy: %s", re.sub(r":([^@/]+)@", ":***@", proxy))
    return httpx.AsyncClient(
        auth=(username, password),
        proxy=proxy,
        headers=_BROWSER_HEADERS,
        timeout=60,
        follow_redirects=True,
    )


async def _get(path: str) -> dict:
    async with _client() as http:
        r = await http.get(f"{_BASE}{path}")
        r.raise_for_status()
        return r.json()


async def _post(path: str, data: dict) -> dict:
    async with _client() as http:
        r = await http.post(f"{_BASE}{path}", data=data)
        r.raise_for_status()
        return r.json()


async def _delete(path: str) -> dict:
    async with _client() as http:
        r = await http.delete(f"{_BASE}{path}")
        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            return {"result": True}


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    """Return True if credentials are valid."""
    try:
        await _get("/user")
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    """Return {used, total, free} in bytes."""
    data = await _get("/user")
    # REST v1 user endpoint returns storage info
    total = int(data.get("space_max", data.get("storage_total", 0)))
    used  = int(data.get("space_used", data.get("storage_used", 0)))
    return {"total": total, "used": used, "free": total - used}


async def add_magnet(magnet: str) -> dict:
    """
    Submit a magnet link to Seedr via the official REST API.
    POST /rest/transfer/magnet  with Basic Auth.
    """
    proxy = _get_proxy()
    if proxy:
        log.info("[Seedr] Proxy active: %s", re.sub(r":([^@/]+)@", ":***@", proxy))

    log.info("[Seedr] Submitting magnet via REST API...")
    result = await _post("/transfer/magnet", {"magnet": magnet})
    log.info("[Seedr] Transfer submitted: %s", result)
    return result


async def list_folder(folder_id: int = 0) -> dict:
    """
    List folder contents. Returns a plain dict with:
      folders: [{id, name, size}, ...]
      files:   [{id, name, size}, ...]
      torrents: [{id, name, progress}, ...]
    """
    path = "/folder" if folder_id == 0 else f"/folder/{folder_id}"
    data = await _get(path)

    folders = [
        {"id": f.get("id"), "name": f.get("name"), "size": f.get("size", 0)}
        for f in data.get("folders", [])
    ]
    files = [
        {
            "folder_file_id": f.get("id"),
            "id": f.get("id"),
            "name": f.get("name"),
            "size": f.get("size", 0),
        }
        for f in data.get("files", [])
    ]
    # REST v1 shows active transfers inside the folder response
    torrents = [
        {
            "id": t.get("id"),
            "name": t.get("name", ""),
            "progress": str(t.get("progress", "0")),
            "size": t.get("size", 0),
        }
        for t in data.get("transfers", data.get("torrents", []))
    ]

    return {
        "folders":    folders,
        "files":      files,
        "torrents":   torrents,
        "space_used": data.get("space_used"),
        "space_max":  data.get("space_max"),
    }


async def get_file_download_url(file_id: int) -> str:
    """
    Get direct download URL for a file.
    GET /rest/file/{id} with follow_redirects gives the final CDN URL.
    """
    username, password = _creds()
    proxy = _get_proxy()
    async with httpx.AsyncClient(
        auth=(username, password),
        proxy=proxy,
        headers=_BROWSER_HEADERS,
        timeout=60,
        follow_redirects=False,   # we want the redirect Location, not the file
    ) as http:
        r = await http.get(f"{_BASE}/file/{file_id}")
        if r.status_code in (301, 302, 303, 307, 308):
            url = r.headers.get("location", "")
            log.info("[Seedr] File %s redirect → %s", file_id, url[:80])
            return url
        # If no redirect, the response body IS the file — return the URL directly
        return f"{_BASE}/file/{file_id}"


async def delete_folder(folder_id: int) -> None:
    """Delete a folder from Seedr to reclaim quota."""
    await _delete(f"/folder/{folder_id}")
    log.info("[Seedr] Deleted folder id=%d", folder_id)


# ─────────────────────────────────────────────────────────────
# Poll until torrent finishes
# ─────────────────────────────────────────────────────────────

async def poll_until_ready(
    torrent_name_hint: str = "",
    timeout_s: int = 3600,
    progress_cb=None,
    existing_folder_ids: Optional[set] = None,
) -> dict:
    """
    Poll Seedr root folder until the torrent finishes.
    Returns folder dict {id, name, size}.
    """
    if existing_folder_ids is None:
        existing_folder_ids = set()

    deadline   = time.time() + timeout_s
    last_pct   = -1.0

    while time.time() < deadline:
        try:
            root     = await list_folder(0)
            folders  = root.get("folders", [])
            torrents = root.get("torrents", [])

            # Active downloads
            downloading = []
            for t in torrents:
                try:
                    pct = float(t.get("progress", "100"))
                except (ValueError, TypeError):
                    pct = 100.0
                if pct < 100:
                    downloading.append({**t, "_pct": pct})

            # New completed folders not in baseline
            new_folders = [
                f for f in folders
                if f.get("id") not in existing_folder_ids
                and (
                    not torrent_name_hint
                    or torrent_name_hint.lower()[:20] in f.get("name", "").lower()
                )
            ]

            if downloading:
                dl  = downloading[0]
                pct = dl["_pct"]
                if pct != last_pct:
                    last_pct = pct
                    name = dl.get("name", "")
                    log.info("[Seedr] Progress: %.1f%%  %s", pct, name)
                    if progress_cb:
                        await progress_cb(pct, name)
            elif new_folders:
                folder = new_folders[-1]
                log.info("[Seedr] Ready: %s (id=%s)", folder.get("name"), folder.get("id"))
                return folder
            else:
                log.debug("[Seedr] No new folder yet — waiting…")

        except Exception as e:
            log.warning("[Seedr] Poll error: %s", e)

        await asyncio.sleep(10)

    raise RuntimeError(f"Seedr download timed out after {timeout_s}s")


# ─────────────────────────────────────────────────────────────
# Get all file URLs (recursive)
# ─────────────────────────────────────────────────────────────

async def get_file_urls(folder_id: int) -> list[dict]:
    """Return [{name, url, size}, ...] for every file in folder_id."""
    result: list[dict] = []

    async def _collect(fid: int) -> None:
        contents = await list_folder(fid)
        for f in contents.get("files", []):
            file_id = f.get("folder_file_id") or f.get("id")
            name    = f.get("name", "file")
            size    = int(f.get("size", 0))
            if not file_id:
                continue
            try:
                dl_url = await get_file_download_url(file_id)
                if dl_url:
                    result.append({"name": name, "url": dl_url, "size": size})
            except Exception as e:
                log.warning("[Seedr] File URL fetch failed for %s: %s", name, e)

        for sub in contents.get("folders", []):
            sub_id = sub.get("id")
            if sub_id:
                try:
                    await _collect(sub_id)
                except Exception as e:
                    log.warning("[Seedr] Sub-folder error: %s", e)

    await _collect(folder_id)
    return result


# ─────────────────────────────────────────────────────────────
# High-level pipeline
# ─────────────────────────────────────────────────────────────

async def download_via_seedr(
    magnet:      str,
    dest:        str,
    progress_cb  = None,
    timeout_s:   int = 3600,
) -> list[str]:
    """
    Full pipeline: add magnet → poll → fetch URLs → download → cleanup.
    Returns list of local file paths in `dest`.
    """
    from services.downloader import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("adding", 0.0, "Submitting to Seedr…")

    # Snapshot existing folders BEFORE adding magnet
    existing_folder_ids: set = set()
    try:
        root = await list_folder(0)
        existing_folder_ids = {
            f.get("id") for f in root.get("folders", [])
            if f.get("id") is not None
        }
        log.info("[Seedr] Baseline: %d existing folder(s)", len(existing_folder_ids))
    except Exception as exc:
        log.warning("[Seedr] Snapshot failed: %s", exc)

    await add_magnet(magnet)

    if progress_cb:
        await progress_cb("waiting", 0.0, "Seedr is downloading…")

    dn_match  = re.search(r"[&?]dn=([^&]+)", magnet)
    name_hint = _up.unquote_plus(dn_match.group(1))[:50] if dn_match else ""

    async def _poll_progress(pct: float, name: str) -> None:
        if progress_cb:
            await progress_cb("downloading", pct, name)

    folder = await poll_until_ready(
        torrent_name_hint=name_hint,
        timeout_s=timeout_s,
        progress_cb=_poll_progress,
        existing_folder_ids=existing_folder_ids,
    )
    folder_id = folder["id"]

    if progress_cb:
        await progress_cb("fetching", 100.0, "Getting download links…")

    files = await get_file_urls(folder_id)
    if not files:
        raise RuntimeError("Seedr returned no files.")

    os.makedirs(dest, exist_ok=True)
    local_paths = []

    for i, f in enumerate(files):
        raw_name   = f["name"]
        clean_name = sanitize_filename(raw_name)

        if progress_cb:
            await progress_cb(
                "dl_file",
                (i / len(files)) * 100,
                f"Downloading {clean_name} ({i+1}/{len(files)})…",
            )

        log.info("[Seedr] Downloading %s → %s", raw_name, clean_name)
        try:
            path = await download_direct(f["url"], dest)
            if os.path.basename(path) != clean_name:
                new_path = os.path.join(os.path.dirname(path), clean_name)
                try:
                    os.rename(path, new_path)
                    path = new_path
                except OSError:
                    pass
            local_paths.append(path)
        except Exception as e:
            log.error("[Seedr] Download failed for %s: %s", raw_name, e)

    try:
        await delete_folder(folder_id)
    except Exception as e:
        log.warning("[Seedr] Cleanup failed: %s", e)

    return local_paths
