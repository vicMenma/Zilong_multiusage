"""
services/seedr.py
Seedr.cc cloud torrent client — REST API v1 with HTTP Basic Auth.

═══════════════════════════════════════════════════════════════════
WHY THIS APPROACH:
  The seedrcc library keeps changing method names between versions
  (v1: addTorrent, v2: add_torrent, but NOT add_magnet → crash).
  The REST API is stable, documented, needs ZERO extra packages.

  REST API v1 docs: https://www.seedr.cc/docs/api/rest/v1/
  Auth: HTTP Basic Auth (email:password)

  Endpoints used:
    POST /rest/transfer/magnet   — add magnet link
    GET  /rest/folder             — list root folder
    GET  /rest/folder/{id}        — list folder contents
    GET  /rest/file/{id}          — download file (follows redirect)
    POST /rest/folder/{id}/delete — delete folder
    GET  /rest/user               — account info

SETUP (.env):
    SEEDR_USERNAME=your@email.com
    SEEDR_PASSWORD=yourpassword
═══════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import urllib.parse as _up
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_REST_BASE = "https://www.seedr.cc/rest"


# ─────────────────────────────────────────────────────────────
# Auth helper
# ─────────────────────────────────────────────────────────────

def _basic_auth() -> aiohttp.BasicAuth:
    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError(
            "Seedr credentials not configured.\n"
            "Add to your .env:\n"
            "  SEEDR_USERNAME=your@email.com\n"
            "  SEEDR_PASSWORD=yourpassword"
        )
    return aiohttp.BasicAuth(username, password)


# ─────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────

async def _rest_get(path: str, timeout_sec: int = 60) -> dict:
    auth    = _basic_auth()
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    url     = f"{_REST_BASE}/{path}"
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as sess:
        async with sess.get(url, allow_redirects=True) as resp:
            if resp.status == 401:
                raise RuntimeError(
                    "Seedr auth failed (401).\n"
                    "Check SEEDR_USERNAME and SEEDR_PASSWORD in .env"
                )
            if resp.status == 403:
                raise RuntimeError("Seedr returned 403 Forbidden.")
            if resp.status == 404:
                raise RuntimeError(f"Seedr endpoint not found (404): {path}")
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            if "json" in ct:
                return await resp.json()
            text = await resp.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"raw": text}


async def _rest_post(path: str, data: dict | None = None) -> dict:
    auth    = _basic_auth()
    timeout = aiohttp.ClientTimeout(total=60)
    url     = f"{_REST_BASE}/{path}"
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as sess:
        async with sess.post(url, data=data, allow_redirects=True) as resp:
            if resp.status == 401:
                raise RuntimeError("Seedr auth failed (401).")
            if resp.status == 403:
                raise RuntimeError("Seedr returned 403.")
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            if "json" in ct:
                return await resp.json()
            text = await resp.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"raw": text}


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    try:
        await _rest_get("user")
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    data = await _rest_get("folder")
    total = int(data.get("space_max", 0))
    used  = int(data.get("space_used", 0))
    return {"total": total, "used": used, "free": total - used}


async def add_magnet(magnet: str) -> dict:
    """POST /rest/transfer/magnet"""
    result = await _rest_post("transfer/magnet", data={"magnet": magnet})
    log.info("[Seedr] Magnet submitted: %s", result)
    if result.get("error"):
        raise RuntimeError(f"Seedr rejected magnet: {result.get('error')}")
    return result


async def list_folder(folder_id: int = 0) -> dict:
    if folder_id:
        return await _rest_get(f"folder/{folder_id}")
    else:
        return await _rest_get("folder")


async def get_file_download_url(file_id: int) -> str:
    """GET /rest/file/{id} — capture redirect URL instead of downloading."""
    auth    = _basic_auth()
    timeout = aiohttp.ClientTimeout(total=30)
    url     = f"{_REST_BASE}/file/{file_id}"
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as sess:
        async with sess.get(url, allow_redirects=False) as resp:
            if resp.status in (301, 302, 303, 307, 308):
                return resp.headers.get("Location", "")
            if resp.status == 200:
                ct = resp.headers.get("Content-Type", "")
                if "json" in ct:
                    data = await resp.json()
                    return data.get("url", str(resp.url))
                return str(resp.url)
            resp.raise_for_status()
    return ""


async def delete_folder(folder_id: int) -> None:
    await _rest_post(f"folder/{folder_id}/delete")
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
    if existing_folder_ids is None:
        existing_folder_ids = set()

    deadline = time.time() + timeout_s
    last_pct = -1.0

    while time.time() < deadline:
        try:
            root = await list_folder(0)
            folders  = root.get("folders", [])
            torrents = root.get("torrents", [])

            downloading = [
                t for t in torrents
                if int(t.get("progress", 100)) < 100
            ]
            new_folders = [
                f for f in folders
                if f.get("id") not in existing_folder_ids
                and (
                    not torrent_name_hint
                    or torrent_name_hint.lower()[:20]
                       in f.get("name", "").lower()
                )
            ]

            if downloading:
                dl  = downloading[0]
                pct = float(dl.get("progress", 0))
                if pct != last_pct:
                    last_pct = pct
                    name = dl.get("name", "")
                    log.info("[Seedr] Progress: %.1f%%  %s", pct, name)
                    if progress_cb:
                        await progress_cb(pct, name)
            elif new_folders:
                folder = new_folders[-1]
                log.info("[Seedr] Ready: %s (id=%s)",
                         folder.get("name"), folder.get("id"))
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
