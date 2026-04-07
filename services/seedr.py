"""
services/seedr.py
Seedr.cc cloud torrent client — async wrapper.

Setup (.env):
    SEEDR_USERNAME=your@email.com
    SEEDR_PASSWORD=yourpassword

Token is cached to data/seedr_token.json and refreshed automatically.

Flow:
    1. add_magnet(magnet)        → Seedr queues download on their servers
    2. poll_until_ready(tid)     → 10-s poll until progress == 100
    3. get_file_urls(folder_id)  → list[{name, url, size}]
    4. delete_folder(folder_id)  → free the 2 GB quota

Free tier: 2 GB storage. Bot auto-deletes each torrent after download.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_BASE  = "https://www.seedr.cc/oauth_test"
_TOKEN_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "seedr_token.json")
)

# ─────────────────────────────────────────────────────────────
# Token management
# ─────────────────────────────────────────────────────────────

_token_cache: dict = {}


def _load_token() -> dict:
    global _token_cache
    if _token_cache:
        return _token_cache
    try:
        with open(_TOKEN_FILE, encoding="utf-8") as f:
            _token_cache = json.load(f)
        return _token_cache
    except FileNotFoundError:
        return {}
    except Exception as e:
        log.warning("[Seedr] Token load error: %s", e)
        return {}


def _save_token(data: dict) -> None:
    global _token_cache
    _token_cache = data
    try:
        os.makedirs(os.path.dirname(_TOKEN_FILE), exist_ok=True)
        with open(_TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.warning("[Seedr] Token save error: %s", e)


async def _get_token(username: str, password: str) -> str:
    """
    Get a valid access token. Uses cache; refreshes if expired; fetches new if missing.
    """
    cached = _load_token()

    # Try cached access token
    if cached.get("access_token"):
        expires_at = cached.get("expires_at", 0)
        if time.time() < expires_at - 60:
            return cached["access_token"]

    # Try refresh token
    if cached.get("refresh_token"):
        try:
            token = await _refresh_token(cached["refresh_token"])
            if token:
                return token
        except Exception:
            pass

    # Fresh login
    return await _login(username, password)


async def _login(username: str, password: str) -> str:
    payload = {
        "grant_type":  "password",
        "client_id":   "seedr_chrome",
        "username":    username,
        "password":    password,
        "type":        "seed",
    }
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.post(f"{_BASE}/token", data=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()

    if "access_token" not in data:
        raise RuntimeError(f"Seedr login failed: {data}")

    data["expires_at"] = time.time() + int(data.get("expires_in", 3600))
    _save_token(data)
    log.info("[Seedr] Logged in successfully")
    return data["access_token"]


async def _refresh_token(refresh_token: str) -> Optional[str]:
    payload = {
        "grant_type":    "refresh_token",
        "client_id":     "seedr_chrome",
        "refresh_token": refresh_token,
    }
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.post(f"{_BASE}/token", data=payload) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

    if "access_token" not in data:
        return None

    data["expires_at"] = time.time() + int(data.get("expires_in", 3600))
    _save_token(data)
    log.info("[Seedr] Token refreshed")
    return data["access_token"]


# ─────────────────────────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────────────────────────

async def _get(token: str, params: dict) -> dict:
    params["access_token"] = token
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.get(f"{_BASE}/resource", params=params) as resp:
            resp.raise_for_status()
            return await resp.json()


async def _post(token: str, data: dict) -> dict:
    data["access_token"] = token
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.post(f"{_BASE}/resource", data=data) as resp:
            resp.raise_for_status()
            return await resp.json()


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    """Return True if SEEDR_USERNAME + SEEDR_PASSWORD are set and valid."""
    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()
    if not username or not password:
        return False
    try:
        await _get_token(username, password)
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    """Return {used, total, free} in bytes."""
    username = os.environ.get("SEEDR_USERNAME", "")
    password = os.environ.get("SEEDR_PASSWORD", "")
    token = await _get_token(username, password)
    data  = await _get(token, {"func": "list", "content_type": "folder", "content_id": "0"})
    spaces = data.get("spaces", {})
    total = int(spaces.get("total", 0))
    used  = int(spaces.get("used",  0))
    return {"total": total, "used": used, "free": total - used}


async def add_magnet(magnet: str) -> dict:
    """
    Submit a magnet to Seedr. Returns the torrent info dict which
    includes 'id' (torrent id) and eventually 'folder_id'.
    """
    username = os.environ.get("SEEDR_USERNAME", "")
    password = os.environ.get("SEEDR_PASSWORD", "")
    token = await _get_token(username, password)

    result = await _post(token, {
        "func":           "add_torrent",
        "torrent_magnet": magnet,
    })

    if not result.get("result"):
        raise RuntimeError(f"Seedr rejected torrent: {result}")

    log.info("[Seedr] Torrent added: %s", result)
    return result


async def list_root(token: str) -> dict:
    """List root folder — shows torrents/folders currently in Seedr."""
    return await _get(token, {
        "func":         "list",
        "content_type": "folder",
        "content_id":   "0",
    })


async def poll_until_ready(
    torrent_name_hint: str = "",
    timeout_s: int = 3600,
    progress_cb=None,
    existing_folder_ids: Optional[set] = None,
) -> dict:
    """
    Poll Seedr until the torrent we just added finishes downloading.
    Returns the folder dict {id, name, size, ...}.
    Raises RuntimeError on timeout.

    existing_folder_ids — set of folder IDs that existed BEFORE add_magnet().
        Any folder in this set is ignored so we never return a stale result
        from a previous run.  Pass an empty set if the account was clean.

    progress_cb(pct: float, name: str) — optional UI update.

    BUG FIXES applied here:
      1. existing_folder_ids baseline — prevents returning old completed folders
         when the new torrent finishes quickly (or Seedr serves a cached copy).
      2. Check happens BEFORE the first sleep so sub-10s downloads are caught.
      3. Name hint filtering is combined with the baseline exclusion.
    """
    if existing_folder_ids is None:
        existing_folder_ids = set()

    username = os.environ.get("SEEDR_USERNAME", "")
    password = os.environ.get("SEEDR_PASSWORD", "")
    token    = await _get_token(username, password)

    deadline  = time.time() + timeout_s
    last_pct  = -1.0
    first_run = True

    while time.time() < deadline:
        # FIX: check BEFORE sleeping on the first iteration so fast downloads
        # (sub-10s, cached torrents) are caught immediately.
        if not first_run:
            await asyncio.sleep(10)
        first_run = False

        try:
            root = await list_root(token)
        except Exception as e:
            log.warning("[Seedr] Poll error: %s", e)
            await asyncio.sleep(5)
            continue

        folders  = root.get("folders", [])
        torrents = root.get("torrents", [])

        # Check if any torrent is still downloading
        downloading = [t for t in torrents if str(t.get("progress", "100")) != "100"]

        # FIX: only consider NEW folders — exclude anything that existed before
        # we called add_magnet().  Also apply name hint if provided.
        new_folders = [
            f for f in folders
            if f.get("id") not in existing_folder_ids
            and (
                not torrent_name_hint
                or torrent_name_hint.lower()[:20] in f.get("name", "").lower()
            )
        ]

        if downloading:
            # Still running — report progress
            dl      = downloading[0]
            pct_raw = dl.get("progress", "0")
            try:
                pct = float(pct_raw)
            except (ValueError, TypeError):
                pct = 0.0

            if pct != last_pct:
                last_pct = pct
                log.info("[Seedr] Progress: %.1f%%  %s", pct, dl.get("name", ""))
                if progress_cb:
                    await progress_cb(pct, dl.get("name", ""))
            continue

        # No active torrents — check for our new folder
        if new_folders:
            folder = new_folders[-1]   # most recently added
            log.info("[Seedr] Ready: %s (id=%s)", folder.get("name"), folder.get("id"))
            return folder

        # Torrent may still be queued (not yet moved to torrents list)
        log.debug("[Seedr] No new folder yet — waiting (existing_ids=%s)…",
                  len(existing_folder_ids))

    raise RuntimeError(f"Seedr download timed out after {timeout_s}s")


async def get_file_urls(folder_id: int) -> list[dict]:
    """
    Return list of {name, url, size} for every file in folder_id,
    recursing into sub-folders.

    BUG FIX: the old code only read top-level files via contents.get("files", []).
    Multi-episode torrents (e.g. "Show S01/E01.mkv, E02.mkv") place files inside
    a sub-folder, so top-level files=[] and the bot reported "no files downloaded".
    Now we walk the entire folder tree.
    """
    username = os.environ.get("SEEDR_USERNAME", "")
    password = os.environ.get("SEEDR_PASSWORD", "")
    token    = await _get_token(username, password)

    async def _collect(fid: int) -> list[dict]:
        contents = await _get(token, {
            "func":         "list",
            "content_type": "folder",
            "content_id":   str(fid),
        })

        result: list[dict] = []

        # Files at this level
        for f in contents.get("files", []):
            file_id = f.get("folder_file_id") or f.get("id")
            name    = f.get("name", "file")
            size    = int(f.get("size", 0))
            try:
                link_data = await _get(token, {
                    "func":           "fetch_file",
                    "folder_file_id": str(file_id),
                })
                url = link_data.get("url", "")
                if url:
                    result.append({"name": name, "url": url, "size": size})
            except Exception as e:
                log.warning("[Seedr] Could not fetch URL for %s: %s", name, e)

        # Recurse into sub-folders
        for sub in contents.get("folders", []):
            sub_id = sub.get("id")
            if sub_id:
                try:
                    result.extend(await _collect(sub_id))
                except Exception as e:
                    log.warning("[Seedr] Sub-folder %s error: %s", sub.get("name"), e)

        return result

    return await _collect(folder_id)


async def delete_folder(folder_id: int) -> None:
    """Delete a folder from Seedr to reclaim quota."""
    username = os.environ.get("SEEDR_USERNAME", "")
    password = os.environ.get("SEEDR_PASSWORD", "")
    token    = await _get_token(username, password)

    await _post(token, {
        "func":       "remove",
        "delete_arr": json.dumps([{"type": "folder", "id": folder_id}]),
    })
    log.info("[Seedr] Deleted folder id=%d", folder_id)


# ─────────────────────────────────────────────────────────────
# High-level helper used by url_handler
# ─────────────────────────────────────────────────────────────

async def download_via_seedr(
    magnet:      str,
    dest:        str,
    progress_cb  = None,
    timeout_s:   int = 3600,
) -> list[str]:
    """
    Full Seedr pipeline:
      add magnet → poll until ready → fetch URLs → download files → delete from Seedr.

    Returns list of local file paths in `dest`.
    progress_cb(stage: str, pct: float, detail: str) — optional.
    """
    from services.downloader import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("adding", 0.0, "Submitting to Seedr…")

    # ── Snapshot BEFORE add_magnet so poll_until_ready never returns a stale folder ──
    # poll_until_ready() uses existing_folder_ids to exclude folders that already
    # existed before this job started.  Without this snapshot it defaults to an
    # empty set — meaning any old folder in the account is treated as "new" and
    # returned immediately on the first poll iteration, causing the bot to download
    # and delete the wrong torrent.
    username = os.environ.get("SEEDR_USERNAME", "")
    password = os.environ.get("SEEDR_PASSWORD", "")
    _pre_token = await _get_token(username, password)
    try:
        _pre_root = await list_root(_pre_token)
        existing_folder_ids: set = {
            f.get("id") for f in _pre_root.get("folders", []) if f.get("id") is not None
        }
        log.info("[Seedr] Baseline: %d existing folder(s) will be excluded from poll",
                 len(existing_folder_ids))
    except Exception as _snap_exc:
        log.warning("[Seedr] Could not snapshot existing folders: %s — using empty set", _snap_exc)
        existing_folder_ids = set()

    # Add
    await add_magnet(magnet)

    if progress_cb:
        await progress_cb("waiting", 0.0, "Seedr is downloading on their servers…")

    # Poll — pass the pre-snapshot so stale folders are excluded
    async def _poll_progress(pct: float, name: str) -> None:
        if progress_cb:
            await progress_cb("downloading", pct, name)

    folder = await poll_until_ready(
        timeout_s=timeout_s,
        progress_cb=_poll_progress,
        existing_folder_ids=existing_folder_ids,
    )
    folder_id = folder["id"]

    if progress_cb:
        await progress_cb("fetching", 100.0, "Getting download links…")

    # Get URLs
    files = await get_file_urls(folder_id)
    if not files:
        raise RuntimeError("Seedr returned no files for this torrent.")

    # Download each file to dest
    os.makedirs(dest, exist_ok=True)
    local_paths = []

    for i, f in enumerate(files):
        raw_name   = f["name"]
        clean_name = sanitize_filename(raw_name)   # safe for CC + filesystem
        dest_path  = os.path.join(dest, clean_name)

        if progress_cb:
            await progress_cb(
                "dl_file",
                (i / len(files)) * 100,
                f"Downloading {clean_name} ({i+1}/{len(files)})…",
            )

        log.info("[Seedr] Downloading %s → %s", raw_name, clean_name)

        try:
            path = await download_direct(f["url"], dest)
            # Rename to sanitized name if downloader used the original
            if os.path.basename(path) != clean_name:
                new_path = os.path.join(os.path.dirname(path), clean_name)
                os.rename(path, new_path)
                path = new_path
            local_paths.append(path)
        except Exception as e:
            log.error("[Seedr] Download failed for %s: %s", raw_name, e)

    # Cleanup Seedr
    try:
        await delete_folder(folder_id)
        log.info("[Seedr] Cleaned up folder %d", folder_id)
    except Exception as e:
        log.warning("[Seedr] Cleanup failed: %s", e)

    return local_paths
