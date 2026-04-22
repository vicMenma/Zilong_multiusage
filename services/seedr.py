"""
services/seedr.py
Seedr.cc cloud torrent client.

═══════════════════════════════════════════════════════════════════
ROOT CAUSE OF PREVIOUS ERRORS
─────────────────────────────────────────────────────────────────
  OLD CODE used REST API v1 with HTTP Basic Auth:
    POST https://www.seedr.cc/rest/transfer/magnet   ← 404 for free users
    GET  https://www.seedr.cc/rest/folder            ← 401 for free users

  WHY IT FAILED:
    • REST API v1 is PREMIUM-ONLY. Free accounts always get 401/404.
    • The endpoint /rest/transfer/magnet no longer works for non-premium.

  CORRECT APPROACH (this file):
    Use the same OAuth2 API as the official Chrome & Kodi extensions.
    This API works for ALL users (free and premium alike).

═══════════════════════════════════════════════════════════════════
SEEDR CHROME/KODI EXTENSION API  (works for ALL account types)
─────────────────────────────────────────────────────────────────
  Auth:  OAuth2 password grant
    POST https://www.seedr.cc/oauth_test/token
         client_id=seedr_xbmc  (public client — no secret needed)
         grant_type=password
         username=<email>
         password=<password>
    → { access_token, refresh_token, token_type, expires_in }

  Token Refresh:
    POST https://www.seedr.cc/oauth_test/token
         grant_type=refresh_token
         refresh_token=<token>
         client_id=seedr_xbmc

  Add magnet / torrent URL:
    POST https://www.seedr.cc/api/folder
         access_token=<token>  (in body, NOT Authorization header)
         func=add_torrent
         torrent_magnet=<magnet_link>

  List root folder:
    GET https://www.seedr.cc/api/folder?access_token=<token>

  Get file download URL:
    GET https://www.seedr.cc/api/file?access_token=<token>&folder_file_id=<id>
    → { url: "https://cdn..." }

  Delete folder:
    POST https://www.seedr.cc/api/folder
         access_token=<token>
         func=delete
         delete_arr[]=folder_<folder_id>

  Delete torrent (active transfer):
    POST https://www.seedr.cc/api/folder
         access_token=<token>
         func=delete
         delete_arr[]=torrent_<torrent_id>

SETUP (.env):
    SEEDR_USERNAME=your@email.com
    SEEDR_PASSWORD=yourpassword
    SEEDR_PROXY=http://user:pass@host:port   (optional, for cloud IPs)
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

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────

_OAUTH_URL  = "https://www.seedr.cc/oauth_test/token"
_API_URL    = "https://www.seedr.cc/api/folder"
_FILE_URL   = "https://www.seedr.cc/api/file"
_CLIENT_ID  = "seedr_xbmc"   # public OAuth client (Chrome/Kodi extension)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# In-process token cache: { username → {access_token, refresh_token, expires_at} }
_TOKEN_CACHE: dict[str, dict] = {}


# ─────────────────────────────────────────────────────────────
# Credentials & proxy helpers
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


def _make_client() -> httpx.AsyncClient:
    proxy = _get_proxy()
    if proxy:
        log.info("[Seedr] Using proxy: %s", re.sub(r":([^@/]+)@", ":***@", proxy))
    return httpx.AsyncClient(
        proxy=proxy,
        headers=_BROWSER_HEADERS,
        timeout=60,
        follow_redirects=True,
    )


# ─────────────────────────────────────────────────────────────
# OAuth2 token management
# ─────────────────────────────────────────────────────────────

async def _fetch_token_password(username: str, password: str) -> dict:
    """Get a new token via password grant."""
    async with _make_client() as http:
        r = await http.post(_OAUTH_URL, data={
            "grant_type": "password",
            "client_id":   _CLIENT_ID,
            "username":    username,
            "password":    password,
        })
        if r.status_code != 200:
            raise RuntimeError(
                f"[Seedr] OAuth login failed ({r.status_code}): {r.text[:300]}"
            )
        data = r.json()
        if "error" in data:
            raise RuntimeError(
                f"[Seedr] OAuth error: {data.get('error')} — "
                f"{data.get('error_description', '')}"
            )
        return data


async def _refresh_token(refresh_tok: str) -> dict:
    """Refresh an expired access token."""
    async with _make_client() as http:
        r = await http.post(_OAUTH_URL, data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_tok,
            "client_id":     _CLIENT_ID,
        })
        if r.status_code != 200:
            raise RuntimeError(
                f"[Seedr] Token refresh failed ({r.status_code}): {r.text[:300]}"
            )
        return r.json()


async def _get_access_token() -> str:
    """
    Return a valid access token, fetching or refreshing as needed.
    Tokens are cached in memory for the process lifetime.
    """
    username, password = _creds()
    cache = _TOKEN_CACHE.get(username)

    # Token still valid (with 60s buffer)
    if cache and cache.get("expires_at", 0) > time.time() + 60:
        return cache["access_token"]

    # Try to refresh if we have a refresh token
    if cache and cache.get("refresh_token"):
        try:
            log.info("[Seedr] Refreshing access token…")
            data = await _refresh_token(cache["refresh_token"])
            _store_token(username, data)
            return data["access_token"]
        except Exception as e:
            log.warning("[Seedr] Refresh failed (%s) — re-authenticating…", e)

    # Full login
    log.info("[Seedr] Authenticating with username/password…")
    data = await _fetch_token_password(username, password)
    _store_token(username, data)
    log.info("[Seedr] Authenticated successfully.")
    return data["access_token"]


def _store_token(username: str, data: dict) -> None:
    expires_in = int(data.get("expires_in", 3600))
    _TOKEN_CACHE[username] = {
        "access_token":  data["access_token"],
        "refresh_token": data.get("refresh_token", ""),
        "expires_at":    time.time() + expires_in,
    }


# ─────────────────────────────────────────────────────────────
# Core API helpers
# ─────────────────────────────────────────────────────────────

async def _api_get(params: dict) -> dict:
    """GET /api/folder with access_token injected."""
    token = await _get_access_token()
    async with _make_client() as http:
        r = await http.get(_API_URL, params={"access_token": token, **params})
        r.raise_for_status()
        return r.json()


async def _api_post(data: dict) -> dict:
    """POST /api/folder with access_token injected."""
    token = await _get_access_token()
    async with _make_client() as http:
        r = await http.post(_API_URL, data={"access_token": token, **data})
        r.raise_for_status()
        try:
            return r.json()
        except Exception:
            return {"result": True, "raw": r.text}


async def _api_file_get(params: dict) -> dict:
    """GET /api/file with access_token injected."""
    token = await _get_access_token()
    async with _make_client() as http:
        r = await http.get(_FILE_URL, params={"access_token": token, **params})
        r.raise_for_status()
        return r.json()


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    """Return True if credentials are valid (attempts token fetch)."""
    try:
        await _get_access_token()
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    """Return {used, total, free} in bytes."""
    data = await _api_get({})
    total = int(data.get("space_max",  data.get("storage_total", 0)))
    used  = int(data.get("space_used", data.get("storage_used",  0)))
    return {"total": total, "used": used, "free": total - used}


async def add_magnet(magnet: str) -> dict:
    """
    Submit a magnet link via the Chrome/Kodi extension API.
    POST /api/folder  func=add_torrent  torrent_magnet=<magnet>

    This works for ALL account types (free and premium).
    """
    log.info("[Seedr] Submitting magnet via extension API…")
    result = await _api_post({
        "func":           "add_torrent",
        "torrent_magnet": magnet,
    })
    log.info("[Seedr] Transfer submitted: %s", result)
    return result


async def list_folder(folder_id: int = 0) -> dict:
    """
    List folder contents.
    Returns dict with: folders, files, torrents (active transfers), space_used, space_max
    """
    params = {} if folder_id == 0 else {"content_id": folder_id}
    data   = await _api_get(params)

    folders = [
        {"id": f.get("id"), "name": f.get("name"), "size": f.get("size", 0)}
        for f in data.get("folders", [])
    ]
    files = [
        {
            "folder_file_id": f.get("id"),
            "id":             f.get("id"),
            "name":           f.get("name"),
            "size":           f.get("size", 0),
        }
        for f in data.get("files", [])
    ]
    # Active transfers appear as "torrents" in the extension API
    torrents = [
        {
            "id":       t.get("id"),
            "name":     t.get("name", ""),
            "progress": str(t.get("progress", "0")),
            "size":     t.get("size", 0),
        }
        for t in data.get("torrents", [])
    ]

    return {
        "folders":    folders,
        "files":      files,
        "torrents":   torrents,
        "space_used": data.get("space_used"),
        "space_max":  data.get("space_max"),
    }


async def get_file_download_url(folder_file_id: int) -> str:
    """
    Get direct CDN download URL for a file.
    GET /api/file?folder_file_id=<id>  → { url: "https://cdn..." }
    """
    data = await _api_file_get({"folder_file_id": folder_file_id})
    url  = data.get("url", "")
    if not url:
        raise RuntimeError(
            f"[Seedr] No URL returned for file {folder_file_id}: {data}"
        )
    log.info("[Seedr] File %s CDN URL: %s…", folder_file_id, url[:80])
    return url


async def delete_folder(folder_id: int) -> None:
    """Delete a completed folder from Seedr to reclaim quota."""
    result = await _api_post({
        "func":       "delete",
        "delete_arr[]": f"folder_{folder_id}",
    })
    log.info("[Seedr] Deleted folder id=%d → %s", folder_id, result)


async def delete_torrent(torrent_id: int) -> None:
    """Delete an active/stuck torrent transfer."""
    result = await _api_post({
        "func":       "delete",
        "delete_arr[]": f"torrent_{torrent_id}",
    })
    log.info("[Seedr] Deleted torrent id=%d → %s", torrent_id, result)


# ─────────────────────────────────────────────────────────────
# Poll until torrent finishes
# ─────────────────────────────────────────────────────────────

async def poll_until_ready(
    torrent_name_hint: str = "",
    timeout_s:         int = 3600,
    progress_cb        = None,
    existing_folder_ids: Optional[set] = None,
) -> dict:
    """
    Poll Seedr root folder until the torrent download completes.
    Returns the new folder dict: {id, name, size}.
    """
    if existing_folder_ids is None:
        existing_folder_ids = set()

    deadline = time.time() + timeout_s
    last_pct = -1.0

    while time.time() < deadline:
        try:
            root     = await list_folder(0)
            folders  = root.get("folders",  [])
            torrents = root.get("torrents", [])

            # Check active downloads
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
    magnet:    str,
    dest:      str,
    progress_cb = None,
    timeout_s: int = 3600,
) -> list[str]:
    """
    Full pipeline: add magnet → poll → fetch file URLs → download → cleanup.
    Returns list of local file paths under `dest`.
    """
    from services.downloader   import download_direct
    from services.cc_sanitize  import sanitize_filename

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
        log.warning("[Seedr] Snapshot failed (non-fatal): %s", exc)

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
        log.warning("[Seedr] Cleanup failed (non-fatal): %s", e)

    return local_paths
