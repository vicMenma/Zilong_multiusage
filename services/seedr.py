"""
services/seedr.py  — ROBUST FREE-ACCOUNT STRATEGY
════════════════════════════════════════════════════════════════════════

ROOT CAUSES OF PREVIOUS FAILURES (all fixed here)
──────────────────────────────────────────────────
BUG-1  add_magnet never checked result:false
BUG-2  poll name-hint filter too strict → always empty new_folders
BUG-3  No storage management for free accounts (~2 GB quota)
BUG-4  _api_post accepted HTTP-200 with result:false as success
BUG-5  get_file_download_url used wrong file id field
BUG-6  No multi-account rotation
BUG-7  No storage pre-flight check
BUG-8  poll race: fast torrents finish before first poll cycle

SETUP (.env)
────────────
  Single account:
    SEEDR_USERNAME=you@email.com
    SEEDR_PASSWORD=yourpassword

  Multi-account rotation (recommended for free tier):
    SEEDR_USERNAME=acc1@email.com,acc2@email.com,acc3@email.com
    SEEDR_PASSWORD=pass1,pass2,pass3

  Proxy (required on cloud IPs — Colab, Render, Railway, etc.):
    SEEDR_PROXY=http://user:pass@host:port
════════════════════════════════════════════════════════════════════════
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

# ── Custom exceptions ─────────────────────────────────────────
class SeedrError(RuntimeError):
    """Seedr API returned an error response."""

class SeedrQuotaError(SeedrError):
    """Not enough free space on Seedr account(s)."""

class SeedrAuthError(SeedrError):
    """Authentication failed."""

# ── Constants ─────────────────────────────────────────────────
_OAUTH_URL = "https://www.seedr.cc/oauth_test/token"
_API_URL   = "https://www.seedr.cc/api/folder"
_FILE_URL  = "https://www.seedr.cc/api/file"
_CLIENT_ID = "seedr_xbmc"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

_TOKEN_CACHE: dict[str, dict] = {}
_MIN_FREE_BYTES = 200 * 1024 * 1024   # 200 MB safety buffer


# ── Credentials — supports comma-separated multi-account ──────
def _all_accounts() -> list[tuple[str, str]]:
    usernames = [u.strip() for u in os.environ.get("SEEDR_USERNAME", "").split(",") if u.strip()]
    passwords = [p.strip() for p in os.environ.get("SEEDR_PASSWORD", "").split(",") if p.strip()]
    if not usernames or not passwords:
        raise SeedrAuthError(
            "Seedr credentials not configured.\n"
            "Add to .env:\n"
            "  SEEDR_USERNAME=your@email.com\n"
            "  SEEDR_PASSWORD=yourpassword\n"
            "  # Multi-account (recommended):\n"
            "  SEEDR_USERNAME=acc1@mail.com,acc2@mail.com\n"
            "  SEEDR_PASSWORD=pass1,pass2"
        )
    if len(passwords) == 1:
        passwords = passwords * len(usernames)
    return list(zip(usernames, passwords))


def _get_proxy() -> Optional[str]:
    return os.environ.get("SEEDR_PROXY", "").strip() or None


def _make_client(timeout: int = 60) -> httpx.AsyncClient:
    proxy = _get_proxy()
    if proxy:
        log.debug("[Seedr] Proxy: %s", re.sub(r":([^@/]+)@", ":***@", proxy))
    return httpx.AsyncClient(
        proxy=proxy,
        headers=_BROWSER_HEADERS,
        timeout=timeout,
        follow_redirects=True,
    )


# ── OAuth2 token management ───────────────────────────────────
async def _fetch_token_password(username: str, password: str) -> dict:
    async with _make_client() as http:
        r = await http.post(_OAUTH_URL, data={
            "grant_type": "password",
            "client_id":   _CLIENT_ID,
            "username":    username,
            "password":    password,
        })
        if r.status_code != 200:
            raise SeedrAuthError(f"OAuth login failed ({r.status_code}): {r.text[:300]}")
        data = r.json()
        if "error" in data:
            raise SeedrAuthError(f"OAuth error: {data.get('error')} — {data.get('error_description','')}")
        if "access_token" not in data:
            raise SeedrAuthError(f"No access_token in response: {data}")
        return data


async def _refresh_token(refresh_tok: str) -> dict:
    async with _make_client() as http:
        r = await http.post(_OAUTH_URL, data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_tok,
            "client_id":     _CLIENT_ID,
        })
        if r.status_code != 200:
            raise SeedrAuthError(f"Token refresh failed ({r.status_code}): {r.text[:300]}")
        data = r.json()
        if "error" in data or "access_token" not in data:
            raise SeedrAuthError(f"Refresh invalid: {data}")
        return data


def _store_token(username: str, data: dict) -> None:
    _TOKEN_CACHE[username] = {
        "access_token":  data["access_token"],
        "refresh_token": data.get("refresh_token", ""),
        "expires_at":    time.time() + int(data.get("expires_in", 3600)),
    }


async def _get_access_token(username: str, password: str) -> str:
    cache = _TOKEN_CACHE.get(username)
    if cache and cache.get("expires_at", 0) > time.time() + 60:
        return cache["access_token"]
    if cache and cache.get("refresh_token"):
        try:
            log.info("[Seedr] Refreshing token for %s…", username[:20])
            data = await _refresh_token(cache["refresh_token"])
            _store_token(username, data)
            return data["access_token"]
        except SeedrAuthError as e:
            log.warning("[Seedr] Refresh failed (%s) — re-logging in…", e)
    log.info("[Seedr] Authenticating %s…", username[:20])
    data = await _fetch_token_password(username, password)
    _store_token(username, data)
    return data["access_token"]


# ── SeedrAccount — one account's full API ─────────────────────
class SeedrAccount:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    async def _token(self) -> str:
        return await _get_access_token(self.username, self.password)

    async def _get(self, url: str, params: dict = None) -> dict:
        token = await self._token()
        async with _make_client() as http:
            r = await http.get(url, params={"access_token": token, **(params or {})})
            r.raise_for_status()
            return r.json()

    async def _post(self, data: dict) -> dict:
        """POST /api/folder — FIX-BUG-4: raise on result:false."""
        token = await self._token()
        async with _make_client() as http:
            r = await http.post(_API_URL, data={"access_token": token, **data})
            r.raise_for_status()
            try:
                body = r.json()
            except Exception:
                return {"result": True, "raw": r.text}
            if body.get("result") is False or str(body.get("result","true")).lower() == "false":
                err = body.get("error") or body.get("message") or str(body)
                raise SeedrError(f"Seedr API error: {err}")
            return body

    async def get_storage(self) -> dict:
        data  = await self._get(_API_URL)
        total = int(data.get("space_max",  data.get("storage_total", 0)))
        used  = int(data.get("space_used", data.get("storage_used",  0)))
        return {"total": total, "used": used, "free": max(0, total - used)}

    async def list_root(self) -> dict:
        data = await self._get(_API_URL)
        folders = [
            {"id": f["id"], "name": f.get("name",""), "size": int(f.get("size",0))}
            for f in data.get("folders", []) if "id" in f
        ]
        files = [
            {"id": f["id"], "name": f.get("name","file"), "size": int(f.get("size",0))}
            for f in data.get("files", []) if "id" in f
        ]
        torrents = []
        for t in data.get("torrents", []):
            try:
                pct = float(str(t.get("progress","0")).replace("%","").strip())
            except (ValueError, TypeError):
                pct = 0.0
            torrents.append({
                "id": t.get("id"), "name": t.get("name",""),
                "progress": pct, "size": int(t.get("size",0)),
            })
        return {"folders": folders, "files": files, "torrents": torrents,
                "space_used": data.get("space_used"), "space_max": data.get("space_max")}

    async def list_folder(self, folder_id: int) -> dict:
        data = await self._get(_API_URL, {"content_id": folder_id})
        folders = [
            {"id": f["id"], "name": f.get("name",""), "size": int(f.get("size",0))}
            for f in data.get("folders", []) if "id" in f
        ]
        files = [
            {"id": f["id"], "name": f.get("name","file"), "size": int(f.get("size",0))}
            for f in data.get("files", []) if "id" in f
        ]
        return {"folders": folders, "files": files}

    async def add_magnet(self, magnet: str) -> int:
        """FIX-BUG-1/4/8: raises on failure, returns torrent_id."""
        log.info("[Seedr] Submitting magnet…")
        result = await self._post({"func": "add_torrent", "torrent_magnet": magnet})
        torrent_id = int(result.get("torrent_id") or result.get("id") or 0)
        log.info("[Seedr] Submitted. torrent_id=%s", torrent_id)
        return torrent_id

    async def get_file_url(self, file_id: int) -> str:
        """FIX-BUG-5: uses raw 'id' from files[]."""
        data = await self._get(_FILE_URL, {"folder_file_id": file_id})
        url  = data.get("url", "")
        if not url:
            raise SeedrError(f"No URL returned for file id={file_id}: {data}")
        return url

    async def delete_folder(self, folder_id: int) -> None:
        try:
            await self._post({"func": "delete", "delete_arr[]": f"folder_{folder_id}"})
        except SeedrError as e:
            log.warning("[Seedr] Delete folder %d: %s (non-fatal)", folder_id, e)

    async def delete_torrent(self, torrent_id: int) -> None:
        try:
            await self._post({"func": "delete", "delete_arr[]": f"torrent_{torrent_id}"})
        except SeedrError as e:
            log.warning("[Seedr] Delete torrent %d: %s (non-fatal)", torrent_id, e)

    async def cleanup_all_folders(self) -> int:
        """FIX-BUG-3: delete all completed folders to free space."""
        root    = await self.list_root()
        deleted = 0
        for f in root.get("folders", []):
            fid = f.get("id")
            if fid is not None:
                log.info("[Seedr] Cleaning up folder: %s (id=%d)", f.get("name","?"), fid)
                await self.delete_folder(fid)
                deleted += 1
        if deleted:
            log.info("[Seedr] Cleaned up %d folder(s).", deleted)
        return deleted

    async def ensure_space(self, needed_bytes: int = 0) -> int:
        """FIX-BUG-3/7: auto-cleanup then raise if still not enough."""
        storage = await self.get_storage()
        free    = storage["free"]
        total   = storage["total"]
        needed  = max(needed_bytes, _MIN_FREE_BYTES)
        log.info(
            "[Seedr] %s: %.0f/%.0f MB used (%.0f MB free)",
            self.username[:20], storage["used"]/1e6, total/1e6, free/1e6,
        )
        if free >= needed:
            return free
        log.info("[Seedr] Low space — cleaning up folders…")
        deleted = await self.cleanup_all_folders()
        if deleted:
            storage = await self.get_storage()
            free    = storage["free"]
        if free < needed:
            raise SeedrQuotaError(
                f"Account {self.username[:20]} has only {free//1024//1024} MB free "
                f"(need ≥{needed//1024//1024} MB). Total: {total//1024//1024} MB."
            )
        return free


# ── Account selector — picks account with most free space ─────
async def _best_account(needed_bytes: int = 0) -> SeedrAccount:
    """FIX-BUG-6: multi-account rotation — returns account with most free space."""
    accounts   = _all_accounts()
    best: Optional[SeedrAccount] = None
    best_free  = -1
    last_error = None

    for username, password in accounts:
        acc = SeedrAccount(username, password)
        try:
            free = await acc.ensure_space(needed_bytes)
            if free > best_free:
                best_free = free
                best      = acc
        except (SeedrQuotaError, SeedrAuthError) as e:
            log.warning("[Seedr] Account %s unavailable: %s", username[:20], e)
            last_error = e
        except Exception as e:
            log.warning("[Seedr] Account %s error: %s", username[:20], e)
            last_error = e

    if best is None:
        raise (last_error or SeedrQuotaError("All Seedr accounts are full or unreachable."))

    log.info("[Seedr] Selected: %s (%.0f MB free)", best.username[:20], best_free/1e6)
    return best


# ── Poll until torrent finishes — REWRITTEN ───────────────────
async def poll_until_ready(
    account:             SeedrAccount,
    torrent_id:          int,
    existing_folder_ids: set,
    timeout_s:           int   = 7200,
    progress_cb                = None,
    poll_interval:       float = 8.0,
) -> dict:
    """
    FIX-BUG-2/7/8:
    • Tracks exact torrent_id (not name hint).
    • Returns as soon as that torrent disappears AND new folder appears.
    • Handles fast torrents that finish before first poll.
    • Heartbeat every 25s so panel never looks frozen.
    """
    deadline         = time.time() + timeout_s
    last_pct         = -1.0
    last_hb          = time.time()
    torrent_seen     = False
    gone_polls       = 0        # how many polls since torrent disappeared

    while time.time() < deadline:
        try:
            root     = await account.list_root()
            folders  = root.get("folders",  [])
            torrents = root.get("torrents", [])
            now      = time.time()

            # Find our torrent
            our = None
            if torrent_id:
                our = next((t for t in torrents if t.get("id") == torrent_id), None)
            if our is None and not torrent_id and torrents:
                our = torrents[0]   # fallback: track any active torrent

            if our is not None:
                torrent_seen = True
                gone_polls   = 0
                pct  = our["progress"]
                name = our.get("name", "")
                changed   = abs(pct - last_pct) >= 0.5
                heartbeat = (now - last_hb) >= 25.0
                if changed or heartbeat:
                    last_pct = pct
                    last_hb  = now
                    log.info("[Seedr] %.1f%%  %s", pct, name)
                    if progress_cb:
                        await progress_cb(pct, name)
            else:
                # Torrent not active — check for new completed folder
                new_folders = [f for f in folders if f.get("id") not in existing_folder_ids]
                if new_folders:
                    folder = max(new_folders, key=lambda f: f.get("id", 0))
                    log.info("[Seedr] ✅ Complete: %s (id=%s)", folder.get("name"), folder.get("id"))
                    if progress_cb:
                        await progress_cb(100.0, folder.get("name", "Done"))
                    return folder

                gone_polls += 1
                if torrent_seen:
                    log.info("[Seedr] Torrent finished, waiting for folder (poll #%d)…", gone_polls)
                else:
                    log.debug("[Seedr] Torrent not visible yet (queued/metadata fetch)…")
                    if now - last_hb >= 25.0:
                        last_hb = now
                        if progress_cb:
                            await progress_cb(0.0, "Waiting for Seedr to start…")

                # If we've seen it and it's gone but no folder for 3 polls, something is wrong
                if torrent_seen and gone_polls >= 5:
                    raise SeedrError(
                        "Torrent disappeared from Seedr without creating a folder. "
                        "It may have been rejected or cancelled. Check your Seedr account."
                    )

        except (SeedrError, SeedrAuthError) as e:
            log.warning("[Seedr] Poll error: %s — retrying…", e)
            if "disappeared" in str(e) or "rejected" in str(e):
                raise
        except Exception as e:
            log.warning("[Seedr] Unexpected poll error: %s", e)

        await asyncio.sleep(poll_interval)

    raise RuntimeError(
        f"Seedr download timed out after {timeout_s // 60} min. "
        "Torrent may still be running — check your Seedr account."
    )


# ── Recursive file URL collector ──────────────────────────────
async def get_file_urls(account: SeedrAccount, folder_id: int) -> list[dict]:
    """FIX-BUG-5: uses raw 'id' from files[]."""
    result: list[dict] = []

    async def _collect(fid: int) -> None:
        contents = await account.list_folder(fid)
        for f in contents.get("files", []):
            file_id = f.get("id")
            if not file_id:
                continue
            try:
                url = await account.get_file_url(file_id)
                if url:
                    result.append({"name": f.get("name","file"), "url": url, "size": f.get("size",0)})
            except Exception as e:
                log.warning("[Seedr] URL fetch failed %s: %s", f.get("name"), e)
        for sub in contents.get("folders", []):
            sid = sub.get("id")
            if sid:
                try:
                    await _collect(sid)
                except Exception as e:
                    log.warning("[Seedr] Sub-folder error fid=%s: %s", sid, e)

    await _collect(folder_id)
    return result


# ── High-level pipeline ───────────────────────────────────────
async def download_via_seedr(
    magnet:    str,
    dest:      str,
    progress_cb = None,
    timeout_s: int = 7200,
) -> list[str]:
    """
    Full pipeline: pick best account → ensure space → add magnet →
    poll → get URLs → download → cleanup.
    Returns list of local file paths under dest.

    progress_cb(stage, pct, detail)
      stage ∈ {adding, waiting, downloading, fetching, dl_file}
    """
    from services.downloader  import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("adding", 0.0, "Selecting Seedr account…")

    # Pick best account (auto-cleans quota)
    account = await _best_account()

    if progress_cb:
        await progress_cb("adding", 2.0, "Snapshotting account state…")

    # Baseline — record existing folders so we can detect the new one
    root = await account.list_root()
    existing_folder_ids = {f["id"] for f in root.get("folders", []) if f.get("id") is not None}

    # Submit magnet
    if progress_cb:
        await progress_cb("adding", 5.0, "Submitting magnet to Seedr…")
    torrent_id = await account.add_magnet(magnet)

    # Poll
    if progress_cb:
        await progress_cb("waiting", 5.0, "Seedr fetching torrent metadata…")

    async def _poll_cb(pct: float, name: str) -> None:
        if progress_cb:
            stage = "downloading" if pct > 0 else "waiting"
            await progress_cb(stage, min(pct, 99.0), name or "Downloading…")

    folder = await poll_until_ready(
        account             = account,
        torrent_id          = torrent_id,
        existing_folder_ids = existing_folder_ids,
        timeout_s           = timeout_s,
        progress_cb         = _poll_cb,
    )
    folder_id = folder["id"]

    # Collect file URLs
    if progress_cb:
        await progress_cb("fetching", 100.0, "Getting CDN download links…")

    files = await get_file_urls(account, folder_id)

    # Edge case: single-file torrent lands directly as a root file
    if not files:
        log.warning("[Seedr] No files in folder — checking root files…")
        fresh = await account.list_root()
        for f in fresh.get("files", []):
            fid = f.get("id")
            if fid and fid not in existing_folder_ids:
                try:
                    url = await account.get_file_url(fid)
                    files.append({"name": f.get("name","file"), "url": url, "size": f.get("size",0)})
                except Exception:
                    pass

    if not files:
        raise SeedrError(
            "Seedr returned no downloadable files. "
            "The torrent may be empty or all files are unsupported."
        )

    # Download locally
    os.makedirs(dest, exist_ok=True)
    local_paths: list[str] = []
    total = len(files)

    for i, f in enumerate(files):
        clean_name = sanitize_filename(f["name"])
        if progress_cb:
            await progress_cb("dl_file", i / total * 100,
                              f"⬇️ {clean_name} ({i+1}/{total})")
        log.info("[Seedr] Downloading %d/%d: %s", i + 1, total, f["name"])
        try:
            path = await download_direct(f["url"], dest)
            expected = os.path.join(dest, clean_name)
            if path != expected:
                try:
                    os.rename(path, expected)
                    path = expected
                except OSError:
                    pass
            local_paths.append(path)
        except Exception as e:
            log.error("[Seedr] Download failed %s: %s", f["name"], e)

    # Cleanup Seedr to reclaim quota
    try:
        await account.delete_folder(folder_id)
        log.info("[Seedr] Cleaned up folder id=%d", folder_id)
    except Exception as e:
        log.warning("[Seedr] Cleanup (non-fatal): %s", e)

    return local_paths


# ── Legacy compatibility shims ────────────────────────────────
async def check_credentials() -> bool:
    try:
        await _best_account()
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    u, p = _all_accounts()[0]
    return await SeedrAccount(u, p).get_storage()


async def add_magnet(magnet: str) -> dict:
    account = await _best_account()
    tid = await account.add_magnet(magnet)
    return {"result": True, "torrent_id": tid}


async def list_folder(folder_id: int = 0) -> dict:
    u, p = _all_accounts()[0]
    acc  = SeedrAccount(u, p)
    return await acc.list_root() if folder_id == 0 else await acc.list_folder(folder_id)


async def delete_folder(folder_id: int) -> None:
    u, p = _all_accounts()[0]
    await SeedrAccount(u, p).delete_folder(folder_id)


async def delete_torrent(torrent_id: int) -> None:
    u, p = _all_accounts()[0]
    await SeedrAccount(u, p).delete_torrent(torrent_id)
