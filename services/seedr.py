"""
services/seedr.py  — COMPLETE REWRITE (Zero-based)
═══════════════════════════════════════════════════════════════════════

WHAT THIS SOLVES (from scratch, my own logic)
─────────────────────────────────────────────
  • Real OAuth2 password flow + token refresh with in-process cache
  • API calls always validate JSON body, never trust HTTP 200 alone
  • Magnet submission returns a torrent_id we track precisely
  • Polling tracks torrent by ID, not by fragile name-matching
  • Fast-finish torrents handled: we snapshot folders BEFORE submit
  • Free-account storage auto-managed: cleanup before every job
  • Multi-account rotation: picks account with most free space
  • Proxy support: set SEEDR_PROXY for cloud-IP environments
  • Recursive file collector + single-file torrent edge case handled
  • Full cleanup after download (reclaim quota for free account)

ENV VARIABLES
─────────────
  Single account:
    SEEDR_USERNAME=you@email.com
    SEEDR_PASSWORD=yourpassword

  Multi-account (recommended for free tier, comma-separated):
    SEEDR_USERNAME=a@mail.com,b@mail.com,c@mail.com
    SEEDR_PASSWORD=passA,passB,passC

  Proxy (needed on cloud IPs — Render, Railway, Colab, etc.):
    SEEDR_PROXY=http://user:pass@host:port
═══════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Callable, Optional

import httpx

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════
# Exceptions
# ══════════════════════════════════════════════════════

class SeedrError(RuntimeError):
    """Generic Seedr failure."""

class SeedrAuthError(SeedrError):
    """Login / token problem."""

class SeedrQuotaError(SeedrError):
    """No free space on any account."""


# ══════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════

_OAUTH_URL   = "https://www.seedr.cc/oauth_test/token"
_API_ROOT    = "https://www.seedr.cc/api/folder"
_API_FILE    = "https://www.seedr.cc/api/file"
_API_RESOURCE = "https://www.seedr.cc/oauth_test/resource.php"  # OAuth resource endpoint (works on free accounts)
_CLIENT_ID   = "seedr_xbmc"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Minimum free bytes we require before submitting a torrent
_MIN_FREE = 256 * 1024 * 1024   # 256 MB

# Token store: { username: {access_token, refresh_token, expires_at} }
_TOKENS: dict[str, dict] = {}


# ══════════════════════════════════════════════════════
# Configuration helpers
# ══════════════════════════════════════════════════════

def _accounts() -> list[tuple[str, str]]:
    """Return list of (username, password) pairs from env."""
    users = [u.strip() for u in os.environ.get("SEEDR_USERNAME", "").split(",") if u.strip()]
    pwds  = [p.strip() for p in os.environ.get("SEEDR_PASSWORD", "").split(",") if p.strip()]
    if not users or not pwds:
        raise SeedrAuthError(
            "Seedr credentials missing.\n"
            "Set in .env:\n"
            "  SEEDR_USERNAME=your@email.com\n"
            "  SEEDR_PASSWORD=yourpassword\n"
            "Multi-account:\n"
            "  SEEDR_USERNAME=a@mail.com,b@mail.com\n"
            "  SEEDR_PASSWORD=passA,passB"
        )
    # If only one password given, reuse for all accounts
    if len(pwds) == 1:
        pwds = pwds * len(users)
    return list(zip(users, pwds))


def _proxy() -> Optional[str]:
    return os.environ.get("SEEDR_PROXY", "").strip() or None


def _http(timeout: int = 60) -> httpx.AsyncClient:
    """Create an httpx client with browser UA and optional proxy."""
    return httpx.AsyncClient(
        proxy=_proxy(),
        headers={"User-Agent": _UA},
        timeout=timeout,
        follow_redirects=True,
    )


# ══════════════════════════════════════════════════════
# OAuth2 token management
# ══════════════════════════════════════════════════════

def _save(user: str, data: dict) -> None:
    _TOKENS[user] = {
        "access_token":  data["access_token"],
        "refresh_token": data.get("refresh_token", ""),
        "expires_at":    time.monotonic() + int(data.get("expires_in", 3600)),
    }


async def _login(user: str, pwd: str) -> str:
    """Full password login, returns access_token."""
    log.info("[Seedr] Logging in: %s", user[:25])
    async with _http() as c:
        r = await c.post(_OAUTH_URL, data={
            "grant_type": "password",
            "client_id":  _CLIENT_ID,
            "username":   user,
            "password":   pwd,
        })
    if r.status_code != 200:
        raise SeedrAuthError(f"Login failed ({r.status_code}): {r.text[:200]}")
    d = r.json()
    if "error" in d or "access_token" not in d:
        raise SeedrAuthError(f"Login rejected: {d}")
    _save(user, d)
    return d["access_token"]


async def _refresh(user: str, rtok: str) -> str:
    """Refresh an existing token, returns access_token."""
    log.info("[Seedr] Refreshing token: %s", user[:25])
    async with _http() as c:
        r = await c.post(_OAUTH_URL, data={
            "grant_type":    "refresh_token",
            "client_id":     _CLIENT_ID,
            "refresh_token": rtok,
        })
    if r.status_code != 200:
        raise SeedrAuthError(f"Refresh failed ({r.status_code})")
    d = r.json()
    if "error" in d or "access_token" not in d:
        raise SeedrAuthError(f"Refresh rejected: {d}")
    _save(user, d)
    return d["access_token"]


async def _token(user: str, pwd: str) -> str:
    """Get a valid access token, refreshing or logging in as needed."""
    t = _TOKENS.get(user)
    if t:
        remaining = t["expires_at"] - time.monotonic()
        if remaining > 90:                     # still valid
            return t["access_token"]
        if t["refresh_token"]:
            try:
                return await _refresh(user, t["refresh_token"])
            except SeedrAuthError:
                pass                           # fall through to full login
    return await _login(user, pwd)


# ══════════════════════════════════════════════════════
# Low-level API calls
# ══════════════════════════════════════════════════════

async def _get(user: str, pwd: str, params: dict = None) -> dict:
    """GET /api/folder with auth."""
    tok = await _token(user, pwd)
    async with _http() as c:
        r = await c.get(_API_ROOT, params={"access_token": tok, **(params or {})})
    r.raise_for_status()
    return r.json()


async def _get_file(user: str, pwd: str, params: dict) -> dict:
    """Get file info — tries /api/file, falls back to resource.php.

    Free accounts often fail on /api/file — the resource.php endpoint
    (used by seedr_xbmc) works universally.
    """
    tok = await _token(user, pwd)
    # Try the standard API first (fast path for premium)
    try:
        async with _http(timeout=15) as c:
            r = await c.get(_API_FILE, params={"access_token": tok, **params})
        r.raise_for_status()
        data = r.json()
        if data.get("url"):
            return data
    except Exception:
        pass
    # Fallback: OAuth resource endpoint (works on free accounts)
    log.debug("[Seedr] /api/file failed, trying resource.php fallback")
    async with _http(timeout=20) as c:
        r = await c.post(
            _API_RESOURCE,
            data={
                "access_token": tok,
                "func": "fetch_file",
                **params,
            },
        )
    r.raise_for_status()
    return r.json()


async def _post(user: str, pwd: str, data: dict) -> dict:
    """
    POST /api/folder with auth.
    Raises SeedrError if the JSON body signals failure,
    even when HTTP status is 200.
    """
    tok = await _token(user, pwd)
    async with _http() as c:
        r = await c.post(_API_ROOT, data={"access_token": tok, **data})
    r.raise_for_status()

    try:
        body = r.json()
    except Exception:
        return {"result": True}  # some endpoints return non-JSON on success

    # Seedr signals errors with result:false even on HTTP 200
    result = body.get("result")
    if result is False or str(result).lower() == "false":
        msg = body.get("error") or body.get("message") or repr(body)
        raise SeedrError(f"Seedr API: {msg}")

    return body


# ══════════════════════════════════════════════════════
# Account operations
# ══════════════════════════════════════════════════════

async def _storage(user: str, pwd: str) -> dict:
    """Return {total, used, free} in bytes."""
    d = await _get(user, pwd)
    total = int(d.get("space_max",  d.get("storage_total", 0)) or 0)
    used  = int(d.get("space_used", d.get("storage_used",  0)) or 0)
    return {"total": total, "used": used, "free": max(0, total - used)}


async def _root(user: str, pwd: str) -> dict:
    """
    List root contents: folders, files, active torrents.
    Normalises progress to a float in [0, 100].
    """
    d = await _get(user, pwd)

    folders = [
        {"id": int(f["id"]), "name": f.get("name", ""), "size": int(f.get("size", 0) or 0)}
        for f in d.get("folders", []) if f.get("id") is not None
    ]
    files = [
        {"id": int(f["id"]), "name": f.get("name", "file"), "size": int(f.get("size", 0) or 0)}
        for f in d.get("files", []) if f.get("id") is not None
    ]
    torrents = []
    for t in d.get("torrents", []):
        try:
            pct = float(str(t.get("progress", 0)).replace("%", "").strip())
        except (ValueError, TypeError):
            pct = 0.0
        # FIX: cast torrent ID to int — _submit_magnet returns int,
        # so the poll comparison t["id"] == torrent_id must use same type
        _tid = t.get("id")
        try:
            _tid = int(_tid) if _tid is not None else None
        except (ValueError, TypeError):
            _tid = None
        torrents.append({
            "id":       _tid,
            "name":     t.get("name", ""),
            "progress": pct,
            "size":     int(t.get("size", 0) or 0),
        })

    return {"folders": folders, "files": files, "torrents": torrents,
            "space_used": d.get("space_used"), "space_max": d.get("space_max")}


async def _list_folder(user: str, pwd: str, folder_id: int) -> dict:
    """List folder contents — tries /api/folder, falls back to resource.php.

    Free accounts often return empty file lists from /api/folder.
    The resource.php endpoint works universally.
    """
    d = await _get(user, pwd, {"content_id": folder_id})

    folders = [
        {"id": int(f["id"]), "name": f.get("name", ""), "size": int(f.get("size", 0) or 0)}
        for f in d.get("folders", []) if f.get("id") is not None
    ]
    files = [
        {"id": int(f["id"]), "name": f.get("name", "file"), "size": int(f.get("size", 0) or 0)}
        for f in d.get("files", []) if f.get("id") is not None
    ]

    # Fallback: if /api/folder returned no files, try resource.php
    if not files:
        log.debug("[Seedr] /api/folder returned 0 files for folder %d — trying resource.php", folder_id)
        try:
            tok = await _token(user, pwd)
            async with _http(timeout=20) as c:
                r = await c.post(
                    _API_RESOURCE,
                    data={
                        "access_token": tok,
                        "func": "folder",
                        "content_id": folder_id,
                    },
                )
            r.raise_for_status()
            d2 = r.json()
            log.debug("[Seedr] resource.php folder %d response keys: %s", folder_id, list(d2.keys()))

            # Parse files from resource.php response
            files_raw = d2.get("files", [])
            for f in files_raw:
                fid = f.get("id") or f.get("folder_file_id")
                if fid is not None:
                    files.append({
                        "id": int(fid),
                        "name": f.get("name", "file"),
                        "size": int(f.get("size", 0) or 0),
                    })

            # Also parse folders from resource.php
            folders_raw = d2.get("folders", [])
            if folders_raw and not folders:
                for f in folders_raw:
                    fid = f.get("id")
                    if fid is not None:
                        folders.append({
                            "id": int(fid),
                            "name": f.get("name", ""),
                            "size": int(f.get("size", 0) or 0),
                        })

            log.info("[Seedr] resource.php fallback: %d files, %d folders for folder %d",
                     len(files), len(folders), folder_id)
        except Exception as e:
            log.warning("[Seedr] resource.php folder listing failed for %d: %s", folder_id, e)

    return {"folders": folders, "files": files}


async def _submit_magnet(user: str, pwd: str, magnet: str) -> Optional[int]:
    """
    Submit a magnet link via the Seedr OAuth resource endpoint.
    Works on free accounts — uses the access token, not Basic Auth.

    Endpoint: POST https://www.seedr.cc/oauth_test/resource.php
    Form fields: access_token, func=add_torrent, torrent_magnet=<magnet>
    """
    tok = await _token(user, pwd)
    async with _http() as c:
        r = await c.post(
            _API_RESOURCE,
            data={
                "access_token":  tok,
                "func":          "add_torrent",
                "torrent_magnet": magnet,
            },
        )
    r.raise_for_status()

    try:
        body = r.json()
    except Exception:
        return None

    result = body.get("result")
    if result is False or str(result).lower() == "false":
        msg = body.get("error") or body.get("message") or repr(body)
        raise SeedrError(f"Seedr torrent submit: {msg}")

    tid = body.get("torrent_id") or body.get("id")
    return int(tid) if tid else None


async def _file_url(user: str, pwd: str, file_id: int) -> str:
    d = await _get_file(user, pwd, {"folder_file_id": file_id})
    url = d.get("url", "")
    if not url:
        raise SeedrError(f"No URL for file id={file_id}: {d}")
    return url


async def _del_folder(user: str, pwd: str, folder_id: int) -> None:
    try:
        await _post(user, pwd, {"func": "delete", "delete_arr[]": f"folder_{folder_id}"})
    except Exception as e:
        log.warning("[Seedr] Delete folder %d (non-fatal): %s", folder_id, e)


async def _del_torrent(user: str, pwd: str, torrent_id: int) -> None:
    try:
        await _post(user, pwd, {"func": "delete", "delete_arr[]": f"torrent_{torrent_id}"})
    except Exception as e:
        log.warning("[Seedr] Delete torrent %d (non-fatal): %s", torrent_id, e)


async def _wipe_all_folders(user: str, pwd: str) -> int:
    """Delete every root folder to reclaim quota. Returns count deleted."""
    root = await _root(user, pwd)
    n = 0
    for f in root["folders"]:
        log.info("[Seedr] Wiping folder '%s' (id=%d)", f["name"], f["id"])
        await _del_folder(user, pwd, f["id"])
        n += 1
    return n


async def _ensure_free(user: str, pwd: str, needed: int = 0) -> int:
    """
    Guarantee at least `needed` (or _MIN_FREE, whichever is larger) bytes free.
    Auto-deletes all completed folders if space is tight.
    Returns bytes free after any cleanup.
    Raises SeedrQuotaError if still not enough.
    """
    want = max(needed, _MIN_FREE)
    s = await _storage(user, pwd)
    log.info(
        "[Seedr] %s: %.0f/%.0f MB used (%.0f MB free)",
        user[:25], s["used"] / 1e6, s["total"] / 1e6, s["free"] / 1e6,
    )

    if s["free"] >= want:
        return s["free"]

    # Try cleanup
    log.info("[Seedr] Low space on %s — wiping old folders…", user[:25])
    deleted = await _wipe_all_folders(user, pwd)

    if deleted:
        s = await _storage(user, pwd)
        log.info("[Seedr] After wipe: %.0f MB free", s["free"] / 1e6)

    if s["free"] < want:
        raise SeedrQuotaError(
            f"{user[:25]}: only {s['free']//1024//1024} MB free "
            f"(need ≥ {want//1024//1024} MB)"
        )
    return s["free"]


# ══════════════════════════════════════════════════════
# Account selector — picks the roomiest account
# ══════════════════════════════════════════════════════

async def _pick_account(needed: int = 0) -> tuple[str, str, int]:
    """
    Try all configured accounts.
    Returns (username, password, free_bytes) for the one with most space.
    Raises SeedrQuotaError / SeedrAuthError if nothing works.
    """
    candidates = _accounts()
    best_user = best_pwd = None
    best_free = -1
    last_err  = None

    for user, pwd in candidates:
        try:
            free = await _ensure_free(user, pwd, needed)
            if free > best_free:
                best_free = free
                best_user = user
                best_pwd  = pwd
        except (SeedrQuotaError, SeedrAuthError, SeedrError) as e:
            log.warning("[Seedr] Account %s skipped: %s", user[:25], e)
            last_err = e
        except Exception as e:
            log.warning("[Seedr] Account %s error: %s", user[:25], e)
            last_err = e

    if best_user is None:
        raise last_err or SeedrQuotaError("All Seedr accounts are full or unreachable.")

    log.info("[Seedr] Selected %s (%.0f MB free)", best_user[:25], best_free / 1e6)
    return best_user, best_pwd, best_free


# ══════════════════════════════════════════════════════
# Polling — waits for torrent to complete
# ══════════════════════════════════════════════════════

async def _poll(
    user: str,
    pwd:  str,
    torrent_id:          Optional[int],
    pre_existing_folder_ids: set,
    timeout_s:           int   = 7200,
    progress_cb:         Optional[Callable] = None,
    interval:            float = 10.0,
) -> dict:
    """
    Poll until the submitted torrent finishes.

    Strategy:
      1. Look for our torrent_id in the active torrents list.
         If found: report progress.
      2. Once the torrent disappears, look for a folder that
         wasn't there before we submitted.
      3. If neither torrent nor new folder shows up for too long
         after we first saw the torrent, raise an error.

    Returns the new folder dict {id, name, size}.
    """
    deadline      = time.monotonic() + timeout_s
    last_pct      = -1.0
    last_hb       = time.monotonic()
    ever_seen     = False    # did we ever see this torrent as active?
    gone_polls    = 0

    while time.monotonic() < deadline:
        try:
            root      = await _root(user, pwd)
            folders   = root["folders"]
            torrents  = root["torrents"]
            now       = time.monotonic()

            # ── Find our torrent ──────────────────────────────────
            active = None
            if torrent_id is not None:
                active = next((t for t in torrents if t["id"] == torrent_id), None)
            if active is None and torrents and not ever_seen:
                # Torrent ID not returned by API — track any active one
                active = torrents[0]

            if active is not None:
                ever_seen  = True
                gone_polls = 0
                pct  = active["progress"]
                name = active.get("name", "…")

                # FIX: If torrent shows 100% but is still in active list,
                # Seedr is finalising — check if folder appeared already
                if pct >= 100.0:
                    new_folders = [
                        f for f in folders
                        if f["id"] not in pre_existing_folder_ids
                    ]
                    if new_folders:
                        folder = max(new_folders, key=lambda f: f["id"])
                        log.info("[Seedr] ✅ Done (100%% + folder): '%s' (id=%d)",
                                 folder["name"], folder["id"])
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder

                # Report only on meaningful change or heartbeat
                if abs(pct - last_pct) >= 1.0 or (now - last_hb) >= 20.0:
                    last_pct = pct
                    last_hb  = now
                    log.info("[Seedr] %.1f%%  %s", pct, name)
                    if progress_cb:
                        await progress_cb(pct, name)

            else:
                # Torrent is not active (done, errored, or not started yet)
                new_folders = [
                    f for f in folders
                    if f["id"] not in pre_existing_folder_ids
                ]
                if new_folders:
                    # Pick the most recent one (highest id)
                    folder = max(new_folders, key=lambda f: f["id"])
                    log.info("[Seedr] ✅ Done: '%s' (id=%d)", folder["name"], folder["id"])
                    if progress_cb:
                        await progress_cb(100.0, folder["name"])
                    return folder

                gone_polls += 1

                if ever_seen:
                    log.info("[Seedr] Torrent gone, waiting for folder (attempt %d)…", gone_polls)

                    # FIX: After 3 failed attempts, fall back to ANY existing folder.
                    # This handles cached/duplicate magnets: Seedr returns torrent_id=None,
                    # the torrent completes instantly, and the folder was already in
                    # pre_existing_folder_ids from the snapshot. The folder IS there,
                    # we just filtered it out.
                    if gone_polls >= 3 and folders:
                        # Pick the largest folder (most likely to be our video)
                        folder = max(folders, key=lambda f: f.get("size", 0))
                        log.info("[Seedr] ✅ Fallback: using existing folder '%s' (id=%d, %d bytes)",
                                 folder["name"], folder["id"], folder.get("size", 0))
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder

                    if gone_polls >= 15:
                        raise SeedrError(
                            "Torrent vanished from Seedr without producing a folder. "
                            "It may have been rejected (private tracker, bad magnet, "
                            "or Seedr flagged it). Check your Seedr dashboard."
                        )
                else:
                    # Not yet visible — might be in Seedr's queue / metadata fetch phase
                    # FIX: Also check for cached torrent (torrent_id=None, instant complete)
                    if gone_polls >= 3 and torrent_id is None and folders:
                        folder = max(folders, key=lambda f: f.get("size", 0))
                        log.info("[Seedr] ✅ Cached torrent fallback: '%s' (id=%d)",
                                 folder["name"], folder["id"])
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder

                    if (now - last_hb) >= 20.0:
                        last_hb = now
                        log.debug("[Seedr] Waiting for torrent to appear…")
                        if progress_cb:
                            await progress_cb(0.0, "Waiting for Seedr to start…")

        except (SeedrError, SeedrAuthError):
            raise   # propagate hard errors
        except Exception as e:
            log.warning("[Seedr] Poll error (will retry): %s", e)

        await asyncio.sleep(interval)

    raise RuntimeError(
        f"Seedr timed out after {timeout_s // 60} min. "
        "The torrent may still be running — check your Seedr account."
    )


# ══════════════════════════════════════════════════════
# Recursive file URL collector
# ══════════════════════════════════════════════════════

async def _collect_files(user: str, pwd: str, folder_id: int) -> list[dict]:
    """
    Walk a folder tree and return all files as:
      [{name, url, size}, ...]

    Raises SeedrError if the folder itself cannot be listed.
    Individual file URL errors are logged and skipped.
    """
    result: list[dict] = []

    async def _walk(fid: int, depth: int = 0) -> None:
        if depth > 5:  # safety: prevent infinite recursion
            return
        try:
            contents = await _list_folder(user, pwd, fid)
        except Exception as e:
            if depth == 0:
                # Top-level folder listing failed — this IS fatal
                raise SeedrError(f"Cannot list Seedr folder {fid}: {e}")
            log.warning("[Seedr] Cannot list subfolder %d: %s", fid, e)
            return

        log.info("[Seedr] Folder %d (depth=%d): %d files, %d subfolders",
                 fid, depth, len(contents["files"]), len(contents["folders"]))

        for f in contents["files"]:
            fid2 = f.get("id")
            if not fid2:
                continue
            try:
                url = await _file_url(user, pwd, fid2)
                result.append({"name": f["name"], "url": url, "size": f.get("size", 0)})
                log.info("[Seedr] Got URL for '%s' (%s)",
                         f["name"], f.get("size", 0))
            except Exception as e:
                log.warning("[Seedr] URL error '%s': %s", f.get("name"), e)

        for sub in contents["folders"]:
            sid = sub.get("id")
            if sid:
                await _walk(sid, depth + 1)

    await _walk(folder_id)
    return result


# ══════════════════════════════════════════════════════
# Public high-level pipeline
# ══════════════════════════════════════════════════════

async def download_via_seedr(
    magnet:      str,
    dest:        str,
    progress_cb: Optional[Callable] = None,
    timeout_s:   int = 7200,
) -> list[str]:
    """
    Full pipeline:
      1. Pick account with most free space (auto-cleanup quota)
      2. Snapshot existing folders (to detect the new one)
      3. Submit magnet
      4. Poll until done
      5. Collect CDN download URLs
      6. Download to `dest`
      7. Cleanup Seedr folder (reclaim quota)

    progress_cb(stage, pct, detail) — stages:
      "selecting"   0%   picking account
      "submitting"  5%   sending magnet
      "waiting"     5%   metadata / queued
      "downloading" N%   torrent progress
      "fetching"   99%   getting CDN links
      "saving"     N%    writing local files

    Returns list of local file paths.
    """
    from services.downloader  import download_direct   # noqa: keep lazy
    from services.cc_sanitize import sanitize_filename  # noqa: keep lazy

    # ── Step 1: account selection ─────────────────────────────
    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")

    user, pwd, _ = await _pick_account()

    # ── Step 2: snapshot ─────────────────────────────────────
    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")

    root_before = await _root(user, pwd)
    pre_ids = {f["id"] for f in root_before["folders"]}

    # ── Step 3: submit magnet ─────────────────────────────────
    if progress_cb:
        await progress_cb("submitting", 5.0, "Submitting magnet to Seedr…")

    torrent_id = await _submit_magnet(user, pwd, magnet)
    log.info("[Seedr] Submitted. torrent_id=%s", torrent_id)

    # ── Step 4: poll ──────────────────────────────────────────
    if progress_cb:
        await progress_cb("waiting", 5.0, "Seedr is fetching torrent…")

    async def _pcb(pct: float, name: str) -> None:
        if progress_cb:
            stage = "downloading" if pct > 0.5 else "waiting"
            await progress_cb(stage, min(pct, 99.0), name or "Downloading…")

    folder = await _poll(
        user, pwd,
        torrent_id          = torrent_id,
        pre_existing_folder_ids = pre_ids,
        timeout_s           = timeout_s,
        progress_cb         = _pcb,
    )
    folder_id = folder["id"]

    # ── Step 5: collect file URLs ─────────────────────────────
    if progress_cb:
        await progress_cb("fetching", 99.0, "Getting CDN links…")

    files = await _collect_files(user, pwd, folder_id)

    # Edge case: single-file torrent sometimes lands as a root file, not a folder
    if not files:
        log.warning("[Seedr] Folder empty — checking for root file…")
        fresh = await _root(user, pwd)
        for f in fresh["files"]:
            fid2 = f.get("id")
            if fid2 and fid2 not in pre_ids:
                try:
                    url = await _file_url(user, pwd, fid2)
                    files.append({"name": f["name"], "url": url, "size": f["size"]})
                except Exception:
                    pass

    if not files:
        raise SeedrError(
            "Seedr produced no downloadable files. "
            "The torrent may be empty or all files are unsupported."
        )

    # ── Step 6: download locally ──────────────────────────────
    # FIX: Added progress reporting during local CDN download.
    # Previously, after Seedr reached 100%, the bot went silent during
    # what could be a multi-minute file download — users thought it froze.
    os.makedirs(dest, exist_ok=True)
    local_paths: list[str] = []

    for i, f in enumerate(files):
        clean = sanitize_filename(f["name"])
        fsize_f = f.get("size", 0)

        # Per-file progress callback that reports through the pipeline cb
        async def _file_progress(done: int, total: int, speed: float, eta: int,
                                 _i=i, _clean=clean, _fsize=fsize_f) -> None:
            if progress_cb:
                pct = (done / total * 100) if total else 0
                speed_s = f"{speed / 1e6:.1f} MB/s" if speed else ""
                await progress_cb(
                    "dl_file", pct,
                    f"⬇️ {_clean[:35]} ({_i+1}/{len(files)})",
                    done_bytes=done, total_bytes=total,
                    speed=speed, eta=eta,
                )

        if progress_cb:
            await progress_cb("dl_file", 0,
                              f"⬇️ {clean[:40]} ({i+1}/{len(files)})",
                              done_bytes=0, total_bytes=fsize_f,
                              speed=0.0, eta=0)

        log.info("[Seedr] Downloading %d/%d: %s", i + 1, len(files), f["name"])
        try:
            path = await download_direct(f["url"], dest, progress=_file_progress)
            # Rename to the sanitised filename if needed
            target = os.path.join(dest, clean)
            if path != target:
                try:
                    os.rename(path, target)
                    path = target
                except OSError:
                    pass
            local_paths.append(path)
        except Exception as e:
            log.error("[Seedr] Failed to download '%s': %s", f["name"], e)

    # ── Step 7: cleanup to reclaim quota ─────────────────────
    try:
        await _del_folder(user, pwd, folder_id)
        log.info("[Seedr] Cleaned folder id=%d", folder_id)
    except Exception as e:
        log.warning("[Seedr] Cleanup (non-fatal): %s", e)

    return local_paths


async def fetch_urls_via_seedr(
    magnet:      str,
    progress_cb: Optional[Callable] = None,
    timeout_s:   int = 7200,
) -> list[dict]:
    """
    Steps 1-5 of the full pipeline WITHOUT local download:
      1. Pick account with most free space
      2. Snapshot existing folders
      3. Submit magnet
      4. Poll until torrent completes on Seedr
      5. Collect CDN download URLs

    Returns list of {name, url, size} dicts.
    The Seedr folder is NOT deleted so that CloudConvert/FreeConvert
    can pull the file directly. Caller is responsible for cleanup
    via delete_folder(folder_id) once the conversion job is done.
    Returns (files, folder_id, user, pwd) so caller can clean up.
    """
    from services.cc_sanitize import sanitize_filename  # noqa: keep lazy

    # ── Step 1: account selection ─────────────────────────────
    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")

    user, pwd, _ = await _pick_account()

    # ── Step 2: snapshot ─────────────────────────────────────
    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")

    root_before = await _root(user, pwd)
    pre_ids = {f["id"] for f in root_before["folders"]}

    # ── Step 3: submit magnet ─────────────────────────────────
    if progress_cb:
        await progress_cb("submitting", 5.0, "Submitting magnet to Seedr…")

    torrent_id = await _submit_magnet(user, pwd, magnet)
    log.info("[Seedr] Submitted. torrent_id=%s", torrent_id)

    # ── Step 4: poll ──────────────────────────────────────────
    if progress_cb:
        await progress_cb("waiting", 5.0, "Seedr is fetching torrent…")

    async def _pcb(pct: float, name: str) -> None:
        if progress_cb:
            stage = "downloading" if pct > 0.5 else "waiting"
            await progress_cb(stage, min(pct, 99.0), name or "Downloading…")

    folder = await _poll(
        user, pwd,
        torrent_id              = torrent_id,
        pre_existing_folder_ids = pre_ids,
        timeout_s               = timeout_s,
        progress_cb             = _pcb,
    )
    folder_id = folder["id"]

    # ── Step 5: collect file URLs ─────────────────────────────
    if progress_cb:
        await progress_cb("fetching", 99.0, "Getting CDN links…")

    files = await _collect_files(user, pwd, folder_id)

    # Edge case: single-file torrent lands as a root file
    if not files:
        log.warning("[Seedr] Folder empty — checking for root file…")
        fresh = await _root(user, pwd)
        for f in fresh["files"]:
            fid2 = f.get("id")
            if fid2 and fid2 not in pre_ids:
                try:
                    url = await _file_url(user, pwd, fid2)
                    files.append({"name": f["name"], "url": url, "size": f["size"]})
                except Exception:
                    pass

    if not files:
        raise SeedrError(
            "Seedr produced no downloadable files. "
            "The torrent may be empty or all files are unsupported."
        )

    # Sanitise filenames in metadata (URL stays raw)
    for f in files:
        f["clean_name"] = sanitize_filename(f["name"])

    # Return files + context needed for deferred cleanup
    return files, folder_id, user, pwd


# ══════════════════════════════════════════════════════
# Convenience / legacy-compat public helpers
# ══════════════════════════════════════════════════════

async def check_credentials() -> bool:
    """Return True if at least one account authenticates successfully."""
    try:
        await _pick_account()
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    """Storage info for the first configured account."""
    user, pwd = _accounts()[0]
    return await _storage(user, pwd)


async def add_magnet(magnet: str) -> dict:
    """Submit a magnet on the best available account. Returns {result, torrent_id}."""
    user, pwd, _ = await _pick_account()
    tid = await _submit_magnet(user, pwd, magnet)
    return {"result": True, "torrent_id": tid}


async def list_folder(folder_id: int = 0) -> dict:
    """List root (folder_id=0) or a specific folder."""
    user, pwd = _accounts()[0]
    if folder_id == 0:
        return await _root(user, pwd)
    return await _list_folder(user, pwd, folder_id)


async def delete_folder(folder_id: int) -> None:
    user, pwd = _accounts()[0]
    await _del_folder(user, pwd, folder_id)


async def delete_torrent(torrent_id: int) -> None:
    user, pwd = _accounts()[0]
    await _del_torrent(user, pwd, torrent_id)
