"""
services/seedr.py  — FIXED v2
═══════════════════════════════════════════════════════════════════════

FIXES IN THIS VERSION
─────────────────────
FIX SEEDR-AUTH: All API calls now send both `access_token` as a query/body
  param AND `Authorization: Bearer {tok}` as an HTTP header.  Seedr's API
  update added header-based auth; the old token-in-params approach no longer
  works for folder listing, causing _root() to return empty data and breaking
  all three Seedr flows (download, Seedr+CC hardsub, Seedr+FC).

FIX SEEDR-ROOT-FALLBACK: _root() now falls back to the resource.php endpoint
  if /api/folder returns empty or fails.  This is the same endpoint used by
  _submit_magnet and _list_folder, ensuring torrent-progress polling works
  even when /api/folder's auth requirement changes.

FIX SEEDR-LIST-ROBUST: _list_folder() resource.php fallback now correctly
  handles both `id` and `folder_file_id` field names from the API response.

Everything else (download_via_seedr, fetch_urls_via_seedr, public helpers)
is unchanged — callers don't need any modifications.
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

_OAUTH_URL    = "https://www.seedr.cc/oauth_test/token"
_API_ROOT     = "https://www.seedr.cc/api/folder"
_API_FILE     = "https://www.seedr.cc/api/file"
_API_RESOURCE = "https://www.seedr.cc/oauth_test/resource.php"
_CLIENT_ID    = "seedr_xbmc"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

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
        if remaining > 90:
            return t["access_token"]
        if t["refresh_token"]:
            try:
                return await _refresh(user, t["refresh_token"])
            except SeedrAuthError:
                pass
    return await _login(user, pwd)


# ══════════════════════════════════════════════════════
# Low-level API calls
# FIX SEEDR-AUTH: all requests now send both token-in-params AND
# Authorization: Bearer header to support both old and new Seedr API auth.
# ══════════════════════════════════════════════════════

def _auth_headers(tok: str) -> dict:
    """
    FIX SEEDR-AUTH: return headers that satisfy both old and new Seedr auth.
    Old API: access_token in query params / POST body.
    New API: Authorization: Bearer header.
    We send both to be safe.
    """
    return {
        "Authorization": f"Bearer {tok}",
        "User-Agent": _UA,
    }


async def _get(user: str, pwd: str, params: dict = None) -> dict:
    """GET /api/folder with auth (token in params AND Authorization header)."""
    tok = await _token(user, pwd)
    async with _http() as c:
        r = await c.get(
            _API_ROOT,
            params={"access_token": tok, **(params or {})},
            headers=_auth_headers(tok),
        )
    r.raise_for_status()
    return r.json()


async def _post(user: str, pwd: str, data: dict) -> dict:
    """
    POST /api/folder with auth.
    FIX SEEDR-AUTH: sends Authorization header in addition to body param.
    Raises SeedrError if the JSON body signals failure.
    """
    tok = await _token(user, pwd)
    async with _http() as c:
        r = await c.post(
            _API_ROOT,
            data={"access_token": tok, **data},
            headers=_auth_headers(tok),
        )
    r.raise_for_status()

    try:
        body = r.json()
    except Exception:
        return {"result": True}

    result = body.get("result")
    if result is False or str(result).lower() == "false":
        msg = body.get("error") or body.get("message") or repr(body)
        raise SeedrError(f"Seedr API: {msg}")

    return body


async def _resource_post(user: str, pwd: str, func: str, extra: dict = None) -> dict:
    """
    POST to resource.php endpoint — works on all Seedr plans.
    FIX SEEDR-AUTH: sends both token-in-body and Authorization header.
    """
    tok = await _token(user, pwd)
    payload = {
        "access_token": tok,
        "func": func,
        **(extra or {}),
    }
    async with _http() as c:
        r = await c.post(
            _API_RESOURCE,
            data=payload,
            headers=_auth_headers(tok),
        )
    r.raise_for_status()
    try:
        body = r.json()
    except Exception:
        return {"result": True}

    result = body.get("result")
    if result is False or str(result).lower() == "false":
        msg = body.get("error") or body.get("message") or repr(body)
        raise SeedrError(f"Seedr resource API: {msg}")

    return body


# ══════════════════════════════════════════════════════
# Root folder parsing helpers
# ══════════════════════════════════════════════════════

def _parse_folders(raw_list: list) -> list[dict]:
    result = []
    for f in raw_list:
        fid = f.get("id")
        if fid is None:
            continue
        try:
            result.append({
                "id":   int(fid),
                "name": f.get("name", ""),
                "size": int(f.get("size", 0) or 0),
            })
        except (ValueError, TypeError):
            pass
    return result


def _parse_files(raw_list: list) -> list[dict]:
    result = []
    for f in raw_list:
        # resource.php may use "folder_file_id" instead of "id"
        fid = f.get("id") or f.get("folder_file_id")
        if fid is None:
            continue
        try:
            result.append({
                "id":   int(fid),
                "name": f.get("name", "file"),
                "size": int(f.get("size", 0) or 0),
            })
        except (ValueError, TypeError):
            pass
    return result


def _parse_torrents(raw_list: list) -> list[dict]:
    result = []
    for t in raw_list:
        try:
            pct = float(str(t.get("progress", 0)).replace("%", "").strip())
        except (ValueError, TypeError):
            pct = 0.0
        _tid = t.get("id")
        try:
            _tid = int(_tid) if _tid is not None else None
        except (ValueError, TypeError):
            _tid = None
        result.append({
            "id":       _tid,
            "name":     t.get("name", ""),
            "progress": pct,
            "size":     int(t.get("size", 0) or 0),
        })
    return result


def _root_from_dict(d: dict) -> dict:
    """Build the root dict from a raw API response (handles both /api/folder and resource.php)."""
    folders  = _parse_folders(d.get("folders", []))
    files    = _parse_files(d.get("files", []))
    torrents = _parse_torrents(d.get("torrents", []))
    return {
        "folders":    folders,
        "files":      files,
        "torrents":   torrents,
        "space_used": d.get("space_used"),
        "space_max":  d.get("space_max"),
    }


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

    FIX SEEDR-ROOT-FALLBACK: tries /api/folder first; if it returns empty
    data (e.g., because the API now requires a different auth format),
    falls back to resource.php which is more reliable across API versions.
    """
    d: dict = {}

    # ── Primary: /api/folder ──────────────────────────────────
    try:
        d = await _get(user, pwd)
        result = _root_from_dict(d)

        # If we got meaningful data, return immediately
        if result["folders"] or result["files"] or result["torrents"]:
            log.debug("[Seedr] _root via /api/folder: %d folders, %d torrents",
                      len(result["folders"]), len(result["torrents"]))
            return result

        # Got a response but it's empty — may still be valid (truly empty account)
        # Fall through to resource.php to double-check
        log.debug("[Seedr] /api/folder returned empty root, verifying with resource.php")
    except Exception as e:
        log.warning("[Seedr] /api/folder root failed (%s) — trying resource.php", e)

    # ── Fallback: resource.php ────────────────────────────────
    try:
        d2 = await _resource_post(user, pwd, "folder", {"content_id": "0"})
        result2 = _root_from_dict(d2)
        log.debug("[Seedr] _root via resource.php: %d folders, %d torrents",
                  len(result2["folders"]), len(result2["torrents"]))
        return result2
    except Exception as e2:
        log.warning("[Seedr] resource.php root also failed: %s", e2)

    # Both failed — return the best we have (possibly empty from first call)
    return _root_from_dict(d) if d else {"folders": [], "files": [], "torrents": []}


async def _list_folder(user: str, pwd: str, folder_id: int) -> dict:
    """
    List folder contents.
    Tries /api/folder first, falls back to resource.php.
    FIX SEEDR-LIST-ROBUST: handles both 'id' and 'folder_file_id' field names.
    """
    # ── Primary: /api/folder ──────────────────────────────────
    try:
        d = await _get(user, pwd, {"content_id": folder_id})
        folders = _parse_folders(d.get("folders", []))
        files   = _parse_files(d.get("files", []))
        if files or folders:
            log.debug("[Seedr] _list_folder %d via /api/folder: %d files", folder_id, len(files))
            return {"folders": folders, "files": files}
    except Exception as e:
        log.debug("[Seedr] /api/folder list %d failed: %s — trying resource.php", folder_id, e)

    # ── Fallback: resource.php ────────────────────────────────
    log.debug("[Seedr] resource.php fallback for folder %d", folder_id)
    try:
        d2 = await _resource_post(user, pwd, "folder", {"content_id": folder_id})
        folders2 = _parse_folders(d2.get("folders", []))
        files2   = _parse_files(d2.get("files", []))
        log.info("[Seedr] resource.php folder %d: %d files, %d subfolders",
                 folder_id, len(files2), len(folders2))
        return {"folders": folders2, "files": files2}
    except Exception as e2:
        log.warning("[Seedr] resource.php list %d failed: %s", folder_id, e2)
        return {"folders": [], "files": []}


async def _submit_magnet(user: str, pwd: str, magnet: str) -> Optional[int]:
    """
    Submit a magnet link via the resource.php endpoint.
    FIX SEEDR-AUTH: also sends Authorization header.
    """
    tok = await _token(user, pwd)
    async with _http() as c:
        r = await c.post(
            _API_RESOURCE,
            data={
                "access_token":   tok,
                "func":           "add_torrent",
                "torrent_magnet": magnet,
            },
            headers=_auth_headers(tok),
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


async def _get_file(user: str, pwd: str, params: dict) -> dict:
    """
    Get file download URL.
    FIX SEEDR-AUTH: sends Authorization header on both attempts.
    """
    tok = await _token(user, pwd)
    headers = _auth_headers(tok)

    # ── Try /api/file first ───────────────────────────────────
    try:
        async with _http(timeout=15) as c:
            r = await c.get(
                _API_FILE,
                params={"access_token": tok, **params},
                headers=headers,
            )
        r.raise_for_status()
        data = r.json()
        if data.get("url"):
            return data
    except Exception:
        pass

    # ── Fallback: resource.php fetch_file ─────────────────────
    log.debug("[Seedr] /api/file failed, trying resource.php fetch_file")
    async with _http(timeout=20) as c:
        r = await c.post(
            _API_RESOURCE,
            data={
                "access_token": tok,
                "func":         "fetch_file",
                **params,
            },
            headers=headers,
        )
    r.raise_for_status()
    return r.json()


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
    """Guarantee at least `needed` (or _MIN_FREE) bytes free."""
    want = max(needed, _MIN_FREE)
    s = await _storage(user, pwd)
    log.info(
        "[Seedr] %s: %.0f/%.0f MB used (%.0f MB free)",
        user[:25], s["used"] / 1e6, s["total"] / 1e6, s["free"] / 1e6,
    )

    if s["free"] >= want:
        return s["free"]

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
# Account selector
# ══════════════════════════════════════════════════════

async def _pick_account(needed: int = 0) -> tuple[str, str, int]:
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
    torrent_id:              Optional[int],
    pre_existing_folder_ids: set,
    timeout_s:               int   = 7200,
    progress_cb:             Optional[Callable] = None,
    interval:                float = 10.0,
) -> dict:
    """
    Poll until the submitted torrent finishes.
    Returns the new folder dict {id, name, size}.
    """
    deadline      = time.monotonic() + timeout_s
    last_pct      = -1.0
    last_hb       = time.monotonic()
    ever_seen     = False
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
                active = torrents[0]

            if active is not None:
                ever_seen  = True
                gone_polls = 0
                pct  = active["progress"]
                name = active.get("name", "…")

                # 100% but still in active list — check if folder appeared
                if pct >= 100.0:
                    new_folders = [f for f in folders if f["id"] not in pre_existing_folder_ids]
                    if new_folders:
                        folder = max(new_folders, key=lambda f: f["id"])
                        log.info("[Seedr] ✅ Done (100%% + folder): '%s' (id=%d)",
                                 folder["name"], folder["id"])
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder

                if abs(pct - last_pct) >= 1.0 or (now - last_hb) >= 20.0:
                    last_pct = pct
                    last_hb  = now
                    log.info("[Seedr] %.1f%%  %s", pct, name)
                    if progress_cb:
                        await progress_cb(pct, name)

            else:
                # Torrent not active — check for new folder
                new_folders = [f for f in folders if f["id"] not in pre_existing_folder_ids]
                if new_folders:
                    folder = max(new_folders, key=lambda f: f["id"])
                    log.info("[Seedr] ✅ Done: '%s' (id=%d)", folder["name"], folder["id"])
                    if progress_cb:
                        await progress_cb(100.0, folder["name"])
                    return folder

                gone_polls += 1

                if ever_seen:
                    log.info("[Seedr] Torrent gone, waiting for folder (attempt %d)…", gone_polls)
                    if gone_polls >= 3 and folders:
                        folder = max(folders, key=lambda f: f.get("size", 0))
                        log.info("[Seedr] ✅ Fallback: '%s' (id=%d)", folder["name"], folder["id"])
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder
                    if gone_polls >= 15:
                        raise SeedrError(
                            "Torrent vanished from Seedr without producing a folder. "
                            "It may have been rejected (private tracker, bad magnet)."
                        )
                else:
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
            raise
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
    """Walk a folder tree and return all files as [{name, url, size}, ...]."""
    result: list[dict] = []

    async def _walk(fid: int, depth: int = 0) -> None:
        if depth > 5:
            return
        try:
            contents = await _list_folder(user, pwd, fid)
        except Exception as e:
            if depth == 0:
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
                log.info("[Seedr] Got URL for '%s'", f["name"])
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
    Full pipeline: submit magnet → poll → collect URLs → download locally → cleanup.

    progress_cb(stage, pct, detail, **kw) — stages:
      "selecting", "submitting", "waiting", "downloading" — 3-arg calls
      "dl_file"  — also passes done_bytes, total_bytes, speed, eta as kwargs
    """
    from services.downloader  import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")

    user, pwd, _ = await _pick_account()

    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")

    root_before = await _root(user, pwd)
    pre_ids = {f["id"] for f in root_before["folders"]}

    if progress_cb:
        await progress_cb("submitting", 5.0, "Submitting magnet to Seedr…")

    torrent_id = await _submit_magnet(user, pwd, magnet)
    log.info("[Seedr] Submitted. torrent_id=%s", torrent_id)

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

    if progress_cb:
        await progress_cb("fetching", 99.0, "Getting CDN links…")

    files = await _collect_files(user, pwd, folder_id)

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

    os.makedirs(dest, exist_ok=True)
    local_paths: list[str] = []

    for i, f in enumerate(files):
        clean   = sanitize_filename(f["name"])
        fsize_f = f.get("size", 0)

        async def _file_progress(done: int, total: int, speed: float, eta: int,
                                  _i=i, _clean=clean, _fsize=fsize_f) -> None:
            if progress_cb:
                pct = (done / total * 100) if total else 0
                await progress_cb(
                    "dl_file", pct,
                    f"⬇️ {_clean[:35]} ({_i+1}/{len(files)})",
                    done_bytes=done, total_bytes=total,
                    speed=speed, eta=eta,
                )

        if progress_cb:
            await progress_cb(
                "dl_file", 0.0,
                f"⬇️ {clean[:40]} ({i+1}/{len(files)})",
                done_bytes=0, total_bytes=fsize_f, speed=0.0, eta=0,
            )

        log.info("[Seedr] Downloading %d/%d: %s", i + 1, len(files), f["name"])
        try:
            path = await download_direct(f["url"], dest, progress=_file_progress)
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
) -> tuple:
    """
    Steps 1–5 without local download.
    Returns (files, folder_id, user, pwd).
    Caller is responsible for calling _del_folder() after conversion completes.
    files[i] = {name, url, size, clean_name}
    """
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")

    user, pwd, _ = await _pick_account()

    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")

    root_before = await _root(user, pwd)
    pre_ids = {f["id"] for f in root_before["folders"]}

    if progress_cb:
        await progress_cb("submitting", 5.0, "Submitting magnet to Seedr…")

    torrent_id = await _submit_magnet(user, pwd, magnet)
    log.info("[Seedr] Submitted. torrent_id=%s", torrent_id)

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

    if progress_cb:
        await progress_cb("fetching", 99.0, "Getting CDN links…")

    files = await _collect_files(user, pwd, folder_id)

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

    for f in files:
        f["clean_name"] = sanitize_filename(f["name"])

    return files, folder_id, user, pwd


# ══════════════════════════════════════════════════════
# Convenience / legacy-compat public helpers
# ══════════════════════════════════════════════════════

async def check_credentials() -> bool:
    try:
        await _pick_account()
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    user, pwd = _accounts()[0]
    return await _storage(user, pwd)


async def add_magnet(magnet: str) -> dict:
    user, pwd, _ = await _pick_account()
    tid = await _submit_magnet(user, pwd, magnet)
    return {"result": True, "torrent_id": tid}


async def list_folder(folder_id: int = 0) -> dict:
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
