"""
services/seedr.py  —  v6

PATCH v6 — rename files on Seedr before fetching CDN URLs
──────────────────────────────────────────────────────────
ROOT CAUSE of INPUT_FILES_NOT_FOUND on CloudConvert (import/url path):
  Seedr CDN URLs embed the original filename in the path, e.g.:
    https://rd16.seedr.cc/ff_get/5914296051/[Erai-raws]_Show_S01E08_[1080p].mkv?st=...
  CloudConvert's ffmpeg arg parser tokenises on whitespace but also chokes
  on brackets ( [], {}, () ) and other shell-special chars in the path.
  Result: ffmpeg can't open /input/.../[Erai-raws]_Show.mkv → no output →
  export task returns INPUT_FILES_NOT_FOUND.

FIX (v6):
  _clean_seedr_name(name)  — strips [] {} () and replaces unsafe chars with _
  _rename_file(user, pwd, file_id, new_name)  — renames via resource.php
  _collect_files() → _walk():
    1. Compute clean name for every file
    2. If different from original → rename on Seedr (resource.php)
    3. Clear the stale embedded URL (it still carries the old name)
    4. Re-fetch via _file_url_fallback() → fresh URL has the clean name
    5. Pass that clean URL directly to CC import/url — no local download

ROOT CAUSE of original error (v5):
  seedrcc was updated: list_contents() returns a typed object, not a dict.
  Fixed by _to_dict() universal converter.

PUBLIC API UNCHANGED.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re as _re
import time
from typing import Callable, Optional

log = logging.getLogger(__name__)


# ── Exceptions ────────────────────────────────────────────────────────────────

class SeedrError(RuntimeError):
    pass

class SeedrAuthError(SeedrError):
    pass

class SeedrQuotaError(SeedrError):
    pass


# ── Constants ─────────────────────────────────────────────────────────────────

_OAUTH_URL    = "https://www.seedr.cc/oauth_test/token"
_API_RESOURCE = "https://www.seedr.cc/oauth_test/resource.php"
_CLIENT_ID    = "seedr_xbmc"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_MIN_FREE = 256 * 1024 * 1024  # 256 MB

_CLIENT_CACHE: dict[str, object] = {}


# ══════════════════════════════════════════════════════════════════════════════
# NEW v6: Clean name generator + Seedr rename
# ══════════════════════════════════════════════════════════════════════════════

def _clean_seedr_name(name: str) -> str:
    """
    Produce a filename safe for CloudConvert's ffmpeg argument parser.

    Removes bracket groups entirely:  [Group],  {tag},  (info)
    Replaces remaining unsafe chars (spaces, !@#$ etc.) with underscore.
    Collapses consecutive underscores and strips leading/trailing ones.

    Examples:
      "[Erai-raws] Show S01E08 [1080p][AAC].mkv" → "Show_S01E08.mkv"
      "My Show - 04 (WEB-DL).mkv"                → "My_Show_-_04.mkv"
      "Already_Clean_S01E01.mkv"                 → "Already_Clean_S01E01.mkv"  (unchanged)
    """
    base, ext = os.path.splitext(name)
    # Remove bracket groups and their content
    base = _re.sub(r'\[[^\]]*\]|\{[^}]*\}|\([^)]*\)', '', base)
    # Replace anything that isn't alphanumeric, dash or dot with underscore
    base = _re.sub(r'[^\w\-]', '_', base)
    # Collapse multiple underscores, strip edges
    base = _re.sub(r'_+', '_', base).strip('_')
    return (base or 'video') + ext.lower()


async def _rename_file(
    username: str, password: str, file_id: int, new_name: str
) -> bool:
    """
    Rename a Seedr file (folder_file) via resource.php.
    Returns True on success, False on any failure (non-fatal — caller falls
    back to using the original name / URL).
    """
    import httpx
    client_obj = await _get_client(username, password)
    token = _extract_token(client_obj) or await _fresh_token(username, password, proxy=False)
    if not token:
        log.warning("[Seedr] _rename_file: no token available")
        return False
    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": _UA},
            timeout=20, follow_redirects=True,
        ) as c:
            r = await c.post(_API_RESOURCE, data={
                "access_token":   token,
                "func":           "rename",
                "folder_file_id": str(file_id),
                "rename_to":      new_name,
            })
            r.raise_for_status()
            body = r.json()
        result = body.get("result")
        ok = result is True or str(result).lower() in ("true", "1")
        if ok:
            log.info("[Seedr] ✅ Renamed file %d → %s", file_id, new_name)
        else:
            log.warning("[Seedr] ⚠ Rename file %d failed: %s", file_id, body)
        return ok
    except Exception as exc:
        log.warning("[Seedr] _rename_file(%d, %r): %s", file_id, new_name, exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Universal response converter
# ══════════════════════════════════════════════════════════════════════════════

def _to_dict(obj) -> dict:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return obj
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict") and callable(obj.dict):
        return obj.dict()
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        return obj.to_dict()
    if hasattr(obj, "as_dict") and callable(obj.as_dict):
        return obj.as_dict()
    if hasattr(obj, "__dict__"):
        return {k: _to_dict(v) for k, v in vars(obj).items()
                if not k.startswith("_")}
    return obj


def _attr(obj, *keys, default=None):
    for key in keys:
        if isinstance(obj, dict):
            v = obj.get(key)
        else:
            v = getattr(obj, key, None)
        if v is not None:
            return v
    return default


def _list_of(obj, *keys) -> list:
    for key in keys:
        if isinstance(obj, dict):
            v = obj.get(key)
        else:
            v = getattr(obj, key, None)
        if v is not None:
            return list(v) if not isinstance(v, list) else v
    return []


# ══════════════════════════════════════════════════════════════════════════════
# Config helpers
# ══════════════════════════════════════════════════════════════════════════════

def _accounts() -> list[tuple[str, str]]:
    users = [u.strip() for u in os.environ.get("SEEDR_USERNAME", "").split(",") if u.strip()]
    pwds  = [p.strip() for p in os.environ.get("SEEDR_PASSWORD", "").split(",") if p.strip()]
    if not users or not pwds:
        raise SeedrAuthError(
            "Seedr credentials missing.\n"
            "Set SEEDR_USERNAME and SEEDR_PASSWORD in .env / Colab secrets."
        )
    if len(pwds) == 1:
        pwds = pwds * len(users)
    return list(zip(users, pwds))


def _proxy() -> Optional[str]:
    return os.environ.get("SEEDR_PROXY", "").strip() or None


# ══════════════════════════════════════════════════════════════════════════════
# seedrcc client factory
# ══════════════════════════════════════════════════════════════════════════════

async def _get_client(username: str, password: str):
    from seedrcc import AsyncSeedr
    cached = _CLIENT_CACHE.get(username)
    if cached is not None:
        try:
            await cached.get_settings()
            return cached
        except Exception:
            log.info("[Seedr] Stale client for %s — re-authenticating", username[:20])
            _CLIENT_CACHE.pop(username, None)
    try:
        client = await AsyncSeedr.from_password(username, password)
        _CLIENT_CACHE[username] = client
        log.info("[Seedr] Authenticated: %s", username[:25])
        return client
    except Exception as exc:
        raise SeedrAuthError(f"Seedr login failed ({username[:25]}): {exc}") from exc


def _invalidate(username: str) -> None:
    _CLIENT_CACHE.pop(username, None)


# ══════════════════════════════════════════════════════════════════════════════
# Response normalisation
# ══════════════════════════════════════════════════════════════════════════════

def _parse_progress(raw) -> float:
    if raw is None:
        return 0.0
    try:
        return float(str(raw).replace("%", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _file_id(f) -> Optional[int]:
    for key in ("folder_file_id", "id", "file_id"):
        v = _attr(f, key)
        if v is not None:
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
    return None


def _normalise_contents(raw) -> dict:
    raw_type = type(raw).__name__
    if raw_type != "dict":
        log.debug("[Seedr] Converting %s → dict", raw_type)

    actual = raw
    for unwrap_key in ("result", "folder", "data"):
        candidate = _attr(raw, unwrap_key)
        if candidate is not None and hasattr(candidate, "__dict__") or (
            isinstance(candidate, dict) and (
                "files" in candidate or "folders" in candidate
            )
        ):
            actual = candidate
            log.debug("[Seedr] Unwrapped response under '%s'", unwrap_key)
            break

    folders_raw  = _list_of(actual, "folders")
    files_raw    = _list_of(actual, "files", "folder_files")
    torrents_raw = _list_of(actual, "torrents")

    folders: list[dict] = []
    for f in folders_raw:
        fid  = _attr(f, "id")
        name = _attr(f, "name", default="")
        size = _attr(f, "size", default=0)
        if fid is None:
            continue
        try:
            folders.append({"id": int(fid), "name": name, "size": int(size or 0)})
        except (ValueError, TypeError):
            pass

    files: list[dict] = []
    for f in files_raw:
        fid = _file_id(f)
        if fid is None:
            continue
        name = _attr(f, "name", default="file")
        size = _attr(f, "size", default=0)
        url = (
            _attr(f, "url") or
            _attr(f, "download_url") or
            _attr(f, "stream_url") or
            _attr(f, "link") or
            _attr(f, "href") or
            ""
        )
        files.append({
            "id":   fid,
            "name": name,
            "size": int(size or 0),
            "url":  url or "",
        })

    torrents: list[dict] = []
    for t in torrents_raw:
        tid = _attr(t, "id")
        try:
            tid = int(tid) if tid is not None else None
        except (ValueError, TypeError):
            tid = None
        torrents.append({
            "id":       tid,
            "name":     _attr(t, "name", default=""),
            "progress": _parse_progress(_attr(t, "progress")),
            "size":     int(_attr(t, "size", default=0) or 0),
        })

    n_with_url = sum(1 for f in files if f.get("url"))
    log.debug(
        "[Seedr] Normalised: %d folders, %d files (%d with embedded URL), %d torrents",
        len(folders), len(files), n_with_url, len(torrents),
    )

    return {
        "folders":    folders,
        "files":      files,
        "torrents":   torrents,
        "space_used": _attr(actual, "space_used"),
        "space_max":  _attr(actual, "space_max"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Core API wrappers
# ══════════════════════════════════════════════════════════════════════════════

async def _root(username: str, password: str) -> dict:
    client = await _get_client(username, password)
    try:
        raw = await client.list_contents(folder_id='0')
        return _normalise_contents(raw)
    except (SeedrError, SeedrAuthError):
        raise
    except Exception as exc:
        _invalidate(username)
        raise SeedrError(f"list_contents(root) failed: {exc}") from exc


async def _list_folder(username: str, password: str, folder_id: int) -> dict:
    client = await _get_client(username, password)
    try:
        raw = await client.list_contents(folder_id=str(folder_id))
        return _normalise_contents(raw)
    except (SeedrError, SeedrAuthError):
        raise
    except Exception as exc:
        _invalidate(username)
        raise SeedrError(f"list_contents({folder_id}) failed: {exc}") from exc


async def _file_url_fallback(username: str, password: str, file_id: int) -> str:
    """
    Get CDN URL via fetch_file or resource.php.
    Called after rename so the returned URL carries the new clean name.
    """
    client = await _get_client(username, password)

    try:
        raw  = await client.fetch_file(file_id=str(file_id))
        data = _to_dict(raw)
        url  = data.get("url") or data.get("download_url") or ""
        if not url and isinstance(data.get("result"), dict):
            url = data["result"].get("url") or data["result"].get("download_url") or ""
        if not url and isinstance(data.get("data"), dict):
            url = data["data"].get("url") or data["data"].get("download_url") or ""
        if not url:
            rv = data.get("result")
            if isinstance(rv, str) and rv.startswith("http"):
                url = rv
        if url:
            return url
        log.warning(
            "[Seedr] fetch_file(%d) returned empty URL. "
            "Keys: %s  url=%r  name=%r",
            file_id, list(data.keys()), data.get("url"), data.get("name"),
        )
    except Exception as exc:
        log.warning("[Seedr] fetch_file(%d): %s", file_id, exc)
        _invalidate(username)

    # Fallback: resource.php
    import httpx
    client2 = await _get_client(username, password)
    token   = _extract_token(client2) or await _fresh_token(username, password, proxy=False)
    if not token:
        raise SeedrError("No token available for resource.php fallback")

    async with httpx.AsyncClient(
        headers={"User-Agent": _UA, "Authorization": f"Bearer {token}"},
        timeout=20, follow_redirects=True,
    ) as c:
        r = await c.post(_API_RESOURCE, data={
            "access_token":   token,
            "func":           "fetch_file",
            "folder_file_id": str(file_id),
        })
        r.raise_for_status()
        body = r.json()

    url = body.get("url") or body.get("download_url") or ""
    if isinstance(body.get("result"), str) and body["result"].startswith("http"):
        url = body["result"]
    if url:
        return url
    raise SeedrError(f"resource.php fetch_file: no URL. Response: {body}")


async def _storage(username: str, password: str) -> dict:
    _TOTAL_KEYS = ("space_max", "storage_max", "storage_total", "space_total",
                   "total_space", "quota", "quota_total", "disk_space")
    _USED_KEYS  = ("space_used", "storage_used", "used_space",
                   "quota_used", "disk_used")

    def _read_storage(data: dict) -> tuple[int, int]:
        sources = [data]
        account = data.get("account")
        if isinstance(account, dict):
            sources.append(account)

        total = used = 0
        for src in sources:
            for k in _TOTAL_KEYS:
                v = src.get(k)
                if v:
                    try:
                        total = int(v)
                        break
                    except (ValueError, TypeError):
                        pass
            if total:
                break
        for src in sources:
            for k in _USED_KEYS:
                v = src.get(k)
                if v is not None:
                    try:
                        used = int(v)
                        break
                    except (ValueError, TypeError):
                        pass
            if used:
                break
        return total, used

    client = await _get_client(username, password)

    try:
        raw  = await client.get_settings()
        data = _to_dict(raw)
        total, used = _read_storage(data)
        if total > 0:
            free = max(0, total - used)
            log.info("[Seedr] Storage (settings): %.0f/%.0f MB (%.0f MB free)",
                     used / 1e6, total / 1e6, free / 1e6)
            return {"total": total, "used": used, "free": free, "unknown": False}
        log.warning(
            "[Seedr] get_settings() returned 0 for all storage fields. "
            "Available keys: %s  Sample values: %s",
            list(data.keys())[:20],
            {k: data[k] for k in list(data.keys())[:10]},
        )
    except Exception as exc:
        log.warning("[Seedr] get_settings() failed: %s", exc)

    try:
        contents = await _root(username, password)
        total = int(contents.get("space_max",  0) or 0)
        used  = int(contents.get("space_used", 0) or 0)
        if total > 0:
            free = max(0, total - used)
            log.info("[Seedr] Storage (list_contents): %.0f/%.0f MB (%.0f MB free)",
                     used / 1e6, total / 1e6, free / 1e6)
            return {"total": total, "used": used, "free": free, "unknown": False}
    except Exception as exc:
        log.warning("[Seedr] list_contents storage fallback failed: %s", exc)

    log.warning(
        "[Seedr] Cannot determine storage for %s — "
        "assuming space is available and proceeding.", username[:25]
    )
    return {"total": 0, "used": 0, "free": 9_999_999_999, "unknown": True}


# ══════════════════════════════════════════════════════════════════════════════
# add_torrent — proxied fallback for Colab cloud IPs
# ══════════════════════════════════════════════════════════════════════════════

async def _fresh_token(username: str, password: str, proxy: bool = True) -> str:
    import httpx
    p = _proxy() if proxy else None
    async with httpx.AsyncClient(
        proxy=p, headers={"User-Agent": _UA},
        timeout=30, follow_redirects=True,
    ) as c:
        r = await c.post(_OAUTH_URL, data={
            "grant_type": "password",
            "client_id":  _CLIENT_ID,
            "username":   username,
            "password":   password,
        })
        r.raise_for_status()
        return r.json().get("access_token", "")


def _extract_token(client) -> str:
    for attr in ("token", "_token", "access_token", "_access_token"):
        v = getattr(client, attr, None)
        if isinstance(v, str) and v:
            return v
    for container in ("_auth", "auth", "_session", "session"):
        obj = getattr(client, container, None)
        if obj:
            for attr in ("token", "access_token", "_token"):
                v = getattr(obj, attr, None)
                if isinstance(v, str) and v:
                    return v
    return ""


async def _submit_magnet(username: str, password: str, magnet: str) -> Optional[int]:
    # Attempt 1: seedrcc
    try:
        client = await _get_client(username, password)
        raw    = await client.add_torrent(magnet_link=magnet)
        result = _to_dict(raw)
        log.info("[Seedr] add_torrent response: %s", result)

        rv = result.get("result")
        if rv is False or str(rv).lower() == "false":
            raise SeedrError(
                f"add_torrent rejected: "
                f"{result.get('error') or result.get('message') or result}"
            )

        tid = (
            result.get("user_torrent_id") or
            result.get("torrent_id") or
            result.get("id") or
            (result.get("data") or {}).get("user_torrent_id") or
            (result.get("data") or {}).get("torrent_id") or
            None
        )
        if not tid:
            rv2 = result.get("result")
            if isinstance(rv2, int) and rv2 > 1:
                tid = rv2
        log.info("[Seedr] Submitted via seedrcc: torrent_id=%s", tid)
        return int(tid) if tid else None

    except SeedrError:
        raise
    except Exception as exc:
        log.warning("[Seedr] seedrcc add_torrent failed: %s — trying proxied fallback", exc)

    # Attempt 2: resource.php + proxy
    p = _proxy()
    if not p:
        log.warning(
            "[Seedr] SEEDR_PROXY not set — add_torrent may be blocked on Colab. "
            "Add SEEDR_PROXY=http://host:port to Colab secrets."
        )
    log.info("[Seedr] add_torrent via resource.php (proxy=%s)", "set" if p else "none")

    import httpx
    token = await _fresh_token(username, password, proxy=True)
    if not token:
        raise SeedrAuthError("Could not obtain OAuth token for proxied add_torrent")

    async with httpx.AsyncClient(
        proxy=p,
        headers={"User-Agent": _UA, "Authorization": f"Bearer {token}"},
        timeout=120, follow_redirects=True,
    ) as c:
        r = await c.post(_API_RESOURCE, data={
            "access_token":   token,
            "func":           "add_torrent",
            "torrent_magnet": magnet,
        })
        r.raise_for_status()
        body = r.json()

    rv = body.get("result")
    if rv is False or str(rv).lower() == "false":
        raise SeedrError(
            f"add_torrent blocked: "
            f"{body.get('error') or body.get('message') or body}"
        )

    tid = body.get("torrent_id") or body.get("id")
    log.info("[Seedr] Submitted via resource.php: torrent_id=%s", tid)
    return int(tid) if tid else None


# ══════════════════════════════════════════════════════════════════════════════
# Folder management
# ══════════════════════════════════════════════════════════════════════════════

async def _del_folder(username: str, password: str, folder_id: int) -> None:
    try:
        client = await _get_client(username, password)
        await client.delete_folder(folder_id=str(folder_id))
        log.info("[Seedr] Deleted folder %d", folder_id)
    except Exception as exc:
        log.warning("[Seedr] Delete folder %d (non-fatal): %s", folder_id, exc)


async def _ensure_free(username: str, password: str, needed: int = 0) -> int:
    want = max(needed, _MIN_FREE)
    s = await _storage(username, password)

    if s.get("unknown"):
        log.warning(
            "[Seedr] %s: storage unreadable — skipping quota check, proceeding.",
            username[:25],
        )
        return s["free"]

    log.info(
        "[Seedr] %s: %.0f/%.0f MB (%.0f MB free)",
        username[:25], s["used"] / 1e6, s["total"] / 1e6, s["free"] / 1e6,
    )
    if s["free"] >= want:
        return s["free"]

    log.info("[Seedr] Low space — wiping old folders…")
    root = await _root(username, password)
    for f in root["folders"]:
        log.info("[Seedr] Wiping '%s' (id=%d)", f["name"], f["id"])
        await _del_folder(username, password, f["id"])

    s = await _storage(username, password)
    if not s.get("unknown") and s["free"] < want:
        raise SeedrQuotaError(
            f"{username[:25]}: {s['free']//1024//1024} MB free "
            f"(need >= {want//1024//1024} MB)"
        )
    return s["free"]


# ══════════════════════════════════════════════════════════════════════════════
# Account selector
# ══════════════════════════════════════════════════════════════════════════════

async def _pick_account(needed: int = 0) -> tuple[str, str, int]:
    best_user = best_pwd = None
    best_free = -1
    last_err  = None
    for user, pwd in _accounts():
        try:
            free = await _ensure_free(user, pwd, needed)
            if free > best_free:
                best_free, best_user, best_pwd = free, user, pwd
        except Exception as exc:
            log.warning("[Seedr] Account %s skipped: %s", user[:25], exc)
            last_err = exc
    if best_user is None:
        raise last_err or SeedrQuotaError("All Seedr accounts are full or unreachable.")
    log.info("[Seedr] Selected %s (%.0f MB free)", best_user[:25], best_free / 1e6)
    return best_user, best_pwd, best_free


# ══════════════════════════════════════════════════════════════════════════════
# File URL collector  —  PATCHED v6: rename before URL fetch
# ══════════════════════════════════════════════════════════════════════════════

async def _collect_files(username: str, password: str, folder_id: int) -> list[dict]:
    """
    Walk folder tree and return [{name, url, size, clean_name}].

    v6 CHANGE: For each file, compute _clean_seedr_name().
      If the name differs from the original:
        1. Rename the file on Seedr via resource.php (changes DB + URL path).
        2. Force _file_url_fallback() to get the fresh URL carrying the new name.
      This ensures the URL we hand to CloudConvert import/url has no brackets,
      spaces, or shell-special chars that would break the ffmpeg argument parser.

    Strategy per file:
      1. Rename to clean name (if needed)          ← NEW v6
      2. Use embedded URL only if name was already clean and URL is present
      3. Otherwise call fetch_file / resource.php  ← always after rename
    """
    result: list[dict] = []

    async def _walk(fid: int, depth: int = 0) -> None:
        if depth > 5:
            return
        try:
            contents = await _list_folder(username, password, fid)
        except Exception as exc:
            if depth == 0:
                raise SeedrError(f"Cannot list folder {fid}: {exc}") from exc
            log.warning("[Seedr] Cannot list subfolder %d: %s", fid, exc)
            return

        n_emb = sum(1 for f in contents["files"] if f.get("url"))
        log.info(
            "[Seedr] Folder %d (depth=%d): %d file(s) (%d with embedded URL), "
            "%d subfolder(s)",
            fid, depth,
            len(contents["files"]), n_emb,
            len(contents["folders"]),
        )

        for f in contents["files"]:
            orig_name = f.get("name", "file")
            size      = f.get("size", 0)
            fid2      = f.get("id")
            emb_url   = f.get("url", "")

            # ── v6: compute clean name ──────────────────────────────────────
            clean_name = _clean_seedr_name(orig_name)
            needs_rename = (clean_name != orig_name)

            if needs_rename and fid2:
                renamed = await _rename_file(username, password, fid2, clean_name)
                if renamed:
                    log.info("[Seedr] Renamed '%s' → '%s'", orig_name, clean_name)
                    # Embedded URL carries old filename — must re-fetch
                    emb_url = ""
                else:
                    # Rename failed — use original name, try embedded URL
                    log.warning(
                        "[Seedr] Rename failed for '%s' — using original name. "
                        "CC import/url may fail if name has special chars.",
                        orig_name,
                    )
                    clean_name = orig_name

            # ── Determine URL ───────────────────────────────────────────────
            if emb_url and not needs_rename:
                # Name was already clean AND we have an embedded URL — use it
                url = emb_url
                log.info("[Seedr] ✅ Embedded URL (clean name) — '%s'", clean_name)
            elif fid2:
                # Either renamed (need fresh URL) or no embedded URL
                try:
                    url = await _file_url_fallback(username, password, fid2)
                    log.info("[Seedr] ✅ Fetched URL%s — '%s'",
                             " (post-rename)" if needs_rename else " (fallback)",
                             clean_name)
                except Exception as exc:
                    log.error(
                        "[Seedr] ❌ ALL strategies failed for '%s' (id=%d): %s",
                        clean_name, fid2, exc,
                    )
                    continue
            else:
                log.warning("[Seedr] ⚠ No id for '%s' — skipping", clean_name)
                continue

            result.append({
                "name":       clean_name,
                "url":        url,
                "size":       int(size or 0),
                "clean_name": clean_name,
            })

        for sub in contents["folders"]:
            sid = sub.get("id")
            if sid:
                await _walk(sid, depth + 1)

    await _walk(folder_id)

    # Diagnostic dump if empty
    if not result:
        log.error(
            "[Seedr] _collect_files: 0 files for folder %d. "
            "Dumping raw list_contents response:", folder_id
        )
        try:
            client = await _get_client(username, password)
            raw    = await client.list_contents(folder_id=str(folder_id))
            raw_d  = _to_dict(raw)
            raw_s  = str(raw_d)
            log.error(
                "[Seedr] RAW list_contents(%d): %s%s",
                folder_id, raw_s[:2000],
                " …[truncated]" if len(raw_s) > 2000 else "",
            )
        except Exception as diag_exc:
            log.error("[Seedr] Diagnostic dump failed: %s", diag_exc)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Polling
# ══════════════════════════════════════════════════════════════════════════════

async def _poll(
    username:               str,
    password:               str,
    torrent_id:             Optional[int],
    pre_existing_folder_ids: set,
    timeout_s:              int = 7200,
    progress_cb:            Optional[Callable] = None,
    interval:               float = 10.0,
) -> dict:
    deadline   = time.monotonic() + timeout_s
    last_pct   = -1.0
    last_hb    = time.monotonic()
    ever_seen  = False
    gone_polls = 0

    while time.monotonic() < deadline:
        try:
            root     = await _root(username, password)
            folders  = root["folders"]
            torrents = root["torrents"]
            now      = time.monotonic()

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

                if pct >= 100.0:
                    new_f = [f for f in folders
                             if f["id"] not in pre_existing_folder_ids]
                    if new_f:
                        folder = max(new_f, key=lambda f: f["id"])
                        log.info("[Seedr] ✅ Complete: '%s' (id=%d)",
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
                new_f = [f for f in folders
                         if f["id"] not in pre_existing_folder_ids]
                if new_f:
                    folder = max(new_f, key=lambda f: f["id"])
                    log.info("[Seedr] ✅ Complete: '%s' (id=%d)",
                             folder["name"], folder["id"])
                    if progress_cb:
                        await progress_cb(100.0, folder["name"])
                    return folder

                gone_polls += 1
                if ever_seen:
                    log.info("[Seedr] Torrent gone, waiting for folder (%d)…",
                             gone_polls)
                    if gone_polls >= 3 and folders:
                        folder = max(folders, key=lambda f: f.get("size", 0))
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder
                    if gone_polls >= 15:
                        raise SeedrError(
                            "Torrent vanished without producing a folder."
                        )
                else:
                    if gone_polls >= 3 and torrent_id is None and folders:
                        folder = max(folders, key=lambda f: f.get("size", 0))
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder
                    if (now - last_hb) >= 20.0:
                        last_hb = now
                        if progress_cb:
                            await progress_cb(0.0, "Waiting for Seedr to start…")

        except (SeedrError, SeedrAuthError):
            raise
        except Exception as exc:
            log.warning("[Seedr] Poll error (retrying): %s", exc)

        await asyncio.sleep(interval)

    raise RuntimeError(
        f"Seedr timed out after {timeout_s // 60} min. "
        "Check your Seedr account — the torrent may still be running."
    )


# ══════════════════════════════════════════════════════════════════════════════
# Public pipelines
# ══════════════════════════════════════════════════════════════════════════════

async def download_via_seedr(
    magnet:      str,
    dest:        str,
    progress_cb: Optional[Callable] = None,
    timeout_s:   int = 7200,
) -> list[str]:
    """Submit → poll → rename → collect CDN URLs → download locally → cleanup."""
    from services.downloader  import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")
    user, pwd, _ = await _pick_account()

    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")
    pre_ids = {f["id"] for f in (await _root(user, pwd))["folders"]}

    if progress_cb:
        await progress_cb("submitting", 5.0, "Submitting magnet to Seedr…")
    torrent_id = await _submit_magnet(user, pwd, magnet)
    log.info("[Seedr] Submitted. torrent_id=%s", torrent_id)

    if progress_cb:
        await progress_cb("waiting", 5.0, "Seedr is fetching torrent…")

    async def _pcb(pct: float, name: str) -> None:
        if progress_cb:
            await progress_cb(
                "downloading" if pct > 0.5 else "waiting",
                min(pct, 99.0), name or "Downloading…",
            )

    folder    = await _poll(user, pwd, torrent_id, pre_ids, timeout_s, _pcb)
    folder_id = folder["id"]

    if progress_cb:
        await progress_cb("fetching", 99.0, "Renaming & getting CDN links…")
    files = await _collect_files(user, pwd, folder_id)
    if not files:
        raise SeedrError(
            "Seedr produced no downloadable files. "
            "Check bot logs for the RAW list_contents diagnostic dump."
        )

    os.makedirs(dest, exist_ok=True)
    local_paths: list[str] = []

    for i, f in enumerate(files):
        clean = f.get("clean_name") or f["name"]
        fsize = f.get("size", 0)

        async def _fp(done: int, total: int, speed: float, eta: int,
                      _i=i, _c=clean) -> None:
            if progress_cb:
                await progress_cb(
                    "dl_file", (done / total * 100) if total else 0,
                    f"⬇️ {_c[:35]} ({_i+1}/{len(files)})",
                    done_bytes=done, total_bytes=total,
                    speed=speed, eta=eta,
                )

        if progress_cb:
            await progress_cb(
                "dl_file", 0.0,
                f"⬇️ {clean[:40]} ({i+1}/{len(files)})",
                done_bytes=0, total_bytes=fsize, speed=0.0, eta=0,
            )

        log.info("[Seedr] Downloading %d/%d: %s", i + 1, len(files), clean)
        try:
            path   = await download_direct(f["url"], dest, progress=_fp)
            target = os.path.join(dest, clean)
            if path != target:
                try:
                    os.rename(path, target)
                    path = target
                except OSError:
                    pass
            local_paths.append(path)
        except Exception as exc:
            log.error("[Seedr] Failed to download '%s': %s", clean, exc)

    try:
        await _del_folder(user, pwd, folder_id)
    except Exception:
        pass

    return local_paths


async def fetch_urls_via_seedr(
    magnet:      str,
    progress_cb: Optional[Callable] = None,
    timeout_s:   int = 7200,
) -> tuple:
    """
    Submit → poll → rename → collect CDN URLs WITHOUT local download.
    Returns (files, folder_id, user, pwd).

    v6: Files now have clean names (no brackets/spaces) because _collect_files
    renames them on Seedr before fetching URLs. The URLs returned carry the
    clean names so they can be passed directly to CC import/url.

    Caller must call _del_folder() after CC delivery.
    """
    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")
    user, pwd, _ = await _pick_account()

    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")
    pre_ids = {f["id"] for f in (await _root(user, pwd))["folders"]}

    if progress_cb:
        await progress_cb("submitting", 5.0, "Submitting magnet to Seedr…")
    torrent_id = await _submit_magnet(user, pwd, magnet)
    log.info("[Seedr] Submitted. torrent_id=%s", torrent_id)

    if progress_cb:
        await progress_cb("waiting", 5.0, "Seedr is fetching torrent…")

    async def _pcb(pct: float, name: str) -> None:
        if progress_cb:
            await progress_cb(
                "downloading" if pct > 0.5 else "waiting",
                min(pct, 99.0), name or "Downloading…",
            )

    folder    = await _poll(user, pwd, torrent_id, pre_ids, timeout_s, _pcb)
    folder_id = folder["id"]

    if progress_cb:
        await progress_cb("fetching", 99.0, "Renaming & getting CDN links…")
    files = await _collect_files(user, pwd, folder_id)
    if not files:
        raise SeedrError(
            "Seedr produced no downloadable files. "
            "Check bot logs for the RAW list_contents diagnostic dump."
        )

    # clean_name is already set by _collect_files (post-rename)
    for f in files:
        f.setdefault("clean_name", f["name"])

    return files, folder_id, user, pwd


# ══════════════════════════════════════════════════════════════════════════════
# Convenience / backward-compat wrappers
# ══════════════════════════════════════════════════════════════════════════════

async def check_credentials() -> bool:
    try:
        await _pick_account()
        return True
    except Exception as exc:
        log.warning("[Seedr] Credential check failed: %s", exc)
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
    return await _root(user, pwd) if folder_id == 0 \
           else await _list_folder(user, pwd, folder_id)


async def delete_folder(folder_id: int) -> None:
    user, pwd = _accounts()[0]
    await _del_folder(user, pwd, folder_id)


async def delete_torrent(torrent_id: int) -> None:
    log.info("[Seedr] delete_torrent(%d) — folder cleanup handles this", torrent_id)
