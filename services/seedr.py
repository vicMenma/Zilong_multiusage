"""
services/seedr.py  —  v4  (embedded-URL fix)

ROOT CAUSE of "Seedr produced no downloadable files"
──────────────────────────────────────────────────────
Seedr's new API embeds the CDN download URL directly inside the folder
listing response (inside each file object).  v2/v3 of this file:
  1. called _normalise_contents() which STRIPPED the 'url' field
  2. fell through to fetch_file() which is broken on the new API
  3. swallowed all fetch_file exceptions with a WARNING
  → result: empty file list every time

FIX (v4)
─────────
  • _normalise_contents() now PRESERVES url / download_url from file dicts
  • _collect_files() uses the embedded URL directly when present
  • fetch_file() is kept as fallback for older-API file IDs without a URL
  • Full exception is logged (not silently dropped) so failures are visible
  • Diagnostic dump added when result is still empty after full walk

PUBLIC API UNCHANGED:
  download_via_seedr / fetch_urls_via_seedr / _del_folder / _collect_files
"""
from __future__ import annotations

import asyncio
import logging
import os
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
# Response normalisation  ← CRITICAL FIX IS HERE
# ══════════════════════════════════════════════════════════════════════════════

def _parse_progress(raw) -> float:
    if raw is None:
        return 0.0
    try:
        return float(str(raw).replace("%", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _file_id_from(f: dict) -> Optional[int]:
    for key in ("folder_file_id", "id", "file_id"):
        v = f.get(key)
        if v is not None:
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
    return None


def _normalise_contents(data: dict) -> dict:
    """
    Normalise a seedrcc list_contents() response.

    CRITICAL FIX (v4): each file's 'url' / 'download_url' field is now
    PRESERVED in the output dict.  Seedr's new API embeds the CDN download
    URL directly in the listing — previous versions stripped it, causing
    every download attempt to fail silently.

    Also handles response shapes where content is nested under 'result' or
    'folder' keys (different seedrcc versions / API revisions).
    """
    top_keys = list(data.keys())
    log.debug("[Seedr] list_contents top-level keys: %s", top_keys)

    # Unwrap nested containers
    actual = data
    for key in ("result", "folder"):
        candidate = data.get(key)
        if isinstance(candidate, dict) and (
            "files" in candidate or "folders" in candidate
        ):
            actual = candidate
            log.debug("[Seedr] Unwrapping under '%s'", key)
            break

    # Handle 'children' mixed list
    if "children" in actual and isinstance(actual["children"], list):
        children = actual["children"]
        extra_f  = [c for c in children if "folder" in str(c.get("type","")).lower()]
        extra_fi = [c for c in children if "file"   in str(c.get("type","")).lower()]
        actual   = {**actual,
                    "folders": list(actual.get("folders", [])) + extra_f,
                    "files":   list(actual.get("files",   [])) + extra_fi}

    folders_raw  = actual.get("folders",  []) or []
    files_raw    = actual.get("files",    []) or []
    torrents_raw = actual.get("torrents", []) or []

    folders: list[dict] = []
    for f in folders_raw:
        fid = f.get("id")
        if fid is None:
            continue
        try:
            folders.append({
                "id":   int(fid),
                "name": f.get("name", ""),
                "size": int(f.get("size", 0) or 0),
            })
        except (ValueError, TypeError):
            pass

    files: list[dict] = []
    for f in files_raw:
        fid = _file_id_from(f)
        if fid is None:
            continue
        # CRITICAL: preserve embedded CDN URL — new Seedr API puts it here
        embedded_url = (
            f.get("url") or
            f.get("download_url") or
            f.get("stream_url") or
            f.get("link") or
            f.get("href") or
            ""
        )
        files.append({
            "id":   fid,
            "name": f.get("name", "file"),
            "size": int(f.get("size", 0) or 0),
            "url":  embedded_url,   # ← was stripped in v2/v3
        })

    torrents: list[dict] = []
    for t in torrents_raw:
        tid = t.get("id")
        try:
            tid = int(tid) if tid is not None else None
        except (ValueError, TypeError):
            tid = None
        torrents.append({
            "id":       tid,
            "name":     t.get("name", ""),
            "progress": _parse_progress(t.get("progress")),
            "size":     int(t.get("size", 0) or 0),
        })

    n_with_url = sum(1 for f in files if f.get("url"))
    log.debug("[Seedr] Normalised: %d folders, %d files (%d with URL), %d torrents",
              len(folders), len(files), n_with_url, len(torrents))

    return {
        "folders":    folders,
        "files":      files,
        "torrents":   torrents,
        "space_used": actual.get("space_used"),
        "space_max":  actual.get("space_max"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Core operations
# ══════════════════════════════════════════════════════════════════════════════

async def _root(username: str, password: str) -> dict:
    client = await _get_client(username, password)
    try:
        return _normalise_contents(await client.list_contents(folder_id='0'))
    except Exception as exc:
        _invalidate(username)
        raise SeedrError(f"list_contents(root) failed: {exc}") from exc


async def _list_folder(username: str, password: str, folder_id: int) -> dict:
    client = await _get_client(username, password)
    try:
        raw = await client.list_contents(folder_id=str(folder_id))
        return _normalise_contents(raw)
    except Exception as exc:
        _invalidate(username)
        raise SeedrError(f"list_contents({folder_id}) failed: {exc}") from exc


async def _file_url_fallback(username: str, password: str, file_id: int) -> str:
    """
    Old-API fallback: get CDN URL via fetch_file or resource.php.
    Only called when the new-API embedded URL is absent in the listing.
    """
    client = await _get_client(username, password)

    # Try seedrcc fetch_file
    try:
        data = await client.fetch_file(file_id=str(file_id))
        url  = (
            data.get("url") or
            data.get("download_url") or
            (data.get("result") or {}).get("url") or
            (data.get("data")   or {}).get("url") or
            ""
        )
        if url:
            return url
        log.warning("[Seedr] fetch_file(%d) no URL — keys: %s", file_id, list(data.keys()))
    except Exception as exc:
        log.warning("[Seedr] fetch_file(%d): %s", file_id, exc)
        _invalidate(username)

    # Fallback: resource.php
    import httpx
    client2 = await _get_client(username, password)
    token   = _extract_token(client2) or await _fresh_token(username, password, proxy=False)
    if not token:
        raise SeedrError("No token for resource.php fallback")

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

    url = (
        body.get("url") or
        body.get("download_url") or
        (body.get("result") or {}).get("url") or
        ""
    )
    if url:
        return url
    raise SeedrError(f"resource.php fetch_file: no URL in response: {body}")


async def _storage(username: str, password: str) -> dict:
    client = await _get_client(username, password)
    try:
        data  = await client.get_settings()
        total = int(data.get("space_max",  data.get("storage_total", 0)) or 0)
        used  = int(data.get("space_used", data.get("storage_used",  0)) or 0)
        return {"total": total, "used": used, "free": max(0, total - used)}
    except Exception:
        try:
            data  = await client.list_contents(folder_id='0')
            total = int(data.get("space_max",  0) or 0)
            used  = int(data.get("space_used", 0) or 0)
            return {"total": total, "used": used, "free": max(0, total - used)}
        except Exception as exc:
            log.warning("[Seedr] storage check failed: %s", exc)
            return {"total": 0, "used": 0, "free": 9_999_999_999}


# ══════════════════════════════════════════════════════════════════════════════
# add_torrent — proxied fallback for Colab cloud IPs
# ══════════════════════════════════════════════════════════════════════════════

async def _fresh_token(username: str, password: str, proxy: bool = True) -> str:
    import httpx
    p = _proxy() if proxy else None
    async with httpx.AsyncClient(
        proxy=p, headers={"User-Agent": _UA}, timeout=30, follow_redirects=True,
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
    # Attempt 1: seedrcc (VPS / residential)
    try:
        client = await _get_client(username, password)
        result = await client.add_torrent(magnet_link=magnet)
        log.info("[Seedr] add_torrent response: %s", result)

        rv = result.get("result")
        if rv is False or str(rv).lower() == "false":
            raise SeedrError(
                f"add_torrent rejected: "
                f"{result.get('error') or result.get('message') or result}"
            )

        tid = (
            result.get("torrent_id") or
            result.get("id") or
            (result.get("data") or {}).get("torrent_id") or
            (isinstance(rv, int) and rv > 0 and rv) or
            None
        )
        log.info("[Seedr] Submitted via seedrcc: torrent_id=%s", tid)
        return int(tid) if tid else None

    except SeedrError:
        raise
    except Exception as exc:
        log.warning("[Seedr] seedrcc add_torrent failed: %s — trying proxied fallback", exc)

    # Attempt 2: resource.php + SEEDR_PROXY (Colab cloud IPs)
    p = _proxy()
    if not p:
        log.warning("[Seedr] SEEDR_PROXY not set — add_torrent may be blocked on Colab.")
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
            f"add_torrent blocked: {body.get('error') or body.get('message') or body}"
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
    log.info("[Seedr] %s: %.0f/%.0f MB (%.0f MB free)",
             username[:25], s["used"] / 1e6, s["total"] / 1e6, s["free"] / 1e6)
    if s["free"] >= want:
        return s["free"]
    log.info("[Seedr] Low space — wiping old folders…")
    root = await _root(username, password)
    for f in root["folders"]:
        log.info("[Seedr] Wiping '%s' (id=%d)", f["name"], f["id"])
        await _del_folder(username, password, f["id"])
    s = await _storage(username, password)
    if s["free"] < want:
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
# File URL collector  ← main fix is here
# ══════════════════════════════════════════════════════════════════════════════

async def _collect_files(username: str, password: str, folder_id: int) -> list[dict]:
    """
    Walk folder tree and return [{name, url, size}] for all files.

    v4 strategy per file:
      1. Use URL embedded in listing  (new Seedr API — no extra request needed)
      2. Fall back to fetch_file / resource.php  (old API, if no embedded URL)
      3. Log the FULL exception if both fail — no silent swallowing
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
            "[Seedr] Folder %d (depth=%d): %d file(s) (%d with embedded URL), %d subfolder(s)",
            fid, depth,
            len(contents["files"]), n_emb,
            len(contents["folders"]),
        )

        for f in contents["files"]:
            name = f.get("name", "file")
            size = f.get("size", 0)

            # ── 1. Embedded URL (new Seedr API) ───────────────────────────────
            url = f.get("url", "")
            if url:
                result.append({"name": name, "url": url, "size": size})
                log.info("[Seedr] ✅ Embedded URL — '%s'", name)
                continue

            # ── 2. fetch_file / resource.php fallback (old API) ───────────────
            fid2 = f.get("id")
            if not fid2:
                log.warning("[Seedr] ⚠ No id and no embedded URL for '%s' — skipping", name)
                continue
            try:
                url = await _file_url_fallback(username, password, fid2)
                result.append({"name": name, "url": url, "size": size})
                log.info("[Seedr] ✅ Fetched URL (fallback) — '%s'", name)
            except Exception as exc:
                # Full exception — not silently dropped
                log.error(
                    "[Seedr] ❌ ALL URL strategies failed for '%s' (id=%d): %s",
                    name, fid2, exc,
                )

        for sub in contents["folders"]:
            sid = sub.get("id")
            if sid:
                await _walk(sid, depth + 1)

    await _walk(folder_id)

    # Diagnostic dump when still empty
    if not result:
        log.error(
            "[Seedr] _collect_files: 0 files collected for folder %d. "
            "Dumping raw API response for diagnosis:", folder_id
        )
        try:
            client = await _get_client(username, password)
            raw    = await client.list_contents(folder_id=str(folder_id))
            raw_s  = str(raw)
            log.error("[Seedr] RAW list_contents(%d): %s%s",
                      folder_id, raw_s[:2000],
                      " …[truncated]" if len(raw_s) > 2000 else "")
        except Exception as diag_exc:
            log.error("[Seedr] Diagnostic dump failed: %s", diag_exc)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Polling
# ══════════════════════════════════════════════════════════════════════════════

async def _poll(
    username: str,
    password: str,
    torrent_id: Optional[int],
    pre_existing_folder_ids: set,
    timeout_s: int = 7200,
    progress_cb: Optional[Callable] = None,
    interval: float = 10.0,
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
                    new_f = [f for f in folders if f["id"] not in pre_existing_folder_ids]
                    if new_f:
                        folder = max(new_f, key=lambda f: f["id"])
                        log.info("[Seedr] ✅ Complete: '%s' (id=%d)", folder["name"], folder["id"])
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
                new_f = [f for f in folders if f["id"] not in pre_existing_folder_ids]
                if new_f:
                    folder = max(new_f, key=lambda f: f["id"])
                    log.info("[Seedr] ✅ Complete: '%s' (id=%d)", folder["name"], folder["id"])
                    if progress_cb:
                        await progress_cb(100.0, folder["name"])
                    return folder

                gone_polls += 1
                if ever_seen:
                    log.info("[Seedr] Torrent gone, waiting for folder (%d)…", gone_polls)
                    if gone_polls >= 3 and folders:
                        folder = max(folders, key=lambda f: f.get("size", 0))
                        if progress_cb:
                            await progress_cb(100.0, folder["name"])
                        return folder
                    if gone_polls >= 15:
                        raise SeedrError("Torrent vanished without producing a folder.")
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
# Public pipeline
# ══════════════════════════════════════════════════════════════════════════════

async def download_via_seedr(
    magnet:      str,
    dest:        str,
    progress_cb: Optional[Callable] = None,
    timeout_s:   int = 7200,
) -> list[str]:
    from services.downloader  import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")
    user, pwd, _ = await _pick_account()

    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")
    root_before = await _root(user, pwd)
    pre_ids     = {f["id"] for f in root_before["folders"]}

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
                min(pct, 99.0), name or "Downloading…"
            )

    folder    = await _poll(user, pwd, torrent_id, pre_ids, timeout_s, _pcb)
    folder_id = folder["id"]

    if progress_cb:
        await progress_cb("fetching", 99.0, "Getting CDN links…")
    files = await _collect_files(user, pwd, folder_id)
    if not files:
        raise SeedrError(
            "Seedr produced no downloadable files. "
            "Check bot logs for RAW list_contents diagnostic dump."
        )

    os.makedirs(dest, exist_ok=True)
    local_paths: list[str] = []

    for i, f in enumerate(files):
        clean = sanitize_filename(f["name"])
        fsize = f.get("size", 0)

        async def _fp(done: int, total: int, speed: float, eta: int,
                      _i=i, _c=clean) -> None:
            if progress_cb:
                await progress_cb(
                    "dl_file", (done / total * 100) if total else 0,
                    f"⬇️ {_c[:35]} ({_i+1}/{len(files)})",
                    done_bytes=done, total_bytes=total, speed=speed, eta=eta,
                )

        if progress_cb:
            await progress_cb("dl_file", 0.0, f"⬇️ {clean[:40]} ({i+1}/{len(files)})",
                              done_bytes=0, total_bytes=fsize, speed=0.0, eta=0)

        log.info("[Seedr] Downloading %d/%d: %s", i + 1, len(files), f["name"])
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
            log.error("[Seedr] Failed to download '%s': %s", f["name"], exc)

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
    Submit → poll → collect CDN URLs WITHOUT local download.
    Returns (files, folder_id, user, pwd).
    Caller must call _del_folder() after CC delivery.
    """
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("selecting", 0.0, "Selecting Seedr account…")
    user, pwd, _ = await _pick_account()

    if progress_cb:
        await progress_cb("submitting", 3.0, "Snapshotting account state…")
    root_before = await _root(user, pwd)
    pre_ids     = {f["id"] for f in root_before["folders"]}

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
                min(pct, 99.0), name or "Downloading…"
            )

    folder    = await _poll(user, pwd, torrent_id, pre_ids, timeout_s, _pcb)
    folder_id = folder["id"]

    if progress_cb:
        await progress_cb("fetching", 99.0, "Getting CDN links…")
    files = await _collect_files(user, pwd, folder_id)
    if not files:
        raise SeedrError(
            "Seedr produced no downloadable files. "
            "Check bot logs for RAW list_contents diagnostic dump."
        )

    for f in files:
        f["clean_name"] = sanitize_filename(f["name"])

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
    return await _root(user, pwd) if folder_id == 0 else await _list_folder(user, pwd, folder_id)


async def delete_folder(folder_id: int) -> None:
    user, pwd = _accounts()[0]
    await _del_folder(user, pwd, folder_id)


async def delete_torrent(torrent_id: int) -> None:
    log.info("[Seedr] delete_torrent(%d) — folder cleanup handles this", torrent_id)
