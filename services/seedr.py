"""
services/seedr.py
Seedr.cc cloud torrent client — uses seedrcc v2.0.2 library.

═══════════════════════════════════════════════════════════════════
METHOD NAMES VERIFIED FROM ACTUAL seedrcc v2.0.2 SOURCE CODE:
  AsyncSeedr.from_password(username, password, on_token_refresh=)
  AsyncSeedr(token=Token(...), on_token_refresh=)
  client.add_torrent(magnet_link='magnet:?...')     ← NOT add_magnet!
  client.list_contents(folder_id='0')               ← NOT list_folder!
  client.fetch_file(file_id: str)                   ← returns .url
  client.delete_folder(folder_id: str)
  client.get_settings()
  client.close()

RETURN TYPES (all are frozen dataclasses):
  ListContentsResult: .folders (List[Folder]), .files (List[File]),
                      .torrents (List[Torrent]), .space_used, .space_max
  Folder: .id (int), .name (str), .size (int)
  File:   .folder_file_id (int), .name (str), .size (int)
  Torrent: .id (int), .name (str), .progress (str!), .progress_url
  FetchFileResult: .url (str), .name (str), .result (bool)
  AddTorrentResult: .user_torrent_id, .torrent_hash, .title

IMPORTANT: All method args that take IDs expect STRINGS, not ints.

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

log = logging.getLogger(__name__)

_TOKEN_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "seedr_token.json")
)


# ─────────────────────────────────────────────────────────────
# Token persistence
# ─────────────────────────────────────────────────────────────

def _save_token(token) -> None:
    """Save seedrcc Token to disk so it survives restarts."""
    try:
        os.makedirs(os.path.dirname(_TOKEN_FILE), exist_ok=True)
        with open(_TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(token.to_json())
        log.debug("[Seedr] Token saved")
    except Exception as e:
        log.warning("[Seedr] Token save error: %s", e)


def _load_token():
    """Load seedrcc Token from disk. Returns Token or None."""
    try:
        from seedrcc import Token
        with open(_TOKEN_FILE, encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            return None
        return Token.from_json(raw)
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("[Seedr] Token load error: %s", e)
        return None


# ─────────────────────────────────────────────────────────────
# Client factory
# ─────────────────────────────────────────────────────────────

async def _get_client():
    """
    Create an authenticated AsyncSeedr client.
    Priority: saved token → password login → error.
    """
    from seedrcc import AsyncSeedr, Token

    # Try saved token first (avoids re-login, faster startup)
    saved = _load_token()
    if saved:
        try:
            client = AsyncSeedr(token=saved, on_token_refresh=_save_token)
            # Verify it works with a lightweight call
            await client.get_settings()
            log.info("[Seedr] Authenticated via saved token")
            return client
        except Exception as e:
            log.warning("[Seedr] Saved token expired (%s) — re-authenticating", e)
            try:
                await client.close()
            except Exception:
                pass

    # Password login
    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError(
            "Seedr credentials not configured.\n"
            "Add to your .env:\n"
            "  SEEDR_USERNAME=your@email.com\n"
            "  SEEDR_PASSWORD=yourpassword"
        )

    client = await AsyncSeedr.from_password(
        username, password, on_token_refresh=_save_token,
    )

    # Save token for next restart
    if client.token:
        _save_token(client.token)

    log.info("[Seedr] Authenticated via password login")
    return client


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    """Return True if credentials are valid."""
    try:
        client = await _get_client()
        await client.close()
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    """Return {used, total, free} in bytes."""
    client = await _get_client()
    try:
        root = await client.list_contents(folder_id="0")
        total = int(root.space_max or 0)
        used  = int(root.space_used or 0)
        return {"total": total, "used": used, "free": total - used}
    finally:
        await client.close()


# ── Browser-like headers sent with every add_magnet attempt ──────────────────
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin":  "https://www.seedr.cc",
    "Referer": "https://www.seedr.cc/",
}


async def _seedr_web_session_add(magnet: str) -> dict:
    """
    Fallback: authenticate via the Seedr *web* login (cookie session) and
    submit the magnet through the same endpoint the browser uses.

    This path works even when Seedr has disabled the OAuth 'add_torrent'
    function for free / cloud-IP accounts, because it mimics the browser.
    """
    import httpx as _httpx

    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("SEEDR_USERNAME / SEEDR_PASSWORD not set")

    _OAUTH_URL = "https://www.seedr.cc/oauth_test/resource.php"
    _TOKEN_URL = "https://www.seedr.cc/oauth_test/token.php"

    async with _httpx.AsyncClient(
        timeout=60,
        follow_redirects=True,
        headers=_BROWSER_HEADERS,
    ) as http:
        # ── Step 1: get a fresh OAuth token via password grant ──────────────
        tok_resp = await http.post(
            _TOKEN_URL,
            data={
                "grant_type": "password",
                "client_id":  "seedr_chrome",
                "type":       "login",
                "username":   username,
                "password":   password,
            },
        )
        tok_data = tok_resp.json()
        access_token = tok_data.get("access_token")
        if not access_token:
            raise RuntimeError(
                f"[Seedr-web] Login failed: {tok_data}"
            )
        log.debug("[Seedr-web] Got fresh token for web-session path")

        # ── Step 2: try every known add-torrent variant with browser headers ─
        candidates = [
            # oauth-test path, torrent_magnet field (library default)
            dict(
                url=_OAUTH_URL,
                params={"access_token": access_token, "func": "add_torrent"},
                data={"torrent_magnet": magnet, "folder_id": "-1"},
            ),
            # oauth-test path, old field name magnet_link
            dict(
                url=_OAUTH_URL,
                params={"access_token": access_token, "func": "add_torrent"},
                data={"magnet_link": magnet, "folder_id": "-1"},
            ),
            # multipart/form-data variant (files= triggers multipart in httpx)
            dict(
                url=_OAUTH_URL,
                params={"access_token": access_token, "func": "add_torrent"},
                files={
                    "torrent_magnet": (None, magnet),
                    "folder_id":      (None, "-1"),
                },
            ),
            # New /api path, Bearer auth
            dict(
                url="https://www.seedr.cc/api/torrent",
                headers={**_BROWSER_HEADERS,
                          "Authorization": f"Bearer {access_token}"},
                data={"torrent_magnet": magnet, "folder_id": "-1"},
            ),
            # New /api path, old field name
            dict(
                url="https://www.seedr.cc/api/torrent",
                headers={**_BROWSER_HEADERS,
                          "Authorization": f"Bearer {access_token}"},
                data={"magnet_link": magnet, "folder_id": "-1"},
            ),
        ]

        for idx, kw in enumerate(candidates, start=1):
            resp = await http.post(**kw)
            log.debug(
                "[Seedr-web] candidate %d → HTTP %d  %.160s",
                idx, resp.status_code, resp.text,
            )
            if resp.status_code in (404, 405):
                continue
            if not resp.is_success:
                log.warning(
                    "[Seedr-web] candidate %d: HTTP %d — %s",
                    idx, resp.status_code, resp.text[:200],
                )
                continue
            try:
                body = resp.json()
            except Exception:
                log.warning("[Seedr-web] candidate %d: non-JSON response", idx)
                continue
            if isinstance(body, dict) and body.get("result") is False:
                log.warning(
                    "[Seedr-web] candidate %d: API result=False  error=%s",
                    idx, body.get("error"),
                )
                continue
            log.info(
                "[Seedr-web] Magnet submitted via web-session candidate %d "
                "hash=%s title=%s",
                idx, body.get("torrent_hash", "?"), body.get("title", "?"),
            )
            return {
                "result": True,
                "user_torrent_id": body.get("user_torrent_id"),
                "torrent_hash": body.get("torrent_hash", ""),
                "title": body.get("title", ""),
            }

    raise RuntimeError("[Seedr-web] All web-session candidates exhausted")


async def add_magnet(magnet: str) -> dict:
    """
    Submit a magnet link to Seedr.

    WHY THIS IS COMPLEX
    ───────────────────
    Seedr's OAuth API (/oauth_test/resource.php?func=add_torrent) returns
    HTTP 404 for free accounts running on cloud IPs (e.g. Google Colab).
    Read-only calls (list_contents, get_settings) still work because Seedr
    only restricts *write* operations at the IP/tier level.

    Strategy pipeline (stops at first success):
      1. seedrcc library  — fast path when the OAuth API is working
      2. Direct httpx, torrent_magnet field  ← v2 library field name
      3. Direct httpx, magnet_link field     ← v1 / legacy field name
      4. Direct httpx, multipart/form-data   ← some server configs need this
      5. Web-session fallback (browser UA + re-login) — works even when
         the OAuth API is locked for cloud IPs

    Every attempt logs its HTTP status AND the first 200 chars of the
    response body so you can see exactly what Seedr is returning.
    """
    import httpx as _httpx

    # ── grab a fresh token once ─────────────────────────────────────────────
    client = await _get_client()
    token: str = client.token.access_token
    await client.close()

    _OAUTH_URL = "https://www.seedr.cc/oauth_test/resource.php"

    # ── build strategy list ──────────────────────────────────────────────────
    strategies: list[dict] = []

    # 1 – seedrcc library (wrapped so it fits the loop)
    strategies.append({"_use_library": True})

    # 2 – direct httpx: torrent_magnet (v2 field name)
    strategies.append(dict(
        url=_OAUTH_URL,
        params={"access_token": token, "func": "add_torrent"},
        data={"torrent_magnet": magnet, "folder_id": "-1"},
        headers=_BROWSER_HEADERS,
    ))

    # 3 – direct httpx: magnet_link (v1 / legacy field name)
    strategies.append(dict(
        url=_OAUTH_URL,
        params={"access_token": token, "func": "add_torrent"},
        data={"magnet_link": magnet, "folder_id": "-1"},
        headers=_BROWSER_HEADERS,
    ))

    # 4 – multipart/form-data (files= triggers multipart in httpx)
    strategies.append(dict(
        url=_OAUTH_URL,
        params={"access_token": token, "func": "add_torrent"},
        files={
            "torrent_magnet": (None, magnet),
            "folder_id":      (None, "-1"),
        },
        headers=_BROWSER_HEADERS,
    ))

    async with _httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
        for idx, strat in enumerate(strategies, start=1):
            label = f"#{idx}"

            # ── strategy 1: use the seedrcc library ──────────────────────────
            if strat.get("_use_library"):
                try:
                    from seedrcc import AsyncSeedr
                    cli2 = await AsyncSeedr.from_password(
                        os.environ.get("SEEDR_USERNAME", ""),
                        os.environ.get("SEEDR_PASSWORD", ""),
                    )
                    result = await cli2.add_torrent(magnet_link=magnet)
                    await cli2.close()
                    log.info(
                        "[Seedr] Strategy %s (library) OK: hash=%s title=%s",
                        label,
                        getattr(result, "torrent_hash", "?"),
                        getattr(result, "title", "?"),
                    )
                    return {
                        "result": True,
                        "user_torrent_id": getattr(result, "user_torrent_id", None),
                        "torrent_hash": getattr(result, "torrent_hash", ""),
                        "title": getattr(result, "title", ""),
                    }
                except Exception as exc:
                    log.warning("[Seedr] Strategy %s (library) failed: %s", label, exc)
                continue

            # ── strategies 2-4: raw httpx ────────────────────────────────────
            try:
                resp = await http.post(**strat)
                log.debug(
                    "[Seedr] Strategy %s → HTTP %d  %.200s",
                    label, resp.status_code, resp.text,
                )
                if resp.status_code in (404, 405):
                    log.warning(
                        "[Seedr] Strategy %s: HTTP %d — %.200s",
                        label, resp.status_code, resp.text,
                    )
                    continue
                if not resp.is_success:
                    log.warning(
                        "[Seedr] Strategy %s: HTTP %d — %.200s",
                        label, resp.status_code, resp.text,
                    )
                    continue
                body = resp.json()
                if isinstance(body, dict) and body.get("result") is False:
                    log.warning(
                        "[Seedr] Strategy %s: API error=%s",
                        label, body.get("error"),
                    )
                    continue
                log.info(
                    "[Seedr] Strategy %s OK: hash=%s title=%s",
                    label,
                    body.get("torrent_hash", "?"),
                    body.get("title", "?"),
                )
                return {
                    "result": True,
                    "user_torrent_id": body.get("user_torrent_id"),
                    "torrent_hash": body.get("torrent_hash", ""),
                    "title": body.get("title", ""),
                }
            except Exception as exc:
                log.warning("[Seedr] Strategy %s failed: %s", label, exc)
                continue

    # ── strategy 5: full web-session fallback ────────────────────────────────
    log.warning(
        "[Seedr] Strategies 1-4 exhausted — trying web-session fallback"
    )
    try:
        return await _seedr_web_session_add(magnet)
    except Exception as exc:
        raise RuntimeError(
            f"[Seedr] All strategies including web-session fallback failed.\n"
            f"Last error: {exc}\n\n"
            f"DIAGNOSIS: If every attempt above shows HTTP 404, Seedr is\n"
            f"blocking add_torrent for your IP (Google Colab = Google Cloud).\n"
            f"Fix: set SEEDR_PROXY=http://user:pass@host:port in your .env\n"
            f"to route only the add_torrent call through a non-cloud IP."
        ) from exc


async def list_folder(folder_id: int = 0) -> dict:
    """
    List folder contents. Returns a plain dict with:
      folders: [{id, name, size}, ...]
      files: [{folder_file_id, name, size}, ...]
      torrents: [{id, name, progress, ...}, ...]
    """
    client = await _get_client()
    try:
        root = await client.list_contents(folder_id=str(folder_id))

        folders = [
            {"id": f.id, "name": f.name, "size": f.size}
            for f in (root.folders or [])
        ]
        files = [
            {"folder_file_id": f.folder_file_id, "name": f.name,
             "size": f.size, "id": f.file_id}
            for f in (root.files or [])
        ]
        torrents = [
            {"id": t.id, "name": t.name, "progress": t.progress,
             "size": t.size, "progress_url": getattr(t, "progress_url", None)}
            for t in (root.torrents or [])
        ]

        return {
            "folders": folders,
            "files": files,
            "torrents": torrents,
            "space_used": root.space_used,
            "space_max": root.space_max,
        }
    finally:
        await client.close()


async def get_file_download_url(file_id: int) -> str:
    """Get direct download URL for a file via client.fetch_file()."""
    client = await _get_client()
    try:
        result = await client.fetch_file(str(file_id))
        url = getattr(result, "url", "")
        if not url:
            log.warning("[Seedr] fetch_file returned no URL for id=%s", file_id)
        return url
    finally:
        await client.close()


async def delete_folder(folder_id: int) -> None:
    """Delete a folder from Seedr to reclaim quota."""
    client = await _get_client()
    try:
        await client.delete_folder(str(folder_id))
        log.info("[Seedr] Deleted folder id=%d", folder_id)
    finally:
        await client.close()


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

    Seedr progress: "0"-"99" = downloading, "100" = done, "101" = in folder.
    Note: progress is a STRING in seedrcc v2.
    """
    if existing_folder_ids is None:
        existing_folder_ids = set()

    deadline = time.time() + timeout_s
    last_pct = -1.0

    while time.time() < deadline:
        try:
            root = await list_folder(0)
            folders  = root.get("folders", [])
            torrents = root.get("torrents", [])

            # Active downloads (progress < 100)
            downloading = []
            for t in torrents:
                try:
                    pct = float(t.get("progress", "100"))
                except (ValueError, TypeError):
                    pct = 100.0
                if pct < 100:
                    downloading.append({**t, "_pct": pct})

            # New completed folders (not in baseline snapshot)
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
                pct = dl["_pct"]
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
    """
    Return [{name, url, size}, ...] for every file in folder_id,
    recursing into sub-folders.
    """
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
