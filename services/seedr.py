"""
services/seedr.py
Seedr.cc cloud torrent client — FULL REWRITE.

═══════════════════════════════════════════════════════════════════
WHY THE REWRITE:
  The old code used https://www.seedr.cc/oauth_test/token which is DEAD
  (returns 404). Seedr killed that endpoint. The entire auth + API layer
  was broken — no magnet could ever be added, polled, or downloaded.

TWO AUTH STRATEGIES (auto-selected):
  1. REST API v1 — HTTP Basic Auth (email:password)
     Works for premium users. Simple, reliable, no tokens.
     Endpoints: /rest/folder, /rest/transfer/magnet, /rest/file/{id}

  2. seedrcc library — OAuth2 device-code or password grant
     Works for ALL users (free + premium). Uses the same internal
     endpoints as the Kodi/Chrome extensions (reverse-engineered).
     Requires `seedrcc` in requirements.txt.

  Strategy selection:
     - If seedrcc is installed → use it (covers free users)
     - Else → fall back to REST API v1 Basic Auth (premium only)

SETUP (.env):
    SEEDR_USERNAME=your@email.com
    SEEDR_PASSWORD=yourpassword

    # Optional: pre-saved token from seedrcc (JSON string)
    # Avoids login on every bot restart. Bot auto-saves after first login.
    # SEEDR_TOKEN=  (auto-managed in data/seedr_token.json)

BUGS FIXED vs old code:
  1. Dead oauth_test endpoint → replaced with REST v1 + seedrcc
  2. Token management was broken → seedrcc handles refresh automatically
  3. add_magnet used wrong endpoint/format → POST /rest/transfer/magnet
  4. list_root used func= params → GET /rest/folder
  5. get_file_urls only read top-level → now recurses sub-folders
  6. poll_until_ready never passed torrent_name_hint
  7. download_via_seedr snapshot race → existing_folder_ids baseline
  8. File download used fetch_file func → GET /rest/file/{id} (redirect)
  9. No progress on Seedr server-side download → now polls transfer status
  10. delete_folder used func=remove → POST /rest/folder/{id}/delete
═══════════════════════════════════════════════════════════════════
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

_REST_BASE = "https://www.seedr.cc/rest"

_TOKEN_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "seedr_token.json")
)

# ─────────────────────────────────────────────────────────────
# Strategy detection
# ─────────────────────────────────────────────────────────────

_HAS_SEEDRCC = False
try:
    from seedrcc import AsyncSeedr, Token as SeedrToken
    _HAS_SEEDRCC = True
    log.info("[Seedr] seedrcc library available — using OAuth2 (free+premium)")
except ImportError:
    log.info("[Seedr] seedrcc not installed — using REST API v1 Basic Auth (premium only)")


# ─────────────────────────────────────────────────────────────
# REST API v1 — Basic Auth helpers (premium fallback)
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


async def _rest_get(path: str, **kwargs) -> dict | bytes:
    """GET /rest/{path} with Basic Auth. Returns JSON dict or raw bytes."""
    auth    = _basic_auth()
    timeout = aiohttp.ClientTimeout(total=60)
    url     = f"{_REST_BASE}/{path}"
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as sess:
        async with sess.get(url, allow_redirects=True, **kwargs) as resp:
            if resp.status == 401:
                raise RuntimeError("Seedr auth failed (401). Check SEEDR_USERNAME/PASSWORD.")
            if resp.status == 403:
                raise RuntimeError(
                    "Seedr REST API returned 403 Forbidden.\n"
                    "REST API v1 requires a premium Seedr account.\n"
                    "For free accounts, install seedrcc: pip install seedrcc"
                )
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            if "json" in ct:
                return await resp.json()
            return await resp.read()


async def _rest_post(path: str, data: dict | None = None) -> dict:
    """POST /rest/{path} with Basic Auth."""
    auth    = _basic_auth()
    timeout = aiohttp.ClientTimeout(total=60)
    url     = f"{_REST_BASE}/{path}"
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as sess:
        async with sess.post(url, data=data, allow_redirects=True) as resp:
            if resp.status == 401:
                raise RuntimeError("Seedr auth failed (401).")
            if resp.status == 403:
                raise RuntimeError("Seedr REST API v1 requires premium. Install seedrcc for free accounts.")
            resp.raise_for_status()
            return await resp.json()


# ─────────────────────────────────────────────────────────────
# seedrcc library — OAuth2 helpers (free + premium)
# ─────────────────────────────────────────────────────────────

def _load_seedrcc_token() -> Optional["SeedrToken"]:
    """Load a previously saved seedrcc Token from disk."""
    if not _HAS_SEEDRCC:
        return None
    try:
        with open(_TOKEN_FILE, encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            return None
        return SeedrToken.from_json(raw)
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("[Seedr] Token load error: %s", e)
        return None


def _save_seedrcc_token(token: "SeedrToken") -> None:
    """Persist seedrcc Token to disk so it survives restarts."""
    try:
        os.makedirs(os.path.dirname(_TOKEN_FILE), exist_ok=True)
        with open(_TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(token.to_json())
        log.debug("[Seedr] Token saved to %s", _TOKEN_FILE)
    except Exception as e:
        log.warning("[Seedr] Token save error: %s", e)


async def _get_seedrcc_client() -> "AsyncSeedr":
    """
    Create an authenticated AsyncSeedr client.
    Priority: saved token → password login → error.
    Saves token after successful auth for next restart.
    """
    if not _HAS_SEEDRCC:
        raise RuntimeError("seedrcc library not installed")

    # Try saved token first (avoids re-login, preserves refresh token)
    saved = _load_seedrcc_token()
    if saved:
        try:
            client = AsyncSeedr(
                token=saved,
                on_token_refresh=lambda t: _save_seedrcc_token(t),
            )
            # Verify it still works
            await client.get_settings()
            log.info("[Seedr] Authenticated via saved token")
            return client
        except Exception as e:
            log.warning("[Seedr] Saved token invalid (%s) — re-authenticating", e)
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
            "Add SEEDR_USERNAME and SEEDR_PASSWORD to .env"
        )

    client = await AsyncSeedr.from_password(
        username, password,
        on_token_refresh=lambda t: _save_seedrcc_token(t),
    )

    # Save the token for next restart
    if client.token:
        _save_seedrcc_token(client.token)

    log.info("[Seedr] Authenticated via password login")
    return client


# ─────────────────────────────────────────────────────────────
# Unified public API — auto-selects strategy
# ─────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    """Return True if credentials are valid."""
    try:
        if _HAS_SEEDRCC:
            client = await _get_seedrcc_client()
            await client.close()
            return True
        else:
            await _rest_get("user")
            return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    """Return {used, total, free} in bytes."""
    if _HAS_SEEDRCC:
        client = await _get_seedrcc_client()
        try:
            settings = await client.get_settings()
            total = int(settings.account.space_max or 0)
            used  = int(settings.account.space_used or 0)
            return {"total": total, "used": used, "free": total - used}
        finally:
            await client.close()
    else:
        data = await _rest_get("user")
        total = int(data.get("space_max", 0))
        used  = int(data.get("space_used", 0))
        return {"total": total, "used": used, "free": total - used}


async def add_magnet(magnet: str) -> dict:
    """Submit a magnet link. Returns response dict."""
    if _HAS_SEEDRCC:
        client = await _get_seedrcc_client()
        try:
            result = await client.add_magnet(magnet)
            log.info("[Seedr] Magnet added via seedrcc: %s", result)
            return {"result": True, "data": result}
        finally:
            await client.close()
    else:
        result = await _rest_post("transfer/magnet", data={"magnet": magnet})
        if not result.get("result") and not result.get("id"):
            raise RuntimeError(f"Seedr rejected magnet: {result}")
        log.info("[Seedr] Magnet added via REST: %s", result)
        return result


async def _list_root_rest() -> dict:
    """List root folder via REST API."""
    return await _rest_get("folder")


async def poll_until_ready(
    torrent_name_hint: str = "",
    timeout_s: int = 3600,
    progress_cb=None,
    existing_folder_ids: Optional[set] = None,
) -> dict:
    """
    Poll until the torrent finishes. Returns folder dict {id, name, size, ...}.

    If using seedrcc: uses the library's folder listing.
    If using REST: polls GET /rest/folder.

    existing_folder_ids: set of folder IDs from BEFORE add_magnet()
        so we never return a stale folder.

    NOTE on Seedr: progress == 100 means downloaded, 101 means moved to folder.
    """
    if existing_folder_ids is None:
        existing_folder_ids = set()

    deadline  = time.time() + timeout_s
    last_pct  = -1.0

    if _HAS_SEEDRCC:
        client = await _get_seedrcc_client()
        try:
            while time.time() < deadline:
                try:
                    # Get root folder contents
                    root = await client.list_contents()

                    # Check active transfers
                    torrents = getattr(root, "torrents", []) or []
                    downloading = [
                        t for t in torrents
                        if getattr(t, "progress", 100) < 100
                    ]

                    # Check for new completed folders
                    folders = getattr(root, "folders", []) or []
                    new_folders = [
                        f for f in folders
                        if getattr(f, "id", None) not in existing_folder_ids
                        and (
                            not torrent_name_hint
                            or torrent_name_hint.lower()[:20]
                               in getattr(f, "name", "").lower()
                        )
                    ]

                    if downloading:
                        dl  = downloading[0]
                        pct = float(getattr(dl, "progress", 0))
                        if pct != last_pct:
                            last_pct = pct
                            name = getattr(dl, "name", "")
                            log.info("[Seedr] Progress: %.1f%%  %s", pct, name)
                            if progress_cb:
                                await progress_cb(pct, name)
                    elif new_folders:
                        folder = new_folders[-1]
                        fdict = {
                            "id":   getattr(folder, "id", 0),
                            "name": getattr(folder, "name", ""),
                            "size": getattr(folder, "size", 0),
                        }
                        log.info("[Seedr] Ready: %s (id=%s)",
                                 fdict["name"], fdict["id"])
                        return fdict
                    else:
                        log.debug("[Seedr] No new folder yet — waiting…")

                except Exception as e:
                    log.warning("[Seedr] Poll error: %s", e)

                await asyncio.sleep(10)
        finally:
            await client.close()
    else:
        # REST API v1 path
        while time.time() < deadline:
            try:
                root = await _list_root_rest()
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


async def get_file_urls(folder_id: int) -> list[dict]:
    """
    Return [{name, url, size}, ...] for every file in folder_id,
    recursing into sub-folders.
    """
    if _HAS_SEEDRCC:
        return await _get_file_urls_seedrcc(folder_id)
    else:
        return await _get_file_urls_rest(folder_id)


async def _get_file_urls_seedrcc(folder_id: int) -> list[dict]:
    """Recurse folder tree via seedrcc library."""
    client = await _get_seedrcc_client()
    try:
        result: list[dict] = []

        async def _collect(fid: int) -> None:
            contents = await client.list_contents(fid)

            for f in getattr(contents, "files", []) or []:
                file_id = getattr(f, "folder_file_id", None) or getattr(f, "id", None)
                name    = getattr(f, "name", "file")
                size    = int(getattr(f, "size", 0))
                if file_id:
                    try:
                        url_data = await client.fetch_file(file_id)
                        url = getattr(url_data, "url", "") or (
                            url_data if isinstance(url_data, str) else ""
                        )
                        if url:
                            result.append({"name": name, "url": url, "size": size})
                    except Exception as e:
                        log.warning("[Seedr] fetch_file %s failed: %s", name, e)

            for sub in getattr(contents, "folders", []) or []:
                sub_id = getattr(sub, "id", None)
                if sub_id:
                    try:
                        await _collect(sub_id)
                    except Exception as e:
                        log.warning("[Seedr] Sub-folder error: %s", e)

        await _collect(folder_id)
        return result
    finally:
        await client.close()


async def _get_file_urls_rest(folder_id: int) -> list[dict]:
    """Recurse folder tree via REST API v1."""
    result: list[dict] = []

    async def _collect(fid: int) -> None:
        contents = await _rest_get(f"folder/{fid}")

        for f in contents.get("files", []):
            file_id = f.get("folder_file_id") or f.get("id")
            name    = f.get("name", "file")
            size    = int(f.get("size", 0))
            if file_id:
                # GET /rest/file/{id} returns a redirect to the actual file URL
                # We just need to get the redirect URL, not download the whole file
                try:
                    auth    = _basic_auth()
                    timeout = aiohttp.ClientTimeout(total=30)
                    url     = f"{_REST_BASE}/file/{file_id}"
                    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as sess:
                        async with sess.get(url, allow_redirects=False) as resp:
                            if resp.status in (301, 302, 303, 307, 308):
                                dl_url = resp.headers.get("Location", "")
                            elif resp.status == 200:
                                # Some responses give the URL directly in JSON
                                ct = resp.headers.get("Content-Type", "")
                                if "json" in ct:
                                    data = await resp.json()
                                    dl_url = data.get("url", "")
                                else:
                                    dl_url = str(resp.url)
                            else:
                                dl_url = ""

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
                    log.warning("[Seedr] Sub-folder %s error: %s", sub.get("name"), e)

    await _collect(folder_id)
    return result


async def delete_folder(folder_id: int) -> None:
    """Delete a folder from Seedr to reclaim quota."""
    if _HAS_SEEDRCC:
        client = await _get_seedrcc_client()
        try:
            await client.delete_folder(folder_id)
            log.info("[Seedr] Deleted folder id=%d via seedrcc", folder_id)
        finally:
            await client.close()
    else:
        await _rest_post(f"folder/{folder_id}/delete")
        log.info("[Seedr] Deleted folder id=%d via REST", folder_id)


# ─────────────────────────────────────────────────────────────
# High-level helper — used by url_handler.py
# ─────────────────────────────────────────────────────────────

async def download_via_seedr(
    magnet:      str,
    dest:        str,
    progress_cb  = None,
    timeout_s:   int = 3600,
) -> list[str]:
    """
    Full Seedr pipeline:
      add magnet → poll until ready → fetch file URLs → download → delete.

    Returns list of local file paths in `dest`.
    progress_cb(stage: str, pct: float, detail: str) — optional.
    """
    from services.downloader import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("adding", 0.0, "Submitting to Seedr…")

    # ── Snapshot existing folders BEFORE adding magnet ────────
    # This prevents poll_until_ready from returning a stale folder
    existing_folder_ids: set = set()
    try:
        if _HAS_SEEDRCC:
            client = await _get_seedrcc_client()
            try:
                root = await client.list_contents()
                existing_folder_ids = {
                    getattr(f, "id", None)
                    for f in (getattr(root, "folders", []) or [])
                    if getattr(f, "id", None) is not None
                }
            finally:
                await client.close()
        else:
            root = await _list_root_rest()
            existing_folder_ids = {
                f.get("id")
                for f in root.get("folders", [])
                if f.get("id") is not None
            }
        log.info("[Seedr] Baseline: %d existing folder(s) excluded from poll",
                 len(existing_folder_ids))
    except Exception as exc:
        log.warning("[Seedr] Snapshot failed (%s) — using empty baseline", exc)

    # ── Add magnet ────────────────────────────────────────────
    await add_magnet(magnet)

    if progress_cb:
        await progress_cb("waiting", 0.0, "Seedr is downloading…")

    # ── Extract torrent name hint from magnet ─────────────────
    import re, urllib.parse as _up
    dn_match = re.search(r"[&?]dn=([^&]+)", magnet)
    name_hint = _up.unquote_plus(dn_match.group(1))[:50] if dn_match else ""

    # ── Poll until done ───────────────────────────────────────
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

    # ── Get file URLs ─────────────────────────────────────────
    files = await get_file_urls(folder_id)
    if not files:
        raise RuntimeError("Seedr returned no files for this torrent.")

    # ── Download each file to dest ────────────────────────────
    os.makedirs(dest, exist_ok=True)
    local_paths = []

    for i, f in enumerate(files):
        raw_name   = f["name"]
        clean_name = sanitize_filename(raw_name)
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
            # Rename if downloader used original name
            if os.path.basename(path) != clean_name:
                new_path = os.path.join(os.path.dirname(path), clean_name)
                os.rename(path, new_path)
                path = new_path
            local_paths.append(path)
        except Exception as e:
            log.error("[Seedr] Download failed for %s: %s", raw_name, e)

    # ── Cleanup Seedr storage ─────────────────────────────────
    try:
        await delete_folder(folder_id)
        log.info("[Seedr] Cleaned up folder %d", folder_id)
    except Exception as e:
        log.warning("[Seedr] Cleanup failed: %s", e)

    return local_paths
