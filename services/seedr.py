"""
services/seedr.py
Seedr.cc cloud torrent client — uses seedrcc v2.0.2 library.

FIX BUG-UH-03: poll_until_ready now fires progress_cb on a 30-second
  heartbeat regardless of percentage change.  Previously the callback
  only fired when pct changed, so a slow/stalled torrent produced no UI
  updates for minutes — making the Seedr panel appear frozen.

═══════════════════════════════════════════════════════════════════
METHOD NAMES VERIFIED FROM ACTUAL seedrcc v2.0.2 SOURCE CODE:
  AsyncSeedr.from_password(username, password, on_token_refresh=)
  client.add_torrent(magnet_link='magnet:?...')
  client.list_contents(folder_id='0')
  client.fetch_file(file_id: str)
  client.delete_folder(folder_id: str)
  client.get_settings()
  client.close()
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
    try:
        os.makedirs(os.path.dirname(_TOKEN_FILE), exist_ok=True)
        with open(_TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(token.to_json())
        log.debug("[Seedr] Token saved")
    except Exception as e:
        log.warning("[Seedr] Token save error: %s", e)


def _load_token():
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
    from seedrcc import AsyncSeedr, Token

    saved = _load_token()
    if saved:
        try:
            client = AsyncSeedr(token=saved, on_token_refresh=_save_token)
            await client.get_settings()
            log.info("[Seedr] Authenticated via saved token")
            return client
        except Exception as e:
            log.warning("[Seedr] Saved token expired (%s) — re-authenticating", e)
            try:
                await client.close()
            except Exception:
                pass

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
    if client.token:
        _save_token(client.token)
    log.info("[Seedr] Authenticated via password login")
    return client


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    try:
        client = await _get_client()
        await client.close()
        return True
    except Exception as e:
        log.warning("[Seedr] Credential check failed: %s", e)
        return False


async def get_storage_info() -> dict:
    client = await _get_client()
    try:
        root = await client.list_contents(folder_id="0")
        total = int(root.space_max or 0)
        used  = int(root.space_used or 0)
        return {"total": total, "used": used, "free": total - used}
    finally:
        await client.close()


async def add_magnet(magnet: str) -> dict:
    client = await _get_client()
    try:
        result = await client.add_torrent(magnet_link=magnet)
        log.info("[Seedr] Magnet submitted: hash=%s title=%s",
                 getattr(result, "torrent_hash", "?"),
                 getattr(result, "title", "?"))
        return {
            "result": True,
            "user_torrent_id": getattr(result, "user_torrent_id", None),
            "torrent_hash": getattr(result, "torrent_hash", ""),
            "title": getattr(result, "title", ""),
        }
    finally:
        await client.close()


async def list_folder(folder_id: int = 0) -> dict:
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
    client = await _get_client()
    try:
        await client.delete_folder(str(folder_id))
        log.info("[Seedr] Deleted folder id=%d", folder_id)
    finally:
        await client.close()


# ─────────────────────────────────────────────────────────────
# Poll until torrent finishes — with 30-second heartbeat
# ─────────────────────────────────────────────────────────────

_HEARTBEAT_INTERVAL = 30   # seconds — fire progress_cb even if pct unchanged


async def poll_until_ready(
    torrent_name_hint: str = "",
    timeout_s: int = 3600,
    progress_cb=None,
    existing_folder_ids: Optional[set] = None,
) -> dict:
    """
    Poll Seedr root folder until the torrent finishes.

    FIX BUG-UH-03: progress_cb now fires on a 30-second heartbeat regardless
    of whether the percentage changed.  This prevents the Seedr panel from
    appearing frozen on slow or stalled torrents.

    Returns folder dict {id, name, size}.
    """
    if existing_folder_ids is None:
        existing_folder_ids = set()

    deadline       = time.time() + timeout_s
    last_pct       = -1.0
    last_heartbeat = time.time()   # FIX BUG-UH-03

    while time.time() < deadline:
        try:
            root = await list_folder(0)
            folders  = root.get("folders", [])
            torrents = root.get("torrents", [])

            downloading = []
            for t in torrents:
                try:
                    pct = float(t.get("progress", "100"))
                except (ValueError, TypeError):
                    pct = 100.0
                if pct < 100:
                    downloading.append({**t, "_pct": pct})

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
                now = time.time()
                # FIX BUG-UH-03: fire on pct change OR 30s heartbeat
                if pct != last_pct or (now - last_heartbeat) >= _HEARTBEAT_INTERVAL:
                    last_pct       = pct
                    last_heartbeat = now
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
    """
    Full pipeline: add magnet → poll → fetch URLs → download → cleanup.
    Returns list of local file paths in `dest`.
    """
    from services.downloader import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("adding", 0.0, "Submitting to Seedr…")

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
