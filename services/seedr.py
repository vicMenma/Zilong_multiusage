"""
services/seedr.py
Seedr.cc cloud torrent client — uses seedrcc v2.1.0 library.

FIX BUG-UH-03: poll_until_ready now fires progress_cb on a 30-second
  heartbeat regardless of percentage change.  Previously the callback
  only fired when pct changed, so a slow/stalled torrent produced no UI
  updates for minutes — making the Seedr panel appear frozen.

FIX BUG-SEEDR-03: add_magnet() now uses a 3-layer submission strategy:
  Layer 1 — seedrcc library (func=add_torrent, torrent_magnet in body,
             access_token+func as URL query params).
  Layer 2 — Direct oauth_test HTTP with CORRECT format: access_token and
             func as URL query params (not POST body), field name is
             torrent_magnet (not magnet_link as the old fallback used).
             The old fallback put everything in the POST body which is
             wrong and also used the wrong field name.
  Layer 3 — Seedr REST API with HTTP Basic Auth (SEEDR_USERNAME +
             SEEDR_PASSWORD).  Tries both /rest/transfer/magnet and
             /rest/torrent/magnet, which are the officially documented
             endpoints.  This is independent of the oauth_test path and
             works even when the OAuth endpoint drifts.

═══════════════════════════════════════════════════════════════════
METHOD NAMES VERIFIED FROM ACTUAL seedrcc v2.1.0 SOURCE CODE:
  AsyncSeedr.from_password(username, password, on_token_refresh=)
  client.add_torrent(magnet_link='magnet:?...')   → payload uses torrent_magnet
  client.list_contents(folder_id='0')             → payload uses content_id
  client.fetch_file(file_id: str)                 → payload uses folder_file_id
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


# ─────────────────────────────────────────────────────────────
# BUG-SEEDR-01 FIX: Seedr removed 'max_invites' from the
# get_settings() response.  seedrcc v2.1.0's AccountInfo still
# declares it as a required positional arg, so every call to
# get_settings() raises:
#   AccountInfo.__init__() missing 1 required positional
#   argument: 'max_invites'
# This causes _get_client() to ALWAYS treat the saved token as
# expired and re-authenticate via password — wasting a round-trip
# on every single API call.
#
# Fix: wrap AccountInfo.__init__ so that missing 'max_invites'
# defaults to 0/None instead of raising TypeError.
#
# NOTE: seedrcc 2.1.0 uses dataclasses with from_dict() which
# filters keys — if 'max_invites' is absent in the API response it
# won't be passed to __init__ and will still raise TypeError.
# The patch handles this by intercepting the TypeError.
# ─────────────────────────────────────────────────────────────

def _patch_seedrcc_account_info() -> None:
    """Make AccountInfo tolerant of Seedr dropping 'max_invites'."""
    try:
        import seedrcc.models as _seedr_models  # type: ignore
        _OrigAccountInfo = _seedr_models.AccountInfo
        _orig_init = _OrigAccountInfo.__init__

        import inspect as _inspect
        sig = _inspect.signature(_orig_init)
        params = list(sig.parameters.values())
        # Only patch if 'max_invites' exists and has no default
        needs_patch = any(
            p.name == "max_invites" and p.default is _inspect.Parameter.empty
            for p in params
        )
        if not needs_patch:
            log.debug("[Seedr] AccountInfo: no patch needed")
            return

        def _patched_init(self, *args, **kwargs):
            try:
                _orig_init(self, *args, **kwargs)
            except TypeError as exc:
                if "max_invites" in str(exc):
                    kwargs.setdefault("max_invites", 0)
                    _orig_init(self, *args, **kwargs)
                else:
                    raise

        _OrigAccountInfo.__init__ = _patched_init
        log.info("[Seedr] AccountInfo patched — 'max_invites' is now optional")
    except Exception as exc:
        log.warning("[Seedr] AccountInfo patch skipped: %s", exc)


_patch_seedrcc_account_info()


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
    """
    Submit a magnet link to Seedr using a 3-layer strategy.

    BUG-SEEDR-03 FIX (supersedes BUG-SEEDR-02):

    Layer 1 — seedrcc library (v2.1.0):
        Sends access_token + func=add_torrent as URL query params,
        torrent_magnet + folder_id in POST body.  This is the canonical
        format.  May still 404 if Seedr has drifted the oauth_test path.

    Layer 2 — Direct oauth_test HTTP with CORRECTED format:
        The old fallback was broken in two ways:
          (a) It put access_token and func in the POST body instead of
              URL query params — Seedr ignores them there.
          (b) It used the field name "magnet_link" instead of the
              correct "torrent_magnet" (verified from seedrcc 2.1.0
              AddTorrentPayload).
        This layer fixes both issues.

    Layer 3 — Seedr REST API with HTTP Basic Auth:
        POST https://www.seedr.cc/rest/transfer/magnet  data=magnet=...
        POST https://www.seedr.cc/rest/torrent/magnet   data=magnet=...
        Uses SEEDR_USERNAME + SEEDR_PASSWORD from env.  Officially
        documented and completely independent of the oauth_test path.
    """
    client = await _get_client()
    try:
        # ── Layer 1: seedrcc library ──────────────────────────────────────
        try:
            result = await client.add_torrent(magnet_link=magnet)
            log.info(
                "[Seedr] Magnet submitted via seedrcc library: hash=%s title=%s",
                getattr(result, "torrent_hash", "?"),
                getattr(result, "title", "?"),
            )
            return {
                "result": True,
                "user_torrent_id": getattr(result, "user_torrent_id", None),
                "torrent_hash":    getattr(result, "torrent_hash", ""),
                "title":           getattr(result, "title", ""),
            }
        except Exception as lib_err:
            log.warning(
                "[Seedr] seedrcc library add_torrent failed (%s) — trying direct HTTP",
                lib_err,
            )

        # ── Layer 2: Direct oauth_test HTTP — CORRECTED format ────────────
        # access_token + func MUST be URL query params, not POST body.
        # Field name is torrent_magnet (not magnet_link as the old code used).
        import httpx

        token_obj    = client.token
        access_token = (
            token_obj.access_token
            if hasattr(token_obj, "access_token")
            else str(token_obj)
        )
        _OAUTH_URL = "https://www.seedr.cc/oauth_test/resource.php"

        async with httpx.AsyncClient(timeout=60) as http:
            try:
                resp = await http.post(
                    _OAUTH_URL,
                    params={"func": "add_torrent", "access_token": access_token},
                    data={"torrent_magnet": magnet, "folder_id": "-1"},
                )
                log.debug(
                    "[Seedr] Direct oauth_test add_torrent → HTTP %d: %s",
                    resp.status_code, resp.text[:300],
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("result") is not False:
                        log.info("[Seedr] Magnet submitted via direct oauth_test: %s", data)
                        return {
                            "result":         True,
                            "user_torrent_id": data.get("user_torrent_id"),
                            "torrent_hash":    data.get("torrent_hash", ""),
                            "title":           data.get("title", ""),
                        }
                    log.warning(
                        "[Seedr] Direct oauth_test returned result=false: %s", data
                    )
                else:
                    log.warning(
                        "[Seedr] Direct oauth_test returned HTTP %d: %s",
                        resp.status_code, resp.text[:200],
                    )
            except Exception as oauth_err:
                log.warning("[Seedr] Direct oauth_test request error: %s", oauth_err)

        # ── Layer 3: Seedr REST API with HTTP Basic Auth ──────────────────
        # Official documented endpoints — independent of oauth_test path.
        # Field name is "magnet" per https://www.seedr.cc/docs/api/rest/v1/
        username = os.environ.get("SEEDR_USERNAME", "").strip()
        password = os.environ.get("SEEDR_PASSWORD", "").strip()

        if not username or not password:
            log.warning(
                "[Seedr] REST API fallback skipped — SEEDR_USERNAME/PASSWORD not set"
            )
        else:
            _REST_PATHS = (
                "/rest/transfer/magnet",  # official docs endpoint
                "/rest/torrent/magnet",   # alternative endpoint seen in the wild
            )
            async with httpx.AsyncClient(timeout=60) as http:
                for path in _REST_PATHS:
                    url = f"https://www.seedr.cc{path}"
                    try:
                        resp = await http.post(
                            url,
                            auth=(username, password),
                            data={"magnet": magnet},
                        )
                        log.debug(
                            "[Seedr] REST %s → HTTP %d: %s",
                            path, resp.status_code, resp.text[:300],
                        )
                        if resp.status_code in (200, 201):
                            try:
                                data = resp.json()
                            except Exception:
                                data = {}
                            # REST API may return {"result": true} or just {}
                            # A 200/201 with non-error body is a success
                            if data.get("result") is not False:
                                log.info(
                                    "[Seedr] Magnet submitted via REST %s: %s",
                                    path, data,
                                )
                                return {
                                    "result":         True,
                                    "user_torrent_id": data.get("user_torrent_id")
                                                      or data.get("id"),
                                    "torrent_hash":   data.get("torrent_hash", ""),
                                    "title":          data.get("title", ""),
                                }
                            log.warning(
                                "[Seedr] REST %s returned result=false: %s", path, data
                            )
                        else:
                            log.warning(
                                "[Seedr] REST %s returned HTTP %d: %s",
                                path, resp.status_code, resp.text[:200],
                            )
                    except Exception as rest_err:
                        log.warning(
                            "[Seedr] REST %s request error: %s", path, rest_err
                        )

    finally:
        await client.close()

    raise RuntimeError(
        "All Seedr magnet submission methods failed (library, direct oauth_test, REST API). "
        "Check your credentials and https://www.seedr.cc for any service disruptions."
    )


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
            "folders":    folders,
            "files":      files,
            "torrents":   torrents,
            "space_used": root.space_used,
            "space_max":  root.space_max,
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
            root     = await list_folder(0)
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
                log.info(
                    "[Seedr] Ready: %s (id=%s)",
                    folder.get("name"), folder.get("id"),
                )
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
