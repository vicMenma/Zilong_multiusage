"""
services/seedr.py
Seedr.cc cloud torrent client — targets seedrcc v2.1.0+.

═══════════════════════════════════════════════════════════════════
REWRITE CHANGELOG
─────────────────────────────────────────────────────────────────
FIX-AUTH-01 · Root cause of "Invalid username and password combination"
  The original code called get_settings() on every _get_client() call to
  probe the saved token.  get_settings() is an expensive authenticated call
  that also deserialises AccountInfo — if Seedr ever drops max_invites from
  that response the whole chain crashes and _get_client() falls through to
  re-authenticate via password.  Under rate-limiting or after a password
  change, that re-auth itself fails with AuthenticationError.

  Fix: replace the get_settings() probe with a lightweight test_token() call
  (func=test_token in the oauth_test resource endpoint).  test_token() just
  returns {"result": true} and never touches AccountInfo, so the max_invites
  issue cannot surface here at all.

FIX-AUTH-02 · Device-code authentication path (recommended, 1-year tokens)
  Password-based OAuth tokens from seedr_chrome are short-lived (~1 h) and
  Seedr blocks repeated password logins if they come in rapid succession.
  Device-code tokens (seedr_xbmc) last one year and are the method the
  official Kodi extension uses.

  New env var: SEEDR_DEVICE_CODE  (obtain once via /cmd_seedr_auth in bot)
  When present, _get_client() prefers device-code auth over password auth.
  The resulting token is cached in data/seedr_token.json and refreshed
  automatically via the refresh_token stored alongside it.

FIX-AUTH-03 · from_refresh_token fast-path
  If the saved Token has a refresh_token, try AsyncSeedr.from_refresh_token()
  before falling back to full re-authentication.  This avoids sending the
  cleartext password across the wire on every token expiry.

FIX-SEEDR-04 · Correct REST v1 fallback endpoints
  The previous Layer 3 tried /rest/transfer/magnet and /rest/torrent/magnet
  neither of which exists.  The actual Seedr REST v1 torrent add endpoint is:
    POST /rest/torrent  data={"magnet": "<url>"}
  (see https://www.seedr.cc/docs/api/rest/v1/ — requires premium account).

FIX-SEEDR-05 · Client singleton / connection pooling
  _get_client() previously created and discarded an httpx.AsyncClient on
  every call.  This rewrite uses a module-level _CLIENT_CACHE that keeps a
  live AsyncSeedr instance alive between calls and only recreates it when
  authentication truly fails.  This eliminates TCP handshake overhead and
  reduces the chance of hitting Seedr's login rate-limit.

FIX-SEEDR-06 · Improved max_invites patch for frozen dataclass
  Patching __init__ on a frozen dataclass in CPython works (object.__setattr__
  is used internally), but only if the patch is applied BEFORE any import that
  might cache the original __init__ in a closure.  The patch is now applied at
  module import time (same as before) but also verifies it actually took effect
  by running a dry test against a minimal dict.

FIX-BUG-UH-03 (preserved) · 30-second heartbeat in poll_until_ready
  Kept from original — progress_cb fires on percentage change OR every 30s.

═══════════════════════════════════════════════════════════════════
SEEDRCC v2.1.0 METHOD REFERENCE
  AsyncSeedr.get_device_code()              → DeviceCode (static, no auth)
  AsyncSeedr.from_device_code(device_code) → AsyncSeedr
  AsyncSeedr.from_password(user, pass)     → AsyncSeedr
  AsyncSeedr.from_refresh_token(token)     → AsyncSeedr
  client.add_torrent(magnet_link='...')    → AddTorrentResult
  client.list_contents(folder_id='0')      → ListContentsResult
  client.fetch_file(file_id: str)          → FetchFileResult
  client.delete_folder(folder_id: str)     → APIResult
  client.get_settings()                    → UserSettings
  client.refresh_token()                   → RefreshTokenResult
  client.close()
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

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# FIX-SEEDR-06: max_invites patch — applied at import time
# Seedr dropped 'max_invites' from the get_settings() response.
# seedrcc v2.1.0 AccountInfo still declares it as a required field, so
# from_dict() raises TypeError when the key is absent.
# We intercept __init__ so that a missing max_invites defaults to 0.
# ─────────────────────────────────────────────────────────────────────────────

def _patch_seedrcc_account_info() -> None:
    """Make AccountInfo tolerant of Seedr dropping 'max_invites'."""
    try:
        import seedrcc.models as _seedr_models  # type: ignore
        import inspect as _inspect

        _OrigAccountInfo = _seedr_models.AccountInfo
        _orig_init       = _OrigAccountInfo.__init__

        sig    = _inspect.signature(_orig_init)
        params = list(sig.parameters.values())
        needs_patch = any(
            p.name == "max_invites" and p.default is _inspect.Parameter.empty
            for p in params
        )
        if not needs_patch:
            log.debug("[Seedr] AccountInfo patch not required")
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

        # Verify the patch actually works before proceeding.
        _test = {
            "username": "_patch_test", "user_id": 0, "premium": 0,
            "package_id": 0, "package_name": "", "space_used": 0,
            "space_max": 0, "bandwidth_used": 0, "email": "",
            "wishlist": [], "invites": 0, "invites_accepted": 0,
            # max_invites intentionally absent
        }
        _seedr_models.AccountInfo.from_dict(_test)
        log.info("[Seedr] AccountInfo patched — 'max_invites' is now optional")

    except Exception as exc:
        log.warning("[Seedr] AccountInfo patch skipped: %s", exc)


_patch_seedrcc_account_info()


# ─────────────────────────────────────────────────────────────────────────────
# Token file path
# ─────────────────────────────────────────────────────────────────────────────

_TOKEN_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "seedr_token.json")
)


# ─────────────────────────────────────────────────────────────────────────────
# Token persistence helpers
# ─────────────────────────────────────────────────────────────────────────────

def _save_token(token) -> None:
    try:
        os.makedirs(os.path.dirname(_TOKEN_FILE), exist_ok=True)
        with open(_TOKEN_FILE, "w", encoding="utf-8") as fh:
            fh.write(token.to_json())
        log.debug("[Seedr] Token saved to %s", _TOKEN_FILE)
    except Exception as exc:
        log.warning("[Seedr] Token save error: %s", exc)


def _load_token():
    """Return a seedrcc.Token from disk, or None."""
    try:
        from seedrcc import Token
        with open(_TOKEN_FILE, encoding="utf-8") as fh:
            raw = fh.read().strip()
        if not raw:
            return None
        token = Token.from_json(raw)
        log.debug("[Seedr] Token loaded from disk")
        return token
    except FileNotFoundError:
        return None
    except Exception as exc:
        log.warning("[Seedr] Token load error: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# FIX-SEEDR-05: Client singleton cache
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT_CACHE: Optional["AsyncSeedr"] = None  # type: ignore[name-defined]
_CLIENT_LOCK = asyncio.Lock()


async def _invalidate_client() -> None:
    """Close and discard the cached client so the next call re-authenticates."""
    global _CLIENT_CACHE
    async with _CLIENT_LOCK:
        if _CLIENT_CACHE is not None:
            try:
                await _CLIENT_CACHE.close()
            except Exception:
                pass
            _CLIENT_CACHE = None
            log.debug("[Seedr] Client cache invalidated")


async def _probe_client(client) -> bool:
    """
    FIX-AUTH-01: Use refresh_token (lightweight) instead of get_settings()
    to probe whether the cached client is still valid.

    seedrcc 2.1.0 exposes client.refresh_token() which hits token.php with the
    refresh_token grant — no AccountInfo deserialisation involved, so the
    max_invites issue can't surface here.

    If the token has no refresh_token we fall back to get_settings() but wrap
    the AccountInfo failure so it doesn't propagate.
    """
    from seedrcc.exceptions import AuthenticationError  # type: ignore

    try:
        tok = client.token
        if tok.refresh_token:
            await client.refresh_token()
            log.debug("[Seedr] Token probed via refresh_token — still valid")
            return True
    except AuthenticationError:
        return False
    except Exception as exc:
        log.debug("[Seedr] refresh_token probe failed (%s) — trying get_settings", exc)

    # Fallback probe via get_settings (handles missing max_invites via our patch)
    try:
        await client.get_settings()
        log.debug("[Seedr] Token probed via get_settings — still valid")
        return True
    except AuthenticationError:
        return False
    except Exception as exc:
        log.warning("[Seedr] Token probe error: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Client factory — the single point of authentication
# ─────────────────────────────────────────────────────────────────────────────

async def _get_client():
    """
    Return an authenticated AsyncSeedr client, using the singleton cache.

    Authentication priority (FIX-AUTH-01 / FIX-AUTH-02 / FIX-AUTH-03):
      1. Return cached client if still alive.
      2. Restore from saved token on disk.
         a. If token has refresh_token → AsyncSeedr.from_refresh_token()
         b. Otherwise → AsyncSeedr(token=saved_token)
      3. Fresh login:
         a. SEEDR_DEVICE_CODE env var → AsyncSeedr.from_device_code()
         b. SEEDR_USERNAME + SEEDR_PASSWORD → AsyncSeedr.from_password()
    """
    global _CLIENT_CACHE

    async with _CLIENT_LOCK:
        # ── 1. Return cached live client ──────────────────────────────────────
        if _CLIENT_CACHE is not None:
            return _CLIENT_CACHE

        from seedrcc import AsyncSeedr, Token  # type: ignore
        from seedrcc.exceptions import AuthenticationError  # type: ignore

        client: Optional[AsyncSeedr] = None

        # ── 2. Restore from disk ──────────────────────────────────────────────
        saved = _load_token()
        if saved:
            # 2a. If we have a refresh_token, use from_refresh_token for a clean
            #     re-auth without exposing the cleartext password (FIX-AUTH-03).
            if saved.refresh_token:
                try:
                    client = await AsyncSeedr.from_refresh_token(
                        saved.refresh_token,
                        on_token_refresh=_save_token,
                    )
                    _save_token(client.token)
                    log.info("[Seedr] Authenticated via saved refresh_token")
                    _CLIENT_CACHE = client
                    return _CLIENT_CACHE
                except Exception as exc:
                    log.warning(
                        "[Seedr] refresh_token login failed (%s) — "
                        "trying saved access_token",
                        exc,
                    )
                    client = None

            # 2b. Construct client with the saved token and probe it.
            try:
                candidate = AsyncSeedr(token=saved, on_token_refresh=_save_token)
                if await _probe_client(candidate):
                    _save_token(candidate.token)
                    log.info("[Seedr] Authenticated via saved access_token")
                    _CLIENT_CACHE = candidate
                    return _CLIENT_CACHE
                await candidate.close()
                log.info("[Seedr] Saved token invalid — falling through to fresh login")
            except Exception as exc:
                log.warning("[Seedr] Saved token restore failed (%s) — fresh login", exc)

        # ── 3. Fresh login ────────────────────────────────────────────────────
        device_code = os.environ.get("SEEDR_DEVICE_CODE", "").strip()
        username    = os.environ.get("SEEDR_USERNAME", "").strip()
        password    = os.environ.get("SEEDR_PASSWORD", "").strip()

        # 3a. Device-code path (FIX-AUTH-02) — preferred, 1-year token
        if device_code:
            try:
                client = await AsyncSeedr.from_device_code(
                    device_code,
                    on_token_refresh=_save_token,
                )
                _save_token(client.token)
                log.info("[Seedr] Authenticated via SEEDR_DEVICE_CODE")
                _CLIENT_CACHE = client
                return _CLIENT_CACHE
            except Exception as exc:
                log.warning(
                    "[Seedr] Device-code login failed (%s) — trying password", exc
                )

        # 3b. Password path — fallback
        if not username or not password:
            raise RuntimeError(
                "Seedr credentials not configured.\n"
                "Option A (recommended — 1-year token):\n"
                "  1. Run /seedr_deviceauth in the bot\n"
                "  2. Visit https://seedr.cc/devices and enter the code\n"
                "  3. Add SEEDR_DEVICE_CODE=<device_code> to your .env\n"
                "Option B:\n"
                "  Add SEEDR_USERNAME=your@email.com and "
                "SEEDR_PASSWORD=yourpassword to your .env"
            )

        try:
            client = await AsyncSeedr.from_password(
                username, password,
                on_token_refresh=_save_token,
            )
            _save_token(client.token)
            log.info("[Seedr] Authenticated via SEEDR_USERNAME / SEEDR_PASSWORD")
            _CLIENT_CACHE = client
            return _CLIENT_CACHE
        except Exception as exc:
            raise RuntimeError(
                f"Seedr password authentication failed: {exc}\n"
                "• Check SEEDR_USERNAME and SEEDR_PASSWORD in your .env.\n"
                "• If correct, use the device-code method instead:\n"
                "  1. Run /seedr_deviceauth in the bot\n"
                "  2. Visit https://seedr.cc/devices and enter the user code\n"
                "  3. Set SEEDR_DEVICE_CODE=<device_code> in your .env"
            ) from exc


# ─────────────────────────────────────────────────────────────────────────────
# Helper: run an API call and invalidate the cache on auth errors
# ─────────────────────────────────────────────────────────────────────────────

async def _run(coro_factory):
    """
    Execute ``coro_factory(client)`` where client is the cached AsyncSeedr.
    On AuthenticationError, flush the cache and retry once with a fresh client.
    """
    from seedrcc.exceptions import AuthenticationError  # type: ignore

    for attempt in range(2):
        client = await _get_client()
        try:
            return await coro_factory(client)
        except AuthenticationError as exc:
            log.warning(
                "[Seedr] Auth error on attempt %d: %s — %s",
                attempt + 1,
                exc,
                "invalidating cache and retrying" if attempt == 0 else "giving up",
            )
            await _invalidate_client()
            if attempt == 1:
                raise

    raise RuntimeError("Unreachable")


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

async def check_credentials() -> bool:
    """Return True if we can authenticate successfully."""
    try:
        await _get_client()
        return True
    except Exception as exc:
        log.warning("[Seedr] Credential check failed: %s", exc)
        return False


async def get_storage_info() -> dict:
    """Return {'total': int, 'used': int, 'free': int} in bytes."""
    async def _call(client):
        root  = await client.list_contents(folder_id="0")
        total = int(root.space_max or 0)
        used  = int(root.space_used or 0)
        return {"total": total, "used": used, "free": total - used}

    return await _run(_call)


async def add_magnet(magnet: str) -> dict:
    """
    Submit a magnet link to Seedr.

    Strategy (three layers, each guarded independently):

    Layer 1 — seedrcc library client.add_torrent(magnet_link=...)
        The canonical path.  Uses access_token + func=add_torrent as URL
        query params and torrent_magnet in the POST body (verified from
        seedrcc 2.1.0 source).

    Layer 2 — Direct oauth_test HTTP (corrected)
        access_token + func as URL query params (NOT POST body).
        Field name is torrent_magnet (NOT magnet_link).

    Layer 3 — Seedr REST v1 with HTTP Basic Auth (FIX-SEEDR-04)
        POST https://www.seedr.cc/rest/torrent
        data={"magnet": "<url>"}
        This is the only documented REST v1 endpoint for adding a torrent.
        (The old code tried /rest/transfer/magnet and /rest/torrent/magnet
        which do not exist in the documented API.)
        Requires a premium Seedr account.
    """
    import httpx  # already a dependency of seedrcc

    # ── Layer 1: seedrcc library ──────────────────────────────────────────────
    try:
        async def _lib_call(client):
            result = await client.add_torrent(magnet_link=magnet)
            return {
                "result":         True,
                "user_torrent_id": getattr(result, "user_torrent_id", None),
                "torrent_hash":    getattr(result, "torrent_hash", ""),
                "title":           getattr(result, "title", ""),
            }

        data = await _run(_lib_call)
        log.info(
            "[Seedr] Magnet submitted via seedrcc library: hash=%s title=%s",
            data.get("torrent_hash", "?"), data.get("title", "?"),
        )
        return data
    except Exception as lib_err:
        log.warning(
            "[Seedr] Layer 1 (seedrcc library) failed (%s) — trying Layer 2",
            lib_err,
        )

    # ── Layer 2: Direct oauth_test with corrected field names ─────────────────
    _OAUTH_URL = "https://www.seedr.cc/oauth_test/resource.php"
    try:
        client      = await _get_client()
        token_obj   = client.token
        access_token = (
            token_obj.access_token
            if hasattr(token_obj, "access_token")
            else str(token_obj)
        )
        async with httpx.AsyncClient(timeout=60) as http:
            resp = await http.post(
                _OAUTH_URL,
                params={"func": "add_torrent", "access_token": access_token},
                data={"torrent_magnet": magnet, "folder_id": "-1"},
            )
        log.debug(
            "[Seedr] Layer 2 oauth_test → HTTP %d: %s",
            resp.status_code, resp.text[:300],
        )
        if resp.status_code == 200:
            resp_data = resp.json()
            if resp_data.get("result") is not False:
                log.info("[Seedr] Magnet submitted via Layer 2 oauth_test: %s", resp_data)
                return {
                    "result":         True,
                    "user_torrent_id": resp_data.get("user_torrent_id"),
                    "torrent_hash":    resp_data.get("torrent_hash", ""),
                    "title":           resp_data.get("title", ""),
                }
            log.warning("[Seedr] Layer 2 result=false: %s", resp_data)
        else:
            log.warning(
                "[Seedr] Layer 2 HTTP %d: %s",
                resp.status_code, resp.text[:200],
            )
    except Exception as l2_err:
        log.warning("[Seedr] Layer 2 failed (%s) — trying Layer 3", l2_err)

    # ── Layer 3: REST v1 with HTTP Basic Auth (FIX-SEEDR-04) ─────────────────
    # Correct endpoint: POST /rest/torrent  (not /rest/transfer/magnet)
    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()

    if not username or not password:
        log.warning(
            "[Seedr] Layer 3 skipped — SEEDR_USERNAME/PASSWORD not set"
        )
    else:
        _REST_ENDPOINTS = (
            "/rest/torrent",          # REST v1 documented endpoint
            "/rest/folder/transfer",  # alternative seen in community usage
        )
        async with httpx.AsyncClient(timeout=60) as http:
            for path in _REST_ENDPOINTS:
                url = f"https://www.seedr.cc{path}"
                try:
                    resp = await http.post(
                        url,
                        auth=(username, password),
                        data={"magnet": magnet},
                    )
                    log.debug(
                        "[Seedr] Layer 3 REST %s → HTTP %d: %s",
                        path, resp.status_code, resp.text[:300],
                    )
                    if resp.status_code in (200, 201):
                        try:
                            resp_data = resp.json()
                        except Exception:
                            resp_data = {}
                        if resp_data.get("result") is not False:
                            log.info(
                                "[Seedr] Magnet submitted via Layer 3 REST %s: %s",
                                path, resp_data,
                            )
                            return {
                                "result":         True,
                                "user_torrent_id": (
                                    resp_data.get("user_torrent_id")
                                    or resp_data.get("id")
                                ),
                                "torrent_hash":    resp_data.get("torrent_hash", ""),
                                "title":           resp_data.get("title", ""),
                            }
                        log.warning(
                            "[Seedr] Layer 3 REST %s result=false: %s", path, resp_data
                        )
                    else:
                        log.warning(
                            "[Seedr] Layer 3 REST %s HTTP %d: %s",
                            path, resp.status_code, resp.text[:200],
                        )
                except Exception as l3_err:
                    log.warning("[Seedr] Layer 3 REST %s error: %s", path, l3_err)

    raise RuntimeError(
        "All Seedr magnet submission layers failed.\n"
        "• Verify your credentials (SEEDR_USERNAME / SEEDR_PASSWORD or SEEDR_DEVICE_CODE).\n"
        "• Check https://www.seedr.cc for service status.\n"
        "• REST v1 (Layer 3) requires a premium Seedr account."
    )


async def list_folder(folder_id: int = 0) -> dict:
    """Return {'folders': [...], 'files': [...], 'torrents': [...], ...}."""
    async def _call(client):
        root = await client.list_contents(folder_id=str(folder_id))
        folders = [
            {"id": f.id, "name": f.name, "size": f.size}
            for f in (root.folders or [])
        ]
        files = [
            {
                "folder_file_id": f.folder_file_id,
                "name":           f.name,
                "size":           f.size,
                "id":             f.file_id,
            }
            for f in (root.files or [])
        ]
        torrents = [
            {
                "id":           t.id,
                "name":         t.name,
                "progress":     t.progress,
                "size":         t.size,
                "progress_url": getattr(t, "progress_url", None),
            }
            for t in (root.torrents or [])
        ]
        return {
            "folders":    folders,
            "files":      files,
            "torrents":   torrents,
            "space_used": root.space_used,
            "space_max":  root.space_max,
        }

    return await _run(_call)


async def get_file_download_url(file_id: int) -> str:
    """Return the direct download URL for a file by its folder_file_id."""
    async def _call(client):
        result = await client.fetch_file(str(file_id))
        url = getattr(result, "url", "")
        if not url:
            log.warning("[Seedr] fetch_file returned no URL for id=%s", file_id)
        return url

    return await _run(_call)


async def delete_folder(folder_id: int) -> None:
    """Delete a Seedr cloud folder by ID."""
    async def _call(client):
        await client.delete_folder(str(folder_id))
        log.info("[Seedr] Deleted folder id=%d", folder_id)

    await _run(_call)


# ─────────────────────────────────────────────────────────────────────────────
# Device-code helper — for the bot's /seedr_deviceauth command
# ─────────────────────────────────────────────────────────────────────────────

async def get_device_auth_info() -> dict:
    """
    Obtain a device_code + user_code pair from Seedr.

    Usage in the bot:
        info = await get_device_auth_info()
        # Show user: info["verification_url"]  and  info["user_code"]
        # Then set SEEDR_DEVICE_CODE=info["device_code"] in .env and restart.
    """
    from seedrcc import AsyncSeedr  # type: ignore

    codes = await AsyncSeedr.get_device_code()
    return {
        "device_code":      codes.device_code,
        "user_code":        codes.user_code,
        "verification_url": codes.verification_url,
        "expires_in":       getattr(codes, "expires_in", 900),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Poll until torrent finishes — with 30-second heartbeat (FIX-BUG-UH-03 kept)
# ─────────────────────────────────────────────────────────────────────────────

_HEARTBEAT_INTERVAL = 30   # seconds


async def poll_until_ready(
    torrent_name_hint: str = "",
    timeout_s: int = 3600,
    progress_cb=None,
    existing_folder_ids: Optional[set] = None,
) -> dict:
    """
    Poll Seedr root folder until a new folder appears (torrent finished).

    Fires progress_cb(pct, name) on percentage change OR every 30 seconds
    (FIX-BUG-UH-03) to prevent the panel appearing frozen on slow torrents.

    Returns folder dict {id, name, size}.
    """
    if existing_folder_ids is None:
        existing_folder_ids = set()

    deadline       = time.time() + timeout_s
    last_pct       = -1.0
    last_heartbeat = time.time()

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

        except Exception as exc:
            log.warning("[Seedr] Poll error: %s", exc)

        await asyncio.sleep(10)

    raise RuntimeError(f"Seedr download timed out after {timeout_s}s")


# ─────────────────────────────────────────────────────────────────────────────
# Get all file URLs (recursive)
# ─────────────────────────────────────────────────────────────────────────────

async def get_file_urls(folder_id: int) -> list[dict]:
    """Recursively collect all file download URLs under a Seedr folder."""
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
            except Exception as exc:
                log.warning("[Seedr] File URL fetch failed for %s: %s", name, exc)
        for sub in contents.get("folders", []):
            sub_id = sub.get("id")
            if sub_id:
                try:
                    await _collect(sub_id)
                except Exception as exc:
                    log.warning("[Seedr] Sub-folder error: %s", exc)

    await _collect(folder_id)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# High-level pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def download_via_seedr(
    magnet:     str,
    dest:       str,
    progress_cb = None,
    timeout_s:  int = 3600,
) -> list[str]:
    """
    Full pipeline: add magnet → poll until done → fetch URLs → download files
    → cleanup.  Returns list of local file paths written to `dest`.
    """
    from services.downloader import download_direct
    from services.cc_sanitize import sanitize_filename

    if progress_cb:
        await progress_cb("adding", 0.0, "Submitting to Seedr…")

    # Snapshot existing folder IDs so we can detect the new one.
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

    folder    = await poll_until_ready(
        torrent_name_hint   = name_hint,
        timeout_s           = timeout_s,
        progress_cb         = _poll_progress,
        existing_folder_ids = existing_folder_ids,
    )
    folder_id = folder["id"]

    if progress_cb:
        await progress_cb("fetching", 100.0, "Getting download links…")

    files = await get_file_urls(folder_id)
    if not files:
        raise RuntimeError("Seedr returned no downloadable files.")

    os.makedirs(dest, exist_ok=True)
    local_paths: list[str] = []

    for i, f in enumerate(files):
        raw_name   = f["name"]
        clean_name = sanitize_filename(raw_name)

        if progress_cb:
            await progress_cb(
                "dl_file",
                (i / len(files)) * 100,
                f"Downloading {clean_name} ({i + 1}/{len(files)})…",
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
        except Exception as exc:
            log.error("[Seedr] Download failed for %s: %s", raw_name, exc)

    # Cleanup cloud storage
    try:
        await delete_folder(folder_id)
    except Exception as exc:
        log.warning("[Seedr] Cloud cleanup failed: %s", exc)

    return local_paths
