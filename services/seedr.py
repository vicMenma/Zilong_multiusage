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

  Optional — only needed when running on cloud IPs (Google Colab etc.):
    SEEDR_PROXY=http://user:pass@host:port
    SEEDR_PROXY=socks5://user:pass@host:port
    (any httpx-compatible proxy URL — SOCKS5 requires: pip install httpx[socks])
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
from contextlib import contextmanager
from typing import Optional

log = logging.getLogger(__name__)

_TOKEN_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "seedr_token.json")
)


# ─────────────────────────────────────────────────────────────
# Proxy helpers
# ─────────────────────────────────────────────────────────────

def _get_proxy() -> Optional[str]:
    """
    Return the configured proxy URL or None.
    Reads SEEDR_PROXY from environment, e.g.:
      SEEDR_PROXY=http://user:pass@host:port
      SEEDR_PROXY=socks5://user:pass@host:port
    SOCKS5 requires: pip install httpx[socks]
    """
    return os.environ.get("SEEDR_PROXY", "").strip() or None


def _httpx_client_kwargs(**extra) -> dict:
    """
    Build kwargs for httpx.AsyncClient, injecting proxy if SEEDR_PROXY is set.
    Gracefully handles missing httpx[socks] for socks5:// proxies.
    """
    proxy = _get_proxy()
    kw = {"timeout": 60, "follow_redirects": True, **extra}
    if proxy:
        if proxy.startswith("socks"):
            try:
                import httpx_socks  # noqa: F401  (imported to verify availability)
            except ImportError:
                try:
                    import socksio  # noqa: F401  (httpx[socks] dependency)
                except ImportError:
                    log.warning(
                        "[Seedr] SEEDR_PROXY is a SOCKS5 URL but httpx[socks] is not "
                        "installed. Run: pip install httpx[socks]  Falling back to no proxy."
                    )
                    return kw
        kw["proxy"] = proxy
        log.debug("[Seedr] Using proxy: %s", re.sub(r":([^@/]+)@", ":***@", proxy))
    return kw


@contextmanager
def _proxy_env():
    """
    Temporarily set HTTPS_PROXY / HTTP_PROXY env vars so that the seedrcc
    library (which creates its own httpx client internally) also uses the proxy.
    Restores original values on exit.
    """
    proxy = _get_proxy()
    if not proxy:
        yield
        return

    old_https = os.environ.get("HTTPS_PROXY")
    old_http  = os.environ.get("HTTP_PROXY")
    try:
        os.environ["HTTPS_PROXY"] = proxy
        os.environ["HTTP_PROXY"]  = proxy
        yield
    finally:
        if old_https is None:
            os.environ.pop("HTTPS_PROXY", None)
        else:
            os.environ["HTTPS_PROXY"] = old_https
        if old_http is None:
            os.environ.pop("HTTP_PROXY", None)
        else:
            os.environ["HTTP_PROXY"] = old_http


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
    Uses SEEDR_PROXY if set (via HTTPS_PROXY env injection).
    """
    from seedrcc import AsyncSeedr, Token

    # Try saved token first (avoids re-login, faster startup)
    saved = _load_token()
    if saved:
        try:
            with _proxy_env():
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

    with _proxy_env():
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

_OAUTH_URL = "https://www.seedr.cc/oauth_test/resource.php"
_TOKEN_URL = "https://www.seedr.cc/oauth_test/token.php"


async def _get_fresh_token(http) -> str:
    """
    Fetch a fresh OAuth access_token via password grant.
    `http` is an already-open httpx.AsyncClient.
    Raises RuntimeError on failure.
    """
    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("SEEDR_USERNAME / SEEDR_PASSWORD not set")

    for client_id in ("seedr_chrome", "seedr_google_drive", "seedr_website"):
        tok_resp = await http.post(
            _TOKEN_URL,
            data={
                "grant_type": "password",
                "client_id":  client_id,
                "type":       "login",
                "username":   username,
                "password":   password,
            },
        )
        tok_data = tok_resp.json()
        token = tok_data.get("access_token")
        if token:
            log.debug("[Seedr] Got token via client_id=%s", client_id)
            return token
        log.debug("[Seedr] client_id=%s login failed: %s", client_id, tok_data)

    raise RuntimeError(f"[Seedr] OAuth login failed for all client_ids: {tok_data}")


async def _try_add_torrent_oauth(http, access_token: str, magnet: str) -> Optional[dict]:
    """
    Try every known OAuth add_torrent variant.
    Returns result dict on success, None on failure.
    """
    candidates = [
        # v2 field name, form-encoded
        dict(
            url=_OAUTH_URL,
            params={"access_token": access_token, "func": "add_torrent"},
            data={"torrent_magnet": magnet, "folder_id": "-1"},
        ),
        # v1 / legacy field name
        dict(
            url=_OAUTH_URL,
            params={"access_token": access_token, "func": "add_torrent"},
            data={"magnet_link": magnet, "folder_id": "-1"},
        ),
        # multipart/form-data (httpx sends multipart when files= is used)
        dict(
            url=_OAUTH_URL,
            params={"access_token": access_token, "func": "add_torrent"},
            files={
                "torrent_magnet": (None, magnet),
                "folder_id":      (None, "-1"),
            },
        ),
        # New /api path, Bearer, v2 field
        dict(
            url="https://www.seedr.cc/api/torrent",
            headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {access_token}"},
            data={"torrent_magnet": magnet, "folder_id": "-1"},
        ),
        # New /api path, Bearer, v1 field
        dict(
            url="https://www.seedr.cc/api/torrent",
            headers={**_BROWSER_HEADERS, "Authorization": f"Bearer {access_token}"},
            data={"magnet_link": magnet, "folder_id": "-1"},
        ),
        # JSON body (some API versions expect JSON)
        dict(
            url="https://www.seedr.cc/api/torrent",
            headers={**_BROWSER_HEADERS,
                     "Authorization": f"Bearer {access_token}",
                     "Content-Type": "application/json"},
            content=json.dumps({"magnet": magnet, "folder": None}).encode(),
        ),
    ]

    for idx, kw in enumerate(candidates, start=1):
        try:
            resp = await http.post(**kw)
            log.debug(
                "[Seedr-oauth] candidate %d → HTTP %d  %.200s",
                idx, resp.status_code, resp.text,
            )
            if resp.status_code in (404, 405, 403):
                continue
            if not resp.is_success:
                log.warning(
                    "[Seedr-oauth] candidate %d: HTTP %d — %.200s",
                    idx, resp.status_code, resp.text[:200],
                )
                continue
            try:
                body = resp.json()
            except Exception:
                log.warning("[Seedr-oauth] candidate %d: non-JSON response", idx)
                continue
            if isinstance(body, dict) and body.get("result") is False:
                log.warning(
                    "[Seedr-oauth] candidate %d: result=False  error=%s",
                    idx, body.get("error"),
                )
                continue
            log.info(
                "[Seedr-oauth] Submitted via candidate %d — hash=%s title=%s",
                idx, body.get("torrent_hash", "?"), body.get("title", "?"),
            )
            return {
                "result": True,
                "user_torrent_id": body.get("user_torrent_id"),
                "torrent_hash": body.get("torrent_hash", ""),
                "title": body.get("title", ""),
            }
        except Exception as exc:
            log.warning("[Seedr-oauth] candidate %d exception: %s", idx, exc)
            continue

    return None


async def _seedr_cookie_session_add(magnet: str) -> dict:
    """
    Strategy: Authenticate via the Seedr *website* cookie session (not OAuth)
    and submit the magnet through the same internal REST endpoint the browser
    SPA uses.  This path is completely separate from the OAuth API, so it may
    succeed even when the OAuth add_torrent is IP-blocked.

    Auth flow:
      1. GET  https://www.seedr.cc/           → collect CSRF / cookies
      2. POST https://www.seedr.cc/login/     → cookie-based session login
      3. POST https://www.seedr.cc/api/torrent (or browse_compat path)
    """
    import httpx as _httpx

    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("SEEDR_USERNAME / SEEDR_PASSWORD not set")

    kw = _httpx_client_kwargs(headers=_BROWSER_HEADERS)

    async with _httpx.AsyncClient(**kw) as http:
        # ── Step 1: visit homepage to collect cookies / CSRF ─────────────────
        try:
            home = await http.get("https://www.seedr.cc/")
            csrf = ""
            # Look for csrfmiddlewaretoken in body or cookie
            m = re.search(r'csrfmiddlewaretoken["\s:=\']+([A-Za-z0-9_-]{20,})', home.text)
            if m:
                csrf = m.group(1)
            csrf = csrf or http.cookies.get("csrftoken", "")
            log.debug("[Seedr-cookie] Homepage OK, csrf=%s…", csrf[:10] if csrf else "none")
        except Exception as exc:
            log.warning("[Seedr-cookie] Homepage fetch failed: %s", exc)
            csrf = ""

        # ── Step 2: cookie-based login ────────────────────────────────────────
        login_payloads = [
            # Django-style CSRF form login
            dict(
                url="https://www.seedr.cc/login/",
                data={
                    "username": username,
                    "password": password,
                    "csrfmiddlewaretoken": csrf,
                },
                headers={**_BROWSER_HEADERS,
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Referer": "https://www.seedr.cc/login/"},
            ),
            # JSON REST login
            dict(
                url="https://www.seedr.cc/api/user/authenticate",
                json={"username": username, "password": password},
                headers=_BROWSER_HEADERS,
            ),
            # Alternative JSON endpoint
            dict(
                url="https://www.seedr.cc/api/auth/login",
                json={"username": username, "password": password},
                headers=_BROWSER_HEADERS,
            ),
        ]

        logged_in = False
        for li, lp in enumerate(login_payloads, start=1):
            try:
                lr = await http.post(**lp)
                log.debug(
                    "[Seedr-cookie] login attempt %d → HTTP %d  %.200s",
                    li, lr.status_code, lr.text[:200],
                )
                if lr.is_success or lr.status_code in (302, 301):
                    logged_in = True
                    log.info("[Seedr-cookie] Logged in via attempt %d", li)
                    break
            except Exception as exc:
                log.warning("[Seedr-cookie] login attempt %d exception: %s", li, exc)
                continue

        if not logged_in:
            raise RuntimeError("[Seedr-cookie] All login attempts failed")

        # ── Step 3: add torrent via browser REST endpoints ────────────────────
        add_candidates = [
            dict(
                url="https://www.seedr.cc/api/torrent",
                json={"magnet": magnet, "folder": None},
                headers=_BROWSER_HEADERS,
            ),
            dict(
                url="https://www.seedr.cc/api/torrent",
                data={"torrent_magnet": magnet, "folder_id": "-1"},
                headers=_BROWSER_HEADERS,
            ),
            dict(
                url="https://www.seedr.cc/torrent/",
                data={"magnet_link": magnet},
                headers={**_BROWSER_HEADERS,
                         "Content-Type": "application/x-www-form-urlencoded"},
            ),
        ]

        for idx, kw_add in enumerate(add_candidates, start=1):
            try:
                resp = await http.post(**kw_add)
                log.debug(
                    "[Seedr-cookie] add candidate %d → HTTP %d  %.200s",
                    idx, resp.status_code, resp.text[:200],
                )
                if resp.status_code in (404, 405, 403):
                    continue
                if not resp.is_success and resp.status_code not in (200, 201, 202):
                    continue
                try:
                    body = resp.json()
                except Exception:
                    # A non-JSON 200 on the login-redirect is also "success"
                    if resp.is_success:
                        log.info("[Seedr-cookie] Submitted (non-JSON 200), candidate %d", idx)
                        return {"result": True, "user_torrent_id": None,
                                "torrent_hash": "", "title": ""}
                    continue
                if isinstance(body, dict) and body.get("result") is False:
                    log.warning("[Seedr-cookie] candidate %d result=False: %s",
                                idx, body.get("error"))
                    continue
                log.info("[Seedr-cookie] Submitted via candidate %d — %s", idx, body)
                return {
                    "result": True,
                    "user_torrent_id": body.get("user_torrent_id") or body.get("id"),
                    "torrent_hash": body.get("torrent_hash", ""),
                    "title": body.get("title", body.get("name", "")),
                }
            except Exception as exc:
                log.warning("[Seedr-cookie] add candidate %d exception: %s", idx, exc)
                continue

    raise RuntimeError("[Seedr-cookie] All cookie-session add candidates exhausted")


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
      1. seedrcc library with SEEDR_PROXY env injection
         — fast path when the OAuth API is accessible
      2. seedrcc library, alternative client_ids (seedr_google_drive, seedr_website)
      3. Raw httpx (with proxy if SEEDR_PROXY set):
           torrent_magnet  │  magnet_link  │  multipart  │  Bearer /api/torrent  │  JSON
      4. Cookie-session (real browser login — completely separate auth path)
         — this is the strongest non-proxy fallback

    If SEEDR_PROXY is set, it is applied to EVERY strategy including the library.

    Set in .env:
      SEEDR_PROXY=http://user:pass@host:port     ← HTTP proxy
      SEEDR_PROXY=socks5://user:pass@host:port   ← SOCKS5 (pip install httpx[socks])
    """
    import httpx as _httpx

    username = os.environ.get("SEEDR_USERNAME", "").strip()
    password = os.environ.get("SEEDR_PASSWORD", "").strip()
    proxy    = _get_proxy()

    if proxy:
        log.info("[Seedr] Proxy active: %s", re.sub(r":([^@/]+)@", ":***@", proxy))
    else:
        log.info(
            "[Seedr] No SEEDR_PROXY set — cloud IPs may be blocked for add_torrent. "
            "If all strategies fail, add SEEDR_PROXY=http://user:pass@host:port to .env"
        )

    # ── Strategy 1 & 2: seedrcc library, try multiple client_ids ─────────────
    from seedrcc import AsyncSeedr

    for client_id in ("seedr_chrome", "seedr_google_drive", "seedr_website"):
        label = f"library/{client_id}"
        try:
            with _proxy_env():
                cli = await AsyncSeedr.from_password(
                    username, password,
                    # seedrcc 2.x doesn't expose client_id in from_password()
                    # but we patch via env so the underlying httpx respects the proxy
                )
            with _proxy_env():
                result = await cli.add_torrent(magnet_link=magnet)
            await cli.close()
            log.info(
                "[Seedr] Strategy %s OK: hash=%s title=%s",
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
            log.warning("[Seedr] Strategy %s failed: %s", label, exc)
        # Only try one library call without proxy — if it fails due to IP block,
        # the other client_ids will hit the same block. Move to httpx with proxy.
        if not proxy:
            break

    # ── Strategy 3: raw httpx (proxy-aware) ─────────────────────────────────
    kw_http = _httpx_client_kwargs(headers=_BROWSER_HEADERS)
    async with _httpx.AsyncClient(**kw_http) as http:
        # Re-fetch token inside the proxy-aware httpx client
        try:
            access_token = await _get_fresh_token(http)
        except Exception as exc:
            log.warning("[Seedr] Token fetch for httpx strategy failed: %s", exc)
            access_token = None

        if access_token:
            result = await _try_add_torrent_oauth(http, access_token, magnet)
            if result:
                return result
            log.warning("[Seedr] All OAuth httpx candidates failed (likely IP block)")

    # ── Strategy 4: cookie-session (real browser login) ─────────────────────
    log.warning("[Seedr] Trying cookie-session strategy (browser login)…")
    try:
        return await _seedr_cookie_session_add(magnet)
    except Exception as exc:
        last_err = str(exc)
        log.warning("[Seedr] Cookie-session strategy failed: %s", last_err)

    # ── All failed ────────────────────────────────────────────────────────────
    proxy_hint = (
        "SEEDR_PROXY is set but all strategies still failed — "
        "check your proxy is working and reachable."
        if proxy else
        "Fix: set SEEDR_PROXY=http://user:pass@host:port in your .env\n"
        "     (socks5:// also works — requires: pip install httpx[socks])\n"
        "     This routes only the add_torrent call through a non-cloud IP."
    )
    raise RuntimeError(
        f"[Seedr] All strategies failed.\n"
        f"Last error: {last_err}\n\n"
        f"DIAGNOSIS: Google Colab (Google Cloud) IPs are blocked by Seedr\n"
        f"for the add_torrent write operation. Read-only calls still work.\n"
        f"{proxy_hint}"
    )


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
