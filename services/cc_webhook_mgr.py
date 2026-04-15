"""
services/cc_webhook_mgr.py
CloudConvert webhook subscription management.

WHY THIS EXISTS
───────────────
Every Colab restart creates a new Cloudflare tunnel with a different public
URL.  Any CloudConvert webhook subscription registered in a previous session
still points to the dead old URL — so completed jobs silently fail to notify
the bot.

This module, called once at tunnel-ready time, atomically:
  1. Lists ALL existing webhook subscriptions for every configured CC API key.
  2. Deletes them all (stale URLs are useless and clutter the CC dashboard).
  3. Registers a fresh subscription pointing to the new /webhook endpoint.

API REFERENCE (CloudConvert v2)
────────────────────────────────
  GET    https://api.cloudconvert.com/v2/webhooks
  POST   https://api.cloudconvert.com/v2/webhooks
  DELETE https://api.cloudconvert.com/v2/webhooks/{id}

USAGE
─────
    from services.cc_webhook_mgr import sync_cc_webhooks

    # Called once when the Cloudflare tunnel URL is confirmed:
    results = await sync_cc_webhooks("https://abc.trycloudflare.com")

    # With explicit key list (overrides env):
    results = await sync_cc_webhooks(tunnel_url, api_keys=["key1", "key2"])
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_CC_API   = "https://api.cloudconvert.com/v2"
_TIMEOUT  = aiohttp.ClientTimeout(total=30)

# Events we care about — job finished or failed covers everything
_CC_EVENTS = ["job.finished", "job.failed"]


# ─────────────────────────────────────────────────────────────
# Low-level API calls
# ─────────────────────────────────────────────────────────────

def _hdr(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


async def list_cc_webhooks(api_key: str) -> list[dict]:
    """
    Return all webhook subscriptions registered for this CC API key.
    Each dict contains at least: {"id": "...", "url": "...", "events": [...]}
    """
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        async with sess.get(f"{_CC_API}/webhooks", headers=_hdr(api_key)) as resp:
            if resp.status == 401:
                raise PermissionError(f"CloudConvert API key invalid (401)")
            data = await resp.json()

    items = data.get("data") or data
    if isinstance(items, dict):
        items = items.get("data", [])
    return items if isinstance(items, list) else []


async def delete_cc_webhook(api_key: str, webhook_id: str) -> bool:
    """
    Delete a single CloudConvert webhook subscription by ID.
    Returns True on success.
    """
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        async with sess.delete(
            f"{_CC_API}/webhooks/{webhook_id}",
            headers=_hdr(api_key),
        ) as resp:
            if resp.status in (200, 204):
                log.debug("[CC-WH] Deleted webhook %s", webhook_id)
                return True
            body = await resp.text()
            log.warning("[CC-WH] Delete %s returned %d: %s",
                        webhook_id, resp.status, body[:120])
            return False


async def create_cc_webhook(
    api_key: str,
    url: str,
    events: list[str] = _CC_EVENTS,
) -> dict:
    """
    Register a new CloudConvert webhook subscription.
    Returns the created webhook dict (contains 'id', 'url', 'events').
    """
    payload = {"url": url, "events": events}
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        async with sess.post(
            f"{_CC_API}/webhooks",
            json=payload,
            headers=_hdr(api_key),
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                msg = (data.get("message") or
                       data.get("data", {}).get("message") or
                       str(data))
                raise RuntimeError(
                    f"CloudConvert webhook creation failed ({resp.status}): {msg}"
                )

    wh = data.get("data") or data
    log.info("[CC-WH] Registered webhook id=%s → %s", wh.get("id", "?"), url)
    return wh


# ─────────────────────────────────────────────────────────────
# High-level: clean-and-register
# ─────────────────────────────────────────────────────────────

async def _sync_one_key(api_key: str, new_webhook_url: str) -> dict:
    """
    For a single CC API key:
      1. List all existing webhook subscriptions.
      2. Delete every one of them.
      3. Register a fresh subscription pointing to new_webhook_url.

    Returns a result dict:
      {"key_tail": "...abc", "deleted": N, "registered": id | None, "error": ""}
    """
    tail = api_key[-6:]
    result = {"key_tail": f"...{tail}", "deleted": 0, "registered": None, "error": ""}

    try:
        # ── Step 1: list ─────────────────────────────────────
        existing = await list_cc_webhooks(api_key)
        log.info("[CC-WH] Key ...%s: found %d existing webhook(s)", tail, len(existing))

        # ── Step 2: delete all ───────────────────────────────
        deleted = 0
        for wh in existing:
            wh_id = wh.get("id") or wh.get("data", {}).get("id")
            if not wh_id:
                continue
            wh_url = wh.get("url", "?")
            log.info("[CC-WH] Deleting webhook id=%s  url=%s", wh_id, wh_url[:60])
            ok = await delete_cc_webhook(api_key, str(wh_id))
            if ok:
                deleted += 1
            await asyncio.sleep(0.3)   # gentle rate-limiting

        result["deleted"] = deleted

        # ── Step 3: register fresh ───────────────────────────
        wh = await create_cc_webhook(api_key, new_webhook_url)
        result["registered"] = wh.get("id", "?")

    except PermissionError as exc:
        result["error"] = str(exc)
        log.error("[CC-WH] Key ...%s invalid: %s", tail, exc)
    except Exception as exc:
        result["error"] = str(exc)[:120]
        log.error("[CC-WH] Key ...%s error: %s", tail, exc, exc_info=True)

    return result


async def sync_cc_webhooks(
    tunnel_base_url:  str,
    api_keys:         Optional[list[str]] = None,
    webhook_path:     str = "/webhook",
) -> list[dict]:
    """
    Clean up all stale CloudConvert webhook subscriptions and register
    a fresh one per API key, pointing to the new tunnel URL.

    Args:
        tunnel_base_url:  e.g. "https://abc123.trycloudflare.com"
        api_keys:         list of CC API keys to process.
                          If None, reads CC_API_KEY from environment.
        webhook_path:     path appended to tunnel_base_url. Default "/webhook".

    Returns:
        List of per-key result dicts (see _sync_one_key).

    Example:
        results = await sync_cc_webhooks("https://abc.trycloudflare.com")
        for r in results:
            print(r["key_tail"], "deleted:", r["deleted"], "new id:", r["registered"])
    """
    new_url = tunnel_base_url.rstrip("/") + webhook_path

    if api_keys is None:
        raw = os.environ.get("CC_API_KEY", "").strip()
        if not raw:
            log.info("[CC-WH] CC_API_KEY not set — skipping CC webhook sync")
            return []
        # parse comma/newline separated keys (same as parse_api_keys)
        import re as _re
        api_keys = [k.strip() for k in _re.split(r"[,\s\n]+", raw) if k.strip()]

    if not api_keys:
        return []

    log.info("[CC-WH] Syncing %d CC key(s) → %s", len(api_keys), new_url)

    tasks   = [_sync_one_key(k, new_url) for k in api_keys]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    # Summary log
    total_del = sum(r["deleted"] for r in results)
    total_ok  = sum(1 for r in results if r.get("registered"))
    total_err = sum(1 for r in results if r.get("error"))
    log.info(
        "[CC-WH] Sync complete: %d deleted, %d registered, %d errors",
        total_del, total_ok, total_err,
    )

    return results
