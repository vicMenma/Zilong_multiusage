"""
services/cc_webhook_mgr.py  —  PATCHED v2
CloudConvert webhook subscription management.

WHAT CHANGED vs previous version
────────────────────────────────
FIX CC-WH-PAG: list_cc_webhooks() now paginates through EVERY page.
  Root cause of "webhooks accumulate on every Colab restart":
    CloudConvert /v2/webhooks returns 25 items per page by default.
    The old code only read page 1 → only the first 25 stale webhooks
    were ever deleted.  After a few Colab restarts you end up with
    hundreds of dead webhooks cluttering your dashboard.

  Fix:
    - Loop with ?page=N until we get an empty page or hit 20 pages
      (safety cap = 500 webhooks).
    - Use per_page=100 for efficiency (CC max is typically 100).
    - Also handles the alternate response shape where data is nested
      under "data" with "current_page" / "last_page" meta fields.

FIX CC-WH-DELAY: small 0.15 s gap between deletes (was 0.3 s) — faster
  cleanup for accounts with many stale webhooks.

FIX CC-WH-EARLY-TUNNEL (coordinated with config.py change):
  Caller (main.py / cloudconvert_hook.py) now calls set_tunnel_url()
  BEFORE opening the aiohttp server, so FreeConvert jobs submitted
  during early startup already have the correct webhook URL.
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
_CC_EVENTS = ["job.finished", "job.failed"]

_MAX_PAGES   = 20    # safety cap — up to 2000 webhooks listed
_PAGE_SIZE   = 100
_DELETE_GAP  = 0.15  # seconds between deletes


def _hdr(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


async def list_cc_webhooks(api_key: str) -> list[dict]:
    """
    Return ALL webhook subscriptions for this key, paginating through
    every page.  See FIX CC-WH-PAG above.
    """
    all_items: list[dict] = []
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        for page in range(1, _MAX_PAGES + 1):
            url = f"{_CC_API}/webhooks?page={page}&per_page={_PAGE_SIZE}"
            async with sess.get(url, headers=_hdr(api_key)) as resp:
                if resp.status == 401:
                    raise PermissionError("CloudConvert API key invalid (401)")
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("[CC-WH] List page %d returned %d: %s",
                                page, resp.status, body[:120])
                    break
                data = await resp.json()

            # Response shape handling:
            #   { "data": [ {...}, ... ], "meta": {...} }   ← paginated
            #   { "data": [ ... ] }                         ← unpaginated
            #   [ ... ]                                     ← bare array
            items = data.get("data") if isinstance(data, dict) else data
            if isinstance(items, dict) and "data" in items:
                items = items["data"]
            if not isinstance(items, list):
                break

            if not items:
                break   # empty page → we're done

            all_items.extend(items)

            # Stop early if response size < requested page size
            if len(items) < _PAGE_SIZE:
                break

    log.info("[CC-WH] Key ...%s: paginated list → %d total webhook(s)",
             api_key[-6:], len(all_items))
    return all_items


async def delete_cc_webhook(api_key: str, webhook_id: str) -> bool:
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        async with sess.delete(
            f"{_CC_API}/webhooks/{webhook_id}",
            headers=_hdr(api_key),
        ) as resp:
            if resp.status in (200, 204):
                return True
            body = await resp.text()
            log.warning("[CC-WH] Delete %s → %d: %s",
                        webhook_id, resp.status, body[:100])
            return False


async def create_cc_webhook(
    api_key: str,
    url: str,
    events: list[str] = _CC_EVENTS,
) -> dict:
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
                       (data.get("data") or {}).get("message") or
                       str(data))
                raise RuntimeError(f"CC webhook create failed ({resp.status}): {msg}")

    wh = data.get("data") or data
    log.info("[CC-WH] Registered webhook id=%s → %s", wh.get("id", "?"), url)
    return wh


async def _sync_one_key(api_key: str, new_webhook_url: str) -> dict:
    tail = api_key[-6:]
    result = {"key_tail": f"...{tail}", "deleted": 0, "registered": None, "error": ""}

    try:
        existing = await list_cc_webhooks(api_key)
        log.info("[CC-WH] Key ...%s: found %d existing webhook(s) across all pages",
                 tail, len(existing))

        deleted = 0
        for wh in existing:
            wh_id = wh.get("id") or (wh.get("data") or {}).get("id")
            if not wh_id:
                continue
            ok = await delete_cc_webhook(api_key, str(wh_id))
            if ok:
                deleted += 1
            await asyncio.sleep(_DELETE_GAP)

        result["deleted"] = deleted
        log.info("[CC-WH] Key ...%s: %d/%d webhook(s) deleted",
                 tail, deleted, len(existing))

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
    webhook_path:     str = "/webhook/cloudconvert",
) -> list[dict]:
    """
    Clean up ALL stale CloudConvert webhook subscriptions (paginated),
    then register a fresh one per API key pointing at tunnel_base_url.
    """
    new_url = tunnel_base_url.rstrip("/") + webhook_path

    if api_keys is None:
        raw = os.environ.get("CC_API_KEY", "").strip()
        if not raw:
            log.info("[CC-WH] CC_API_KEY not set — skipping sync")
            return []
        import re as _re
        api_keys = [k.strip() for k in _re.split(r"[,\s\n]+", raw) if k.strip()]

    if not api_keys:
        return []

    log.info("[CC-WH] Syncing %d CC key(s) → %s", len(api_keys), new_url)

    tasks   = [_sync_one_key(k, new_url) for k in api_keys]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    total_del = sum(r["deleted"] for r in results)
    total_ok  = sum(1 for r in results if r.get("registered"))
    total_err = sum(1 for r in results if r.get("error"))
    log.info(
        "[CC-WH] Sync complete: %d stale webhooks purged, %d new registered, %d errors",
        total_del, total_ok, total_err,
    )

    return results
