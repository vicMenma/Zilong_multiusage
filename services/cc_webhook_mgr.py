"""
services/cc_webhook_mgr.py  —  v3 (Colab-optimised persistent webhook)

WHY THIS MATTERS ON COLAB
─────────────────────────
Every Colab restart opens a NEW Cloudflare tunnel with a NEW URL.
Old code always ran: list ALL webhooks (up to 20 paginated API calls) +
delete each one + create new = many API calls and accumulating stale entries.

NEW BEHAVIOUR
─────────────
data/cc_webhooks.json  stores  { "<key_tail>": {"id": "...", "url": "..."} }

On each restart:

  Case 1 — Same URL (EC2 / VPS with fixed WEBHOOK_BASE_URL):
    Verify the stored webhook still exists → REUSE it.
    Zero extra API calls.

  Case 2 — New URL (Colab / dynamic tunnel) + have a stored ID:
    DELETE just that one webhook (1 API call) + CREATE new (1 API call).
    Store new ID.  = 2 API calls total, no accumulation.

  Case 3 — First ever run (no stored entry):
    Full paginated cleanup of any stale webhooks + CREATE new.
    Store ID for next time.

Previously fixed (v2):
  CC-WH-PAG: list_cc_webhooks() paginates through ALL pages.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_CC_API    = "https://api.cloudconvert.com/v2"
_TIMEOUT   = aiohttp.ClientTimeout(total=30)
_CC_EVENTS = ["job.finished", "job.failed"]

_MAX_PAGES  = 20
_PAGE_SIZE  = 100
_DELETE_GAP = 0.15

# ── Persistent store ──────────────────────────────────────────────────────────
_WEBHOOK_STORE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "cc_webhooks.json")
)


def _load_webhook_store() -> dict:
    """Load  { key_tail: {id, url} }  from disk.  Returns {} on any error."""
    try:
        with open(_WEBHOOK_STORE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        log.warning("[CC-WH] Store load: %s", exc)
        return {}


def _save_webhook_store(store: dict) -> None:
    try:
        os.makedirs(os.path.dirname(_WEBHOOK_STORE_PATH), exist_ok=True)
        with open(_WEBHOOK_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump(store, f, indent=2)
    except Exception as exc:
        log.warning("[CC-WH] Store save: %s", exc)


async def _verify_webhook(api_key: str, webhook_id: str, expected_url: str) -> bool:
    """
    Return True if webhook_id still exists on CC and points to expected_url.
    Returns False on 404, network error, or URL mismatch — never raises.
    """
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
            async with sess.get(
                f"{_CC_API}/webhooks/{webhook_id}",
                headers=_hdr(api_key),
            ) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
        wh  = data.get("data") or data
        got = (wh.get("url") or "").rstrip("/")
        exp = expected_url.rstrip("/")
        return got == exp
    except Exception as exc:
        log.debug("[CC-WH] Verify %s: %s", webhook_id, exc)
        return False


def _hdr(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


# ── CC API helpers ────────────────────────────────────────────────────────────

async def list_cc_webhooks(api_key: str) -> list[dict]:
    """Return ALL webhook subscriptions, paginating through every page."""
    all_items: list[dict] = []
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        for page in range(1, _MAX_PAGES + 1):
            url = f"{_CC_API}/webhooks?page={page}&per_page={_PAGE_SIZE}"
            async with sess.get(url, headers=_hdr(api_key)) as resp:
                if resp.status == 401:
                    raise PermissionError("CloudConvert API key invalid (401)")
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("[CC-WH] List page %d → %d: %s",
                                page, resp.status, body[:120])
                    break
                data = await resp.json()

            items = data.get("data") if isinstance(data, dict) else data
            if isinstance(items, dict) and "data" in items:
                items = items["data"]
            if not isinstance(items, list) or not items:
                break
            all_items.extend(items)
            if len(items) < _PAGE_SIZE:
                break

    log.info("[CC-WH] Key ...%s: %d webhook(s) on account",
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
            f"{_CC_API}/webhooks", json=payload, headers=_hdr(api_key),
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                msg = (data.get("message") or
                       (data.get("data") or {}).get("message") or
                       str(data))
                raise RuntimeError(
                    f"CC webhook create failed ({resp.status}): {msg}"
                )

    wh = data.get("data") or data
    log.info("[CC-WH] Registered id=%s → %s", wh.get("id", "?"), url)
    return wh


# ── Per-key sync (Colab-optimised) ───────────────────────────────────────────

async def _sync_one_key(api_key: str, new_webhook_url: str) -> dict:
    tail   = api_key[-6:]
    result = {
        "key_tail":   f"...{tail}",
        "deleted":    0,
        "registered": None,
        "reused":     False,
        "error":      "",
    }

    store = _load_webhook_store()
    entry = store.get(tail, {})

    # ── Case 1: URL unchanged → verify and reuse (EC2 / same Cloudflare URL) ──
    if entry.get("id") and \
       entry.get("url", "").rstrip("/") == new_webhook_url.rstrip("/"):
        if await _verify_webhook(api_key, entry["id"], new_webhook_url):
            result["registered"] = entry["id"]
            result["reused"]     = True
            log.info("[CC-WH] Key ...%s: ✅ reusing webhook %s (0 API calls)",
                     tail, entry["id"])
            return result
        # Stored ID is gone from CC — fall through to recreate
        log.info("[CC-WH] Key ...%s: stored webhook gone — will recreate", tail)

    # ── Case 2: URL changed but we know the old ID → targeted delete (Colab) ──
    if entry.get("id") and \
       entry.get("url", "").rstrip("/") != new_webhook_url.rstrip("/"):
        try:
            ok = await delete_cc_webhook(api_key, entry["id"])
            if ok:
                result["deleted"] = 1
                log.info("[CC-WH] Key ...%s: deleted old webhook %s (1 API call)",
                         tail, entry["id"])
            # Mark entry as cleared so we skip full cleanup below
            entry = {"_targeted_delete_done": True}
        except Exception as exc:
            log.debug("[CC-WH] Targeted delete failed (%s) — "
                      "falling back to full cleanup", exc)
            entry = {}   # force full cleanup

    # ── Case 3: No stored entry (first ever run) → full paginated cleanup ─────
    if not entry:
        try:
            existing = await list_cc_webhooks(api_key)
            log.info("[CC-WH] Key ...%s: first run — %d webhook(s) to clean",
                     tail, len(existing))
            deleted = 0
            for wh in existing:
                wh_id = wh.get("id") or (wh.get("data") or {}).get("id")
                if not wh_id:
                    continue
                if await delete_cc_webhook(api_key, str(wh_id)):
                    deleted += 1
                await asyncio.sleep(_DELETE_GAP)
            result["deleted"] = deleted
        except PermissionError as exc:
            result["error"] = str(exc)
            log.error("[CC-WH] Key ...%s invalid: %s", tail, exc)
            return result
        except Exception as exc:
            result["error"] = str(exc)[:120]
            log.error("[CC-WH] Key ...%s cleanup error: %s", tail, exc)
            return result

    # ── Create fresh webhook + persist the ID ─────────────────────────────────
    try:
        wh    = await create_cc_webhook(api_key, new_webhook_url)
        wh_id = str(wh.get("id", "?"))
        result["registered"] = wh_id
        store[tail] = {"id": wh_id, "url": new_webhook_url}
        _save_webhook_store(store)
        log.info("[CC-WH] Key ...%s: new webhook %s stored", tail, wh_id)
    except Exception as exc:
        result["error"] = str(exc)[:120]
        log.error("[CC-WH] Key ...%s create error: %s", tail, exc)

    return result


# ── Public entry point ────────────────────────────────────────────────────────

async def sync_cc_webhooks(
    tunnel_base_url:  str,
    api_keys:         Optional[list[str]] = None,
    webhook_path:     str = "/webhook/cloudconvert",
) -> list[dict]:
    """
    Ensure each CC API key has exactly one webhook pointing at
    tunnel_base_url + webhook_path, using the persistent store to
    minimise API calls on Colab restarts.
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

    log.info("[CC-WH] Syncing %d key(s) → %s", len(api_keys), new_url)

    results = await asyncio.gather(
        *[_sync_one_key(k, new_url) for k in api_keys],
        return_exceptions=False,
    )

    reused    = sum(1 for r in results if r.get("reused"))
    total_del = sum(r["deleted"] for r in results)
    total_new = sum(1 for r in results if r.get("registered") and not r.get("reused"))
    total_err = sum(1 for r in results if r.get("error"))

    if reused:
        log.info("[CC-WH] %d reused · %d new · %d purged · %d errors",
                 reused, total_new, total_del, total_err)
    else:
        log.info("[CC-WH] %d new · %d purged · %d errors",
                 total_new, total_del, total_err)

    return list(results)
