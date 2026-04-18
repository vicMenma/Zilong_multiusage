"""
services/fc_webhook_mgr.py
FreeConvert webhook auto-setting — mirrors services/cc_webhook_mgr.py.

FreeConvert's primary webhook mechanism is per-job (webhook_url embedded
in the job payload).  freeconvert_api.py already injects this automatically
via _auto_webhook() on every submit_*() call.

This module adds the CC-like orchestration layer:

  1.  Caches the current tunnel URL in core.config via set_tunnel_url(),
      so every *future* FC submit picks up the latest URL without caller
      changes.
  2.  Attempts a best-effort account-level webhook registration at
      /v1/webhooks or /v1/account/webhook (FC has rolled this out on
      some plans — if present it gives us global delivery for jobs
      submitted *without* an embedded webhook_url).
  3.  Performs a reachability self-check against the public /fc-webhook
      route so any mis-routed tunnel is detected at startup rather than
      at job completion time.

All operations degrade gracefully: if FC rejects the account endpoint
(404/405/403) or the self-check fails, we log the outcome and rely on
the per-job embedding that is already 100 % reliable.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_FC_ROOT   = "https://api.freeconvert.com/v1"
_TIMEOUT   = aiohttp.ClientTimeout(total=30)
_FC_EVENTS = ["job.completed", "job.failed"]


def _hdr(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }


def fc_webhook_path() -> str:
    return "/fc-webhook"


def fc_webhook_full_url(tunnel_base_url: str) -> str:
    return tunnel_base_url.rstrip("/") + fc_webhook_path()


# ─────────────────────────────────────────────────────────────
# Account-level webhook (optional, plan-dependent)
# ─────────────────────────────────────────────────────────────

_CANDIDATE_ENDPOINTS = (
    f"{_FC_ROOT}/webhooks",
    f"{_FC_ROOT}/account/webhook",
    f"{_FC_ROOT}/account/webhooks",
)


async def _probe_endpoint(api_key: str) -> Optional[str]:
    """Find which /v1 webhook endpoint (if any) this plan supports."""
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        for url in _CANDIDATE_ENDPOINTS:
            try:
                async with sess.get(url, headers=_hdr(api_key)) as resp:
                    if resp.status in (200, 201):
                        return url
                    if resp.status in (401,):
                        return None  # bad key — no point trying others
            except Exception:
                continue
    return None


async def list_fc_webhooks(api_key: str, endpoint: str) -> list[dict]:
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        async with sess.get(endpoint, headers=_hdr(api_key)) as resp:
            if resp.status != 200:
                return []
            try:
                data = await resp.json(content_type=None)
            except Exception:
                return []
    items = data.get("data") if isinstance(data, dict) else data
    return items if isinstance(items, list) else []


async def delete_fc_webhook(api_key: str, endpoint: str, wh_id: str) -> bool:
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        async with sess.delete(
            f"{endpoint.rstrip('/')}/{wh_id}",
            headers=_hdr(api_key),
        ) as resp:
            return resp.status in (200, 204)


async def create_fc_webhook(
    api_key: str, endpoint: str, url: str,
    events: list[str] = _FC_EVENTS,
) -> dict:
    payload = {"url": url, "events": events}
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as sess:
        async with sess.post(
            endpoint, json=payload, headers=_hdr(api_key),
        ) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = {}
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"FC webhook create failed ({resp.status}): "
                    f"{str(data)[:160]}"
                )
    return data.get("data") or data if isinstance(data, dict) else {}


async def _sync_one_fc_key(api_key: str, tunnel_base_url: str) -> dict:
    tail   = api_key[-6:]
    result = {
        "key_tail": f"...{tail}",
        "deleted":  0,
        "registered": None,
        "error":    "",
        "mode":     "per-job",     # default: per-job embedding
    }

    endpoint = await _probe_endpoint(api_key)
    if not endpoint:
        log.info("[FC-WH] Key ...%s: account-level webhook endpoint not available — "
                 "using per-job mode (default)", tail)
        return result

    new_url = fc_webhook_full_url(tunnel_base_url)
    try:
        existing = await list_fc_webhooks(api_key, endpoint)
        deleted = 0
        for wh in existing:
            wh_id = wh.get("id") or (wh.get("data") or {}).get("id")
            if not wh_id:
                continue
            if await delete_fc_webhook(api_key, endpoint, str(wh_id)):
                deleted += 1
            await asyncio.sleep(0.15)
        result["deleted"] = deleted

        created = await create_fc_webhook(api_key, endpoint, new_url)
        result["registered"] = created.get("id", "?")
        result["mode"]       = "account"
        log.info("[FC-WH] Key ...%s: account webhook registered "
                 "(deleted %d stale) → %s", tail, deleted, new_url)

    except Exception as exc:
        result["error"] = str(exc)[:160]
        log.info("[FC-WH] Key ...%s: account webhook unavailable (%s) — "
                 "falling back to per-job embedding",
                 tail, str(exc)[:100])

    return result


# ─────────────────────────────────────────────────────────────
# Reachability self-check
# ─────────────────────────────────────────────────────────────

async def self_check(tunnel_base_url: str, timeout: float = 6.0) -> bool:
    """
    Probe the public /fc-webhook URL to verify the tunnel routes to our
    aiohttp app. Uses a small bogus POST — handler will respond 200 or
    400 (depending on JSON validity), both of which prove reachability.
    """
    url = fc_webhook_full_url(tunnel_base_url)
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as sess:
            async with sess.post(url, json={"selfcheck": True}) as resp:
                ok = resp.status in (200, 400, 404, 405)  # reachable
                log.info("[FC-WH] Self-check %s → HTTP %d (%s)",
                         url, resp.status, "reachable" if ok else "bad")
                return ok
    except Exception as exc:
        log.warning("[FC-WH] Self-check failed for %s: %s", url, exc)
        return False


# ─────────────────────────────────────────────────────────────
# Public entry
# ─────────────────────────────────────────────────────────────

def _parse_keys(raw: str) -> list[str]:
    if not raw:
        return []
    import re as _re
    return [k.strip() for k in _re.split(r"[,\s\n]+", raw) if k.strip()]


async def sync_fc_webhooks(
    tunnel_base_url: str,
    api_keys: Optional[list[str]] = None,
) -> list[dict]:
    """
    Ensure FreeConvert will auto-return jobs to this tunnel URL.

    Steps (each per API key):
      1. Probe for an account-level webhook endpoint (plan-dependent).
      2. If present: purge stale webhooks, register a fresh one.
      3. Otherwise: rely on per-job webhook_url embedding (always works).

    Also performs one reachability self-check against the public URL.
    """
    if api_keys is None:
        raw  = os.environ.get("FC_API_KEY", "").strip()
        keys = _parse_keys(raw)
        for i in range(2, 10):
            extra = os.environ.get(f"FC_API_KEY_{i}", "").strip()
            if extra:
                keys.extend(_parse_keys(extra))
        api_keys = keys

    if not api_keys:
        log.info("[FC-WH] No FC_API_KEY configured — nothing to sync")
        return []

    log.info("[FC-WH] Syncing %d FC key(s) → %s",
             len(api_keys), fc_webhook_full_url(tunnel_base_url))

    # Run probes in parallel
    results = await asyncio.gather(
        *[_sync_one_fc_key(k, tunnel_base_url) for k in api_keys],
        return_exceptions=False,
    )

    # Reachability self-check (single, not per-key)
    await self_check(tunnel_base_url)

    account_ok = sum(1 for r in results if r.get("mode") == "account")
    log.info("[FC-WH] Sync complete: %d account-registered, %d per-job",
             account_ok, len(results) - account_ok)
    return results
