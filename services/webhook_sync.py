"""
services/webhook_sync.py
Webhook synchronisation — called once when the Cloudflare tunnel comes up.

FIXES IN THIS VERSION
─────────────────────
FIX BUG-WS-01: _poll_cc_pending() called `await cc_job_store.list_processing()`
  which does not exist on CCJobStore. Correct: sync `cc_job_store.active_jobs()`.

FIX BUG-WS-02: _poll_cc_pending() imported from `plugins.webhook` (no such
  module). Correct: `services.cloudconvert_hook`.

FIX BUG-WS-03: error status stored as "failed" instead of "error", inconsistent
  with CCJob.status enum ("processing" | "finished" | "error").

FIX BUG-WS-04 (CRITICAL — why webhook delivery never worked):
  on_tunnel_ready() had cc_path="/webhook" as the default.
  The actual CloudConvert webhook handler in cloudconvert_hook.py is mounted at
  /webhook/cloudconvert.  So every CC webhook subscription was pointing at
  {tunnel}/webhook (404) instead of {tunnel}/webhook/cloudconvert.
  CC would POST to the wrong URL → bot receives nothing → jobs only processed
  via the slow 5-second ccstatus poller, never via instant webhook.
  Fix: changed default to cc_path="/webhook/cloudconvert".
"""
from __future__ import annotations

import asyncio
import logging
import os

log = logging.getLogger(__name__)


async def on_tunnel_ready(
    tunnel_url: str,
    *,
    notify_uid: int | None = None,
    # FIX BUG-WS-04: was "/webhook" — actual route is "/webhook/cloudconvert"
    cc_path:    str        = "/webhook/cloudconvert",
) -> dict:
    """
    Sync CloudConvert webhook subscriptions for the new tunnel URL.
    FreeConvert is NOT synced here — FC webhooks are per-job.
    """
    if not tunnel_url:
        log.warning("[WH-Sync] tunnel_url is empty — skipping")
        return {"tunnel": "", "cc": []}

    tunnel_url = tunnel_url.rstrip("/")
    log.info("[WH-Sync] Syncing CC webhooks → %s%s", tunnel_url, cc_path)

    from services.cc_webhook_mgr import sync_cc_webhooks
    cc_results = await sync_cc_webhooks(tunnel_url, webhook_path=cc_path)

    # Update cfg so FC submit_*() calls use the new URL
    try:
        from core.config import set_tunnel_url
        set_tunnel_url(tunnel_url)
        log.info("[WH-Sync] tunnel_url set → %s", tunnel_url)
    except Exception as exc:
        log.debug("[WH-Sync] cfg update: %s", exc)

    uid = notify_uid or _admin_uid()
    if uid:
        asyncio.create_task(_notify(uid, tunnel_url, cc_results, cc_path))

    return {"tunnel": tunnel_url, "cc": cc_results}


async def on_tunnel_reconnected(new_url: str) -> None:
    """Convenience alias for tunnel watchdog reconnect events."""
    await on_tunnel_ready(new_url)


async def poll_pending_jobs() -> None:
    """Poll CC and FC for jobs that completed while the bot was offline."""
    await asyncio.gather(
        _poll_cc_pending(),
        _poll_fc_pending(),
        return_exceptions=True,
    )


async def _poll_cc_pending() -> None:
    """Poll CloudConvert for jobs that finished while bot was offline."""
    try:
        from services.cc_job_store import cc_job_store

        # FIX BUG-WS-01: active_jobs() is sync — no await
        pending = cc_job_store.active_jobs()
        if not pending:
            return
        log.info("[WH-Sync] %d pending CC job(s) to poll", len(pending))

        import aiohttp as _ah
        api_key = os.environ.get("CC_API_KEY", "").strip()
        if not api_key:
            log.warning("[WH-Sync] CC_API_KEY not set — cannot poll pending jobs")
            return

        for job in pending:
            try:
                async with _ah.ClientSession(timeout=_ah.ClientTimeout(total=10)) as sess:
                    async with sess.get(
                        f"https://api.cloudconvert.com/v2/jobs/{job.job_id}",
                        headers={"Authorization": f"Bearer {api_key}"},
                    ) as resp:
                        data = await resp.json()

                jdata  = data.get("data") or data
                status = jdata.get("status", "")

                if status == "finished":
                    log.info("[WH-Sync] CC job %s finished offline — recovering", job.job_id)
                    try:
                        # FIX BUG-WS-02: correct module (not plugins.webhook)
                        from services.cloudconvert_hook import _handle_cc_job
                        await _handle_cc_job(job.job_id, jdata, api_key)
                    except Exception as e:
                        log.warning("[WH-Sync] _handle_cc_job failed: %s", e)

                elif status == "error":
                    # FIX BUG-WS-03: "error" matches CCJob enum, not "failed"
                    await cc_job_store.update(job.job_id, status="error")

                else:
                    log.debug("[WH-Sync] CC job %s still %s", job.job_id, status)

            except Exception as exc:
                log.warning("[WH-Sync] CC poll %s: %s", job.job_id, exc)

    except Exception as exc:
        log.error("[WH-Sync] _poll_cc_pending: %s", exc)


async def _poll_fc_pending() -> None:
    """Poll FreeConvert for jobs whose webhook URL pointed at the old dead tunnel."""
    try:
        from services.fc_job_store import fc_job_store
        pending = await fc_job_store.list_processing()
        if not pending:
            return
        log.info("[WH-Sync] %d pending FC job(s) to poll", len(pending))

        import aiohttp as _ah

        for job in pending:
            api_key = job.api_key or os.environ.get("FC_API_KEY", "").strip()
            if not api_key:
                log.warning("[WH-Sync] No FC key for job %s — skip", job.job_id)
                continue
            try:
                async with _ah.ClientSession(timeout=_ah.ClientTimeout(total=15)) as sess:
                    async with sess.get(
                        f"https://api.freeconvert.com/v1/process/jobs/{job.job_id}",
                        headers={"Authorization": f"Bearer {api_key}"},
                    ) as resp:
                        data = await resp.json()

                jdata  = data.get("data") or data
                status = (jdata.get("status") or "").lower()

                if status == "completed":
                    log.info("[WH-Sync] FC job %s completed — recovering", job.job_id)
                    from plugins.fc_webhook import _handle_completion
                    await _handle_completion(job, jdata)

                elif status in ("failed", "error", "cancelled"):
                    err = jdata.get("message", status)
                    await fc_job_store.update(job.job_id, status="failed", error=err[:200])
                    log.warning("[WH-Sync] FC job %s: %s", job.job_id, err)

                else:
                    log.info("[WH-Sync] FC job %s still %s", job.job_id, status)

            except Exception as exc:
                log.warning("[WH-Sync] FC poll %s: %s", job.job_id, exc)

    except Exception as exc:
        log.error("[WH-Sync] _poll_fc_pending: %s", exc)


def _admin_uid() -> int | None:
    raw = os.environ.get("ADMIN_ID", "").strip()
    try:
        return int(raw) if raw else None
    except ValueError:
        return None


async def _notify(
    uid: int,
    tunnel_url: str,
    cc: list,
    cc_path: str = "/webhook/cloudconvert",
) -> None:
    try:
        from core.session import get_client
        from pyrogram import enums
        client = get_client()

        lines = []
        for r in cc:
            tail = r.get("key_tail", "?")
            d    = r.get("deleted", 0)
            reg  = r.get("registered")
            err  = r.get("error", "")
            if err:
                lines.append(f"  {tail}  ❌ {err[:50]}")
            else:
                lines.append(f"  {tail}  {d} deleted · ✅ <code>{str(reg)[:12]}</code>")

        cc_section   = "\n".join(lines) if lines else "  (no CC keys configured)"
        webhook_full = f"{tunnel_url}{cc_path}"

        text = (
            "✅ <b>Webhook sync complete</b>\n"
            "──────────────────────\n\n"
            f"🔗 CC endpoint:\n<code>{webhook_full}</code>\n\n"
            f"☁️ <b>CloudConvert</b>\n{cc_section}\n\n"
            "🆓 <b>FreeConvert</b>\n"
            "  Per-job — no registration needed ✅\n"
            "  <i>(URL embedded at job submission time)</i>"
        )
        await client.send_message(
            uid, text,
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as exc:
        log.debug("[WH-Sync] Telegram notify failed: %s", exc)
