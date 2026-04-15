"""
services/webhook_sync.py
Webhook synchronisation — called once when the Cloudflare tunnel comes up.

╔══════════════════════════════════════════════════════════════════════════╗
║  HOW EACH PLATFORM DELIVERS WEBHOOKS                                    ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  ☁️  CLOUDCONVERT — global subscription model                            ║
║  CC uses persistent webhook subscriptions at /v2/webhooks.               ║
║  A single subscription fires for ALL jobs on that API key.               ║
║  Subscriptions survive Colab restarts — stale entries point at the       ║
║  dead old tunnel URL.                                                    ║
║  → MUST delete old subscriptions and register a fresh one on start.     ║
║                                                                          ║
║  🆓  FREECONVERT — per-job webhook model                                 ║
║  FC embeds the webhook URL inside each job payload at submission time.   ║
║  There is NO global subscription to manage or clean up.                  ║
║  Every new job automatically gets the fresh URL because submit_*()       ║
║  reads cfg.tunnel_url at call time.                                      ║
║  → Nothing to register. Nothing to delete.                              ║
║                                                                          ║
║  WHAT ABOUT FC JOBS SUBMITTED BEFORE A CRASH?                            ║
║  Those jobs have the OLD URL baked in. FC will POST to it and get a      ║
║  502/timeout. poll_pending_jobs() rescues them by polling FC directly.  ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

HOW TO INTEGRATE
────────────────
In your tunnel watchdog, after the new URL is confirmed:

    from services.webhook_sync import on_tunnel_ready, poll_pending_jobs

    async def _after_tunnel_start(new_url: str):
        await on_tunnel_ready(new_url)
        await poll_pending_jobs()
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
    cc_path:    str        = "/webhook",
) -> dict:
    """
    Sync CloudConvert webhook subscriptions for the new tunnel URL.

    FreeConvert is intentionally NOT synced here — FC webhooks are
    per-job and the new URL is picked up automatically on the next
    job submission via cfg.tunnel_url.

    Args:
        tunnel_url:  New public URL, e.g. "https://abc.trycloudflare.com"
        notify_uid:  Telegram UID for summary message. Falls back to ADMIN_ID.
        cc_path:     Webhook path for CC. Default "/webhook".

    Returns:
        {"tunnel": str, "cc": list[dict]}
    """
    if not tunnel_url:
        log.warning("[WH-Sync] tunnel_url is empty — skipping")
        return {"tunnel": "", "cc": []}

    tunnel_url = tunnel_url.rstrip("/")
    log.info("[WH-Sync] Syncing CC webhooks → %s", tunnel_url)

    from services.cc_webhook_mgr import sync_cc_webhooks
    cc_results = await sync_cc_webhooks(tunnel_url, webhook_path=cc_path)

    # Update cfg so every subsequent FC submit_*() call uses the new URL
    try:
        from core.config import cfg
        cfg.tunnel_url = tunnel_url         # type: ignore[attr-defined]
        log.info("[WH-Sync] cfg.tunnel_url → %s", tunnel_url)
    except Exception as exc:
        log.debug("[WH-Sync] cfg update: %s", exc)

    uid = notify_uid or _admin_uid()
    if uid:
        asyncio.create_task(_notify(uid, tunnel_url, cc_results))

    return {"tunnel": tunnel_url, "cc": cc_results}


async def on_tunnel_reconnected(new_url: str) -> None:
    """Convenience alias for tunnel watchdog reconnect events."""
    await on_tunnel_ready(new_url)


async def poll_pending_jobs() -> None:
    """
    After a restart, poll both CC and FC for jobs that completed while
    the bot was offline.

    CC: jobs finished during offline window never delivered the webhook
        because the subscription pointed at the dead URL.
    FC: jobs have the OLD tunnel URL baked in; FC tried to POST to it
        and got no response. Direct poll recovers them.
    """
    await asyncio.gather(
        _poll_cc_pending(),
        _poll_fc_pending(),
        return_exceptions=True,
    )


async def _poll_cc_pending() -> None:
    try:
        from services.cc_job_store import cc_job_store
        pending = await cc_job_store.list_processing()
        if not pending:
            return
        log.info("[WH-Sync] %d pending CC job(s) to poll", len(pending))

        import aiohttp as _ah
        api_key = os.environ.get("CC_API_KEY", "").strip()
        if not api_key:
            log.warning("[WH-Sync] CC_API_KEY not set — cannot poll")
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
                    log.info("[WH-Sync] CC job %s finished — recovering", job.job_id)
                    try:
                        from plugins.webhook import _handle_cc_job
                        await _handle_cc_job(job.job_id, jdata, api_key)
                    except ImportError:
                        log.warning("[WH-Sync] plugins.webhook._handle_cc_job not importable")
                elif status == "error":
                    await cc_job_store.update(job.job_id, status="failed")

            except Exception as exc:
                log.warning("[WH-Sync] CC poll %s: %s", job.job_id, exc)

    except Exception as exc:
        log.error("[WH-Sync] _poll_cc_pending: %s", exc)


async def _poll_fc_pending() -> None:
    """
    Poll FreeConvert for jobs whose embedded webhook_url pointed at the
    old dead tunnel. FC already tried to deliver and got no response.
    We recover by calling GET /v1/process/jobs/{id} directly.
    """
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
                    # Still processing on FC side — new webhook when it finishes
                    # will go to the dead URL, but next poll_pending_jobs() will catch it
                    log.info("[WH-Sync] FC job %s still %s — will retry on next start", job.job_id, status)

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


async def _notify(uid: int, tunnel_url: str, cc: list) -> None:
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

        cc_section = "\n".join(lines) if lines else "  (no CC keys configured)"

        text = (
            "✅ <b>Webhook sync complete</b>\n"
            "──────────────────────\n\n"
            f"🔗 <code>{tunnel_url}</code>\n\n"
            f"☁️ <b>CloudConvert</b>\n{cc_section}\n\n"
            "🆓 <b>FreeConvert</b>\n"
            "  Per-job — no registration needed ✅\n"
            "  <i>(URL is embedded at job submission time)</i>"
        )
        await client.send_message(
            uid, text,
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as exc:
        log.debug("[WH-Sync] Telegram notify failed: %s", exc)
