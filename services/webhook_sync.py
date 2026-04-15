"""
services/webhook_sync.py
Webhook synchronisation — called once when the Cloudflare tunnel comes up.

PROBLEM
───────
Every Colab restart creates a new Cloudflare tunnel with a different public
URL.  Any webhook subscription registered in the previous session still points
to the now-dead old URL.  When CloudConvert or FreeConvert complete a job they
POST to that URL, get a connection-refused, and the bot never knows the job
finished.

SOLUTION
────────
After the tunnel URL is established (or changes), call:

    await on_tunnel_ready(new_tunnel_url)

This will, for every configured API key on both platforms:
  1. List all existing webhook subscriptions.
  2. Delete them all.
  3. Register a single fresh subscription pointing to the new URL.

HOW TO INTEGRATE
────────────────
In your tunnel watchdog (wherever you restart cloudflared and get the new URL):

    from services.webhook_sync import on_tunnel_ready

    async def _after_tunnel_start(new_url: str):
        await on_tunnel_ready(new_url)

Typically this lives in services/tunnel.py or core/server.py, after the line
that confirms the tunnel URL.

WHAT IS REGISTERED
──────────────────
CloudConvert:
  → https://{tunnel}/webhook        (handled by plugins/webhook.py)
  → subscribed events: job.finished, job.failed

FreeConvert:
  → https://{tunnel}/fc-webhook     (handled by plugins/fc_webhook.py)
  → subscribed events: job.completed, job.failed

TELEGRAM SUMMARY
────────────────
Set ADMIN_ID in env or cfg to receive a Telegram message after sync:

    ✅ Webhook sync complete  (https://abc.trycloudflare.com)
    ──────────────────────
    ☁️ CloudConvert  2 deleted · 2 registered  [key1 ✓, key2 ✓]
    🆓 FreeConvert   1 deleted · 1 registered  [key1 ✓]
"""
from __future__ import annotations

import asyncio
import logging
import os

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Main entry-point
# ─────────────────────────────────────────────────────────────

async def on_tunnel_ready(
    tunnel_url: str,
    *,
    notify_uid:   int | None  = None,
    cc_path:      str         = "/webhook",
    fc_path:      str         = "/fc-webhook",
) -> dict:
    """
    Run CC + FC webhook cleanup/registration concurrently.

    Args:
        tunnel_url:  The new public URL, e.g. "https://abc.trycloudflare.com".
        notify_uid:  Telegram user ID to notify on completion.
                     If None, falls back to ADMIN_ID env var.
        cc_path:     Webhook path registered with CloudConvert. Default /webhook.
        fc_path:     Webhook path registered with FreeConvert. Default /fc-webhook.

    Returns:
        {
            "tunnel":  str,
            "cc":      list[dict],   # per-key results from sync_cc_webhooks
            "fc":      list[dict],   # per-key results from sync_fc_webhooks
        }
    """
    if not tunnel_url:
        log.warning("[WH-Sync] tunnel_url is empty — skipping sync")
        return {"tunnel": "", "cc": [], "fc": []}

    tunnel_url = tunnel_url.rstrip("/")
    log.info("[WH-Sync] Starting webhook sync for tunnel: %s", tunnel_url)

    from services.cc_webhook_mgr import sync_cc_webhooks
    from services.freeconvert_api import sync_fc_webhooks

    cc_results, fc_results = await asyncio.gather(
        sync_cc_webhooks(tunnel_url, webhook_path=cc_path),
        sync_fc_webhooks(tunnel_url, webhook_path=fc_path),
        return_exceptions=True,
    )

    if isinstance(cc_results, BaseException):
        log.error("[WH-Sync] CC sync raised: %s", cc_results)
        cc_results = []
    if isinstance(fc_results, BaseException):
        log.error("[WH-Sync] FC sync raised: %s", fc_results)
        fc_results = []

    result = {"tunnel": tunnel_url, "cc": cc_results, "fc": fc_results}

    # Telegram notification (best-effort)
    uid = notify_uid or _admin_uid()
    if uid:
        asyncio.create_task(_notify(uid, tunnel_url, cc_results, fc_results))

    return result


# ─────────────────────────────────────────────────────────────
# Tunnel watchdog integration helper
# ─────────────────────────────────────────────────────────────

async def on_tunnel_reconnected(new_url: str) -> None:
    """
    Convenience wrapper — call this from your tunnel watchdog when a
    reconnect produces a new URL.

    Also re-registers the new URL with CloudConvert jobs in cc_job_store
    that are still pending (they already have a notification_url set, but
    those are per-job and can't be changed after submission — what this does
    instead is ensure the global subscription covers all future completions).
    """
    await on_tunnel_ready(new_url)

    # Persist the new tunnel URL for other services
    try:
        from core.config import cfg
        cfg.tunnel_url = new_url          # type: ignore[attr-defined]
        log.info("[WH-Sync] cfg.tunnel_url updated → %s", new_url)
    except Exception as exc:
        log.debug("[WH-Sync] Could not update cfg.tunnel_url: %s", exc)


# ─────────────────────────────────────────────────────────────
# Optional startup: poll pending jobs in both stores
# ─────────────────────────────────────────────────────────────

async def poll_pending_jobs(client=None) -> None:
    """
    After a restart, jobs submitted before the crash may have completed while
    the bot was down.  This function polls both CC and FC job stores and
    handles any that are now finished.

    Call after on_tunnel_ready() so the new webhook is registered first.
    """
    await _poll_cc_pending(client)
    await _poll_fc_pending(client)


async def _poll_cc_pending(client=None) -> None:
    """Poll CloudConvert for jobs that are still 'processing' in cc_job_store."""
    try:
        from services.cc_job_store import cc_job_store
        pending = await cc_job_store.list_processing()
        if not pending:
            return
        log.info("[WH-Sync] %d pending CC job(s) — polling for completion", len(pending))

        import aiohttp as _ah
        cc_base = "https://api.cloudconvert.com/v2"

        for job in pending:
            api_key = os.environ.get("CC_API_KEY", "").strip()
            if not api_key:
                break
            try:
                async with _ah.ClientSession(
                    timeout=_ah.ClientTimeout(total=10)
                ) as sess:
                    async with sess.get(
                        f"{cc_base}/jobs/{job.job_id}",
                        headers={"Authorization": f"Bearer {api_key}"},
                    ) as resp:
                        data = await resp.json()

                status = (data.get("data") or data).get("status", "")
                if status == "finished":
                    log.info("[WH-Sync] CC job %s is finished — triggering webhook handler", job.job_id)
                    # Re-trigger the CC webhook handler with the job data
                    try:
                        from plugins.webhook import _handle_cc_job
                        await _handle_cc_job(job.job_id, data.get("data") or data, api_key)
                    except ImportError:
                        log.warning("[WH-Sync] plugins.webhook._handle_cc_job not found")
                elif status == "error":
                    await cc_job_store.update(job.job_id, status="failed")
                    log.warning("[WH-Sync] CC job %s errored", job.job_id)
            except Exception as exc:
                log.warning("[WH-Sync] CC poll failed for job %s: %s", job.job_id, exc)

    except Exception as exc:
        log.error("[WH-Sync] _poll_cc_pending failed: %s", exc)


async def _poll_fc_pending(client=None) -> None:
    """Poll FreeConvert for jobs that are still 'processing' in fc_job_store."""
    try:
        from services.fc_job_store import fc_job_store
        pending = await fc_job_store.list_processing()
        if not pending:
            return
        log.info("[WH-Sync] %d pending FC job(s) — polling for completion", len(pending))

        from services.freeconvert_api import _fc_get_job

        for job in pending:
            api_key = job.api_key or os.environ.get("FC_API_KEY", "").strip()
            if not api_key:
                break
            try:
                data   = await _fc_get_job(api_key, job.job_id)
                status = (data.get("status") or "").lower()

                if status == "completed":
                    log.info("[WH-Sync] FC job %s completed — triggering FC webhook handler", job.job_id)
                    from plugins.fc_webhook import _handle_completion
                    await _handle_completion(job, data)
                elif status in ("failed", "error"):
                    await fc_job_store.update(job.job_id, status="failed")
                    log.warning("[WH-Sync] FC job %s failed", job.job_id)
            except Exception as exc:
                log.warning("[WH-Sync] FC poll failed for job %s: %s", job.job_id, exc)

    except Exception as exc:
        log.error("[WH-Sync] _poll_fc_pending failed: %s", exc)


# ─────────────────────────────────────────────────────────────
# Telegram notification helper
# ─────────────────────────────────────────────────────────────

def _admin_uid() -> int | None:
    raw = os.environ.get("ADMIN_ID", "").strip()
    try:
        return int(raw) if raw else None
    except ValueError:
        return None


def _fmt_results(results: list[dict], platform: str) -> str:
    if not results:
        return f"  {platform}  —  (no keys configured)\n"
    lines = []
    for r in results:
        tail = r.get("key_tail", "?")
        d    = r.get("deleted", 0)
        reg  = r.get("registered")
        err  = r.get("error", "")
        if err:
            lines.append(f"  {tail}  ❌ {err[:50]}")
        else:
            lines.append(f"  {tail}  {d} deleted · ✅ registered <code>{str(reg)[:12]}</code>")
    return "\n".join(lines)


async def _notify(uid: int, tunnel_url: str, cc: list, fc: list) -> None:
    try:
        from core.session import get_client
        from pyrogram import enums
        client = get_client()

        cc_section = _fmt_results(cc, "☁️ CloudConvert")
        fc_section = _fmt_results(fc, "🆓 FreeConvert")

        text = (
            "✅ <b>Webhook sync complete</b>\n"
            "──────────────────────\n\n"
            f"🔗 <code>{tunnel_url}</code>\n\n"
            f"<b>CloudConvert</b>\n{cc_section}\n\n"
            f"<b>FreeConvert</b>\n{fc_section}"
        )
        await client.send_message(
            uid, text,
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as exc:
        log.debug("[WH-Sync] Telegram notify failed: %s", exc)
