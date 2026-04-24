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

FIX BUG-WS-MULTIKEY (CRITICAL — offline recovery broken for multi-key setups):
  _poll_cc_pending() was using the raw CC_API_KEY environment variable string
  directly as a Bearer token. When CC_API_KEY contains multiple keys
  (e.g. "key1,key2,key3"), the Authorization header became
  "Bearer key1,key2,key3" — invalid, always returned 401 from CloudConvert.
  Fix: replaced the manual aiohttp.get() with check_job_status() from
  cloudconvert_api.py, which already handles multi-key iteration correctly.
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
    Sync CloudConvert AND FreeConvert webhook subscriptions for the new
    tunnel URL.

    CloudConvert has a global /v2/webhooks endpoint (always used).
    FreeConvert prefers per-job webhook_url embedding, but we also try an
    account-level endpoint opportunistically (see services/fc_webhook_mgr.py).

    IMPORTANT: set_tunnel_url() is called BEFORE any FC submit_*() runs,
    so even jobs started a millisecond after this function returns will
    auto-embed the new webhook URL.
    """
    if not tunnel_url:
        log.warning("[WH-Sync] tunnel_url is empty — skipping")
        return {"tunnel": "", "cc": [], "fc": []}

    tunnel_url = tunnel_url.rstrip("/")

    # Update cfg BEFORE sync so per-job webhooks embed the latest URL
    try:
        from core.config import set_tunnel_url
        set_tunnel_url(tunnel_url)
        log.info("[WH-Sync] tunnel_url set → %s", tunnel_url)
    except Exception as exc:
        log.debug("[WH-Sync] cfg update: %s", exc)

    log.info("[WH-Sync] Syncing CC webhooks → %s%s", tunnel_url, cc_path)
    from services.cc_webhook_mgr import sync_cc_webhooks
    cc_results = await sync_cc_webhooks(tunnel_url, webhook_path=cc_path)

    # FC auto-setting — mirror of CC sync (may no-op if account endpoint absent)
    fc_results: list = []
    try:
        from services.fc_webhook_mgr import sync_fc_webhooks
        fc_results = await sync_fc_webhooks(tunnel_url)
    except Exception as exc:
        log.warning("[WH-Sync] FC sync error: %s — per-job webhooks still active", exc)

    uid = notify_uid or _admin_uid()
    if uid:
        asyncio.create_task(_notify(uid, tunnel_url, cc_results, cc_path, fc_results))

    return {"tunnel": tunnel_url, "cc": cc_results, "fc": fc_results}


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
        api_key_raw = os.environ.get("CC_API_KEY", "").strip()
        if not api_key_raw:
            log.warning("[WH-Sync] CC_API_KEY not set — cannot poll pending jobs")
            return

        # FIX BUG-WS-MULTIKEY: do NOT use the raw CC_API_KEY string directly as
        # a Bearer token. If CC_API_KEY = "key1,key2", Authorization becomes
        # "Bearer key1,key2" which is invalid (401 on CloudConvert).
        # Use check_job_status() which already handles multi-key iteration correctly.
        from services.cloudconvert_api import check_job_status as _cc_status

        for job in pending:
            try:
                jdata = await _cc_status(api_key_raw, job.job_id)
                if not jdata:
                    log.debug("[WH-Sync] CC job %s not found on any key", job.job_id)
                    continue

                status = jdata.get("status", "")

                if status == "finished":
                    log.info("[WH-Sync] CC job %s finished offline — recovering", job.job_id)
                    try:
                        # FIX BUG-WS-02: correct module (not plugins.webhook)
                        from services.cloudconvert_hook import _handle_cc_job
                        await _handle_cc_job(job.job_id, jdata, api_key_raw)
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
                    # FIX: Atomically claim delivery to prevent a race with the
                    # FC webhook handler firing simultaneously on the same job.
                    claimed = await fc_job_store.try_claim_delivery(job.job_id)
                    if not claimed:
                        log.info("[WH-Sync] FC job %s already claimed — skip", job.job_id)
                        continue
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
    fc: list | None = None,
) -> None:
    try:
        from core.session import get_client
        from pyrogram import enums
        client = get_client()

        cc_lines = []
        for r in cc:
            tail = r.get("key_tail", "?")
            d    = r.get("deleted", 0)
            reg  = r.get("registered")
            err  = r.get("error", "")
            if err:
                cc_lines.append(f"  {tail}  ❌ {err[:50]}")
            else:
                cc_lines.append(f"  {tail}  {d} deleted · ✅ <code>{str(reg)[:12]}</code>")
        cc_section = "\n".join(cc_lines) if cc_lines else "  (no CC keys configured)"

        fc_lines = []
        for r in (fc or []):
            tail = r.get("key_tail", "?")
            mode = r.get("mode", "per-job")
            reg  = r.get("registered")
            err  = r.get("error", "")
            if err and not reg:
                fc_lines.append(f"  {tail}  per-job ✅  <i>(account: {err[:40]})</i>")
            elif mode == "account" and reg:
                fc_lines.append(f"  {tail}  account ✅ <code>{str(reg)[:12]}</code>")
            else:
                fc_lines.append(f"  {tail}  per-job ✅")
        fc_section = "\n".join(fc_lines) if fc_lines else \
                     "  Per-job — URL embedded at job submission ✅"

        cc_full = f"{tunnel_url}{cc_path}"
        fc_full = f"{tunnel_url}/fc-webhook"

        text = (
            "✅ <b>Webhook sync complete</b>\n"
            "──────────────────────\n\n"
            f"🔗 CC endpoint:\n<code>{cc_full}</code>\n"
            f"🔗 FC endpoint:\n<code>{fc_full}</code>\n\n"
            f"☁️ <b>CloudConvert</b>\n{cc_section}\n\n"
            f"🆓 <b>FreeConvert</b>\n{fc_section}"
        )
        await client.send_message(
            uid, text,
            parse_mode=enums.ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as exc:
        log.debug("[WH-Sync] Telegram notify failed: %s", exc)
