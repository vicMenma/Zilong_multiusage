"""
plugins/fc_webhook.py
FreeConvert webhook receiver — mirrors plugins/webhook.py (CloudConvert).

FreeConvert POSTs the full job JSON to the registered webhook URL when
a job's status changes.  This module:

  1. Parses the incoming JSON.
  2. Looks up the job in fc_job_store.
  3. On "completed": downloads the output file and uploads it to Telegram.
  4. On "failed":    notifies the user with the error message.

NEW — RECURRING FC POLLER
─────────────────────────
FreeConvert's primary webhook mechanism is per-job (webhook_url embedded
in the job payload). But webhooks can fail: the tunnel may be down, FC
may retry with the wrong URL after a bot restart, or FC may drop the
callback entirely.

_fc_poll_loop() is a background task (like CC's _poll_loop in ccstatus.py)
that periodically polls the FC API for every job in "processing" state and
delivers completed jobs independently of webhooks.

  • Polls every 15 s when jobs are active.
  • Backs off to 30 s when idle; stops after 3 consecutive idle cycles.
  • _ensure_fc_poller() (re)starts the task when needed.
  • fc_job_store.set_on_job_added(_ensure_fc_poller) ensures the poller
    is (re)started automatically whenever a new FC job is submitted,
    without a circular import.

DELIVERY RETRY
──────────────
After a failed delivery _handle_completion resets the job to "processing"
in the finally block. _ensure_fc_poller() is then called so the poller
picks it up on the next cycle.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

from aiohttp import web
from pyrogram import enums

from services.fc_job_store import fc_job_store, FCJob
from services.utils import cleanup, make_tmp, human_size

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Recurring FC job poller
# ─────────────────────────────────────────────────────────────

_fc_poller_task: Optional[asyncio.Task] = None


def _ensure_fc_poller() -> None:
    """(Re)start the FC poll loop if it is not already running."""
    global _fc_poller_task
    if _fc_poller_task and not _fc_poller_task.done():
        return
    try:
        _fc_poller_task = asyncio.get_running_loop().create_task(_fc_poll_loop())
        log.info("[FC-Poller] Started")
    except RuntimeError:
        # No running event loop yet (called at import time) — harmless
        pass


async def _fc_poll_loop() -> None:
    """
    Background task: poll FC API every 15 s for jobs in 'processing' state.
    Delivers completed jobs via _handle_completion(), reports failures.
    Stops automatically after 3 consecutive idle cycles (no processing jobs).
    """
    api_key_raw = os.environ.get("FC_API_KEY", "").strip()
    if not api_key_raw:
        log.warning("[FC-Poller] No FC_API_KEY — poller disabled")
        return

    from services.freeconvert_api import parse_fc_keys, _fc_get_job
    keys = parse_fc_keys(api_key_raw)
    if not keys:
        log.warning("[FC-Poller] No valid FC keys found — poller disabled")
        return

    log.info("[FC-Poller] Running with %d key(s)", len(keys))
    consecutive_idle = 0

    while True:
        try:
            pending = await fc_job_store.list_processing()
        except Exception as exc:
            log.warning("[FC-Poller] list_processing failed: %s", exc)
            await asyncio.sleep(30)
            continue

        if not pending:
            consecutive_idle += 1
            if consecutive_idle >= 3:
                log.info("[FC-Poller] No active FC jobs — poller stopping")
                return
            await asyncio.sleep(30)
            continue

        consecutive_idle = 0

        for job in pending:
            # Use the key that originally submitted this job; fall back to first
            job_key = job.api_key or keys[0]
            try:
                jdata  = await _fc_get_job(job_key, job.job_id)
                status = (jdata.get("status") or "").lower()

                if status == "completed":
                    log.info("[FC-Poller] Job %s completed — delivering to uid=%d",
                             job.job_id, job.uid)
                    claimed = await fc_job_store.try_claim_delivery(job.job_id)
                    if claimed:
                        asyncio.get_running_loop().create_task(
                            _handle_completion(job, jdata)
                        )
                    else:
                        log.info("[FC-Poller] Job %s already claimed — skip", job.job_id)

                elif status in ("failed", "error", "cancelled"):
                    # FreeConvert uses result.errorCode + result.msg; also check message
                    r = jdata.get("result") or {}
                    err = (r.get("msg") or r.get("errorCode") or
                           jdata.get("message") or jdata.get("msg") or status)[:200]
                    log.warning("[FC-Poller] Job %s %s: %s", job.job_id, status, err)
                    await fc_job_store.update(job.job_id, status="failed", error=err)
                    await _notify_failure(job, err)

                else:
                    log.debug("[FC-Poller] Job %s still %s", job.job_id, status)

            except Exception as exc:
                log.warning("[FC-Poller] Poll error for job %s: %s", job.job_id, exc)

        await asyncio.sleep(15)


# ─────────────────────────────────────────────────────────────
# Webhook handler (aiohttp route)
# ─────────────────────────────────────────────────────────────

async def handle_fc_webhook(request: web.Request) -> web.Response:
    """
    POST /fc-webhook
    Responds 200 immediately then processes the payload asynchronously.
    """
    try:
        payload = await request.json()
    except Exception as exc:
        log.warning("[FC-WH] Bad JSON: %s", exc)
        return web.Response(status=400, text="bad json")

    data    = payload.get("data") or payload
    job_id  = data.get("id", "")
    status  = (data.get("status") or "").lower()

    if not job_id:
        log.warning("[FC-WH] Webhook with no job ID")
        return web.Response(status=200, text="ok")

    log.info("[FC-WH] Received  job=%s  status=%s", job_id, status)

    asyncio.create_task(_process_webhook(job_id, status, data))

    return web.Response(status=200, text="ok")


# ─────────────────────────────────────────────────────────────
# Background webhook processor
# ─────────────────────────────────────────────────────────────

async def _process_webhook(job_id: str, status: str, data: dict) -> None:
    job = await fc_job_store.get(job_id)

    if not job:
        log.warning("[FC-WH] job %s not found in store — ignoring", job_id)
        return

    if status == "processing":
        log.debug("[FC-WH] job %s still processing, ignoring", job_id)
        return

    if status in ("failed", "error", "cancelled"):
        r = data.get("result") or {}
        err = (r.get("msg") or r.get("errorCode") or
               data.get("message") or data.get("msg") or f"Job {status}")
        log.warning("[FC-WH] job %s failed: %s", job_id, err)
        await fc_job_store.update(job_id, status="failed", error=err[:200])
        await _notify_failure(job, err)
        return

    if status == "completed":
        # Atomically claim delivery — prevents race with _fc_poll_loop.
        claimed = await fc_job_store.try_claim_delivery(job_id)
        if not claimed:
            log.info("[FC-WH] job %s already claimed for delivery — skipping duplicate",
                     job_id)
            return
        await _handle_completion(job, data)
        return

    log.debug("[FC-WH] Unhandled status '%s' for job %s", status, job_id)


# ─────────────────────────────────────────────────────────────
# Completion: download + upload
# ─────────────────────────────────────────────────────────────

async def _handle_completion(job: FCJob, data: dict) -> None:
    from services.downloader import download_direct
    from services.uploader import upload_file

    download_url = _extract_download_url(data)
    if not download_url:
        log.error("[FC-WH] No download URL for completed job %s", job.job_id)
        await _notify_failure(job, "Completed but no output URL found")
        return

    log.info("[FC-WH] Downloading result for job %s  uid=%d", job.job_id, job.uid)

    from core.config import cfg as _cfg
    tmp = make_tmp(_cfg.download_dir, job.uid)
    try:
        local_path = await download_direct(download_url, tmp)
        fname = job.output_name or os.path.basename(local_path)

        # Rename to intended output name if needed
        if os.path.basename(local_path) != fname:
            new_path = os.path.join(tmp, fname)
            try:
                os.rename(local_path, new_path)
                local_path = new_path
            except OSError:
                pass

        fsize = os.path.getsize(local_path)
        log.info("[FC-WH] Downloaded %s (%s) for uid=%d", fname, human_size(fsize), job.uid)

        client = _get_client()
        if client:
            _type_label = {
                "hardsub":  "🔥 Hardsub",
                "convert":  "🔄 Convert",
                "compress": "📐 Compress",
            }.get(job.job_type, "✅ Job")

            sub_line = f"\n💬 <code>{job.sub_fname[:40]}</code>" if job.sub_fname else ""

            st = await _floodwait_send(
                client, job.uid,
                f"{_type_label} <b>done!</b>\n"
                "──────────────────────\n\n"
                f"📄 <code>{job.fname[:42]}</code>\n"
                f"📦 <code>{fname[:42]}</code>{sub_line}\n"
                f"💾 <code>{human_size(fsize)}</code>\n\n"
                "⬆️ Uploading…",
            )
            await upload_file(client, st, local_path, user_id=job.uid)
            # CRITICAL: flag upload-completed IMMEDIATELY so any post-upload
            # exception cannot trigger a duplicate on next poll / restart.
            try:
                await fc_job_store.mark_uploaded(job.job_id)
            except Exception as _e:
                log.warning("[FC-WH] mark_uploaded failed for %s: %s", job.job_id, _e)
        else:
            log.error("[FC-WH] No Pyrogram client available — cannot upload for uid=%d",
                      job.uid)

    except Exception as exc:
        log.error("[FC-WH] Completion handler failed for job %s: %s",
                  job.job_id, exc, exc_info=True)
        try:
            refreshed = await fc_job_store.get(job.job_id)
        except Exception:
            refreshed = None
        if not (refreshed and refreshed.uploaded):
            await _notify_failure(job, str(exc)[:200])
        else:
            log.info("[FC-WH] Post-upload error ignored for %s — "
                     "user already received the file", job.job_id)
    finally:
        cleanup(tmp)
        # FIX DELIVERY-RETRY-RESTART: after the store update, re-ensure the
        # poller is running so any retry (job reset to "processing") is picked up.
        try:
            refreshed = await fc_job_store.get(job.job_id)
            if refreshed and refreshed.uploaded:
                await fc_job_store.remove(job.job_id)
            elif refreshed:
                # Delivery failed — reset to "processing" so the poller retries.
                await fc_job_store.update(job.job_id, status="processing")
                log.info("[FC-WH] Delivery failed for %s — reset to 'processing' for retry",
                         job.job_id)
                # Restart poller so it picks up the retry immediately.
                _ensure_fc_poller()
        except Exception as _re:
            log.warning("[FC-WH] Store cleanup for %s: %s", job.job_id, _re)


# ─────────────────────────────────────────────────────────────
# Failure notification
# ─────────────────────────────────────────────────────────────

async def _notify_failure(job: FCJob, error_msg: str) -> None:
    client = _get_client()
    if not client:
        return
    try:
        await _floodwait_send(
            client, job.uid,
            f"❌ <b>FreeConvert job failed</b>\n"
            "──────────────────────\n\n"
            f"📄 <code>{job.fname[:42]}</code>\n"
            f"🆔 <code>{job.job_id}</code>\n\n"
            f"<code>{error_msg[:300]}</code>",
        )
    except Exception as exc:
        log.error("[FC-WH] Could not notify uid=%d of failure: %s", job.uid, exc)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _extract_download_url(data: dict) -> str:
    """Extract the output file URL from a completed FreeConvert job dict.

    FreeConvert API returns the export URL as a flat string in result.url —
    NOT as result.files[0].url (that is CloudConvert's format).
    We support both shapes so the code works regardless of API version.
    """
    tasks = data.get("tasks") or []

    def _url_from_result(result: dict) -> str:
        # FreeConvert v1: result.url  (string)
        url = result.get("url") or ""
        if isinstance(url, str) and url:
            return url
        # CloudConvert / legacy: result.files[0].url
        for key in ("files", "output", "outputs"):
            files = result.get(key) or []
            if isinstance(files, list) and files:
                return files[0].get("url", "") if isinstance(files[0], dict) else ""
            if isinstance(files, dict):
                return files.get("url", "")
        return ""

    if isinstance(tasks, list):
        for task in tasks:
            op   = task.get("operation") or task.get("name") or ""
            stat = (task.get("status") or "").lower()
            if "export" in op and stat == "completed":
                url = _url_from_result(task.get("result") or {})
                if url:
                    return url
    elif isinstance(tasks, dict):
        for _name, task in tasks.items():
            op   = task.get("operation") or ""
            stat = (task.get("status") or "").lower()
            if "export" in op and stat == "completed":
                url = _url_from_result(task.get("result") or {})
                if url:
                    return url

    return ""


def _get_client():
    try:
        from core.session import get_client
        return get_client()
    except Exception:
        return None


async def _floodwait_send(client, uid: int, text: str, max_retries: int = 5):
    """FloodWait-safe send_message."""
    from pyrogram.errors import FloodWait
    import asyncio as _asyncio

    for attempt in range(max_retries):
        try:
            return await client.send_message(
                uid, text,
                parse_mode=enums.ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except FloodWait as fw:
            wait = min(fw.value + 2, 90)
            log.warning("[FC-WH] FloodWait %ds (attempt %d/%d)", fw.value, attempt + 1, max_retries)
            if attempt < max_retries - 1:
                await _asyncio.sleep(wait)
            else:
                raise
        except Exception:
            raise


# ─────────────────────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────────────────────

async def startup_load() -> None:
    """
    Call once at bot startup to load persisted FC jobs and start the poller.

    NEW: Registers _ensure_fc_poller as the on_job_added callback on
    fc_job_store so the poller is (re)started automatically every time a
    new FC job is submitted anywhere in the codebase — without circular
    imports.
    """
    await fc_job_store.load()
    count = await fc_job_store.count()
    log.info("[FC-WH] Job store ready — %d job(s) loaded", count)

    # Register callback: any future fc_job_store.add() will restart the poller
    fc_job_store.set_on_job_added(_ensure_fc_poller)

    # Start the recurring poller immediately (picks up jobs from previous session
    # and any jobs submitted between now and the first webhook delivery)
    _ensure_fc_poller()
    log.info("[FC-WH] Recurring FC job poller started")
