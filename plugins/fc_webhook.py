"""
plugins/fc_webhook.py
FreeConvert webhook receiver — mirrors plugins/webhook.py (CloudConvert).

FreeConvert POSTs the full job JSON to the registered webhook URL when
a job's status changes.  This module:

  1. Parses the incoming JSON.
  2. Looks up the job in fc_job_store.
  3. On "completed": downloads the output file and uploads it to Telegram.
  4. On "failed":    notifies the user with the error message.

REGISTRATION
────────────
Add this route to your aiohttp app (in main.py / server.py):

    from plugins.fc_webhook import handle_fc_webhook
    app.router.add_post("/fc-webhook", handle_fc_webhook)

Then pass the tunnel URL to FreeConvert jobs:

    from services.freeconvert_api import fc_webhook_url
    wh = fc_webhook_url(cfg.tunnel_url)   # e.g. "https://xxx.trycloudflare.com/fc-webhook"
    job_id = await submit_hardsub(api_key, ..., webhook_url=wh)

PAYLOAD SHAPE (FreeConvert → bot)
──────────────────────────────────
FreeConvert sends:
  {
    "data": {
      "id": "<job_id>",
      "status": "completed" | "failed" | "processing",
      "tasks": [
        { "name": "export",   "status": "completed",
          "result": { "files": [{ "url": "...", "filename": "..." }] } },
        ...
      ],
      "message": "<error text on failure>"
    }
  }
"""
from __future__ import annotations

import logging
import os
import time

from aiohttp import web
from pyrogram import enums

from services.fc_job_store import fc_job_store
from services.utils import cleanup, make_tmp, human_size

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Main handler
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

    # Respond immediately — don't block FreeConvert's delivery
    import asyncio
    asyncio.create_task(_process_webhook(job_id, status, data))

    return web.Response(status=200, text="ok")


# ─────────────────────────────────────────────────────────────
# Background processor
# ─────────────────────────────────────────────────────────────

async def _process_webhook(job_id: str, status: str, data: dict) -> None:
    job = await fc_job_store.get(job_id)

    if not job:
        log.warning("[FC-WH] job %s not found in store — ignoring", job_id)
        return

    if status == "processing":
        # Intermediate update — nothing to do yet
        log.debug("[FC-WH] job %s still processing, ignoring", job_id)
        return

    if status in ("failed", "error", "cancelled"):
        err = data.get("message") or f"Job {status}"
        log.warning("[FC-WH] job %s failed: %s", job_id, err)
        await fc_job_store.update(job_id, status="failed", error=err[:200])
        await _notify_failure(job, err)
        return

    if status == "completed":
        # FIX: Atomically claim this job for delivery.
        # Prevents a double-upload race with _poll_fc_pending running simultaneously
        # (e.g. bot restarts, poller fires, then FC retries the webhook delivery).
        claimed = await fc_job_store.try_claim_delivery(job_id)
        if not claimed:
            log.info("[FC-WH] job %s already claimed for delivery — skipping duplicate", job_id)
            return
        await _handle_completion(job, data)
        return

    log.debug("[FC-WH] Unhandled status '%s' for job %s", status, job_id)


# ─────────────────────────────────────────────────────────────
# Completion: download + upload
# ─────────────────────────────────────────────────────────────

async def _handle_completion(job, data: dict) -> None:
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

        # Send status message then upload
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
        else:
            log.error("[FC-WH] No Pyrogram client available — cannot upload for uid=%d", job.uid)

    except Exception as exc:
        log.error("[FC-WH] Completion handler failed for job %s: %s", job.job_id, exc, exc_info=True)
        await _notify_failure(job, str(exc)[:200])
    finally:
        cleanup(tmp)
        try:
            await fc_job_store.remove(job.job_id)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Failure notification
# ─────────────────────────────────────────────────────────────

async def _notify_failure(job, error_msg: str) -> None:
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
    """
    Extract the output file URL from a completed FreeConvert job dict.
    Checks both list-form and dict-form tasks.
    """
    tasks = data.get("tasks") or []

    if isinstance(tasks, list):
        for task in tasks:
            op   = task.get("operation") or task.get("name") or ""
            stat = (task.get("status") or "").lower()
            if "export" in op and stat == "completed":
                result = task.get("result") or {}
                files  = result.get("files") or []
                if files:
                    return files[0].get("url", "")
    elif isinstance(tasks, dict):
        for _name, task in tasks.items():
            op   = task.get("operation") or ""
            stat = (task.get("status") or "").lower()
            if "export" in op and stat == "completed":
                result = task.get("result") or {}
                files  = result.get("files") or []
                if files:
                    return files[0].get("url", "")

    return ""


def _get_client():
    """Return the running Pyrogram client, or None."""
    try:
        from core.session import get_client
        return get_client()
    except Exception:
        return None


async def _floodwait_send(client, uid: int, text: str, max_retries: int = 5):
    """FloodWait-safe send_message (same pattern as url_handler.py)."""
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
# Startup helper — load store
# ─────────────────────────────────────────────────────────────

async def startup_load() -> None:
    """Call once at bot startup to load persisted FC jobs."""
    await fc_job_store.load()
    count = await fc_job_store.count()
    log.info("[FC-WH] Job store ready — %d job(s) loaded", count)
