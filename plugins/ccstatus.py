"""
services/ccstatus.py
/ccstatus — live panel showing CloudConvert hardsub/convert jobs.

Adaptive polling: 5 s when any job is processing, 60 s when idle.
Primary delivery: poller checks CC API, then downloads export URL via
aiohttp and uploads to Telegram directly — no webhook dependency.
Webhook is a bonus; this poller is the authoritative delivery path.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from services.cc_job_store import cc_job_store, CCJob

log = logging.getLogger(__name__)

# uid → live panel Message; poller edits this on every sweep
_open_panels: dict[int, Message] = {}

_poller_task: Optional[asyncio.Task] = None


# ─────────────────────────────────────────────────────────────
# Panel renderer
# ─────────────────────────────────────────────────────────────

def _status_icon(status: str) -> str:
    return {"processing": "⏳", "finished": "✅", "error": "❌"}.get(status, "❓")


def _render_panel(uid: int) -> str:
    jobs = cc_job_store.jobs_for_user(uid)
    if not jobs:
        return (
            "☁️ <b>CloudConvert Jobs</b>\n"
            "──────────────────────\n\n"
            "<i>No jobs found for your account.</i>\n\n"
            "Use /hardsub to start a new hardsub job."
        )

    lines = [
        "☁️ <b>CloudConvert Jobs</b>",
        "──────────────────────",
        "",
    ]

    for j in jobs[:10]:
        icon  = _status_icon(j.status)
        fname = (j.fname[:38] + "…") if len(j.fname) > 38 else j.fname
        lines.append(f"{icon} <code>{fname}</code>")
        lines.append(f"   🆔 <code>{j.job_id[:20]}</code>")

        if j.status == "processing":
            msg_s = j.task_message or "Processing…"
            lines.append(f"   🔄 <i>{msg_s}</i>")
        elif j.status == "finished":
            if j.notified:
                lines.append("   📤 <i>Uploaded to Telegram</i>")
            else:
                lines.append("   ⬆️ <i>Uploading…</i>")
        elif j.status == "error":
            err = (j.error_msg or "Unknown error")[:60]
            lines.append(f"   ❌ <i>{err}</i>")

        lines.append("")

    lines += [
        "──────────────────────",
        f"<i>Updated {time.strftime('%H:%M:%S')}</i>",
    ]
    return "\n".join(lines)


def _panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="ccs|refresh"),
         InlineKeyboardButton("❌ Close",   callback_data="ccs|close")],
    ])


# ─────────────────────────────────────────────────────────────
# /ccstatus command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("ccstatus"))
async def cmd_ccstatus(client: Client, msg: Message):
    uid  = msg.from_user.id
    text = _render_panel(uid)
    st   = await msg.reply(
        text, parse_mode=enums.ParseMode.HTML, reply_markup=_panel_kb(),
    )
    _open_panels[uid] = st
    _ensure_poller()


@Client.on_callback_query(filters.regex(r"^ccs\|"))
async def ccstatus_cb(client: Client, cb: CallbackQuery):
    uid    = cb.from_user.id
    action = cb.data.split("|")[1]
    await cb.answer()

    if action == "close":
        _open_panels.pop(uid, None)
        try:
            await cb.message.delete()
        except Exception:
            pass
        return

    if action == "refresh":
        text = _render_panel(uid)
        try:
            await cb.message.edit(
                text,
                parse_mode=enums.ParseMode.HTML,
                reply_markup=_panel_kb(),
            )
            _open_panels[uid] = cb.message
        except Exception as exc:
            if "MESSAGE_NOT_MODIFIED" not in str(exc):
                log.debug("ccstatus refresh edit failed: %s", exc)


# ─────────────────────────────────────────────────────────────
# Export download + Telegram upload
# ─────────────────────────────────────────────────────────────

async def _download_export(url: str, dest_path: str) -> None:
    """Stream download from CloudConvert export URL — 8 MB chunks, 2 h timeout."""
    timeout = aiohttp.ClientTimeout(total=7200)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.get(url, allow_redirects=True) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as fh:
                async for chunk in resp.content.iter_chunked(8 * 1024 * 1024):
                    fh.write(chunk)


async def _deliver_job(job: CCJob) -> None:
    """Download finished job from CC and upload to Telegram."""
    from core.session import get_client
    from services.utils import make_tmp, cleanup
    from services.uploader import upload_file

    client = get_client()
    tmp    = make_tmp(cfg.download_dir, job.uid)
    fname  = job.output_name or job.fname or "output.mp4"
    dest   = os.path.join(tmp, fname)

    try:
        await client.send_message(
            job.uid,
            f"☁️ <b>CloudConvert Finished</b>\n"
            f"──────────────────────\n\n"
            f"📁 <code>{fname[:50]}</code>\n\n"
            f"⬇️ <i>Downloading from CloudConvert CDN…</i>",
            parse_mode="html",
        )

        await _download_export(job.export_url, dest)

        # Send a real status message so upload progress is visible
        st = await client.send_message(
            job.uid,
            f"📤 <b>Uploading…</b>\n<code>{fname[:50]}</code>",
            parse_mode="html",
        )
        await upload_file(client, st, dest)
        # job is already marked notified before this task was spawned,
        # but call again as a safe no-op in case of any edge case.
        await cc_job_store.mark_notified(job.job_id)
        log.info("[CCStatus] Delivered job %s to uid=%d", job.job_id, job.uid)

    except Exception as exc:
        log.error("[CCStatus] Delivery failed for job %s: %s", job.job_id, exc)
        try:
            await client.send_message(
                job.uid,
                f"❌ <b>CloudConvert delivery failed</b>\n"
                f"<code>{fname}</code>\n\n"
                f"<code>{str(exc)[:200]}</code>",
                parse_mode="html",
            )
        except Exception:
            pass
    finally:
        cleanup(tmp)


# ─────────────────────────────────────────────────────────────
# Poller
# ─────────────────────────────────────────────────────────────

def _ensure_poller() -> None:
    global _poller_task
    if _poller_task and not _poller_task.done():
        return
    _poller_task = asyncio.create_task(_poll_loop())
    log.info("[CCStatus] Poller started")


async def _poll_loop() -> None:
    api_key = os.environ.get("CC_API_KEY", "").strip()
    if not api_key:
        log.warning("[CCStatus] No CC_API_KEY — poller disabled")
        return

    from services.cloudconvert_api import check_job_status

    consecutive_idle = 0

    while True:
        active      = cc_job_store.active_jobs()
        undelivered = cc_job_store.undelivered_jobs()
        # Use 5 s interval whenever there is real work: active jobs being processed,
        # or finished jobs waiting to be downloaded and uploaded to Telegram.
        interval = 5 if (active or undelivered) else 60

        for job in active:
            try:
                data   = await check_job_status(api_key, job.job_id)
                status = data.get("status", "")
                tasks  = data.get("tasks", [])

                msgs = []
                for t in tasks:
                    if t.get("status") not in ("finished", "waiting", "pending"):
                        m = t.get("message") or t.get("status") or ""
                        if m:
                            msgs.append(m)
                task_msg = msgs[0] if msgs else "Processing…"

                if status == "finished":
                    export_url = ""
                    for t in tasks:
                        if (t.get("operation") == "export/url"
                                and t.get("status") == "finished"):
                            files = (t.get("result") or {}).get("files", [])
                            if files:
                                export_url = files[0].get("url", "")
                                break

                    if export_url:
                        await cc_job_store.finish(job.job_id, export_url=export_url)
                        # Mark notified BEFORE spawning the task so a second poller
                        # cycle or a restart cannot pick up the same job and double-upload.
                        await cc_job_store.mark_notified(job.job_id)
                        asyncio.create_task(_deliver_job(
                            cc_job_store.get(job.job_id) or job
                        ))
                    else:
                        await cc_job_store.finish(
                            job.job_id,
                            error_msg="Job finished but no export URL found",
                        )

                elif status == "error":
                    err = data.get("message") or "CloudConvert reported an error"
                    await cc_job_store.finish(job.job_id, error_msg=err)

                else:
                    await cc_job_store.update(job.job_id, task_message=task_msg)

            except Exception as exc:
                log.warning("[CCStatus] Poll error job %s: %s", job.job_id, exc)

        for uid, panel_msg in list(_open_panels.items()):
            try:
                text = _render_panel(uid)
                await panel_msg.edit(
                    text,
                    parse_mode=enums.ParseMode.HTML,
                    reply_markup=_panel_kb(),
                )
            except Exception as exc:
                err = str(exc)
                if "MESSAGE_NOT_MODIFIED" in err or "message was not modified" in err.lower():
                    pass
                elif "MESSAGE_ID_INVALID" in err or "message to edit not found" in err.lower():
                    _open_panels.pop(uid, None)
                else:
                    log.debug("[CCStatus] Panel edit uid=%d: %s", uid, err)

        # Deliver any jobs resolved by the webhook (status=finished, export_url set, not yet notified).
        # The webhook updates cc_job_store but never calls _deliver_job directly to avoid double-upload.
        # The poller is the single authoritative delivery path — we must pick these up here.
        for job in cc_job_store.undelivered_jobs():
            log.info("[CCStatus] Delivering webhook-resolved job %s to uid=%d", job.job_id, job.uid)
            # Mark notified immediately to prevent re-delivery on next poll cycle
            await cc_job_store.mark_notified(job.job_id)
            asyncio.create_task(_deliver_job(job))

        if not cc_job_store.active_jobs() and not cc_job_store.undelivered_jobs() and not _open_panels:
            consecutive_idle += 1
            if consecutive_idle >= 3:
                log.info("[CCStatus] Poller stopping — no active jobs, no open panels")
                return
        else:
            consecutive_idle = 0

        await asyncio.sleep(interval)
