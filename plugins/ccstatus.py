"""
services/ccstatus.py
/ccstatus — live panel showing CloudConvert hardsub/convert jobs.

Adaptive polling: 5 s when any job is processing/undelivered/in-flight,
60 s when idle.

Primary delivery: poller checks CC API, then downloads export URL via
aiohttp and uploads to Telegram directly — no webhook dependency.
Webhook is a bonus; this poller is the authoritative delivery path.

FIX POLLER-PREMATURE-STOP: _deliver_job now tracks each active delivery
  in _in_flight_delivery_tids.  The idle check in _poll_loop includes
  this set, so the poller keeps running as long as any delivery task is
  in-flight — even after the job moves from active_jobs() to
  undelivered_jobs() and the claim is taken (notified=True).

  Previously, once a job was claimed (notified=True), it disappeared from
  both active_jobs() and undelivered_jobs().  With an empty _open_panels,
  the poller counted "idle" and stopped after 3 × 60 s = 180 s.  For a
  1-2 GB file, download + upload easily exceeds 180 s.  If the upload
  then failed, release_claim() set notified=False — but the poller was
  already gone and the job was permanently orphaned.

FIX DELIVERY-RETRY-RESTART: After release_claim() re-opens a job for
  retry, _ensure_poller() is called to guarantee the poller is running
  to pick it up on the next cycle.

FIX UPLOAD-USER-ID: upload_file() now receives user_id=job.uid so user
  settings (prefix/suffix, auto_forward, progress_style) are applied.

FIX C-04 (audit v3): _deliver_job now retries up to 3 times on failure.
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

# FIX POLLER-PREMATURE-STOP: track delivery tasks currently running so
# the poller does not incorrectly declare itself "idle" and exit while
# a long download+upload is still in progress.
_in_flight_delivery_tids: set = set()


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

async def _download_export(url: str, dest_path: str, progress_cb=None) -> int:
    """Stream download from CloudConvert export URL — 8 MB chunks, 2 h timeout.
    Returns number of bytes written.
    If progress_cb is provided it is called as: await progress_cb(done, total)
    """
    timeout = aiohttp.ClientTimeout(total=7200)
    total = 0
    done  = 0
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.get(url, allow_redirects=True) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", 0))
            with open(dest_path, "wb") as fh:
                async for chunk in resp.content.iter_chunked(8 * 1024 * 1024):
                    fh.write(chunk)
                    done += len(chunk)
                    if progress_cb:
                        try:
                            await progress_cb(done, total)
                        except Exception:
                            pass
    return done


async def _deliver_job(job: CCJob) -> None:
    """
    Download finished job from CC and upload to Telegram.
    Registers TaskRecords in the global tracker so the /status panel
    shows CC-download progress and TG-upload progress.

    FIX POLLER-PREMATURE-STOP: registers job.job_id in
    _in_flight_delivery_tids at entry and removes it in finally, so the
    poll loop knows a delivery is active even after the job leaves
    active_jobs() and undelivered_jobs().

    FIX DELIVERY-RETRY-RESTART: after release_claim(), calls
    _ensure_poller() so the retry is picked up even if the poller had
    already declared itself idle.

    FIX DUPLICATE-DELIVERY: mark_uploaded() is called IMMEDIATELY after
    upload_file() returns so any post-upload exception cannot re-trigger
    delivery.
    """
    from core.session import get_client
    from services.utils import make_tmp, cleanup, human_size
    from services.uploader import upload_file
    from services.task_runner import tracker, TaskRecord
    import time as _time

    # Register as in-flight so poller does not stop prematurely
    _in_flight_delivery_tids.add(job.job_id)

    client    = get_client()
    tmp       = make_tmp(cfg.download_dir, job.uid)
    fname     = job.output_name or job.fname or "output.mp4"
    dest      = os.path.join(tmp, fname)
    uploaded  = False

    # ── TaskRunner: register CC-download task ───────────────
    dl_tid = tracker.new_tid()
    dl_rec = TaskRecord(
        tid=dl_tid, user_id=job.uid,
        label=f"CC↓ {fname}",
        fname=fname,
        mode="dl", engine="http",
        state="☁️ CloudConvert ready",
    )
    await tracker.register(dl_rec)

    try:
        await client.send_message(
            job.uid,
            f"☁️ <b>CloudConvert Finished</b>\n"
            f"──────────────────────\n\n"
            f"📁 <code>{fname[:50]}</code>\n\n"
            f"⬇️ <i>Downloading from CloudConvert CDN…</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        # Progress callback feeds the TaskRunner tracker
        _dl_start = _time.time()
        _dl_last  = [_dl_start]

        async def _dl_progress(done: int, total: int) -> None:
            now = _time.time()
            if now - _dl_last[0] < 3.0:
                return
            _dl_last[0] = now
            elapsed = now - _dl_start
            speed   = done / elapsed if elapsed else 0.0
            eta     = int((total - done) / speed) if (speed and total > done) else 0
            await tracker.update(
                dl_tid,
                state="⬇️ Downloading from CC…",
                done=done, total=total,
                speed=speed, eta=eta,
                elapsed=elapsed,
            )

        await tracker.update(dl_tid, state="⬇️ Downloading from CC…")
        dl_bytes = await _download_export(job.export_url, dest, progress_cb=_dl_progress)
        await tracker.finish(dl_tid, success=True)
        log.info("[CCStatus] Downloaded %s → %s (%s)",
                 fname, dest, human_size(dl_bytes))

        st = await client.send_message(
            job.uid,
            f"📤 <b>Uploading…</b>\n<code>{fname[:50]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        # FIX UPLOAD-USER-ID: pass user_id so prefix/suffix/auto-forward apply
        await upload_file(client, st, dest, user_id=job.uid)

        # CRITICAL: mark uploaded IMMEDIATELY so no post-upload failure
        # (LOG_CHANNEL forward, cleanup, etc.) can cause a re-delivery.
        uploaded = True
        try:
            await cc_job_store.mark_uploaded(job.job_id)
        except Exception as _e:
            log.warning("[CCStatus] mark_uploaded failed for %s: %s — "
                        "continuing (local guard prevents retry)", job.job_id, _e)
        log.info("[CCStatus] Delivered job %s to uid=%d", job.job_id, job.uid)

    except Exception as exc:
        log.error("[CCStatus] Delivery failed for job %s: %s", job.job_id, exc)
        # Mark CC download task as failed if it wasn't completed yet
        dl_state = tracker._tasks.get(dl_tid)
        if dl_state and not dl_state.is_terminal:
            await tracker.finish(dl_tid, success=False, msg=str(exc)[:60])

        if uploaded:
            # File IS in Telegram — do NOT schedule a retry.
            log.info("[CCStatus] Post-upload error for %s ignored — "
                     "user already received the file", job.job_id)
        else:
            # Upload did not complete — release the claim so the poller
            # can retry on its next cycle (up to 3 times).
            retry_count = getattr(job, "_delivery_retries", 0)
            if retry_count < 3:
                job._delivery_retries = retry_count + 1
                try:
                    await cc_job_store.release_claim(job.job_id)
                    log.info("[CCStatus] Will retry delivery for %s (attempt %d/3)",
                             job.job_id, retry_count + 1)
                    # FIX DELIVERY-RETRY-RESTART: ensure the poller is running
                    # to pick up the re-opened job — it may have gone idle.
                    _ensure_poller()
                except Exception:
                    pass
            try:
                await client.send_message(
                    job.uid,
                    f"❌ <b>CloudConvert delivery failed</b>\n"
                    f"<code>{fname}</code>\n\n"
                    f"<code>{str(exc)[:200]}</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception:
                pass
    finally:
        # Unregister from in-flight tracking regardless of outcome
        _in_flight_delivery_tids.discard(job.job_id)
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
        # FIX POLLER-PREMATURE-STOP: include in-flight deliveries in "busy" check.
        # Without this, the poller went idle as soon as a job was claimed
        # (notified=True removes it from undelivered_jobs), even though a
        # download+upload task could still be running for many minutes.
        in_flight   = bool(_in_flight_delivery_tids)

        interval = 5 if (active or undelivered or in_flight) else 60

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
                        claimed = await cc_job_store.try_claim_delivery(job.job_id)
                        if claimed:
                            asyncio.create_task(_deliver_job(
                                cc_job_store.get(job.job_id) or job
                            ))
                        else:
                            log.info("[CCStatus] job %s already claimed "
                                     "(webhook likely beat poller) — skip", job.job_id)
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

        # Pick up jobs resolved on CC but not yet delivered to Telegram
        for job in cc_job_store.undelivered_jobs():
            claimed = await cc_job_store.try_claim_delivery(job.job_id)
            if not claimed:
                continue
            log.info("[CCStatus] Claim-delivering job %s to uid=%d", job.job_id, job.uid)
            asyncio.create_task(_deliver_job(job))

        # FIX POLLER-PREMATURE-STOP: only count idle when ALL of these are empty:
        #   active_jobs, undelivered_jobs, open panels, in-flight deliveries.
        # Previously in_flight was missing — poller stopped while uploads ran.
        is_idle = (
            not cc_job_store.active_jobs()
            and not cc_job_store.undelivered_jobs()
            and not _open_panels
            and not _in_flight_delivery_tids
        )
        if is_idle:
            consecutive_idle += 1
            if consecutive_idle >= 3:
                log.info("[CCStatus] Poller stopping — no active jobs, no open panels, "
                         "no in-flight deliveries")
                return
        else:
            consecutive_idle = 0

        await asyncio.sleep(interval)
