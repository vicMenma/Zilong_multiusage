"""
services/cloudconvert_hook.py
Receives CloudConvert webhooks and auto-downloads + uploads
finished files through the existing Zilong pipeline.

CRITICAL FIX: _process_file previously called smart_download() to retrieve
CC export URLs. smart_download → _dispatch → download_parallel fires 8
simultaneous Range requests. CC export URLs are single-use signed tokens —
the token is consumed on request #1 and requests #2-8 get 403/empty.
The assembled file is only 1/8 of the actual output. Webhook delivery
silently produced truncated/corrupt files on every single CC job.

Fix: use download_direct() instead. CC export URLs are plain HTTPS direct
links — no parallel range splitting needed or wanted. download_direct uses
a single streaming GET, respects the token, and gets the full file.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os

from aiohttp import web
from pyrogram import enums as _enums
from services.cloudconvert_api import parse_api_keys, pick_best_key

log = logging.getLogger(__name__)

WEBHOOK_SECRET: str = ""
_runner = None
_site   = None

LISTEN_PORT = 8765


def _verify_signature(payload: bytes, signature: str) -> bool:
    if not WEBHOOK_SECRET:
        return True
    if signature.startswith("sha256="):
        signature = signature[7:]
    expected = hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def _extract_urls(data: dict) -> list[dict]:
    results = []
    job   = data.get("job", {})
    tasks = job.get("tasks", [])
    for task in tasks:
        if task.get("operation") not in ("export/url",):
            continue
        if task.get("status") != "finished":
            continue
        files = (task.get("result") or {}).get("files", [])
        for f in files:
            url = f.get("url")
            if url:
                results.append({
                    "url":      url,
                    "filename": f.get("filename", "cloudconvert_file"),
                })
    return results


async def _process_file(url: str, filename: str, owner_id: int) -> None:
    # FIX: _enums must be imported at the TOP of this function.
    # It was previously imported at line ~126 (after send_message calls at lines
    # ~93 and ~106 that already use _enums.ParseMode.HTML), causing a NameError
    # on every error path. The log showed: "Could not DM owner: name '_enums' is not defined"
    from pyrogram import enums as _enums
    from core.config import cfg
    from core.session import get_client, settings as _settings
    from services.uploader import upload_file
    from services.utils import cleanup, make_tmp, smart_clean_filename, human_size
    # CC export URLs are single-use signed tokens — use download_direct (single
    # streaming GET), not smart_download which fires 8 parallel Range requests
    # and burns the token on request #1, corrupting the assembled file.
    from services.downloader import download_direct

    client = get_client()
    tmp    = make_tmp(cfg.download_dir, owner_id)

    try:
        path = await download_direct(url, tmp)

        if not os.path.isfile(path):
            log.error("[CC-Hook] No file after download: %s", filename)
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert download failed</b>\n"
                f"<code>{filename}</code>\n<i>No output file found.</i>",
                parse_mode=_enums.ParseMode.HTML,
            )
            cleanup(tmp)
            return

        fsize = os.path.getsize(path)
        if fsize > cfg.file_limit_b:
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert file too large</b>\n"
                f"<code>{filename}</code>\n"
                f"Size: <code>{human_size(fsize)}</code>\n"
                f"Limit: <code>{human_size(cfg.file_limit_b)}</code>",
                parse_mode=_enums.ParseMode.HTML,
            )
            cleanup(tmp)
            return

        s          = await _settings.get(owner_id)
        cleaned    = smart_clean_filename(os.path.basename(path))
        name, ext  = os.path.splitext(cleaned)
        prefix     = s.get("prefix", "").strip()
        suffix     = s.get("suffix", "").strip()
        final_name = f"{prefix}{name}{suffix}{ext}"

        if final_name != os.path.basename(path):
            new_path = os.path.join(os.path.dirname(path), final_name)
            try:
                os.rename(path, new_path)
                path = new_path
            except OSError:
                pass

        st = await client.send_message(
            owner_id,
            f"📤 <b>Uploading…</b>\n<code>{os.path.basename(path)}</code>",
            parse_mode=_enums.ParseMode.HTML,
        )
        await upload_file(client, st, path, user_id=owner_id)

    except Exception as exc:
        log.error("[CC-Hook] Pipeline failed for %s: %s", filename, exc)
        try:
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert auto-upload failed</b>\n"
                f"<code>{filename}</code>\n"
                f"<code>{str(exc)[:200]}</code>",
                parse_mode=_enums.ParseMode.HTML,
            )
        except Exception:
            pass
    finally:
        cleanup(tmp)


async def handle_cloudconvert(request: web.Request) -> web.Response:
    from core.config import cfg

    try:
        body = await request.read()

        sig = request.headers.get("CloudConvert-Signature", "")
        if WEBHOOK_SECRET and not _verify_signature(body, sig):
            log.warning("[CC-Hook] Invalid signature — rejected")
            return web.json_response({"error": "invalid signature"}, status=403)

        data  = await request.json()
        event = data.get("event", "")
        log.info("[CC-Hook] Received event: %s", event)

        if event not in ("job.finished", "job.failed"):
            return web.json_response({"status": "ignored", "event": event})

        job   = data.get("job", {})
        job_id = job.get("id", "")

        # Extract files
        files = _extract_urls(data)

        if not files:
            log.warning("[CC-Hook] No export URLs in payload (job_id=%s)", job_id)
            return web.json_response({"status": "no_urls"})

        # -------------------------------
        # ✅ UPDATE cc_job_store (SAFE)
        # -------------------------------
        try:
            from services.cc_job_store import cc_job_store

            job_obj = cc_job_store.get(job_id)

            if job_obj and not job_obj.notified:
                await cc_job_store.finish(job_id, export_url=files[0]["url"])
                log.info("[CC-Hook] Updated cc_job_store for job %s", job_id)

            elif not job_obj:
                # Inject external job (optional fallback)
                try:
                    from services.cc_job_store import CCJob
                    import time

                    synthetic = CCJob(
                        job_id      = job_id,
                        uid         = cfg.owner_id,
                        fname       = files[0]["filename"],
                        output_name = files[0]["filename"],
                        status      = "finished",
                        export_url  = files[0]["url"],
                        finished_at = time.time(),
                    )

                    await cc_job_store.add(synthetic)

                    log.info("[CC-Hook] Injected external job %s", job_id)

                except Exception as inj_exc:
                    log.error("[CC-Hook] Injection failed: %s", inj_exc)

        except Exception as store_exc:
            log.warning("[CC-Hook] cc_job_store update failed: %s", store_exc)

        # -------------------------------
        # 🚀 IMMEDIATE DOWNLOAD (MAIN FIX)
        # -------------------------------
        for f in files:
            url = f.get("url")
            filename = f.get("filename", "file")

            if not url:
                continue

            log.info("[CC-Hook] Triggering download: %s (%s)", filename, job_id)

            asyncio.create_task(
                _safe_process_file(url, filename, cfg.owner_id, job_id)
            )

        return web.json_response({
            "status": "ok",
            "files": [f["filename"] for f in files]
        })

    except Exception as exc:
        log.error("[CC-Hook] Error: %s", exc)
        return web.json_response({"error": str(exc)}, status=500)

async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "online", "service": "cloudconvert-webhook"})


def _build_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/webhook/cloudconvert", handle_cloudconvert)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/", handle_health)
    return app


async def _register_cc_webhook(base_url: str, api_key: str) -> None:
    from core.config import cfg
    import aiohttp as _aio

    webhook_url = f"{base_url.rstrip('/')}/webhook/cloudconvert"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    try:
        async with _aio.ClientSession() as sess:
            async with sess.get(
                "https://api.cloudconvert.com/v2/webhooks", headers=headers
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for wh in data.get("data", []):
                        wh_id = wh.get("id")
                        if wh_id:
                            async with sess.delete(
                                f"https://api.cloudconvert.com/v2/webhooks/{wh_id}",
                                headers=headers,
                            ) as dr:
                                if dr.status in (200, 204):
                                    log.info("[CC-Hook] Deleted old webhook %s", wh_id)

            payload: dict = {
                "url":    webhook_url,
                "events": ["job.finished", "job.failed"],
            }
            if WEBHOOK_SECRET:
                payload["signing_secret"] = WEBHOOK_SECRET

            async with sess.post(
                "https://api.cloudconvert.com/v2/webhooks",
                json=payload, headers=headers,
            ) as resp:
                data = await resp.json()
                if resp.status in (200, 201):
                    wh_id = (data.get("data") or data).get("id", "?")
                    log.info("[CC-Hook] Webhook registered id=%s → %s", wh_id, webhook_url)
                else:
                    log.error("[CC-Hook] Registration failed %d: %s", resp.status, data)

    except Exception as exc:
        log.error("[CC-Hook] Registration error: %s", exc)

    try:
        from core.session import get_client
        client = get_client()
        await client.send_message(
            cfg.owner_id,
            f"☁️ <b>CloudConvert Webhook Active</b>\n"
            f"──────────────────────\n\n"
            f"🌐 <b>URL:</b>\n<code>{webhook_url}</code>\n\n"
            f"🔑 <b>Secret:</b> {'✅ Set' if WEBHOOK_SECRET else '❌ None (open endpoint)'}\n\n"
            f"<i>Auto-registered with CloudConvert.\n"
            f"ccstatus poller also running as backup.</i>",
            parse_mode=_enums.ParseMode.HTML,
        )
    except Exception as exc:
        log.warning("[CC-Hook] Could not DM owner: %s", exc)


async def start_webhook_server(
    port: int = LISTEN_PORT,
    ngrok_token: str = "",   # kept for API compat, unused
) -> str:
    global _runner, _site

    app     = _build_app()
    _runner = web.AppRunner(app)
    await _runner.setup()
    _site = web.TCPSite(_runner, "0.0.0.0", port)
    await _site.start()
    log.info("[CC-Hook] Webhook server listening on 0.0.0.0:%d", port)

    if not WEBHOOK_SECRET:
        log.warning(
            "[CC-Hook] No CC_WEBHOOK_SECRET — webhook accepts ALL POST requests."
        )

    from core.config import cfg
    raw_keys = cfg.cc_api_key

    base_url = os.environ.get("WEBHOOK_BASE_URL", "").strip().rstrip("/")
    if base_url:
        webhook_url = f"{base_url}/webhook/cloudconvert"
        log.info("[CC-Hook] Using WEBHOOK_BASE_URL: %s", webhook_url)
        if raw_keys:
            async def _register_with_best_key() -> None:
                api_keys = parse_api_keys(raw_keys)
                log.info("[CC-Hook] Checking credits on %d API key(s)…", len(api_keys))
                try:
                    best_key, credits = await pick_best_key(api_keys)
                    log.info("[CC-Hook] Using key with %d credits for webhook registration", credits)
                    await _register_cc_webhook(base_url, best_key)
                except RuntimeError as exc:
                    log.error("[CC-Hook] All CC API keys exhausted: %s", exc)
                except Exception as exc:
                    log.error("[CC-Hook] Key selection failed: %s", exc)
            asyncio.create_task(_register_with_best_key())
        else:
            log.warning(
                "[CC-Hook] No CC_API_KEY — auto-registration skipped. "
                "Add %s manually in CloudConvert dashboard.", webhook_url
            )
        return webhook_url

    log.info(
        "[CC-Hook] No WEBHOOK_BASE_URL set. "
        "Webhook server running locally only. "
        "The ccstatus poller will poll CloudConvert API and deliver jobs."
    )
    return ""


async def stop_webhook_server() -> None:
    global _runner, _site
    if _site:
        await _site.stop()
    if _runner:
        await _runner.cleanup()


async def _safe_process_file(url: str, filename: str, user_id: int, job_id: str):
    try:
        log.info("[CC-Download] Starting: %s (%s)", filename, job_id)
        await _process_file(url, filename, user_id)
        log.info("[CC-Download] Completed: %s (%s)", filename, job_id)
    except Exception as e:
        log.error("[CC-Download] FAILED for %s (%s): %s", filename, job_id, str(e), exc_info=True)
