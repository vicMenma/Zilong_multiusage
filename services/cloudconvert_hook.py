"""
services/cloudconvert_hook.py
Receives CloudConvert webhooks and auto-downloads + uploads
finished files through the existing Zilong pipeline.

FIX: Added startup warning when CC_WEBHOOK_SECRET is empty.
Without a secret, any POST to the ngrok URL triggers downloads/uploads.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os

from aiohttp import web

log = logging.getLogger(__name__)

WEBHOOK_SECRET: str = ""
_runner = None
_site   = None

LISTEN_PORT = 8765


def _verify_signature(payload: bytes, signature: str) -> bool:
    if not WEBHOOK_SECRET:
        return True
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
    from core.config import cfg
    from core.session import get_client, settings as _settings
    from services.downloader import smart_download
    from services.uploader import upload_file
    from services.task_runner import runner as _runner
    from services.utils import cleanup, make_tmp, smart_clean_filename, largest_file, human_size

    client = get_client()
    tmp    = make_tmp(cfg.download_dir, owner_id)

    try:
        path = await smart_download(url, tmp, user_id=owner_id, label=filename)

        if os.path.isdir(path):
            resolved = largest_file(path)
            if resolved:
                path = resolved

        if not os.path.isfile(path):
            log.error("[CC-Hook] No file after download: %s", filename)
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert download failed</b>\n"
                f"<code>{filename}</code>\n<i>No output file found.</i>",
                parse_mode="html",
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
                parse_mode="html",
            )
            cleanup(tmp)
            return

        s       = await _settings.get(owner_id)
        cleaned = smart_clean_filename(os.path.basename(path))
        name, ext = os.path.splitext(cleaned)
        prefix = s.get("prefix", "").strip()
        suffix = s.get("suffix", "").strip()
        final_name = f"{prefix}{name}{suffix}{ext}"

        if final_name != os.path.basename(path):
            new_path = os.path.join(os.path.dirname(path), final_name)
            try:
                os.rename(path, new_path)
                path = new_path
            except OSError:
                pass

        sem = _runner._get_upload_sem()
        async with sem:
            from types import SimpleNamespace
            dummy_msg = SimpleNamespace(
                edit=lambda *a, **kw: asyncio.sleep(0),
                delete=lambda: asyncio.sleep(0),
                chat=SimpleNamespace(id=owner_id),
            )
            await upload_file(client, dummy_msg, path)

    except Exception as exc:
        log.error("[CC-Hook] Pipeline failed for %s: %s", filename, exc)
        try:
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert auto-upload failed</b>\n"
                f"<code>{filename}</code>\n"
                f"<code>{str(exc)[:200]}</code>",
                parse_mode="html",
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

        if event != "job.finished":
            return web.json_response({"status": "ignored", "event": event})

        files = _extract_urls(data)
        if not files:
            log.warning("[CC-Hook] No export URLs in payload")
            return web.json_response({"status": "no_urls"})

        from core.session import get_client
        client = get_client()

        for f in files:
            url  = f["url"]
            name = f["filename"]

            try:
                await client.send_message(
                    cfg.owner_id,
                    f"☁️ <b>CloudConvert Auto-Upload</b>\n"
                    f"──────────────────────\n\n"
                    f"📁 <b>File:</b> <code>{name[:50]}</code>\n\n"
                    f"<i>Downloading and uploading automatically…</i>",
                    parse_mode="html",
                )
            except Exception as notify_exc:
                log.warning("[CC-Hook] Could not notify owner: %s", notify_exc)

            asyncio.create_task(_process_file(url, name, cfg.owner_id))

        log.info("[CC-Hook] Enqueued %d file(s)", len(files))
        return web.json_response({"status": "ok", "enqueued": [f["filename"] for f in files]})

    except Exception as e:
        log.error("[CC-Hook] Error: %s", e)
        return web.json_response({"error": str(e)}, status=500)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "online", "service": "cloudconvert-webhook"})


def _build_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/webhook/cloudconvert", handle_cloudconvert)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/", handle_health)
    return app


async def start_webhook_server(port: int = LISTEN_PORT, ngrok_token: str = "") -> str:
    global _runner, _site

    app     = _build_app()
    _runner = web.AppRunner(app)
    await _runner.setup()
    _site = web.TCPSite(_runner, "0.0.0.0", port)
    await _site.start()
    log.info("[CC-Hook] Webhook server listening on port %d", port)

    # FIX: warn when no secret is set so operator knows the endpoint is open
    if not WEBHOOK_SECRET:
        log.warning(
            "[CC-Hook] ⚠️  No CC_WEBHOOK_SECRET set — webhook accepts ALL POST requests. "
            "Anyone who discovers the ngrok URL can trigger file uploads. "
            "Set CC_WEBHOOK_SECRET in .env and configure it in CloudConvert's webhook settings."
        )

    public_url = ""
    if ngrok_token:
        try:
            from pyngrok import ngrok, conf
            conf.get_default().auth_token = ngrok_token
            tunnel     = ngrok.connect(port, "http")
            public_url = tunnel.public_url
            webhook_url = f"{public_url}/webhook/cloudconvert"
            log.info("[CC-Hook] ngrok tunnel: %s", public_url)
            log.info("[CC-Hook] Webhook URL: %s", webhook_url)
            return webhook_url
        except ImportError:
            log.error("[CC-Hook] pyngrok not installed — pip install pyngrok")
        except Exception as e:
            log.error("[CC-Hook] ngrok error: %s", e)
    else:
        log.info("[CC-Hook] No NGROK_TOKEN — server on localhost:%d only", port)

    return public_url


async def stop_webhook_server():
    global _runner, _site
    try:
        from pyngrok import ngrok
        ngrok.kill()
    except Exception:
        pass
    if _site:
        await _site.stop()
    if _runner:
        await _runner.cleanup()
