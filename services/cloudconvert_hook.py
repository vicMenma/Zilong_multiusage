"""
services/cloudconvert_hook.py
Receives CloudConvert webhooks and auto-downloads + uploads
finished files through the existing Zilong pipeline.

AWS FIX — why webhook works on Colab but not AWS:
  On Colab: ngrok creates a public tunnel → CC can reach port 8765 → webhooks arrive.
  On AWS:   public IP exists but port 8765 is closed in Security Group AND the
            old code only registered a URL when NGROK_TOKEN was set — so CC
            never knew where to send callbacks.

HOW TO FIX ON AWS:
  Step 1 — Open port 8765 in EC2 Security Group (inbound TCP from 0.0.0.0/0)
  Step 2 — Add to .env:
              WEBHOOK_BASE_URL=http://3.121.160.242:8765
           (replace with your actual EC2 public IP)
  Step 3 — Restart the bot: sudo systemctl restart zilongbot
  The bot will auto-register the webhook with CloudConvert and DM you the URL.

URL priority order:
  1. WEBHOOK_BASE_URL env var  →  AWS / any VPS with static public IP
  2. NGROK_TOKEN env var       →  Colab / dynamic IP via ngrok tunnel
  3. (nothing)                 →  ccstatus poller handles delivery as fallback

The ccstatus poller always runs as a backup regardless of webhook status.
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
    # FIX: CloudConvert sends the signature as "sha256=<hexhash>".
    # Strip the prefix before comparing — without this the HMAC digest
    # never matches and every valid webhook is rejected with 403.
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

        if event not in ("job.finished", "job.failed"):
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
                    f"☁️ <b>CloudConvert Webhook Received</b>\n"
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


# ─────────────────────────────────────────────────────────────
# CC API auto-registration
# ─────────────────────────────────────────────────────────────

async def _register_cc_webhook(base_url: str, api_key: str) -> None:
    """Delete existing CC webhooks, register the new one, DM owner."""
    from core.config import cfg
    import aiohttp as _aio

    webhook_url = f"{base_url.rstrip('/')}/webhook/cloudconvert"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    try:
        async with _aio.ClientSession() as sess:
            # delete old webhooks
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

            # register new webhook
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

    # DM owner
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
            parse_mode="html",
        )
    except Exception as exc:
        log.warning("[CC-Hook] Could not DM owner: %s", exc)


# ─────────────────────────────────────────────────────────────
# Server lifecycle
# ─────────────────────────────────────────────────────────────

async def start_webhook_server(
    port: int = LISTEN_PORT,
    ngrok_token: str = "",
) -> str:
    """
    Start the webhook HTTP server and return the public webhook URL.

    URL priority:
      1. WEBHOOK_BASE_URL env var (AWS EC2 / static public IP) — no ngrok needed
      2. ngrok_token              (Colab / dynamic IP)
      3. empty string             (local only — poller handles delivery)
    """
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
    api_key = cfg.cc_api_key

    # ── Priority 1: static public URL (AWS / VPS) ─────────────────────────
    base_url = os.environ.get("WEBHOOK_BASE_URL", "").strip().rstrip("/")
    if base_url:
        webhook_url = f"{base_url}/webhook/cloudconvert"
        log.info("[CC-Hook] Using static WEBHOOK_BASE_URL: %s", webhook_url)
        if api_key:
            asyncio.create_task(_register_cc_webhook(base_url, api_key))
        else:
            log.warning(
                "[CC-Hook] No CC_API_KEY — auto-registration skipped. "
                "Add %s manually in CloudConvert dashboard.", webhook_url
            )
        return webhook_url

    # ── Priority 2: ngrok (Colab / dynamic IP) ────────────────────────────
    if ngrok_token:
        try:
            from pyngrok import ngrok, conf
            conf.get_default().auth_token = ngrok_token
            tunnel      = ngrok.connect(port, "http")
            public_url  = tunnel.public_url
            webhook_url = f"{public_url}/webhook/cloudconvert"
            log.info("[CC-Hook] ngrok tunnel active: %s", webhook_url)
            if api_key:
                asyncio.create_task(_register_cc_webhook(public_url, api_key))
            return webhook_url
        except ImportError:
            log.error("[CC-Hook] pyngrok not installed — pip install pyngrok")
        except Exception as exc:
            log.error("[CC-Hook] ngrok error: %s", exc)

    # ── Priority 3: no public URL ─────────────────────────────────────────
    log.info(
        "[CC-Hook] No WEBHOOK_BASE_URL or NGROK_TOKEN set. "
        "Webhook server running locally only. "
        "The ccstatus poller will poll CloudConvert API and deliver jobs."
    )
    return ""


async def stop_webhook_server() -> None:
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
