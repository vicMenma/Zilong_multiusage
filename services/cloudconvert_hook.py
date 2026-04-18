"""
services/cloudconvert_hook.py
Receives CloudConvert webhooks and auto-downloads + uploads
finished files through the existing Zilong pipeline.

Tunnel priority (automatic, no config needed):
  1. Cloudflare Tunnel (cloudflared) — preferred, no account required
  2. ngrok                           — fallback if NGROK_TOKEN is set
  3. localhost only                  — if both fail

CRITICAL FIX — BUG-01: _process_file now uses download_direct() instead of smart_download().
  CC export URLs are single-use signed tokens.  smart_download() plumbs through
  the full TaskRecord / tracker machinery and may trigger retry logic that burns
  the token.  download_direct() is a single streaming GET — exactly what CC needs.
  Previous code had "CRITICAL FIX: use download_direct" in the docstring but still
  called smart_download in the implementation — this is now corrected.

CRITICAL FIX — BUG-12: _verify_signature now strips the "sha256=" prefix.
  CloudConvert sends the CloudConvert-Signature header as "sha256=<hex>".
  The previous compare_digest(expected, signature) always returned False
  because expected was just <hex> while signature was "sha256=<hex>".
  Effect: every webhook was rejected with 403 whenever CC_WEBHOOK_SECRET was set,
  making the webhook feature completely non-functional in production.

CRITICAL FIX — BUG-WH-DOUBLE: start_webhook_server() no longer calls
  _register_webhook_all_keys() (previously at the preset-URL path AND the
  cloudflare/ngrok tunnel path). That caused TWO webhook registration passes
  per startup:
    Pass 1 (start_webhook_server): creates N webhooks (1 per CC key).
    Pass 2 (on_tunnel_ready in main.py): deletes Pass-1 webhooks + N more old
      ones (paginated), then creates N fresh ones.
  Net: N webhooks — but Pass 1 used a non-paginated GET so it only ever cleaned
  up page-1 worth of stale webhooks. Over many restarts (e.g. Colab sessions)
  these accumulate (user hit 52). Now start_webhook_server() only starts the
  HTTP server, opens the tunnel, and calls set_tunnel_url(). ALL webhook
  registration/cleanup is handled exclusively by on_tunnel_ready() → sync_cc_webhooks()
  which paginates properly and is the single authoritative registration path.

CRITICAL FIX — BUG-WH-FINISH: _deliver_cc_job_direct() now calls
  cc_job_store.finish() instead of cc_job_store.update() so that finished_at
  is set. Without it, delivered jobs never evict from the store and the
  ccstatus panel shows stale "Uploading…" state permanently.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import subprocess
import re
import atexit

from aiohttp import web

log = logging.getLogger(__name__)

WEBHOOK_SECRET: str = ""
_web_runner = None
_web_site = None
_tunnel_proc: subprocess.Popen | None = None
LISTEN_PORT = 8765

# Tunnel detection timeout — short so bot never hangs on startup
_TUNNEL_TIMEOUT = 30


def _verify_signature(payload: bytes, signature: str) -> bool:
    if not WEBHOOK_SECRET:
        return True
    expected = hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    # FIX BUG-12: CC sends "sha256=<hex>" — strip the prefix before comparing.
    # Previously compare_digest(expected, signature) always failed because
    # expected == "<hex>" but signature == "sha256=<hex>".
    sig_hex = signature.removeprefix("sha256=")
    return hmac.compare_digest(expected, sig_hex)


def _extract_urls(data: dict) -> list[dict]:
    results = []
    job = data.get("job", {})
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
                    "url": url,
                    "filename": f.get("filename", "cloudconvert_file"),
                })
    return results


async def _process_file(url: str, filename: str, owner_id: int) -> None:
    from core.config import cfg
    from core.session import get_client, settings as _settings
    # FIX BUG-01: use download_direct — NOT smart_download.
    # CC export URLs are single-use signed S3 tokens.  smart_download builds a
    # TaskRecord and may retry / range-request the URL, consuming the token and
    # returning a corrupt file.  download_direct is a single streaming GET.
    from services.downloader import download_direct
    from services.uploader import upload_file
    from services.utils import cleanup, make_tmp, smart_clean_filename, largest_file, human_size

    client = get_client()
    tmp = make_tmp(cfg.download_dir, owner_id)

    try:
        path = await download_direct(url, tmp)

        if os.path.isdir(path):
            resolved = largest_file(path)
            if resolved:
                path = resolved

        if not os.path.isfile(path):
            log.error("[CC-Hook] No file after download: %s", filename)
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert download failed</b>\n<code>{filename}</code>\n<i>No output file found.</i>",
                parse_mode="html",
            )
            cleanup(tmp)
            return

        fsize = os.path.getsize(path)
        if fsize > cfg.file_limit_b:
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert file too large</b>\n<code>{filename}</code>\n"
                f"Size: <code>{human_size(fsize)}</code>\nLimit: <code>{human_size(cfg.file_limit_b)}</code>",
                parse_mode="html",
            )
            cleanup(tmp)
            return

        s = await _settings.get(owner_id)
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

        from types import SimpleNamespace
        dummy_msg = SimpleNamespace(
            edit=lambda *a, **kw: asyncio.sleep(0),
            delete=lambda: asyncio.sleep(0),
            chat=SimpleNamespace(id=owner_id),
        )
        await upload_file(client, dummy_msg, path, user_id=owner_id)

    except Exception as exc:
        log.error("[CC-Hook] Pipeline failed for %s: %s", filename, exc)
        try:
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert auto-upload failed</b>\n<code>{filename}</code>\n"
                f"<code>{str(exc)[:200]}</code>",
                parse_mode="html",
            )
        except Exception:
            pass
    finally:
        cleanup(tmp)


async def _deliver_cc_job_direct(job_id: str, export_url: str, filename: str) -> None:
    """
    Direct delivery path used by the webhook.
    Uses try_claim_delivery() to atomically claim the job so the poller
    cannot race us on the same job_id.

    If this task crashes mid-delivery, C-04 retry logic in _deliver_job
    (called on next poll cycle) will un-mark notified and retry.
    """
    from services.cc_job_store import cc_job_store
    from core.session import get_client

    job = cc_job_store.get(job_id)
    if job is None:
        log.info("[CC-Hook] job %s not in store — external submission", job_id)
        return

    # Atomically claim delivery — returns False if poller already claimed it.
    claimed = await cc_job_store.try_claim_delivery(job_id)
    if not claimed:
        log.info("[CC-Hook] job %s already claimed by poller — skip", job_id)
        return

    # FIX BUG-WH-FINISH: use finish() not update() so finished_at is set.
    # Without finished_at the job never evicts from the store and the panel
    # stays stuck showing the wrong status.
    await cc_job_store.finish(job_id, export_url=export_url)

    log.info("[CC-Hook] Direct-delivering job %s → uid=%d file=%s",
             job_id, job.uid, filename)

    from plugins.ccstatus import _deliver_job
    job_refreshed = cc_job_store.get(job_id) or job
    try:
        await _deliver_job(job_refreshed)
    except Exception as exc:
        log.error("[CC-Hook] direct delivery crashed for %s: %s", job_id, exc)
        # _deliver_job handles its own retry bookkeeping (C-04)


async def handle_cloudconvert(request: web.Request) -> web.Response:
    from core.config import cfg

    try:
        body = await request.read()
        sig = request.headers.get("CloudConvert-Signature", "")
        if WEBHOOK_SECRET and not _verify_signature(body, sig):
            log.warning("[CC-Hook] Invalid signature — rejected")
            return web.json_response({"error": "invalid signature"}, status=403)

        data = await request.json()
        event = data.get("event", "")
        log.info("[CC-Hook] Received event: %s", event)

        # Handle both job.finished and job.failed
        if event == "job.failed":
            job_id = (data.get("job") or {}).get("id", "")
            err_msg = "CloudConvert reported failure"
            try:
                for t in (data.get("job") or {}).get("tasks", []):
                    if t.get("status") == "error":
                        m = t.get("message") or t.get("code") or ""
                        if m:
                            err_msg = m
                            break
            except Exception:
                pass
            if job_id:
                try:
                    from services.cc_job_store import cc_job_store
                    job_rec = cc_job_store.get(job_id)
                    if job_rec is not None and not job_rec.notified:
                        await cc_job_store.finish(job_id, error_msg=err_msg)
                        try:
                            from core.session import get_client
                            await get_client().send_message(
                                job_rec.uid,
                                f"❌ <b>CloudConvert failed</b>\n"
                                f"<code>{job_rec.fname[:50]}</code>\n\n"
                                f"<code>{err_msg[:200]}</code>",
                                parse_mode="html",
                            )
                            await cc_job_store.mark_notified(job_id)
                        except Exception as exc:
                            log.warning("[CC-Hook] notify fail for %s: %s", job_id, exc)
                except Exception as exc:
                    log.warning("[CC-Hook] store update (failed) %s: %s", job_id, exc)
            return web.json_response({"status": "failure_acknowledged"})

        if event != "job.finished":
            return web.json_response({"status": "ignored", "event": event})

        files = _extract_urls(data)
        if not files:
            log.warning("[CC-Hook] No export URLs in payload")
            return web.json_response({"status": "no_urls"})

        job_id = (data.get("job") or {}).get("id", "")

        # If job is tracked by the bot, use try_claim_delivery + direct delivery.
        # This fixes auto-return: previously the webhook deferred to the poller,
        # and if the poller had gone idle/died, the user never got the file.
        if job_id:
            try:
                from services.cc_job_store import cc_job_store
                job_rec = cc_job_store.get(job_id)
                if job_rec is not None:
                    export_url = files[0]["url"] if files else ""
                    filename   = files[0]["filename"] if files else job_rec.output_name
                    # Deliver in background so we can return 200 to CC fast
                    asyncio.create_task(
                        _deliver_cc_job_direct(job_id, export_url, filename)
                    )
                    return web.json_response({
                        "status": "ok", "delivery": "direct", "job_id": job_id,
                    })
            except Exception as exc:
                log.warning("[CC-Hook] cc_job_store check for %s: %s — falling back to direct upload",
                            job_id, exc)

        # Job not in store (external CC submission) — process file directly.
        for f in files:
            asyncio.create_task(_process_file(f["url"], f["filename"], cfg.owner_id))

        log.info("[CC-Hook] Enqueued %d file(s) for direct delivery", len(files))
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
    # FreeConvert per-job webhooks
    try:
        from plugins.fc_webhook import handle_fc_webhook
        app.router.add_post("/fc-webhook", handle_fc_webhook)
        log.info("[CC-Hook] /fc-webhook route registered")
    except Exception as _e:
        log.warning("[CC-Hook] Could not register /fc-webhook route: %s", _e)
    return app


async def _register_webhook_with_cc(webhook_url: str, api_key: str, secret: str) -> bool:
    """Register (or replace) webhook on a single CloudConvert account."""
    import aiohttp as _aiohttp
    CC_API = "https://api.cloudconvert.com/v2"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"url": webhook_url, "events": ["job.finished", "job.failed"]}
    if secret:
        payload["signing_secret"] = secret

    try:
        async with _aiohttp.ClientSession() as sess:
            # delete existing webhooks on this account first
            async with sess.get(f"{CC_API}/webhooks", headers=headers) as resp:
                if resp.status == 200:
                    existing = (await resp.json()).get("data", [])
                    for wh in existing:
                        wh_id = wh.get("id")
                        if wh_id:
                            await sess.delete(f"{CC_API}/webhooks/{wh_id}", headers=headers)
                            log.info("[CC-Hook] Deleted old webhook id=%s (key=...%s)",
                                     wh_id, api_key[-6:])

            async with sess.post(f"{CC_API}/webhooks", json=payload, headers=headers) as resp:
                data = await resp.json()
                if resp.status in (200, 201):
                    wh_id = (data.get("data") or {}).get("id", "?")
                    log.info("[CC-Hook] Webhook registered: id=%s url=%s key=...%s",
                             wh_id, webhook_url, api_key[-6:])
                    return True
                else:
                    log.warning("[CC-Hook] Webhook registration failed (key=...%s): %s %s",
                                api_key[-6:], resp.status, data.get("message", str(data)))
                    return False
    except Exception as exc:
        log.warning("[CC-Hook] Webhook auto-registration error (key=...%s): %s",
                    api_key[-6:], exc)
        return False


async def _register_webhook_all_keys(
    webhook_url: str, api_keys_raw: str, secret: str
) -> tuple[int, int]:
    """
    Register the webhook on EVERY account in the comma-separated CC_API_KEY string.
    Returns (ok_count, total_count).
    """
    from services.cloudconvert_api import parse_api_keys

    keys = parse_api_keys(api_keys_raw)
    if not keys:
        log.warning("[CC-Hook] No valid API keys found in CC_API_KEY — webhook not registered")
        return 0, 0

    log.info("[CC-Hook] Registering webhook on %d account(s)…", len(keys))

    results = await asyncio.gather(
        *[_register_webhook_with_cc(webhook_url, k, secret) for k in keys],
        return_exceptions=True,
    )

    ok = sum(1 for r in results if r is True)
    log.info("[CC-Hook] Webhook registration complete: %d/%d accounts OK", ok, len(keys))
    return ok, len(keys)


# ── Tunnel backends ───────────────────────────────────────────────────────────

def _install_cloudflared() -> bool:
    """Download cloudflared binary if not already present."""
    if subprocess.run(["which", "cloudflared"], capture_output=True).returncode == 0:
        return True
    log.info("[CC-Hook] Installing cloudflared…")
    r = subprocess.run(
        "curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest"
        "/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared"
        " && chmod +x /usr/local/bin/cloudflared",
        shell=True, capture_output=True,
    )
    return r.returncode == 0


async def _open_cloudflare_tunnel(port: int) -> str:
    global _tunnel_proc

    loop = asyncio.get_event_loop()
    ok = await loop.run_in_executor(None, _install_cloudflared)
    if not ok:
        log.warning("[CC-Hook] cloudflared install failed")
        return ""

    try:
        proc = subprocess.Popen(
            ["cloudflared", "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )
        _tunnel_proc = proc
        atexit.register(lambda: proc.terminate())

        deadline = loop.time() + _TUNNEL_TIMEOUT

        while loop.time() < deadline:
            try:
                line = await asyncio.wait_for(
                    loop.run_in_executor(None, proc.stdout.readline),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                if proc.poll() is not None:
                    break
                continue

            if not line:
                break

            log.debug("[cloudflared] %s", line.rstrip())
            m = re.search(r"(https://[a-z0-9\-]+\.trycloudflare\.com)", line)
            if m:
                url = m.group(1).rstrip("/")
                log.info("[CC-Hook] Cloudflare tunnel active: %s", url)
                return url

        log.warning("[CC-Hook] cloudflared timed out after %ds", _TUNNEL_TIMEOUT)
        return ""

    except Exception as exc:
        log.warning("[CC-Hook] cloudflared error: %s", exc)
        return ""


async def _open_ngrok_tunnel(port: int, token: str) -> str:
    global _tunnel_proc

    # Try pyngrok
    try:
        from pyngrok import ngrok as _ngrok, conf as _conf
        _conf.get_default().auth_token = token
        loop = asyncio.get_event_loop()
        tunnel = await loop.run_in_executor(None, lambda: _ngrok.connect(port, "http"))
        url = tunnel.public_url
        if url.startswith("http://"):
            url = "https://" + url[7:]
        log.info("[CC-Hook] ngrok tunnel (pyngrok) active: %s", url)
        return url.rstrip("/")
    except ImportError:
        log.info("[CC-Hook] pyngrok not installed — trying ngrok CLI")
    except Exception as exc:
        log.warning("[CC-Hook] pyngrok failed: %s — trying CLI", exc)

    # Try ngrok CLI
    try:
        proc = subprocess.Popen(
            ["ngrok", "http", str(port), "--log=stdout", f"--authtoken={token}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        _tunnel_proc = proc
        atexit.register(lambda: proc.terminate())

        loop = asyncio.get_event_loop()
        deadline = loop.time() + _TUNNEL_TIMEOUT

        while loop.time() < deadline:
            try:
                line = await asyncio.wait_for(
                    loop.run_in_executor(None, proc.stdout.readline),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                try:
                    import aiohttp as _ah
                    async with _ah.ClientSession() as s:
                        async with s.get(
                            "http://localhost:4040/api/tunnels",
                            timeout=_ah.ClientTimeout(total=2),
                        ) as r:
                            if r.status == 200:
                                data = await r.json()
                                for t in data.get("tunnels", []):
                                    pu = t.get("public_url", "")
                                    if pu.startswith("https://"):
                                        log.info("[CC-Hook] ngrok (API): %s", pu)
                                        return pu.rstrip("/")
                except Exception:
                    pass
                if proc.poll() is not None:
                    break
                continue

            if not line:
                break

            log.debug("[ngrok] %s", line.rstrip())
            m = re.search(r"(https://[a-z0-9\-]+\.ngrok[\-a-z\.io]+)", line)
            if m:
                url = m.group(1).rstrip("/")
                log.info("[CC-Hook] ngrok tunnel (CLI) active: %s", url)
                return url

        log.warning("[CC-Hook] ngrok CLI timed out after %ds", _TUNNEL_TIMEOUT)
        return ""

    except FileNotFoundError:
        log.warning("[CC-Hook] ngrok CLI not found")
        return ""
    except Exception as exc:
        log.warning("[CC-Hook] ngrok CLI error: %s", exc)
        return ""


async def _handle_cc_job(job_id: str, data: dict, api_key: str) -> None:
    """
    Recovery entry-point used by webhook_sync.poll_pending_jobs().
    Called when a CC job completed while the bot was offline (no webhook delivery).

    Uses direct delivery with try_claim_delivery() — matches handle_cloudconvert.
    """
    from core.config import cfg
    files = _extract_urls(data)
    if not files:
        log.warning("[CC-Hook] _handle_cc_job: no export URLs in job %s", job_id)
        return

    if job_id:
        try:
            from services.cc_job_store import cc_job_store
            job_rec = cc_job_store.get(job_id)
            if job_rec is not None:
                export_url = files[0]["url"] if files else ""
                filename   = files[0]["filename"] if files else job_rec.output_name
                log.info("[CC-Hook] Recovery direct-delivery: job %s → uid=%d",
                         job_id, job_rec.uid)
                asyncio.create_task(
                    _deliver_cc_job_direct(job_id, export_url, filename)
                )
                return
        except Exception as exc:
            log.warning("[CC-Hook] Recovery cc_job_store for %s: %s — direct delivery", job_id, exc)

    log.info("[CC-Hook] Recovering offline CC job %s (not in store) — %d file(s)", job_id, len(files))
    for f in files:
        asyncio.create_task(_process_file(f["url"], f["filename"], cfg.owner_id))


# ── Public entry point ────────────────────────────────────────────────────────

async def start_webhook_server(port: int = LISTEN_PORT) -> str:
    global _web_runner, _web_site

    # 1. Start local HTTP server
    app = _build_app()
    _web_runner = web.AppRunner(app)
    await _web_runner.setup()
    _web_site = web.TCPSite(_web_runner, "0.0.0.0", port)
    await _web_site.start()
    log.info("[CC-Hook] Webhook server listening on port %d", port)

    if not WEBHOOK_SECRET:
        log.warning("[CC-Hook] ⚠️  No CC_WEBHOOK_SECRET set — webhook accepts ALL POST requests.")

    from core.config import cfg
    api_key = cfg.cc_api_key or os.environ.get("CC_API_KEY", "").strip()
    ngrok_token = cfg.ngrok_token or os.environ.get("NGROK_TOKEN", "").strip()

    # 2. Use preset URL if provided (VPS / EC2 / manual)
    preset_url = os.environ.get("WEBHOOK_BASE_URL", "").strip().rstrip("/")
    if preset_url:
        webhook_url = f"{preset_url}/webhook/cloudconvert"
        log.info("[CC-Hook] Using preset WEBHOOK_BASE_URL: %s", webhook_url)
        # Store tunnel base URL so FC jobs can build /fc-webhook URLs
        # NOTE: Do NOT register webhooks here — main.py calls on_tunnel_ready()
        # (→ sync_cc_webhooks) which does full paginated cleanup + registration.
        # Registering here AND there causes double-registration per restart.
        try:
            from core.config import set_tunnel_url
            set_tunnel_url(preset_url)
        except Exception:
            pass
        return webhook_url

    # 3. Try Cloudflare tunnel (no account needed, always first)
    log.info("[CC-Hook] Opening Cloudflare tunnel (timeout: %ds)…", _TUNNEL_TIMEOUT)
    public_url = await _open_cloudflare_tunnel(port)

    # 4. Fallback to ngrok
    if not public_url:
        if ngrok_token:
            log.info("[CC-Hook] Cloudflare unavailable — trying ngrok…")
            public_url = await _open_ngrok_tunnel(port, ngrok_token)
        else:
            log.info("[CC-Hook] No NGROK_TOKEN set — skipping ngrok fallback")

    if not public_url:
        log.warning("[CC-Hook] No tunnel available — webhook is localhost-only (no external delivery).")
        return f"http://localhost:{port}/webhook/cloudconvert"

    webhook_url = f"{public_url}/webhook/cloudconvert"

    # Store base URL so FC jobs can build /fc-webhook callback URLs
    try:
        from core.config import set_tunnel_url
        set_tunnel_url(public_url)
    except Exception:
        pass

    # NOTE: Do NOT register webhooks here.
    # main.py calls on_tunnel_ready() → sync_cc_webhooks() which does:
    #   1. Paginated list of ALL existing webhooks (no page-1-only truncation)
    #   2. Delete every stale one
    #   3. Register exactly ONE fresh webhook per API key
    # Registering here as well (step 5 in the old code) caused two webhook
    # registrations on every restart, leading to the 52-webhook accumulation.

    # Notify owner: tunnel is up, webhook sync will follow in main.py
    try:
        from core.session import get_client
        client = get_client()
        tunnel_type = "Cloudflare" if "trycloudflare" in public_url else "ngrok"
        await client.send_message(
            cfg.owner_id,
            f"🌐 <b>{tunnel_type} Tunnel Active</b>\n"
            f"──────────────────────\n\n"
            f"<code>{webhook_url}</code>\n\n"
            "<i>Webhook sync (cleanup + registration) running now…</i>",
            parse_mode="html",
            disable_web_page_preview=True,
        )
    except Exception as notify_exc:
        log.warning("[CC-Hook] Could not notify owner: %s", notify_exc)

    return webhook_url


async def stop_webhook_server():
    global _tunnel_proc
    if _tunnel_proc is not None:
        try:
            _tunnel_proc.terminate()
        except Exception:
            pass
        _tunnel_proc = None
    if _web_site:
        await _web_site.stop()
    if _web_runner:
        await _web_runner.cleanup()
