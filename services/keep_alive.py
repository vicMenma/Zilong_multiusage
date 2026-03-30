"""
services/keep_alive.py
Proper Colab keep-alive via HTTP health server + ngrok tunnel.

Replaces the fragile JS-click hack in colab_launcher.py.

How it works:
  1. Starts an aiohttp HTTP server on port 8080 inside the bot process
  2. Exposes it publicly via ngrok (reuses the existing NGROK_TOKEN)
  3. Prints the public URL so you can add it to UptimeRobot
  4. UptimeRobot pings every 5 minutes → Colab sees network activity → stays alive

UptimeRobot setup (free):
  1. Go to uptimerobot.com → Add New Monitor
  2. Monitor Type: HTTP(s)
  3. URL: paste the ngrok URL printed below
  4. Monitoring Interval: 5 minutes
  5. Save — done.

If NGROK_TOKEN is not set, the server runs on localhost only and the
existing JS heartbeat in colab_launcher.py remains the fallback.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

log = logging.getLogger(__name__)

_runner: Optional[object] = None
_site:   Optional[object] = None
_public_url: str = ""

_START_TIME = time.time()


async def _handle_health(request) -> "web.Response":
    from aiohttp import web
    from services.utils import human_dur
    from core.bot_name import get_bot_name
    from services.task_runner import tracker

    uptime    = human_dur(int(time.time() - _START_TIME))
    active    = len(tracker.active_tasks())
    bot_name  = get_bot_name()

    return web.json_response({
        "status":   "online",
        "bot":      bot_name,
        "uptime":   uptime,
        "active_tasks": active,
        "timestamp": int(time.time()),
    })


async def _handle_root(request) -> "web.Response":
    from aiohttp import web
    from core.bot_name import get_bot_name
    bot_name = get_bot_name()
    html = f"""<!DOCTYPE html>
<html>
<head><title>{bot_name} Bot</title></head>
<body style="font-family:monospace;background:#111;color:#0f0;padding:20px">
<h2>⚡ {bot_name.upper()} BOT</h2>
<p>Status: <b>ONLINE</b></p>
<p>Uptime: {int(time.time() - _START_TIME)}s</p>
<p><a href="/health" style="color:#0f0">/health (JSON)</a></p>
</body></html>"""
    return web.Response(text=html, content_type="text/html")


def _build_app():
    from aiohttp import web
    app = web.Application()
    app.router.add_get("/",       _handle_root)
    app.router.add_get("/health", _handle_health)
    app.router.add_get("/ping",   _handle_health)
    return app


async def start(port: int = 8080, ngrok_token: str = "") -> str:
    """
    Start the health server. Returns the public URL (empty if no ngrok).
    Call this from main.py after the bot starts.
    """
    global _runner, _site, _public_url

    from aiohttp import web

    app     = _build_app()
    _runner = web.AppRunner(app)
    await _runner.setup()
    _site = web.TCPSite(_runner, "0.0.0.0", port)
    await _site.start()
    log.info("🏥 Health server listening on 0.0.0.0:%d", port)

    if not ngrok_token:
        log.info(
            "ℹ️  No NGROK_TOKEN — health server is localhost only.\n"
            "    Set NGROK_TOKEN in Colab Secrets to get a public URL for UptimeRobot."
        )
        return ""

    try:
        from pyngrok import ngrok, conf
        conf.get_default().auth_token = ngrok_token

        # Use a named tunnel so we don't stack up multiple tunnels on restart
        try:
            ngrok.disconnect("http://localhost:" + str(port))
        except Exception:
            pass

        tunnel      = ngrok.connect(port, "http", bind_tls=True)
        _public_url = tunnel.public_url
        log.info("🌐 Keep-alive public URL: %s", _public_url)
        return _public_url

    except ImportError:
        log.error("pyngrok not installed — pip install pyngrok")
    except Exception as exc:
        log.error("ngrok error: %s", exc)

    return ""


async def stop() -> None:
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
    _runner = _site = None
