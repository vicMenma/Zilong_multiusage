"""
services/keep_alive.py
Colab keep-alive via HTTP health server + Serveo SSH tunnel.

How it works:
  1. Starts an aiohttp HTTP server on a free port inside the bot process
  2. Exposes it publicly via Serveo (free, no account, no token needed)
     ssh -R 80:localhost:PORT serveo.net
  3. Prints the public HTTPS URL so you can add it to UptimeRobot
  4. UptimeRobot pings every 5 minutes → Colab sees network activity → stays alive

UptimeRobot setup (free):
  1. Go to uptimerobot.com → Add New Monitor
  2. Monitor Type: HTTP(s)
  3. URL: paste the Serveo URL printed in the logs
  4. Monitoring Interval: 5 minutes
  5. Save — done.
"""
from __future__ import annotations

import logging
import os
import re
import subprocess
import threading
import time
from typing import Optional

from aiohttp import web

log = logging.getLogger(__name__)

_runner:     Optional[object] = None
_site:       Optional[object] = None
_public_url: str = ""
_START_TIME  = time.time()


# ── Health endpoints ──────────────────────────────────────────

async def _handle_health(request) -> web.Response:
    from services.utils import human_dur
    from core.bot_name import get_bot_name
    from services.task_runner import tracker

    uptime   = human_dur(int(time.time() - _START_TIME))
    active   = len(tracker.active_tasks())
    bot_name = get_bot_name()

    return web.json_response({
        "status":       "online",
        "bot":          bot_name,
        "uptime":       uptime,
        "active_tasks": active,
        "timestamp":    int(time.time()),
    })


async def _handle_root(request) -> web.Response:
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


# ── Port finder ───────────────────────────────────────────────

def _find_free_port(start: int = 8080, end: int = 8199) -> int:
    import socket
    for p in range(start, end):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", p))
                return p
        except OSError:
            continue
    raise RuntimeError(f"No free port found in range {start}-{end}")


# ── Serveo tunnel ─────────────────────────────────────────────

def _open_serveo(local_port: int) -> str:
    """
    Open a Serveo SSH reverse tunnel: public HTTPS → localhost:local_port.
    Blocks up to 20 seconds waiting for the URL, then returns it (or "" on failure).
    Keeps the SSH process alive in a daemon thread for the lifetime of the bot.
    """
    result: dict = {"url": ""}

    def _run() -> None:
        cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
            "-R", f"80:localhost:{local_port}",
            "serveo.net",
        ]
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            for line in proc.stdout:
                if not result["url"]:
                    m = re.search(r"(https://\S+\.serveo\.net)", line)
                    if m:
                        result["url"] = m.group(1)
                        log.info("🌐 Serveo tunnel active: %s → localhost:%d",
                                 result["url"], local_port)
        except Exception as exc:
            log.warning("Serveo thread error: %s", exc)

    threading.Thread(target=_run, daemon=True, name="serveo-keepalive").start()

    deadline = time.time() + 20
    while time.time() < deadline:
        if result["url"]:
            return result["url"]
        time.sleep(0.5)

    log.warning("Serveo tunnel did not return a URL within 20s — health is localhost only.")
    return ""


# ── Public API ────────────────────────────────────────────────

async def start(port: int = 8080, ngrok_token: str = "") -> str:
    """
    Start the health server and open a Serveo tunnel.
    `ngrok_token` parameter kept for API compatibility but is unused.
    Returns the public URL (empty string if tunnel fails).
    """
    global _runner, _site, _public_url

    from aiohttp import web

    # If a public URL was pre-set by the launcher via env, use it as-is
    env_url = os.environ.get("HEALTH_PUBLIC_URL", "").strip()
    if env_url:
        log.info("🌐 Keep-alive public URL (from env): %s", env_url)
        _public_url = env_url

    # Find a free port
    try:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("0.0.0.0", port))
    except OSError:
        port = _find_free_port(8081, 8199)
        log.info("🏥 Port 8080 busy — using port %d instead", port)

    app     = _build_app()
    _runner = web.AppRunner(app)
    await _runner.setup()
    _site = web.TCPSite(_runner, "0.0.0.0", port)
    await _site.start()
    log.info("🏥 Health server listening on 0.0.0.0:%d", port)

    # URL already known from env — no tunnel needed
    if _public_url:
        return _public_url

    # Open Serveo tunnel
    url = _open_serveo(port)
    if url:
        _public_url = url
        log.info(
            "📌 Add this URL to UptimeRobot (5-min interval) to keep Colab alive:\n"
            "    %s/health", url,
        )
        return url

    log.warning(
        "⚠️  Serveo tunnel failed — health server is localhost only.\n"
        "    Colab may disconnect after ~90 min without external pings."
    )
    return ""


async def stop() -> None:
    global _runner, _site
    if _site:
        await _site.stop()
    if _runner:
        await _runner.cleanup()
    _runner = _site = None
