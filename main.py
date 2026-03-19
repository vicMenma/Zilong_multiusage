"""
Zilong Bot — main.py
Entry point. Loads config, builds client, registers plugins, starts.

Koyeb support: if KOYEB=1 is set in env, a lightweight HTTP health-check
server is started on port PORT (default 8000) so Koyeb's health probe passes.
"""
import asyncio
import logging
import os
import glob

# ── uvloop: must be installed BEFORE asyncio.run() is ever called ────────────
try:
    import uvloop
    uvloop.install()
    _UVLOOP = True
except ImportError:
    _UVLOOP = False
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("zilong.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

if _UVLOOP:
    log.info("⚡ uvloop active — high-performance event loop enabled")
else:
    log.warning("⚠️  uvloop not installed — using default asyncio event loop (slower)")

# Remove stale sessions before import
for _f in glob.glob("*.session") + glob.glob("*.session-journal"):
    try:
        os.remove(_f)
        log.info("Removed stale session: %s", _f)
    except OSError:
        pass

from pyrogram import Client, idle
from core.config import cfg
from services.task_runner import runner



# ── workers parameter ─────────────────────────────────────────────────────────
# pyrofork's Client accepts 'workers' (default 4) which sets the dispatcher
# thread pool size. Higher values improve async scheduling under heavy I/O.
_WORKERS = int(os.environ.get("BOT_WORKERS", "16"))


def build_client() -> Client:
    import inspect
    kwargs: dict = dict(
        name="ZilongBot",
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        bot_token=cfg.bot_token,
        plugins={"root": "plugins"},
        workdir="/tmp",
    )
    sig = inspect.signature(Client.__init__)
    if "workers" in sig.parameters:
        kwargs["workers"] = _WORKERS
        log.info("⚙️  workers=%d (async dispatch pool)", _WORKERS)
    # concurrent_transmissions: pyrofork opens N independent encrypted MTProto
    # streams per upload — each stream handles its own chunk concurrently.
    # This is the correct way to get parallel uploads in pyrofork >= 2.3.x
    # (the save_file patch no longer works as that function was removed).
    if "concurrent_transmissions" in sig.parameters:
        ct = int(os.environ.get("UPLOAD_PARTS_PARALLEL", "8"))
        kwargs["concurrent_transmissions"] = ct
        log.info("⚡ concurrent_transmissions=%d (parallel upload streams)", ct)
    return Client(**kwargs)


async def main() -> None:
    # ── Koyeb health server ────────────────────────────────────
    if os.environ.get("KOYEB", "").strip() == "1":
        from koyeb_server import start_health_server
        port = int(os.environ.get("PORT", 8000))
        start_health_server(port)
        log.info("🌐 Koyeb health server started on port %d", port)


    client = build_client()

    import core.session as _cs
    _cs._client = client

    await client.start()
    me = await client.get_me()
    log.info("✅ @%s (id=%d) started", me.username or me.first_name, me.id)

    runner.start()
    log.info("🚀 Task runner started (max %d concurrent)", 5)

    log.info("📡 Bot is running. Press Ctrl+C to stop.")
    await idle()

    log.info("👋 Shutting down…")
    runner.stop()
    await client.stop()
    log.info("✅ Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())

