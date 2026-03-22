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

from pyrogram import Client, idle, filters, handlers, enums
from core.config import cfg
from core.bot_name import get_bot_name, set_bot_name, is_name_configured
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
    # Add workers only if the parameter exists (it does in all known pyrofork versions)
    sig = inspect.signature(Client.__init__)
    if "workers" in sig.parameters:
        kwargs["workers"] = _WORKERS
        log.info("⚙️  workers=%d (async dispatch pool)", _WORKERS)
    if "max_concurrent_transmissions" in sig.parameters:
        ct = int(os.environ.get("UPLOAD_PARTS_PARALLEL", "8"))
        kwargs["max_concurrent_transmissions"] = ct
        log.info("⚡ max_concurrent_transmissions=%d (parallel upload streams)", ct)
    return Client(**kwargs)



async def _ask_bot_name(client) -> None:
    """
    Interactively ask the owner for a bot name on first launch.
    Registers a one-shot private-message handler, waits up to 5 minutes,
    then removes the handler and persists the chosen name.
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    async def _on_name(_, msg):
        name = msg.text.strip()
        if name and not fut.done():
            fut.set_result(name)

    handler = handlers.MessageHandler(
        _on_name,
        filters.user(cfg.owner_id) & filters.text & filters.private,
    )
    client.add_handler(handler, group=-99)

    try:
        await client.send_message(
            cfg.owner_id,
            "👋 <b>First-time setup</b>\n\n"
            "What do you want to call this bot?\n"
            "Send me just the name — for example: <code>Kitagawa</code>\n\n"
            "The progress panel will then show:\n"
            "<b>⚡️ KITAGAWA MULTIUSAGE BOT</b>",
            parse_mode=enums.ParseMode.HTML,
        )
        name = await asyncio.wait_for(fut, timeout=300)
    except asyncio.TimeoutError:
        log.warning("Bot-name setup timed out — using default Zilong")
        name = "Zilong"
    finally:
        client.remove_handler(handler, group=-99)

    set_bot_name(name)
    display = name.title() + " Multiusage Bot"
    await client.send_message(
        cfg.owner_id,
        f"✅ Name saved! The panel will now show:\n<b>⚡️ {display.upper()}</b>",
        parse_mode=enums.ParseMode.HTML,
    )
    log.info("Bot name configured: %s", name)

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

    # ── CloudConvert webhook server ────────────────────────────
    if cfg.ngrok_token:
        import services.cloudconvert_hook as cc_hook
        if cfg.cc_webhook_secret:
            cc_hook.WEBHOOK_SECRET = cfg.cc_webhook_secret
        webhook_url = await cc_hook.start_webhook_server(
            port=8765, ngrok_token=cfg.ngrok_token,
        )
        log.info("☁️ CloudConvert webhook started: %s", webhook_url or "localhost only")
    else:
        log.info("ℹ️ No NGROK_TOKEN — CloudConvert webhook disabled")

    # ── First-launch: ask owner for a bot name ─────────────────
    if not is_name_configured():
        await _ask_bot_name(client)

    log.info("📡 Bot is running. Press Ctrl+C to stop.")
    await idle()

    log.info("👋 Shutting down…")
    # Stop CloudConvert webhook if running
    if cfg.ngrok_token:
        try:
            from services.cloudconvert_hook import stop_webhook_server
            await stop_webhook_server()
        except Exception:
            pass
    runner.stop()
    await client.stop()
    log.info("✅ Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
