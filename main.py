"""
Zilong Bot — main.py
Entry point. Loads config, builds client, registers plugins, starts.

Koyeb support: if KOYEB=1 is set in env, a lightweight HTTP health-check
server is started on port PORT (default 8000) so Koyeb's health probe passes.

AWS FIX:
  The ccstatus poller (plugins/ccstatus.py) is now started automatically at
  bot boot, regardless of whether the webhook is reachable. This means:
  - On Colab: webhook fires first (fast), poller confirms delivery
  - On AWS without WEBHOOK_BASE_URL: poller polls CC API every 5s and
    downloads + uploads results directly — no inbound HTTP needed at all
  - On AWS with WEBHOOK_BASE_URL: webhook fires first, poller as backup
"""
import asyncio
import logging
import os
import glob

try:
    import uvloop
    uvloop.install()
    _UVLOOP = True
except ImportError:
    _UVLOOP = False

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

for _f in glob.glob("*.session") + glob.glob("*.session-journal"):
    try:
        os.remove(_f)
        log.info("Removed stale session: %s", _f)
    except OSError:
        pass

from pyrogram import Client, idle, filters, handlers, enums
from core.config import cfg
from core.bot_name import get_bot_name, set_bot_name, is_name_configured
from services.task_runner import runner, MAX_CONCURRENT

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
    if "max_concurrent_transmissions" in sig.parameters:
        # 8 parallel MTProto streams — sweet spot for Colab without triggering
        # server-side throttle. Set UPLOAD_PARTS_PARALLEL in .env to override.
        ct = int(os.environ.get("UPLOAD_PARTS_PARALLEL", "8"))
        kwargs["max_concurrent_transmissions"] = ct
        log.info("⚡ max_concurrent_transmissions=%d (parallel upload streams)", ct)
    if "sleep_threshold" in sig.parameters:
        # Auto-sleep on FloodWait ≤ threshold seconds; raise immediately above it.
        # Avoids silent multi-minute stalls on unexpected long waits.
        st = int(os.environ.get("SLEEP_THRESHOLD", "60"))
        kwargs["sleep_threshold"] = st
        log.info("⏱  sleep_threshold=%ds", st)
    return Client(**kwargs)


async def _ask_bot_name(client) -> None:
    loop = asyncio.get_running_loop()
    fut  = loop.create_future()

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
    log.info("🚀 Task runner started (max %d concurrent)", MAX_CONCURRENT)

    # ── Webhook server (optional — for receiving CC callbacks) ────────────
    webhook_url = ""
    has_webhook_config = bool(
        os.environ.get("WEBHOOK_BASE_URL", "").strip()
        or cfg.ngrok_token
    )

    if cfg.cc_api_key or has_webhook_config:
        import services.cloudconvert_hook as cc_hook
        if cfg.cc_webhook_secret:
            cc_hook.WEBHOOK_SECRET = cfg.cc_webhook_secret

        webhook_url = await cc_hook.start_webhook_server(
            port=8765,
            ngrok_token=cfg.ngrok_token,
        )
        if webhook_url:
            log.info("☁️  CloudConvert webhook active: %s", webhook_url)
        else:
            log.info(
                "☁️  Webhook server running (local only). "
                "Set WEBHOOK_BASE_URL in .env to enable inbound callbacks on AWS."
            )
    else:
        log.info("ℹ️  No CC_API_KEY — CloudConvert features disabled")

    # ── ccstatus auto-poller — always start if CC_API_KEY is set ─────────
    if cfg.cc_api_key:
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
            log.info(
                "📡 ccstatus auto-poller started "
                "(polls CC API; delivers jobs even without inbound webhook)"
            )
        except Exception as exc:
            log.warning("ccstatus poller startup failed: %s", exc)

    if not is_name_configured():
        await _ask_bot_name(client)

    log.info("📡 Bot is running. Press Ctrl+C to stop.")
    await idle()

    log.info("👋 Shutting down…")
    if has_webhook_config or cfg.cc_api_key:
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
