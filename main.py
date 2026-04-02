"""
Zilong Bot — main.py
Entry point. Loads config, builds client, registers plugins, starts.
"""
"""
Zilong Bot — main.py
Entry point. Loads config, builds client, registers plugins, starts.
"""
import asyncio
import logging
import os
import glob
import sys

# ── Single-instance guard ─────────────────────────────────────────────────────
_PID_FILE = "/tmp/zilong_bot.pid"

def _acquire_pid_lock() -> None:
    if os.path.exists(_PID_FILE):
        try:
            old_pid = int(open(_PID_FILE).read().strip())
            os.kill(old_pid, 0)
            print(
                f"❌ Another bot instance is already running (PID {old_pid}).\n"
                f"   Kill it first:  kill {old_pid}\n"
                f"   Or delete:      rm {_PID_FILE}",
                file=sys.stderr,
            )
            sys.exit(1)
        except (ValueError, ProcessLookupError, PermissionError):
            pass
    with open(_PID_FILE, "w") as _pf:
        _pf.write(str(os.getpid()))

def _release_pid_lock() -> None:
    try:
        os.remove(_PID_FILE)
    except OSError:
        pass

_acquire_pid_lock()
import atexit
atexit.register(_release_pid_lock)
# ─────────────────────────────────────────────────────────────────────────────

try:
    import uvloop
    uvloop.install()
    _UVLOOP = True
except ImportError:
    _UVLOOP = False

# Python 3.12 no longer creates an implicit event loop on the main thread.
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

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
    log.info("⚡ uvloop active")
else:
    log.warning("⚠️  uvloop not installed — using default asyncio event loop")

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
for _f in (
    glob.glob("*.session") + glob.glob("*.session-journal") +
    glob.glob(os.path.join(_DATA_DIR, "*.session-journal"))
):
    try:
        os.remove(_f)
        log.info("Removed stale session artifact: %s", _f)
    except OSError:
        pass

from pyrogram import Client, idle, filters, handlers, enums
from core.config import cfg
from core.bot_name import get_bot_name, set_bot_name, is_name_configured
from services.task_runner import runner, MAX_CONCURRENT

def build_client() -> Client:
    _session_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(_session_dir, exist_ok=True)
    return Client(
        name="ZilongBot",
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        bot_token=cfg.bot_token,
        plugins={"root": "plugins"},
        workdir=_session_dir,
    )

async def _ask_bot_name(client) -> None:
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    async def _on_name(_, msg):
        name = msg.text.strip()
        if name and not name.startswith("/") and not fut.done():
            fut.set_result(name)
            try:
                await msg.delete()
            except Exception:
                pass

    handler = handlers.MessageHandler(
        _on_name,
        filters.user(cfg.owner_id) & filters.text & filters.private,
    )
    client.add_handler(handler, group=-99)

    try:
        prompt_msg = await client.send_message(
            cfg.owner_id,
            "👋 <b>Welcome! First-time setup</b>\n\n"
            "What do you want to call this bot?\n"
            "Send me just the name — for example: <code>Kitagawa</code>\n\n"
            "This name will appear in:\n"
            "  • <b>/start</b> header\n"
            "  • <b>Progress panels</b>\n"
            "  • <b>All status messages</b>\n\n"
            "<i>You can change it later with /botname.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        name = await asyncio.wait_for(fut, timeout=300)
    except asyncio.TimeoutError:
        log.warning("Bot-name setup timed out — using default Zilong")
        name = "Zilong"
    finally:
        client.remove_handler(handler, group=-99)

    set_bot_name(name)

    try:
        await prompt_msg.delete()
    except Exception:
        pass

    display = name.upper() + " MULTIUSAGE BOT"
    confirm = await client.send_message(
        cfg.owner_id,
        f"✅ <b>Name saved!</b>\n\n"
        f"The bot will now show:\n"
        f"<b>⚡ {display}</b>\n\n"
        f"<i>You can change it anytime with /botname.</i>\n"
        f"🟢 <i>Starting…</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    async def _del():
        await asyncio.sleep(15)
        try:
            await confirm.delete()
        except Exception:
            pass
    asyncio.create_task(_del())

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

    from pyrogram.errors import FloodWait as _FloodWait
    _MAX_AUTH_RETRIES = 5
    for _attempt in range(1, _MAX_AUTH_RETRIES + 1):
        try:
            await client.start()
            break
        except _FloodWait as _fw:
            log.warning(
                "🚦 FloodWait on auth — waiting %ds (attempt %d/%d)",
                _fw.value, _attempt, _MAX_AUTH_RETRIES,
            )
            print(f"FLOOD_WAIT_SECONDS={_fw.value}", flush=True)
            if _attempt >= _MAX_AUTH_RETRIES:
                log.error("❌ Giving up after %d FloodWait retries.", _MAX_AUTH_RETRIES)
                raise
            await asyncio.sleep(_fw.value + 5)

    me = await client.get_me()
    log.info("✅ @%s (id=%d) started", me.username or me.first_name, me.id)

    runner.start()
    log.info("🚀 Task runner started (max %d concurrent)", MAX_CONCURRENT)

    # ── Webhook server (optional) ───────────────────────────────────
    webhook_url = ""
    has_webhook_config = bool(
        os.environ.get("WEBHOOK_BASE_URL", "").strip()
        or cfg.ngrok_token  # keep check for backwards env var
    )

    if cfg.cc_api_key or has_webhook_config:
        import services.cloudconvert_hook as cc_hook
        if cfg.cc_webhook_secret:
            cc_hook.WEBHOOK_SECRET = cfg.cc_webhook_secret

        # ✂️ Remove ngrok_token – we use Serveo instead
        webhook_url = await cc_hook.start_webhook_server(
            port=8765
        )

        if webhook_url:
            log.info("☁️  CloudConvert webhook active: %s", webhook_url)
        else:
            log.info("☁️  Webhook server running (local only).")
    else:
        log.info("ℹ️  No CC_API_KEY — CloudConvert features disabled")

    # ── ccstatus auto-poller ─────────────────────────────────
    if cfg.cc_api_key:
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
            log.info("📡 ccstatus auto-poller started")
        except Exception as exc:
            log.warning("ccstatus poller startup failed: %s", exc)

    if not is_name_configured():
        await _ask_bot_name(client)

    bot_name = get_bot_name()
    log.info("🤖 Bot name: %s", bot_name.upper())

    # ── Keep-alive health server (optional) ─────────────────────
    keepalive_url = ""
    try:
        from services.keep_alive import start as _ka_start
        keepalive_url = await _ka_start(
            port=8080,
            ngrok_token=cfg.ngrok_token,  # keep as-is for keepalive only
        )
        if keepalive_url:
            log.info("🏥 Keep-alive URL: %s", keepalive_url)
            try:
                await client.send_message(
                    cfg.owner_id,
                    f"🏥 <b>Keep-Alive Active</b>\n"
                    f"🌐 <code>{keepalive_url}/health</code>\n",
                    parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
        else:
            log.info("🏥 Health server on localhost:8080 (no public URL)")
    except Exception as exc:
        log.warning("Keep-alive server failed to start: %s", exc)

    log.info("📡 Bot is running. Press Ctrl+C to stop.")
    await idle()

    log.info("👋 Shutting down…")
    try:
        from services.keep_alive import stop as _ka_stop
        await _ka_stop()
    except Exception:
        pass
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
