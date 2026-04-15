"""
Zilong Bot — main.py
Entry point. Loads config, builds client, registers plugins, starts.

FIXES
─────
MAIN-01: FC_API_KEY checked via os.environ directly (not just cfg) to handle
  any dotenv load-order edge cases.
MAIN-02: data/ directory created at startup before any service writes to it.
MAIN-03: on_tunnel_ready called with explicit cc_path="/webhook/cloudconvert"
  so CC webhooks always register to the correct URL.
MAIN-04: FC import errors logged at WARNING instead of silently swallowed.
MAIN-05: Webhook server started when FC_API_KEY is set, not just CC_API_KEY.
"""
import asyncio
import logging
import os
import glob
import sys

# ── Single-instance guard ──────────────────────────────────────────────────
_PID_FILE = "/tmp/zilong_bot.pid"

def _acquire_pid_lock() -> None:
    if os.path.exists(_PID_FILE):
        try:
            old_pid = int(open(_PID_FILE).read().strip())
            os.kill(old_pid, 0)
            print(f"❌ Another instance running (PID {old_pid}).\n   Kill: kill {old_pid}", file=sys.stderr)
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

try:
    import uvloop
    uvloop.install()
    _UVLOOP = True
except ImportError:
    _UVLOOP = False

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
    log.warning("⚠️  uvloop not installed")

# MAIN-02: always ensure data/ exists before any service that writes to it
_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(_DATA_DIR, exist_ok=True)

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
            "👋 <b>Welcome! First-time setup</b>\n\nWhat do you want to call this bot?\n"
            "Send just the name — e.g. <code>Kitagawa</code>\n\n<i>Change later with /botname.</i>",
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

    confirm = await client.send_message(
        cfg.owner_id,
        f"✅ <b>Name saved: {name.upper()} MULTIUSAGE BOT</b>\n🟢 <i>Starting…</i>",
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
        log.info("🌐 Koyeb health server on port %d", port)

    client = build_client()
    import core.session as _cs
    _cs._client = client

    from pyrogram.errors import FloodWait as _FloodWait
    for _attempt in range(1, 6):
        try:
            await client.start()
            break
        except _FloodWait as _fw:
            log.warning("🚦 FloodWait %ds on auth (attempt %d/5)", _fw.value, _attempt)
            print(f"FLOOD_WAIT_SECONDS={_fw.value}", flush=True)
            if _attempt >= 5:
                raise
            await asyncio.sleep(_fw.value + 5)

    me = await client.get_me()
    log.info("✅ @%s (id=%d) started", me.username or me.first_name, me.id)

    runner.start()
    log.info("🚀 Task runner started (max %d concurrent)", MAX_CONCURRENT)

    # MAIN-01: check env directly too, not just cfg (handles dotenv timing)
    cc_api_key = cfg.cc_api_key or os.environ.get("CC_API_KEY", "").strip()
    fc_api_key = cfg.fc_api_key or os.environ.get("FC_API_KEY", "").strip()
    has_webhook_config = bool(
        os.environ.get("WEBHOOK_BASE_URL", "").strip() or cfg.ngrok_token
    )

    # MAIN-05: start webhook server for EITHER CC or FC key
    webhook_url = ""
    if cc_api_key or fc_api_key or has_webhook_config:
        import services.cloudconvert_hook as cc_hook
        if cfg.cc_webhook_secret:
            cc_hook.WEBHOOK_SECRET = cfg.cc_webhook_secret

        webhook_url = await cc_hook.start_webhook_server(port=8765)
        if webhook_url:
            log.info("☁️  Webhook server: %s", webhook_url)
        else:
            log.info("☁️  Webhook server on port 8765 (no public URL)")

        # MAIN-03: explicit cc_path to avoid any default value confusion
        try:
            from services.webhook_sync import on_tunnel_ready, poll_pending_jobs
            from core.config import get_tunnel_url
            _turl = get_tunnel_url()
            if _turl and cc_api_key:
                await on_tunnel_ready(_turl, cc_path="/webhook/cloudconvert")
                log.info("🔄 CC webhooks synced → %s", _turl)
            elif not _turl:
                log.warning("⚠️  No tunnel URL — CC webhook sync skipped")
            await poll_pending_jobs()
            log.info("🔄 Pending jobs recovery complete")
        except Exception as exc:
            log.warning("webhook_sync failed: %s", exc)
    else:
        log.info("ℹ️  No API keys — webhook server not started")

    # FC job store startup
    if fc_api_key:
        try:
            import plugins.fc_webhook as fc_webhook
            await fc_webhook.startup_load()
            log.info("🆓 FreeConvert job store loaded")
        except ImportError as exc:
            # MAIN-04: log at warning so user can see what's missing
            log.warning(
                "⚠️  plugins/fc_webhook.py missing — FreeConvert disabled.\n"
                "    Ensure fc_webhook.py and fc_job_store.py are in your repo.\n"
                "    Error: %s", exc,
            )
        except Exception as exc:
            log.warning("FC webhook startup failed: %s", exc)
    else:
        log.info("ℹ️  No FC_API_KEY — FreeConvert disabled")

    # CC status poller
    if cc_api_key:
        try:
            from plugins.ccstatus import _ensure_poller
            _ensure_poller()
            log.info("📡 ccstatus auto-poller started")
        except Exception as exc:
            log.warning("ccstatus poller failed: %s", exc)

    if not is_name_configured():
        await _ask_bot_name(client)

    bot_name = get_bot_name()
    log.info("🤖 Bot name: %s", bot_name.upper())

    # Nyaa tracker
    try:
        from plugins.nyaa_tracker import start_nyaa_poller
        start_nyaa_poller()
    except Exception as exc:
        log.warning("Nyaa poller failed: %s", exc)

    # Keep-alive
    try:
        from services.keep_alive import start as _ka_start
        keepalive_url = await _ka_start(port=8080, ngrok_token=cfg.ngrok_token)
        if keepalive_url:
            log.info("🏥 Keep-alive: %s", keepalive_url)
            try:
                await client.send_message(
                    cfg.owner_id,
                    f"🏥 <b>Keep-Alive Active</b>\n🌐 <code>{keepalive_url}/health</code>",
                    parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
    except Exception as exc:
        log.warning("Keep-alive failed: %s", exc)

    log.info("📡 Bot is running. Ctrl+C to stop.")
    await idle()

    log.info("👋 Shutting down…")
    try:
        from services.keep_alive import stop as _ka_stop
        await _ka_stop()
    except Exception:
        pass
    if cc_api_key or fc_api_key or has_webhook_config:
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
