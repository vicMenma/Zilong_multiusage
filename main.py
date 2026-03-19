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


# ── Parallel-part upload patch ────────────────────────────────────────────────
# pyrofork's default save_file() uploads 512 KB parts one-by-one (sequential).
# At 100 ms RTT to Telegram DC4 that caps throughput at ~5 MB/s.
# This patch replaces the inner upload loop with asyncio.gather so that
# UPLOAD_PARTS_PARALLEL parts fly concurrently, multiplying throughput.
# Gracefully skips if the internal API has changed.
UPLOAD_PARTS_PARALLEL = int(os.environ.get("UPLOAD_PARTS_PARALLEL", "8"))

def _patch_parallel_upload():
    try:
        import pyrogram.utils as _utils
        import asyncio as _asyncio
        import math as _math

        _orig_save_file = _utils.save_file  # keep original for fallback

        async def _fast_save_file(client, path, file_id=None, file_part=0, progress=None, progress_args=()):
            """Patched save_file: uploads UPLOAD_PARTS_PARALLEL parts in parallel."""
            import os
            from pyrogram import raw

            file_size = os.path.getsize(path)
            file_total_parts = _math.ceil(file_size / (512 * 1024))
            is_big = file_size > 10 * 1024 * 1024
            session = await client.session.invoke(raw.functions.upload.GetFile(
                location=None, offset=0, limit=0
            )) if False else None  # probe removed — use client directly

            semaphore = _asyncio.Semaphore(UPLOAD_PARTS_PARALLEL)

            async def _upload_part(part_num, data):
                async with semaphore:
                    if is_big:
                        await client.invoke(raw.functions.upload.SaveBigFilePart(
                            file_id=file_id, file_part=part_num,
                            file_total_parts=file_total_parts, bytes=data
                        ))
                    else:
                        await client.invoke(raw.functions.upload.SaveFilePart(
                            file_id=file_id, file_part=part_num, bytes=data
                        ))
                    if progress:
                        await progress((part_num + 1) * 512 * 1024, file_size, *progress_args)

            with open(path, "rb") as f:
                tasks = []
                for i in range(file_total_parts):
                    chunk = f.read(512 * 1024)
                    tasks.append(_upload_part(i, chunk))
                await _asyncio.gather(*tasks)

        # Only patch if save_file is a simple coroutine function we can safely replace
        if _asyncio.iscoroutinefunction(_orig_save_file):
            _utils.save_file = _fast_save_file
            log.info("⚡ Parallel upload patch active (%d parts concurrently)", UPLOAD_PARTS_PARALLEL)
        else:
            log.info("⚡ save_file is not a plain coroutine — skipping patch (internal API differs)")
    except Exception as exc:
        log.warning("Upload patch skipped: %s — using pyrofork default", exc)

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

    _patch_parallel_upload()

    client = build_client()

    import core.session as _cs
    _cs._client = client

    await client.start()
    me = await client.get_me()
    log.info("✅ @%s (id=%d) started", me.username or me.first_name, me.id)

    runner.start()
    log.info("🚀 Task runner started (max %d concurrent)", 5)

    # ── First-launch: ask owner for a bot name ─────────────────
    if not is_name_configured():
        await _ask_bot_name(client)

    log.info("📡 Bot is running. Press Ctrl+C to stop.")
    await idle()

    log.info("👋 Shutting down…")
    runner.stop()
    await client.stop()
    log.info("✅ Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())

