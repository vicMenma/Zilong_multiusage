"""
Zilong Bot — main.py
Clean entry point. Loads config, builds client, registers plugins, starts.
"""
import asyncio
import logging
import os
import glob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("zilong.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Remove stale sessions before import so Pyrogram doesn't pick them up
for _f in glob.glob("*.session") + glob.glob("*.session-journal"):
    try:
        os.remove(_f)
        log.info("Removed stale session: %s", _f)
    except OSError:
        pass

from pyrogram import Client, idle
from core.config import cfg
from core.session import SessionStore
from services.task_runner import TaskRunner


def build_client() -> Client:
    return Client(
        name="ZilongBot",
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        bot_token=cfg.bot_token,
        plugins={"root": "plugins"},
        workdir="/tmp",          # keep session file in /tmp for Colab
    )


async def main() -> None:
    client = build_client()

    # Make client available globally (plugins import it)
    import core.session as _cs
    _cs._client = client

    runner = TaskRunner()

    await client.start()
    me = await client.get_me()
    log.info("✅ @%s started — %d handler group(s)", me.username,
             sum(len(h) for h in client.dispatcher.groups.values()))

    runner.start()
    log.info("🚀 Task runner started")

    await idle()

    log.info("👋 Shutting down…")
    runner.stop()
    await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
