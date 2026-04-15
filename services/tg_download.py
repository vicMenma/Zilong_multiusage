"""
services/tg_download.py
Download a Telegram file with non-blocking progress panel.

FIX BUG-UH-04: PanelUpdater interval raised from 1.0 s → 3.0 s.
  A 1 s edit rate against a multi-hour download accumulates thousands of
  Telegram API calls, saturating the per-method FloodWait budget.
  3 s still gives a responsive UI while reducing API pressure by 3×.
"""
from __future__ import annotations

import os
import time

from pyrogram import Client, enums

from services.utils import PanelUpdater, progress_panel
from core.session import settings


async def tg_download(
    client:    Client,
    file_id:   str,
    dest_path: str,
    msg,
    fname:     str = "",
    fsize:     int = 0,
    user_id:   int = 0,
    label:     str = "",
) -> str:
    from services.task_runner import _stats_cache

    display_name = fname or label or os.path.basename(dest_path) or "file"
    start        = time.time()

    user_cfg     = await settings.get(user_id)
    panel_style  = user_cfg.get("progress_style", "B")

    def _build(state: dict) -> str:
        return progress_panel(
            mode       = "dl",
            fname      = display_name,
            done       = state.get("done", 0),
            total      = state.get("total", fsize),
            speed      = state.get("speed", 0.0),
            eta        = state.get("eta", 0),
            elapsed    = time.time() - start,
            engine     = "telegram",
            link_label = "Telegram",
            cpu        = float(_stats_cache.get("cpu", 0)),
            ram_used   = int(_stats_cache.get("ram_used", 0)),
            disk_free  = int(_stats_cache.get("disk_free", 0)),
            style      = panel_style,
        )

    # FIX BUG-UH-04: interval raised from 1.0 → 3.0 to reduce FloodWait pressure.
    async with PanelUpdater(msg, _build, interval=3.0) as pu:

        async def _prog(current: int, total: int) -> None:
            elapsed = time.time() - start
            speed   = current / elapsed if elapsed else 0.0
            eta     = int((total - current) / speed) if (speed and total > current) else 0
            pu.tick(done=current, total=total or fsize, speed=speed, eta=eta)

        path = await client.download_media(file_id, file_name=dest_path, progress=_prog)

    return path
