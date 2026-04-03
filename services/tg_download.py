"""
services/tg_download.py
Download a Telegram file with inline per-task progress (new panel design).

REWRITE:
  - Edits msg directly with the new progress_panel() format
  - No tracker registration (handled by callers if needed)
  - 1.5 s edit throttle
  - Pulls system stats from _stats_cache for CPU/RAM/Disk display
"""
from __future__ import annotations

import os
import time

from pyrogram import Client, enums

from services.utils import progress_panel, safe_edit
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
    last_edit    = [start - 4.0]   # allow first edit immediately

    user_cfg     = await settings.get(user_id)
    panel_style  = user_cfg.get("progress_style", "B")

    async def _prog(current: int, total: int) -> None:
        now = time.time()
        if now - last_edit[0] < 4.0:
            return
        last_edit[0] = now

        elapsed = now - start
        speed   = current / elapsed if elapsed else 0.0
        eta     = int((total - current) / speed) if (speed and total > current) else 0

        s = _stats_cache
        text = progress_panel(
            mode        = "dl",
            fname       = display_name,
            done        = current,
            total       = total or fsize,
            speed       = speed,
            eta         = eta,
            elapsed     = elapsed,
            engine      = "telegram",
            link_label  = "Telegram",
            cpu         = float(s.get("cpu", 0)),
            ram_used    = int(s.get("ram_used", 0)),
            disk_free   = int(s.get("disk_free", 0)),
            style       = panel_style,
        )
        await safe_edit(msg, text, parse_mode=enums.ParseMode.HTML)

    path = await client.download_media(file_id, file_name=dest_path, progress=_prog)

    return path
