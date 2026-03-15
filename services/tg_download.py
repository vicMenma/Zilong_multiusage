"""
services/tg_download.py
Download a Telegram file (by file_id or Message) with progress panel updates
and global tracker registration.
"""
from __future__ import annotations

import os
import time
from typing import Optional

from pyrogram import Client, enums

from services.utils import progress_panel, safe_edit


async def tg_download(
    client:    Client,
    file_id:   str,
    dest_path: str,
    msg,
    fname:     str  = "",
    fsize:     int  = 0,
    user_id:   int  = 0,
    label:     str  = "",
) -> str:
    """
    Download Telegram file to dest_path.
    Edits msg with live progress AND registers in the global tracker.
    Returns local path.
    """
    from services.task_runner import tracker, TaskRecord

    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=user_id,
        label=label or fname or "Download",
        mode="dl", engine="telegram",
        fname=fname, total=fsize,
    )
    await tracker.register(record)

    start = time.time()
    last  = [start]

    async def _prog(current: int, total: int) -> None:
        now = time.time()
        if now - last[0] < 3.0:
            return
        last[0]  = now
        elapsed  = now - start
        speed    = current / elapsed if elapsed else 0
        eta      = int((total - current) / speed) if speed else 0
        panel    = progress_panel(
            mode="dl", fname=fname or "file",
            done=current, total=total or fsize,
            speed=speed, eta=eta, elapsed=elapsed,
            engine="telegram",
        )
        await safe_edit(msg, panel, parse_mode=enums.ParseMode.HTML)
        record.update(
            done=current, total=total or fsize,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📥 Downloading",
        )

    path = await client.download_media(file_id, file_name=dest_path, progress=_prog)

    fsize_done = os.path.getsize(path) if path and os.path.exists(path) else fsize
    record.update(state="✅ Done", done=fsize_done, total=fsize_done)
    return path
