"""
services/tg_download.py
Download a Telegram file with progress routed through the unified panel.

Changes vs original:
- Progress updates only update tracker (no inline safe_edit calls)
- Panel loop handles all editing at its own rate (1.5 s)
- runner._wake_panel() called on meaningful progress change
"""
from __future__ import annotations

import os
import time

from pyrogram import Client

from services.utils import safe_edit


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
    from services.task_runner import tracker, TaskRecord, runner
    from pyrogram import enums

    # Derive user_id / chat_id from msg if not passed explicitly
    uid = user_id
    if not uid:
        try:
            uid = msg.chat.id if hasattr(msg, "chat") and msg.chat else 0
        except Exception:
            uid = 0

    tid    = tracker.new_tid()
    record = TaskRecord(
        tid=tid, user_id=uid,
        label=label or fname or "Download",
        mode="dl", engine="telegram",
        fname=fname, total=fsize,
    )
    await tracker.register(record)

    start = time.time()
    last  = [start]

    async def _prog(current: int, total: int) -> None:
        now = time.time()
        if now - last[0] < 0.5:
            return
        last[0]  = now
        elapsed  = now - start
        speed    = current / elapsed if elapsed else 0
        eta      = int((total - current) / speed) if speed else 0
        record.update(
            done=current, total=total or fsize,
            speed=speed, eta=eta, elapsed=elapsed,
            state="📥 Downloading",
        )
        runner._wake_panel(uid)

    path = await client.download_media(file_id, file_name=dest_path, progress=_prog)

    fsize_done = os.path.getsize(path) if path and os.path.exists(path) else fsize
    record.update(state="✅ Done", done=fsize_done, total=fsize_done)
    runner._wake_panel(uid, immediate=True)
    return path
