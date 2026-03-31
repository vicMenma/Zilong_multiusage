"""
services/task_runner.py
Global task registry + /status panel renderer.

REWRITE:
  - Removed LivePanel class (replaced by per-task inline progress in
    tg_download.py and uploader.py — each task edits its own message)
  - GlobalTracker kept for /status overview
  - render_panel() uses the new panel design
  - _stats_cache background updater kept
  - TaskRunner simplified: no auto_panel, no _wake_panel complexity
  - MAX_CONCURRENT semaphore kept for queue management
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)

MAX_CONCURRENT = 5
TASK_LINGER    = 60   # seconds to keep finished tasks visible in /status

_task_semaphore: Optional[asyncio.Semaphore] = None

# ── Background stats cache ─────────────────────────────────────
_stats_cache: dict = {
    "cpu": 0.0, "ram_pct": 0.0, "ram_used": 0,
    "disk_free": 0, "dl_speed": 0.0, "ul_speed": 0.0,
}


async def _stats_updater() -> None:
    from services.utils import system_stats
    while True:
        try:
            _stats_cache.update(await system_stats())
        except Exception:
            pass
        await asyncio.sleep(5)


def _get_semaphore() -> asyncio.Semaphore:
    global _task_semaphore
    if _task_semaphore is None:
        _task_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    return _task_semaphore


# ─────────────────────────────────────────────────────────────
# TaskRecord
# ─────────────────────────────────────────────────────────────

@dataclass
class TaskRecord:
    tid:        str
    user_id:    int
    label:      str
    mode:       str   = "dl"      # dl | ul | proc | magnet | queue
    engine:     str   = ""
    state:      str   = "⏳ Queued"
    fname:      str   = ""
    done:       int   = 0
    total:      int   = 0
    speed:      float = 0.0
    eta:        int   = 0
    elapsed:    float = 0.0
    seeds:      int   = 0
    meta_phase: bool  = False
    started:    float = field(default_factory=time.time)
    finished:   Optional[float] = None
    seq:        int   = 0

    def update(self, **kw) -> None:
        for k, v in kw.items():
            if hasattr(self, k):
                setattr(self, k, v)
        self.elapsed = time.time() - self.started
        if self.state.startswith(("✅", "❌")) and self.finished is None:
            self.finished = time.time()

    def pct(self) -> float:
        return min((self.done / self.total * 100) if self.total else 0.0, 100.0)

    @property
    def is_terminal(self) -> bool:
        return self.state.startswith(("✅", "❌"))


# ─────────────────────────────────────────────────────────────
# GlobalTracker
# ─────────────────────────────────────────────────────────────

class GlobalTracker:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._lock  = asyncio.Lock()
        self._seq   = 0

    def new_tid(self) -> str:
        return uuid.uuid4().hex[:8].upper()

    async def register(self, record: TaskRecord) -> None:
        async with self._lock:
            self._evict()
            self._seq += 1
            record.seq = self._seq
            self._tasks[record.tid] = record

    async def update(self, tid: str, **kw) -> None:
        async with self._lock:
            t = self._tasks.get(tid)
            if t:
                t.update(**kw)

    async def finish(self, tid: str, success: bool = True, msg: str = "") -> None:
        state = "✅ Done" if success else f"❌ {msg or 'Failed'}"
        await self.update(tid, state=state)

    def tasks_for_user(self, uid: int) -> list[TaskRecord]:
        self._evict()
        return sorted(
            (t for t in self._tasks.values() if t.user_id == uid),
            key=lambda t: t.seq,
        )

    def all_tasks(self) -> list[TaskRecord]:
        self._evict()
        return sorted(self._tasks.values(), key=lambda t: t.seq)

    def active_tasks(self) -> list[TaskRecord]:
        return [t for t in self.all_tasks() if not t.is_terminal]

    def _evict(self) -> None:
        now  = time.time()
        dead = [
            k for k, t in self._tasks.items()
            if t.is_terminal and t.finished and now - t.finished > TASK_LINGER
        ]
        for k in dead:
            self._tasks.pop(k, None)


tracker = GlobalTracker()


# ─────────────────────────────────────────────────────────────
# /status panel renderer  (used by admin.py and panel.py)
# ─────────────────────────────────────────────────────────────

def _pbar(pct: float, w: int = 16) -> str:
    filled = round(min(max(pct, 0), 100) / 100 * w)
    return "█" * filled + "░" * (w - filled)


def _compact(n: float) -> str:
    for div, sym in ((1 << 40, "T"), (1 << 30, "G"), (1 << 20, "M"), (1 << 10, "K")):
        if n >= div:
            return f"{n / div:.1f}{sym}"
    return f"{int(n)}B"


async def render_panel(target_uid: Optional[int] = None) -> str:
    from core.bot_name import get_bot_name
    from services.utils import human_size, human_dur, pct_bar

    tasks  = tracker.tasks_for_user(target_uid) if target_uid else tracker.all_tasks()
    active = [t for t in tasks if not t.is_terminal]
    done   = [t for t in tasks if t.is_terminal]

    s         = _stats_cache
    cpu       = float(s.get("cpu", 0.0))
    ram_pct   = float(s.get("ram_pct", 0.0))
    disk_free = int(s.get("disk_free", 0))
    dl_spd    = float(s.get("dl_speed", 0.0))
    ul_spd    = float(s.get("ul_speed", 0.0))
    bot_name  = get_bot_name().upper()

    dl_s = f"{human_size(dl_spd)}/s" if dl_spd else "—"
    ul_s = f"{human_size(ul_spd)}/s" if ul_spd else "—"

    SEP  = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    SEP2 = "────────────────────────────────"

    lines: list[str] = [
        SEP,
        f"⚡  <b>{bot_name} MULTIUSAGE BOT</b>",
        SEP,
        f"↓ <code>{dl_s}</code>   ↑ <code>{ul_s}</code>",
        "",
    ]

    display = active + done[-3:]

    if not display:
        lines.append("<i>No active tasks — idle.</i>")
        lines.append("")
    else:
        for t in display:
            fname_s = ((t.fname or t.label)[:32] + "…") if len(t.fname or t.label) > 32 else (t.fname or t.label)
            arrow = "↑" if t.mode == "ul" else ("⚙" if t.mode == "proc" else "↓")

            if t.is_terminal:
                icon = "✅" if t.state.startswith("✅") else "❌"
                lines.append(f"{arrow}  <code>{fname_s}</code>  {icon}")

            elif t.meta_phase:
                lines.append(f"{arrow}  <code>{fname_s}</code>")
                lines.append(f"   🔍 <i>Fetching metadata…</i>")

            elif t.state == "⏳ Queued":
                lines.append(f"🕐  <code>{fname_s}</code>  <i>queued</i>")

            else:
                pct   = t.pct()
                spd_s = f"{human_size(t.speed)}/s" if t.speed else "—"
                eta_s = human_dur(t.eta) if t.eta > 0 else ""
                lines.append(f"{arrow}  <code>{fname_s}</code>  ⚡ <code>{spd_s}</code>")
                bar_line = f"   <code>{_pbar(pct)}</code>  {pct:.0f}%"
                if eta_s:
                    bar_line += f"  <i>{eta_s}</i>"
                lines.append(bar_line)

            lines.append("")

    slots = sum(1 for t in active if not t.state.startswith("⏳"))
    lines += [
        SEP2,
        f"🖥 <code>{cpu:.0f}%</code>  "
        f"🧠 <code>{ram_pct:.0f}%</code>  "
        f"💾 <code>{_compact(disk_free)}</code>  "
        f"📋 <code>{slots}/{MAX_CONCURRENT}</code>",
    ]
    return "\n".join(lines)


def render_panel_kb(uid: int):
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    tasks  = tracker.tasks_for_user(uid)
    active = [t for t in tasks if not t.is_terminal]
    rows: list = []

    row: list = []
    for t in active[:8]:
        short = (t.fname or t.label)[:12].strip()
        row.append(InlineKeyboardButton(
            f"❌ #{t.seq} {short}",
            callback_data=f"panel|cancel|{t.tid}",
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if active:
        rows.append([
            InlineKeyboardButton("❌ Cancel All", callback_data=f"panel|cancel_all|{uid}"),
            InlineKeyboardButton("🔄 Refresh",    callback_data=f"panel|refresh|{uid}"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("🔄 Refresh", callback_data=f"panel|refresh|{uid}"),
            InlineKeyboardButton("✖ Close",    callback_data=f"panel|close|{uid}"),
        ])

    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────
# TaskRunner — simplified (no LivePanel)
# ─────────────────────────────────────────────────────────────

class TaskRunner:
    def __init__(self) -> None:
        self._task_handles: dict[str, asyncio.Task] = {}
        self._running = False

    def start(self) -> None:
        self._running = True
        global _task_semaphore
        _task_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        try:
            asyncio.get_running_loop().create_task(_stats_updater())
            log.info("📊 Stats updater started")
        except Exception as e:
            log.warning("Could not start stats updater: %s", e)

    def stop(self) -> None:
        self._running = False
        for handle in self._task_handles.values():
            if not handle.done():
                handle.cancel()
        try:
            from services.downloader import _YTDLP_POOL
            if _YTDLP_POOL is not None:
                _YTDLP_POOL.shutdown(wait=False)
        except Exception:
            pass

    async def cancel_task(self, tid: str) -> bool:
        handle = self._task_handles.get(tid)
        if handle and not handle.done():
            handle.cancel()
        t = tracker._tasks.get(tid)
        if t and not t.is_terminal:
            await tracker.finish(tid, success=False, msg="Cancelled")
            self._task_handles.pop(tid, None)
            return True
        return False

    async def cancel_all(self, uid: int) -> int:
        count = 0
        for t in tracker.tasks_for_user(uid):
            if not t.is_terminal:
                if await self.cancel_task(t.tid):
                    count += 1
        return count

    def _wake_panel(self, uid: int, immediate: bool = False) -> None:
        """No-op stub — LivePanel removed, inline progress handles display."""
        pass

    async def submit(
        self,
        user_id:      int,
        label:        str,
        coro_factory: Callable[[TaskRecord], Awaitable[None]],
        fname:        str = "",
        total:        int = 0,
        mode:         str = "dl",
        engine:       str = "",
    ) -> TaskRecord:
        tid    = tracker.new_tid()
        record = TaskRecord(
            tid=tid, user_id=user_id, label=label,
            fname=fname, total=total, mode=mode, engine=engine,
        )
        await tracker.register(record)
        task = asyncio.get_running_loop().create_task(
            self._run_task(record, coro_factory)
        )
        self._task_handles[tid] = task
        return record

    async def _run_task(self, record: TaskRecord, factory) -> None:
        needs_slot = record.mode in ("dl", "proc", "magnet")
        try:
            if needs_slot:
                sem = _get_semaphore()
                if sem._value == 0:
                    record.update(state="⏳ Queued")
                async with sem:
                    record.update(state="⚙️ Running")
                    await factory(record)
                    record.update(state="✅ Done", done=record.total or record.done)
            else:
                record.update(state="📤 Uploading")
                await factory(record)
                record.update(state="✅ Done", done=record.total or record.done)
        except asyncio.CancelledError:
            record.update(state="❌ Cancelled")
        except Exception as exc:
            log.error("Task %s failed: %s", record.tid, exc)
            record.update(state=f"❌ {str(exc)[:60]}")
        finally:
            self._task_handles.pop(record.tid, None)


runner = TaskRunner()
