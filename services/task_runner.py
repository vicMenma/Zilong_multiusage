"""
services/task_runner.py
Global task registry + /status panel renderer.

CHANGES v4:
  - TaskRunner._raw_tasks: any coroutine can self-register via
    runner.register_raw(tid, asyncio.current_task()) so cancel works
    even for tasks NOT submitted through runner.submit().
  - cancel_task() checks both _task_handles and _raw_tasks.
  - render_panel() completely redesigned: clean header, per-task rows
    with seq#, progress bar, engine label, completed summary.
  - render_panel_kb() redesigned with per-task cancel + footer row.
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
TASK_LINGER    = 120   # seconds to keep finished tasks visible in /status

_task_semaphore: Optional[asyncio.Semaphore] = None

# ── Background stats cache ────────────────────────────────────
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
    mode:       str   = "dl"   # dl | ul | proc | magnet | seedr | queue
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
# /status panel renderer
# ─────────────────────────────────────────────────────────────

def _pbar(pct: float, w: int = 15) -> str:
    filled = round(min(max(pct, 0), 100) / 100 * w)
    return "█" * filled + "░" * (w - filled)


def _compact(n: float) -> str:
    for div, sym in ((1 << 40, "T"), (1 << 30, "G"), (1 << 20, "M"), (1 << 10, "K")):
        if n >= div:
            return f"{n / div:.1f}{sym}"
    return f"{int(n)}B"


_MODE_ARROW = {
    "dl":     "↓",
    "magnet": "🧲",
    "seedr":  "☁️",
    "ul":     "↑",
    "proc":   "⚙",
}
_ENGINE_LABEL = {
    "aria2c":    "aria2c",
    "ytdlp":     "yt-dlp",
    "ytdl":      "yt-dlp",
    "gdrive":    "GDrive",
    "mediafire": "MF",
    "telegram":  "TG",
    "magnet":    "BitTorrent",
    "seedr":     "Seedr",
    "direct":    "HTTP",
    "http":      "HTTP",
    "tg_file":   "TG",
}


async def render_panel(target_uid: Optional[int] = None) -> str:
    from core.bot_name import get_bot_name
    from services.utils import human_size, human_dur

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

    n_active  = len(active)
    n_queued  = sum(1 for t in active if t.state == "⏳ Queued")
    n_running = n_active - n_queued

    SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    lines: list[str] = [
        SEP,
        f"⚡  <b>{bot_name} MULTIUSAGE BOT</b>",
        SEP,
        (
            f"💻 CPU <code>{cpu:.0f}%</code>  "
            f"🧠 RAM <code>{ram_pct:.0f}%</code>  "
            f"💾 <code>{_compact(disk_free)}</code> free"
        ),
        (
            f"↓ <code>{dl_s}</code>  "
            f"↑ <code>{ul_s}</code>  "
            f"📋 <code>{n_running}/{MAX_CONCURRENT}</code> slots"
        ),
        SEP,
    ]

    if not tasks:
        lines += ["", "  <i>Bot is idle — no tasks.</i>", ""]
    else:
        # Summary badge
        if n_active:
            parts = [f"<b>{n_running} running</b>"]
            if n_queued:
                parts.append(f"{n_queued} queued")
            if done:
                parts.append(f"{len(done)} finished")
            lines.append(f"  [ {'  ·  '.join(parts)} ]")
        else:
            lines.append(f"  [ <i>{len(done)} finished</i> ]")
        lines.append("")

        # Show active tasks first, then last 4 finished
        display = active + done[-4:]

        for t in display:
            fname_s = (t.fname or t.label)
            if len(fname_s) > 36:
                fname_s = fname_s[:35] + "…"

            arrow = _MODE_ARROW.get(t.mode, "↓")
            eng   = _ENGINE_LABEL.get((t.engine or "").lower(), t.engine)

            # ── Completed tasks (compact one-liner) ──────────
            if t.is_terminal:
                ok  = t.state.startswith("✅")
                ico = "✅" if ok else "❌"
                dur = f"  <i>{human_dur(int(t.elapsed))}</i>" if t.elapsed > 1 else ""
                lines.append(f"{ico}  <code>{fname_s}</code>{dur}")

            # ── Metadata fetch ────────────────────────────────
            elif t.meta_phase:
                lines.append(f"🔍  <b>#{t.seq}</b>  <code>{fname_s}</code>")
                lines.append("     <i>Fetching torrent metadata…</i>")

            # ── Queued (waiting for slot) ─────────────────────
            elif t.state == "⏳ Queued":
                lines.append(f"🕐  <b>#{t.seq}</b>  <code>{fname_s}</code>  <i>queued</i>")

            # ── Active task with progress ─────────────────────
            else:
                pct   = t.pct()
                eng_s = f" <i>[{eng}]</i>" if eng else ""
                lines.append(f"{arrow}  <b>#{t.seq}</b>  <code>{fname_s}</code>{eng_s}")

                bar_line = f"     <code>[{_pbar(pct)}]</code>  <b>{pct:.0f}%</b>"
                meta: list[str] = []
                if t.speed:
                    meta.append(f"⚡ <code>{human_size(t.speed)}/s</code>")
                if t.eta > 0:
                    meta.append(f"<i>ETA {human_dur(t.eta)}</i>")
                if t.seeds:
                    meta.append(f"🌱 <code>{t.seeds}</code>")
                if meta:
                    bar_line += "  " + "  ".join(meta)
                lines.append(bar_line)

            lines.append("")

    lines.append(SEP)
    return "\n".join(lines)


def render_panel_kb(uid: int):
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    tasks  = tracker.tasks_for_user(uid)
    active = [t for t in tasks if not t.is_terminal]
    rows: list = []

    # Per-task cancel buttons (2 per row, up to 8 tasks)
    row: list = []
    for t in active[:8]:
        label = (t.fname or t.label)[:14].strip()
        row.append(InlineKeyboardButton(
            f"❌ #{t.seq} {label}",
            callback_data=f"panel|cancel|{t.tid}",
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    # Footer
    if active:
        rows.append([
            InlineKeyboardButton("❌ Cancel All", callback_data=f"panel|cancel_all|{uid}"),
            InlineKeyboardButton("🔄 Refresh",    callback_data=f"panel|refresh|{uid}"),
            InlineKeyboardButton("✖ Close",       callback_data=f"panel|close|{uid}"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("🔄 Refresh", callback_data=f"panel|refresh|{uid}"),
            InlineKeyboardButton("✖ Close",    callback_data=f"panel|close|{uid}"),
        ])

    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────
# TaskRunner
# ─────────────────────────────────────────────────────────────

class TaskRunner:
    def __init__(self) -> None:
        self._task_handles: dict[str, asyncio.Task] = {}  # submit()-based tasks
        self._raw_tasks:    dict[str, asyncio.Task] = {}  # self-registering tasks
        self._running = False
        self._stats_task: Optional[asyncio.Task] = None    # FIX M-01

    def start(self) -> None:
        self._running = True
        global _task_semaphore
        _task_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        try:
            # FIX M-01 (audit v3): store reference so stop() can cancel it
            self._stats_task = asyncio.get_running_loop().create_task(_stats_updater())
            log.info("📊 Stats updater started")
        except Exception as e:
            log.warning("Could not start stats updater: %s", e)

    def stop(self) -> None:
        self._running = False
        # FIX M-01: cancel the stats updater task
        if self._stats_task and not self._stats_task.done():
            self._stats_task.cancel()
        for handle in list(self._task_handles.values()) + list(self._raw_tasks.values()):
            if not handle.done():
                handle.cancel()
        try:
            from services.downloader import _YTDLP_POOL
            if _YTDLP_POOL is not None:
                _YTDLP_POOL.shutdown(wait=False)
        except Exception:
            pass

    def register_raw(self, tid: str, task: Optional[asyncio.Task]) -> None:
        """
        Register any asyncio.Task not submitted via submit().
        Call from inside the coroutine:
            runner.register_raw(tid, asyncio.current_task())
        """
        if task is None:
            return
        self._raw_tasks[tid] = task
        # prune completed tasks
        stale = [k for k, t in self._raw_tasks.items() if t.done()]
        for k in stale:
            self._raw_tasks.pop(k, None)

    async def cancel_task(self, tid: str) -> bool:
        """
        Cancel task by TID — checks both registries.
        Returns True if the task was found and cancelled.
        """
        found = False

        h = self._task_handles.get(tid)
        if h and not h.done():
            h.cancel()
            found = True

        r = self._raw_tasks.get(tid)
        if r and not r.done():
            r.cancel()
            found = True

        t = tracker._tasks.get(tid)
        if t and not t.is_terminal:
            await tracker.finish(tid, success=False, msg="Cancelled")
            self._task_handles.pop(tid, None)
            self._raw_tasks.pop(tid, None)
            return True

        return found

    async def cancel_all(self, uid: int) -> int:
        count = 0
        for t in tracker.tasks_for_user(uid):
            if not t.is_terminal:
                if await self.cancel_task(t.tid):
                    count += 1
        return count

    def _wake_panel(self, uid: int, immediate: bool = False) -> None:
        """No-op stub — inline progress handles display."""
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
        else:
            try:
                from plugins.usage import session as _us
                if record.mode == "ul":
                    _us.bytes_uploaded += record.total or record.done
                    _us.files_uploaded += 1
                elif record.mode in ("dl", "magnet", "seedr"):
                    _us.bytes_downloaded += record.total or record.done
                    _us.files_downloaded += 1
            except Exception:
                pass
        finally:
            self._task_handles.pop(record.tid, None)
            self._raw_tasks.pop(record.tid, None)


runner = TaskRunner()
