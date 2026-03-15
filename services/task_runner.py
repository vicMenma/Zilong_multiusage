"""
services/task_runner.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Global task registry + unified live progress panel.

Every operation in the bot (download, upload, FFmpeg, stream
extract, torrent) registers a TaskRecord here.

The unified panel (/status) shows ALL active tasks across ALL
plugins in one message, auto-refreshed every 3 seconds, with
CPU / RAM / Disk / Network stats at the bottom.

Key design decisions
  • One asyncio.Lock per panel message — no concurrent edits
  • Edit throttle: min 3s between edits per panel (flood safe)
  • Panel auto-expires after 5 min (sends final snapshot + "Expired")
  • Tasks stay visible for 15s after completion (✅/❌), then evict
  • Any plugin calls:  tracker.register(tid, record)  /  tracker.update(tid, **kw)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)

MAX_WORKERS   = 4      # concurrent queue jobs
EDIT_INTERVAL = 3.0    # min seconds between panel edits
PANEL_TTL     = 300    # panel auto-expires after 5 min
TASK_LINGER   = 15     # finished tasks stay visible for 15s


# ─────────────────────────────────────────────────────────────
# TaskRecord  — one per active operation
# ─────────────────────────────────────────────────────────────

_ENGINE_LABEL: dict[str, str] = {
    "telegram": "📲 Telegram",
    "ytdlp":    "▶️  yt-dlp",
    "aria2":    "🧨 Aria2",
    "direct":   "🔗 Direct",
    "gdrive":   "☁️  GDrive",
    "ffmpeg":   "⚙️  FFmpeg",
    "magnet":   "🧲 Aria2",
    "mediafire":"📁 Mediafire",
}

_MODE_ICON: dict[str, str] = {
    "dl":     "📥",
    "ul":     "📤",
    "proc":   "⚙️",
    "magnet": "🧲",
    "queue":  "⏳",
}


@dataclass
class TaskRecord:
    tid:     str
    user_id: int
    label:   str              # short human name shown in panel header
    mode:    str  = "dl"      # dl | ul | proc | magnet | queue
    engine:  str  = ""        # ytdlp | aria2 | ffmpeg | telegram | direct …
    state:   str  = "⏳ Queued"
    fname:   str  = ""
    done:    int  = 0
    total:   int  = 0
    speed:   float = 0.0
    eta:     int  = 0
    elapsed: float = 0.0
    seeds:   int  = 0
    started: float = field(default_factory=time.time)
    finished: Optional[float] = None   # set when terminal state reached

    # internal — not shown
    _factory: Optional[Callable] = field(default=None, repr=False, compare=False)

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

    @property
    def engine_label(self) -> str:
        return _ENGINE_LABEL.get(self.engine, self.engine or "")

    @property
    def mode_icon(self) -> str:
        return _MODE_ICON.get(self.mode, "📦")


# ─────────────────────────────────────────────────────────────
# GlobalTracker  — singleton registry of all TaskRecords
# ─────────────────────────────────────────────────────────────

class GlobalTracker:
    """
    All plugins call tracker.register() / tracker.update().
    The panel broadcaster reads all_tasks() to build the unified panel.
    """

    def __init__(self) -> None:
        self._tasks:  dict[str, TaskRecord] = {}
        self._lock:   asyncio.Lock = asyncio.Lock()

    def new_tid(self) -> str:
        return uuid.uuid4().hex[:8].upper()

    async def register(self, record: TaskRecord) -> None:
        async with self._lock:
            self._evict()
            self._tasks[record.tid] = record

    async def update(self, tid: str, **kw) -> None:
        async with self._lock:
            t = self._tasks.get(tid)
            if t:
                t.update(**kw)

    async def finish(self, tid: str, success: bool = True, msg: str = "") -> None:
        state = ("✅ Done" if success else f"❌ {msg}") if not msg or success else f"❌ {msg}"
        await self.update(tid, state=state)

    def all_tasks(self) -> list[TaskRecord]:
        """Return snapshot; finished tasks linger for TASK_LINGER seconds."""
        now  = time.time()
        dead = [
            tid for tid, t in self._tasks.items()
            if t.is_terminal and t.finished and now - t.finished > TASK_LINGER
        ]
        for tid in dead:
            self._tasks.pop(tid, None)
        return list(self._tasks.values())

    def active_tasks(self) -> list[TaskRecord]:
        return [t for t in self.all_tasks() if not t.is_terminal]

    def _evict(self) -> None:
        """Remove stale terminal tasks."""
        now  = time.time()
        dead = [
            tid for tid, t in self._tasks.items()
            if t.is_terminal and t.finished and now - t.finished > TASK_LINGER
        ]
        for tid in dead:
            self._tasks.pop(tid, None)


tracker = GlobalTracker()


# ─────────────────────────────────────────────────────────────
# Unified panel renderer
# ─────────────────────────────────────────────────────────────

async def render_panel(target_uid: Optional[int] = None) -> str:
    """
    Build the full unified panel text.
    target_uid: if set, show only that user's tasks (for /status).
                if None, show ALL tasks (admin /stats).
    """
    from services.utils import (
        system_stats, human_size, human_dur,
        pct_bar, speed_emoji,
    )

    tasks = tracker.all_tasks()
    if target_uid is not None:
        tasks = [t for t in tasks if t.user_id == target_uid]

    active   = [t for t in tasks if not t.is_terminal]
    finished = [t for t in tasks if t.is_terminal]

    lines: list[str] = []

    # ── Header ────────────────────────────────────────────────
    if not tasks:
        lines += [
            "⚡ <b>ZILONG BOT</b>  <i>— No active tasks</i>",
            "──────────────────────",
        ]
    else:
        n_act = len(active)
        n_fin = len(finished)
        parts = []
        if n_act:  parts.append(f"{n_act} active")
        if n_fin:  parts.append(f"{n_fin} finished")
        lines += [
            f"⚡ <b>ZILONG BOT</b>  <i>— {', '.join(parts)}</i>",
            "──────────────────────",
        ]

    # ── Active tasks ──────────────────────────────────────────
    for t in active:
        pct    = t.pct()
        bar    = pct_bar(pct, 14)
        spd_s  = (human_size(t.speed) + "/s") if t.speed else "—"
        eta_s  = human_dur(t.eta) if t.eta > 0 else "—"
        el_s   = human_dur(int(t.elapsed)) if t.elapsed else "0s"
        done_s = human_size(t.done)
        tot_s  = human_size(t.total) if t.total else ""
        fname  = (t.fname[:40] + "…") if len(t.fname) > 40 else t.fname

        lines += [
            f"",
            f"{t.mode_icon} <b>{t.label}</b>  <code>[{t.tid}]</code>",
        ]
        if fname:
            lines.append(f"    📄 <code>{fname}</code>")
        lines += [
            f"    <code>[{bar}]</code>  <b>{pct:.1f}%</b>  {t.state}",
            f"    {speed_emoji(t.speed)} <code>{spd_s}</code>"
            + (f"  ·  {t.engine_label}" if t.engine_label else ""),
            f"    ⏳ ETA <code>{eta_s}</code>  🕰 <code>{el_s}</code>",
            f"    ✅ <code>{done_s}</code>"
            + (f" / <code>{tot_s}</code>" if tot_s else ""),
        ]
        if t.seeds:
            lines.append(f"    🌱 Seeders <code>{t.seeds}</code>")

    # ── Finished tasks (brief) ────────────────────────────────
    if finished:
        lines.append("")
        for t in finished:
            fname = (t.fname[:35] + "…") if len(t.fname) > 35 else t.fname
            sz_s  = human_size(t.done) if t.done else human_size(t.total)
            el_s  = human_dur(int(t.elapsed)) if t.elapsed else ""
            lines.append(
                f"{t.state}  <code>{fname or t.label}</code>"
                + (f"  <code>{sz_s}</code>" if sz_s else "")
                + (f"  <i>({el_s})</i>" if el_s else "")
            )

    # ── System stats footer ───────────────────────────────────
    stats = await system_stats()
    cpu   = stats.get("cpu", 0.0)
    rp    = stats.get("ram_pct", 0.0)
    df    = stats.get("disk_free", 0)
    dl    = stats.get("dl_speed", 0.0)
    ul    = stats.get("ul_speed", 0.0)

    def ring(p: float) -> str:
        return "🟢" if p < 40 else ("🟡" if p < 70 else "🔴")

    lines += [
        "",
        "──────────────────────",
        f"🖥  CPU  {ring(cpu)}<code>[{pct_bar(cpu, 10)}]</code> <b>{cpu:.0f}%</b>"
        f"   💾 RAM  {ring(rp)}<code>[{pct_bar(rp, 10)}]</code> <b>{rp:.0f}%</b>",
        f"💿  Disk free <code>{human_size(df)}</code>"
        f"   🌐 ⬇<code>{human_size(dl)}/s</code>  ⬆<code>{human_size(ul)}/s</code>",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# LivePanel  — one per /status message, auto-refreshes
# ─────────────────────────────────────────────────────────────

class LivePanel:
    """
    Wraps a Telegram message and keeps it updated with render_panel()
    every EDIT_INTERVAL seconds until TTL expires or stop() is called.
    """

    def __init__(self, msg, uid: Optional[int] = None) -> None:
        self._msg       = msg
        self._uid       = uid
        self._lock      = asyncio.Lock()
        self._task:  Optional[asyncio.Task] = None
        self._stopped   = False
        self._last_edit = 0.0

    def start(self) -> None:
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._loop())

    def stop(self) -> None:
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()

    async def refresh(self) -> None:
        """Force an immediate refresh (called after task state changes)."""
        now = time.time()
        if now - self._last_edit < 1.0:
            return
        await self._edit()

    async def _edit(self) -> None:
        from services.utils import safe_edit
        from pyrogram import enums
        async with self._lock:
            try:
                text = await render_panel(self._uid)
                await safe_edit(self._msg, text, parse_mode=enums.ParseMode.HTML)
                self._last_edit = time.time()
            except Exception as exc:
                log.debug("LivePanel edit error: %s", exc)

    async def _loop(self) -> None:
        deadline = time.time() + PANEL_TTL
        while not self._stopped and time.time() < deadline:
            await asyncio.sleep(EDIT_INTERVAL)
            if self._stopped:
                break
            await self._edit()

        if not self._stopped:
            # Panel expired — add notice
            from services.utils import safe_edit
            from pyrogram import enums
            try:
                text = await render_panel(self._uid)
                await safe_edit(
                    self._msg,
                    text + "\n\n<i>⏱ Panel expired. Send /status to refresh.</i>",
                    parse_mode=enums.ParseMode.HTML,
                )
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# TaskRunner  — queue-based job executor
# ─────────────────────────────────────────────────────────────

class TaskRunner:
    """
    Global job queue. Plugins can submit coroutines that get a TaskRecord
    automatically registered in the GlobalTracker.
    """

    def __init__(self) -> None:
        self._queue:   asyncio.Queue         = asyncio.Queue()
        self._workers: list[asyncio.Task]    = []
        self._panels:  dict[int, LivePanel]  = {}   # uid → LivePanel
        self._running  = False

    # ── Lifecycle ─────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        loop = asyncio.get_event_loop()
        for _ in range(MAX_WORKERS):
            self._workers.append(loop.create_task(self._worker()))

    def stop(self) -> None:
        self._running = False
        for w in self._workers:
            w.cancel()
        for p in self._panels.values():
            p.stop()

    # ── Panel management ──────────────────────────────────────

    def open_panel(self, uid: int, msg, target_uid: Optional[int] = None) -> LivePanel:
        """Open (or replace) the live panel for a user."""
        old = self._panels.get(uid)
        if old:
            old.stop()
        panel = LivePanel(msg, uid=target_uid)
        self._panels[uid] = panel
        panel.start()
        return panel

    def close_panel(self, uid: int) -> None:
        p = self._panels.pop(uid, None)
        if p:
            p.stop()

    # ── Submit a tracked job ──────────────────────────────────

    async def submit(
        self,
        user_id: int,
        label:   str,
        coro_factory: Callable[[TaskRecord], Awaitable[None]],
        fname:  str = "",
        total:  int = 0,
        mode:   str = "dl",
        engine: str = "",
    ) -> TaskRecord:
        """
        Submit a job. Returns TaskRecord immediately.
        coro_factory receives the TaskRecord and calls record.update(**kw).
        """
        tid    = tracker.new_tid()
        record = TaskRecord(
            tid=tid, user_id=user_id, label=label,
            fname=fname, total=total, mode=mode, engine=engine,
        )
        await tracker.register(record)
        await self._queue.put((record, coro_factory))
        return record

    # ── Worker ────────────────────────────────────────────────

    async def _worker(self) -> None:
        while self._running:
            try:
                record, factory = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0,
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            record.update(state="⚙️ Running")
            try:
                await factory(record)
                record.update(state="✅ Done", done=record.total or record.done)
            except asyncio.CancelledError:
                record.update(state="❌ Cancelled")
            except Exception as exc:
                log.error("Task %s failed: %s", record.tid, exc)
                short = str(exc)[:60]
                record.update(state=f"❌ {short}")
            finally:
                self._queue.task_done()


# ── Singletons ────────────────────────────────────────────────
runner = TaskRunner()
