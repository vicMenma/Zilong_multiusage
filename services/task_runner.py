"""
services/task_runner.py
Global task registry + unified live progress panel.

Changes:
- MAX_CONCURRENT = 5: hard cap on parallel active tasks via asyncio.Semaphore
- Panel completely redesigned: card-style layout, richer visual hierarchy
- Panel auto-send on task arrival (auto_panel) preserved
- EDIT_INTERVAL kept at 1.5 s
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)

MAX_CONCURRENT = 5          # hard parallel task cap
EDIT_INTERVAL  = 1.5
PANEL_TTL      = 600
TASK_LINGER    = 15   # finished tasks stay 15s so panel sees them before eviction

# Global semaphore — limits truly concurrent task execution to 5
_task_semaphore: Optional[asyncio.Semaphore] = None


def _get_semaphore() -> asyncio.Semaphore:
    global _task_semaphore
    if _task_semaphore is None:
        _task_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    return _task_semaphore


# ─────────────────────────────────────────────────────────────
# TaskRecord
# ─────────────────────────────────────────────────────────────

_ENGINE_ICON: dict[str, str] = {
    "telegram": "📲",
    "ytdlp":    "▶️",
    "aria2":    "🧨",
    "direct":   "🔗",
    "gdrive":   "☁️",
    "ffmpeg":   "⚙️",
    "magnet":   "🧲",
    "mediafire":"📁",
}

_ENGINE_LABEL: dict[str, str] = {
    "telegram": "Telegram",
    "ytdlp":    "yt-dlp",
    "aria2":    "Aria2",
    "direct":   "Direct",
    "gdrive":   "GDrive",
    "ffmpeg":   "FFmpeg",
    "magnet":   "Aria2",
    "mediafire":"Mediafire",
}

_MODE_ICON: dict[str, str] = {
    "dl":     "📥",
    "ul":     "📤",
    "proc":   "⚙️",
    "magnet": "🧲",
    "queue":  "⏳",
}

_MODE_LABEL: dict[str, str] = {
    "dl":     "Download",
    "ul":     "Upload",
    "proc":   "Processing",
    "magnet": "Torrent",
    "queue":  "Queued",
}


@dataclass
class TaskRecord:
    tid:      str
    user_id:  int
    label:    str
    mode:     str   = "dl"
    engine:   str   = ""
    state:    str   = "⏳ Queued"
    fname:    str   = ""
    done:     int   = 0
    total:    int   = 0
    speed:    float = 0.0
    eta:      int   = 0
    elapsed:  float = 0.0
    seeds:    int   = 0
    meta_phase: bool = False
    started:  float = field(default_factory=time.time)
    finished: Optional[float] = None
    seq:      int   = 0

    _dirty:   asyncio.Event      = field(default_factory=asyncio.Event, repr=False)

    def update(self, **kw) -> None:
        changed = False
        for k, v in kw.items():
            if hasattr(self, k) and k not in ("_dirty",):
                if getattr(self, k) != v:
                    setattr(self, k, v)
                    changed = True
        self.elapsed = time.time() - self.started
        if self.state.startswith(("✅", "❌")) and self.finished is None:
            self.finished = time.time()
            changed = True
        if changed:
            try:
                self._dirty.set()
            except Exception:
                pass

    def pct(self) -> float:
        return min((self.done / self.total * 100) if self.total else 0.0, 100.0)

    @property
    def is_terminal(self) -> bool:
        return self.state.startswith(("✅", "❌"))

    @property
    def engine_icon(self) -> str:
        return _ENGINE_ICON.get(self.engine, "📦")

    @property
    def engine_lbl(self) -> str:
        return _ENGINE_LABEL.get(self.engine, self.engine or "")

    @property
    def mode_icon(self) -> str:
        return _MODE_ICON.get(self.mode, "📦")

    @property
    def mode_lbl(self) -> str:
        return _MODE_LABEL.get(self.mode, self.mode.upper())


# ─────────────────────────────────────────────────────────────
# GlobalTracker
# ─────────────────────────────────────────────────────────────

class GlobalTracker:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._lock:  asyncio.Lock = asyncio.Lock()
        self._seq:   int = 0

    def new_tid(self) -> str:
        return uuid.uuid4().hex[:8].upper()

    async def register(self, record: TaskRecord) -> None:
        # Uploads start immediately — show correct state from the start
        if record.mode == "ul" and record.state == "⏳ Queued":
            record.state = "📤 Uploading"
        async with self._lock:
            self._evict()
            self._seq += 1
            record.seq = self._seq
            self._tasks[record.tid] = record
        asyncio.create_task(
            runner.auto_panel(record.user_id)
        )

    async def update(self, tid: str, **kw) -> None:
        async with self._lock:
            t = self._tasks.get(tid)
            if t:
                t.update(**kw)

    async def finish(self, tid: str, success: bool = True, msg: str = "") -> None:
        state = "✅ Done" if success else f"❌ {msg or 'Failed'}"
        await self.update(tid, state=state)

    def tasks_for_user(self, user_id: int) -> list[TaskRecord]:
        self._evict_sync()
        return sorted(
            [t for t in self._tasks.values() if t.user_id == user_id],
            key=lambda t: t.seq,
        )

    def all_tasks(self) -> list[TaskRecord]:
        self._evict_sync()
        return sorted(self._tasks.values(), key=lambda t: t.seq)

    def active_tasks(self) -> list[TaskRecord]:
        return [t for t in self.all_tasks() if not t.is_terminal]

    def queued_count(self) -> int:
        """Tasks waiting for a semaphore slot."""
        return sum(1 for t in self._tasks.values() if t.state == "⏳ Queued")

    def _evict(self) -> None:
        now  = time.time()
        dead = [
            tid for tid, t in self._tasks.items()
            if t.is_terminal and t.finished and now - t.finished > TASK_LINGER
        ]
        for k in dead:
            self._tasks.pop(k, None)

    def _evict_sync(self) -> None:
        now  = time.time()
        dead = [
            tid for tid, t in self._tasks.items()
            if t.is_terminal and t.finished and now - t.finished > TASK_LINGER
        ]
        for k in dead:
            self._tasks.pop(k, None)


tracker = GlobalTracker()


# ─────────────────────────────────────────────────────────────
# Panel renderer  — redesigned card-style layout
# ─────────────────────────────────────────────────────────────

def _spd_icon(bps: float) -> str:
    mib = bps / (1024 * 1024)
    if mib >= 50: return "🚀"
    if mib >= 10: return "⚡"
    if mib >= 1:  return "🔥"
    if mib >= .1: return "🏃"
    return "🐢"


def _ring(p: float) -> str:
    return "🟢" if p < 40 else ("🟡" if p < 70 else "🔴")


def _bar(pct: float, cells: int = 10) -> str:
    """Compact emoji progress bar — 10 cells × 2 char-widths = 20 chars total.
    Fits on any mobile screen inside Telegram's monospace block."""
    filled = round(pct / 100 * cells)
    return "🟩" * filled + "⬜" * (cells - filled)


_SEP = "═══════════════════════════"


async def render_panel(target_uid: Optional[int] = None) -> str:
    from services.utils import human_size, human_dur, system_stats

    tasks  = tracker.tasks_for_user(target_uid) if target_uid else tracker.all_tasks()
    active = [t for t in tasks if not t.is_terminal]

    n_running = sum(
        1 for t in active
        if t.mode in ("dl", "proc", "magnet") and not t.state.startswith("⏳")
    )
    n_queued  = sum(1 for t in active if t.state.startswith("⏳"))
    n_uploads = sum(1 for t in active if t.mode == "ul")

    lines: list[str] = [
        "⚡️ <b>ZILONG MULTIUSAGE BOT</b>",
        _SEP,
        "",
    ]

    # ── Active tasks ──────────────────────────────────────────
    for t in active:
        pct     = t.pct()
        elapsed = human_dur(int(t.elapsed)) if t.elapsed else "0s"
        fname   = t.fname or t.label
        fname_s = (fname[:40] + "…") if len(fname) > 40 else fname

        if t.state.startswith("⏳"):
            lines += [
                f"📁 <code>{fname_s}</code>",
                f"⏳ <b>Queued</b> — waiting for a free slot",
                "",
            ]
            continue

        if t.state == "🔍 Analyzing…":
            lines += [
                f"📁 <code>{fname_s}</code>",
                f"🔍 <b>Analyzing…</b>  <code>{elapsed}</code>",
                "",
            ]
            continue

        if t.meta_phase:
            lines += [
                f"📁 <code>{fname_s}</code>",
                f"🔍 <b>Fetching metadata…</b>  <code>{elapsed}</code>",
                "",
            ]
            continue

        spd_s = (human_size(t.speed) + "/s") if t.speed else "—"
        eta_s = human_dur(t.eta) if t.eta > 0 else "—"
        mode_lbl = {"dl": "Download", "ul": "Upload", "magnet": "Torrent", "proc": "Processing"}.get(t.mode, t.mode)

        lines.append(f"📁 <code>{fname_s}</code>  <b>{pct:.1f}%</b>")
        lines.append(_bar(pct))
        lines.append(f"{_spd_icon(t.speed)} <b>Speed:</b> <code>{spd_s}</code>")

        if t.total:
            lines.append(
                f"🔄 <b>Done:</b> <code>{human_size(t.done)}</code>"
                f" of <code>{human_size(t.total)}</code>"
            )
        elif t.done:
            lines.append(f"🔄 <b>Done:</b> <code>{human_size(t.done)}</code>")

        lines.append(f"⏳ <b>ETA:</b> <code>{eta_s}</code> | <b>Elapsed:</b> <code>{elapsed}</code>")
        lines.append(f"⚙️ <b>Engine:</b> <code>{t.engine_lbl}</code> · <code>{mode_lbl}</code>")

        if t.seeds:
            lines.append(f"🌱 <b>Seeds:</b> <code>{t.seeds}</code>")

        lines.append("")

    # ── sysINFO footer ────────────────────────────────────────
    stats   = await system_stats()
    cpu     = stats.get("cpu", 0.0)
    rp      = stats.get("ram_pct", 0.0)
    df      = stats.get("disk_free", 0)
    dl      = stats.get("dl_speed", 0.0)
    ul      = stats.get("ul_speed", 0.0)
    slots_s = f"{MAX_CONCURRENT - n_running}/{MAX_CONCURRENT}"
    ul_tag  = f" · 📤 {n_uploads} uploading" if n_uploads else ""
    q_tag   = f" · ⏳ {n_queued} queued"      if n_queued  else ""

    lines += [
        _SEP,
        f"🖥 <b>CPU:</b> <code>{cpu:.1f}%</code> | 💿 <b>FREE:</b> <code>{human_size(df)}</code>",
        f"💾 <b>RAM:</b> <code>{rp:.1f}%</code> | {_ring(rp)} <b>DL Slots:</b> <code>{slots_s}</code>{ul_tag}{q_tag}",
        f"⬇️ <b>DL:</b> <code>{human_size(dl)}/s</code> | ⬆️ <b>UL:</b> <code>{human_size(ul)}/s</code>",
    ]

    return "\n".join(lines)



class LivePanel:
    def __init__(self, msg, uid: int) -> None:
        self._msg      = msg
        self._uid      = uid
        self._lock     = asyncio.Lock()
        self._task:    Optional[asyncio.Task] = None
        self._stopped  = False
        self._last_txt = ""
        self._wake_ev  = asyncio.Event()
        self._last_edit = 0.0
        self._last_activity = time.time()

    def wake(self, immediate: bool = False) -> None:
        """Wake the panel loop for a re-render."""
        self._last_activity = time.time()
        if immediate:
            self._last_edit = 0.0
        self._wake_ev.set()

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._loop())

    def stop(self) -> None:
        self._stopped = True
        self._wake_ev.set()
        if self._task and not self._task.done():
            self._task.cancel()

    def is_idle(self) -> bool:
        return time.time() - self._last_activity > PANEL_TTL

    async def _edit(self) -> None:
        from services.utils import safe_edit
        from pyrogram import enums
        async with self._lock:
            try:
                text = await render_panel(self._uid)
                if text == self._last_txt:
                    return
                await safe_edit(self._msg, text, parse_mode=enums.ParseMode.HTML)
                self._last_txt  = text
                self._last_edit = time.time()
            except Exception as exc:
                log.debug("LivePanel edit uid=%d: %s", self._uid, exc)

    async def _loop(self) -> None:
        had_tasks = False

        while not self._stopped:
            try:
                await asyncio.wait_for(self._wake_ev.wait(), timeout=EDIT_INTERVAL)
            except asyncio.TimeoutError:
                pass

            if self._stopped:
                break

            self._wake_ev.clear()

            since_last = time.time() - self._last_edit
            if since_last < 1.0:
                await asyncio.sleep(1.0 - since_last)

            tasks = tracker.tasks_for_user(self._uid)

            if tasks:
                had_tasks = True

            # Delete panel only when ALL tasks are fully gone from memory
            if had_tasks and not tasks:
                try:
                    await self._msg.delete()
                except Exception:
                    pass
                self._stopped = True
                break

            await self._edit()

            if self.is_idle():
                log.debug("LivePanel uid=%d idle TTL — stopping", self._uid)
                self._stopped = True
                break

        if runner._panels.get(self._uid) is self:
            runner._panels.pop(self._uid, None)


# ─────────────────────────────────────────────────────────────
# TaskRunner
# ─────────────────────────────────────────────────────────────

class TaskRunner:
    # How many uploads can run concurrently.
    # The original semaphore=1 was a workaround for a stock pyrogram deadlock
    # where two concurrent send_video() calls competed for the same single DC
    # connection.  pyrofork >= 2.3.40 with concurrent_transmissions=N opens N
    # independent encrypted streams per upload, so each upload manages its own
    # connection pool internally — the deadlock cannot occur anymore.
    # 3 is the safe maximum: above this Telegram's MTProto rate-limiter starts
    # dropping chunks for a single bot token, which actually reduces throughput.
    _UPLOAD_CONCURRENCY: int = int(os.environ.get("UPLOAD_CONCURRENCY", "3"))

    def __init__(self) -> None:
        self._panels:       dict[int, LivePanel] = {}
        self._panel_locks:  dict[int, asyncio.Lock] = {}
        self._running       = False
        self._upload_sem:   asyncio.Semaphore | None = None

    def _get_upload_sem(self) -> asyncio.Semaphore:
        if self._upload_sem is None:
            self._upload_sem = asyncio.Semaphore(self._UPLOAD_CONCURRENCY)
        return self._upload_sem

    def _panel_lock(self, uid: int) -> asyncio.Lock:
        if uid not in self._panel_locks:
            self._panel_locks[uid] = asyncio.Lock()
        return self._panel_locks[uid]

    def start(self) -> None:
        self._running = True
        # Pre-create the semaphore on the running event loop
        global _task_semaphore
        _task_semaphore  = asyncio.Semaphore(MAX_CONCURRENT)
        self._upload_sem = asyncio.Semaphore(self._UPLOAD_CONCURRENCY)

    def stop(self) -> None:
        self._running = False
        for p in self._panels.values():
            p.stop()
        # Shut down the yt-dlp process pool if it was created
        try:
            from services.downloader import _YTDLP_POOL
            if _YTDLP_POOL is not None:
                _YTDLP_POOL.shutdown(wait=False)
        except Exception:
            pass

    # ── Panel lifecycle ───────────────────────────────────────

    def open_panel(self, uid: int, msg, target_uid: Optional[int] = None) -> LivePanel:
        old = self._panels.get(uid)
        if old:
            old.stop()
        effective_uid = target_uid if target_uid is not None else uid
        panel = LivePanel(msg, uid=effective_uid)
        self._panels[uid] = panel
        panel.start()
        return panel

    def attach_panel(self, uid: int, msg) -> None:
        panel = self._panels.get(uid)
        if panel and not panel._stopped:
            panel.wake()
            return
        if panel:
            panel.stop()
        new_panel = LivePanel(msg, uid=uid)
        self._panels[uid] = new_panel
        new_panel.start()

    async def ensure_panel(self, uid: int, client, chat_id: int) -> None:
        async with self._panel_lock(uid):
            panel = self._panels.get(uid)
            if panel and not panel._stopped:
                panel.wake()

    async def auto_panel(self, uid: int) -> None:
        async with self._panel_lock(uid):
            existing = self._panels.get(uid)

            # Always stop the old panel and delete its message so the new
            # panel appears at the bottom of the chat on every new link sent.
            if existing:
                existing.stop()
                try:
                    await existing._msg.delete()
                except Exception:
                    pass
                self._panels.pop(uid, None)

            # Send a fresh panel — handle FloodWait by waiting then retrying once
            for attempt in range(2):
                try:
                    from core.session import get_client
                    from pyrogram import enums
                    from pyrogram.errors import FloodWait
                    client = get_client()
                    initial_text = await render_panel(uid)
                    msg = await client.send_message(
                        uid,
                        initial_text,
                        parse_mode=enums.ParseMode.HTML,
                    )
                    new_panel = LivePanel(msg, uid=uid)
                    self._panels[uid] = new_panel
                    new_panel.start()
                    return
                except FloodWait as fw:
                    if attempt == 0:
                        log.warning("auto_panel FloodWait %ds — waiting", fw.value)
                        await asyncio.sleep(fw.value)
                    else:
                        log.warning("auto_panel uid=%d FloodWait on retry — skipping", uid)
                except Exception as exc:
                    log.warning("auto_panel uid=%d failed: %s", uid, exc)
                    return

    def close_panel(self, uid: int) -> None:
        p = self._panels.pop(uid, None)
        if p:
            p.stop()

    def _wake_panel(self, uid: int, immediate: bool = False) -> None:
        p = self._panels.get(uid)
        if p:
            p.wake(immediate=immediate)

    # ── Task submission with semaphore ─────────────────────────

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
        tid    = tracker.new_tid()
        record = TaskRecord(
            tid=tid, user_id=user_id, label=label,
            fname=fname, total=total, mode=mode, engine=engine,
        )
        await tracker.register(record)
        loop = asyncio.get_running_loop()
        loop.create_task(self._run_task(record, coro_factory))
        return record

    async def _run_task(self, record: TaskRecord, factory) -> None:
        # Uploads are pure network I/O — never gate them, always run immediately.
        # Only downloads (dl/magnet) and processing (proc) consume CPU/disk slots.
        needs_slot = record.mode in ("dl", "proc", "magnet")

        if needs_slot:
            sem = _get_semaphore()
            if sem._value == 0:
                record.update(state="⏳ Queued")
            async with sem:
                record.update(state="⚙️ Running")
                try:
                    await factory(record)
                    record.update(state="✅ Done", done=record.total or record.done)
                except asyncio.CancelledError:
                    record.update(state="❌ Cancelled")
                except Exception as exc:
                    log.error("Task %s failed: %s", record.tid, exc)
                    record.update(state=f"❌ {str(exc)[:60]}")
        else:
            # Uploads and other I/O — run freely, no slot consumed
            record.update(state="📤 Uploading")
            try:
                await factory(record)
                record.update(state="✅ Done", done=record.total or record.done)
            except asyncio.CancelledError:
                record.update(state="❌ Cancelled")
            except Exception as exc:
                log.error("Task %s failed: %s", record.tid, exc)
                record.update(state=f"❌ {str(exc)[:60]}")


runner = TaskRunner()
