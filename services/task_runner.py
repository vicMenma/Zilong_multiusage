"""
services/task_runner.py
Global task registry + unified live progress panel.

Upload concurrency gates removed — uploads now go straight through
(mirrors telegram.py style).  Download / processing slot limiting
(MAX_CONCURRENT) is kept so CPU-heavy FFmpeg jobs don't stack up.
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

MAX_CONCURRENT = 5
EDIT_INTERVAL  = 1.5
PANEL_TTL      = 600
TASK_LINGER    = 15

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
            result = await system_stats()
            _stats_cache.update(result)
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

_ENGINE_ICON: dict[str, str] = {
    "telegram": "📲", "ytdlp": "▶️", "aria2": "🧨", "direct": "🔗",
    "gdrive": "☁️", "ffmpeg": "⚙️", "magnet": "🧲", "mediafire": "📁",
}
_ENGINE_LABEL: dict[str, str] = {
    "telegram": "Telegram", "ytdlp": "yt-dlp", "aria2": "Aria2",
    "direct": "Direct", "gdrive": "GDrive", "ffmpeg": "FFmpeg",
    "magnet": "Aria2", "mediafire": "Mediafire",
}
_MODE_ICON: dict[str, str] = {
    "dl": "📥", "ul": "📤", "proc": "⚙️", "magnet": "🧲", "queue": "⏳",
}
_MODE_LABEL: dict[str, str] = {
    "dl": "Download", "ul": "Upload", "proc": "Processing",
    "magnet": "Torrent", "queue": "Queued",
}


@dataclass
class TaskRecord:
    tid:        str
    user_id:    int
    label:      str
    mode:       str   = "dl"
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
        if record.mode == "ul" and record.state == "⏳ Queued":
            record.state = "📤 Uploading"
        async with self._lock:
            self._evict()
            self._seq += 1
            record.seq = self._seq
            self._tasks[record.tid] = record
        existing = runner._panels.get(record.user_id)
        if not existing or existing._stopped:
            asyncio.create_task(runner.auto_panel(record.user_id))

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
        return sum(1 for t in self._tasks.values() if t.state == "⏳ Queued")

    def _evict(self) -> None:
        now  = time.time()
        dead = [k for k, t in self._tasks.items()
                if t.is_terminal and t.finished and now - t.finished > TASK_LINGER]
        for k in dead:
            self._tasks.pop(k, None)

    def _evict_sync(self) -> None:
        self._evict()


tracker = GlobalTracker()


# ─────────────────────────────────────────────────────────────
# Panel renderer
# ─────────────────────────────────────────────────────────────

def _prog_bar(pct: float, cells: int = 10) -> str:
    filled = round(pct / 100 * cells)
    return "█" * filled + "░" * (cells - filled)


_SEP  = "════════════════════════════════"
_SEP2 = "╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌"

_MODE_HEADER = {
    "dl":     "📥 Downloading",
    "ul":     "📤 Uploading",
    "magnet": "🧲 Torrent",
    "proc":   "⚙️ Processing",
}


async def render_panel(target_uid: Optional[int] = None) -> str:
    from core.bot_name import get_bot_name
    from services.utils import human_size, human_dur

    tasks  = tracker.tasks_for_user(target_uid) if target_uid else tracker.all_tasks()
    active = [t for t in tasks if not t.is_terminal]

    n_running = sum(
        1 for t in active
        if t.mode in ("dl", "proc", "magnet") and not t.state.startswith("⏳")
    )

    bot_name   = get_bot_name().upper()
    active_lbl = f"{len(active)} active" if active else "idle"

    lines: list[str] = [
        f"⚡ <b>{bot_name} MULTIUSAGE BOT</b>   <code>● {active_lbl}</code>",
        _SEP,
    ]

    for i, t in enumerate(active):
        pct      = t.pct()
        elapsed  = human_dur(int(t.elapsed)) if t.elapsed else "0s"
        fname    = t.fname or t.label
        fname_s  = (fname[:38] + "…") if len(fname) > 38 else fname
        mode_hdr = _MODE_HEADER.get(t.mode, f"⚙️ {t.mode}")

        if i > 0:
            lines += ["", _SEP2, ""]

        lines.append(f"<b>[ {i + 1} ]  {mode_hdr}</b>")

        if t.state.startswith("⏳"):
            lines += [
                f"🏷️ <b>Name</b>     <code>{fname_s}</code>",
                f"🔄 <b>Status</b>   Queued — waiting for a free slot",
            ]
            continue

        if t.state == "🔍 Analyzing…" or t.meta_phase:
            phase = "Fetching metadata…" if t.meta_phase else "Analyzing…"
            lines += [
                f"🏷️ <b>Name</b>     <code>{fname_s}</code>",
                f"🔄 <b>Status</b>   {phase}   <code>{elapsed}</code>",
            ]
            continue

        spd_s   = (human_size(t.speed) + "/s") if t.speed else "—"
        eta_s   = human_dur(t.eta) if t.eta > 0 else "—"
        mode_lbl = _MODE_HEADER.get(t.mode, t.mode).split(" ", 1)[-1]

        lines.append(f"🏷️ <b>Name</b>     <code>{fname_s}</code>")
        lines.append(f"🔄 <b>Status</b>   {mode_lbl}  via {t.engine_lbl}")
        lines.append(f"📊 <b>Progress</b> <code>{_prog_bar(pct)}</code>  <b>{pct:.1f}%</b>")
        lines.append(f"🔥 <b>Speed</b>    <code>{spd_s}</code>")

        if t.total:
            lines.append(
                f"💾 <b>Written</b>  <code>{human_size(t.done)}</code>"
                f"  of  <code>{human_size(t.total)}</code>"
            )
        elif t.done:
            lines.append(f"💾 <b>Written</b>  <code>{human_size(t.done)}</code>")

        lines.append(
            f"⏳ <b>Remains</b>  <code>{eta_s}</code>   elapsed <code>{elapsed}</code>"
        )
        if t.seeds:
            lines.append(f"🌱 <b>Seeds</b>    <code>{t.seeds}</code>")

    stats   = _stats_cache
    cpu     = stats.get("cpu", 0.0)
    rp      = stats.get("ram_pct", 0.0)
    df      = stats.get("disk_free", 0)
    dl      = stats.get("dl_speed", 0.0)
    ul      = stats.get("ul_speed", 0.0)
    slots_s = f"{MAX_CONCURRENT - n_running}/{MAX_CONCURRENT}"

    lines += [
        "",
        _SEP,
        f"🖥 <b>CPU</b>  <code>{cpu:.1f}%</code>      💾 <b>Mem</b>   <code>{rp:.1f}%</code>",
        f"💿 <b>Disk</b> <code>{human_size(df)}</code>  🎰 <b>Slots</b> <code>{slots_s}</code>",
        f"⬆️ <b>Up</b>   <code>{human_size(ul)}/s</code>  ⬇️ <b>Down</b>  <code>{human_size(dl)}/s</code>",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# LivePanel
# ─────────────────────────────────────────────────────────────

class LivePanel:
    def __init__(self, msg, uid: int) -> None:
        self._msg           = msg
        self._uid           = uid
        self._lock          = asyncio.Lock()
        self._task:         Optional[asyncio.Task] = None
        self._stopped       = False
        self._last_txt      = ""
        self._wake_ev       = asyncio.Event()
        self._last_edit     = 0.0
        self._last_activity = time.time()

    def wake(self, immediate: bool = False) -> None:
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
                log.warning("LivePanel edit uid=%d failed: %s", self._uid, exc)

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
# TaskRunner  (upload semaphore fully removed)
# ─────────────────────────────────────────────────────────────

class TaskRunner:
    def __init__(self) -> None:
        self._panels:      dict[int, LivePanel] = {}
        self._panel_locks: dict[int, asyncio.Lock] = {}
        self._running      = False

    def _panel_lock(self, uid: int) -> asyncio.Lock:
        if uid not in self._panel_locks:
            self._panel_locks[uid] = asyncio.Lock()
        return self._panel_locks[uid]

    def start(self) -> None:
        self._running = True
        global _task_semaphore
        _task_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_stats_updater())
            log.info("📊 Background stats updater started (every 5s)")
        except Exception as e:
            log.warning("Could not start stats updater: %s", e)

    def stop(self) -> None:
        self._running = False
        for p in self._panels.values():
            p.stop()
        try:
            from services.downloader import _YTDLP_POOL
            if _YTDLP_POOL is not None:
                _YTDLP_POOL.shutdown(wait=False)
        except Exception:
            pass

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

            if existing and not existing._stopped:
                existing.wake()
                return

            if existing:
                existing.stop()
                try:
                    await existing._msg.delete()
                except Exception:
                    pass
                self._panels.pop(uid, None)

            for attempt in range(2):
                try:
                    from core.session import get_client
                    from pyrogram import enums
                    from pyrogram.errors import FloodWait
                    client       = get_client()
                    initial_text = await render_panel(uid)
                    msg = await client.send_message(
                        uid, initial_text, parse_mode=enums.ParseMode.HTML,
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

    async def submit(
        self,
        user_id: int, label: str,
        coro_factory: Callable[[TaskRecord], Awaitable[None]],
        fname: str = "", total: int = 0,
        mode: str = "dl", engine: str = "",
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
