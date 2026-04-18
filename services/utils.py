"""
services/utils.py
Pure helper functions — no Telegram types, no side effects.

REWRITE:
  - New progress_panel() matching the Image 1 design:
      DOWNLOADING FROM » Link 01
      Name » filename.mkv
      [████████░░] 68.3%
      Speed / Engine / ETA / Elapsed / Done / Total
      CPU [█████░] 47% / RAM / Disk Free
      Footer quote
  - smart_clean_filename: noise guard after 3rd token (kept)
  - All language tables kept
  - system_stats kept
"""
from __future__ import annotations

import asyncio
import logging
import os
import re as _re
import shutil
import time
from typing import Optional

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Language tables
# ─────────────────────────────────────────────────────────────

LANG_FLAG: dict[str, str] = {
    "eng": "🇬🇧", "en": "🇬🇧", "jpn": "🇯🇵", "ja": "🇯🇵",
    "fra": "🇫🇷", "fre": "🇫🇷", "fr": "🇫🇷", "deu": "🇩🇪", "ger": "🇩🇪", "de": "🇩🇪",
    "spa": "🇪🇸", "es": "🇪🇸", "por": "🇧🇷", "pt": "🇧🇷", "ita": "🇮🇹", "it": "🇮🇹",
    "kor": "🇰🇷", "ko": "🇰🇷", "chi": "🇨🇳", "zho": "🇨🇳", "zh": "🇨🇳",
    "rus": "🇷🇺", "ru": "🇷🇺", "ara": "🇸🇦", "ar": "🇸🇦", "hin": "🇮🇳", "hi": "🇮🇳",
    "tha": "🇹🇭", "th": "🇹🇭", "vie": "🇻🇳", "vi": "🇻🇳", "ind": "🇮🇩", "id": "🇮🇩",
    "msa": "🇲🇾", "ms": "🇲🇾", "tur": "🇹🇷", "tr": "🇹🇷", "pol": "🇵🇱", "pl": "🇵🇱",
    "nld": "🇳🇱", "nl": "🇳🇱", "swe": "🇸🇪", "sv": "🇸🇪", "nor": "🇳🇴", "no": "🇳🇴",
    "dan": "🇩🇰", "da": "🇩🇰", "fin": "🇫🇮", "fi": "🇫🇮", "heb": "🇮🇱", "he": "🇮🇱",
    "ces": "🇨🇿", "cze": "🇨🇿", "ron": "🇷🇴", "rum": "🇷🇴", "hun": "🇭🇺", "hu": "🇭🇺",
    "bul": "🇧🇬", "bg": "🇧🇬", "ukr": "🇺🇦", "uk": "🇺🇦", "und": "🌐",
}

LANG_NAME: dict[str, str] = {
    "eng": "English", "en": "English", "jpn": "Japanese", "ja": "Japanese",
    "fra": "French", "fre": "French", "fr": "French",
    "deu": "German", "ger": "German", "de": "German",
    "spa": "Spanish", "es": "Spanish", "por": "Portuguese", "pt": "Portuguese",
    "ita": "Italian", "it": "Italian", "kor": "Korean", "ko": "Korean",
    "chi": "Chinese", "zho": "Chinese", "zh": "Chinese",
    "rus": "Russian", "ru": "Russian", "ara": "Arabic", "ar": "Arabic",
    "hin": "Hindi", "hi": "Hindi", "tha": "Thai", "th": "Thai",
    "vie": "Vietnamese", "vi": "Vietnamese", "ind": "Indonesian", "id": "Indonesian",
    "msa": "Malay", "ms": "Malay", "tur": "Turkish", "tr": "Turkish",
    "pol": "Polish", "pl": "Polish", "nld": "Dutch", "nl": "Dutch",
    "swe": "Swedish", "sv": "Swedish", "nor": "Norwegian", "no": "Norwegian",
    "dan": "Danish", "da": "Danish", "fin": "Finnish", "fi": "Finnish",
    "heb": "Hebrew", "he": "Hebrew", "ces": "Czech", "cze": "Czech",
    "ron": "Romanian", "rum": "Romanian", "hun": "Hungarian", "hu": "Hungarian",
    "bul": "Bulgarian", "bg": "Bulgarian", "ukr": "Ukrainian", "uk": "Ukrainian",
    "und": "Unknown",
}


def lang_flag(lang: str) -> str:
    return LANG_FLAG.get(lang.lower(), "🌐")


def lang_name(lang: str) -> str:
    return LANG_NAME.get(lang.lower(), lang.upper())


# ─────────────────────────────────────────────────────────────
# Formatters
# ─────────────────────────────────────────────────────────────

def human_size(n: float) -> str:
    if n < 0:
        n = 0.0
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} PiB"


def human_dur(secs: float) -> str:
    s = int(max(0, secs))
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d:   return f"{d}d {h}h {m}m"
    if h:   return f"{h}h {m}m {s}s"
    if m:   return f"{m}m {s}s"
    return f"{s}s"


_KiB = 1024
_MiB = 1024 * _KiB

def speed_emoji(speed: float) -> str:
    """Return an emoji that reflects the current transfer speed (bytes/s)."""
    if speed <= 0:          return "🐌"   # stalled / no data yet
    if speed < 512 * _KiB: return "🐢"   # < 512 KiB/s  — very slow
    if speed < 2  * _MiB:  return "🔥"   # < 2 MiB/s    — normal
    if speed < 10 * _MiB:  return "⚡"   # < 10 MiB/s   — fast
    if speed < 50 * _MiB:  return "🚀"   # < 50 MiB/s   — very fast
    return "🌪️"                           # ≥ 50 MiB/s   — blazing


def fmt_hms(secs: float) -> str:
    s = int(max(0, secs))
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def pct_bar(pct: float, length: int = 14) -> str:
    filled = int(min(max(pct, 0), 100) / 100 * length)
    return "█" * filled + "░" * (length - filled)


# ─────────────────────────────────────────────────────────────
# Engine display map
# ─────────────────────────────────────────────────────────────

_ENGINE_DISPLAY: dict[str, str] = {
    "telegram":  "Telegram 📲",
    "ytdlp":     "yt-dlp ▶️",
    "aria2":     "Aria2c 🧨",
    "magnet":    "Aria2c 🧲",
    "direct":    "Direct 🔗",
    "gdrive":    "GDrive ☁️",
    "ffmpeg":    "FFmpeg ⚙️",
    "mediafire": "Mediafire 📁",
    "cc":        "CloudConvert ☁️",
}


def engine_display(engine: str) -> str:
    return _ENGINE_DISPLAY.get(engine, engine.capitalize() if engine else "—")


# ─────────────────────────────────────────────────────────────
# Progress panel — new design matching Image 1
# ─────────────────────────────────────────────────────────────

_PANEL_FOOTER = (
    "💗 <i>When I'm Doin This, Do Something Else !"
    " Because, Time Is Precious</i> ✨"
)

_MODE_META: dict[str, tuple[str, str, str]] = {
    "dl":     ("📥", "DOWNLOADING FROM", "🔗"),
    "ul":     ("📤", "UPLOADING TO",     "📡"),
    "magnet": ("🧲", "DOWNLOADING FROM", "🔗"),
    "proc":   ("⚙️", "PROCESSING",       "🔧"),
    "queue":  ("⏳", "QUEUED",           "📋"),
}


def _progress_panel_b(
    *,
    mode:       str   = "dl",
    fname:      str   = "",
    done:       int   = 0,
    total:      int   = 0,
    speed:      float = 0.0,
    eta:        int   = 0,
    elapsed:    float = 0.0,
    engine:     str   = "",
    state:      str   = "",
    link_label: str   = "Link 01",
    cpu:        float = 0.0,
    ram_used:   int   = 0,
    disk_free:  int   = 0,
    seeds:      int   = 0,
) -> str:
    """
    Style B — Cards, no monospace.
    Labels in bold, values in italic.
    Stats use Option-4 layout:
        🔥 Speed  —  ·  ⏳ ETA  —  ·  🕰 Elapsed  2s
        · · · · · · · · · · · ·
        ✅ Done  5.50 KiB  ·  📦 Total  5.50 KiB
    """
    pct    = min((done / total * 100) if total else 0.0, 100.0)
    bar_w  = 12
    filled = round(pct / 100 * bar_w)
    bar    = "▰" * filled + "▱" * (bar_w - filled)

    spd_s  = (human_size(speed) + "/s") if speed else "—"
    eta_s  = human_dur(eta) if eta > 0 else "—"
    el_s   = human_dur(elapsed) if elapsed else "0s"
    done_s = human_size(done)
    tot_s  = human_size(total) if total else "—"
    eng_s  = engine_display(engine)

    m_icon, m_hdr, _ = _MODE_META.get(mode, ("📦", "PROCESSING", "🔧"))
    fname_s = (fname[:46] + "…") if len(fname) > 46 else fname
    spd_icon = speed_emoji(speed)

    SEP = "──────────────────────"
    DOT = "· · · · · · · · · · · ·"

    lines: list[str] = [
        f"{m_icon} <b>{m_hdr}</b>",
        SEP,
        f"🏷  Name »  <b>{fname_s}</b>",
        "",
        f"{bar}  <b>{pct:.1f}%</b>  ·  <i>{eng_s}</i>",
        "",
        SEP,
        f"{spd_icon} <b>Speed</b>  <i>{spd_s}</i>   ·  ",
        f"⏳ <b>ETA</b>  <i>{eta_s}</i>   ·  ",
        f"🕰 <b>Elapsed</b>  <i>{el_s}</i>",
        DOT,
        f"✅ <b>Done</b>  <i>{done_s}</i> · 📦 <b>Total</b>  <i>{tot_s}</i>",
    ]

    if seeds:
        lines.append(f"🌱 <b>Seeds</b>  <i>{seeds}</i>")

    if cpu or ram_used or disk_free:
        ram_s  = human_size(ram_used)
        disk_s = human_size(disk_free)
        lines += [
            "",
            SEP,
            f"🖥 <b>CPU</b> <i>{cpu:.0f}%</i>  ·  💾 <b>RAM</b> <i>{ram_s}</i>  ·  💿 <b>Disk</b> <i>{disk_s} free</i>",
        ]

    lines += ["", SEP, _PANEL_FOOTER]
    return "\n".join(lines)


def _progress_panel_c(
    *,
    mode:       str   = "dl",
    fname:      str   = "",
    done:       int   = 0,
    total:      int   = 0,
    speed:      float = 0.0,
    eta:        int   = 0,
    elapsed:    float = 0.0,
    engine:     str   = "",
    state:      str   = "",
    link_label: str   = "Link 01",
    cpu:        float = 0.0,
    ram_used:   int   = 0,
    disk_free:  int   = 0,
    seeds:      int   = 0,
) -> str:
    """
    Style C — Dark accent (Alt 4).
    ▬▬▬ thick separators, ━━━╌╌╌ progress bar, no monospace.
    System row uses plain italic values (no labels).

        🧲 DOWNLOADING FROM
        ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
        🏷  [BuriBuri] Crayon Shin-chan…

        ━━━━━━━━╌╌╌╌  66.7%  ·  Aria2c 🧲

        ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
        🔥 Speed  42.5 MiB/s  ·  ⏳ ETA  1m 12s  ·  🕰 Elapsed  3m 45s
        · · · · · · · · · · · ·
        ✅ Done  892 MiB  ·  📦 Total  1.31 GiB
        ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
        🖥 47%  ·  💾 1.24 GiB  ·  💿 48.3 GiB free
        ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
        💗 When I'm Doin This…
    """
    pct    = min((done / total * 100) if total else 0.0, 100.0)
    bar_w  = 12
    filled = round(pct / 100 * bar_w)
    bar    = "━" * filled + "╌" * (bar_w - filled)

    spd_s  = (human_size(speed) + "/s") if speed else "—"
    eta_s  = human_dur(eta) if eta > 0 else "—"
    el_s   = human_dur(elapsed) if elapsed else "0s"
    done_s = human_size(done)
    tot_s  = human_size(total) if total else "—"
    eng_s  = engine_display(engine)

    m_icon, m_hdr, _ = _MODE_META.get(mode, ("📦", "PROCESSING", "🔧"))
    fname_s = (fname[:46] + "…") if len(fname) > 46 else fname
    spd_icon = speed_emoji(speed)

    SEP = "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"
    DOT = "· · · · · · · · · · · ·"

    lines: list[str] = [
        f"{m_icon} <b>{m_hdr}</b>",
        SEP,
        f"🏷  <b>{fname_s}</b>",
        "",
        f"{bar}  <b>{pct:.1f}%</b>  ·  <i>{eng_s}</i>",
        "",
        SEP,
        f"{spd_icon} <b>Speed</b>  <i>{spd_s}</i>   ·  ",
        f"⏳ <b>ETA</b>  <i>{eta_s}</i>   ·  ",
        f"🕰 <b>Elapsed</b>  <i>{el_s}</i>",
        DOT,
        f"✅ <b>Done</b>  <i>{done_s}</i> · 📦 <b>Total</b>  <i>{tot_s}</i>",
    ]

    if seeds:
        lines.append(f"🌱 <b>Seeds</b>  <i>{seeds}</i>")

    if cpu or ram_used or disk_free:
        ram_s  = human_size(ram_used)
        disk_s = human_size(disk_free)
        lines += [
            SEP,
            f"🖥 <i>{cpu:.0f}%</i>  ·  💾 <i>{ram_s}</i>  ·  💿 <i>{disk_s} free</i>",
        ]

    lines += [SEP, _PANEL_FOOTER]
    return "\n".join(lines)


def progress_panel(
    *,
    mode:       str   = "dl",
    fname:      str   = "",
    done:       int   = 0,
    total:      int   = 0,
    speed:      float = 0.0,
    eta:        int   = 0,
    elapsed:    float = 0.0,
    engine:     str   = "",
    state:      str   = "",
    link_label: str   = "Link 01",
    cpu:        float = 0.0,
    ram_used:   int   = 0,
    disk_free:  int   = 0,
    seeds:      int   = 0,
    style:      str   = "B",     # "B" (cards) | "C" (minimal) | legacy = original
) -> str:
    """
    Build a Telegram-formatted progress message.
    Dispatches to the appropriate style renderer based on `style`:
      "B" — cards modulaires (default)
      "C" — minimaliste
      anything else — legacy original design

    Original design (legacy):

        📥 DOWNLOADING FROM » 🔗Link 01
        🏷 Name » filename.mkv
        [████████░░░░░░░░░░] 44.3%
        ──────────────────────
        🔥  Speed      42.51 MiB/s
        ...
    """
    _kw = dict(
        mode=mode, fname=fname, done=done, total=total,
        speed=speed, eta=eta, elapsed=elapsed, engine=engine,
        state=state, link_label=link_label, cpu=cpu,
        ram_used=ram_used, disk_free=disk_free, seeds=seeds,
    )
    if style == "B":
        return _progress_panel_b(**_kw)
    if style == "C":
        return _progress_panel_c(**_kw)

    # ── Legacy / original design ──────────────────────────────
    pct     = min((done / total * 100) if total else 0.0, 100.0)
    bar_w   = 18
    filled  = round(pct / 100 * bar_w)
    bar     = "█" * filled + "░" * (bar_w - filled)

    spd_s   = (human_size(speed) + "/s") if speed else "—"
    eta_s   = human_dur(eta) if eta > 0 else "—"
    el_s    = human_dur(elapsed) if elapsed else "0s"
    done_s  = human_size(done)
    total_s = human_size(total) if total else "—"

    m_icon, m_hdr, m_link_icon = _MODE_META.get(mode, ("📦", "PROCESSING", "🔧"))
    eng_s = engine_display(engine)
    spd_icon = speed_emoji(speed)

    fname_s = (fname[:48] + "…") if len(fname) > 48 else fname

    SEP = "──────────────────────"

    lines: list[str] = [
        f"<code>{m_icon} {m_hdr}</code>",
        SEP,
        f"<code>🏷  Name »  {fname_s}</code>",
        "",
        f"<code>[{bar}]  {pct:.1f}%</code>",
        "",
        SEP,
        f"<code>{spd_icon}  Speed     {spd_s}</code>",
        f"<code>⚙️  Engine    {eng_s}</code>",
        f"<code>⏳  ETA       {eta_s}</code>",
        f"<code>🕰  Elapsed   {el_s}</code>",
        f"<code>✅  Done      {done_s}</code>",
        f"<code>📦  Total     {total_s}</code>",
    ]

    if seeds:
        lines.append(f"<code>🌱  Seeds     {seeds}</code>")

    # System stats block — only if we have data
    if cpu or ram_used or disk_free:
        cpu_bar_w   = 10
        cpu_filled  = round(cpu / 100 * cpu_bar_w)
        cpu_bar_str = "█" * cpu_filled + "░" * (cpu_bar_w - cpu_filled)
        ram_s       = human_size(ram_used)
        disk_s      = human_size(disk_free)
        lines += [
            "",
            SEP,
            f"<code>🖥  CPU       [{cpu_bar_str}] {cpu:.0f}%</code>",
            f"<code>💾  RAM       {ram_s}</code>",
            f"<code>💿  Disk Free  {disk_s}</code>",
        ]

    lines += [
        "",
        SEP,
        _PANEL_FOOTER,
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Telegram safe_edit
# ─────────────────────────────────────────────────────────────

_TG_MAX = 4096

_EDIT_SUPPRESSED = frozenset({
    "MESSAGE_NOT_MODIFIED",
    "message was not modified",
    "MESSAGE_ID_INVALID",
    "message to edit not found",
    "Bad Request: message is not modified",
    "MESSAGE_TOO_LONG",
})


async def safe_edit(msg, text: str, **kwargs) -> None:
    """
    Non-blocking fire-and-forget edit.
    NEVER sleeps — skips the edit on FloodWait instead of blocking the caller.
    Use PanelUpdater for high-frequency progress updates.
    """
    if len(text) > _TG_MAX:
        text = text[:_TG_MAX - 64] + "\n\n<i>⚠️ Truncated</i>"
    try:
        await msg.edit(text, **kwargs)
    except Exception as e:
        err = str(e)
        # These are all expected / harmless — swallow silently
        if any(x in err for x in _EDIT_SUPPRESSED):
            return
        if "FLOOD_WAIT" in err:
            # Don't sleep — just drop this edit. PanelUpdater handles backoff.
            return
        if "peer_id_invalid" in err.lower() or "Chat not found" in err:
            return
        # Unexpected — log once but never raise (never crash a download for a UI edit)
        log.debug("safe_edit unexpected error: %s", err[:120])


# ─────────────────────────────────────────────────────────────
# PanelUpdater — non-blocking progress panel
# ─────────────────────────────────────────────────────────────

class PanelUpdater:
    """
    Completely decouples Telegram message edits from progress callbacks.

    HOW IT WORKS
    ────────────
    1. Progress callbacks call updater.tick(done=x, total=y, speed=s, eta=e)
       — this is a plain dict update, O(1), never awaited, never blocks.
    2. A background asyncio.Task wakes up every `interval` seconds,
       calls build_fn(state) to render the panel, and edits the message.
    3. FloodWait → track the wait, apply adaptive interval expansion, skip
       edits silently until the wait expires. The base interval is restored
       after 10 consecutive successful edits.
    4. MESSAGE_NOT_MODIFIED → content unchanged, skip silently.
    5. Message deleted / invalid → stop the task gracefully.

    ADAPTIVE PACING (user-requested 1s updates)
    ────────────────────────────────────────────
    Base interval defaults to 1.0 s for near-real-time progress feedback.
    On FloodWait, effective sleep is doubled (up to 30 s) until 10 successful
    edits in a row restore the base. This keeps the panel snappy on healthy
    connections and well-behaved when Telegram throttles us.

    USAGE
    ─────
        async with PanelUpdater(msg, build_fn) as pu:
            async def on_progress(current, total):
                elapsed = time.time() - start
                speed = current / elapsed if elapsed else 0.0
                eta = int((total - current) / speed) if speed and total > current else 0
                pu.tick(done=current, total=total, speed=speed, eta=eta)

            await client.send_video(..., progress=on_progress)
        # __aexit__ cancels the background task and does one final edit.
    """

    _MAX_EFFECTIVE_INTERVAL = 30.0
    _RESTORE_AFTER_OK       = 10

    def __init__(
        self,
        msg,
        build_fn,        # callable(state: dict) -> str
        interval: float = 1.0,
        start_state: Optional[dict] = None,
    ):
        self._msg              = msg
        self._build            = build_fn
        self._base_interval    = max(0.5, float(interval))
        self._effective_interval = self._base_interval
        self._state:    dict = dict(start_state or {})
        self._task:     Optional[asyncio.Task] = None
        self._last_text: str = ""
        self._flood_until: float = 0.0
        self._stopped:  bool = False
        self._ok_streak: int = 0
        self._floods:   int = 0

    # Back-compat: some callers read ._interval
    @property
    def _interval(self) -> float:
        return self._effective_interval

    # ── Non-blocking state update (called from progress callbacks) ──

    def tick(self, **kw) -> None:
        """Update shared state. Non-blocking — safe to call from any callback."""
        self._state.update(kw)

    # ── Lifecycle ────────────────────────────────────────────────────

    async def start(self) -> "PanelUpdater":
        self._task = asyncio.get_running_loop().create_task(self._loop())
        return self

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        # Final edit with latest state
        await self._try_edit()

    async def __aenter__(self) -> "PanelUpdater":
        return await self.start()

    async def __aexit__(self, *_) -> None:
        await self.stop()

    # ── Background loop ──────────────────────────────────────────────

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._effective_interval)
            except asyncio.CancelledError:
                return
            await self._try_edit()

    def _register_ok(self) -> None:
        self._ok_streak += 1
        if self._ok_streak >= self._RESTORE_AFTER_OK and \
           self._effective_interval > self._base_interval:
            self._effective_interval = self._base_interval
            self._ok_streak = 0

    def _register_flood(self, wait_s: float) -> None:
        self._floods += 1
        self._ok_streak = 0
        # Adaptive: double the effective interval each time we flood, cap at 30s
        self._effective_interval = min(
            max(self._effective_interval * 2.0, self._base_interval * 2.0),
            self._MAX_EFFECTIVE_INTERVAL,
        )
        self._flood_until = time.time() + max(1.0, wait_s)

    async def _try_edit(self) -> None:
        if self._msg is None:
            return
        if time.time() < self._flood_until:
            return  # still in backoff window
        try:
            text = self._build(self._state)
            if not text:
                return
            if len(text) > _TG_MAX:
                text = text[:_TG_MAX - 64] + "\n\n<i>⚠️ Truncated</i>"
            if text == self._last_text:
                self._register_ok()  # no-op still counts as "healthy"
                return   # nothing changed — no-op, saves an API round-trip
            from pyrogram import enums as _pe
            await self._msg.edit(text, parse_mode=_pe.ParseMode.HTML,
                                 disable_web_page_preview=True)
            self._last_text = text
            self._register_ok()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            err = str(exc)
            if "FLOOD_WAIT" in err:
                import re as _re_fw
                m = _re_fw.search(r"FLOOD_WAIT_(\d+)", err)
                wait = int(m.group(1)) if m else 30
                self._register_flood(wait)
                log.debug("PanelUpdater flood-wait %ds (effective interval → %.1fs)",
                          wait, self._effective_interval)
            elif any(x in err for x in (
                "MESSAGE_NOT_MODIFIED", "message was not modified",
                "Bad Request: message is not modified",
            )):
                self._last_text = ""  # force re-render next tick
                self._register_ok()
            elif any(x in err for x in (
                "MESSAGE_ID_INVALID", "message to edit not found",
                "peer_id_invalid", "Chat not found",
            )):
                # Message was deleted — stop updating
                if self._task and not self._task.done():
                    self._task.cancel()
            else:
                log.debug("PanelUpdater edit error: %s", err[:120])


# ─────────────────────────────────────────────────────────────
# Filesystem helpers
# ─────────────────────────────────────────────────────────────

def make_tmp(base: str, user_id: int) -> str:
    path = os.path.join(base, str(user_id), str(int(time.time() * 1000)))
    os.makedirs(path, exist_ok=True)
    return path


def cleanup(path: str) -> None:
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


def safe_fname(name: str) -> str:
    keep = " ._-()"
    return "".join(c for c in name if c.isalnum() or c in keep).strip() or "file"


_VIDEO_EXTS_SET = frozenset({
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv",
    ".ts", ".m2ts", ".wmv", ".m4v", ".rmvb", ".mpg", ".mpeg",
})


def all_video_files(directory: str, min_bytes: int = 5 * 1024 * 1024) -> list[str]:
    """
    Return every video file in `directory` (recursively) that is at least
    `min_bytes` large, sorted naturally by filename.
    Files ending in .aria2 are skipped.
    If no video files are found, returns all non-.aria2 files >= min_bytes
    (so plain-document batches still work).
    """
    videos: list[tuple[str, str]] = []   # (lower_name, path)
    others: list[tuple[str, str]] = []

    try:
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fn in files:
                if fn.endswith(".aria2"):
                    continue
                fp = os.path.join(root, fn)
                try:
                    if os.path.getsize(fp) < min_bytes:
                        continue
                except OSError:
                    continue
                ext = os.path.splitext(fn)[1].lower()
                if ext in _VIDEO_EXTS_SET:
                    videos.append((fn.lower(), fp))
                else:
                    others.append((fn.lower(), fp))
    except Exception:
        pass

    results = videos or others
    results.sort(key=lambda x: x[0])
    return [fp for _, fp in results]


def largest_file(directory: str) -> Optional[str]:
    best: Optional[str] = None
    best_sz = -1
    try:
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fn in files:
                if fn.endswith(".aria2"):
                    continue
                fp = os.path.join(root, fn)
                try:
                    sz = os.path.getsize(fp)
                    if sz > best_sz:
                        best_sz, best = sz, fp
                except OSError:
                    pass
    except Exception:
        pass
    return best


# ─────────────────────────────────────────────────────────────
# Smart filename cleaner
# ─────────────────────────────────────────────────────────────

_KEEP_TAGS: frozenset[str] = frozenset({
    "VOSTFR", "VOSTA", "VF", "VO", "VOFR", "MULTI", "FRENCH", "ENGLISH",
    "ENG", "FR", "JAP", "JPN", "KOR", "CHI", "SPA", "ITA", "GER", "POR",
    "DUAL", "TRUEFRENCH", "SUBFRENCH", "VOSTEN", "VOSTJP",
})

_NOISE_RE = _re.compile(
    r"^("
    r"\d{3,4}p|4[Kk]|UHD|SDR|HDR10?\+?|DV|DoVi|10[Bb]it|8[Bb]it"
    r"|WEB[\-\.]?DL|WEB|BluRay|Blu[\-\.]?Ray|BDRip|BRRip|HDRip"
    r"|HDTV|PDTV|DVDRip|DVDScr|CAMRip|TELESYNC|TS"
    r"|AMZN|NF|DSNP|HMAX|ATVP|CR|ADN|DISNEY\+?"
    r"|x26[45]|HEVC|AVC|H\.?26[45]|XviD|DivX|VP9|AV1"
    r"|AAC|AC3|DTS(?:[\-\.]?HD)?|FLAC|MP3|TrueHD|Atmos|EAC3|DD5|DD\+"
    r"|REPACK|PROPER|EXTENDED|UNRATED|THEATRICAL|REMASTERED|REMUX"
    r")\Z",
    _re.IGNORECASE,
)


def smart_clean_filename(fname: str) -> str:
    """Strip release tags from a filename, preserving show name and episode id."""
    name, ext = os.path.splitext(fname)

    # Strip leading group tag e.g. "[SubsPlease]"
    name = _re.sub(r"^\[[^\]]{1,40}\]\s*", "", name).strip()

    # Replace dots/underscores used as word separators
    if " " not in name and ("." in name or "_" in name):
        name = name.replace(".", " ").replace("_", " ")

    tokens = name.split()
    keep: list[str] = []

    for i, tok in enumerate(tokens):
        bare = _re.sub(r"[^\w\+]", "", tok).upper()
        if bare in _KEEP_TAGS:
            keep.append(tok)
            continue
        # Only block on noise after 3rd token to protect show names
        if i >= 2 and _NOISE_RE.match(bare):
            break
        if tok.startswith("-") and len(tok) > 1 and i > 0:
            break
        if tok.startswith("(") and tok.endswith(")") and i > 0:
            break
        if tok.startswith("[") and tok.endswith("]") and i > 0:
            break
        keep.append(tok)

    result = " ".join(keep).strip(" -_.,")
    return (result + ext) if result else fname


# ─────────────────────────────────────────────────────────────
# System stats
# ─────────────────────────────────────────────────────────────

async def system_stats() -> dict:
    out: dict = {
        "cpu": 0.0, "ram_pct": 0.0, "ram_used": 0,
        "disk_free": 0, "dl_speed": 0.0, "ul_speed": 0.0,
    }
    try:
        import psutil
        out["cpu"]       = psutil.cpu_percent(interval=None)
        vm               = psutil.virtual_memory()
        out["ram_pct"]   = vm.percent
        out["ram_used"]  = vm.used
        out["disk_free"] = psutil.disk_usage("/").free
        n1 = psutil.net_io_counters()
        await asyncio.sleep(0.25)
        n2 = psutil.net_io_counters()
        out["dl_speed"]  = (n2.bytes_recv - n1.bytes_recv) / 0.25
        out["ul_speed"]  = (n2.bytes_sent - n1.bytes_sent) / 0.25
    except Exception:
        try:
            out["disk_free"] = shutil.disk_usage("/").free
        except Exception:
            pass
    return out
