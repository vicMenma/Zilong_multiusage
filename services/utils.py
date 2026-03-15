"""
services/utils.py
Pure helper functions — no Telegram types, no side effects.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time
from typing import Optional

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Formatters
# ─────────────────────────────────────────────────────────────

def human_size(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.2f} {unit}"
        n /= 1024.0
    return f"{n:.2f} PiB"


def human_dur(secs: float) -> str:
    s = int(secs)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d:   return f"{d}d {h}h {m}m {s}s"
    if h:   return f"{h}h {m}m {s}s"
    if m:   return f"{m}m {s}s"
    return f"{s}s"


def fmt_hms(secs: float) -> str:
    """Format as H:MM:SS or M:SS."""
    s = int(secs)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def pct_bar(pct: float, length: int = 14) -> str:
    filled = int(min(pct, 100) / 100 * length)
    return "█" * filled + "░" * (length - filled)


def speed_emoji(bps: float) -> str:
    mib = bps / (1024 * 1024)
    if mib >= 50: return "🚀"
    if mib >= 10: return "⚡"
    if mib >= 1:  return "🔥"
    if mib >= .1: return "🏃"
    return "🐢"


# ─────────────────────────────────────────────────────────────
# Progress panel
# ─────────────────────────────────────────────────────────────

def progress_panel(
    *,
    mode: str = "dl",
    fname: str = "",
    done: int = 0,
    total: int = 0,
    speed: float = 0,
    eta: int = 0,
    elapsed: float = 0,
    engine: str = "",
    state: str = "",
    seeds: int = 0,
) -> str:
    pct   = min((done / total * 100) if total else 0, 100)
    bar   = pct_bar(pct, 14)
    spd_s = human_size(speed) + "/s" if speed else "—"
    eta_s = human_dur(eta) if eta > 0 else "—"
    el_s  = human_dur(elapsed) if elapsed else "0s"

    header_map = {
        "dl":     "📥 <b>DOWNLOADING</b>",
        "ul":     "📤 <b>UPLOADING</b>",
        "magnet": "🧲 <b>TORRENT/MAGNET</b>",
        "proc":   "⚙️ <b>PROCESSING</b>",
    }
    header = header_map.get(mode, "⚙️ <b>PROCESSING</b>")
    if state:
        header += f"  <i>— {state}</i>"

    engine_map = {
        "telegram": "📲 Telegram",
        "ytdlp":    "▶️ yt-dlp",
        "aria2":    "🧨 Aria2",
        "direct":   "🔗 Direct",
        "gdrive":   "☁️ GDrive",
        "ffmpeg":   "⚙️ FFmpeg",
    }
    eng_s = engine_map.get(engine, engine)

    lines = [header, ""]
    if fname:
        short = fname[:52] + "…" if len(fname) > 52 else fname
        lines.append(f"📄 <code>{short}</code>")

    lines += [
        "",
        f"<code>[{bar}]</code>  <b>{pct:.1f}%</b>",
        "──────────────────────",
        f"{speed_emoji(speed)}  <b>Speed</b>  <code>{spd_s}</code>",
    ]
    if eng_s:
        lines.append(f"⚙️  <b>Engine</b>  <code>{eng_s}</code>")
    lines += [
        f"⏳  <b>ETA</b>     <code>{eta_s}</code>",
        f"🕰  <b>Elapsed</b> <code>{el_s}</code>",
        f"✅  <b>Done</b>    <code>{human_size(done)}</code>"
        + (f" / <code>{human_size(total)}</code>" if total else ""),
    ]
    if seeds:
        lines.append(f"🌱  <b>Seeders</b> <code>{seeds}</code>")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# Telegram helpers
# ─────────────────────────────────────────────────────────────

async def safe_edit(msg, text: str, **kwargs) -> None:
    """Edit message, swallow idempotent / not-found errors."""
    try:
        await msg.edit(text, **kwargs)
    except Exception as e:
        err = str(e)
        if any(x in err for x in (
            "MESSAGE_NOT_MODIFIED", "message was not modified",
            "MESSAGE_ID_INVALID", "message to edit not found",
        )):
            return
        raise


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


def largest_file(directory: str) -> Optional[str]:
    best: Optional[str] = None
    best_sz = -1
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fname in files:
            if fname.endswith(".aria2"):
                continue
            fp = os.path.join(root, fname)
            try:
                sz = os.path.getsize(fp)
                if sz > best_sz:
                    best_sz, best = sz, fp
            except OSError:
                pass
    return best


# ─────────────────────────────────────────────────────────────
# System stats
# ─────────────────────────────────────────────────────────────

async def system_stats() -> dict:
    out = {"cpu": 0.0, "ram_pct": 0.0, "disk_free": 0, "dl_speed": 0.0, "ul_speed": 0.0}
    try:
        import psutil
        out["cpu"] = psutil.cpu_percent(interval=None)
        vm = psutil.virtual_memory()
        out["ram_pct"] = vm.percent
        out["disk_free"] = psutil.disk_usage("/").free
        n1 = psutil.net_io_counters()
        await asyncio.sleep(0.25)
        n2 = psutil.net_io_counters()
        out["dl_speed"] = (n2.bytes_recv - n1.bytes_recv) / 0.25
        out["ul_speed"] = (n2.bytes_sent - n1.bytes_sent) / 0.25
    except Exception:
        try:
            out["disk_free"] = shutil.disk_usage("/").free
        except Exception:
            pass
    return out


def idle_panel(stats: dict) -> str:
    def ring(p):
        return "🟢" if p < 40 else ("🟡" if p < 70 else "🔴")

    cpu = stats.get("cpu", 0)
    rp  = stats.get("ram_pct", 0)
    df  = stats.get("disk_free", 0)
    dl  = stats.get("dl_speed", 0)
    ul  = stats.get("ul_speed", 0)
    return "\n".join([
        "⚡ <b>ZILONG BOT</b>  <i>— Idle</i>",
        "──────────────────────",
        f"🖥  CPU  {ring(cpu)}<code>[{pct_bar(cpu, 10)}]</code> <b>{cpu:.0f}%</b>",
        f"💾  RAM  {ring(rp)}<code>[{pct_bar(rp, 10)}]</code> <b>{rp:.0f}%</b>",
        f"💿  Disk  <code>{human_size(df)} free</code>",
        f"🌐  ⬇ <code>{human_size(dl)}/s</code>  ⬆ <code>{human_size(ul)}/s</code>",
    ])
