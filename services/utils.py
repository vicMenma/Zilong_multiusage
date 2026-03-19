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
# Shared language lookup tables (single source of truth)
# ─────────────────────────────────────────────────────────────

LANG_FLAG: dict[str, str] = {
    "eng":"🇬🇧","en":"🇬🇧","jpn":"🇯🇵","ja":"🇯🇵",
    "fra":"🇫🇷","fre":"🇫🇷","fr":"🇫🇷","deu":"🇩🇪","ger":"🇩🇪","de":"🇩🇪",
    "spa":"🇪🇸","es":"🇪🇸","por":"🇧🇷","pt":"🇧🇷","ita":"🇮🇹","it":"🇮🇹",
    "kor":"🇰🇷","ko":"🇰🇷","chi":"🇨🇳","zho":"🇨🇳","zh":"🇨🇳",
    "rus":"🇷🇺","ru":"🇷🇺","ara":"🇸🇦","ar":"🇸🇦","hin":"🇮🇳","hi":"🇮🇳",
    "tha":"🇹🇭","th":"🇹🇭","vie":"🇻🇳","vi":"🇻🇳","ind":"🇮🇩","id":"🇮🇩",
    "msa":"🇲🇾","ms":"🇲🇾","tur":"🇹🇷","tr":"🇹🇷","pol":"🇵🇱","pl":"🇵🇱",
    "nld":"🇳🇱","nl":"🇳🇱","swe":"🇸🇪","sv":"🇸🇪","nor":"🇳🇴","no":"🇳🇴",
    "dan":"🇩🇰","da":"🇩🇰","fin":"🇫🇮","fi":"🇫🇮","heb":"🇮🇱","he":"🇮🇱",
    "ces":"🇨🇿","cze":"🇨🇿","ron":"🇷🇴","rum":"🇷🇴","hun":"🇭🇺","hu":"🇭🇺",
    "bul":"🇧🇬","bg":"🇧🇬","ukr":"🇺🇦","uk":"🇺🇦","und":"🌐",
}

LANG_NAME: dict[str, str] = {
    "eng":"English","en":"English","jpn":"Japanese","ja":"Japanese",
    "fra":"French","fre":"French","fr":"French","deu":"German","ger":"German","de":"German",
    "spa":"Spanish","es":"Spanish","por":"Portuguese","pt":"Portuguese",
    "ita":"Italian","it":"Italian","kor":"Korean","ko":"Korean",
    "chi":"Chinese","zho":"Chinese","zh":"Chinese","rus":"Russian","ru":"Russian",
    "ara":"Arabic","ar":"Arabic","hin":"Hindi","hi":"Hindi","tha":"Thai","th":"Thai",
    "vie":"Vietnamese","vi":"Vietnamese","ind":"Indonesian","id":"Indonesian",
    "msa":"Malay","ms":"Malay","tur":"Turkish","tr":"Turkish","pol":"Polish","pl":"Polish",
    "nld":"Dutch","nl":"Dutch","swe":"Swedish","sv":"Swedish","nor":"Norwegian","no":"Norwegian",
    "dan":"Danish","da":"Danish","fin":"Finnish","fi":"Finnish","heb":"Hebrew","he":"Hebrew",
    "ces":"Czech","cze":"Czech","ron":"Romanian","rum":"Romanian","hun":"Hungarian","hu":"Hungarian",
    "bul":"Bulgarian","bg":"Bulgarian","ukr":"Ukrainian","uk":"Ukrainian","und":"Unknown",
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
        n = 0
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
    if d:   return f"{d}d {h}h {m}m {s}s"
    if h:   return f"{h}h {m}m {s}s"
    if m:   return f"{m}m {s}s"
    return f"{s}s"


def fmt_hms(secs: float) -> str:
    """Format as H:MM:SS or M:SS."""
    s = int(max(0, secs))
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def pct_bar(pct: float, length: int = 14) -> str:
    filled = int(min(max(pct, 0), 100) / 100 * length)
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
        "magnet":   "🧲 Aria2",
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

_TG_MAX = 4096   # Telegram hard message length limit

async def safe_edit(msg, text: str, **kwargs) -> None:
    """Edit message, swallow idempotent / not-found errors.
    Automatically truncates text to Telegram's 4096-char limit.
    """
    # Hard truncate — never let an oversized panel kill the live loop
    if len(text) > _TG_MAX:
        text = text[:_TG_MAX - 64] + "\n\n<i>⚠️ Panel truncated — too many active tasks</i>"
    try:
        await msg.edit(text, **kwargs)
    except Exception as e:
        err = str(e)
        if any(x in err for x in (
            "MESSAGE_NOT_MODIFIED", "message was not modified",
            "MESSAGE_ID_INVALID", "message to edit not found",
            "Bad Request: message is not modified",
            "MESSAGE_TOO_LONG", "message is too long",
        )):
            return
        # Don't re-raise floods or peer not found quietly
        if "FLOOD_WAIT" in err or "peer_id_invalid" in err.lower():
            log.debug("safe_edit suppressed: %s", err[:100])
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


# ─────────────────────────────────────────────────────────────
# Smart filename cleaner
# ─────────────────────────────────────────────────────────────
import re as _re

# Language/audio tags we explicitly KEEP — they are meaningful to the user
_KEEP_TAGS: frozenset[str] = frozenset({
    "VOSTFR", "VOSTA", "VF", "VO", "VOFR", "MULTI", "FRENCH", "ENGLISH",
    "ENG", "FR", "JAP", "JPN", "KOR", "CHI", "SPA", "ITA", "GER", "POR",
    "DUAL", "TRUEFRENCH", "SUBFRENCH", "VOSTEN", "VOSTJP",
})

# Technical noise we strip — matched case-insensitively as whole tokens
_NOISE_RE = _re.compile(
    r"^("
    r"\d{3,4}p"                              # 720p 1080p 480p 2160p
    r"|4[Kk]|UHD|SDR|HDR|HDR10|HDR10\+|DV|DoVi|10[Bb]it|8[Bb]it"
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
    """
    Strip technical release tags from an anime/series filename, keeping
    the show name, episode identifier, and language tag.

    Examples
    --------
    "Oshi no Ko S03E10 VOSTFR 720p WEB x264 AAC -Tsundere-Raws (ADN).mkv"
        → "Oshi no Ko S03E10 VOSTFR.mkv"
    "Jujutsu Kaisen S03E09 1080p BluRay x265.mkv"
        → "Jujutsu Kaisen S03E09.mkv"
    "[SubsPlease] Vinland Saga - 24 (1080p) [ABC123].mkv"
        → "Vinland Saga - 24.mkv"
    """
    name, ext = os.path.splitext(fname)

    # 1. Strip leading group tag in square brackets e.g. "[SubsPlease]"
    name = _re.sub(r"^\[[^\]]{1,40}\]\s*", "", name).strip()

    # Replace dots/underscores used as word separators with spaces
    # (but only if the whole string looks like it uses them as separators)
    if " " not in name and ("." in name or "_" in name):
        name = name.replace(".", " ").replace("_", " ")

    tokens = name.split()
    keep: list[str] = []
    last_lang_idx: int = -1

    for i, tok in enumerate(tokens):
        bare = _re.sub(r"[^\w\+]", "", tok).upper()  # strip punctuation for matching

        if bare in _KEEP_TAGS:
            keep.append(tok)
            last_lang_idx = len(keep) - 1
            continue

        if _NOISE_RE.match(bare):
            # Stop here — everything from this token on is technical noise.
            # If a lang tag was the previous token, we already kept it.
            break

        # Strip trailing release-group tokens: "-GroupName" (hyphen + text, no spaces)
        # A bare "-" token (episode separator like "Vinland Saga - 24") must be kept.
        if tok.startswith("-") and len(tok) > 1 and i > 0:
            break
        if tok.startswith("(") and tok.endswith(")") and i > 0:
            break
        if tok.startswith("[") and tok.endswith("]") and i > 0:
            break

        keep.append(tok)

    result = " ".join(keep).strip(" -_.,")
    if not result:
        return fname          # fallback: return original if we stripped everything
    return result + ext


def largest_file(directory: str) -> Optional[str]:
    best: Optional[str] = None
    best_sz = -1
    try:
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
    except Exception:
        pass
    return best


# ─────────────────────────────────────────────────────────────
# System stats
# ─────────────────────────────────────────────────────────────

async def system_stats() -> dict:
    out = {"cpu": 0.0, "ram_pct": 0.0, "ram_used": 0, "disk_free": 0, "dl_speed": 0.0, "ul_speed": 0.0}
    try:
        import psutil
        out["cpu"] = psutil.cpu_percent(interval=None)
        vm = psutil.virtual_memory()
        out["ram_pct"]  = vm.percent
        out["ram_used"] = vm.used
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
