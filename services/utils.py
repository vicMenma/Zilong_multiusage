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


def _progress_panel_1(
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
    Style 1 — Clean card (Image 1).

        Downloading              Aria2c · 0.0%
        [BuriBuri] Crayon Shin-chan — 0069 [720p]…
        ████████████████████████  0.0%

        Speed          —
        ETA            —
        Done / Total   0.00 B / 295 MiB
        Elapsed        7s

        CPU 13%   RAM 803 MiB   Disk 86.2 GiB

        💗 When I'm Doin This, Do Something Else!
    """
    pct    = min((done / total * 100) if total else 0.0, 100.0)
    bar_w  = 24
    filled = round(pct / 100 * bar_w)
    bar    = "█" * filled + " " * (bar_w - filled)

    spd_s  = (human_size(speed) + "/s") if speed else "—"
    eta_s  = human_dur(eta) if eta > 0 else "—"
    el_s   = human_dur(elapsed) if elapsed else "0s"
    done_s = human_size(done)
    tot_s  = human_size(total) if total else "—"
    eng_s  = engine_display(engine)

    m_icon, m_hdr, _ = _MODE_META.get(mode, ("📦", "Processing", "🔧"))
    mode_lbl = m_hdr.capitalize()
    fname_s  = (fname[:52] + "…") if len(fname) > 52 else fname

    SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    lines: list[str] = [
        f"<b>{mode_lbl}</b>                    <code>{eng_s} · {pct:.1f}%</code>",
        f"<code>{fname_s}</code>",
        "",
        f"<code>{bar}  {pct:.1f}%</code>",
        "",
        SEP,
        f"Speed          <code>{spd_s}</code>",
        f"ETA            <code>{eta_s}</code>",
        f"Done / Total   <code>{done_s} / {tot_s}</code>",
        f"Elapsed        <code>{el_s}</code>",
    ]

    if seeds:
        lines.append(f"Seeds          <code>{seeds}</code>")

    if cpu or ram_used or disk_free:
        ram_s  = human_size(ram_used)
        disk_s = human_size(disk_free)
        lines += [
            "",
            SEP,
            f"CPU <code>{cpu:.0f}%</code>    RAM <code>{ram_s}</code>    Disk <code>{disk_s}</code>",
        ]

    lines += ["", _PANEL_FOOTER]
    return "\n".join(lines)


def _progress_panel_2(
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
    Style 2 — Grid cards (Image 2).

        [C] [BuriBuri] Crayon Shin-chan — 0069…   Aria2c

        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │ Speed   │  │ ETA     │  │ Done    │
        │   —     │  │   —     │  │ 0.00 B  │
        └─────────┘  └─────────┘  └─────────┘
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │ Total   │  │ Elapsed │  │Progress │
        │ 295 MiB │  │  7s     │  │  0.0%   │
        └─────────┘  └─────────┘  └─────────┘
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │ CPU     │  │ RAM     │  │ Disk    │
        │  13%    │  │ 803 MiB │  │86.2 GiB │
        └─────────┘  └─────────┘  └─────────┘

        💗 When I'm Doin This, Do Something Else!
    """
    pct    = min((done / total * 100) if total else 0.0, 100.0)

    spd_s  = (human_size(speed) + "/s") if speed else "—"
    eta_s  = human_dur(eta) if eta > 0 else "—"
    el_s   = human_dur(elapsed) if elapsed else "0s"
    done_s = human_size(done)
    tot_s  = human_size(total) if total else "—"
    eng_s  = engine_display(engine)

    _, m_hdr, _ = _MODE_META.get(mode, ("📦", "Processing", "🔧"))
    fname_s = (fname[:38] + "…") if len(fname) > 38 else fname

    SEP = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    def card(label: str, value: str) -> str:
        return f"<code>[{label:<8}  {value:>10}]</code>"

    lines: list[str] = [
        f"📦 <b>{fname_s}</b>   <code>{eng_s}</code>",
        SEP,
        card("Speed",   spd_s),
        card("ETA",     eta_s),
        card("Done",    done_s),
        SEP,
        card("Total",   tot_s),
        card("Elapsed", el_s),
        card("Progress", f"{pct:.1f}%"),
    ]

    if seeds:
        lines += [SEP, card("Seeds", str(seeds))]

    if cpu or ram_used or disk_free:
        ram_s  = human_size(ram_used)
        disk_s = human_size(disk_free)
        lines += [
            SEP,
            card("CPU",  f"{cpu:.0f}%"),
            card("RAM",  ram_s),
            card("Disk", disk_s),
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
    style:      str   = "1",     # "1" (clean card) | "2" (grid cards)
) -> str:
    """
    Build a Telegram-formatted progress message.
    Dispatches to the appropriate style renderer based on `style`:
      "1" — Clean card with progress bar (Image 1, default)
      "2" — Grid cards layout (Image 2)
    """
    _kw = dict(
        mode=mode, fname=fname, done=done, total=total,
        speed=speed, eta=eta, elapsed=elapsed, engine=engine,
        state=state, link_label=link_label, cpu=cpu,
        ram_used=ram_used, disk_free=disk_free, seeds=seeds,
    )
    if style == "2":
        return _progress_panel_2(**_kw)
    return _progress_panel_1(**_kw)


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
    import re as _re2
    if len(text) > _TG_MAX:
        text = text[:_TG_MAX - 64] + "\n\n<i>⚠️ Truncated</i>"
    for _attempt in range(3):
        try:
            await msg.edit(text, **kwargs)
            return
        except Exception as e:
            err = str(e)
            if any(x in err for x in _EDIT_SUPPRESSED):
                return
            if "FLOOD_WAIT" in err:
                m = _re2.search(r"FLOOD_WAIT_(\d+)", err)
                wait = min(int(m.group(1)) if m else 30, 60)  # cap at 60s
                log.warning("safe_edit FLOOD_WAIT %ds — backing off", wait)
                await asyncio.sleep(wait)
                continue  # retry after sleep
            if "peer_id_invalid" in err.lower():
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
