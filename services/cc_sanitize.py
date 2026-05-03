"""
services/cc_sanitize.py  —  v3

Produces clean, human-readable filenames for CloudConvert output.

OUTPUT FORMAT:  {Title} {S0204} {TAG}.mp4

EXAMPLES
─────────
  [Erai-raws] Tsue to Tsurugi no Wistoria 2nd Season - 04 [720p][MultiSub][ABCD].mkv
  → Tsue to Tsurugi no Wistoria S0204 VOSTFR.mp4

  [SubsPlease] One Piece - 1118 (1080p) [A2B3C4D5].mkv
  → One Piece 1118 VOSTFR.mp4

  [HorribleSubs] Attack on Titan S04E28 [1080p].mkv
  → Attack on Titan S0428 VOSTFR.mp4

  [Erai-raws] Tensei Shitara Slime 3rd Season - 12 [720p].mkv
  → Tensei Shitara Slime S0312 VOSTFR.mp4

  My.Anime.Show.S02E04.FRENCH.1080p.BluRay.x264.mkv
  → My Anime Show S0204 VOSTFR.mp4

  [ToonsHub] Barbaroi S01E01 1080p WEB-DL AAC2.0 H264.mkv
  → Barbaroi S0101 VOSTFR.mp4
"""
from __future__ import annotations

import os
import re

# ── Ordinal → int ─────────────────────────────────────────────────────────────

_ORDINALS = {
    "1st": 1, "first":  1,
    "2nd": 2, "second": 2,
    "3rd": 3, "third":  3,
    "4th": 4, "fourth": 4,
    "5th": 5, "fifth":  5,
    "6th": 6, "sixth":  6,
    "7th": 7, "seventh":7,
    "8th": 8, "eighth": 8,
    "9th": 9, "ninth":  9,
}

def _ord2int(s: str) -> int | None:
    return _ORDINALS.get(s.lower())


# ── Quality / codec noise words to strip from title ───────────────────────────
# These appear as standalone words after removing brackets.
_NOISE = re.compile(
    r'\b('
    r'\d{3,4}p'                              # 720p 1080p 2160p
    r'|(?:WEB[-\s]?DL|WEBRip|BluRay|Blu[-\s]?Ray|HDTV|AMZN|DSNP|NF|CR)'
    r'|(?:AVC|HEVC|x264|x265|H\.?264|H\.?265|XviD)'
    r'|(?:AAC2?\.?\d*|AC3|DTS|FLAC|MP3|E-AC-3|EAC3|Opus)'
    r'|(?:MultiSub|Multi|Dual|French|FRENCH|VOSTFR|VOSTA|MULTi)'
    r'|(?:10bit|8bit|HDR|SDR|DoVi|Remux|REPACK|PROPER)'
    r')\b',
    re.IGNORECASE,
)

# ── Regex patterns ────────────────────────────────────────────────────────────

_RE_BRACKETS  = re.compile(r'\[[^\]]*\]|\{[^}]*\}|\([^)]*\)')
_RE_SEP_DOTS  = re.compile(r'\.(?=[a-zA-Z0-9])')
_RE_MULTISPC  = re.compile(r'[ \t]{2,}')
_RE_MULTI_DASH= re.compile(r'\s*[-–]+\s*')
_RE_EDGES     = re.compile(r'^[\s\-_]+|[\s\-_]+$')
_RE_UNSAFE_FS = re.compile(r'[\\/:*?"<>|]')
_RE_UNSAFE_URL= re.compile(r'[^\w\-.]')

# ── Season/episode patterns (tried in order) ──────────────────────────────────

_EP_PATTERNS = [
    # S02E04 / s2e4
    re.compile(
        r'[Ss](?P<season>\d{1,2})[Ee](?P<episode>\d{1,3})'
    ),
    # 2nd Season - 04
    re.compile(
        r'(?P<ord>1st|2nd|3rd|[4-9]th|first|second|third|fourth|fifth|'
        r'sixth|seventh|eighth|ninth)\s+Season\s*[-–]\s*(?P<episode>\d{1,3})',
        re.IGNORECASE
    ),
    # Season 2 - 04
    re.compile(
        r'Season\s+(?P<season>\d{1,2})\s*(?:[-–]|Episode)?\s*(?P<episode>\d{1,3})',
        re.IGNORECASE
    ),
    # Bare " - 04" or " - 118"
    re.compile(r'\s[-–]\s*(?P<episode>\d{1,3})(?=\s|$)'),
]


def _extract_ep(name: str) -> tuple:
    """Return (start, end, compact_code) or (None, None, '')."""
    for pat in _EP_PATTERNS:
        m = pat.search(name)
        if not m:
            continue
        g = m.groupdict()
        if "ord" in g and g["ord"]:
            season = _ord2int(g["ord"]) or 1
        elif "season" in g and g["season"]:
            season = int(g["season"])
        else:
            season = None
        episode = int(g["episode"])
        # Compact code: S0204 or zero-padded episode only
        code = f"S{season:02d}{episode:02d}" if season else f"{episode:02d}"
        return m.start(), m.end(), code
    return None, None, ""


# ── Core cleaner ─────────────────────────────────────────────────────────────

def _clean_title(name: str) -> tuple[str, str]:
    """Return (clean_title_with_spaces, ep_code)."""
    base, _ = os.path.splitext(name)

    # 1. Remove all bracket groups
    base = _RE_BRACKETS.sub(" ", base)

    # 2. Strip quality/codec noise words BEFORE dot conversion
    #    so "AAC2.0" is still one token and gets fully removed
    base = _NOISE.sub(" ", base)

    # 3. Dot-separators → spaces  (My.Anime.Show → My Anime Show)
    base = _RE_SEP_DOTS.sub(" ", base)

    # 4. Remove unsafe filesystem chars
    base = _RE_UNSAFE_FS.sub(" ", base)

    # 5. Extract season/episode
    start, end, ep_code = _extract_ep(base)
    if start is not None:
        base = (base[:start] + " " + base[end:])

    # 6. Collapse dashes that are now standalone separators
    base = _RE_MULTI_DASH.sub(" ", base)

    # 7. Remove orphan dots left after noise stripping in dot-separated names
    base = re.sub(r'(?<!\w)\.\s*|\s*\.(?!\w)', ' ', base)

    # 8. Collapse spaces, strip edges
    base = _RE_MULTISPC.sub(" ", base)
    base = _RE_EDGES.sub("", base)

    return base or "Video", ep_code


# ── Public API ────────────────────────────────────────────────────────────────

def sanitize_filename(name: str) -> str:
    """
    Clean filename for local storage. Keeps spaces.

    >>> sanitize_filename("[Erai-raws] Wistoria 2nd Season - 04 [720p][ABCD].mkv")
    'Wistoria S0204.mkv'
    """
    if not name:
        return "video.mkv"
    _, ext = os.path.splitext(name)
    title, ep_code = _clean_title(name)
    base = f"{title} {ep_code}".strip() if ep_code else title
    return base + ext.lower()


# Backward-compat alias used by cloudconvert_api.py
sanitize_for_cc = sanitize_filename


def build_cc_output_name(input_name: str, tag: str = "VOSTFR") -> str:
    """
    Build clean CC output name: 'Title S0204 VOSTFR.mp4'

    >>> build_cc_output_name("[Erai-raws] Wistoria 2nd Season - 04 [720p][ABCD].mkv")
    'Wistoria S0204 VOSTFR.mp4'
    """
    title, ep_code = _clean_title(input_name)
    parts = [title]
    if ep_code:
        parts.append(ep_code)
    parts.append(tag)
    return " ".join(parts) + ".mp4"


def sanitize_url_filename(name: str) -> str:
    """Strict ASCII, underscores, no spaces — for embedding in URLs."""
    base, ext = os.path.splitext(sanitize_filename(name))
    base = base.replace(" ", "_")
    base = _RE_UNSAFE_URL.sub("", base)
    base = re.sub(r'_+', '_', base).strip("_") or "video"
    return base + ext.lower()
