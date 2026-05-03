"""
services/cc_sanitize.py  —  v2

Filename sanitisation for CloudConvert job submission.

PROBLEM FIXED (v2)
───────────────────
Anime/fansub releases from Nyaa / Erai-raws have filenames like:
  [Erai-raws] Tsue to Tsurugi no Wistoria 2nd Season - 04
  [720p CR WEB-DL AVC AAC][MultiSub][D0BBA05B].mkv

CloudConvert's import/url task rejects any filename with [], {}, ()
or other non-URL-safe characters, returning:
  "422: Task import-video: The url format is invalid."

RULES (v2)
──────────
1. Strip ALL bracket groups and their contents:  [tag], {tag}, (tag)
2. Replace spaces, hyphens, dots (between words) with _
3. Collapse repeated separators: ____ → _
4. Strip leading/trailing underscores
5. Keep only safe chars: A-Z a-z 0-9 _ - .
6. Always preserve the original file extension
7. build_cc_output_name() appends a clean tag (VOSTFR, etc.)
   and ensures the result is safe for CC

EXAMPLES
─────────
  [Erai-raws] Tsue to Tsurugi no Wistoria 2nd Season - 04
  [720p CR WEB-DL AVC AAC][MultiSub][D0BBA05B].mkv
  → Tsue_to_Tsurugi_no_Wistoria_2nd_Season_04.mkv

  [SubsPlease] One Piece - 1118 (1080p) [A2B3C4D5].mkv
  → One_Piece_1118.mkv

  My.Anime.Show.S02E04.FRENCH.1080p.BluRay.x264.mkv
  → My_Anime_Show_S02E04_FRENCH_1080p_BluRay_x264.mkv
"""
from __future__ import annotations

import os
import re


# ── Character class constants ─────────────────────────────────────────────────

# Brackets and everything inside them (non-greedy)
_RE_BRACKETS = re.compile(r'\[[^\]]*\]|\{[^}]*\}|\([^)]*\)')

# Any run of characters that isn't alphanumeric or underscore/hyphen
_RE_UNSAFE   = re.compile(r'[^\w\-.]')

# Dots used as word separators (not the extension dot)
_RE_DOTS     = re.compile(r'\.(?=[a-zA-Z0-9])')

# Multiple underscores / hyphens collapsed to single
_RE_MULTI    = re.compile(r'[_\-]{2,}')

# Leading / trailing underscores and hyphens
_RE_EDGES    = re.compile(r'^[_\-]+|[_\-]+$')

# Season/episode markers — keep them clearly separated
_RE_SXXEXX   = re.compile(r'(?i)(S\d{1,2}E\d{1,2})')


def sanitize_filename(name: str) -> str:
    """
    Sanitise a filename for CloudConvert submission.
    Preserves the file extension.  Returns a safe ASCII filename.

    >>> sanitize_filename("[Erai-raws] Wistoria - 04 [720p][MultiSub][ABCD1234].mkv")
    'Wistoria_04.mkv'
    """
    if not name:
        return "video.mkv"

    base, ext = os.path.splitext(name)
    ext = ext.lower()

    # 1. Remove all bracket groups and their contents
    base = _RE_BRACKETS.sub(" ", base)

    # 2. Normalise dot-separated words to spaces
    base = _RE_DOTS.sub(" ", base)

    # 3. Replace any unsafe character with underscore
    base = _RE_UNSAFE.sub("_", base)

    # 4. Collapse runs of separators
    base = _RE_MULTI.sub("_", base)

    # 5. Strip edges
    base = _RE_EDGES.sub("", base)

    # 6. If completely empty after stripping, use fallback
    if not base:
        base = "video"

    return base + ext


def build_cc_output_name(input_name: str, tag: str = "VOSTFR") -> str:
    """
    Build a sanitised output filename for a CloudConvert hardsub job.

    The tag (e.g. "VOSTFR") is appended before the extension.
    The result is guaranteed safe for CC import/url filename parameter.

    >>> build_cc_output_name("[Erai-raws] Wistoria - 04 [720p][ABCD].mkv", "VOSTFR")
    'Wistoria_04_VOSTFR.mp4'
    """
    clean_base, _ = os.path.splitext(sanitize_filename(input_name))
    # Output is always MP4 (CloudConvert hardsub pipeline)
    return f"{clean_base}_{tag}.mp4"


# ── Backward-compat alias used by cloudconvert_api.py ────────────────────────
sanitize_for_cc = sanitize_filename


def sanitize_url_filename(name: str) -> str:
    """
    Extra-strict sanitisation for filenames embedded inside URLs.
    Only keeps alphanumerics, underscore, hyphen, and dot.
    Used when the filename must appear in a CloudConvert import URL path.
    """
    base, ext = os.path.splitext(sanitize_filename(name))
    # Remove anything that isn't [A-Za-z0-9_\-]
    base = re.sub(r'[^A-Za-z0-9_\-]', '', base)
    base = _RE_MULTI.sub("_", base)
    base = _RE_EDGES.sub("", base) or "video"
    return base + ext.lower()
