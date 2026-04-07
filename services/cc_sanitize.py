"""
services/cc_sanitize.py
Filename sanitizer for CloudConvert and general filesystem safety.

WHY THIS EXISTS:
  CloudConvert passes filenames directly into an FFmpeg command:
      -vf subtitles='/input/import-sub/FILENAME'

  Characters that silently break CloudConvert jobs:
      '        — breaks the subtitles='...' filter quoting
      :        — FFmpeg filter-graph separator (e.g. subtitles:si=0)
      \\        — escape character, corrupts the path
      [ ]      — glob expansion on CC's Linux runner (confirmed bug)
      ( )      — confuses some shell parsers
      & | < >  — shell metacharacters
      spaces   — dropped or mishandled by CC's S3 object naming
      Unicode  — CC's runner may not handle non-ASCII filenames reliably
      accented — é, ü, ñ etc. survive NFKD normalization → ASCII approximation

  Safe character set for CC: [a-zA-Z0-9._-]
  Everything else is replaced with underscore.

USAGE:
    from services.cc_sanitize import sanitize_for_cc, sanitize_filename

    # For CloudConvert jobs (aggressive — CC-safe only):
    safe = sanitize_for_cc("Oshi no Ko [1080p] (VOSTFR).mkv")
    # → "Oshi_no_Ko_1080p_VOSTFR.mkv"

    # For general downloads (keeps more chars, just fixes filesystem issues):
    safe = sanitize_filename("Oshi no Ko [1080p] (VOSTFR).mkv")
    # → "Oshi_no_Ko_1080p_VOSTFR.mkv"
"""
from __future__ import annotations

import os
import re
import unicodedata


def sanitize_for_cc(filename: str, max_name_len: int = 60) -> str:
    """
    Aggressive sanitizer for CloudConvert filenames.
    Keeps only [a-zA-Z0-9._-]. All else → underscore.
    Preserves file extension (lowercased, kept as-is).

    Examples:
        "Oshi no Ko S02E05 [1080p][HEVC][VOSTFR].mkv"
        → "Oshi_no_Ko_S02E05_1080p_HEVC_VOSTFR.mkv"

        "Épisode 01 — Titre (Blu-Ray).mp4"
        → "Episode_01_Titre_Blu-Ray.mp4"

        "[SubsPlease] Show Name - 01 (1080p) [ABCD1234].mkv"
        → "SubsPlease_Show_Name_-_01_1080p_ABCD1234.mkv"
    """
    name, ext = os.path.splitext(filename)
    ext = ext.lower()

    # Step 1 — Unicode normalization: é → e + combining_accent
    nfkd = unicodedata.normalize("NFKD", name)
    # Drop combining characters (the accent marks)
    ascii_approx = "".join(c for c in nfkd if not unicodedata.combining(c))

    # Step 2 — Replace every char outside [a-zA-Z0-9._-] with underscore
    safe = re.sub(r"[^a-zA-Z0-9.\-]", "_", ascii_approx)

    # Step 3 — Collapse runs of underscores / dots
    safe = re.sub(r"_+", "_", safe)
    safe = re.sub(r"\.+", ".", safe)

    # Step 4 — Strip leading/trailing underscores and dots
    safe = safe.strip("_.")

    # Step 5 — Enforce max length (prevents S3 key issues and very long FFmpeg commands)
    if len(safe) > max_name_len:
        safe = safe[:max_name_len].rstrip("_.")

    return (safe or "file") + ext


def sanitize_filename(filename: str, max_name_len: int = 120) -> str:
    """
    Lighter sanitizer for general filesystem use (Telegram uploads, local storage).
    Allows spaces and a wider character set. Still blocks filesystem-unsafe chars.
    Useful for downloaded filenames from Seedr or aria2 before saving to disk.

    Blocked: \\ / : * ? " < > |  (Windows + Linux unsafe)
    Replaces them with underscore. Spaces are kept (unlike sanitize_for_cc).
    """
    name, ext = os.path.splitext(filename)

    # Block filesystem-unsafe chars
    safe = re.sub(r'[\\/:*?"<>|]', "_", name)

    # Collapse multiple underscores
    safe = re.sub(r"_+", "_", safe)

    # Strip
    safe = safe.strip("_. ")

    if len(safe) > max_name_len:
        safe = safe[:max_name_len].rstrip("_. ")

    return (safe or "file") + ext


def build_cc_output_name(source_filename: str, suffix: str = "VOSTFR") -> str:
    """
    Build a CloudConvert-safe output filename from a source filename.
    Strips the original extension, sanitizes, appends suffix, forces .mp4.

    Examples:
        build_cc_output_name("Oshi no Ko S02E05 [1080p].mkv", "VOSTFR")
        → "Oshi_no_Ko_S02E05_1080p_VOSTFR.mp4"

        build_cc_output_name("show.s01e01.720p.WEB.mkv", "FR")
        → "show.s01e01.720p.WEB_FR.mp4"
    """
    name = os.path.splitext(source_filename)[0]

    # Sanitize the base name
    safe_base = sanitize_for_cc(name + ".tmp")[:-4]  # remove the .tmp we added for ext handling

    # Append suffix
    if suffix:
        safe_suffix = re.sub(r"[^a-zA-Z0-9]", "_", suffix).strip("_")
        safe_base   = f"{safe_base}_{safe_suffix}"

    return safe_base + ".mp4"
