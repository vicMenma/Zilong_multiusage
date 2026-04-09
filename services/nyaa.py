"""
services/nyaa.py
Nyaa.si RSS feed scraper for anime torrent tracking.

Uses Nyaa's public RSS endpoint — no scraping/JS needed.
RSS URL: https://nyaa.si/?page=rss&q=QUERY&c=1_2&f=0
  c=1_2 = Anime - English-translated
  c=1_4 = Anime - Raw
  f=0   = No filter (all)
  f=2   = Trusted only

Each RSS item provides: title, link (torrent page), magnet (nyaa:infoHash
in the guid or extracted from the page), size, seeders, date.

DESIGN:
  - search_nyaa(): one-shot search, returns parsed results
  - NyaaEntry dataclass for each result
  - match_entry(): checks if a Nyaa title matches a watchlist entry
    using normalized multi-language title comparison
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote_plus

import aiohttp

log = logging.getLogger(__name__)

_NYAA_RSS = "https://nyaa.si/?page=rss"
_NYAA_VIEW = "https://nyaa.si/view/"
_NYAA_NS = {"nyaa": "https://nyaa.si/xmlns/nyaa"}


@dataclass
class NyaaEntry:
    title:       str
    link:        str            # https://nyaa.si/view/12345
    magnet:      str = ""
    torrent_url: str = ""       # https://nyaa.si/download/12345.torrent
    size:        str = ""       # "1.4 GiB"
    seeders:     int = 0
    leechers:    int = 0
    downloads:   int = 0
    category:    str = ""
    pub_date:    str = ""
    info_hash:   str = ""
    uploader:    str = ""       # extracted from title pattern [UploaderName]

    def __post_init__(self):
        # Extract uploader from title if present: [SubsPlease] Title - 01 ...
        m = re.match(r'^\[([^\]]+)\]', self.title)
        if m and not self.uploader:
            self.uploader = m.group(1).strip()

        # Build torrent URL from link if missing
        if not self.torrent_url and self.link:
            nid = re.search(r'/view/(\d+)', self.link)
            if nid:
                self.torrent_url = f"https://nyaa.si/download/{nid.group(1)}.torrent"

        # Extract info_hash from magnet
        if self.magnet and not self.info_hash:
            ih = re.search(r'btih:([a-fA-F0-9]{40}|[A-Za-z2-7]{32})', self.magnet)
            if ih:
                self.info_hash = ih.group(1).upper()


def _parse_rss(xml_text: str) -> list[NyaaEntry]:
    """Parse Nyaa RSS XML into NyaaEntry list."""
    entries: list[NyaaEntry] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.warning("[Nyaa] RSS parse error: %s", exc)
        return []

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link  = (item.findtext("link") or "").strip()
        guid  = (item.findtext("guid") or "").strip()

        # Nyaa puts the magnet link in the guid sometimes
        magnet = ""
        if guid.startswith("magnet:"):
            magnet = guid

        # Nyaa namespace fields
        seeders   = int(item.findtext("nyaa:seeders",   "0", _NYAA_NS) or 0)
        leechers  = int(item.findtext("nyaa:leechers",  "0", _NYAA_NS) or 0)
        downloads = int(item.findtext("nyaa:downloads",  "0", _NYAA_NS) or 0)
        size      = (item.findtext("nyaa:size", "", _NYAA_NS) or "").strip()
        category  = (item.findtext("nyaa:category", "", _NYAA_NS) or "").strip()
        info_hash = (item.findtext("nyaa:infoHash", "", _NYAA_NS) or "").strip()
        pub_date  = (item.findtext("pubDate") or "").strip()

        # Build magnet from info_hash if we don't have one
        if not magnet and info_hash:
            dn = quote_plus(title)
            magnet = (
                f"magnet:?xt=urn:btih:{info_hash}"
                f"&dn={dn}"
                f"&tr=http%3A%2F%2Fnyaa.tracker.wf%3A7777%2Fannounce"
                f"&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce"
                f"&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce"
            )

        entries.append(NyaaEntry(
            title=title, link=link, magnet=magnet,
            size=size, seeders=seeders, leechers=leechers,
            downloads=downloads, category=category,
            pub_date=pub_date, info_hash=info_hash.upper() if info_hash else "",
        ))

    return entries


async def search_nyaa(
    query:    str,
    category: str  = "1_2",   # 1_2=Anime English-translated, 1_4=Raw, 1_0=All Anime
    filter_:  int  = 0,       # 0=No filter, 2=Trusted
    sort:     str  = "id",    # id (newest), seeders, size, downloads
    order:    str  = "desc",
    timeout:  int  = 15,
) -> list[NyaaEntry]:
    """
    Search Nyaa via RSS feed.
    Returns newest results first (by default).
    """
    params = {
        "page": "rss",
        "q":    query,
        "c":    category,
        "f":    str(filter_),
        "s":    sort,
        "o":    order,
    }
    url = _NYAA_RSS + "&" + "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"User-Agent": "ZilongBot/2.0"},
        ) as sess:
            async with sess.get(url) as resp:
                if resp.status != 200:
                    log.warning("[Nyaa] HTTP %d for query=%s", resp.status, query)
                    return []
                xml_text = await resp.text()
    except Exception as exc:
        log.warning("[Nyaa] Request failed: %s", exc)
        return []

    results = _parse_rss(xml_text)
    log.info("[Nyaa] Search '%s' → %d results", query, len(results))
    return results


async def fetch_magnet_from_page(nyaa_url: str) -> str:
    """
    Fallback: fetch the Nyaa torrent page and extract the magnet link
    from the page HTML. Used when RSS doesn't provide the magnet.
    """
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent": "ZilongBot/2.0"},
        ) as sess:
            async with sess.get(nyaa_url) as resp:
                html = await resp.text()
        m = re.search(r'href="(magnet:\?[^"]+)"', html)
        if m:
            return m.group(1)
    except Exception as exc:
        log.warning("[Nyaa] Page fetch failed: %s", exc)
    return ""


def match_title(
    nyaa_title: str,
    watchlist_titles: list[str],
    required_uploader: str = "",
    required_quality:  str = "",
) -> bool:
    """
    Check if a Nyaa torrent title matches a watchlist entry.

    Handles:
      - [Uploader] Title - Episode (Quality) [Hash].mkv  format
      - Multi-language title matching via watchlist_titles (EN + JP + romaji)
      - Quality filtering (1080p, 720p, etc.)
      - Uploader filtering (e.g. "Tsundere Raws", "SubsPlease")

    Returns True if the entry matches ALL specified criteria.
    """
    nt_lower = nyaa_title.lower()

    # ── Uploader check ────────────────────────────────────────
    if required_uploader:
        # Extract [UploaderName] from the Nyaa title
        uploader_m = re.match(r'^\[([^\]]+)\]', nyaa_title)
        actual_uploader = uploader_m.group(1).strip().lower() if uploader_m else ""
        if required_uploader.lower() not in actual_uploader:
            return False

    # ── Quality check ─────────────────────────────────────────
    if required_quality:
        # Accept "1080p", "720p", "480p", "4K" etc.
        if required_quality.lower() not in nt_lower:
            return False

    # ── Title matching ────────────────────────────────────────
    # Strip the [Uploader] prefix and trailing metadata for matching
    clean_nyaa = re.sub(r'^\[[^\]]+\]\s*', '', nyaa_title)
    # Remove episode number, quality, hash, extension from end
    # Pattern: "Title - 01 (1080p) [ABCD].mkv" → "Title"
    # Also:    "Title S02E05 1080p WEB" → "Title"
    clean_nyaa = re.sub(
        r'\s*[-–]\s*\d+.*$', '', clean_nyaa  # "Title - 01 ..."
    )
    clean_nyaa = re.sub(
        r'\s+S\d+E\d+.*$', '', clean_nyaa, flags=re.I  # "Title S02E05 ..."
    )
    clean_nyaa = re.sub(
        r'\s+(?:Episode|Ep\.?)\s*\d+.*$', '', clean_nyaa, flags=re.I
    )
    clean_nyaa = clean_nyaa.strip()

    from services.anilist import normalize_for_match, titles_match

    for wt in watchlist_titles:
        if titles_match(clean_nyaa, wt):
            return True
        # Also check if the full nyaa title contains the watchlist title
        wt_norm = normalize_for_match(wt)
        nt_norm = normalize_for_match(nyaa_title)
        if wt_norm and wt_norm in nt_norm:
            return True

    return False


def extract_episode(nyaa_title: str) -> Optional[int]:
    """
    Extract episode number from a Nyaa torrent title.
    Handles:  "Title - 01", "Title S02E05", "Title Episode 3"
    Returns None if no episode number found (could be a batch).
    """
    # [Group] Title - 01 (1080p)
    m = re.search(r'[-–]\s*(\d{1,4})\s*(?:\(|v\d|\[|\.mkv|\.mp4|$)', nyaa_title)
    if m:
        return int(m.group(1))
    # S02E05
    m = re.search(r'S\d+E(\d+)', nyaa_title, re.I)
    if m:
        return int(m.group(1))
    # Episode 5 / Ep 5 / Ep.5
    m = re.search(r'(?:Episode|Ep\.?)\s*(\d+)', nyaa_title, re.I)
    if m:
        return int(m.group(1))
    return None
