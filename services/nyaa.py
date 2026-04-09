"""
services/nyaa.py
Nyaa.si RSS feed scraper for anime torrent tracking.

RSS URL: https://nyaa.si/?page=rss&q=QUERY&c=1_2&f=0
  c=1_0 = Anime - All
  c=1_2 = Anime - English-translated
  c=1_4 = Anime - Raw
  f=0   = No filter
  f=2   = Trusted only
"""
from __future__ import annotations

import logging
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus

import aiohttp

log = logging.getLogger(__name__)

_NYAA_RSS = "https://nyaa.si/?page=rss"
_NYAA_NS  = {"nyaa": "https://nyaa.si/xmlns/nyaa"}

# Standard trackers for building magnets
_TRACKERS = (
    "http://nyaa.tracker.wf:7777/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
)


@dataclass
class NyaaEntry:
    title:       str
    link:        str
    magnet:      str       = ""
    torrent_url: str       = ""
    size:        str       = ""
    seeders:     int       = 0
    leechers:    int       = 0
    downloads:   int       = 0
    category:    str       = ""
    pub_date:    str       = ""
    info_hash:   str       = ""
    uploader:    str       = ""

    def __post_init__(self):
        # Extract uploader from [GroupName] prefix
        m = re.match(r'^\[([^\]]+)\]', self.title)
        if m and not self.uploader:
            self.uploader = m.group(1).strip()

        # Build torrent URL from link
        if not self.torrent_url and self.link:
            nid = re.search(r'/view/(\d+)', self.link)
            if nid:
                self.torrent_url = f"https://nyaa.si/download/{nid.group(1)}.torrent"

        # Extract info_hash from magnet
        if self.magnet and not self.info_hash:
            ih = re.search(r'btih:([a-fA-F0-9]{40}|[A-Za-z2-7]{32})', self.magnet)
            if ih:
                self.info_hash = ih.group(1).upper()

        # Build magnet from info_hash if missing
        if not self.magnet and self.info_hash:
            self.magnet = self._build_magnet()

    def _build_magnet(self) -> str:
        dn = quote_plus(self.title)
        trs = "&".join(f"tr={quote_plus(t)}" for t in _TRACKERS)
        return f"magnet:?xt=urn:btih:{self.info_hash}&dn={dn}&{trs}"


def _parse_rss(xml_text: str) -> list[NyaaEntry]:
    entries: list[NyaaEntry] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.warning("[Nyaa] RSS parse error: %s", exc)
        return []

    for item in root.findall(".//item"):
        title     = (item.findtext("title") or "").strip()
        link      = (item.findtext("link") or "").strip()
        guid      = (item.findtext("guid") or "").strip()
        seeders   = int(item.findtext("nyaa:seeders",   "0", _NYAA_NS) or 0)
        leechers  = int(item.findtext("nyaa:leechers",  "0", _NYAA_NS) or 0)
        downloads = int(item.findtext("nyaa:downloads",  "0", _NYAA_NS) or 0)
        size      = (item.findtext("nyaa:size", "", _NYAA_NS) or "").strip()
        category  = (item.findtext("nyaa:categoryId", "", _NYAA_NS) or "").strip()
        info_hash = (item.findtext("nyaa:infoHash", "", _NYAA_NS) or "").strip()
        pub_date  = (item.findtext("pubDate") or "").strip()

        magnet = guid if guid.startswith("magnet:") else ""

        entries.append(NyaaEntry(
            title=title, link=link, magnet=magnet,
            size=size, seeders=seeders, leechers=leechers,
            downloads=downloads, category=category,
            pub_date=pub_date, info_hash=info_hash.upper() if info_hash else "",
        ))

    return entries


def _short_date(pub_date: str) -> str:
    """'Wed, 09 Apr 2026 18:01:00 -0000' → 'Apr 09 18:01'"""
    try:
        parts = pub_date.split()
        return f"{parts[2]} {parts[1]} {parts[4][:5]}"
    except Exception:
        return pub_date[:16] if pub_date else ""


async def search_nyaa(
    query:    str,
    category: str = "1_2",
    filter_:  int = 0,
    sort:     str = "id",
    order:    str = "desc",
    timeout:  int = 15,
) -> list[NyaaEntry]:
    params = f"page=rss&q={quote_plus(query)}&c={category}&f={filter_}&s={sort}&o={order}"
    url = f"{_NYAA_RSS}&{params}"

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
    log.info("[Nyaa] Search '%s' (c=%s) → %d results", query, category, len(results))
    return results


def match_title(
    nyaa_title: str,
    watchlist_titles: list[str],
    required_uploader: str = "",
    required_quality:  str = "",
) -> bool:
    nt_lower = nyaa_title.lower()

    if required_uploader:
        uploader_m = re.match(r'^\[([^\]]+)\]', nyaa_title)
        actual = uploader_m.group(1).strip().lower() if uploader_m else ""
        if required_uploader.lower() not in actual:
            return False

    if required_quality:
        if required_quality.lower() not in nt_lower:
            return False

    clean_nyaa = re.sub(r'^\[[^\]]+\]\s*', '', nyaa_title)
    clean_nyaa = re.sub(r'\s*[-–]\s*\d+.*$', '', clean_nyaa)
    clean_nyaa = re.sub(r'\s+S\d+E\d+.*$', '', clean_nyaa, flags=re.I)
    clean_nyaa = re.sub(r'\s+(?:Episode|Ep\.?)\s*\d+.*$', '', clean_nyaa, flags=re.I)
    clean_nyaa = clean_nyaa.strip()

    from services.anilist import normalize_for_match, titles_match

    for wt in watchlist_titles:
        if titles_match(clean_nyaa, wt):
            return True
        wt_norm = normalize_for_match(wt)
        nt_norm = normalize_for_match(nyaa_title)
        if wt_norm and len(wt_norm) >= 4 and wt_norm in nt_norm:
            return True

    return False


def extract_episode(nyaa_title: str) -> Optional[int]:
    m = re.search(r'[-–]\s*(\d{1,4})\s*(?:v\d|\(|\[|\.mkv|\.mp4|$)', nyaa_title)
    if m:
        return int(m.group(1))
    m = re.search(r'S\d+E(\d+)', nyaa_title, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r'(?:Episode|Ep\.?)\s*(\d+)', nyaa_title, re.I)
    if m:
        return int(m.group(1))
    return None
