"""
services/anilist.py
AniList GraphQL client — resolves anime titles across languages.

Given ANY title (English, Romaji, or Japanese), returns all known
aliases so the Nyaa scraper can match regardless of language.

AniList API is free, no key needed, rate-limited to 90 req/min.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_ANILIST_URL = "https://graphql.anilist.co"

_SEARCH_QUERY = """
query ($search: String) {
  Page(page: 1, perPage: 5) {
    media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
      id
      title {
        romaji
        english
        native
      }
      synonyms
      season
      seasonYear
      episodes
      status
      format
    }
  }
}
"""

_BY_ID_QUERY = """
query ($id: Int) {
  Media(id: $id, type: ANIME) {
    id
    title {
      romaji
      english
      native
    }
    synonyms
    season
    seasonYear
    episodes
    status
    format
    nextAiringEpisode {
      episode
      airingAt
    }
  }
}
"""


async def search_anime(query: str, timeout: int = 10) -> list[dict]:
    """
    Search AniList for anime matching `query`.
    Returns list of results, each with:
      id, titles (dict), synonyms (list), season, year, episodes, status, format
    """
    payload = {"query": _SEARCH_QUERY, "variables": {"search": query}}
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as sess:
            async with sess.post(_ANILIST_URL, json=payload) as resp:
                if resp.status == 429:
                    log.warning("[AniList] Rate limited — waiting 5s")
                    await asyncio.sleep(5)
                    async with sess.post(_ANILIST_URL, json=payload) as resp2:
                        data = await resp2.json()
                else:
                    data = await resp.json()
    except Exception as exc:
        log.warning("[AniList] Search failed: %s", exc)
        return []

    results = []
    for media in (data.get("data") or {}).get("Page", {}).get("media", []):
        title = media.get("title") or {}
        results.append({
            "id":       media.get("id"),
            "romaji":   title.get("romaji", ""),
            "english":  title.get("english", ""),
            "native":   title.get("native", ""),
            "synonyms": media.get("synonyms") or [],
            "season":   media.get("season", ""),
            "year":     media.get("seasonYear"),
            "episodes": media.get("episodes"),
            "status":   media.get("status", ""),
            "format":   media.get("format", ""),
        })
    return results


async def get_anime_by_id(anilist_id: int) -> Optional[dict]:
    """Fetch a single anime by AniList ID."""
    payload = {"query": _BY_ID_QUERY, "variables": {"id": anilist_id}}
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as sess:
            async with sess.post(_ANILIST_URL, json=payload) as resp:
                data = await resp.json()
    except Exception as exc:
        log.warning("[AniList] ID lookup failed: %s", exc)
        return None

    media = (data.get("data") or {}).get("Media")
    if not media:
        return None
    title = media.get("title") or {}
    return {
        "id":       media.get("id"),
        "romaji":   title.get("romaji", ""),
        "english":  title.get("english", ""),
        "native":   title.get("native", ""),
        "synonyms": media.get("synonyms") or [],
        "season":   media.get("season", ""),
        "year":     media.get("seasonYear"),
        "episodes": media.get("episodes"),
        "status":   media.get("status", ""),
        "format":   media.get("format", ""),
        "next_ep":  media.get("nextAiringEpisode"),
    }


def all_titles(anime: dict) -> list[str]:
    """
    Extract every known title/synonym from an AniList result.
    Returns a deduplicated list, all stripped, no empties.
    """
    titles: list[str] = []
    for key in ("romaji", "english", "native"):
        val = anime.get(key, "")
        if val and val.strip():
            titles.append(val.strip())
    for syn in anime.get("synonyms", []):
        if syn and syn.strip():
            titles.append(syn.strip())
    # Deduplicate preserving order
    seen: set = set()
    unique: list[str] = []
    for t in titles:
        low = t.lower()
        if low not in seen:
            seen.add(low)
            unique.append(t)
    return unique


def normalize_for_match(title: str) -> str:
    """
    Normalize a title for fuzzy matching:
      - lowercase
      - strip season/part indicators
      - remove punctuation except spaces
      - collapse whitespace
    """
    s = title.lower().strip()
    # Remove common season suffixes
    s = re.sub(r'\s*(season|part|cour)\s*\d+', '', s)
    s = re.sub(r'\s*s\d+$', '', s)
    s = re.sub(r'\s*\d+(st|nd|rd|th)\s*season', '', s)
    # Remove punctuation (keep CJK characters and alphanumeric)
    s = re.sub(r'[^\w\s\u3000-\u9fff\uff00-\uffef]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def titles_match(title_a: str, title_b: str) -> bool:
    """
    Check if two titles refer to the same anime.
    Handles partial matches for long titles.
    """
    na = normalize_for_match(title_a)
    nb = normalize_for_match(title_b)
    if not na or not nb:
        return False
    # Exact match after normalization
    if na == nb:
        return True
    # One contains the other (handles "Title" matching "Title 2nd Season")
    if na in nb or nb in na:
        return True
    return False
