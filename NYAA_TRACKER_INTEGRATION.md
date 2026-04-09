"""
NYAA_TRACKER_INTEGRATION.md
────────────────────────────────────────────────────

## New Files (copy to your repo)

| File                       | Location                    |
|----------------------------|-----------------------------|
| `anilist.py`               | `services/anilist.py`       |
| `nyaa.py`                  | `services/nyaa.py`          |
| `nyaa_tracker.py`          | `plugins/nyaa_tracker.py`   |

## Edits to Existing Files

### 1. `main.py` — start the Nyaa poller after bot starts

After the line:
```python
    log.info("🤖 Bot name: %s", bot_name.upper())
```

Add:
```python
    # ── Nyaa anime tracker poller ────────────────────────────
    try:
        from plugins.nyaa_tracker import start_nyaa_poller
        start_nyaa_poller()
    except Exception as exc:
        log.warning("Nyaa tracker poller startup failed: %s", exc)
```

### 2. `plugins/start.py` — add Nyaa commands to help text

Add to the _help_text() function, after the Hardsub section:

```python
        "📡 <b>Nyaa Tracker</b>\\n"
        "› /nyaa_add — track anime on Nyaa (auto-scrape weekly)\\n"
        "› /nyaa_list — show tracked anime\\n"
        "› /nyaa_remove — stop tracking\\n"
        "› /nyaa_check — manual check now\\n"
        "› /nyaa_search — one-shot Nyaa search\\n"
        "› /nyaa_dump — set dump channel for raw results\\n\\n"
```

### 3. `.env.example` — document Seedr credentials (if not already there)

```
# Seedr credentials (for Nyaa auto-download pipeline)
SEEDR_USERNAME=
SEEDR_PASSWORD=
```

## How It Works

```
Owner: /nyaa_add Oshi no Ko | wednesday | SubsPlease | 1080p
  ↓
Bot queries AniList → resolves: "Oshi no Ko", "推しの子", "[Oshi No Ko]"
  ↓
Poller checks Nyaa RSS every 10 min on Wednesdays
  ↓
New episode found: [SubsPlease] Oshi no Ko - 13 (1080p) [ABCD1234].mkv
  ↓
Title matched via multi-language alias comparison
  ↓
Result sent to dump channel (with Seedr/Download/Skip buttons)
  ↓
If auto_seedr=True: magnet auto-sent to Seedr
  ↓
Seedr downloads at datacenter speed
  ↓
Bot downloads from Seedr → uploads to owner chat
  ↓
(Optional) If auto_hardsub=True → Seedr+Hardsub pipeline
```

## Commands

| Command                          | Description                         |
|----------------------------------|-------------------------------------|
| `/nyaa_add Title \\| day \\| group \\| quality` | Add anime to watchlist    |
| `/nyaa_list`                     | Show all tracked anime              |
| `/nyaa_remove <id>`             | Remove from watchlist               |
| `/nyaa_toggle <id>`             | Enable/disable entry                |
| `/nyaa_edit <id> <field> <val>` | Edit entry (day, uploader, etc.)    |
| `/nyaa_check`                    | Force check all entries now         |
| `/nyaa_search <query>`          | One-shot Nyaa search                |
| `/nyaa_dump <channel>`          | Set dump channel for notifications  |

## Title Matching (EN ↔ JP)

When you add an anime by English name, the bot:
1. Queries AniList GraphQL API (free, no key needed)
2. Gets: English title, Romaji, Native (Japanese), all synonyms
3. Stores ALL as aliases for matching
4. Nyaa titles are matched against ALL aliases using normalized comparison

Example: Adding "Oshi no Ko" stores:
  - "Oshi no Ko" (romaji, user input)
  - "Oshi No Ko" (AniList romaji)
  - "[Oshi No Ko]" (AniList English)
  - "推しの子" (AniList native/Japanese)

When Nyaa has `[SubsPlease] 推しの子 - 13 (1080p).mkv`, the bot matches
it against the stored Japanese title "推しの子" → match!

## Duplicate Prevention

Each watchlist entry tracks seen info_hashes. A torrent is only
processed once, even if it appears in multiple poll cycles.

## Data Files

| File                          | Purpose                     |
|-------------------------------|-----------------------------|
| `data/nyaa_watchlist.json`    | Watchlist entries + seen hashes |
| `data/nyaa_config.json`       | Dump channel, poll interval |
"""
