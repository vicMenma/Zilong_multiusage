# CLAUDE.md — Zilong Multiusage Bot

> Drop this file in the repo root. It tells Claude everything about the
> project so you never have to re-explain architecture, patterns, or past
> fixes in a new session.

---

## Project Identity

- **Name**: Zilong Multiusage Bot  
- **Type**: Telegram bot (bot account, NOT user account)  
- **Framework**: Pyrogram / Pyrofork  
- **Language**: Python 3.10+  
- **Repo**: `vicMenma/Zilong_multiusage`  
- **Entry point**: `main.py`  
- **Session file**: stored in `data/ZilongBot.session`  
- **Runtime data dir**: `data/` (created at startup by main.py)

---

## Deployment Targets

| Platform     | Config file          | Notes |
|--------------|----------------------|-------|
| AWS EC2      | systemd unit         | Primary production target |
| Google Colab | `colab_launcher.py`  | Dev/testing, force-push to GitHub to deploy |
| Koyeb        | `koyeb.yaml`         | `KOYEB=1` env var enables health server |
| Fly.io       | `fly.toml`           | Alternative cloud |

- `.env` file is loaded with `load_dotenv(override=True)` so `.env` always beats any pre-set env vars (important on Colab)
- Single-instance guard via `/tmp/zilong_bot.pid` — kill old PID before restart
- `uvloop` installed for performance; falls back gracefully if missing

---

## Directory Layout

```
main.py                  ← entry point, plugin loader, startup orchestration
core/
  config.py              ← Config dataclass (frozen), all env vars, get/set_tunnel_url()
  session.py             ← SessionStore, UserStore, SettingsStore singletons
  bot_name.py            ← get/set_bot_name(), is_name_configured()
plugins/                 ← Pyrogram message/callback handlers (auto-loaded)
  admin.py               ← /ban, /unban, /broadcast
  archive.py             ← zip/rar creation and extraction
  caption_templates.py   ← build_caption(), has_custom_template()
  ccstatus.py            ← /ccstatus, CloudConvert balance poller
  extras.py              ← misc utility commands
  fc_seedr.py            ← FreeConvert + Seedr pipeline
  fc_webhook.py          ← FreeConvert webhook receiver + job store
  forwarder.py           ← auto-forward to channels
  hardsub.py             ← /hardsub via CloudConvert
  media_router.py        ← dispatches incoming files/photos (group=3)
  nyaa_tracker.py        ← Nyaa anime watchlist + scheduler
  panel.py               ← /status inline button callbacks
  resize.py              ← /compress, /resize via local FFmpeg
  seedr_hardsub.py       ← Seedr → CloudConvert hardsub pipeline
  start.py               ← /start, /help, /settings, /botname
  stream_extractor.py    ← m3u8 / HLS stream handling
  uploader.py            ← /upload command handler
  url_handler.py         ← URL + magnet + torrent file dispatcher
  usage.py               ← /usage stats (session-only, not persisted)
  video.py               ← video processing commands (merge, extract, burn)
services/
  anilist.py             ← AniList GraphQL queries
  cc_job_store.py        ← CloudConvert job persistence (data/cc_jobs.json)
  cc_sanitize.py         ← sanitize_for_cc(): filenames → [a-zA-Z0-9._-]
  cc_webhook_mgr.py      ← CloudConvert webhook registration
  cloudconvert_api.py    ← CC API v2 client, multi-key rotation
  cloudconvert_hook.py   ← aiohttp webhook server, tunnel (cloudflared → ngrok)
  downloader.py          ← smart_download(), all download strategies
  fc_job_store.py        ← FreeConvert job persistence
  ffmpeg.py              ← all FFmpeg/ffprobe operations
  freeconvert_api.py     ← FreeConvert API client
  keep_alive.py          ← health endpoint (port 8080)
  nyaa.py                ← Nyaa RSS scraper
  seedr.py               ← Seedr.cc async client wrapper (seedrcc v2.0.2)
  task_runner.py         ← TaskRunner, GlobalTracker, render_panel()
  telegraph.py           ← Telegraph image upload
  tg_download.py         ← Telegram file download with PanelUpdater
  uploader.py            ← upload_file() — uploads local file to Telegram
  utils.py               ← pure helpers: human_size, progress_panel, safe_edit, etc.
  webhook_sync.py        ← on_tunnel_ready(), poll_pending_jobs()
```

---

## Core Architecture Patterns

### TaskRunner + GlobalTracker (`services/task_runner.py`)

Central task registry. Every download/upload/process is a `TaskRecord`.

```python
# Submit a task through the runner (acquires semaphore slot)
record = await runner.submit(
    user_id=uid, label="filename.mkv", mode="dl", engine="aria2c",
    coro_factory=lambda rec: my_download_coro(rec),
)

# Self-register a task NOT submitted via submit() so cancel works
runner.register_raw(tid, asyncio.current_task())
```

Key fields on `TaskRecord`: `tid`, `seq`, `user_id`, `label`, `fname`, `mode`, `engine`, `state`, `done`, `total`, `speed`, `eta`, `seeds`, `meta_phase`

`mode` values: `"dl"` | `"ul"` | `"proc"` | `"magnet"` | `"seedr"` | `"queue"`

`MAX_CONCURRENT = 5` — controlled by `asyncio.Semaphore`. Modes `dl`, `proc`, `magnet` consume a slot; `ul` and `seedr` do not.

Cancel checks both `_task_handles` (submit-based) and `_raw_tasks` (self-registered).

### PanelUpdater Pattern (critical — do not break)

**Problem**: calling `await safe_edit()` inside a Pyrogram progress callback suspends the entire transfer coroutine on `FLOOD_WAIT`, causing downloads/uploads to stall.

**Solution**: `PanelUpdater` is a context manager that runs a background task editing the message on an interval, completely decoupled from the transfer coroutine.

```python
async with PanelUpdater(client, msg, record, tracker, interval=5.0) as pu:
    await do_the_actual_transfer(progress_cb=pu.update_progress)
```

`PanelUpdater` lives in `services/utils.py`. Used in:
- `services/uploader.py`
- `services/downloader.py`
- `services/tg_download.py`

**Never** call `await safe_edit()` directly from inside a Pyrogram progress callback. Always use PanelUpdater.

### SettingsStore (`core/session.py`)

Persists to `data/settings.json`. Defaults:

```python
{
    "upload_mode":      "auto",     # "auto" | "document"
    "prefix":           "",
    "suffix":           "",
    "thumb_id":         None,       # Telegram file_id
    "auto_forward":     False,
    "forward_channels": [],         # [{"id": int, "name": str}]
    "progress_style":   "B",        # "B" (cards) | "C" (minimal)
}
```

Access: `await settings.get(uid)` → dict with defaults merged. Update: `await settings.update(uid, {"key": val})`.

### SessionStore (`core/session.py`)

In-memory file-processing sessions with 30-min TTL (measured from last access). `FileSession.waiting` holds the expected reply type: `"merge_av"` | `"merge_vs"` | `"burn_sub"` | `"merge_vids"`.

`_evict()` is only called inside locked async methods — never call it outside a lock.

---

## Download Pipeline (`services/downloader.py`)

`smart_download(url, dest_dir, record, ...)` is the main entry. It calls `classify(url)` then dispatches:

| Classifier result | Strategy |
|-------------------|----------|
| `"magnet"` | aria2c, META_TIMEOUT=3min, registers raw task |
| `"ytdlp"` | yt-dlp in ProcessPoolExecutor |
| `"gdrive"` | service account streaming |
| `"mediafire"` | scrape + direct |
| `"direct"` | 4-attempt fallback chain via aiohttp |
| `"tg"` | `tg_download()` |

`PanelUpdater` interval = **5.0 s** (was 1s — caused FloodWait accumulation on long downloads, BUG-UH-04).

`_YTDLP_POOL`: `ProcessPoolExecutor` for yt-dlp — shut down in `runner.stop()`.

CC export URLs must use `download_direct()` NOT `smart_download()` — CC URLs are single-use signed tokens that break on retry logic (BUG-01).

---

## Upload Pipeline (`services/uploader.py`)

`upload_file(client, msg, path, record, ...)` detects file type by extension:
- Video → `send_video` with thumbnail
- Audio → `send_audio`
- Photo → `send_photo`
- Document → `send_document`

Auto-splits files > 1.9 GiB. FloodWait retry up to 4 attempts.

Thumbnail: generated via `ffmpeg.get_thumb()`, output at **1280×720** JPEG (`-q:v 3`). Telegram accepts these fine despite the documented 200KB "limit". Old 320×320 caused upscale blur (fixed).

`build_caption()` from `plugins/caption_templates.py` must be called to produce the final caption before sending.

After upload: forwards to `LOG_CHANNEL` if set.

---

## CloudConvert Integration

**API client** (`services/cloudconvert_api.py`):
- Multi-key rotation: `CC_API_KEY` is comma-separated, `pick_best_key()` checks credits concurrently across all keys
- All filenames sanitized via `sanitize_for_cc()` before passing to CC (reduces to `[a-zA-Z0-9._-]`)

**Webhook server** (`services/cloudconvert_hook.py`):
- Port 8765, aiohttp
- Tunnel priority: cloudflared → ngrok → localhost-only
- Signature verification strips `"sha256="` prefix before `compare_digest` (BUG-12 fix)
- `_process_file()` uses `download_direct()` — never `smart_download()`

**Webhook sync** (`services/webhook_sync.py`):
- `on_tunnel_ready(url, cc_path="/webhook/cloudconvert")` registers webhook at CC
- `poll_pending_jobs()` recovers jobs that completed while bot was offline

**Startup flow** in `main.py`:
- Webhook server starts if `CC_API_KEY` OR `FC_API_KEY` OR `WEBHOOK_BASE_URL`/`NGROK_TOKEN` is set
- CC webhooks sync only if tunnel URL is available AND `CC_API_KEY` is set
- `WEBHOOK_BASE_URL` env var = public URL (set by `colab_launcher.py` on Colab, set manually on VPS)

---

## FreeConvert Integration

- `FC_API_KEY` env var enables it
- Job store: `services/fc_job_store.py` → persists to `data/fc_jobs.json`
- Webhook: `plugins/fc_webhook.py` loaded at startup via `startup_load()`
- FC+Seedr pipeline: `plugins/fc_seedr.py`
- Per-job webhook URL embeds tunnel URL via `get_tunnel_url()`

---

## Seedr Integration (`services/seedr.py`)

Library: `seedrcc` v2.0.2. Verified method names:
```python
AsyncSeedr.from_password(username, password, on_token_refresh=)
client.add_torrent(magnet_link='magnet:?...')
client.list_contents(folder_id='0')
client.fetch_file(file_id: str)
client.delete_folder(folder_id: str)
client.get_settings()
client.close()
```

`poll_until_ready(client, magnet, existing_folder_ids, progress_cb)`:
- `existing_folder_ids` **must** be passed — it's the list of folder IDs before adding the torrent, used to detect the new folder. If passed as empty list or None, it cannot find the right folder (BUG-UH-03 root cause).
- Progress callback fires every 30s heartbeat regardless of percentage change.
- Token persisted to `data/seedr_token.json`.

---

## Nyaa Tracker (`plugins/nyaa_tracker.py`)

- Watchlist persisted to `data/nyaa_watchlist.json`
- Config in `data/nyaa_config.json`
- 10 results per page, timezone-aware scheduling
- `_setup` and `_schedule_waiting` dicts have TTL eviction (FIX H-03/H-04)
- `_magnet_cache` uses `_magnet_cache_put()` for TTL-wrapped writes (FIX M-06)
- Common uploaders list: Erai-raws, SubsPlease, Tsundere-Raws, EMBER, ToonsHub, DKB, ANi
- No auto-seedr — everything is button-driven

---

## Status Panel (`services/task_runner.py`)

`render_panel(uid)` → HTML string. `render_panel_kb(uid)` → `InlineKeyboardMarkup`.

Panel format:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡  BOT NAME MULTIUSAGE BOT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💻 CPU 42%  🧠 RAM 61%  💾 14.2G free
↓ 3.1M/s  ↑ —  📋 2/5 slots
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  [ 2 running  ·  1 queued ]

↓  #3  filename.mkv  [yt-dlp]
     [████████░░░░░░░]  54%  ⚡ 3.1M/s  ETA 2m
```

Progress bars: ASCII `█░` in `<code>` tags. Completed tasks: compact one-liner. Terminal states start with `✅` or `❌`.

Inline keyboard: per-task cancel buttons (2/row, max 8 tasks) + footer with Cancel All / Refresh / Close.

Panel callback data format: `"panel|{action}|{target}"` where action ∈ `cancel`, `cancel_all`, `refresh`, `close`.

---

## Config Reference (`.env`)

```bash
# Required
API_ID=
API_HASH=
BOT_TOKEN=
OWNER_ID=

# Optional
ADMINS=                    # space-separated extra admin IDs
FILE_LIMIT_MB=2048         # default 2 GiB
DOWNLOAD_DIR=/tmp/zilong_dl
LOG_CHANNEL=0              # Telegram channel ID, 0 = disabled

# Aria2c
ARIA2_HOST=http://localhost
ARIA2_PORT=6800
ARIA2_SECRET=

# Google Drive
GDRIVE_SA_JSON=service_account.json

# CloudConvert
CC_API_KEY=key1,key2       # comma-separated for multi-key rotation
CC_WEBHOOK_SECRET=         # optional HMAC secret
WEBHOOK_BASE_URL=          # public URL for port 8765 (auto-set on Colab)
NGROK_TOKEN=               # fallback tunnel if cloudflared fails

# FreeConvert
FC_API_KEY=

# Koyeb
KOYEB=1                    # enables health server on $PORT
PORT=8000
```

---

## Known Bugs Fixed — Do Not Reintroduce

| Bug ID | Description | Fix |
|--------|-------------|-----|
| BUG-01 | CC webhook download used `smart_download()` — burns single-use token on retry | Use `download_direct()` |
| BUG-12 | CC signature verify always failed — `"sha256="` prefix not stripped | `sig_hex = signature.removeprefix("sha256=")` |
| BUG-UH-01 | Upload aborted after magnet download — FloodWait from PanelUpdater cascaded into `send_message` | `_floodwait_send()` with retries; PanelUpdater interval → 5s |
| BUG-UH-03 | Seedr panel frozen on slow torrents | 30s heartbeat in `poll_until_ready()` |
| BUG-UH-04 | PanelUpdater at 1s interval accumulated FLOOD_WAIT on long downloads | Interval raised to 5.0s |
| BUG-11 | SessionStore TTL measured from creation, not last access | `s.created = now` on each `get()` |
| BUG-10 | Magnet cache TTL bypass in nyaa | `_magnet_cache_put()` wrapper |
| M-04 | `is_downloaded()` returned True for deleted files | Added `os.path.isfile()` check |
| C-01 | `_evict()` outside lock caused `RuntimeError: dict changed size` | `_evict()` only inside locked methods |
| M-01 | Stats updater task leaked on shutdown | Stored in `_stats_task`, cancelled in `stop()` |
| H-03/H-04 | Nyaa `_setup` / `_schedule_waiting` leaked forever on abandon | TTL eviction added |
| M-06 | Nyaa magnet cache used raw dict assignment, bypassing TTL | Use `_magnet_cache_put()` |
| MAIN-01 | `CC_API_KEY` not found if dotenv loaded after env export on Colab | Check `os.environ` directly too |
| MAIN-02 | `data/` dir not created before first service write | `os.makedirs(data/)` at startup |
| MAIN-03 | CC webhook registered to wrong path | Explicit `cc_path="/webhook/cloudconvert"` |
| MAIN-04 | FC import errors swallowed silently | Log at WARNING |
| MAIN-05 | Webhook server only started for CC key | Start for CC OR FC key |

---

## Critical Rules — Never Violate

1. **PanelUpdater** — never call `await safe_edit()` from inside a Pyrogram progress callback. Always use `PanelUpdater` context manager.
2. **Bot account** — this is a bot token client, NOT a user account. Pyrogram user-account methods (`get_messages` with peer resolution, etc.) behave differently.
3. **Seedr `poll_until_ready()`** — always pass `existing_folder_ids` (the folder list snapshot taken BEFORE `add_torrent()`).
4. **CC filenames** — always run through `sanitize_for_cc()` before passing to any CloudConvert API call.
5. **`_evict()`** in SessionStore — only call inside `async with self._lock`.
6. **CC download** — use `download_direct()`, never `smart_download()`.
7. **`build_caption()`** — must be called before `send_video/send_document`, not skipped.
8. **`runner.register_raw(tid, asyncio.current_task())`** — call this inside any coroutine that needs to be cancellable but was NOT submitted via `runner.submit()`.

---

## When Asking Claude for Help

- Reference files by name + function, e.g.: "in `services/downloader.py`, the `_dispatch()` function"
- For bugs: state the symptom, the suspected file/function, and what you've already tried
- For new features: state which existing pattern to follow (e.g. "like the existing hardsub flow")
- Ask for diffs/patches, not full file rewrites — saves tokens and is easier to review
- The project uses `from __future__ import annotations` everywhere — type hints are strings, not evaluated at runtime
