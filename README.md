# ⚡ Zilong Bot — Rewrite

Full-featured Telegram media bot. Clean architecture, reliable FFmpeg pipeline, Colab-first.

## What Changed vs Original

| Area | Before | After |
|---|---|---|
| **Database** | In-memory dicts, wiped on restart | In-memory with clean dataclasses, TTL eviction |
| **Session locking** | None — race conditions possible | `asyncio.Lock` per file session |
| **FFmpeg trim/split** | `-to` after `-ss before -i` (off-by-N bug) | `-t` (duration) after fast seek; accurate `-to` for trim tool |
| **Thumbnail brightness** | `ffprobe signalstats` on a JPEG (always fails) | PIL `ImageStat` → file-size fallback |
| **Live updater** | Background loop editing every 2s per user (flood risk) | Per-message throttle, edit only when changed |
| **Telegraph token** | New account every Colab restart | Persisted to `/tmp/zilong_telegraph.token` |
| **Concat escaping** | Shell quote escaping (breaks on `'`) | Correct concat demuxer backslash escaping |
| **Premium tiers** | Complex paid/free gate | Removed — single flat file limit for everyone |
| **Circular imports** | Runtime imports inside handlers | Clean dependency graph, no runtime imports |
| **Architecture** | God-files (utils.py ~600 lines) | `core/` `services/` `plugins/` separation |

## Structure

```
zilong/
├── main.py                  ← entry point
├── core/
│   ├── config.py            ← all config, validated at startup
│   └── session.py           ← FileSession, UserStore, SettingsStore
├── services/
│   ├── ffmpeg.py            ← all FFmpeg/ffprobe ops
│   ├── downloader.py        ← download strategies (no Telegram coupling)
│   ├── uploader.py          ← upload with FloodWait retry
│   ├── tg_download.py       ← Telegram file download with progress
│   ├── task_runner.py       ← asyncio.Queue job runner
│   ├── telegraph.py         ← Telegraph with persistent token
│   └── utils.py             ← formatters, progress panel, filesystem helpers
└── plugins/
    ├── start.py             ← /start /help /settings /info
    ├── admin.py             ← /ban /broadcast /stats /log /restart
    ├── media_router.py      ← receives files, dispatches to plugins
    ├── video.py             ← all video operations
    ├── url_handler.py       ← URL downloads, stream selector
    ├── archive.py           ← extract / create archives
    ├── forwarder.py         ← forward without tag
    └── extras.py            ← /show_thumb /del_thumb /json_formatter
```

## Google Colab (primary target)

Open `colab_launcher.py` in Colab, fill in the 4 cells at the top, then **Runtime → Run all**.

```
API_ID    = 12345678
API_HASH  = "abc123..."
BOT_TOKEN = "123456:ABC..."
OWNER_ID  = 987654321
```

The launcher:
- Installs ffmpeg, aria2, mediainfo, p7zip, unrar
- Clones the repo fresh each run
- Starts aria2c daemon
- Runs the bot with exponential back-off auto-restart (max 10 attempts)

## EC2 / VPS

```bash
git clone https://github.com/vicMenma/Zilong_multiusage
cd Zilong_multiusage
bash setup_ec2.sh
nano .env
sudo systemctl start zilong
sudo journalctl -u zilong -f
```

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_ID` | ✅ | — | From my.telegram.org |
| `API_HASH` | ✅ | — | From my.telegram.org |
| `BOT_TOKEN` | ✅ | — | From @BotFather |
| `OWNER_ID` | ✅ | — | Your Telegram user ID |
| `FILE_LIMIT_MB` | ❌ | 2048 | Max file size in MB |
| `ADMINS` | ❌ | — | Extra admin IDs (space-separated) |
| `LOG_CHANNEL` | ❌ | 0 | Forward uploads here |
| `ARIA2_HOST` | ❌ | localhost | aria2c RPC host |
| `ARIA2_PORT` | ❌ | 6800 | aria2c RPC port |
| `GDRIVE_SA_JSON` | ❌ | — | Google service account JSON path |

## Commands

| Command | Description |
|---|---|
| `/start` | Welcome message |
| `/help` | Full feature list |
| `/settings` | Upload preferences |
| `/info` | Your account info |
| `/status` | System stats |
| `/forward` | Forward without tag |
| `/createarchive` | Start archive creation |
| `/mergedone` | Finish video merge |
| `/show_thumb` | View saved thumbnail |
| `/del_thumb` | Delete saved thumbnail |
| `/json_formatter` | Pretty-print JSON |
| `/admin` | *(admin)* Admin commands list |
| `/ban_user <id>` | *(admin)* Ban a user |
| `/unban_user <id>` | *(admin)* Unban a user |
| `/banned_list` | *(admin)* List banned users |
| `/stats` | *(admin)* Bot + system stats |
| `/log` | *(admin)* Last 50 log lines |
| `/restart` | *(admin)* Restart bot |
| `/broadcast` | *(admin)* Broadcast (reply to msg) |

## FFmpeg Fixes Applied

**Trim accuracy:** `-ss` is placed *after* `-i` for frame-accurate cuts in the trim tool. For split (where speed matters more than frame accuracy) `-ss` is before `-i` with `-t` (duration), avoiding the classic `-to`-after-fast-seek off-by-N-seconds bug.

**Concat escaping:** The concat list file uses backslash escaping (`\'`) not shell-quote escaping. Single quotes in filenames no longer break merges.

**Thumbnail brightness:** PIL `ImageStat.Stat` on the extracted JPEG, with a file-size heuristic fallback. The original `ffprobe signalstats` filter on a JPEG silently failed every time.

**Stream probe:** 3-pass strategy — standard → doubled analyzeduration/probesize → mediainfo JSON fallback. MKV/HEVC/WEBM detection is reliable.

**Duration:** 4-strategy cascade — format.duration → stream.duration → HH:MM:SS tags (MKV matroska) → nb_frames/fps.
