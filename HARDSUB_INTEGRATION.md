# CloudConvert Hardsub Plugin — Integration Guide

## New Files (copy to your repo)

| File | Location |
|------|----------|
| `cloudconvert_api.py` | `services/cloudconvert_api.py` |
| `hardsub.py` | `plugins/hardsub.py` |

## Edits to Existing Files

### 1. `core/config.py` — add CC_API_KEY field

After the `cc_webhook_secret` line, add:

```python
    cc_api_key: str = field(default_factory=lambda:
        os.environ.get("CC_API_KEY", ""))
```

### 2. `colab_launcher.py` — add CC_API_KEY credential

In the credentials section at the top, add:

```python
CC_API_KEY = ""  # @param {type:"string"}
```

In the secret resolution section, add:

```python
if not CC_API_KEY: CC_API_KEY = _secret("CC_API_KEY")
```

In the `env_lines` list, add:

```python
f"CC_API_KEY={CC_API_KEY}",
```

### 3. `plugins/start.py` — add /hardsub to help text

Add to the HELP_TEXT:

```
🔥 <b>Hardsub</b>
› /hardsub — burn subtitles via CloudConvert
› Supports: video file, URL, magnet + subtitle (.ass/.srt)
› Output: MP4 with hardcoded subs, auto-uploaded
```

### 4. `.env.example` / `env.example` — document the key

Add:

```
# CloudConvert API key (for /hardsub command)
# Get it at: cloudconvert.com/dashboard/api/v2/keys
CC_API_KEY=
```

## How It Works

```
User: /hardsub
  ↓
Bot: "Send video (file / URL / magnet)"
  ↓
User sends video file or pastes URL
  ↓
Bot: "Now send subtitle file"
  ↓
User sends .ass / .srt file
  ↓
Bot creates CloudConvert job:
  - import-video (URL import or file upload)
  - import-sub (file upload)
  - hardsub (FFmpeg command: subtitles filter + libx264)
  - export (temporary URL)
  ↓
CloudConvert processes (2-5 min for 24min episode)
  ↓
job.finished webhook fires → existing cloudconvert_hook.py
  ↓
Auto-downloads + uploads hardsubbed MP4 to Telegram
```

## Direct URL Mode (fastest)

When user pastes a direct HTTP link (e.g. from SonicBit):
- CloudConvert imports the video directly from the URL
- No local download or upload needed
- Only the small subtitle file gets uploaded
- Total time: ~2-5 minutes for a 24min episode

## Local File Mode

When user sends a Telegram file or magnet:
- Bot downloads video locally
- Uploads video + subtitle to CloudConvert
- Upload time depends on Colab bandwidth (~1-3 min for 500MB)
- CloudConvert processes (~2-5 min)
- Total: ~5-10 minutes

## Free Tier Limits

CloudConvert free plan: 25 conversion minutes/day
A typical 24-min anime episode uses ~3-5 conversion minutes
So you can hardsub ~5-8 episodes per day on the free plan
