# @title ⚡ Zilong Bot — Colab Launcher
# @markdown ## Credentials
# @markdown
# @markdown **Recommended:** Add secrets via the 🔑 icon in the left panel:
# @markdown - `API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID`, `GITHUB_TOKEN`

API_ID    = 0      # @param {type:"integer"}
API_HASH  = ""     # @param {type:"string"}
BOT_TOKEN = ""     # @param {type:"string"}
OWNER_ID  = 0      # @param {type:"integer"}

FILE_LIMIT_MB = 2048   # @param {type:"integer"}
LOG_CHANNEL   = 0      # @param {type:"integer"}

# CloudConvert auto-upload (optional)
NGROK_TOKEN       = ""  # @param {type:"string"}
CC_WEBHOOK_SECRET = ""  # @param {type:"string"}

# CloudConvert hardsub API key (for /hardsub command)
CC_API_KEY = ""  # @param {type:"string"}

# GitHub personal access token — required for private repo clone
GITHUB_TOKEN = ""  # @param {type:"string"}

import os, sys, subprocess, shutil, time, glob
from datetime import datetime

# FIX (BUG 1): correct repo URL — Zilong_v2, not Zilong_multiusage
REPO_NAME = "Zilong_v2"
BASE_DIR  = f"/content/zilong"


def _log(level: str, msg: str):
    icons = {"INFO": "ℹ️", "OK": "✅", "WARN": "⚠️", "ERR": "❌", "STEP": "🔧"}
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {icons.get(level, '')} {msg}", flush=True)


def _secret(name: str) -> str:
    try:
        from google.colab import userdata
        val = userdata.get(name)
        if val:
            return str(val).strip()
    except Exception:
        pass
    return os.environ.get(name, "").strip()


print("⚡ Zilong Bot — Colab Launcher")
print("─" * 50)
_log("STEP", "Resolving credentials…")

if not API_ID:
    try: API_ID = int(_secret("API_ID"))
    except: API_ID = 0
if not API_HASH:  API_HASH  = _secret("API_HASH")
if not BOT_TOKEN: BOT_TOKEN = _secret("BOT_TOKEN")
if not OWNER_ID:
    try: OWNER_ID = int(_secret("OWNER_ID"))
    except: OWNER_ID = 0
if not FILE_LIMIT_MB:
    try: FILE_LIMIT_MB = int(_secret("FILE_LIMIT_MB") or 2048)
    except: FILE_LIMIT_MB = 2048
if not LOG_CHANNEL:
    try: LOG_CHANNEL = int(_secret("LOG_CHANNEL") or 0)
    except: LOG_CHANNEL = 0
# FIX: also check NGROK_AUTHTOKEN as alias
if not NGROK_TOKEN:
    NGROK_TOKEN = _secret("NGROK_TOKEN") or _secret("NGROK_AUTHTOKEN")
if not CC_WEBHOOK_SECRET: CC_WEBHOOK_SECRET = _secret("CC_WEBHOOK_SECRET")
if not CC_API_KEY:        CC_API_KEY        = _secret("CC_API_KEY")
if not GITHUB_TOKEN:      GITHUB_TOKEN      = _secret("GITHUB_TOKEN")

errors = []
if not API_ID:    errors.append("API_ID is required")
if not API_HASH:  errors.append("API_HASH is required")
if not BOT_TOKEN: errors.append("BOT_TOKEN is required")
if not OWNER_ID:  errors.append("OWNER_ID is required")
if errors:
    print()
    for e in errors: print(f"  ❌ {e}")
    print()
    raise SystemExit("Fill in credentials and run again.")

_log("OK", f"Credentials loaded  (API_ID={API_ID}, OWNER_ID={OWNER_ID})")
if NGROK_TOKEN:  _log("OK", "CloudConvert webhook enabled (NGROK_TOKEN set)")
if CC_API_KEY:   _log("OK", "CloudConvert hardsub enabled (CC_API_KEY set)")
if GITHUB_TOKEN: _log("OK", "GitHub token set — will clone private repo")

_log("STEP", "Installing system packages…")
subprocess.run(
    "apt-get update -qq && "
    "apt-get install -y -qq ffmpeg aria2 mediainfo p7zip-full unrar 2>/dev/null",
    shell=True, capture_output=True,
)
_log("OK", "System packages ready")

_log("STEP", "Cloning repository…")
if os.path.exists(BASE_DIR):
    shutil.rmtree(BASE_DIR)

# FIX (BUG 1): inject GITHUB_TOKEN into clone URL for private repo access
if GITHUB_TOKEN:
    REPO_URL = f"https://{GITHUB_TOKEN}@github.com/vicMenma/{REPO_NAME}.git"
else:
    REPO_URL = f"https://github.com/vicMenma/{REPO_NAME}.git"

r = subprocess.run(["git", "clone", "--depth=1", REPO_URL, BASE_DIR],
                   capture_output=True, text=True)
if r.returncode != 0:
    # Sanitize token from error message before printing
    err_clean = r.stderr.replace(GITHUB_TOKEN, "***") if GITHUB_TOKEN else r.stderr
    raise SystemExit(f"❌ Clone failed:\n{err_clean[:300]}")
_log("OK", f"Cloned {REPO_NAME} to {BASE_DIR}")

_log("STEP", "Installing Python packages…")
subprocess.run(
    [sys.executable, "-m", "pip", "uninstall", "-q", "-y", "pyrogram"],
    capture_output=True,
)
subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q",
     "-r", f"{BASE_DIR}/requirements.txt"],
    check=True,
)
_log("OK", "Python packages installed")

_log("STEP", "Starting aria2c daemon…")
subprocess.Popen(
    "aria2c --enable-rpc --rpc-listen-all=true --rpc-allow-origin-all "
    "--max-connection-per-server=16 --split=16 --seed-time=0 --daemon 2>/dev/null",
    shell=True,
)
time.sleep(2)
_log("OK", "aria2c started")

# FIX (BUG 9): inject ALL resolved credentials directly into Popen env so they
# override any stale values that may already be set in os.environ.
# This also ensures load_dotenv(override=True) in config.py sees the right values.
env_lines = [
    f"API_ID={API_ID}",
    f"API_HASH={API_HASH}",
    f"BOT_TOKEN={BOT_TOKEN}",
    f"OWNER_ID={OWNER_ID}",
    f"FILE_LIMIT_MB={FILE_LIMIT_MB}",
    f"LOG_CHANNEL={LOG_CHANNEL}",
    "DOWNLOAD_DIR=/tmp/zilong_dl",
    "ARIA2_HOST=http://localhost",
    "ARIA2_PORT=6800",
    "ARIA2_SECRET=",
    f"NGROK_TOKEN={NGROK_TOKEN}",
    f"CC_WEBHOOK_SECRET={CC_WEBHOOK_SECRET}",
    f"CC_API_KEY={CC_API_KEY}",
    "UPLOAD_CONCURRENCY=3",
    "BOT_WORKERS=16",
    "UPLOAD_PARTS_PARALLEL=16",
]
for optional in ("ADMINS", "GDRIVE_SA_JSON", "ARIA2_SECRET"):
    val = _secret(optional)
    if val:
        env_lines.append(f"{optional}={val}")

with open(f"{BASE_DIR}/.env", "w") as f:
    f.write("\n".join(env_lines))

for sf in glob.glob(os.path.join(BASE_DIR, "*.session*")):
    try: os.remove(sf)
    except OSError: pass

_log("OK", "Environment configured (.env written)")

_log("STEP", "Applying thumbnail duration fix to uploader.py…")
try:
    import re as _re
    _up = os.path.join(BASE_DIR, "services", "uploader.py")
    _src = open(_up).read()
    _c = 0
    for _old, _new in [
        ('"-v", "error",',                        '"-v", "quiet",'),
        ('"-show_entries", "format=duration",',    '"-show_streams", "-show_format",'),
        ('"-of", "default=noprint_wrappers=1:nokey=1",', '"-of", "json",'),
    ]:
        if _old in _src:
            _src = _src.replace(_old, _new, 1); _c += 1
    _OLD_DUR = 'duration = float(out_b.decode().strip() or "0")'
    _NEW_DUR = (
        "import json as _jj\n"
        "        _jdata = _jj.loads(out_b.decode(errors='replace') or '{}')\n"
        "        duration = 0\n"
        "        for _jst in _jdata.get('streams', []):\n"
        "            if _jst.get('codec_type') == 'video':\n"
        "                try: duration = int(float(_jst.get('duration', 0) or 0))\n"
        "                except: pass\n"
        "                for _jk in ('DURATION', 'DURATION-eng', 'DURATION-jpn'):\n"
        "                    _jv = (_jst.get('tags') or {}).get(_jk, '')\n"
        "                    if not duration and _jv and ':' in str(_jv):\n"
        "                        try:\n"
        "                            _jp = str(_jv).split(':')\n"
        "                            duration = int(float(_jp[0]))*3600+int(float(_jp[1]))*60+int(float(_jp[2].split('.')[0]))\n"
        "                        except: pass\n"
        "                    if duration: break\n"
        "                break\n"
        "        if not duration:\n"
        "            try: duration = int(float(_jdata.get('format', {}).get('duration') or 0))\n"
        "            except: pass"
    )
    if _OLD_DUR in _src:
        _src = _src.replace(_OLD_DUR, _NEW_DUR, 1); _c += 1
    if _c:
        open(_up, "w").write(_src)
        _log("OK", f"Thumbnail fix applied ({_c} replacements)")
    else:
        _log("OK", "Thumbnail fix already present")
except Exception as _e:
    _log("WARN", f"Patch failed: {_e}")

os.chdir(BASE_DIR)

# ── Keep Colab alive ──────────────────────────────────────────
_log("STEP", "Activating Colab keep-alive…")
try:
    from IPython.display import display, Javascript
    display(Javascript('''
    function ColabKeepAlive() {
        document.querySelector("#top-toolbar .colab-connect-button")?.click();
        document.querySelector("colab-connect-button")?.shadowRoot
            ?.querySelector("#connect")?.click();
        document.querySelector("#ok")?.click();
    }
    setInterval(ColabKeepAlive, 60000);
    console.log("Colab keep-alive: clicking connect every 60s");
    '''))
    _log("OK", "JS keep-alive injected (clicks connect every 60s)")
except Exception:
    _log("WARN", "Not in Colab notebook — JS keep-alive skipped")

import threading

def _heartbeat():
    while True:
        time.sleep(300)
        ts = datetime.now().strftime("%H:%M")
        print(f"[{ts}] 💓", end="", flush=True)

_hb = threading.Thread(target=_heartbeat, daemon=True)
_hb.start()
_log("OK", "Heartbeat thread started (every 5 min)")

_log("OK", "Starting bot…\n" + "─" * 50)

MAX_RESTARTS = 50

restart_count = 0

# FIX (BUG 9): inject credentials directly into subprocess env so they survive
# even if the subprocess re-reads os.environ and load_dotenv hasn't fired yet
_bot_env = {**os.environ, "PYTHONUNBUFFERED": "1"}
for line in env_lines:
    if "=" in line:
        k, _, v = line.partition("=")
        _bot_env[k] = v

while restart_count < MAX_RESTARTS:
    t_start = datetime.now()
    proc = subprocess.Popen(
        [sys.executable, "-u", "main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True, bufsize=1,
        env=_bot_env,
    )
    for line in proc.stdout:
        print(line, end="", flush=True)
    proc.wait()

    elapsed = (datetime.now() - t_start).seconds
    if proc.returncode == 0:
        _log("OK", "Bot stopped cleanly.")
        break

    if elapsed > 300:
        restart_count = 0

    restart_count += 1
    _log("WARN", f"Crashed (exit={proc.returncode}) after {elapsed}s  [{restart_count}/{MAX_RESTARTS}]")
    if restart_count >= MAX_RESTARTS:
        _log("ERR", "Too many restarts — stopping.")
        break
    wait = min(5 * restart_count, 30)
    _log("WARN", f"Restarting in {wait}s…")
    time.sleep(wait)
