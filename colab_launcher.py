# @title ⚡ Zilong Bot — Colab Launcher
# @markdown ## Credentials
# @markdown
# @markdown **Recommended:** Add secrets via the 🔑 icon in the left panel:
# @markdown - `API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID`

API_ID    = 0      # @param {type:"integer"}
API_HASH  = ""     # @param {type:"string"}
BOT_TOKEN = ""     # @param {type:"string"}
OWNER_ID  = 0      # @param {type:"integer"}

FILE_LIMIT_MB = 2048   # @param {type:"integer"}
LOG_CHANNEL   = 0      # @param {type:"integer"}

import os, sys, subprocess, shutil, time, glob
from datetime import datetime

REPO_URL = "https://github.com/vicMenma/Zilong_multiusage.git"
BASE_DIR = "/content/zilong"


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
r = subprocess.run(["git", "clone", "--depth=1", REPO_URL, BASE_DIR],
                   capture_output=True, text=True)
if r.returncode != 0:
    raise SystemExit(f"❌ Clone failed:\n{r.stderr[:300]}")
_log("OK", f"Cloned to {BASE_DIR}")

_log("STEP", "Installing Python packages…")
# Remove stock pyrogram before installing pyrofork — both expose the same
# `pyrogram` namespace. If both are installed, stock pyrogram wins and
# pyrofork-specific internals won't be available.
subprocess.run(
    [sys.executable, "-m", "pip", "uninstall", "-q", "-y", "pyrogram"],
    capture_output=True,  # silence "not installed" warnings
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
    # ── Upload speed tuning ───────────────────────────────────────────────
    # UPLOAD_CONCURRENCY: simultaneous independent file uploads (was 1 = sequential)
    "UPLOAD_CONCURRENCY=3",
    # BOT_WORKERS: pyrofork dispatcher thread pool size (default 4 → 16)
    "BOT_WORKERS=16",
    # UPLOAD_PARTS_PARALLEL: concurrent 512KB MTProto parts per upload (default 1 → 8)
    # 8 parts × 512KB / ~100ms RTT ≈ 40 MB/s theoretical — real ~10-25 MB/s on Colab
    "UPLOAD_PARTS_PARALLEL=8",
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
_log("OK", "Starting bot…\n" + "─" * 50)

MAX_RESTARTS = 10
restart_count = 0

while restart_count < MAX_RESTARTS:
    t_start = datetime.now()
    proc = subprocess.Popen(
        [sys.executable, "-u", "main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    for line in proc.stdout:
        print(line, end="", flush=True)
    proc.wait()

    elapsed = (datetime.now() - t_start).seconds
    if proc.returncode == 0:
        _log("OK", "Bot stopped cleanly.")
        break

    restart_count += 1
    _log("WARN", f"Crashed (exit={proc.returncode}) after {elapsed}s  [{restart_count}/{MAX_RESTARTS}]")
    if restart_count >= MAX_RESTARTS:
        _log("ERR", "Too many restarts — stopping.")
        break
    wait = min(5 * restart_count, 30)
    _log("WARN", f"Restarting in {wait}s…")
    time.sleep(wait)
