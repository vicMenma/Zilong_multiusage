# @title ⚡ Zilong Bot — Colab Launcher
# @markdown Fill in your credentials below, then Runtime → Run all.

# ── Credentials ───────────────────────────────────────────────
API_ID    = 0      # @param {type:"integer"}
API_HASH  = ""     # @param {type:"string"}
BOT_TOKEN = ""     # @param {type:"string"}
OWNER_ID  = 0      # @param {type:"integer"}

# ── Optional ──────────────────────────────────────────────────
FILE_LIMIT_MB = 2048   # @param {type:"integer"}
LOG_CHANNEL   = 0      # @param {type:"integer"}

# ─────────────────────────────────────────────────────────────
import os, sys, subprocess, shutil, time, glob
from datetime import datetime

REPO_URL = "https://github.com/vicMenma/Zilong_multiusage.git"
BASE_DIR = "/content/zilong"


def _log(level: str, msg: str):
    icons = {"INFO": "ℹ️", "OK": "✅", "WARN": "⚠️", "ERR": "❌", "STEP": "🔧"}
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {icons.get(level, '')} {msg}", flush=True)


# ── Validate credentials ──────────────────────────────────────
errors = []
if not API_ID:    errors.append("API_ID is required")
if not API_HASH:  errors.append("API_HASH is required")
if not BOT_TOKEN: errors.append("BOT_TOKEN is required")
if not OWNER_ID:  errors.append("OWNER_ID is required")
if errors:
    for e in errors:
        print(f"❌ {e}")
    raise SystemExit("Fill in all required credentials and run again.")

print("⚡ Zilong Bot — Colab Launcher")
print("─" * 50)

# ── System packages ───────────────────────────────────────────
_log("STEP", "Installing system packages…")
subprocess.run(
    "apt-get update -qq && "
    "apt-get install -y -qq ffmpeg aria2 mediainfo p7zip-full unrar 2>/dev/null",
    shell=True, capture_output=True,
)
_log("OK", "System packages ready")

# ── Clone / update repo ───────────────────────────────────────
_log("STEP", "Cloning repository…")
if os.path.exists(BASE_DIR):
    shutil.rmtree(BASE_DIR)
r = subprocess.run(["git", "clone", "--depth=1", REPO_URL, BASE_DIR],
                   capture_output=True, text=True)
if r.returncode != 0:
    raise SystemExit(f"❌ Clone failed:\n{r.stderr[:300]}")
_log("OK", f"Cloned to {BASE_DIR}")

# ── Python dependencies ───────────────────────────────────────
_log("STEP", "Installing Python packages…")
subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q",
     "-r", f"{BASE_DIR}/requirements.txt"],
    check=True,
)
_log("OK", "Python packages installed")

# ── aria2c daemon ─────────────────────────────────────────────
_log("STEP", "Starting aria2c daemon…")
subprocess.Popen(
    "aria2c --enable-rpc --rpc-listen-all=true --rpc-allow-origin-all "
    "--max-connection-per-server=16 --split=16 --seed-time=0 --daemon 2>/dev/null",
    shell=True,
)
time.sleep(2)
_log("OK", "aria2c started")

# ── Write .env ────────────────────────────────────────────────
env_content = "\n".join([
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
])
with open(f"{BASE_DIR}/.env", "w") as f:
    f.write(env_content)

# Remove any stale sessions
for sf in glob.glob(os.path.join(BASE_DIR, "*.session*")):
    try:
        os.remove(sf)
    except OSError:
        pass

_log("OK", "Environment configured")

# ── Run bot with auto-restart ─────────────────────────────────
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
        text=True,
        bufsize=1,
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
    _log("WARN", f"Crashed (exit={proc.returncode}) after {elapsed}s  "
                 f"[attempt {restart_count}/{MAX_RESTARTS}]")

    if restart_count >= MAX_RESTARTS:
        _log("ERR", "Too many restarts — stopping.")
        break

    wait = min(5 * restart_count, 30)   # back-off: 5s, 10s, 15s … max 30s
    _log("WARN", f"Restarting in {wait}s…")
    time.sleep(wait)
