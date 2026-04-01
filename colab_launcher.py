# @title ⚡ Zilong Bot — Colab Launcher
# @markdown ## Credentials
# @markdown
# @markdown **Recommended:** Store credentials in the 🔑 Secrets panel (left sidebar)
# @markdown and toggle **"Notebook access"** ON for each one.
# @markdown The form fields below are a fallback — Secrets always win.
# @markdown
# @markdown | Secret | Required | Example |
# @markdown |---|---|---|
# @markdown | `API_ID` | ✅ | `12345678` |
# @markdown | `API_HASH` | ✅ | `abcdef…` |
# @markdown | `BOT_TOKEN` | ✅ | `123456:ABC…` |
# @markdown | `OWNER_ID` | ✅ | `987654321` |
# @markdown | `GITHUB_TOKEN` | private repo | PAT with `repo` scope |
# @markdown | `CC_API_KEY` | hardsub | CloudConvert key |
# @markdown | `NGROK_TOKEN` | webhook (legacy) | ngrok authtoken |
# @markdown | `WEBHOOK_BASE_URL` | webhook | auto-set by launcher via Serveo; override for VPS |

API_ID    = 0      # @param {type:"integer"}
API_HASH  = ""     # @param {type:"string"}
BOT_TOKEN = ""     # @param {type:"string"}
OWNER_ID  = 0      # @param {type:"integer"}

FILE_LIMIT_MB = 2048   # @param {type:"integer"}
LOG_CHANNEL   = 0      # @param {type:"integer"}

NGROK_TOKEN       = ""  # @param {type:"string"}
CC_WEBHOOK_SECRET = ""  # @param {type:"string"}
CC_API_KEY        = ""  # @param {type:"string"}
GITHUB_TOKEN      = ""  # @param {type:"string"}
# Set manually only for VPS/EC2 — on Colab this is auto-filled via Serveo tunnel
WEBHOOK_BASE_URL  = ""  # @param {type:"string"}

# ─────────────────────────────────────────────────────────────────────────────
import os, sys, subprocess, shutil, time, glob, threading
from datetime import datetime

REPO_NAME = "Zilong_multiusage"
BASE_DIR  = "/content/zilong"


def _log(level: str, msg: str) -> None:
    icons = {"INFO": "ℹ️", "OK": "✅", "WARN": "⚠️", "ERR": "❌", "STEP": "🔧"}
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {icons.get(level, '')} {msg}", flush=True)


def _secret(name: str) -> str:
    """Colab Secrets → env var fallback."""
    try:
        from google.colab import userdata
        val = userdata.get(name)
        if val:
            return str(val).strip()
    except Exception:
        pass
    return os.environ.get(name, "").strip()


def _secret_int(name: str, default: int = 0) -> int:
    try:
        return int(_secret(name) or default)
    except (ValueError, TypeError):
        return default


# ── Resolve credentials — Secrets always win over form params ─────────────
print("⚡ Zilong Bot — Colab Launcher")
print("─" * 50)
_log("STEP", "Resolving credentials…")

API_ID            = _secret_int("API_ID")    or API_ID
API_HASH          = _secret("API_HASH")      or API_HASH
BOT_TOKEN         = _secret("BOT_TOKEN")     or BOT_TOKEN
OWNER_ID          = _secret_int("OWNER_ID")  or OWNER_ID
FILE_LIMIT_MB     = _secret_int("FILE_LIMIT_MB") or FILE_LIMIT_MB or 2048
LOG_CHANNEL       = _secret_int("LOG_CHANNEL")   or LOG_CHANNEL
NGROK_TOKEN       = _secret("NGROK_TOKEN") or _secret("NGROK_AUTHTOKEN") or NGROK_TOKEN
CC_WEBHOOK_SECRET = _secret("CC_WEBHOOK_SECRET") or CC_WEBHOOK_SECRET
CC_API_KEY        = _secret("CC_API_KEY")        or CC_API_KEY
GITHUB_TOKEN      = _secret("GITHUB_TOKEN")      or GITHUB_TOKEN
WEBHOOK_BASE_URL  = _secret("WEBHOOK_BASE_URL")  or WEBHOOK_BASE_URL

errors = []
if not API_ID:    errors.append("API_ID is required")
if not API_HASH:  errors.append("API_HASH is required")
if not BOT_TOKEN: errors.append("BOT_TOKEN is required")
if not OWNER_ID:  errors.append("OWNER_ID is required")
if errors:
    print()
    for e in errors:
        print(f"  ❌ {e}")
    print()
    print("👆 Add missing secrets via the 🔑 panel (left sidebar).")
    raise SystemExit("Missing required credentials.")

_log("OK", f"API_ID={API_ID}  OWNER_ID={OWNER_ID}")
if WEBHOOK_BASE_URL: _log("OK", f"CloudConvert webhook URL set: {WEBHOOK_BASE_URL}")
elif NGROK_TOKEN:    _log("OK", "NGROK_TOKEN set (legacy — Serveo tunnel preferred)")
if CC_API_KEY:       _log("OK", "CloudConvert hardsub enabled (CC_API_KEY set)")
if GITHUB_TOKEN:     _log("OK", "GitHub token set — will clone private repo")

# ── System packages ───────────────────────────────────────────────────────
_log("STEP", "Installing system packages…")
subprocess.run(
    "apt-get update -qq && "
    "apt-get install -y -qq ffmpeg aria2 mediainfo p7zip-full unrar 2>/dev/null",
    shell=True, capture_output=True,
)
_log("OK", "System packages ready")

# ── Clone ─────────────────────────────────────────────────────────────────
_log("STEP", "Cloning repository…")
if os.path.exists(BASE_DIR):
    shutil.rmtree(BASE_DIR)

REPO_URL = (
    f"https://{GITHUB_TOKEN}@github.com/vicMenma/{REPO_NAME}.git"
    if GITHUB_TOKEN
    else f"https://github.com/vicMenma/{REPO_NAME}.git"
)
r = subprocess.run(
    ["git", "clone", "--depth=1", REPO_URL, BASE_DIR],
    capture_output=True, text=True,
)
if r.returncode != 0:
    err_clean = r.stderr.replace(GITHUB_TOKEN, "***") if GITHUB_TOKEN else r.stderr
    raise SystemExit(f"❌ Clone failed:\n{err_clean[:300]}")
_log("OK", f"Cloned {REPO_NAME} → {BASE_DIR}")

# ── Python packages ───────────────────────────────────────────────────────
_log("STEP", "Installing Python packages…")
subprocess.run(
    [sys.executable, "-m", "pip", "uninstall", "-q", "-y", "pyrogram"],
    capture_output=True,
)
subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", "-r", f"{BASE_DIR}/requirements.txt"],
    check=True,
)
_log("OK", "Python packages installed")

# ── aria2c daemon ─────────────────────────────────────────────────────────
_log("STEP", "Starting aria2c daemon…")
subprocess.Popen(
    "aria2c --enable-rpc --rpc-listen-all=true --rpc-allow-origin-all "
    "--max-connection-per-server=16 --split=16 --seed-time=0 --daemon 2>/dev/null",
    shell=True,
)
time.sleep(2)
_log("OK", "aria2c started")

# ── Write .env ────────────────────────────────────────────────────────────
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
    f"WEBHOOK_BASE_URL={WEBHOOK_BASE_URL}",
]
for optional in ("ADMINS", "GDRIVE_SA_JSON"):
    val = _secret(optional)
    if val:
        env_lines.append(f"{optional}={val}")

with open(f"{BASE_DIR}/.env", "w") as f:
    f.write("\n".join(env_lines))

for sf in glob.glob(os.path.join(BASE_DIR, "*.session*")):
    try:
        os.remove(sf)
    except OSError:
        pass

_log("OK", "Environment configured (.env written)")

os.chdir(BASE_DIR)

# ── Colab keep-alive ──────────────────────────────────────────────────────
_log("STEP", "Activating Colab keep-alive…")
try:
    from IPython.display import display, Javascript
    display(Javascript("""
    function ColabKeepAlive() {
        document.querySelector("#top-toolbar .colab-connect-button")?.click();
        document.querySelector("colab-connect-button")?.shadowRoot
            ?.querySelector("#connect")?.click();
        document.querySelector("#ok")?.click();
    }
    setInterval(ColabKeepAlive, 60000);
    console.log("Colab keep-alive: clicking connect every 60s");
    """))
    _log("OK", "JS keep-alive injected (clicks connect every 60s)")
except Exception:
    _log("WARN", "Not in Colab notebook — JS keep-alive skipped")


def _heartbeat() -> None:
    while True:
        time.sleep(300)
        print(f"\r[{datetime.now().strftime('%H:%M')}] 💓", end="", flush=True)


threading.Thread(target=_heartbeat, daemon=True).start()
_log("OK", "Heartbeat thread started (every 5 min)")

# ── Kill any stale bot instances before starting ──────────────────────────
_log("STEP", "Checking for stale bot instances…")
_PID_FILE = "/tmp/zilong_bot.pid"
try:
    if os.path.exists(_PID_FILE):
        _old_pid = int(open(_PID_FILE).read().strip())
        try:
            import signal
            os.kill(_old_pid, signal.SIGTERM)
            time.sleep(2)
            try:
                os.kill(_old_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            _log("WARN", f"Killed stale bot process (PID {_old_pid})")
        except ProcessLookupError:
            pass
        os.remove(_PID_FILE)
except Exception as _ke:
    _log("WARN", f"Could not clean up stale PID: {_ke}")

# ── Open Serveo tunnel for CloudConvert webhook (port 8765) ──────────────
# Only runs when CC_API_KEY is set and WEBHOOK_BASE_URL is not already
# provided (e.g. manually set for VPS deployments).
def _open_webhook_tunnel() -> str:
    """
    SSH reverse-tunnel: Serveo public HTTPS → localhost:8765
    Returns the public base URL (e.g. https://xyz.serveo.net) or '' on failure.
    """
    import re as _re2
    try:
        proc = subprocess.Popen(
            [
                "ssh", "-o", "StrictHostKeyChecking=no",
                "-o", "ServerAliveInterval=30",
                "-R", "80:localhost:8765",
                "serveo.net",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 20
        for line in proc.stdout:
            m = _re2.search(r"(https://\S+\.serveo\.net)", line)
            if m:
                return m.group(1).rstrip("/")
            if time.time() > deadline:
                break
        _log("WARN", "Serveo webhook tunnel timed out — webhook unavailable.")
        return ""
    except Exception as exc:
        _log("WARN", f"Serveo webhook tunnel failed: {exc}")
        return ""


if CC_API_KEY and not WEBHOOK_BASE_URL:
    _log("STEP", "Opening Serveo tunnel for CloudConvert webhook (port 8765)…")
    WEBHOOK_BASE_URL = _open_webhook_tunnel()
    if WEBHOOK_BASE_URL:
        _log("OK", f"Webhook tunnel active: {WEBHOOK_BASE_URL}/webhook/cloudconvert")
        # Patch env_lines and .env so the bot picks it up
        env_lines = [
            ln if not ln.startswith("WEBHOOK_BASE_URL=") else f"WEBHOOK_BASE_URL={WEBHOOK_BASE_URL}"
            for ln in env_lines
        ]
        with open(f"{BASE_DIR}/.env", "w") as _f:
            _f.write("\n".join(env_lines))
    else:
        _log("WARN", "No webhook URL — falling back to ccstatus poller (~5 s lag).")
elif WEBHOOK_BASE_URL:
    _log("OK", f"Using provided WEBHOOK_BASE_URL: {WEBHOOK_BASE_URL}")

# ── Bot restart loop ──────────────────────────────────────────────────────
_log("OK", "Starting bot…\n" + "─" * 50)

MAX_RESTARTS  = 50
restart_count = 0

_bot_env = {**os.environ, "PYTHONUNBUFFERED": "1"}
for line in env_lines:
    if "=" in line and not line.startswith("#"):
        k, _, v = line.partition("=")
        _bot_env[k] = v

import re as _re
_FLOOD_RE = _re.compile(
    r"(?:FLOOD_WAIT_SECONDS=(\d+)"
    r"|A wait of (\d+) seconds is required"
    r")"
)

def _parse_flood_wait(output_lines: list) -> int:
    for ln in reversed(output_lines):
        m = _FLOOD_RE.search(ln)
        if m:
            return int(m.group(1) or m.group(2))
    return 0


while restart_count < MAX_RESTARTS:
    t_start = datetime.now()
    proc = subprocess.Popen(
        [sys.executable, "-u", "main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True, bufsize=1,
        env=_bot_env,
    )

    captured_lines: list = []
    for line in proc.stdout:
        print(line, end="", flush=True)
        captured_lines.append(line)
    proc.wait()

    try:
        proc.kill()
    except Exception:
        pass

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

    flood_wait = _parse_flood_wait(captured_lines)
    if flood_wait > 0:
        _log("WARN", f"FloodWait detected — waiting {flood_wait + 5}s before restart…")
        time.sleep(flood_wait + 5)
    else:
        wait = min(5 * restart_count, 30)
        _log("WARN", f"Restarting in {wait}s…")
        time.sleep(wait)
