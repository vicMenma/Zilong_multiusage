# @title ⚡ Zilong Bot — Colab Launcher
# @markdown Credentials go here OR in 🔑 Secrets panel (Secrets always win)

API_ID    = 0      # @param {type:"integer"}
API_HASH  = ""     # @param {type:"string"}
BOT_TOKEN = ""     # @param {type:"string"}
OWNER_ID  = 0      # @param {type:"integer"}

FILE_LIMIT_MB = 2048   # @param {type:"integer"}
LOG_CHANNEL   = 0      # @param {type:"integer"}

NGROK_TOKEN       = ""  # @param {type:"string"}
CC_WEBHOOK_SECRET = ""  # @param {type:"string"}
CC_API_KEY        = ""  # @param {type:"string"}
FC_API_KEY        = ""  # @param {type:"string"}  ← supports key1,key2,key3
SEEDR_USERNAME    = ""  # @param {type:"string"}
SEEDR_PASSWORD    = ""  # @param {type:"string"}
GITHUB_TOKEN      = ""  # @param {type:"string"}
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
FC_API_KEY        = _secret("FC_API_KEY")        or FC_API_KEY   # FIX: was missing
SEEDR_USERNAME    = _secret("SEEDR_USERNAME")    or SEEDR_USERNAME
SEEDR_PASSWORD    = _secret("SEEDR_PASSWORD")    or SEEDR_PASSWORD
GITHUB_TOKEN      = _secret("GITHUB_TOKEN")      or GITHUB_TOKEN
WEBHOOK_BASE_URL  = _secret("WEBHOOK_BASE_URL")  or WEBHOOK_BASE_URL

errors = []
if not API_ID:    errors.append("API_ID is required")
if not API_HASH:  errors.append("API_HASH is required")
if not BOT_TOKEN: errors.append("BOT_TOKEN is required")
if not OWNER_ID:  errors.append("OWNER_ID is required")
if errors:
    for e in errors: print(f"  ❌ {e}")
    raise SystemExit("Missing required credentials.")

_log("OK", f"API_ID={API_ID}  OWNER_ID={OWNER_ID}")
if CC_API_KEY: _log("OK", f"CC_API_KEY: {len(CC_API_KEY.split(','))} key(s)")
if FC_API_KEY: _log("OK", f"FC_API_KEY: {len(FC_API_KEY.split(','))} key(s)")
if WEBHOOK_BASE_URL: _log("OK", f"WEBHOOK_BASE_URL: {WEBHOOK_BASE_URL}")

_log("STEP", "Installing system packages…")
subprocess.run(
    "apt-get update -qq && apt-get install -y -qq ffmpeg aria2 mediainfo p7zip-full unrar 2>/dev/null",
    shell=True, capture_output=True,
)
_log("OK", "System packages ready")

_log("STEP", "Cloning repository…")
if os.path.exists(BASE_DIR):
    shutil.rmtree(BASE_DIR)

REPO_URL = (
    f"https://{GITHUB_TOKEN}@github.com/vicMenma/{REPO_NAME}.git"
    if GITHUB_TOKEN else
    f"https://github.com/vicMenma/{REPO_NAME}.git"
)
r = subprocess.run(["git", "clone", "--depth=1", REPO_URL, BASE_DIR], capture_output=True, text=True)
if r.returncode != 0:
    err_clean = r.stderr.replace(GITHUB_TOKEN, "***") if GITHUB_TOKEN else r.stderr
    raise SystemExit(f"❌ Clone failed:\n{err_clean[:300]}")
_log("OK", f"Cloned {REPO_NAME} → {BASE_DIR}")

# ── Python packages ───────────────────────────────────────────────────────
_log("STEP", "Installing Python packages…")
subprocess.run([sys.executable, "-m", "pip", "uninstall", "-q", "-y", "pyrogram"], capture_output=True)
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", f"{BASE_DIR}/requirements.txt"], check=True)
_log("OK", "Python packages installed")

# ── aria2c daemon ─────────────────────────────────────────────────────────
_log("STEP", "Starting aria2c daemon…")
subprocess.Popen("aria2c --enable-rpc --rpc-listen-all=true --rpc-allow-origin-all "
                 "--max-connection-per-server=16 --split=16 --seed-time=0 --daemon 2>/dev/null",
                 shell=True)
time.sleep(2)
_log("OK", "aria2c started")

# ── Write .env ────────────────────────────────────────────────────────────
env_lines = [
    f"API_ID={API_ID}", f"API_HASH={API_HASH}", f"BOT_TOKEN={BOT_TOKEN}", f"OWNER_ID={OWNER_ID}",
    f"FILE_LIMIT_MB={FILE_LIMIT_MB}", f"LOG_CHANNEL={LOG_CHANNEL}",
    "DOWNLOAD_DIR=/tmp/zilong_dl", "ARIA2_HOST=http://localhost", "ARIA2_PORT=6800", "ARIA2_SECRET=",
    f"NGROK_TOKEN={NGROK_TOKEN}", f"CC_WEBHOOK_SECRET={CC_WEBHOOK_SECRET}",
    f"CC_API_KEY={CC_API_KEY}",
    f"FC_API_KEY={FC_API_KEY}",   # FIX: was missing — FC features silently disabled without this
    f"SEEDR_USERNAME={SEEDR_USERNAME}", f"SEEDR_PASSWORD={SEEDR_PASSWORD}",
    f"WEBHOOK_BASE_URL={WEBHOOK_BASE_URL}",
]
for opt in ("ADMINS", "GDRIVE_SA_JSON"):
    val = _secret(opt)
    if val: env_lines.append(f"{opt}={val}")

with open(f"{BASE_DIR}/.env", "w") as f:
    f.write("\n".join(env_lines))
_log("OK", f".env written (CC_API_KEY={'set' if CC_API_KEY else 'empty'}, FC_API_KEY={'set' if FC_API_KEY else 'empty'})")

for sf in glob.glob(os.path.join(BASE_DIR, "*.session*")):
    try: os.remove(sf)
    except OSError: pass

os.chdir(BASE_DIR)

# ── Colab keep-alive ──────────────────────────────────────────────────────
try:
    from IPython.display import display, Javascript
    display(Javascript("""
    setInterval(function(){
        document.querySelector("#top-toolbar .colab-connect-button")?.click();
        document.querySelector("colab-connect-button")?.shadowRoot?.querySelector("#connect")?.click();
        document.querySelector("#ok")?.click();
    }, 60000);
    """))
    _log("OK", "JS keep-alive injected")
except Exception:
    _log("WARN", "Not in Colab — JS keep-alive skipped")

threading.Thread(target=lambda: [time.sleep(300) or print(f"\r[{datetime.now().strftime('%H:%M')}] 💓", end="", flush=True) for _ in iter(int, 1)], daemon=True).start()

# ── Kill stale PID ────────────────────────────────────────────────────────
_PID_FILE = "/tmp/zilong_bot.pid"
try:
    if os.path.exists(_PID_FILE):
        _old_pid = int(open(_PID_FILE).read().strip())
        try:
            import signal; os.kill(_old_pid, signal.SIGTERM); time.sleep(2)
            try: os.kill(_old_pid, signal.SIGKILL)
            except ProcessLookupError: pass
        except ProcessLookupError: pass
        os.remove(_PID_FILE)
except Exception: pass

# ── Tunnel ────────────────────────────────────────────────────────────────
import re as _re2
_tunnel_proc = None
_TUNNEL_TIMEOUT = 30


def _install_cloudflared():
    if subprocess.run(["which","cloudflared"], capture_output=True).returncode == 0: return True
    r = subprocess.run("curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared", shell=True, capture_output=True)
    return r.returncode == 0


def _open_cf():
    global _tunnel_proc
    if not _install_cloudflared(): return ""
    try:
        proc = subprocess.Popen(["cloudflared","tunnel","--url","http://localhost:8765","--no-autoupdate"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        _tunnel_proc = proc
        deadline = time.time() + _TUNNEL_TIMEOUT
        for line in proc.stdout:
            m = _re2.search(r"(https://[a-z0-9\-]+\.trycloudflare\.com)", line)
            if m: url = m.group(1).rstrip("/"); _log("OK", f"Cloudflare tunnel: {url}"); return url
            if time.time() > deadline: break
        return ""
    except Exception as exc:
        _log("WARN", f"cloudflared: {exc}"); return ""


def _open_ngrok():
    global _tunnel_proc
    if not NGROK_TOKEN: return ""
    try:
        from pyngrok import ngrok as _ng, conf as _conf
        _conf.get_default().auth_token = NGROK_TOKEN
        t = _ng.connect(8765, "http"); url = t.public_url
        if url.startswith("http://"): url = "https://" + url[7:]
        _log("OK", f"ngrok: {url}"); return url.rstrip("/")
    except Exception: pass
    try:
        proc = subprocess.Popen(["ngrok","http","8765","--log=stdout",f"--authtoken={NGROK_TOKEN}"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        _tunnel_proc = proc; deadline = time.time() + _TUNNEL_TIMEOUT
        for line in proc.stdout:
            m = _re2.search(r"(https://[a-z0-9\-]+\.ngrok[\-a-z\.io]+)", line)
            if m: url = m.group(1).rstrip("/"); _log("OK", f"ngrok CLI: {url}"); return url
            if time.time() > deadline: break
        return ""
    except Exception: return ""


def _watchdog():
    global _tunnel_proc
    while True:
        time.sleep(20)
        proc = _tunnel_proc
        if proc is None or proc.poll() is not None:
            _log("WARN", "[Tunnel] Dropped — reconnecting…")
            url = _open_cf() or (NGROK_TOKEN and _open_ngrok()) or ""
            if url: _log("OK", f"[Tunnel] Restored: {url}")


_has_any_key = bool(CC_API_KEY or FC_API_KEY)
if _has_any_key and not WEBHOOK_BASE_URL:
    _log("STEP", "Opening Cloudflare tunnel (port 8765)…")
    WEBHOOK_BASE_URL = _open_cf()
    if not WEBHOOK_BASE_URL and NGROK_TOKEN:
        _log("STEP", "Cloudflare unavailable — trying ngrok…")
        WEBHOOK_BASE_URL = _open_ngrok()
    if WEBHOOK_BASE_URL:
        _log("OK", f"Tunnel active: {WEBHOOK_BASE_URL}")
        env_lines = [ln if not ln.startswith("WEBHOOK_BASE_URL=") else f"WEBHOOK_BASE_URL={WEBHOOK_BASE_URL}" for ln in env_lines]
        with open(f"{BASE_DIR}/.env","w") as _f: _f.write("\n".join(env_lines))
        threading.Thread(target=_watchdog, daemon=True, name="tunnel-watchdog").start()
    else:
        _log("WARN", "No tunnel — poller compensates")
elif WEBHOOK_BASE_URL:
    _log("OK", f"Using WEBHOOK_BASE_URL: {WEBHOOK_BASE_URL}")

# ── Bot restart loop ──────────────────────────────────────────────────────
_log("OK", "Starting bot…\n" + "─"*50)

MAX_RESTARTS = 50; restart_count = 0
_bot_env = {**os.environ, "PYTHONUNBUFFERED": "1"}
for line in env_lines:
    if "=" in line and not line.startswith("#"):
        k, _, v = line.partition("="); _bot_env[k] = v

import re as _re
_FLOOD_RE = _re.compile(r"(?:FLOOD_WAIT_SECONDS=(\d+)|A wait of (\d+) seconds is required)")

def _parse_flood(lines):
    for ln in reversed(lines):
        m = _FLOOD_RE.search(ln)
        if m: return int(m.group(1) or m.group(2))
    return 0

while restart_count < MAX_RESTARTS:
    t_start = datetime.now()
    proc = subprocess.Popen([sys.executable,"-u","main.py"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=_bot_env)
    captured = []
    for line in proc.stdout:
        print(line, end="", flush=True); captured.append(line)
    proc.wait()
    try: proc.kill()
    except Exception: pass
    elapsed = (datetime.now()-t_start).seconds
    if proc.returncode == 0: _log("OK","Bot stopped cleanly."); break
    if elapsed > 300: restart_count = 0
    restart_count += 1
    _log("WARN", f"Crashed (exit={proc.returncode}) after {elapsed}s [{restart_count}/{MAX_RESTARTS}]")
    if restart_count >= MAX_RESTARTS: _log("ERR","Too many restarts."); break
    fw = _parse_flood(captured)
    if fw > 0: _log("WARN",f"FloodWait {fw+5}s"); time.sleep(fw+5)
    else:
        wait = min(5*restart_count, 30); _log("WARN",f"Restarting in {wait}s…"); time.sleep(wait)
