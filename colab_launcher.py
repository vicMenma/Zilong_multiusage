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
elif NGROK_TOKEN:    _log("OK", "NGROK_TOKEN set (ngrok fallback enabled)")
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


# ════════════════════════════════════════════════════════════════════════════
# POST-CLONE PATCHES — applied every run after clone
#
# WHY: The git clone gets the latest GitHub commit. These patches fix bugs
# that are structural (aria2p API misuse) and must be applied regardless
# of what's in the repo.
#
# PATCH A — url_handler.py: _probe_magnet_file
#   Root cause of the 30 MB bug:
#   Old code used aria2p API with TWO separate magnet adds:
#     dl  = api.add_magnet(magnet, {"bt-metadata-only": "true"})  ← metadata fetch
#     dl2 = api.add_magnet(magnet, {"select-file": "1",
#                                    "follow-torrent": "mem"})    ← file download
#   With follow-torrent=mem, dl2 had no cached metadata → re-fetched trackers.
#   aria2c fired is_complete=True after metadata phase (~30 MB) → loop broke.
#   The actual file NEVER downloaded. PROBE_ENOUGH was irrelevant.
#   Fix: smart_download uses aria2c subprocess which handles metadata
#   transparently and waits for the real file to complete.
#
# PATCH B — stream_extractor.py: se_mag_cb action="file"
#   Same broken aria2p pattern — same fix.
# ════════════════════════════════════════════════════════════════════════════

import re as _patch_re
import ast as _patch_ast

_log("STEP", "Applying post-clone patches…")

# ── PATCH A: url_handler.py ───────────────────────────────────────────────

_URL_HANDLER = os.path.join(BASE_DIR, "plugins", "url_handler.py")

_NEW_PROBE_FUNC = '''async def _probe_magnet_file(magnet: str, uid: int, st) -> tuple:
    """
    Download complete magnet file then probe streams locally.
    Uses smart_download (aria2c subprocess) — NOT aria2p API.

    Root cause of 30 MB bug: aria2p 2-add pattern fires is_complete=True
    after metadata phase (~30 MB), never downloading the actual file.
    """
    from services import ffmpeg as FF
    from services.downloader import smart_download as _smart_dl
    from services.utils import largest_file, all_video_files, make_tmp, cleanup, human_size

    tmp = make_tmp(cfg.download_dir, uid)

    await safe_edit(st,
        "🧲 <b>Magnet — Downloading Complete File</b>\\n"
        "──────────────────────\\n\\n"
        "<i>Using aria2c subprocess for reliable full download.\\n"
        "Stream analysis will be 100% accurate.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        path_or_dir = await _smart_dl(
            magnet, tmp,
            user_id=uid,
            label="Magnet Probe",
            msg=st,
        )

        if os.path.isdir(path_or_dir):
            path = largest_file(path_or_dir)
            if not path:
                files = all_video_files(path_or_dir, min_bytes=0)
                path = files[0] if files else None
        else:
            path = path_or_dir

        if not path or not os.path.isfile(path):
            await safe_edit(st,
                "❌ <b>No file found after download.</b>",
                parse_mode=enums.ParseMode.HTML,
            )
            cleanup(tmp)
            return None, None, {}

        fname = os.path.basename(path)
        fsize = os.path.getsize(path)

        await safe_edit(st,
            f"🔍 <b>Probing streams…</b>\\n\\n"
            f"📄 <code>{fname[:50]}</code>\\n"
            f"💾 <code>{human_size(fsize)}</code>",
            parse_mode=enums.ParseMode.HTML,
        )

        sd, dur = await asyncio.gather(
            FF.probe_streams(path),
            FF.probe_duration(path),
        )
        return path, tmp, {"streams": sd, "duration": dur, "fname": fname}

    except Exception as exc:
        import logging as _lg
        _lg.getLogger(__name__).error("[MagnetProbe] %s", exc, exc_info=True)
        await safe_edit(st,
            f"❌ <b>Magnet download failed</b>\\n\\n<code>{str(exc)[:300]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        cleanup(tmp)
        return None, None, {}
'''

try:
    with open(_URL_HANDLER, "r", encoding="utf-8") as _f:
        _uh_src = _f.read()

    # Replace the entire _probe_magnet_file function
    # Pattern: from "async def _probe_magnet_file" to next top-level async def
    _fn_pat = _patch_re.compile(
        r"async def _probe_magnet_file\(.*?(?=\n(?:async def |def |class |# ─{10}))",
        _patch_re.DOTALL,
    )

    if _fn_pat.search(_uh_src):
        _uh_patched = _fn_pat.sub(_NEW_PROBE_FUNC.strip(), _uh_src, count=1)
    else:
        # Fallback: insert before _hardsub_magnet_dl
        _marker = "\nasync def _hardsub_magnet_dl("
        if _marker in _uh_src:
            _uh_patched = _uh_src.replace(
                _marker,
                "\n\n" + _NEW_PROBE_FUNC.strip() + "\n" + _marker,
                1,
            )
        else:
            _uh_patched = _uh_src
            _log("WARN", "PATCH A: _probe_magnet_file insertion point not found")

    # Verify syntax before writing
    try:
        _patch_ast.parse(_uh_patched)
        with open(_URL_HANDLER, "w", encoding="utf-8") as _f:
            _f.write(_uh_patched)
        _log("OK", "PATCH A: url_handler.py — _probe_magnet_file → smart_download ✅")
    except SyntaxError as _se:
        _log("ERR", f"PATCH A: syntax error, skipping — {_se}")

except Exception as _pe:
    _log("ERR", f"PATCH A failed: {_pe}")


# ── PATCH B: stream_extractor.py se_mag_cb action="file" ─────────────────

_STREAM_EXT = os.path.join(BASE_DIR, "plugins", "stream_extractor.py")

_NEW_SE_FILE_ACTION = '''        # PATCHED: use smart_download instead of broken aria2p select-file
        # Old code: api.add_magnet + follow-torrent=mem → stops at 30 MB (metadata phase)
        st = await cb.message.edit(
            f"🧲 Downloading <code>{fname[:50]}</code>…\\n"
            "<i>Full download — no 30 MB limit.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
        tmp = make_tmp(cfg.download_dir, uid)
        try:
            from services.downloader import smart_download as _se_dl
            _dl_path = await _se_dl(magnet, tmp, user_id=uid, label=fname, msg=st)
            if os.path.isdir(_dl_path):
                _resolved = largest_file(_dl_path)
                if not _resolved:
                    raise FileNotFoundError("No output file found in torrent download")
                _dl_path = _resolved
            elif not os.path.isfile(_dl_path):
                raise FileNotFoundError("No output file found")
        except Exception as exc:
            cleanup(tmp)
            return await safe_edit(st,
                f"❌ Download failed: <code>{exc}</code>",
                parse_mode=enums.ParseMode.HTML)

        await upload_file(client, st, _dl_path)
        cleanup(tmp)
        return'''

try:
    with open(_STREAM_EXT, "r", encoding="utf-8") as _f:
        _se_src = _f.read()

    # Find the broken aria2p block inside se_mag_cb action=="file"
    # It starts after the file_idx/fname lines and contains api.add_magnet
    _broken_marker = 'api.add_magnet(magnet, options'
    if _broken_marker in _se_src:
        # Find the action=="file" block containing it
        _block_pat = _patch_re.compile(
            r'(        st\s+=\s+await cb\.message\.edit\(\s*\n'
            r'.*?'
            r'api\.add_magnet\(magnet.*?'
            r'cleanup\(tmp\)\s*\n)',
            _patch_re.DOTALL,
        )
        _se_patched = _block_pat.sub(_NEW_SE_FILE_ACTION + "\n", _se_src, count=1)
        if _se_patched != _se_src:
            try:
                _patch_ast.parse(_se_patched)
                with open(_STREAM_EXT, "w", encoding="utf-8") as _f:
                    _f.write(_se_patched)
                _log("OK", "PATCH B: stream_extractor.py — se_mag_cb file action → smart_download ✅")
            except SyntaxError as _se2:
                _log("WARN", f"PATCH B: syntax error after patch, skipping — {_se2}")
        else:
            _log("INFO", "PATCH B: stream_extractor.py — pattern not found, may already be patched")
    else:
        _log("INFO", "PATCH B: stream_extractor.py — no aria2p pattern found (already patched or different version)")

except Exception as _pe:
    _log("ERR", f"PATCH B failed: {_pe}")


_log("OK", "Post-clone patches complete ✅")
# ════════════════════════════════════════════════════════════════════════════


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

# ── Open public tunnel for CloudConvert webhook (port 8765) ──────────────
import re as _re2

_tunnel_proc = None
_TUNNEL_TIMEOUT = 30


def _install_cloudflared() -> bool:
    if subprocess.run(["which", "cloudflared"], capture_output=True).returncode == 0:
        return True
    _log("INFO", "Installing cloudflared…")
    r = subprocess.run(
        "curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest"
        "/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared"
        " && chmod +x /usr/local/bin/cloudflared",
        shell=True, capture_output=True,
    )
    return r.returncode == 0


def _open_cloudflare_tunnel() -> str:
    global _tunnel_proc
    if not _install_cloudflared():
        _log("WARN", "cloudflared install failed")
        return ""
    try:
        proc = subprocess.Popen(
            ["cloudflared", "tunnel", "--url", "http://localhost:8765", "--no-autoupdate"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        _tunnel_proc = proc
        deadline = time.time() + _TUNNEL_TIMEOUT
        for line in proc.stdout:
            _log("INFO", f"[cloudflared] {line.rstrip()}")
            m = _re2.search(r"(https://[a-z0-9\-]+\.trycloudflare\.com)", line)
            if m:
                url = m.group(1).rstrip("/")
                _log("OK", f"Cloudflare tunnel active: {url}")
                return url
            if time.time() > deadline:
                break
        _log("WARN", f"cloudflared timed out after {_TUNNEL_TIMEOUT}s")
        return ""
    except Exception as exc:
        _log("WARN", f"cloudflared error: {exc}")
        return ""


def _open_ngrok_tunnel() -> str:
    global _tunnel_proc
    if not NGROK_TOKEN:
        return ""
    try:
        from pyngrok import ngrok as _ngrok, conf as _conf
        _conf.get_default().auth_token = NGROK_TOKEN
        tunnel = _ngrok.connect(8765, "http")
        url = tunnel.public_url
        if url.startswith("http://"):
            url = "https://" + url[7:]
        _log("OK", f"ngrok tunnel (pyngrok) active: {url}")
        return url.rstrip("/")
    except ImportError:
        _log("INFO", "pyngrok not installed — trying ngrok CLI")
    except Exception as exc:
        _log("WARN", f"pyngrok failed: {exc} — trying ngrok CLI")
    try:
        proc = subprocess.Popen(
            ["ngrok", "http", "8765", "--log=stdout", f"--authtoken={NGROK_TOKEN}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        _tunnel_proc = proc
        deadline = time.time() + _TUNNEL_TIMEOUT
        for line in proc.stdout:
            _log("INFO", f"[ngrok] {line.rstrip()}")
            m = _re2.search(r"(https://[a-z0-9\-]+\.ngrok[\-a-z\.io]+)", line)
            if m:
                url = m.group(1).rstrip("/")
                _log("OK", f"ngrok tunnel (CLI) active: {url}")
                return url
            if time.time() > deadline:
                break
        _log("WARN", f"ngrok CLI timed out after {_TUNNEL_TIMEOUT}s")
        return ""
    except FileNotFoundError:
        _log("WARN", "ngrok CLI not found")
        return ""
    except Exception as exc:
        _log("WARN", f"ngrok CLI error: {exc}")
        return ""


def _tunnel_watchdog() -> None:
    global _tunnel_proc
    while True:
        time.sleep(20)
        proc = _tunnel_proc
        if proc is None or proc.poll() is not None:
            _log("WARN", "[Tunnel] Dropped — reconnecting…")
            url = _open_cloudflare_tunnel() or (NGROK_TOKEN and _open_ngrok_tunnel()) or ""
            if url:
                _log("OK", f"[Tunnel] Restored: {url}")
            else:
                _log("WARN", "[Tunnel] Reconnect failed — will retry in 20s")


if CC_API_KEY and not WEBHOOK_BASE_URL:
    _log("STEP", "Opening Cloudflare tunnel for CloudConvert webhook (port 8765)…")
    WEBHOOK_BASE_URL = _open_cloudflare_tunnel()

    if not WEBHOOK_BASE_URL and NGROK_TOKEN:
        _log("STEP", "Cloudflare unavailable — trying ngrok…")
        WEBHOOK_BASE_URL = _open_ngrok_tunnel()

    if WEBHOOK_BASE_URL:
        _log("OK", f"Webhook tunnel active: {WEBHOOK_BASE_URL}/webhook/cloudconvert")
        env_lines = [
            ln if not ln.startswith("WEBHOOK_BASE_URL=") else f"WEBHOOK_BASE_URL={WEBHOOK_BASE_URL}"
            for ln in env_lines
        ]
        with open(f"{BASE_DIR}/.env", "w") as _f:
            _f.write("\n".join(env_lines))
        threading.Thread(target=_tunnel_watchdog, daemon=True, name="tunnel-watchdog").start()
        _log("OK", "Tunnel watchdog started (auto-reconnects on drop)")
    else:
        _log("WARN", "No tunnel available — webhook localhost-only (ccstatus poller active).")
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
