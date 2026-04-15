# @title ⚡ Zilong Bot — Colab Launcher
# @markdown Credentials go here OR in 🔑 Secrets panel (Secrets always win)
# @markdown
# @markdown | Secret | Required |
# @markdown |--------|----------|
# @markdown | `API_ID` | ✅ |
# @markdown | `API_HASH` | ✅ |
# @markdown | `BOT_TOKEN` | ✅ |
# @markdown | `OWNER_ID` | ✅ |
# @markdown | `CC_API_KEY` | CloudConvert hardsub |
# @markdown | `FC_API_KEY` | FreeConvert convert/compress (comma-sep for multiple) |
# @markdown | `GITHUB_TOKEN` | private repo |

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

# ════════════════════════════════════════════════════════════════════════════
# POST-CLONE FILE INJECTION + PATCHES
# ════════════════════════════════════════════════════════════════════════════

import re as _patch_re
import ast as _patch_ast

_log("STEP", "Injecting critical service files…")


def _write_file(rel_path: str, content: str, label: str) -> None:
    full = os.path.join(BASE_DIR, rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    try:
        _patch_ast.parse(content)
    except SyntaxError as e:
        _log("ERR", f"{label}: syntax error — {e}")
        return
    with open(full, "w", encoding="utf-8") as f:
        f.write(content)
    _log("OK", f"✅ {rel_path} ({label})")


def _patch_file(rel_path: str, old: str, new: str, label: str) -> bool:
    full = os.path.join(BASE_DIR, rel_path)
    try:
        with open(full, "r", encoding="utf-8") as f:
            src = f.read()
    except FileNotFoundError:
        _log("WARN", f"{label}: not found")
        return False
    if old not in src:
        if new in src:
            _log("INFO", f"{label}: already applied")
            return True
        _log("WARN", f"{label}: pattern not found")
        return False
    patched = src.replace(old, new, 1)
    try:
        _patch_ast.parse(patched)
    except SyntaxError as e:
        _log("ERR", f"{label}: syntax error — {e}")
        return False
    with open(full, "w", encoding="utf-8") as f:
        f.write(patched)
    _log("OK", f"✅ {label}")
    return True


# ── INJECT 1: services/fc_job_store.py ────────────────────────────────────
_write_file("services/fc_job_store.py", """\
from __future__ import annotations
import asyncio, json, logging, os, time
from dataclasses import asdict, dataclass, field
from typing import Optional

log = logging.getLogger(__name__)
_STORE_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data", "fc_jobs.json"))
_JOB_TTL = 48 * 3600


@dataclass
class FCJob:
    job_id: str; uid: int; fname: str; output_name: str
    status: str = "processing"; job_type: str = "hardsub"
    sub_fname: str = ""; api_key: str = ""
    created_at: float = field(default_factory=time.time); error: str = ""


class FCJobStore:
    def __init__(self, path=_STORE_PATH):
        self._path = path; self._jobs: dict[str, FCJob] = {}; self._lock = asyncio.Lock()

    async def load(self):
        async with self._lock:
            try:
                with open(self._path, encoding="utf-8") as fh:
                    self._jobs = {k: FCJob(**v) for k, v in json.load(fh).items()}
                log.info("[FC-Store] Loaded %d job(s)", len(self._jobs))
            except FileNotFoundError:
                self._jobs = {}
            except Exception as exc:
                log.warning("[FC-Store] Load error: %s", exc); self._jobs = {}
            now = time.time()
            expired = [k for k, v in self._jobs.items() if v.created_at < now - _JOB_TTL]
            for k in expired: del self._jobs[k]

    async def _save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        try:
            with open(self._path + ".tmp", "w", encoding="utf-8") as fh:
                json.dump({k: asdict(v) for k, v in self._jobs.items()}, fh, indent=2)
            os.replace(self._path + ".tmp", self._path)
        except Exception as exc:
            log.error("[FC-Store] Save error: %s", exc)

    async def add(self, job: FCJob):
        async with self._lock: self._jobs[job.job_id] = job; await self._save()

    async def get(self, job_id: str) -> Optional[FCJob]:
        async with self._lock: return self._jobs.get(job_id)

    async def update(self, job_id: str, **kw) -> Optional[FCJob]:
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job: return None
            [setattr(job, k, v) for k, v in kw.items() if hasattr(job, k)]
            await self._save(); return job

    async def remove(self, job_id: str):
        async with self._lock:
            if job_id in self._jobs: del self._jobs[job_id]; await self._save()

    async def list_by_uid(self, uid: int) -> list[FCJob]:
        async with self._lock:
            return sorted([j for j in self._jobs.values() if j.uid == uid], key=lambda j: j.created_at, reverse=True)

    async def list_processing(self) -> list[FCJob]:
        async with self._lock: return [j for j in self._jobs.values() if j.status == "processing"]

    async def count(self) -> int:
        async with self._lock: return len(self._jobs)


fc_job_store = FCJobStore()
""", "fc_job_store.py (NEW)")


# ── INJECT 2: plugins/fc_webhook.py ───────────────────────────────────────
_write_file("plugins/fc_webhook.py", """\
from __future__ import annotations
import asyncio, logging, os
from aiohttp import web
from pyrogram import enums
from services.fc_job_store import fc_job_store

log = logging.getLogger(__name__)


async def handle_fc_webhook(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception as exc:
        log.warning("[FC-WH] Bad JSON: %s", exc); return web.Response(status=400, text="bad json")
    data = payload.get("data") or payload
    job_id = data.get("id", ""); status = (data.get("status") or "").lower()
    if not job_id: return web.Response(status=200, text="ok")
    log.info("[FC-WH] job=%s status=%s", job_id, status)
    asyncio.create_task(_process_webhook(job_id, status, data))
    return web.Response(status=200, text="ok")


async def _process_webhook(job_id, status, data):
    job = await fc_job_store.get(job_id)
    if not job: return
    if status == "processing": return
    if status in ("failed", "error", "cancelled"):
        err = data.get("message") or f"Job {status}"
        await fc_job_store.update(job_id, status="failed", error=err[:200])
        await _notify_failure(job, err)
    elif status == "completed":
        await fc_job_store.update(job_id, status="completed")
        await _handle_completion(job, data)


async def _handle_completion(job, data):
    from services.downloader import download_direct
    from services.uploader import upload_file
    from services.utils import cleanup, make_tmp, human_size
    from core.config import cfg
    url = _extract_url(data)
    if not url:
        await _notify_failure(job, "Completed but no output URL"); return
    tmp = make_tmp(cfg.download_dir, job.uid)
    try:
        path = await download_direct(url, tmp)
        fname = job.output_name or os.path.basename(path)
        if os.path.basename(path) != fname:
            try: os.rename(path, os.path.join(tmp, fname)); path = os.path.join(tmp, fname)
            except OSError: pass
        client = _get_client()
        if client:
            lbl = {"hardsub":"🔥 Hardsub","convert":"🔄 Convert","compress":"📐 Compress"}.get(job.job_type,"✅")
            st = await _fw_send(client, job.uid,
                f"{lbl} <b>done!</b>\\n<code>{job.fname[:42]}</code>\\n"
                f"<code>{human_size(os.path.getsize(path))}</code>\\n⬆️ Uploading…")
            await upload_file(client, st, path, user_id=job.uid)
    except Exception as exc:
        log.error("[FC-WH] %s", exc)
        await _notify_failure(job, str(exc)[:200])
    finally:
        cleanup(tmp)
    await fc_job_store.remove(job.job_id)


async def _notify_failure(job, msg):
    client = _get_client()
    if client:
        try:
            await _fw_send(client, job.uid,
                f"❌ <b>FreeConvert failed</b>\\n<code>{job.fname[:42]}</code>\\n<code>{msg[:300]}</code>")
        except Exception: pass


def _extract_url(data):
    for task in (data.get("tasks") or []):
        op = (task.get("operation") or task.get("name") or "").lower()
        if "export" in op and (task.get("status") or "").lower() == "completed":
            result = task.get("result") or {}
            for key in ("files","output","outputs"):
                files = result.get(key) or []
                if isinstance(files, list) and files: return files[0].get("url","")
    return ""


def _get_client():
    try:
        from core.session import get_client; return get_client()
    except Exception: return None


async def _fw_send(client, uid, text, retries=5):
    from pyrogram.errors import FloodWait
    for i in range(retries):
        try:
            return await client.send_message(uid, text, parse_mode=enums.ParseMode.HTML, disable_web_page_preview=True)
        except FloodWait as fw:
            if i < retries-1: await asyncio.sleep(min(fw.value+2, 90))
            else: raise
        except Exception: raise


async def startup_load():
    await fc_job_store.load()
    _log_count = await fc_job_store.count()
    log.info("[FC-WH] Store ready — %d job(s)", _log_count)
""", "fc_webhook.py (NEW)")


# ── INJECT 3: services/webhook_sync.py ────────────────────────────────────
_write_file("services/webhook_sync.py", """\
\"\"\"
services/webhook_sync.py
BUG-WS-04 FIX: cc_path default was /webhook, correct is /webhook/cloudconvert
BUG-WS-01 FIX: active_jobs() is sync — no await
BUG-WS-02 FIX: import from services.cloudconvert_hook
BUG-WS-03 FIX: status=error not failed
\"\"\"
from __future__ import annotations
import asyncio, logging, os
log = logging.getLogger(__name__)


async def on_tunnel_ready(tunnel_url: str, *, notify_uid=None, cc_path="/webhook/cloudconvert") -> dict:
    if not tunnel_url: return {"tunnel": "", "cc": []}
    tunnel_url = tunnel_url.rstrip("/")
    log.info("[WH-Sync] Syncing CC → %s%s", tunnel_url, cc_path)
    from services.cc_webhook_mgr import sync_cc_webhooks
    cc_results = await sync_cc_webhooks(tunnel_url, webhook_path=cc_path)
    try:
        from core.config import set_tunnel_url; set_tunnel_url(tunnel_url)
    except Exception: pass
    uid = notify_uid or _admin_uid()
    if uid: asyncio.create_task(_notify(uid, tunnel_url, cc_results, cc_path))
    return {"tunnel": tunnel_url, "cc": cc_results}


async def on_tunnel_reconnected(new_url): await on_tunnel_ready(new_url)


async def poll_pending_jobs():
    await asyncio.gather(_poll_cc(), _poll_fc(), return_exceptions=True)


async def _poll_cc():
    try:
        from services.cc_job_store import cc_job_store
        pending = cc_job_store.active_jobs()   # BUG-WS-01: sync, no await
        if not pending: return
        import aiohttp as _ah
        api_key = os.environ.get("CC_API_KEY","").strip()
        if not api_key: return
        for job in pending:
            try:
                async with _ah.ClientSession(timeout=_ah.ClientTimeout(total=10)) as s:
                    async with s.get(f"https://api.cloudconvert.com/v2/jobs/{job.job_id}",
                                     headers={"Authorization": f"Bearer {api_key}"}) as r:
                        data = await r.json()
                jdata = data.get("data") or data
                status = jdata.get("status","")
                if status == "finished":
                    from services.cloudconvert_hook import _handle_cc_job  # BUG-WS-02
                    await _handle_cc_job(job.job_id, jdata, api_key)
                elif status == "error":
                    await cc_job_store.update(job.job_id, status="error")  # BUG-WS-03
            except Exception as exc:
                log.warning("[WH-Sync] CC poll %s: %s", job.job_id, exc)
    except Exception as exc:
        log.error("[WH-Sync] _poll_cc: %s", exc)


async def _poll_fc():
    try:
        from services.fc_job_store import fc_job_store
        pending = await fc_job_store.list_processing()
        if not pending: return
        import aiohttp as _ah
        for job in pending:
            api_key = job.api_key or os.environ.get("FC_API_KEY","").strip()
            if not api_key: continue
            try:
                async with _ah.ClientSession(timeout=_ah.ClientTimeout(total=15)) as s:
                    async with s.get(f"https://api.freeconvert.com/v1/process/jobs/{job.job_id}",
                                     headers={"Authorization": f"Bearer {api_key}"}) as r:
                        data = await r.json()
                status = (data.get("data") or data).get("status","").lower()
                jdata  = (data.get("data") or data)
                if status == "completed":
                    from plugins.fc_webhook import _handle_completion
                    await _handle_completion(job, jdata)
                elif status in ("failed","error","cancelled"):
                    await fc_job_store.update(job.job_id, status="failed",
                                              error=jdata.get("message",status)[:200])
            except Exception as exc:
                log.warning("[WH-Sync] FC poll %s: %s", job.job_id, exc)
    except Exception as exc:
        log.error("[WH-Sync] _poll_fc: %s", exc)


def _admin_uid():
    try: return int(os.environ.get("ADMIN_ID","")) or None
    except: return None


async def _notify(uid, tunnel_url, cc, cc_path):
    try:
        from core.session import get_client
        from pyrogram import enums
        lines = []
        for r in cc:
            tail=r.get("key_tail","?"); d=r.get("deleted",0); reg=r.get("registered"); err=r.get("error","")
            lines.append(f"  {tail}  {'❌ '+err[:50] if err else str(d)+' deleted · ✅ '+str(reg)[:12]}")
        text = (f"✅ <b>Webhook sync</b>\\n🔗 <code>{tunnel_url}{cc_path}</code>\\n\\n"
                f"☁️ CC:\\n" + (chr(10).join(lines) or "  (no CC keys)") +
                "\\n\\n🆓 FC: per-job ✅")
        await (await get_client().__class__).send_message  # just import test
        client = get_client()
        await client.send_message(uid, text, parse_mode=enums.ParseMode.HTML, disable_web_page_preview=True)
    except Exception: pass
""", "webhook_sync.py (all BUG-WS fixes)")


# ── PATCH: services/downloader.py — PanelUpdater interval ─────────────────
_patch_file("services/downloader.py",
    "_updater = PanelUpdater(msg, _build_dl_panel, interval=1.0)",
    "_updater = PanelUpdater(msg, _build_dl_panel, interval=5.0)",
    "BUG-UH-04: downloader interval 1→5s")

# ── PATCH: services/tg_download.py — PanelUpdater interval ────────────────
_patch_file("services/tg_download.py",
    "async with PanelUpdater(msg, _build, interval=1.0) as pu:",
    "async with PanelUpdater(msg, _build, interval=3.0) as pu:",
    "BUG-UH-04: tg_download interval 1→3s")

# ── PATCH: cloudconvert_hook.py — BUG-12 signature prefix ─────────────────
_patch_file("services/cloudconvert_hook.py",
    "    return hmac.compare_digest(expected, signature)",
    "    sig_hex = signature.removeprefix('sha256=')\n    return hmac.compare_digest(expected, sig_hex)",
    "BUG-12: signature sha256= prefix strip")

# ── PATCH: cloudconvert_hook.py — BUG-01 download_direct ──────────────────
_patch_file("services/cloudconvert_hook.py",
    "    from services.downloader import smart_download\n",
    "    from services.downloader import download_direct  # BUG-01\n",
    "BUG-01a: import download_direct")
_patch_file("services/cloudconvert_hook.py",
    "        path = await smart_download(url, tmp)\n",
    "        path = await download_direct(url, tmp)\n",
    "BUG-01b: call download_direct")

# ── PATCH: services/webhook_sync.py cc_path (defensive, if inject missed) ─
_patch_file("services/webhook_sync.py",
    'cc_path:    str        = "/webhook"',
    'cc_path:    str        = "/webhook/cloudconvert"',
    "BUG-WS-04: cc_path defensive")
_patch_file("services/webhook_sync.py",
    'cc_path="/webhook"',
    'cc_path="/webhook/cloudconvert"',
    "BUG-WS-04: cc_path call-site defensive")

# ── PATCH: plugins/video.py _IGNORED ─────────────────────────────────────
_patch_file("plugins/video.py",
    '"nyaa_add","nyaa_list","nyaa_remove","nyaa_check",\n    "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",\n}',
    '"nyaa_add","nyaa_list","nyaa_remove","nyaa_check",\n    "nyaa_search","nyaa_dump","nyaa_toggle","nyaa_edit",\n    "resize","compress","hardsub","botname","ccstatus","convert",\n    "captiontemplate","usage","allow","deny","allowed","cancel",\n}',
    "BUG-07: video.py _IGNORED")

# ── PATCH: freeconvert_api.py FC-01/02 (fabricated options) ───────────────
_fc_api_path = os.path.join(BASE_DIR, "services", "freeconvert_api.py")
if os.path.exists(_fc_api_path):
    with open(_fc_api_path) as _f: _fc_src = _f.read()
    _bad = "'subtitle_task'" in _fc_src or '"subtitle_task"' in _fc_src
    if _bad:
        # Replace "input" → "depends_on" throughout
        _fc_fixed = (_fc_src
            .replace('"input":         "import-file"',    '"depends_on":    ["import-file"]')
            .replace('"input":     "import-file"',        '"depends_on": ["import-file"]')
            .replace('"input":     ["convert-file"]',     '"depends_on": ["convert-file"]')
            .replace('"input":     ["compress-file"]',    '"depends_on": ["compress-file"]')
            .replace('"input":     ["hardsub"]',          '"depends_on": ["hardsub"]')
            .replace("'subtitle_task': 'import-subtitle',", "# FC-01 REMOVED subtitle_task")
            .replace('"subtitle_task": "import-subtitle",', "# FC-01 REMOVED subtitle_task")
            .replace("'subtitle_burn': True,", "# FC-01 REMOVED subtitle_burn")
            .replace('"subtitle_burn": True,', "# FC-01 REMOVED subtitle_burn")
        )
        try:
            _patch_ast.parse(_fc_fixed)
            with open(_fc_api_path, "w") as _f: _f.write(_fc_fixed)
            _log("OK", "✅ freeconvert_api.py FC-01/02 patched")
        except SyntaxError as e:
            _log("WARN", f"freeconvert_api.py patch failed: {e}")
    else:
        _log("INFO", "freeconvert_api.py FC-01/02 already clean")

_log("OK", "All injections and patches complete ✅")


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
