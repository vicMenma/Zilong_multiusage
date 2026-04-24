"""
services/freeconvert_api.py  —  REWRITE v4
FreeConvert.com API v1 client.

═══════════════════════════════════════════════════════════════════
BUG FIXES IN THIS VERSION
═══════════════════════════════════════════════════════════════════

BUG-FC-01 (CRITICAL) — create_hardsub_job: subtitle was never burned
─────────────────────────────────────────────────────────────────
OLD code had TWO independent fatal mistakes:

  1.  "subtitle_file": "import-subtitle"  in `options` is NOT a
      FreeConvert API option.  FC silently ignores unknown options,
      so every hardsub job produced a video WITHOUT burned subtitles.

  2.  "input": "import-video"  (a single string) only connected the
      video to the convert task.  The import-subtitle task was a
      dangling orphan — it completed but its output was never consumed.
      This is true for BOTH the subtitle-upload path AND the
      subtitle-URL path.

FIX — Two properly-wired structures depending on how the subtitle
arrives:

  Path A — subtitle_url given (external URL, no upload):
    • import-video         ← video (url or upload)
    • import-subtitle      ← subtitle via import/url
    • hardsub:
        input: ["import-video", "import-subtitle"]   ← ARRAY input
        options: { burn_subtitle: True }              ← correct option
    • export

  Path B — subtitle_path given (local file upload):
    • Same structure — import-subtitle via import/upload, then
      array input + burn_subtitle.

  In both paths the subtitle import task IS connected to the convert
  task via the array input, and burn_subtitle: True tells FC to render
  the subtitle track into the output video.

  The "subtitle_url" option (single string in options) IS also
  supported by FreeConvert when you have an external URL and want to
  skip the import task entirely.  We use that as an alternative for
  Path A to keep the job smaller (3 tasks vs 4).

BUG-FC-02 (CRITICAL) — upload_file_to_task: zero progress feedback
─────────────────────────────────────────────────────────────────
Large video uploads (500 MB–2 GB) ran as a single aiohttp POST with
no progress reporting.  The Telegram UI appeared frozen for up to
20 minutes.

FIX — upload_file_to_task now accepts an optional
  progress_cb(bytes_sent: int, total_bytes: int) -> Coroutine
and streams the file in UPLOAD_CHUNK_SIZE (default 1 MB) chunks
through a custom async generator, calling progress_cb after each
chunk.  submit_hardsub / submit_convert / submit_compress thread
this callback through from their callers.

BUG-FC-03 (MEDIUM) — wait_for_job: coarse progress jumps
─────────────────────────────────────────────────────────────────
Progress was computed as  done_count / total_tasks * 100  giving
hard jumps: 0 → 25 → 50 → 75 → 100 for a 4-task job.

FIX — wait_for_job now tries to extract the `percent` field from
the currently-processing task (FreeConvert populates this on running
tasks).  Falls back to task-count estimate when no `percent` is
available.  Current task name/operation is also surfaced in the
progress detail string.

BUG-FC-04 (MEDIUM) — submit_* helpers: no progress_cb pipe for uploads
─────────────────────────────────────────────────────────────────
submit_hardsub / submit_convert / submit_compress called
upload_file_to_task without any progress callback, so the upload
phase was always silent regardless of what the caller passed.

FIX — all three submit_* helpers now accept an optional
  upload_progress_cb(bytes_sent: int, total_bytes: int)
and forward it to upload_file_to_task.  Existing callers pass nothing
and get the old behaviour (no progress from upload, progress only
during FC processing).

BUG-FC-05 (LOW) — get_export_url: task field precedence
─────────────────────────────────────────────────────────────────
FC completed-job responses list tasks where each task object has:
  name      → the logical task key (e.g. "export")
  operation → the operation type  (e.g. "export/url")
Both contain the word "export", so the old code worked in practice,
but the precedence (operation → name) was backwards for some
response shapes.  Fixed to check name first, then operation.

═══════════════════════════════════════════════════════════════════
PUBLIC API (unchanged signatures — all additions are keyword-only)
═══════════════════════════════════════════════════════════════════
  create_hardsub_job(api_key, *, video_url, sub_url, video_fname,
                     sub_fname, output_fname, crf, preset,
                     scale_height, webhook_url)           → dict
  create_convert_job(api_key, *, input_url, input_path,
                     output_format, scale_height, crf, preset,
                     webhook_url)                          → str  (job_id)
  create_compress_job(api_key, *, input_url, input_path,
                      target_mb, output_format, webhook_url) → str
  upload_file_to_task(api_key, job_id, task_name, file_path,
                      progress_cb=None)                    → None
  wait_for_job(api_key, job_id, timeout_s, poll_interval,
               progress_cb)                                → dict
  get_export_url(job)                                      → str
  submit_convert(api_key, *, video_path, video_url,
                 scale_height, crf, preset, output_name,
                 webhook_url, upload_progress_cb)          → str
  submit_compress(api_key, *, video_path, video_url,
                  target_mb, output_name, webhook_url,
                  upload_progress_cb)                      → str
  submit_hardsub(api_key, *, video_path, video_url,
                 subtitle_path, subtitle_url, output_name,
                 crf, preset, scale_height, webhook_url,
                 upload_progress_cb)                       → str
  run_fc_job(api_key, job_id, dest_dir, output_name,
             progress_cb, timeout_s)                       → str  (local path)
  parse_fc_keys(raw)                                       → list[str]
  pick_best_fc_key(keys)                                   → tuple[str, float]
  get_fc_api_key()                                         → str
  fc_webhook_url(base_url)                                 → str
"""
from __future__ import annotations

import asyncio
import logging
import os
import re as _re
import time
from typing import Callable, Coroutine, Optional

import aiohttp

log = logging.getLogger(__name__)

_FC_BASE        = "https://api.freeconvert.com/v1/process"
_FC_ROOT        = "https://api.freeconvert.com/v1"
_TIMEOUT_SHORT  = aiohttp.ClientTimeout(total=30)
_TIMEOUT_UPLOAD = aiohttp.ClientTimeout(total=7200)

# Upload chunk size for progress-aware streaming (1 MB default)
UPLOAD_CHUNK_SIZE = 1 * 1024 * 1024   # 1 MB


# ─────────────────────────────────────────────────────────────
# Error extraction
# ─────────────────────────────────────────────────────────────

def _extract_fc_error(data: dict | str, status: int = 0) -> str:
    """
    Extract the most-readable error message from a FreeConvert API response.
    FC responses vary wildly in shape:
      - {"message": "..."}
      - {"errors": {"field": ["err1", "err2"]}}
      - {"errors": [{"message": "..."}]}
      - {"error": "..."}
      - HTML error pages (string)
      - deeply-nested validation payloads
    Returns a user-friendly single-line message, never empty.
    """
    if isinstance(data, str):
        s = data.strip()
        return (s[:300] + "…") if len(s) > 300 else (s or f"HTTP {status}")

    if not isinstance(data, dict):
        return f"HTTP {status}: {str(data)[:200]}"

    # Top-level "message"
    msg = data.get("message") or data.get("error") or ""
    if isinstance(msg, str) and msg.strip():
        return msg.strip()[:300]

    # "errors" can be dict (field→[msgs]) or list
    errs = data.get("errors")
    if isinstance(errs, dict):
        fragments = []
        for field, v in errs.items():
            if isinstance(v, list):
                fragments.append(f"{field}: {'; '.join(str(x) for x in v)}")
            else:
                fragments.append(f"{field}: {v}")
        if fragments:
            return " · ".join(fragments)[:300]
    if isinstance(errs, list) and errs:
        first = errs[0]
        if isinstance(first, dict):
            m = first.get("message") or first.get("detail") or str(first)
        else:
            m = str(first)
        return m[:300]

    # Task-level error lookup
    for t in (data.get("tasks") or []):
        if (t.get("status") or "") in ("error", "failed"):
            r = t.get("result") or {}
            m = (r.get("msg") or r.get("errorCode") or
                 r.get("message") or r.get("error") or
                 t.get("message") or t.get("msg") or t.get("code"))
            if m:
                return str(m)[:300]

    return f"HTTP {status}: {str(data)[:200]}"


# ─────────────────────────────────────────────────────────────
# Webhook URL helpers
# ─────────────────────────────────────────────────────────────

def fc_webhook_url(base_url: str) -> str:
    return base_url.rstrip("/") + "/fc-webhook"


def _auto_webhook() -> Optional[str]:
    """
    Return current FC webhook URL derived from the live tunnel, or None.
    Every submit_*() helper calls this so webhooks are embedded without
    caller changes.
    """
    try:
        from core.config import get_tunnel_url
        turl = get_tunnel_url()
        if turl:
            return fc_webhook_url(turl)
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────
# Filename sanitiser
# ─────────────────────────────────────────────────────────────

def _safe(name: str) -> str:
    return _re.sub(r"[^\w.\-]", "_", name)


# ─────────────────────────────────────────────────────────────
# Low-level job fetch
# ─────────────────────────────────────────────────────────────

async def _fc_get_job(api_key: str, job_id: str) -> dict:
    headers = {"Authorization": f"Bearer {api_key}"}
    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.get(f"{_FC_BASE}/jobs/{job_id}", headers=headers) as resp:
            data = await resp.json(content_type=None)
    return data.get("data") or data


# ─────────────────────────────────────────────────────────────
# Wait for a specific task to reach "waiting" state (upload ready)
# ─────────────────────────────────────────────────────────────

async def _wait_for_task_ready(
    api_key: str, job_id: str, task_name: str, timeout: int = 120,
) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job   = await _fc_get_job(api_key, job_id)
        tasks = job.get("tasks") or []
        task  = next((t for t in tasks if t.get("name") == task_name), None)
        if not task:
            raise RuntimeError(f"[FC] Task '{task_name}' not found in job {job_id}")
        status = task.get("status", "")
        if status == "waiting":
            form_url = (task.get("result") or {}).get("form", {}).get("url", "")
            if form_url:
                return task
        elif status == "error":
            raise RuntimeError(f"[FC] Task '{task_name}' failed before upload")
        await asyncio.sleep(3)
    raise RuntimeError(f"[FC] Import task '{task_name}' never ready in {timeout}s")


# ─────────────────────────────────────────────────────────────
# BUG-FC-02 FIX: upload_file_to_task with streaming progress
# ─────────────────────────────────────────────────────────────

async def upload_file_to_task(
    api_key:     str,
    job_id:      str,
    task_name:   str,
    file_path:   str,
    progress_cb: Optional[Callable[[int, int], Coroutine]] = None,
) -> None:
    """
    Upload a local file to a FreeConvert import/upload task.

    BUG-FC-02 FIX: Previously uploaded as a single monolithic POST
    with no progress reporting.  Now streams the file in chunks of
    UPLOAD_CHUNK_SIZE bytes and calls
      await progress_cb(bytes_sent, total_bytes)
    after each chunk so callers can update the Telegram status panel.

    progress_cb is optional; pass None for silent uploads.
    """
    task      = await _wait_for_task_ready(api_key, job_id, task_name)
    form      = (task.get("result") or {}).get("form") or {}
    upload_url = form.get("url", "")
    params     = form.get("parameters") or {}
    fname      = os.path.basename(file_path)
    total_size = os.path.getsize(file_path)

    if not upload_url:
        raise RuntimeError(f"[FC] No upload URL for task '{task_name}'")

    log.info("[FC-API] Uploading %s → job=%s task=%s (%d bytes)",
             fname, job_id, task_name, total_size)

    # ── Build a streaming async generator for the file field ──
    # We need multipart/form-data with the fixed params FIRST, then the file.
    # aiohttp handles multipart natively; we wrap the file in a chunked reader
    # that fires progress_cb after each chunk.

    async def _chunked_file_reader():
        sent = 0
        with open(file_path, "rb") as fh:
            while True:
                chunk = fh.read(UPLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                sent += len(chunk)
                yield chunk
                if progress_cb:
                    try:
                        await progress_cb(sent, total_size)
                    except Exception:
                        pass  # never let progress errors crash the upload

    form_data = aiohttp.FormData()
    for k, v in params.items():
        form_data.add_field(k, str(v))

    # aiohttp supports async generators as field values
    form_data.add_field(
        "file",
        _chunked_file_reader(),
        filename=fname,
        content_type="application/octet-stream",
    )

    async with aiohttp.ClientSession(timeout=_TIMEOUT_UPLOAD) as sess:
        async with sess.post(upload_url, data=form_data, allow_redirects=True) as resp:
            if resp.status not in (200, 201, 204, 301, 302):
                body = await resp.text()
                raise RuntimeError(f"[FC] Upload failed ({resp.status}): {body[:200]}")

    log.info("[FC-API] Upload complete: %s (%d bytes)", fname, total_size)


# ─────────────────────────────────────────────────────────────
# BUG-FC-01 FIX: create_hardsub_job — correct subtitle wiring
# ─────────────────────────────────────────────────────────────

async def create_hardsub_job(
    api_key: str,
    *,
    video_url:     Optional[str] = None,
    sub_url:       Optional[str] = None,
    video_fname:   str  = "video.mkv",
    sub_fname:     str  = "subtitle.ass",
    output_fname:  str  = "output.mp4",
    crf:           int  = 20,
    preset:        str  = "medium",
    scale_height:  int  = 0,
    webhook_url:   Optional[str] = None,
) -> dict:
    """
    Create a FreeConvert hardsub job.

    BUG-FC-01 FIX — TWO correct wiring approaches, chosen automatically:

    Path A (subtitle_url given — external URL):
      We use subtitle_url directly in the convert task options.
      This avoids a separate import-subtitle task and is the most
      reliable approach when the subtitle is publicly accessible.
      Job structure:
        import-video   → import/url  OR  import/upload
        hardsub        → convert  {input: "import-video",
                                   options: {subtitle_url: sub_url,
                                             burn_subtitle: True}}
        export         → export/url

    Path B (no subtitle_url — local file upload):
      We create an import-subtitle task and connect it to the convert
      task via an ARRAY input.  FreeConvert uses burn_subtitle: True
      to detect and burn the subtitle stream from the second input.
      Job structure:
        import-video      → import/url  OR  import/upload
        import-subtitle   → import/upload
        hardsub           → convert  {input: ["import-video",
                                              "import-subtitle"],
                                      options: {burn_subtitle: True}}
        export            → export/url

    In the OLD code:
      • "subtitle_file": "import-subtitle" was in options  ← FC ignores it
      • input: "import-video"  ← subtitle task dangled, never consumed
    → every hardsub job produced un-subtitled video or failed silently.
    """
    headers  = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    v_safe   = _safe(video_fname)
    v_ext    = os.path.splitext(v_safe)[1].lstrip(".").lower() or "mkv"

    tasks: dict = {}

    # ── Video import ──────────────────────────────────────────
    if video_url:
        tasks["import-video"] = {
            "operation": "import/url", "url": video_url, "filename": v_safe,
        }
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    # ── Hardsub convert options (common to both paths) ────────
    hs_options: dict = {
        "video_codec":   "h264",
        "crf":           crf,
        "preset":        preset,
        "audio_codec":   "aac",
        "audio_bitrate": "128k",
    }
    if scale_height > 0:
        hs_options["video_height"] = scale_height

    # ── Path A: subtitle URL given — embed directly in options ─
    if sub_url:
        # Use subtitle_url option; connect only video as input.
        # FreeConvert fetches the subtitle file from the URL and burns it.
        hs_options["subtitle_url"]   = sub_url
        hs_options["burn_subtitle"]  = True

        tasks["hardsub"] = {
            "operation":     "convert",
            "input":         "import-video",    # single string — video only
            "input_format":  v_ext,
            "output_format": "mp4",
            "options":       hs_options,
        }

        log.debug("[FC-API] Hardsub Path A (subtitle_url): %s", sub_url[:60])

    # ── Path B: subtitle must be uploaded — multi-input array ──
    else:
        s_safe = _safe(sub_fname)
        tasks["import-subtitle"] = {"operation": "import/upload"}

        # BUG-FC-01 FIX: input MUST be an array so the subtitle import
        # task is actually consumed.  burn_subtitle: True tells FC to
        # render the subtitle into the video output.
        hs_options["burn_subtitle"] = True

        tasks["hardsub"] = {
            "operation":     "convert",
            "input":         ["import-video", "import-subtitle"],  # ← ARRAY
            "input_format":  v_ext,
            "output_format": "mp4",
            "options":       hs_options,
        }

        log.debug("[FC-API] Hardsub Path B (subtitle upload): %s", s_safe)

    tasks["export"] = {"operation": "export/url", "input": "hardsub"}

    payload: dict   = {"tasks": tasks}
    effective_wh    = webhook_url or _auto_webhook()
    if effective_wh:
        payload["webhook_url"] = effective_wh

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(
            f"{_FC_BASE}/jobs", json=payload, headers=headers,
        ) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = await resp.text()
            if resp.status not in (200, 201):
                err = _extract_fc_error(data, resp.status)
                log.error("[FC-API] Hardsub create failed %d: %s", resp.status, err)
                raise RuntimeError(f"[FC] Hardsub create ({resp.status}): {err}")

    inner  = (data.get("data") or data) if isinstance(data, dict) else {}
    job_id = inner.get("id", "?")
    log.info(
        "[FC-API] Hardsub job: %s  crf=%d  preset=%s  scale=%s  "
        "sub=%s  webhook=%s",
        job_id, crf, preset,
        f"{scale_height}p" if scale_height else "original",
        "url" if sub_url else "upload",
        "yes" if effective_wh else "no (poller will handle)",
    )
    return inner


# ─────────────────────────────────────────────────────────────
# create_convert_job — unchanged except logging
# ─────────────────────────────────────────────────────────────

async def create_convert_job(
    api_key: str,
    *,
    input_url:     Optional[str] = None,
    input_path:    Optional[str] = None,
    output_format: str  = "mp4",
    scale_height:  int  = 0,
    crf:           int  = 23,
    preset:        str  = "medium",
    webhook_url:   Optional[str] = None,
) -> str:
    if not input_url and not input_path:
        raise ValueError("[FC] Provide either input_url or input_path")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    input_name = _safe(
        os.path.basename(input_path) if input_path
        else (input_url.split("/")[-1].split("?")[0] if input_url else "in.mp4")
    )
    in_ext = os.path.splitext(input_name)[1].lstrip(".").lower() or "mp4"

    tasks: dict = {}
    if input_url:
        tasks["import-file"] = {
            "operation": "import/url", "url": input_url, "filename": input_name,
        }
    else:
        tasks["import-file"] = {"operation": "import/upload"}

    cv_options: dict = {
        "video_codec":   "h264",
        "crf":           crf,
        "preset":        preset,
        "audio_codec":   "aac",
        "audio_bitrate": "128k",
    }
    if scale_height > 0:
        cv_options["video_height"] = scale_height

    tasks["convert-file"] = {
        "operation":     "convert",
        "input":         "import-file",
        "input_format":  in_ext,
        "output_format": output_format,
        "options":       cv_options,
    }
    tasks["export"] = {"operation": "export/url", "input": "convert-file"}

    payload: dict = {"tasks": tasks}
    effective_wh  = webhook_url or _auto_webhook()
    if effective_wh:
        payload["webhook_url"] = effective_wh

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(
            f"{_FC_BASE}/jobs", json=payload, headers=headers,
        ) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = await resp.text()
            if resp.status not in (200, 201):
                err = _extract_fc_error(data, resp.status)
                log.error("[FC-API] Convert create failed %d: %s", resp.status, err)
                raise RuntimeError(f"[FC] Convert create ({resp.status}): {err}")

    if not isinstance(data, dict):
        raise RuntimeError("[FC] Convert create: non-JSON response")
    job_id = (data.get("data") or data).get("id", "?")
    log.info("[FC-API] Convert job: %s  scale=%s  crf=%d  preset=%s  webhook=%s",
             job_id, f"{scale_height}p" if scale_height else "original",
             crf, preset, "yes" if effective_wh else "no")
    return job_id


# ─────────────────────────────────────────────────────────────
# create_compress_job — unchanged
# ─────────────────────────────────────────────────────────────

async def create_compress_job(
    api_key: str,
    *,
    input_url:     Optional[str] = None,
    input_path:    Optional[str] = None,
    target_mb:     float = 50.0,
    output_format: str   = "mp4",
    webhook_url:   Optional[str] = None,
) -> str:
    if not input_url and not input_path:
        raise ValueError("[FC] Provide either input_url or input_path")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    input_name = _safe(
        os.path.basename(input_path) if input_path
        else (input_url.split("/")[-1].split("?")[0] if input_url else "in.mp4")
    )
    in_ext = os.path.splitext(input_name)[1].lstrip(".").lower() or "mp4"

    tasks: dict = {}
    if input_url:
        tasks["import-file"] = {
            "operation": "import/url", "url": input_url, "filename": input_name,
        }
    else:
        tasks["import-file"] = {"operation": "import/upload"}

    tasks["compress-file"] = {
        "operation":     "compress",
        "input":         "import-file",
        "input_format":  in_ext,
        "output_format": output_format,
        "options": {
            "target_size": int(target_mb),   # MB — FreeConvert native unit
        },
    }
    tasks["export"] = {"operation": "export/url", "input": "compress-file"}

    payload: dict = {"tasks": tasks}
    effective_wh  = webhook_url or _auto_webhook()
    if effective_wh:
        payload["webhook_url"] = effective_wh

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(
            f"{_FC_BASE}/jobs", json=payload, headers=headers,
        ) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = await resp.text()
            if resp.status not in (200, 201):
                err = _extract_fc_error(data, resp.status)
                log.error("[FC-API] Compress create failed %d: %s", resp.status, err)
                raise RuntimeError(f"[FC] Compress create ({resp.status}): {err}")

    if not isinstance(data, dict):
        raise RuntimeError("[FC] Compress create: non-JSON response")
    job_id = (data.get("data") or data).get("id", "?")
    log.info("[FC-API] Compress job: %s  target=%.0f MB  webhook=%s",
             job_id, target_mb, "yes" if effective_wh else "no")
    return job_id


# ─────────────────────────────────────────────────────────────
# BUG-FC-03 FIX: wait_for_job — smooth progress with task percent
# ─────────────────────────────────────────────────────────────

def _task_progress(tasks: list) -> tuple[float, str]:
    """
    BUG-FC-03 FIX: Extract best current progress estimate from FC tasks.

    Priority:
      1. Active task with a numeric `percent` field → use directly
      2. Active task without percent → count done/total (coarse)
      3. No active task → 95 % (almost done) or 0 % (not started)

    Returns (pct: float, detail: str).
    """
    if not tasks:
        return 0.0, "⏳ Starting…"

    total = len(tasks)
    done  = 0
    active_pct:  Optional[float] = None
    active_name: str = ""

    for t in tasks:
        status = (t.get("status") or "").lower()
        name   = t.get("name") or t.get("operation") or "task"

        if status == "completed":
            done += 1
        elif status in ("processing", "running", "active"):
            # Try to get per-task percent  (FC populates this on running tasks)
            raw = t.get("percent") or t.get("progress")
            try:
                active_pct  = float(raw)
                active_name = name
            except (TypeError, ValueError):
                active_name = name

    if active_pct is not None:
        # Weight active task as one additional partial task
        pct = min(95.0, (done / total * 100) + (active_pct / total))
        return pct, f"⏳ {active_name} — {active_pct:.0f}%"

    if active_name:
        # Active but no percent field
        base = done / total * 100
        return min(90.0, base + 5.0), f"⏳ {active_name}…"

    if done == total:
        return 95.0, "⏳ Finalizing…"

    return min(90.0, done / total * 100), f"⏳ Processing… ({done}/{total} done)"


async def wait_for_job(
    api_key:       str,
    job_id:        str,
    timeout_s:     int   = 7200,
    poll_interval: float = 5.0,
    progress_cb    = None,
) -> dict:
    """
    Poll until the FC job completes, fails, or times out.

    BUG-FC-03 FIX: progress is now extracted from the active task's
    `percent` field (when available) rather than done_count/total_tasks,
    giving smooth 0→100 progress rather than hard jumps.
    """
    deadline   = time.time() + timeout_s
    _poll_errs = 0

    while time.time() < deadline:
        try:
            job = await _fc_get_job(api_key, job_id)
            _poll_errs = 0
        except Exception as exc:
            _poll_errs += 1
            log.warning("[FC-API] Poll error for job %s (attempt %d): %s — retrying",
                        job_id, _poll_errs, exc)
            if _poll_errs >= 5:
                raise RuntimeError(
                    f"[FC] Job {job_id}: 5 consecutive poll failures — last: {exc}"
                ) from exc
            await asyncio.sleep(min(poll_interval * _poll_errs, 30.0))
            continue

        status = (job.get("status") or "").lower()

        if status == "completed":
            log.info("[FC-API] Job %s completed", job_id)
            if progress_cb:
                try:
                    await progress_cb(100.0, "✅ Complete")
                except Exception:
                    pass
            return job

        if status in ("failed", "cancelled", "error"):
            tasks   = job.get("tasks") or []
            err_msg = job.get("message") or job.get("msg") or ""
            for t in tasks:
                if (t.get("status") or "") in ("error", "failed"):
                    r = t.get("result") or {}
                    t_err = (
                        r.get("msg") or r.get("errorCode") or
                        r.get("message") or r.get("error") or
                        t.get("message") or t.get("msg") or ""
                    )
                    if t_err:
                        err_msg = t_err
                        break
            raise RuntimeError(f"[FC] Job {job_id} {status}: {err_msg or 'Unknown error'}")

        # In-progress: report progress
        if progress_cb:
            tasks = job.get("tasks") or []
            try:
                pct, detail = _task_progress(tasks)
                await progress_cb(pct, detail)
            except Exception:
                pass

        await asyncio.sleep(poll_interval)

    raise RuntimeError(f"[FC] Job {job_id} timed out after {timeout_s}s")


# ─────────────────────────────────────────────────────────────
# BUG-FC-05 FIX: get_export_url — correct task field precedence
# ─────────────────────────────────────────────────────────────

def get_export_url(job: dict) -> str:
    """
    Extract the output file URL from a completed FreeConvert job dict.

    BUG-FC-05 FIX: FC completed-job task objects have:
      name      → logical key (e.g. "export")
      operation → operation type (e.g. "export/url")

    OLD code checked  `task.get("operation") or task.get("name")`  which
    could miss cases where operation was absent.  Now we check name first,
    then operation, so both response shapes work reliably.
    """
    tasks = job.get("tasks") or []

    def _url_from_result(result: dict) -> str:
        # FreeConvert v1: result.url  (plain string)
        url = result.get("url") or ""
        if isinstance(url, str) and url:
            return url
        # CloudConvert / legacy: result.files[0].url
        for key in ("files", "output", "outputs"):
            files = result.get(key) or []
            if isinstance(files, list) and files:
                return files[0].get("url", "") if isinstance(files[0], dict) else ""
            if isinstance(files, dict):
                return files.get("url", "")
        return ""

    def _is_export(task: dict) -> bool:
        # BUG-FC-05 FIX: check name first (most reliable), then operation
        name = (task.get("name") or "").lower()
        op   = (task.get("operation") or "").lower()
        return "export" in name or "export" in op

    if isinstance(tasks, list):
        for task in tasks:
            if _is_export(task) and (task.get("status") or "").lower() == "completed":
                url = _url_from_result(task.get("result") or {})
                if url:
                    return url
    elif isinstance(tasks, dict):
        for _name, task in tasks.items():
            if _is_export(task) and (task.get("status") or "").lower() == "completed":
                url = _url_from_result(task.get("result") or {})
                if url:
                    return url

    return ""


# ─────────────────────────────────────────────────────────────
# BUG-FC-04 FIX: submit helpers with upload_progress_cb
# ─────────────────────────────────────────────────────────────

async def submit_convert(
    api_key: str,
    *,
    video_path:         Optional[str] = None,
    video_url:          Optional[str] = None,
    scale_height:       int  = 0,
    crf:                int  = 23,
    preset:             str  = "medium",
    output_name:        str  = "converted.mp4",
    webhook_url:        Optional[str] = None,
    upload_progress_cb: Optional[Callable] = None,  # BUG-FC-04 FIX
) -> str:
    """
    Create a convert job and, if input is a local file, upload it.

    BUG-FC-04 FIX: upload_progress_cb(bytes_sent, total_bytes) is now
    forwarded to upload_file_to_task so the caller can show upload progress.
    """
    job_id = await create_convert_job(
        api_key,
        input_url=video_url, input_path=video_path,
        scale_height=scale_height, crf=crf, preset=preset,
        webhook_url=webhook_url,
    )
    if video_path and not video_url:
        await upload_file_to_task(
            api_key, job_id, "import-file", video_path,
            progress_cb=upload_progress_cb,
        )
    return job_id


async def submit_compress(
    api_key: str,
    *,
    video_path:         Optional[str] = None,
    video_url:          Optional[str] = None,
    target_mb:          float = 50.0,
    output_name:        str   = "compressed.mp4",
    webhook_url:        Optional[str] = None,
    upload_progress_cb: Optional[Callable] = None,  # BUG-FC-04 FIX
) -> str:
    job_id = await create_compress_job(
        api_key,
        input_url=video_url, input_path=video_path,
        target_mb=target_mb,
        webhook_url=webhook_url,
    )
    if video_path and not video_url:
        await upload_file_to_task(
            api_key, job_id, "import-file", video_path,
            progress_cb=upload_progress_cb,
        )
    return job_id


async def submit_hardsub(
    api_key: str,
    *,
    video_path:         Optional[str] = None,
    video_url:          Optional[str] = None,
    subtitle_path:      Optional[str] = None,
    subtitle_url:       Optional[str] = None,
    output_name:        str  = "hardsub.mp4",
    crf:                int  = 20,
    preset:             str  = "medium",
    scale_height:       int  = 0,
    webhook_url:        Optional[str] = None,
    upload_progress_cb: Optional[Callable] = None,  # BUG-FC-04 FIX
) -> str:
    """
    Create an FC hardsub job and upload local files if needed.

    BUG-FC-01 + BUG-FC-04 FIX:
      • The job is now wired correctly (see create_hardsub_job docstring).
      • upload_progress_cb is forwarded to upload_file_to_task.

    If both subtitle_url and subtitle_path are given, subtitle_url takes
    precedence (Path A — no subtitle upload needed).
    """
    if not video_path and not video_url:
        raise ValueError("[FC] Provide either video_path or video_url")
    if not subtitle_path and not subtitle_url:
        raise ValueError("[FC] Provide either subtitle_path or subtitle_url")

    video_fname    = (os.path.basename(video_path)    if video_path    else
                      (video_url or "video.mkv").split("/")[-1].split("?")[0])
    subtitle_fname = (os.path.basename(subtitle_path) if subtitle_path else
                      (subtitle_url or "subtitle.ass").split("/")[-1].split("?")[0])

    job = await create_hardsub_job(
        api_key,
        video_url=video_url, sub_url=subtitle_url,
        video_fname=video_fname, sub_fname=subtitle_fname,
        output_fname=output_name,
        crf=crf, preset=preset, scale_height=scale_height,
        webhook_url=webhook_url,
    )
    job_id = job.get("id", "")
    if not job_id:
        raise RuntimeError("[FC] Hardsub job creation returned no ID")

    # Upload video if it's a local file
    if video_path and not video_url:
        await upload_file_to_task(
            api_key, job_id, "import-video", video_path,
            progress_cb=upload_progress_cb,
        )

    # Upload subtitle only in Path B (no subtitle_url)
    # Path A (subtitle_url given) uses import/url — no upload needed
    if subtitle_path and not subtitle_url:
        await upload_file_to_task(
            api_key, job_id, "import-subtitle", subtitle_path,
            progress_cb=upload_progress_cb,
        )

    return job_id


# ─────────────────────────────────────────────────────────────
# High-level run helper — download result after job completes
# ─────────────────────────────────────────────────────────────

async def run_fc_job(
    api_key:     str,
    job_id:      str,
    dest_dir:    str,
    output_name: str   = "",
    progress_cb  = None,
    timeout_s:   int   = 7200,
) -> str:
    """
    Poll until job completes, download the output file, return local path.
    progress_cb(pct: float, detail: str) is called during polling.
    """
    from services.downloader import download_direct

    job = await wait_for_job(
        api_key, job_id,
        timeout_s=timeout_s,
        progress_cb=progress_cb,
    )
    url = get_export_url(job)
    if not url:
        raise RuntimeError(f"[FC] No export URL in completed job {job_id}")

    local_path = await download_direct(url, dest_dir)

    if output_name:
        new_path = os.path.join(dest_dir, output_name)
        try:
            os.rename(local_path, new_path)
            local_path = new_path
        except OSError:
            pass

    log.info("[FC-API] Result: %s", os.path.basename(local_path))
    return local_path


# ─────────────────────────────────────────────────────────────
# Multi-key support
# ─────────────────────────────────────────────────────────────

def parse_fc_keys(raw: str) -> list[str]:
    if not raw:
        return []
    parts = _re.split(r"[,\s\n]+", raw.strip())
    return [p.strip() for p in parts if p.strip()]


async def _fc_get_usage(api_key: str) -> float:
    """
    FIX FC-USAGE-EP: old code hit /v1/process/usage which returns 404.
    Try the real endpoints; on any ambiguity return a LARGE number so the
    job is attempted (FC will return a clean error if truly exhausted).

    FIX FC-401-BAIL: previous version returned 0.0 immediately on the
    first 401 response, skipping the second endpoint and marking the key
    as dead.  FreeConvert's /v1/account endpoint may return 401 for valid
    free-tier keys (auth scope changed without breaking job creation).
    Now we `continue` on 401 to try the next endpoint.  Only after ALL
    endpoints are exhausted without a positive response do we assume the
    key might still work (return 1e6) — the actual job submission will
    surface a real 401 error if the key is truly invalid.
    """
    headers   = {"Authorization": f"Bearer {api_key}"}
    endpoints = [
        f"{_FC_ROOT}/account",
        f"{_FC_ROOT}/user",
    ]
    _got_401 = 0

    for endpoint in endpoints:
        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
                async with sess.get(endpoint, headers=headers) as resp:
                    if resp.status == 401:
                        _got_401 += 1
                        log.warning(
                            "[FC-API] Key ...%s returned 401 on %s — "
                            "trying next endpoint before marking invalid",
                            api_key[-6:], endpoint,
                        )
                        continue
                    if resp.status == 404:
                        continue
                    if resp.status == 429:
                        log.warning("[FC-API] Rate-limited on usage check — assume OK")
                        return 1e6
                    if resp.status not in (200, 201):
                        continue
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        continue

            inner = data
            if isinstance(data.get("data"), dict):
                inner = data["data"]

            log.debug("[FC-API] Usage ...%s (%s): %s",
                      api_key[-6:], endpoint, list(inner.keys()))

            for key in ("minutes_remaining", "conversions_remaining",
                        "minutes_left", "conversions_left",
                        "remaining", "credits_remaining", "credits"):
                v = inner.get(key)
                if v is not None:
                    try:
                        remaining = float(v)
                        log.info("[FC-API] Key ...%s: %.1f remaining (%s)",
                                 api_key[-6:], remaining, key)
                        return max(0.0, remaining)
                    except (ValueError, TypeError):
                        pass

            # Compute from used/limit
            used  = next((float(inner[k]) for k in
                          ("minutes_used", "conversions_used", "used")
                          if k in inner), None)
            limit = next((float(inner[k]) for k in
                          ("minutes_limit", "conversions_limit", "limit", "total")
                          if k in inner), None)
            if used is not None and limit is not None and limit > 0:
                remaining = max(0.0, limit - used)
                log.info("[FC-API] Key ...%s: %.1f / %.1f remaining",
                         api_key[-6:], remaining, limit)
                return remaining

            log.info("[FC-API] Key ...%s: usage response has unrecognized schema "
                     "(keys=%s) — assuming available", api_key[-6:],
                     list(inner.keys())[:8])
            return 1e6

        except Exception as exc:
            log.debug("[FC-API] Endpoint %s failed: %s", endpoint, exc)
            continue

    if _got_401 and _got_401 == len(endpoints):
        log.warning(
            "[FC-API] Key ...%s: all %d endpoint(s) returned 401 — "
            "account API may have changed auth requirements.  "
            "Assuming key is still valid for job creation; "
            "a real 401 will appear if the key is truly invalid.",
            api_key[-6:], len(endpoints),
        )
    else:
        log.info("[FC-API] Key ...%s: usage endpoints unreachable — assuming available",
                 api_key[-6:])
    return 1e6


async def pick_best_fc_key(keys: list[str]) -> tuple[str, float]:
    if not keys:
        raise RuntimeError(
            "No FreeConvert API keys configured.\n"
            "Add FC_API_KEY=your_key to .env or Colab secrets."
        )

    results    = await asyncio.gather(*[_fc_get_usage(k) for k in keys])
    best_idx   = int(max(range(len(results)), key=lambda i: results[i]))
    best_key   = keys[best_idx]
    best_val   = results[best_idx]

    if all(r == 0.0 for r in results):
        raise RuntimeError(
            f"All {len(keys)} FreeConvert key(s) appear invalid or exhausted.\n"
            "If this is wrong, try again — FreeConvert account endpoint may be "
            "temporarily unavailable."
        )

    log.info("[FC-API] Selected key %d/%d (...%s)  ~%s available",
             best_idx + 1, len(keys), best_key[-6:],
             "many" if best_val >= 1e5 else f"{best_val:.1f}")
    return best_key, best_val


def get_fc_api_key() -> str:
    raw  = os.environ.get("FC_API_KEY", "").strip()
    keys = parse_fc_keys(raw)
    for i in range(2, 10):
        extra = os.environ.get(f"FC_API_KEY_{i}", "").strip()
        if extra:
            keys.extend(parse_fc_keys(extra))
    if not keys:
        raise RuntimeError(
            "FreeConvert API key not configured.\n"
            "Add FC_API_KEY=your_key to .env or Colab secrets."
        )
    return keys[0]
