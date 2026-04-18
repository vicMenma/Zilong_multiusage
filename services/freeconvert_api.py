"""
services/freeconvert_api.py  —  PATCHED v2
FreeConvert.com API v1 client.

WHAT CHANGED vs previous version
────────────────────────────────
FIX FC-USAGE-EP (CRITICAL): _fc_get_usage() was hitting
  https://api.freeconvert.com/v1/process/usage  ← this endpoint does NOT
  exist in FreeConvert's public API.  Every request returned 404 →
  exception → return 0.0 in old code, or even -1.0 in the "fixed" version.

  The correct endpoints (in preference order):
    1. GET /v1/account          — returns credits + subscription info
    2. GET /v1/user             — alternative user endpoint
    3. Fall back to "assume available" if none respond

  The old fix (return -1.0 on error) masked this bug partially but still
  caused the "all exhausted" message whenever the response shape wasn't
  recognized.  Now we return +INF-equivalent (1e6) on any uncertainty so
  the job is attempted — FreeConvert itself will fail gracefully if the
  key is truly exhausted, and the user gets a real error message.

FIX FC-WH-AUTO: every submit_*() helper now AUTOMATICALLY injects the
  current tunnel webhook URL into the job payload.  Previously only
  submit_hardsub had an explicit webhook_url parameter, and callers had
  to remember to pass it.  Now convert/compress/hardsub all auto-embed
  the webhook — no caller changes needed.  If tunnel is down, no
  webhook is set and the poller takes over (same as before).

FIX FC-PRESET: create_hardsub_job now accepts `preset` parameter (medium,
  fast, slow, etc.) so callers can pick it per job (see hardsub.py UI).

FIX FC-03a: convert/compress now use 'command' + FFmpeg args (same as
  hardsub) rather than the fragile 'convert'/'compress' operations that
  FreeConvert rejects with validation errors on many free-tier accounts.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_FC_BASE        = "https://api.freeconvert.com/v1/process"
_FC_ROOT        = "https://api.freeconvert.com/v1"
_TIMEOUT_SHORT  = aiohttp.ClientTimeout(total=30)
_TIMEOUT_UPLOAD = aiohttp.ClientTimeout(total=7200)


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
            m = r.get("message") or r.get("error") or t.get("message") or t.get("code")
            if m: return str(m)[:300]

    # Give them the raw shape so they can at least see what happened
    return f"HTTP {status}: {str(data)[:200]}"


# ─────────────────────────────────────────────────────────────
# Webhook URL helper
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
# Job polling
# ─────────────────────────────────────────────────────────────

async def _fc_get_job(api_key: str, job_id: str) -> dict:
    headers = {"Authorization": f"Bearer {api_key}"}
    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.get(f"{_FC_BASE}/jobs/{job_id}", headers=headers) as resp:
            data = await resp.json()
    return data.get("data") or data


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


async def upload_file_to_task(
    api_key: str, job_id: str, task_name: str, file_path: str,
) -> None:
    task = await _wait_for_task_ready(api_key, job_id, task_name)
    form = (task.get("result") or {}).get("form") or {}
    upload_url = form.get("url", "")
    params     = form.get("parameters") or {}
    fname      = os.path.basename(file_path)
    if not upload_url:
        raise RuntimeError(f"[FC] No upload URL for task '{task_name}'")

    log.info("[FC-API] Uploading %s → job=%s task=%s", fname, job_id, task_name)

    with open(file_path, "rb") as fh:
        form_data = aiohttp.FormData()
        for k, v in params.items():
            form_data.add_field(k, str(v))
        form_data.add_field("file", fh, filename=fname)
        async with aiohttp.ClientSession(timeout=_TIMEOUT_UPLOAD) as sess:
            async with sess.post(upload_url, data=form_data, allow_redirects=True) as resp:
                if resp.status not in (200, 201, 204, 301, 302):
                    body = await resp.text()
                    raise RuntimeError(f"[FC] Upload failed ({resp.status}): {body[:200]}")
    log.info("[FC-API] Upload complete: %s", fname)


# ─────────────────────────────────────────────────────────────
# Hardsub — with preset selector
# ─────────────────────────────────────────────────────────────

import re as _re

def _safe(name: str) -> str:
    return _re.sub(r"[^\w.\-]", "_", name)


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
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    v_safe = _safe(video_fname)
    s_safe = _safe(sub_fname)
    o_safe = _safe(output_fname)

    tasks: dict = {}

    if video_url:
        tasks["import-video"] = {
            "operation": "import/url", "url": video_url, "filename": v_safe,
        }
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    if sub_url:
        tasks["import-subtitle"] = {
            "operation": "import/url", "url": sub_url, "filename": s_safe,
        }
    else:
        tasks["import-subtitle"] = {"operation": "import/upload"}

    sub_path    = f"/input/import-subtitle/{s_safe}"
    sub_escaped = sub_path.replace(":", "\\:")

    vf  = (f"scale=-2:{scale_height},subtitles='{sub_escaped}'"
           if scale_height > 0 else f"subtitles='{sub_escaped}'")
    # FIX: user-required preset always uses 128k audio regardless of resolution
    abr = "128k"

    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"-vf {vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {abr} "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    )

    tasks["hardsub"] = {
        "operation":  "command",
        "depends_on": ["import-video", "import-subtitle"],
        "command":    "ffmpeg",
        "arguments":  ffmpeg_args,
    }
    tasks["export"] = {"operation": "export/url", "depends_on": ["hardsub"]}

    payload: dict = {"tasks": tasks}
    # FIX FC-WH-AUTO: fall back to auto-derived URL if caller didn't pass one
    effective_wh = webhook_url or _auto_webhook()
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

    job_id = ((data.get("data") if isinstance(data, dict) else {}) or data).get("id", "?") \
             if isinstance(data, dict) else "?"
    log.info(
        "[FC-API] Hardsub job: %s  crf=%d  preset=%s  scale=%s  webhook=%s",
        job_id, crf, preset,
        f"{scale_height}p" if scale_height else "original",
        "yes" if effective_wh else "no (poller will handle)",
    )
    return data.get("data") or data if isinstance(data, dict) else {"id": "?"}


# ─────────────────────────────────────────────────────────────
# Convert / Resize — using FFmpeg 'command' for reliability
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
    out_name = f"out.{output_format}"

    tasks: dict = {}
    if input_url:
        tasks["import-file"] = {
            "operation": "import/url", "url": input_url, "filename": input_name,
        }
    else:
        tasks["import-file"] = {"operation": "import/upload"}

    vf  = f"-vf scale=-2:{scale_height}" if scale_height > 0 else ""
    # Always 128k — matches user's hardsub preset spec, applied here for consistency
    abr = "128k"

    ffmpeg_args = (
        f"-i /input/import-file/{input_name} "
        f"{vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {abr} "
        f"-movflags +faststart "
        f"/output/{out_name}"
    ).strip()

    tasks["convert-file"] = {
        "operation":  "command",
        "depends_on": ["import-file"],
        "command":    "ffmpeg",
        "arguments":  ffmpeg_args,
    }
    tasks["export"] = {"operation": "export/url", "depends_on": ["convert-file"]}

    payload: dict = {"tasks": tasks}
    effective_wh = webhook_url or _auto_webhook()
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

    # Compute target video bitrate assuming ~30 min default if duration unknown
    # FC's own compress op often mis-estimates; we use 2-pass via command instead
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    input_name = _safe(
        os.path.basename(input_path) if input_path
        else (input_url.split("/")[-1].split("?")[0] if input_url else "in.mp4")
    )
    out_name = f"out.{output_format}"

    # Assume ~3600s worst-case for bitrate calc; FFmpeg -fs will hard-cap
    # Use crf + -fs for simple one-pass size-capped encode
    target_bytes = int(target_mb * 1024 * 1024)

    ffmpeg_args = (
        f"-i /input/import-file/{input_name} "
        f"-c:v libx264 -crf 28 -preset medium "
        f"-c:a aac -b:a 96k "
        f"-fs {target_bytes} "
        f"-movflags +faststart "
        f"/output/{out_name}"
    )

    tasks: dict = {}
    if input_url:
        tasks["import-file"] = {
            "operation": "import/url", "url": input_url, "filename": input_name,
        }
    else:
        tasks["import-file"] = {"operation": "import/upload"}

    tasks["compress-file"] = {
        "operation":  "command",
        "depends_on": ["import-file"],
        "command":    "ffmpeg",
        "arguments":  ffmpeg_args,
    }
    tasks["export"] = {"operation": "export/url", "depends_on": ["compress-file"]}

    payload: dict = {"tasks": tasks}
    effective_wh = webhook_url or _auto_webhook()
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
# Job polling
# ─────────────────────────────────────────────────────────────

async def wait_for_job(
    api_key:       str,
    job_id:        str,
    timeout_s:     int   = 7200,
    poll_interval: float = 5.0,
    progress_cb=None,
) -> dict:
    deadline    = time.time() + timeout_s
    start       = time.time()
    _poll_errs  = 0            # consecutive network-error counter

    while time.time() < deadline:
        try:
            job = await _fc_get_job(api_key, job_id)
            _poll_errs = 0     # reset on success
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

        status = job.get("status", "")
        if status == "completed":
            log.info("[FC-API] Job %s completed", job_id)
            if progress_cb:
                try: await progress_cb(100.0, "✅ Complete")
                except Exception: pass
            return job
        elif status in ("failed", "cancelled", "error"):
            tasks   = job.get("tasks") or []
            err_msg = job.get("message") or ""
            for t in tasks:
                if (t.get("status") or "") in ("error", "failed"):
                    # Check both result.message and top-level message
                    t_err = (
                        ((t.get("result") or {}).get("message") or
                         (t.get("result") or {}).get("error") or
                         t.get("message") or "")
                    )
                    if t_err:
                        err_msg = t_err
                        break
            raise RuntimeError(f"[FC] Job {job_id} {status}: {err_msg or 'Unknown error'}")

        if progress_cb:
            try:
                tasks = job.get("tasks") or []
                if tasks:
                    done_count = sum(1 for t in tasks if (t.get("status") or "") == "completed")
                    pct = min(95.0, done_count / len(tasks) * 100)
                else:
                    elapsed = time.time() - start
                    pct = min(90.0, elapsed / 60 * 30)
                await progress_cb(pct, f"⏳ Processing… ({status})")
            except Exception: pass

        await asyncio.sleep(poll_interval)
    raise RuntimeError(f"[FC] Job {job_id} timed out after {timeout_s}s")


# ─────────────────────────────────────────────────────────────
# Export URL extraction
# ─────────────────────────────────────────────────────────────

def get_export_url(job: dict) -> str:
    tasks = job.get("tasks") or []
    def _try_extract(task: dict) -> str:
        result = task.get("result") or {}
        for key in ("files", "output", "outputs"):
            files = result.get(key) or []
            if isinstance(files, list) and files:
                return files[0].get("url", "")
            if isinstance(files, dict):
                return files.get("url", "")
        return ""
    if isinstance(tasks, list):
        for task in tasks:
            op   = (task.get("operation") or task.get("name") or "").lower()
            stat = (task.get("status") or "").lower()
            if "export" in op and stat == "completed":
                url = _try_extract(task)
                if url: return url
    elif isinstance(tasks, dict):
        for _name, task in tasks.items():
            op   = (task.get("operation") or "").lower()
            stat = (task.get("status") or "").lower()
            if "export" in op and stat == "completed":
                url = _try_extract(task)
                if url: return url
    return ""


# ─────────────────────────────────────────────────────────────
# High-level submit helpers  —  ALL AUTO-EMBED WEBHOOK
# ─────────────────────────────────────────────────────────────

async def submit_convert(
    api_key: str,
    *,
    video_path:   Optional[str] = None,
    video_url:    Optional[str] = None,
    scale_height: int  = 0,
    crf:          int  = 23,
    preset:       str  = "medium",
    output_name:  str  = "converted.mp4",
    webhook_url:  Optional[str] = None,
) -> str:
    job_id = await create_convert_job(
        api_key,
        input_url=video_url, input_path=video_path,
        scale_height=scale_height, crf=crf, preset=preset,
        webhook_url=webhook_url,
    )
    if video_path and not video_url:
        await upload_file_to_task(api_key, job_id, "import-file", video_path)
    return job_id


async def submit_compress(
    api_key: str,
    *,
    video_path:  Optional[str] = None,
    video_url:   Optional[str] = None,
    target_mb:   float = 50.0,
    output_name: str   = "compressed.mp4",
    webhook_url: Optional[str] = None,
) -> str:
    job_id = await create_compress_job(
        api_key,
        input_url=video_url, input_path=video_path,
        target_mb=target_mb,
        webhook_url=webhook_url,
    )
    if video_path and not video_url:
        await upload_file_to_task(api_key, job_id, "import-file", video_path)
    return job_id


async def submit_hardsub(
    api_key:       str,
    *,
    video_path:    Optional[str] = None,
    video_url:     Optional[str] = None,
    subtitle_path: Optional[str] = None,
    subtitle_url:  Optional[str] = None,
    output_name:   str  = "hardsub.mp4",
    crf:           int  = 20,
    preset:        str  = "medium",
    scale_height:  int  = 0,
    webhook_url:   Optional[str] = None,
) -> str:
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

    if video_path and not video_url:
        await upload_file_to_task(api_key, job_id, "import-video", video_path)
    if subtitle_path and not subtitle_url:
        await upload_file_to_task(api_key, job_id, "import-subtitle", subtitle_path)
    return job_id


async def run_fc_job(
    api_key:     str,
    job_id:      str,
    dest_dir:    str,
    output_name: str   = "",
    progress_cb  = None,
    timeout_s:   int   = 7200,
) -> str:
    from services.downloader import download_direct
    job = await wait_for_job(api_key, job_id, timeout_s=timeout_s, progress_cb=progress_cb)
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
# Multi-key support — FIX FC-USAGE-EP
# ─────────────────────────────────────────────────────────────

def parse_fc_keys(raw: str) -> list[str]:
    if not raw: return []
    import re as _re2
    parts = _re2.split(r"[,\s\n]+", raw.strip())
    return [p.strip() for p in parts if p.strip()]


async def _fc_get_usage(api_key: str) -> float:
    """
    FIX FC-USAGE-EP: old code hit /v1/process/usage which returns 404.
    Try the real endpoints; on any ambiguity return a LARGE number so the
    job is attempted (FC will return a clean error if truly exhausted).
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    # Real endpoints that exist in FreeConvert v1:
    endpoints = [
        f"{_FC_ROOT}/account",
        f"{_FC_ROOT}/user",
    ]
    for endpoint in endpoints:
        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
                async with sess.get(endpoint, headers=headers) as resp:
                    if resp.status == 401:
                        log.warning("[FC-API] Key ...%s returned 401 on %s — invalid",
                                    api_key[-6:], endpoint)
                        return 0.0
                    if resp.status == 404:
                        continue   # try next endpoint
                    if resp.status in (429,):
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

            # Try common field names for "remaining"
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
            used = next((float(inner[k]) for k in
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

            # Data present but no known fields → assume available
            log.info("[FC-API] Key ...%s: usage response has unrecognized schema "
                     "(keys=%s) — assuming available", api_key[-6:],
                     list(inner.keys())[:8])
            return 1e6

        except Exception as exc:
            log.debug("[FC-API] Endpoint %s failed: %s", endpoint, exc)
            continue

    # All endpoints failed — assume key is available rather than falsely
    # reporting "exhausted"
    log.info("[FC-API] Key ...%s: usage endpoints unreachable — assuming available",
             api_key[-6:])
    return 1e6


async def pick_best_fc_key(keys: list[str]) -> tuple[str, float]:
    if not keys:
        raise RuntimeError(
            "No FreeConvert API keys configured.\n"
            "Add FC_API_KEY=your_key to .env or Colab secrets."
        )

    results = await asyncio.gather(*[_fc_get_usage(k) for k in keys])
    best_idx = int(max(range(len(results)), key=lambda i: results[i]))
    best_key = keys[best_idx]
    best_val = results[best_idx]

    # Only refuse if EVERY key came back exactly 0.0 (confirmed 401 or exhausted)
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
    raw = os.environ.get("FC_API_KEY", "").strip()
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
