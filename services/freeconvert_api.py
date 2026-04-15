"""
services/freeconvert_api.py
FreeConvert.com API v1 client — video convert and compress.

Used as a cloud alternative to local FFmpeg for convert/resize operations.
CloudConvert remains the exclusive provider for hardsub (subtitle burn-in).

SETUP — SINGLE KEY
──────────────────
Get a free API key at: freeconvert.com → Account → API Keys (free tier)
Add to .env or Colab Secrets:

    FC_API_KEY=your_key_here

SETUP — MULTIPLE KEYS (rotation)
─────────────────────────────────
Separate keys with commas or newlines — the bot picks the key with the most
remaining minute-credits before each job:

    FC_API_KEY=key1,key2,key3
    # or
    FC_API_KEY=key1
    FC_API_KEY_2=key2
    FC_API_KEY_3=key3

Use parse_fc_keys() + pick_best_fc_key() exactly like the CloudConvert helpers.

FREE TIER LIMITS (per key)
──────────────────────────
  • 25 conversion-minutes / day
  • 1 GB max file size per job
  • Export URLs expire after 24 hours

USAGE (example)
───────────────
    from services.freeconvert_api import (
        get_fc_api_key, parse_fc_keys, pick_best_fc_key,
        submit_convert, submit_compress, download_result,
    )

    # Auto-pick best key from env:
    api_key = get_fc_api_key()

    # Multi-key: pick key with most remaining credits:
    keys = parse_fc_keys(os.environ.get("FC_API_KEY", ""))
    best_key, minutes_left = await pick_best_fc_key(keys)

    # Convert + resize to 720p:
    job_id = await submit_convert(best_key, video_path=path, scale_height=720)

    # Compress to 150 MB:
    job_id = await submit_compress(best_key, video_path=path, target_mb=150)

    # Wait for job and download result:
    local_path = await download_result(best_key, job_id, dest_dir="/tmp/out")
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_FC_BASE = "https://api.freeconvert.com/v1/process"
_TIMEOUT_SHORT = aiohttp.ClientTimeout(total=30)
_TIMEOUT_UPLOAD = aiohttp.ClientTimeout(total=7200)


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

async def _fc_get_job(api_key: str, job_id: str) -> dict:
    """Fetch the current state of a job."""
    headers = {"Authorization": f"Bearer {api_key}"}
    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.get(f"{_FC_BASE}/jobs/{job_id}", headers=headers) as resp:
            data = await resp.json()
    return data.get("data") or data


async def _wait_for_task_ready(
    api_key: str, job_id: str, task_name: str, timeout: int = 120,
) -> dict:
    """
    Poll until the named import/upload task reaches 'waiting' state
    with a valid upload form URL.  Uploading before this causes instant ERROR.
    """
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

    raise RuntimeError(f"[FC] Import task '{task_name}' never reached waiting state in {timeout}s")


# ─────────────────────────────────────────────────────────────
# File upload
# ─────────────────────────────────────────────────────────────

async def upload_file_to_task(
    api_key: str, job_id: str, task_name: str, file_path: str,
) -> None:
    """Upload a local file to a FreeConvert import/upload task."""
    task = await _wait_for_task_ready(api_key, job_id, task_name)
    result = task.get("result") or {}
    form   = result.get("form") or {}

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
                    raise RuntimeError(
                        f"[FC] Upload failed ({resp.status}): {body[:200]}"
                    )

    log.info("[FC-API] Upload complete: %s", fname)


# ─────────────────────────────────────────────────────────────
# Job creation — Convert / Resize
# ─────────────────────────────────────────────────────────────

async def create_convert_job(
    api_key: str,
    *,
    input_url:     Optional[str] = None,
    input_path:    Optional[str] = None,
    output_format: str  = "mp4",
    scale_height:  int  = 0,
    crf:           int  = 23,
) -> str:
    """
    Create a convert/resize job.  Returns the job ID.
    Uses libx264 + AAC for maximum compatibility.
    """
    if not input_url and not input_path:
        raise ValueError("[FC] Provide either input_url or input_path")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    tasks: dict = {}
    if input_url:
        tasks["import-file"] = {"operation": "import/url", "url": input_url}
    else:
        tasks["import-file"] = {"operation": "import/upload"}

    convert_opts: dict = {
        "video_codec": "libx264",
        "crf":         crf,
        "audio_codec": "aac",
        "audio_bitrate": "128k",
    }
    if scale_height > 0:
        convert_opts["scale"] = f"-2:{scale_height}"

    tasks["convert-file"] = {
        "operation":     "convert",
        "input":         "import-file",
        "output_format": output_format,
        "options":       convert_opts,
    }
    tasks["export"] = {"operation": "export/url", "input": "convert-file"}

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(f"{_FC_BASE}/jobs", json={"tasks": tasks}, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"[FC] Convert job creation failed ({resp.status}): "
                    f"{data.get('message', str(data))}"
                )

    job_id = (data.get("data") or data).get("id", "?")
    log.info("[FC-API] Convert job created: %s  scale=%s  crf=%d",
             job_id, f"{scale_height}p" if scale_height else "original", crf)
    return job_id


# ─────────────────────────────────────────────────────────────
# Job creation — Compress to target size
# ─────────────────────────────────────────────────────────────

async def create_compress_job(
    api_key: str,
    *,
    input_url:     Optional[str] = None,
    input_path:    Optional[str] = None,
    target_mb:     float = 50.0,
    output_format: str   = "mp4",
) -> str:
    """
    Create a compress job targeting a specific file size in MB.
    Returns the job ID.
    """
    if not input_url and not input_path:
        raise ValueError("[FC] Provide either input_url or input_path")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    tasks: dict = {}
    if input_url:
        tasks["import-file"] = {"operation": "import/url", "url": input_url}
    else:
        tasks["import-file"] = {"operation": "import/upload"}

    tasks["compress-file"] = {
        "operation":     "compress",
        "input":         "import-file",
        "output_format": output_format,
        "options":       {"target_size": int(target_mb * 1024 * 1024)},
    }
    tasks["export"] = {"operation": "export/url", "input": "compress-file"}

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(f"{_FC_BASE}/jobs", json={"tasks": tasks}, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"[FC] Compress job creation failed ({resp.status}): "
                    f"{data.get('message', str(data))}"
                )

    job_id = (data.get("data") or data).get("id", "?")
    log.info("[FC-API] Compress job created: %s  target=%.0f MB", job_id, target_mb)
    return job_id


# ─────────────────────────────────────────────────────────────
# Job polling
# ─────────────────────────────────────────────────────────────

async def wait_for_job(
    api_key:       str,
    job_id:        str,
    timeout_s:     int   = 7200,
    poll_interval: float = 5.0,
) -> dict:
    """
    Poll until the job reaches 'completed' or 'failed'.
    Returns the final job dict.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job    = await _fc_get_job(api_key, job_id)
        status = job.get("status", "")

        if status == "completed":
            log.info("[FC-API] Job %s completed", job_id)
            return job
        elif status in ("failed", "cancelled", "error"):
            msg = job.get("message") or f"Job {status}"
            raise RuntimeError(f"[FC] Job {job_id} {status}: {msg}")

        log.debug("[FC-API] Job %s status=%s", job_id, status)
        await asyncio.sleep(poll_interval)

    raise RuntimeError(f"[FC] Job {job_id} timed out after {timeout_s}s")


# ─────────────────────────────────────────────────────────────
# Export URL extraction
# ─────────────────────────────────────────────────────────────

def get_export_url(job: dict) -> str:
    """Extract the download URL from a completed job's export task."""
    for task in (job.get("tasks") or []):
        if task.get("operation") in ("export/url",) and task.get("status") == "completed":
            files = (task.get("result") or {}).get("files") or []
            if files:
                return files[0].get("url", "")
    return ""


# ─────────────────────────────────────────────────────────────
# High-level submit helpers
# ─────────────────────────────────────────────────────────────

async def submit_convert(
    api_key: str,
    *,
    video_path:   Optional[str] = None,
    video_url:    Optional[str] = None,
    scale_height: int  = 0,
    crf:          int  = 23,
    output_name:  str  = "converted.mp4",
) -> str:
    """
    Submit a convert/resize job and (if local file) upload it.
    Returns the job ID — non-blocking, caller polls via wait_for_job().
    """
    job_id = await create_convert_job(
        api_key,
        input_url=video_url,
        input_path=video_path,
        scale_height=scale_height,
        crf=crf,
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
) -> str:
    """
    Submit a compress job. Returns the job ID.
    """
    job_id = await create_compress_job(
        api_key,
        input_url=video_url,
        input_path=video_path,
        target_mb=target_mb,
    )
    if video_path and not video_url:
        await upload_file_to_task(api_key, job_id, "import-file", video_path)
    return job_id


async def download_result(
    api_key:     str,
    job_id:      str,
    dest_dir:    str,
    output_name: str = "",
) -> str:
    """
    Wait for job completion then download the result.
    Returns the local file path.
    """
    from services.downloader import download_direct

    job = await wait_for_job(api_key, job_id)
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

    log.info("[FC-API] Result downloaded: %s", os.path.basename(local_path))
    return local_path


# ─────────────────────────────────────────────────────────────
# Multi-key support
# ─────────────────────────────────────────────────────────────

def parse_fc_keys(raw: str) -> list[str]:
    """
    Parse one or more FreeConvert API keys from a raw string.

    Accepts comma-separated, newline-separated, or space-separated keys.
    Strips whitespace and filters empty strings.

    Example inputs that all produce ["key1", "key2", "key3"]:
        "key1,key2,key3"
        "key1\\nkey2\\nkey3"
        "key1 key2 key3"
    """
    if not raw:
        return []
    import re as _re
    parts = _re.split(r"[,\s\n]+", raw.strip())
    return [p.strip() for p in parts if p.strip()]


async def _fc_get_usage(api_key: str) -> float:
    """
    Fetch remaining conversion-minutes for a key.
    Returns remaining minutes as a float (higher = more quota left).
    Returns 0.0 on error so exhausted/invalid keys sort to the end.

    FreeConvert usage endpoint: GET /v1/process/usage
    Response: {"minutes_used": N, "minutes_limit": M, ...}
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
            async with sess.get(
                "https://api.freeconvert.com/v1/process/usage",
                headers=headers,
            ) as resp:
                if resp.status == 401:
                    log.warning("[FC-API] Key invalid or expired (401)")
                    return 0.0
                data = await resp.json()

        # FreeConvert returns minutes used/limit — try common field names
        used  = float(data.get("minutes_used")  or data.get("conversions_used")  or 0)
        limit = float(data.get("minutes_limit") or data.get("conversions_limit") or 25)
        remaining = max(0.0, limit - used)
        log.debug("[FC-API] Key ...%s: %.1f / %.1f minutes remaining",
                  api_key[-6:], remaining, limit)
        return remaining
    except Exception as exc:
        log.warning("[FC-API] Usage check failed for key ...%s: %s", api_key[-6:], exc)
        return 0.0


async def pick_best_fc_key(keys: list[str]) -> tuple[str, float]:
    """
    Given a list of FreeConvert API keys, return (best_key, minutes_remaining)
    where best_key is the one with the most remaining conversion-minutes.

    Raises RuntimeError if all keys are exhausted or the list is empty.

    Usage:
        keys = parse_fc_keys(os.environ.get("FC_API_KEY", ""))
        key, minutes = await pick_best_fc_key(keys)
        job_id = await submit_convert(key, ...)
    """
    if not keys:
        raise RuntimeError(
            "No FreeConvert API keys configured.\n"
            "Add FC_API_KEY=your_key to .env or Colab secrets."
        )

    results = await asyncio.gather(*[_fc_get_usage(k) for k in keys])
    best_idx     = int(max(range(len(results)), key=lambda i: results[i]))
    best_key     = keys[best_idx]
    best_minutes = results[best_idx]

    if best_minutes <= 0:
        raise RuntimeError(
            f"All {len(keys)} FreeConvert API key(s) are exhausted for today.\n"
            "Free tier resets at midnight UTC. Add more keys or wait until reset."
        )

    log.info(
        "[FC-API] Selected key %d/%d  (...%s)  %.1f minutes remaining",
        best_idx + 1, len(keys), best_key[-6:], best_minutes,
    )
    return best_key, best_minutes


def get_fc_api_key() -> str:
    """
    Read and return the first available FreeConvert API key from the environment.

    Checks FC_API_KEY first (supports comma-separated multi-key string).
    Falls back to FC_API_KEY_2, FC_API_KEY_3 … FC_API_KEY_9 as individual keys.

    For async best-key selection use parse_fc_keys() + pick_best_fc_key() instead.
    Raises RuntimeError if nothing is configured.
    """
    raw = os.environ.get("FC_API_KEY", "").strip()
    keys = parse_fc_keys(raw)

    # Also check FC_API_KEY_2 … FC_API_KEY_9
    for i in range(2, 10):
        extra = os.environ.get(f"FC_API_KEY_{i}", "").strip()
        if extra:
            keys.extend(parse_fc_keys(extra))

    if not keys:
        raise RuntimeError(
            "FreeConvert API key not configured.\n"
            "Add FC_API_KEY=your_key to .env or Colab secrets.\n"
            "Get a free key at: freeconvert.com → Account → API Keys"
        )

    return keys[0]


# ─────────────────────────────────────────────────────────────
# Hardsub job (subtitle burn-in)
# ─────────────────────────────────────────────────────────────

async def create_hardsub_job(
    api_key:       str,
    *,
    video_url:     Optional[str] = None,    # import from URL
    sub_url:       Optional[str] = None,    # subtitle from URL
    output_format: str  = "mp4",
    crf:           int  = 20,
    preset:        str  = "medium",
    scale_height:  int  = 0,
    webhook_url:   Optional[str] = None,
) -> dict:
    """
    Create a FreeConvert hardsub job (subtitle burn-in via FFmpeg subtitles filter).
    Files can be provided as URLs; use upload_file_to_task() for local files.
    Returns the raw job dict.
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    tasks: dict = {}

    # Video import
    if video_url:
        tasks["import-video"] = {"operation": "import/url", "url": video_url}
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    # Subtitle import
    if sub_url:
        tasks["import-subtitle"] = {"operation": "import/url", "url": sub_url}
    else:
        tasks["import-subtitle"] = {"operation": "import/upload"}

    # FFmpeg convert with subtitle filter
    convert_opts: dict = {
        "video_codec":  "libx264",
        "preset":       preset,
        "crf":          crf,
        "audio_codec":  "aac",
        "audio_bitrate": "192k",
        "movflags":     "+faststart",
        # Subtitle burn-in: reference the subtitle import task by name
        "subtitle_task": "import-subtitle",
        "subtitle_burn": True,
    }
    if scale_height > 0:
        convert_opts["scale"] = f"-2:{scale_height}"

    tasks["hardsub"] = {
        "operation":     "convert",
        "input":         "import-video",
        "output_format": output_format,
        "options":       convert_opts,
    }
    tasks["export"] = {"operation": "export/url", "input": "hardsub"}

    payload: dict = {"tasks": tasks}
    if webhook_url:
        payload["webhook_url"] = webhook_url

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(
            f"{_FC_BASE}/jobs", json=payload, headers=headers
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"[FC] Hardsub job creation failed ({resp.status}): "
                    f"{data.get('message', str(data))}"
                )

    job_id = (data.get("data") or data).get("id", "?")
    log.info("[FC-API] Hardsub job created: %s  crf=%d  preset=%s", job_id, crf, preset)
    return data.get("data") or data


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
    """
    Submit a FreeConvert hardsub job.
    Returns the job_id.

    Provide local files (video_path / subtitle_path) or remote URLs
    (video_url / subtitle_url) — or mix.

    If webhook_url is set, FreeConvert will POST the completed job JSON
    to that URL instead of requiring polling.
    """
    if not video_path and not video_url:
        raise ValueError("[FC] Provide either video_path or video_url")
    if not subtitle_path and not subtitle_url:
        raise ValueError("[FC] Provide either subtitle_path or subtitle_url")

    job = await create_hardsub_job(
        api_key,
        video_url=video_url,
        sub_url=subtitle_url,
        crf=crf,
        preset=preset,
        scale_height=scale_height,
        webhook_url=webhook_url,
    )
    job_id = job.get("id", "")
    if not job_id:
        raise RuntimeError("[FC] Hardsub job creation returned no ID")

    # Upload local files if needed
    if video_path and not video_url:
        await upload_file_to_task(api_key, job_id, "import-video", video_path)
    if subtitle_path and not subtitle_url:
        await upload_file_to_task(api_key, job_id, "import-subtitle", subtitle_path)

    log.info("[FC-API] Hardsub job submitted: %s  out=%s  webhook=%s",
             job_id, output_name, "yes" if webhook_url else "no")
    return job_id


# ─────────────────────────────────────────────────────────────
# Webhook URL injection helpers
# ─────────────────────────────────────────────────────────────

def fc_webhook_url(base_url: str) -> str:
    """
    Build the FreeConvert webhook callback URL.
    base_url should be the public tunnel URL without trailing slash.

    Example:
        base_url = "https://abc123.trycloudflare.com"
        → "https://abc123.trycloudflare.com/fc-webhook"
    """
    return base_url.rstrip("/") + "/fc-webhook"
