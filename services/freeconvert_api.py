"""
services/freeconvert_api.py
FreeConvert.com API v1 client — video convert, compress, and hardsub.

FIXES IN THIS VERSION
─────────────────────
FIX FC-01: create_hardsub_job() used fabricated options ('subtitle_task',
  'subtitle_burn') that FreeConvert does not recognise. The convert task also
  only listed the video as its input, so the subtitle file was never accessible.
  Fixed: use FreeConvert's 'command' operation with explicit FFmpeg arguments
  (same pattern as CloudConvert) which is the reliable cross-platform approach.
  The subtitle task is listed in 'depends_on' so it is uploaded before the
  FFmpeg command runs.

FIX FC-02: Tasks used 'input' (string/list) for dependencies. FreeConvert v1
  uses 'depends_on' as an array of task names. Using 'input' works as an alias
  in some endpoints but 'depends_on' is the canonical field and is always safe.

FIX FC-03: create_convert_job() and create_compress_job() also used 'input'
  as a string — updated to 'depends_on' array for consistency and correctness.

FIX FC-04: export task used 'input': ['hardsub'] (list). FreeConvert export
  tasks use 'depends_on' as well; updated to match.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_FC_BASE       = "https://api.freeconvert.com/v1/process"
_TIMEOUT_SHORT  = aiohttp.ClientTimeout(total=30)
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

    raise RuntimeError(
        f"[FC] Import task '{task_name}' never reached waiting state in {timeout}s"
    )


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
# Hardsub — FIX FC-01/FC-02
# ─────────────────────────────────────────────────────────────

async def create_hardsub_job(
    api_key: str,
    *,
    video_url:     Optional[str] = None,
    sub_url:       Optional[str] = None,
    video_fname:   str  = "video.mkv",
    sub_fname:     str  = "subtitle.ass",
    output_fname:  str  = "output.mp4",
    output_format: str  = "mp4",
    crf:           int  = 20,
    preset:        str  = "medium",
    scale_height:  int  = 0,
    webhook_url:   Optional[str] = None,
) -> dict:
    """
    Create a FreeConvert hardsub job via FFmpeg command operation.

    FIX FC-01: Replaced fabricated 'subtitle_task'/'subtitle_burn' options
      with an explicit FFmpeg -vf subtitles filter command.  FreeConvert
      supports a 'command' operation that takes raw FFmpeg arguments —
      this is the reliable approach (same as CloudConvert).

    FIX FC-02: Replaced 'input' with 'depends_on' arrays throughout.
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # ── 1. Sanitize filenames for FFmpeg path safety ──────────
    import re as _re
    def _safe(name: str) -> str:
        return _re.sub(r"[^\w.\-]", "_", name)

    v_safe = _safe(video_fname)
    s_safe = _safe(sub_fname)
    o_safe = _safe(output_fname)

    tasks: dict = {}

    # ── 2. Import tasks ───────────────────────────────────────
    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url":       video_url,
            "filename":  v_safe,
        }
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    if sub_url:
        tasks["import-subtitle"] = {
            "operation": "import/url",
            "url":       sub_url,
            "filename":  s_safe,
        }
    else:
        tasks["import-subtitle"] = {"operation": "import/upload"}

    # ── 3. FFmpeg command for hardsub ─────────────────────────
    # Build the subtitles filter path
    sub_path    = f"/input/import-subtitle/{s_safe}"
    sub_escaped = sub_path.replace(":", "\\:")

    if scale_height > 0:
        vf = f"scale=-2:{scale_height},subtitles='{sub_escaped}'"
    else:
        vf = f"subtitles='{sub_escaped}'"

    abr = "128k" if scale_height and scale_height <= 480 else "192k"

    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"-vf {vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {abr} "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    )

    # FIX FC-01: Use 'command' operation with explicit FFmpeg args
    # FIX FC-02: Use 'depends_on' array, not 'input'
    tasks["hardsub"] = {
        "operation":  "command",
        "depends_on": ["import-video", "import-subtitle"],
        "command":    "ffmpeg",
        "arguments":  ffmpeg_args,
    }

    # ── 4. Export ─────────────────────────────────────────────
    # FIX FC-02: 'depends_on' array, not 'input' list
    tasks["export"] = {
        "operation":  "export/url",
        "depends_on": ["hardsub"],
    }

    # ── 5. Assemble payload ───────────────────────────────────
    payload: dict = {"tasks": tasks}
    if webhook_url:
        payload["webhook_url"] = webhook_url

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(
            f"{_FC_BASE}/jobs", json=payload, headers=headers,
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                msg = (data.get("message") or
                       str(data.get("errors") or data)[:200])
                raise RuntimeError(
                    f"[FC] Hardsub job creation failed ({resp.status}): {msg}"
                )

    job_id = (data.get("data") or data).get("id", "?")
    log.info(
        "[FC-API] Hardsub job created: %s  crf=%d  preset=%s  scale=%s",
        job_id, crf, preset, f"{scale_height}p" if scale_height else "original",
    )
    return data.get("data") or data


# ─────────────────────────────────────────────────────────────
# Convert / Resize — FIX FC-02/FC-03
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
    FIX FC-02/FC-03: uses 'depends_on' instead of 'input' string.
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
        "video_codec":   "libx264",
        "crf":           crf,
        "audio_codec":   "aac",
        "audio_bitrate": "128k",
    }
    if scale_height > 0:
        convert_opts["scale"] = f"-2:{scale_height}"

    # FIX FC-02: 'depends_on' array
    tasks["convert-file"] = {
        "operation":     "convert",
        "depends_on":    ["import-file"],
        "output_format": output_format,
        "options":       convert_opts,
    }
    # FIX FC-04: export also uses 'depends_on'
    tasks["export"] = {
        "operation":  "export/url",
        "depends_on": ["convert-file"],
    }

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(
            f"{_FC_BASE}/jobs", json={"tasks": tasks}, headers=headers,
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"[FC] Convert job creation failed ({resp.status}): "
                    f"{data.get('message', str(data))}"
                )

    job_id = (data.get("data") or data).get("id", "?")
    log.info(
        "[FC-API] Convert job created: %s  scale=%s  crf=%d",
        job_id, f"{scale_height}p" if scale_height else "original", crf,
    )
    return job_id


# ─────────────────────────────────────────────────────────────
# Compress — FIX FC-02/FC-03
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
    FIX FC-02/FC-03: uses 'depends_on' instead of 'input'.
    """
    if not input_url and not input_path:
        raise ValueError("[FC] Provide either input_url or input_path")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    tasks: dict = {}
    if input_url:
        tasks["import-file"] = {"operation": "import/url", "url": input_url}
    else:
        tasks["import-file"] = {"operation": "import/upload"}

    # FIX FC-02: 'depends_on' array
    tasks["compress-file"] = {
        "operation":     "compress",
        "depends_on":    ["import-file"],
        "output_format": output_format,
        "options":       {"target_size": int(target_mb * 1024 * 1024)},
    }
    tasks["export"] = {
        "operation":  "export/url",
        "depends_on": ["compress-file"],
    }

    async with aiohttp.ClientSession(timeout=_TIMEOUT_SHORT) as sess:
        async with sess.post(
            f"{_FC_BASE}/jobs", json={"tasks": tasks}, headers=headers,
        ) as resp:
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
    """Poll until the job reaches 'completed' or 'failed'. Returns final job dict."""
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
# Export URL extraction — handles both FC payload shapes
# ─────────────────────────────────────────────────────────────

def get_export_url(job: dict) -> str:
    """
    Extract the download URL from a completed job's export task.
    Handles both list-form and dict-form tasks, and both 'files' and
    'output' keys in the result (FreeConvert uses both in different contexts).
    """
    tasks = job.get("tasks") or []

    def _try_extract(task: dict) -> str:
        result = task.get("result") or {}
        # FreeConvert may use 'files' or 'output' depending on operation
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
                if url:
                    return url
    elif isinstance(tasks, dict):
        for _name, task in tasks.items():
            op   = (task.get("operation") or "").lower()
            stat = (task.get("status") or "").lower()
            if "export" in op and stat == "completed":
                url = _try_extract(task)
                if url:
                    return url

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
    """Submit a convert/resize job and upload local file if provided."""
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
    """Submit a compress job. Returns the job ID."""
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
    """Wait for job completion then download the result. Returns local path."""
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
# Hardsub high-level submit
# ─────────────────────────────────────────────────────────────

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
    Submit a FreeConvert hardsub job.  Returns the job_id.

    Uses the FFmpeg 'command' operation for reliable subtitle burn-in
    (FIX FC-01: replaced fabricated subtitle_task/subtitle_burn options).

    Provide local files (video_path / subtitle_path) or remote URLs
    (video_url / subtitle_url) — or mix both.
    """
    if not video_path and not video_url:
        raise ValueError("[FC] Provide either video_path or video_url")
    if not subtitle_path and not subtitle_url:
        raise ValueError("[FC] Provide either subtitle_path or subtitle_url")

    video_fname    = os.path.basename(video_path)    if video_path    else (video_url or "video.mkv").split("/")[-1].split("?")[0]
    subtitle_fname = os.path.basename(subtitle_path) if subtitle_path else (subtitle_url or "subtitle.ass").split("/")[-1].split("?")[0]

    job = await create_hardsub_job(
        api_key,
        video_url=video_url,
        sub_url=subtitle_url,
        video_fname=video_fname,
        sub_fname=subtitle_fname,
        output_fname=output_name,
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

    log.info(
        "[FC-API] Hardsub submitted: job=%s  out=%s  webhook=%s",
        job_id, output_name, "yes" if webhook_url else "no",
    )
    return job_id


# ─────────────────────────────────────────────────────────────
# Multi-key support
# ─────────────────────────────────────────────────────────────

def parse_fc_keys(raw: str) -> list[str]:
    """Parse comma/newline/space-separated FC API keys."""
    if not raw:
        return []
    import re as _re
    parts = _re.split(r"[,\s\n]+", raw.strip())
    return [p.strip() for p in parts if p.strip()]


async def _fc_get_usage(api_key: str) -> float:
    """Fetch remaining conversion-minutes for a key. Returns 0.0 on error."""
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
    """Return (best_key, minutes_remaining) — raises if all keys exhausted."""
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
    """Read the first available FreeConvert API key from the environment."""
    raw = os.environ.get("FC_API_KEY", "").strip()
    keys = parse_fc_keys(raw)

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
# Webhook URL helper
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
