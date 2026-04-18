"""
services/cloudconvert_api.py
CloudConvert API v2 client — hardsubbing + conversion + compression
with multi-key rotation.

WHAT CHANGED
────────────
FIX CC-SIZE-01: audio bitrate is now ALWAYS 128k (was 192k for non-480p).
  Combined with CRF 23 default this brings 720p hardsub output back to
  reasonable sizes (~150-250 MB for a 24-min episode instead of 400 MB).

FIX CC-SIZE-02: default CRF raised from 20 → 23 (FFmpeg "normal quality").
  CRF 20 is already very near-lossless and often produces files 2-3×
  bigger than CRF 23. The user-facing preset in hardsub.py passes whatever
  the user picked, but the library default now matches what we actually
  recommend.

NEW CC-COMPRESS: create_compress_job() + submit_compress() + run_cc_job()
  CloudConvert can now be used as an alternative to FreeConvert for
  /compress — the UI lets the user pick the backend.

NEW CC-RUN: run_cc_job() polls an existing CC job until completion,
  downloads the export URL, and returns a local path. This mirrors
  freeconvert_api.run_fc_job() so resize.py can use either backend
  with the same inline-polling flow.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp

from services.cc_sanitize import sanitize_for_cc

log = logging.getLogger(__name__)

CC_API = "https://api.cloudconvert.com/v2"

_CC_TIMEOUT_SHORT = aiohttp.ClientTimeout(total=30)
_CC_TIMEOUT_UPLOAD = aiohttp.ClientTimeout(total=7200)


# ─────────────────────────────────────────────────────────────
# Multi-key credit checking & rotation
# ─────────────────────────────────────────────────────────────

def parse_api_keys(raw: str) -> list[str]:
    return [k.strip() for k in raw.split(",") if k.strip()]


async def check_credits(api_key: str) -> int:
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with aiohttp.ClientSession(timeout=_CC_TIMEOUT_SHORT) as sess:
            async with sess.get(f"{CC_API}/users/me", headers=headers) as resp:
                if resp.status != 200:
                    log.warning("[CC-API] Credit check failed: HTTP %d", resp.status)
                    return -1
                data = await resp.json()
                credits = data.get("data", {}).get("credits", 0)
                return int(credits)
    except Exception as exc:
        log.warning("[CC-API] Credit check error: %s", exc)
        return -1


async def pick_best_key(api_keys: list[str]) -> tuple[str, int]:
    if len(api_keys) == 1:
        credits = await check_credits(api_keys[0])
        # FIX BUG-CC-CRED: use <= 0 not == 0.
        # check_credits() returns -1 on network/auth errors.
        # The old check (credits == 0) let -1 pass silently, causing jobs to be
        # submitted against an unreachable or invalid key.
        # Consistent with the multi-key path which already uses best_credits <= 0.
        if credits <= 0:
            reason = (
                "network/auth error — check CC_API_KEY and connectivity"
                if credits < 0 else
                "0 credits remaining — wait for daily reset or add more keys (comma-separated)"
            )
            raise RuntimeError(f"CloudConvert API key unusable: {reason}")
        return api_keys[0], credits

    tasks   = [check_credits(key) for key in api_keys]
    results = await asyncio.gather(*tasks)

    best_key     = ""
    best_credits = -1
    status_lines = []

    for key, credits in zip(api_keys, results):
        short = key[-8:]
        status_lines.append(f"  ...{short}: {credits} credits")
        if credits > best_credits:
            best_credits = credits
            best_key     = key

    log.info("[CC-API] Key rotation — %d keys checked:\n%s",
             len(api_keys), "\n".join(status_lines))

    if best_credits <= 0:
        raise RuntimeError(
            f"CloudConvert: all {len(api_keys)} API keys exhausted (0 credits).\n"
            "Wait for daily reset or add more keys."
        )

    log.info("[CC-API] Selected key ...%s (%d credits remaining)", best_key[-8:], best_credits)
    return best_key, best_credits


# ─────────────────────────────────────────────────────────────
# Job status polling
# ─────────────────────────────────────────────────────────────

async def check_job_status(api_key: str, job_id: str) -> dict:
    """
    Query the status of a CC job.

    `api_key` may be either a single key or the comma-separated CC_API_KEY
    string. If multiple keys are passed, each one is tried until a non-404
    response is seen — this ensures jobs created with key #2 are pollable
    even when CC_API_KEY lists key #1 first.
    """
    keys = parse_api_keys(api_key) or [api_key]
    last_err: Optional[Exception] = None

    for key in keys:
        headers = {"Authorization": f"Bearer {key}"}
        try:
            async with aiohttp.ClientSession(timeout=_CC_TIMEOUT_SHORT) as sess:
                async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
                    # 404 likely means the job belongs to a different account.
                    if resp.status == 404:
                        continue
                    data = await resp.json()
                    return data.get("data", data)
        except Exception as exc:
            last_err = exc
            continue

    if last_err:
        raise last_err
    # All 404s → return an empty dict rather than crashing the caller.
    return {}


async def _wait_for_task_ready(
    api_key: str, job_id: str, task_name: str, timeout: int = 120
) -> dict:
    """
    Poll until the named import/upload task reaches 'waiting' state
    with a valid S3 form URL. Uploading before this causes instant ERROR.
    """
    keys     = parse_api_keys(api_key)
    key      = keys[0] if keys else api_key
    headers  = {"Authorization": f"Bearer {key}"}
    deadline = time.time() + timeout

    while time.time() < deadline:
        async with aiohttp.ClientSession(timeout=_CC_TIMEOUT_SHORT) as sess:
            async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
                data = await resp.json()

        job  = data.get("data", data)
        task = _find_task(job, task_name)
        if not task:
            raise RuntimeError(
                f"Task '{task_name}' not found in job {job_id}. "
                "Job may have failed before task was created."
            )

        status = task.get("status", "")
        if status == "waiting":
            url = get_upload_url(task)
            if url:
                log.debug("[CC-API] Task '%s' ready (waiting) with S3 URL", task_name)
                return task
        elif status == "error":
            err = task.get("message") or f"Task '{task_name}' failed"
            raise RuntimeError(f"[CC-API] Task '{task_name}' error: {err}")
        elif status == "finished":
            log.warning("[CC-API] Task '%s' already finished — skipping upload poll", task_name)
            return task

        await asyncio.sleep(3)

    raise RuntimeError(
        f"Task '{task_name}' never reached 'waiting' state after {timeout}s."
    )


# ─────────────────────────────────────────────────────────────
# Job creation — Hardsub
# ─────────────────────────────────────────────────────────────

async def create_hardsub_job(
    api_key: str,
    *,
    video_url: Optional[str] = None,
    video_filename: str = "video.mkv",
    subtitle_filename: str = "subtitle.ass",
    output_filename: str = "output.mp4",
    crf: int = 23,
    preset: str = "medium",
    scale_height: int = 0,
) -> dict:
    # PATCH: sanitize_for_cc() reduces filenames to [a-zA-Z0-9._-] only —
    # CloudConvert silently fails on brackets, colons, accented chars, Unicode.
    v_safe = sanitize_for_cc(video_filename)
    s_safe = sanitize_for_cc(subtitle_filename)
    o_safe = sanitize_for_cc(output_filename)

    log.debug("[CC-API] Sanitized names: video=%s sub=%s out=%s", v_safe, s_safe, o_safe)

    tasks: dict = {}

    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url":       video_url,
            "filename":  v_safe,
        }
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    tasks["import-sub"] = {"operation": "import/upload"}

    sub_path    = f"/input/import-sub/{s_safe}"
    sub_escaped = sub_path.replace("\\", "\\\\").replace(":", "\\:")

    if scale_height > 0:
        vf = f"scale=-2:{scale_height},subtitles='{sub_escaped}'"
    else:
        vf = f"subtitles='{sub_escaped}'"

    # FIX CC-SIZE-01: audio bitrate always 128k — 192k produced oversized 720p
    # hardsubs (~400 MB for a 24 min episode). 128k AAC is transparent for
    # stereo anime audio and matches the user-facing preset.
    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"-vf {vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a 128k "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    )

    tasks["hardsub"] = {
        "operation": "command",
        "input":     ["import-video", "import-sub"],
        "engine":    "ffmpeg",
        "command":   "ffmpeg",
        "arguments": ffmpeg_args,
    }

    tasks["export"] = {
        "operation": "export/url",
        "input":     ["hardsub"],
    }

    payload = {"tasks": tasks, "tag": "zilong-hardsub"}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    async with aiohttp.ClientSession(timeout=_CC_TIMEOUT_SHORT) as sess:
        async with sess.post(f"{CC_API}/jobs", json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                error = data.get("message", str(data))
                raise RuntimeError(f"CloudConvert job creation failed ({resp.status}): {error}")

    job = data.get("data", data)
    log.info("[CC-API] Hardsub job: id=%s  crf=%d  preset=%s  tasks=%d",
             job.get("id"), crf, preset, len(job.get("tasks", [])))
    return job


# ─────────────────────────────────────────────────────────────
# Job creation — Convert (resolution/format, no subtitles)
# ─────────────────────────────────────────────────────────────

async def create_convert_job(
    api_key: str,
    *,
    video_url: Optional[str] = None,
    video_filename: str = "video.mkv",
    output_filename: str = "output.mp4",
    crf: int = 23,
    preset: str = "medium",
    scale_height: int = 0,
) -> dict:
    v_safe = sanitize_for_cc(video_filename)
    o_safe = sanitize_for_cc(output_filename)

    log.debug("[CC-API] Sanitized names: video=%s out=%s", v_safe, o_safe)

    tasks: dict = {}

    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url":       video_url,
            "filename":  v_safe,
        }
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    vf = f"-vf scale=-2:{scale_height}" if scale_height > 0 else ""

    # FIX CC-SIZE-01: audio bitrate always 128k.
    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"{vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a 128k "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    ).strip()

    tasks["convert"] = {
        "operation": "command",
        "input":     ["import-video"],
        "engine":    "ffmpeg",
        "command":   "ffmpeg",
        "arguments": ffmpeg_args,
    }

    tasks["export"] = {
        "operation": "export/url",
        "input":     ["convert"],
    }

    payload = {"tasks": tasks, "tag": "zilong-convert"}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    async with aiohttp.ClientSession(timeout=_CC_TIMEOUT_SHORT) as sess:
        async with sess.post(f"{CC_API}/jobs", json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                error = data.get("message", str(data))
                raise RuntimeError(f"CloudConvert convert job failed ({resp.status}): {error}")

    job = data.get("data", data)
    log.info("[CC-API] Convert job: id=%s  scale=%s  crf=%d  preset=%s",
             job.get("id"),
             f"{scale_height}p" if scale_height else "original",
             crf, preset)
    return job


# ─────────────────────────────────────────────────────────────
# Job creation — Compress (target file size, one-pass CRF + -fs)
# ─────────────────────────────────────────────────────────────

async def create_compress_job(
    api_key: str,
    *,
    video_url:       Optional[str] = None,
    video_filename:  str   = "video.mkv",
    output_filename: str   = "compressed.mp4",
    target_mb:       float = 50.0,
) -> dict:
    """
    Compress a video to approximately `target_mb` MB on CloudConvert.

    Strategy matches FreeConvert compress:
      - libx264 CRF 28 (quality target, trades size for quality)
      - AAC 96k audio (lower than default 128k since we're compressing)
      - -fs <bytes> hard-caps the output file size
    """
    v_safe = sanitize_for_cc(video_filename)
    o_safe = sanitize_for_cc(output_filename)

    tasks: dict = {}
    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url":       video_url,
            "filename":  v_safe,
        }
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    target_bytes = int(target_mb * 1024 * 1024)

    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"-c:v libx264 -crf 28 -preset medium "
        f"-c:a aac -b:a 96k "
        f"-fs {target_bytes} "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    )

    tasks["compress"] = {
        "operation": "command",
        "input":     ["import-video"],
        "engine":    "ffmpeg",
        "command":   "ffmpeg",
        "arguments": ffmpeg_args,
    }

    tasks["export"] = {"operation": "export/url", "input": ["compress"]}

    payload = {"tasks": tasks, "tag": "zilong-compress"}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    async with aiohttp.ClientSession(timeout=_CC_TIMEOUT_SHORT) as sess:
        async with sess.post(f"{CC_API}/jobs", json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                error = data.get("message", str(data))
                raise RuntimeError(f"CloudConvert compress job failed ({resp.status}): {error}")

    job = data.get("data", data)
    log.info("[CC-API] Compress job: id=%s  target=%.0f MB  crf=28",
             job.get("id"), target_mb)
    return job


# ─────────────────────────────────────────────────────────────
# File upload helpers
# ─────────────────────────────────────────────────────────────

def _find_task(job: dict, name: str) -> Optional[dict]:
    for task in job.get("tasks", []):
        if task.get("name") == name:
            return task
    return None


def get_upload_url(task: dict) -> Optional[str]:
    result = task.get("result") or {}
    form   = result.get("form") or {}
    return form.get("url")


def get_upload_params(task: dict) -> dict:
    result = task.get("result") or {}
    form   = result.get("form") or {}
    return form.get("parameters") or {}


async def upload_file_to_task(
    task: dict,
    file_path: str,
    filename: Optional[str] = None,
    api_key: str = "",
    job_id: str = "",
    task_name: str = "",
) -> None:
    """
    Upload a local file to a CloudConvert import/upload task.
    Polls _wait_for_task_ready() first so we never hit the instant-ERROR
    caused by uploading while the task is still 'pending'.
    """
    if api_key and job_id and task_name:
        task = await _wait_for_task_ready(api_key, job_id, task_name)

    url = get_upload_url(task)
    if not url:
        raise RuntimeError(
            "No upload URL in task result. "
            "Pass api_key + job_id + task_name so the function can poll for readiness."
        )

    params = get_upload_params(task)
    raw_fname  = filename or os.path.basename(file_path)
    safe_fname = sanitize_for_cc(raw_fname)
    fsize      = os.path.getsize(file_path)

    log.info("[CC-API] Uploading %s → %s (%d bytes) to %s…",
             raw_fname, safe_fname, fsize, url[:60])

    with open(file_path, "rb") as fh:
        data = aiohttp.FormData()
        for key, value in params.items():
            data.add_field(key, str(value))
        data.add_field("file", fh, filename=safe_fname)
        async with aiohttp.ClientSession(timeout=_CC_TIMEOUT_UPLOAD) as sess:
            async with sess.post(url, data=data, allow_redirects=True) as resp:
                if resp.status not in (200, 201, 204, 301, 302):
                    body = await resp.text()
                    raise RuntimeError(
                        f"Upload failed ({resp.status}): {body[:300]}"
                    )

    log.info("[CC-API] Upload complete: %s", safe_fname)


# ─────────────────────────────────────────────────────────────
# Export URL extraction
# ─────────────────────────────────────────────────────────────

def get_export_url(job: dict) -> str:
    """Pull the first export-task's signed file URL from a finished job."""
    for task in job.get("tasks", []):
        if (task.get("operation") == "export/url"
                and task.get("status") == "finished"):
            files = (task.get("result") or {}).get("files") or []
            if files:
                url = files[0].get("url", "")
                if url:
                    return url
    return ""


# ─────────────────────────────────────────────────────────────
# High-level submit — Hardsub (with auto key rotation)
# ─────────────────────────────────────────────────────────────

async def submit_hardsub(
    api_key: str,
    video_path:    Optional[str] = None,
    video_url:     Optional[str] = None,
    subtitle_path: str = "",
    output_name:   str = "hardsub.mp4",
    crf:           int = 23,
    preset:        str = "medium",
    scale_height:  int = 0,
    user_id:       int = 0,
) -> str:
    if not video_path and not video_url:
        raise ValueError("Provide either video_path or video_url")
    if not subtitle_path or not os.path.isfile(subtitle_path):
        raise ValueError(f"Subtitle file not found: {subtitle_path}")

    keys = parse_api_keys(api_key)
    if not keys:
        raise ValueError("No API keys provided in CC_API_KEY")

    selected_key, credits = await pick_best_key(keys)
    log.info("[CC-API] Hardsub: key with %d credits (preset=%s crf=%d)", credits, preset, crf)

    video_fname = (
        os.path.basename(video_path) if video_path
        else video_url.split("/")[-1].split("?")[0]
    )
    sub_fname = os.path.basename(subtitle_path)

    job = await create_hardsub_job(
        selected_key,
        video_url=video_url if not video_path else None,
        video_filename=video_fname,
        subtitle_filename=sub_fname,
        output_filename=output_name,
        crf=crf,
        preset=preset,
        scale_height=scale_height,
    )

    job_id = job.get("id", "?")

    sub_task = _find_task(job, "import-sub")
    if not sub_task:
        raise RuntimeError("No import-sub task found in job")
    await upload_file_to_task(
        sub_task, subtitle_path, sub_fname,
        api_key=selected_key, job_id=job_id, task_name="import-sub",
    )

    if video_path:
        vid_task = _find_task(job, "import-video")
        if not vid_task:
            raise RuntimeError("No import-video task found in job")
        await upload_file_to_task(
            vid_task, video_path, video_fname,
            api_key=selected_key, job_id=job_id, task_name="import-video",
        )

    log.info("[CC-API] Hardsub job submitted: %s → %s", job_id, output_name)
    return job_id


# ─────────────────────────────────────────────────────────────
# High-level submit — Convert (with auto key rotation)
# ─────────────────────────────────────────────────────────────

async def submit_convert(
    api_key: str,
    video_path:   Optional[str] = None,
    video_url:    Optional[str] = None,
    output_name:  str = "converted.mp4",
    crf:          int = 23,
    preset:       str = "medium",
    scale_height: int = 0,
) -> str:
    if not video_path and not video_url:
        raise ValueError("Provide either video_path or video_url")

    keys = parse_api_keys(api_key)
    if not keys:
        raise ValueError("No API keys provided in CC_API_KEY")

    selected_key, credits = await pick_best_key(keys)
    log.info("[CC-API] Convert: using key with %d credits remaining", credits)

    video_fname = (
        os.path.basename(video_path) if video_path
        else video_url.split("/")[-1].split("?")[0]
    )

    job = await create_convert_job(
        selected_key,
        video_url=video_url if not video_path else None,
        video_filename=video_fname,
        output_filename=output_name,
        crf=crf,
        preset=preset,
        scale_height=scale_height,
    )

    job_id = job.get("id", "?")

    if video_path:
        vid_task = _find_task(job, "import-video")
        if not vid_task:
            raise RuntimeError("No import-video task found in job")
        await upload_file_to_task(
            vid_task, video_path, video_fname,
            api_key=selected_key, job_id=job_id, task_name="import-video",
        )

    log.info("[CC-API] Convert job submitted: %s → %s", job_id, output_name)
    return job_id


# ─────────────────────────────────────────────────────────────
# High-level submit — Compress (with auto key rotation)
# ─────────────────────────────────────────────────────────────

async def submit_compress(
    api_key:      str,
    video_path:   Optional[str] = None,
    video_url:    Optional[str] = None,
    target_mb:    float = 50.0,
    output_name:  str   = "compressed.mp4",
) -> str:
    if not video_path and not video_url:
        raise ValueError("Provide either video_path or video_url")

    keys = parse_api_keys(api_key)
    if not keys:
        raise ValueError("No API keys provided in CC_API_KEY")

    selected_key, credits = await pick_best_key(keys)
    log.info("[CC-API] Compress: key with %d credits (target=%.0f MB)", credits, target_mb)

    video_fname = (
        os.path.basename(video_path) if video_path
        else video_url.split("/")[-1].split("?")[0]
    )

    job = await create_compress_job(
        selected_key,
        video_url=video_url if not video_path else None,
        video_filename=video_fname,
        output_filename=output_name,
        target_mb=target_mb,
    )

    job_id = job.get("id", "?")

    if video_path:
        vid_task = _find_task(job, "import-video")
        if not vid_task:
            raise RuntimeError("No import-video task found in job")
        await upload_file_to_task(
            vid_task, video_path, video_fname,
            api_key=selected_key, job_id=job_id, task_name="import-video",
        )

    log.info("[CC-API] Compress job submitted: %s → %s", job_id, output_name)
    return job_id


# ─────────────────────────────────────────────────────────────
# Job polling + export download (for inline-run callers like resize.py)
# ─────────────────────────────────────────────────────────────

async def wait_for_cc_job(
    api_key:       str,
    job_id:        str,
    timeout_s:     int   = 7200,
    poll_interval: float = 5.0,
    progress_cb         = None,
) -> dict:
    """
    Poll a CC job until it reaches finished or error state.
    Mirrors freeconvert_api.wait_for_job so callers can be symmetric.
    """
    deadline    = time.time() + timeout_s
    start       = time.time()
    _poll_errs  = 0

    while time.time() < deadline:
        try:
            job = await check_job_status(api_key, job_id)
            _poll_errs = 0
        except Exception as exc:
            _poll_errs += 1
            log.warning("[CC-API] Poll error for %s (attempt %d): %s",
                        job_id, _poll_errs, exc)
            if _poll_errs >= 5:
                raise RuntimeError(
                    f"[CC] Job {job_id}: 5 consecutive poll failures — last: {exc}"
                ) from exc
            await asyncio.sleep(min(poll_interval * _poll_errs, 30.0))
            continue

        status = job.get("status", "")
        if status == "finished":
            log.info("[CC-API] Job %s finished", job_id)
            if progress_cb:
                try: await progress_cb(100.0, "✅ Complete")
                except Exception: pass
            return job
        elif status in ("error", "failed", "cancelled"):
            tasks   = job.get("tasks") or []
            err_msg = job.get("message") or ""
            for t in tasks:
                if t.get("status") in ("error", "failed"):
                    t_err = (t.get("message")
                             or (t.get("result") or {}).get("message")
                             or (t.get("result") or {}).get("error") or "")
                    if t_err:
                        err_msg = t_err
                        break
            raise RuntimeError(f"[CC] Job {job_id} {status}: {err_msg or 'Unknown error'}")

        if progress_cb:
            try:
                tasks = job.get("tasks") or []
                if tasks:
                    done_count = sum(1 for t in tasks if (t.get("status") or "") == "finished")
                    pct = min(95.0, done_count / len(tasks) * 100)
                else:
                    elapsed = time.time() - start
                    pct = min(90.0, elapsed / 60 * 30)
                await progress_cb(pct, f"⏳ Processing… ({status})")
            except Exception: pass

        await asyncio.sleep(poll_interval)

    raise RuntimeError(f"[CC] Job {job_id} timed out after {timeout_s}s")


async def run_cc_job(
    api_key:     str,
    job_id:      str,
    dest_dir:    str,
    output_name: str = "",
    progress_cb      = None,
    timeout_s:   int = 7200,
) -> str:
    """
    Wait for a CC job to finish, download the export, and return the
    local path. Mirrors freeconvert_api.run_fc_job().

    Use `download_direct()` under the hood because CC export URLs are
    single-use signed tokens — any retry logic burns them (BUG-01).
    """
    from services.downloader import download_direct

    job = await wait_for_cc_job(api_key, job_id, timeout_s=timeout_s, progress_cb=progress_cb)
    url = get_export_url(job)
    if not url:
        raise RuntimeError(f"[CC] No export URL in completed job {job_id}")

    local_path = await download_direct(url, dest_dir)
    if output_name:
        new_path = os.path.join(dest_dir, output_name)
        try:
            os.rename(local_path, new_path)
            local_path = new_path
        except OSError:
            pass
    log.info("[CC-API] Result: %s", os.path.basename(local_path))
    return local_path
