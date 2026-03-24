"""
services/cloudconvert_api.py
CloudConvert API v2 client — hardsubbing + conversion with multi-key rotation.

Multi-API key support:
  Set CC_API_KEY to a comma-separated list of keys:
    CC_API_KEY=eyJ...key1,eyJ...key2,eyJ...key3
  The bot checks remaining credits on each key via GET /v2/users/me
  and picks the one with the most minutes available.

Flows:
  1. submit_hardsub  — burn subtitles into video
  2. submit_convert  — resolution/format conversion (no subtitles)

FIXES:
  - upload_file_to_task() now polls until the import task is in "waiting"
    state with a valid S3 form URL before uploading. Without this the CC
    task is still in "pending" and the upload instantly returns ERROR.
  - upload_file_to_task() uses a context manager for the file handle so
    it is always closed even if the upload raises — fixes FD leak.
  - _wait_for_task_ready() added as a proper polling helper.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

CC_API = "https://api.cloudconvert.com/v2"


# ─────────────────────────────────────────────────────────────
# Multi-key credit checking & rotation
# ─────────────────────────────────────────────────────────────

def parse_api_keys(raw: str) -> list[str]:
    """Parse comma-separated API keys from env var."""
    return [k.strip() for k in raw.split(",") if k.strip()]


async def check_credits(api_key: str) -> int:
    """
    Check remaining conversion credits for a single API key.
    Returns credits (minutes) remaining, or -1 on error.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with aiohttp.ClientSession() as sess:
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
    """
    Check credits on all keys concurrently, return the one with most credits.
    Returns (best_key, credits). Raises RuntimeError if all keys exhausted.
    """
    if len(api_keys) == 1:
        credits = await check_credits(api_keys[0])
        if credits == 0:
            raise RuntimeError(
                "CloudConvert: 0 credits remaining on your only API key.\n"
                "Wait for daily reset or add more keys (comma-separated in CC_API_KEY)."
            )
        return api_keys[0], max(credits, 0)

    tasks = [check_credits(key) for key in api_keys]
    results = await asyncio.gather(*tasks)

    best_key = ""
    best_credits = -1
    status_lines = []

    for key, credits in zip(api_keys, results):
        short = key[-8:]
        status_lines.append(f"  ...{short}: {credits} credits")
        if credits > best_credits:
            best_credits = credits
            best_key = key

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
    """Check the status of a CloudConvert job."""
    keys = parse_api_keys(api_key)
    key = keys[0] if keys else api_key
    headers = {"Authorization": f"Bearer {key}"}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
            data = await resp.json()
    return data.get("data", data)


async def _wait_for_task_ready(
    api_key: str, job_id: str, task_name: str, timeout: int = 120
) -> dict:
    """
    Poll GET /v2/jobs/{job_id} until the named task reaches 'waiting' status
    and its result.form.url is populated (i.e. the S3 upload endpoint is ready).

    This is mandatory before calling upload_file_to_task — firing the upload
    while the task is still 'pending' causes an instant ERROR from CC.

    Returns the task dict with a valid form URL.
    Raises RuntimeError if the task never becomes ready within `timeout` seconds.
    """
    keys = parse_api_keys(api_key)
    key  = keys[0] if keys else api_key
    headers  = {"Authorization": f"Bearer {key}"}
    deadline = time.time() + timeout

    while time.time() < deadline:
        async with aiohttp.ClientSession() as sess:
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
            # status is waiting but form URL not populated yet — keep polling
        elif status in ("error",):
            err = task.get("message") or f"Task '{task_name}' failed"
            raise RuntimeError(f"[CC-API] Task '{task_name}' error: {err}")
        elif status == "finished":
            # Shouldn't happen for import tasks but handle gracefully
            log.warning("[CC-API] Task '%s' already finished — skipping upload poll", task_name)
            return task

        await asyncio.sleep(3)

    raise RuntimeError(
        f"Task '{task_name}' never reached 'waiting' state after {timeout}s. "
        "Check CloudConvert dashboard for job details."
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
    crf: int = 20,
    preset: str = "medium",
    scale_height: int = 0,
) -> dict:
    v_safe = video_filename.replace("'", "\\'").replace(" ", "_")
    s_safe = subtitle_filename.replace("'", "\\'").replace(" ", "_")
    o_safe = output_filename.replace("'", "\\'").replace(" ", "_")

    tasks: dict = {}

    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url": video_url,
            "filename": v_safe,
        }
    else:
        tasks["import-video"] = {
            "operation": "import/upload",
        }

    tasks["import-sub"] = {
        "operation": "import/upload",
    }

    sub_path    = f"/input/import-sub/{s_safe}"
    sub_escaped = sub_path.replace("\\", "\\\\").replace(":", "\\:")

    if scale_height > 0:
        vf = f"scale=-2:{scale_height},subtitles='{sub_escaped}'"
    else:
        vf = f"subtitles='{sub_escaped}'"

    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"-vf {vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {'128k' if scale_height and scale_height <= 480 else '192k'} "
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

    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{CC_API}/jobs", json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                error = data.get("message", str(data))
                raise RuntimeError(f"CloudConvert job creation failed ({resp.status}): {error}")

    job = data.get("data", data)
    log.info("[CC-API] Hardsub job created: id=%s  tasks=%d",
             job.get("id"), len(job.get("tasks", [])))
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
    crf: int = 20,
    preset: str = "medium",
    scale_height: int = 0,
) -> dict:
    v_safe = video_filename.replace("'", "\\'").replace(" ", "_")
    o_safe = output_filename.replace("'", "\\'").replace(" ", "_")

    tasks: dict = {}

    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url":       video_url,
            "filename":  v_safe,
        }
    else:
        tasks["import-video"] = {"operation": "import/upload"}

    if scale_height > 0:
        vf  = f"-vf scale=-2:{scale_height}"
        abr = "128k" if scale_height <= 480 else "192k"
    else:
        vf  = ""
        abr = "192k"

    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"{vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {abr} "
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

    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{CC_API}/jobs", json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                error = data.get("message", str(data))
                raise RuntimeError(f"CloudConvert convert job failed ({resp.status}): {error}")

    job = data.get("data", data)
    log.info("[CC-API] Convert job created: id=%s  tasks=%d",
             job.get("id"), len(job.get("tasks", [])))
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

    FIX 1: Polls _wait_for_task_ready() before uploading so we never hit
            the instant-ERROR caused by uploading while the task is still
            in 'pending' state.
    FIX 2: Opens the file with a context manager so the FD is always
            closed even if the upload fails — prevents EMFILE on long runs.
    """
    # If we have enough info, poll until the task is truly ready
    if api_key and job_id and task_name:
        task = await _wait_for_task_ready(api_key, job_id, task_name)

    url = get_upload_url(task)
    if not url:
        raise RuntimeError(
            "No upload URL in task result. "
            "Pass api_key + job_id + task_name so the function can poll for readiness."
        )

    params = get_upload_params(task)
    fname  = filename or os.path.basename(file_path)
    fsize  = os.path.getsize(file_path)
    log.info("[CC-API] Uploading %s (%d bytes) to %s…", fname, fsize, url[:60])

    # FIX: context manager guarantees the FD is closed after the request
    with open(file_path, "rb") as fh:
        data = aiohttp.FormData()
        for key, value in params.items():
            data.add_field(key, str(value))
        data.add_field(
            "file", fh,
            filename=fname.replace(" ", "_"),
        )
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, data=data, allow_redirects=True) as resp:
                if resp.status not in (200, 201, 204, 301, 302):
                    body = await resp.text()
                    raise RuntimeError(
                        f"Upload failed ({resp.status}): {body[:300]}"
                    )

    log.info("[CC-API] Upload complete: %s", fname)


# ─────────────────────────────────────────────────────────────
# High-level submit — Hardsub (with auto key rotation)
# ─────────────────────────────────────────────────────────────

async def submit_hardsub(
    api_key: str,
    video_path:    Optional[str] = None,
    video_url:     Optional[str] = None,
    subtitle_path: str = "",
    output_name:   str = "hardsub.mp4",
    crf:           int = 20,
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
    log.info("[CC-API] Hardsub: using key with %d credits remaining", credits)

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
        scale_height=scale_height,
    )

    job_id = job.get("id", "?")

    # Upload subtitle — always required
    sub_task = _find_task(job, "import-sub")
    if not sub_task:
        raise RuntimeError("No import-sub task found in job")
    await upload_file_to_task(
        sub_task, subtitle_path, sub_fname,
        api_key=selected_key, job_id=job_id, task_name="import-sub",
    )

    # Upload video file only when no URL was provided
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
    crf:          int = 20,
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
