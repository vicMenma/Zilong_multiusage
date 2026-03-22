"""
services/cloudconvert_api.py
CloudConvert API v2 client — specifically for hardsubbing video + subtitle.

Flow:
  1. Create a job with: import-video, import-subtitle, command (ffmpeg hardsub), export
  2. Upload video + subtitle to the import tasks
  3. Webhook (already in cloudconvert_hook.py) handles the finished export

Uses the `command` task type for full FFmpeg control:
  ffmpeg -i /input/import-video/<file> -vf subtitles=/input/import-sub/<file> \
         -c:v libx264 -crf 20 -preset medium -c:a copy /output/<output>.mp4
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

CC_API = "https://api.cloudconvert.com/v2"


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
    """
    Create a CloudConvert job that hardcodes a subtitle into a video.

    Two modes:
      - video_url set     → CloudConvert fetches the video via import/url
      - video_url is None → returns an import/upload task (caller must upload)

    Subtitle is always import/upload (small file, fast upload).

    Returns the full job response dict including task IDs and upload URLs.
    """
    # Sanitise filenames for FFmpeg path safety
    v_safe = video_filename.replace("'", "\\'").replace(" ", "_")
    s_safe = subtitle_filename.replace("'", "\\'").replace(" ", "_")
    o_safe = output_filename.replace("'", "\\'").replace(" ", "_")

    # Build tasks
    tasks: dict = {}

    # ── Import video ──────────────────────────────────────────
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

    # ── Import subtitle (always upload) ───────────────────────
    tasks["import-sub"] = {
        "operation": "import/upload",
    }

    # ── FFmpeg hardsub command ────────────────────────────────
    # Uses the `command` task for full FFmpeg control.
    # /input/<task-name>/<filename> is how CC maps imported files.
    # /output/<filename> is where the result goes.
    #
    # The subtitles filter in FFmpeg needs the file path escaped
    # with colons and backslashes for the filtergraph.
    sub_path = f"/input/import-sub/{s_safe}"
    # Escape for FFmpeg subtitles filter: colons and backslashes
    sub_escaped = sub_path.replace("\\", "\\\\").replace(":", "\\:")

    # Build -vf filter chain: optional scale + subtitles
    if scale_height > 0:
        # Scale first (so subtitles render at the target resolution),
        # -2 keeps width divisible by 2 for h264
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
        "input": ["import-video", "import-sub"],
        "engine": "ffmpeg",
        "command": "ffmpeg",
        "arguments": ffmpeg_args,
    }

    # ── Export result ─────────────────────────────────────────
    tasks["export"] = {
        "operation": "export/url",
        "input": ["hardsub"],
    }

    # ── Create the job ────────────────────────────────────────
    payload = {
        "tasks": tasks,
        "tag": "zilong-hardsub",
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
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


def _find_task(job: dict, name: str) -> Optional[dict]:
    """Find a task by name in the job response."""
    for task in job.get("tasks", []):
        if task.get("name") == name:
            return task
    return None


def get_upload_url(task: dict) -> Optional[str]:
    """Extract the upload URL from an import/upload task."""
    result = task.get("result") or {}
    form = result.get("form") or {}
    return form.get("url")


def get_upload_params(task: dict) -> dict:
    """Extract the upload form parameters from an import/upload task."""
    result = task.get("result") or {}
    form = result.get("form") or {}
    return form.get("parameters") or {}


async def upload_file_to_task(
    task: dict,
    file_path: str,
    filename: Optional[str] = None,
) -> None:
    """
    Upload a local file to a CloudConvert import/upload task.

    Uses multipart form upload with the parameters from the task result.
    """
    url = get_upload_url(task)
    params = get_upload_params(task)

    if not url:
        raise RuntimeError("No upload URL in task — task may not be in 'waiting' state")

    fname = filename or os.path.basename(file_path)
    fsize = os.path.getsize(file_path)
    log.info("[CC-API] Uploading %s (%s bytes) to %s", fname, fsize, url[:60])

    data = aiohttp.FormData()
    # All form parameters must come before the file
    for key, value in params.items():
        data.add_field(key, str(value))
    # File must be the last field
    data.add_field(
        "file",
        open(file_path, "rb"),
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


async def submit_hardsub(
    api_key: str,
    video_path: Optional[str] = None,
    video_url: Optional[str] = None,
    subtitle_path: str = "",
    output_name: str = "hardsub.mp4",
    crf: int = 20,
    scale_height: int = 0,
) -> str:
    """
    High-level: create hardsub job, upload files, return job ID.

    The existing webhook in cloudconvert_hook.py will handle the
    finished export automatically.

    Args:
        api_key:       CloudConvert API key
        video_path:    Local path to video (for upload mode)
        video_url:     Direct URL to video (CloudConvert fetches it)
        subtitle_path: Local path to .ass / .srt subtitle file
        output_name:   Desired output filename
        crf:           FFmpeg CRF quality (lower = better, 18-23 recommended)
        scale_height:  Target height in pixels (0 = keep original)

    Returns:
        Job ID string
    """
    if not video_path and not video_url:
        raise ValueError("Provide either video_path or video_url")
    if not subtitle_path or not os.path.isfile(subtitle_path):
        raise ValueError(f"Subtitle file not found: {subtitle_path}")

    video_fname = os.path.basename(video_path) if video_path else video_url.split("/")[-1].split("?")[0]
    sub_fname = os.path.basename(subtitle_path)

    # Create the job
    job = await create_hardsub_job(
        api_key,
        video_url=video_url if not video_path else None,
        video_filename=video_fname,
        subtitle_filename=sub_fname,
        output_filename=output_name,
        crf=crf,
        scale_height=scale_height,
    )

    job_id = job.get("id", "?")

    # Upload subtitle (always needed)
    sub_task = _find_task(job, "import-sub")
    if sub_task:
        await upload_file_to_task(sub_task, subtitle_path, sub_fname)
    else:
        raise RuntimeError("No import-sub task found in job")

    # Upload video if local file (not URL import)
    if video_path:
        vid_task = _find_task(job, "import-video")
        if vid_task:
            await upload_file_to_task(vid_task, video_path, video_fname)
        else:
            raise RuntimeError("No import-video task found in job")

    log.info("[CC-API] Hardsub job submitted: %s → %s", job_id, output_name)
    return job_id


async def check_job_status(api_key: str, job_id: str) -> dict:
    """Check the status of a CloudConvert job."""
    headers = {"Authorization": f"Bearer {api_key}"}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
            data = await resp.json()
    return data.get("data", data)
