"""
services/cc_job_store.py
Persistent JSON store for CloudConvert jobs.
Used by hardsub.py (write) and ccstatus.py (read/poll).

Fields per job:
  job_id, uid, fname, sub_fname, output_name, status, error_msg,
  export_url, finished_at, notified, progress_pct, task_message,
  progress_at, created_at

Finished/error jobs linger for 6 h then are evicted.

FIX RESTART-STALE: _load() now resets any job where notified=True but
  uploaded=False.  These are "claimed-but-not-yet-delivered" jobs from
  a previous session; on restart no delivery is in progress, so the
  claim is stale.  Resetting notified→False puts them back into
  undelivered_jobs() so the startup poller can re-deliver them.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

log = logging.getLogger(__name__)

_STORE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "cc_jobs.json")
)

JOB_LINGER = 6 * 3600  # keep finished/error jobs 6 h


@dataclass
class CCJob:
    job_id:           str
    uid:              int
    fname:            str
    sub_fname:        str   = ""
    output_name:      str   = ""
    status:           str   = "processing"   # processing | finished | error
    error_msg:        str   = ""
    export_url:       str   = ""
    finished_at:      float = 0.0
    notified:         bool  = False       # claim gate (set at start of delivery)
    uploaded:         bool  = False       # NEW: file IS in Telegram (no retry)
    delivering_since: float = 0.0         # NEW: in-flight claim timestamp
    progress_pct:     float = 0.0
    task_message:     str   = ""
    progress_at:      float = field(default_factory=time.time)
    created_at:       float = field(default_factory=time.time)
    # FIX CRIT-01: Seedr deferred cleanup — folder must stay alive until CC
    # finishes pulling the video via import/url.  Cleaned after delivery.
    seedr_folder_id:  int   = 0
    seedr_user:       str   = ""
    seedr_pwd:        str   = ""


class CCJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, CCJob] = {}
        self._lock = asyncio.Lock()
        self._load()

    # ── Persistence ───────────────────────────────────────────

    def _load(self) -> None:
        try:
            with open(_STORE_PATH, encoding="utf-8") as fh:
                raw = json.load(fh)
            for job_id, d in raw.items():
                try:
                    self._jobs[job_id] = CCJob(**d)
                except TypeError:
                    pass  # schema changed — skip stale entry
            log.info("[CCJobStore] Loaded %d jobs from disk", len(self._jobs))
        except FileNotFoundError:
            pass
        except Exception as exc:
            log.warning("[CCJobStore] Load error: %s", exc)

        # FIX RESTART-STALE: Any job with notified=True but uploaded=False
        # was mid-delivery when the bot crashed/restarted.  Since no delivery
        # task is running now, the claim is stale.  Reset it so these jobs
        # surface in undelivered_jobs() and the startup poller can re-deliver.
        _reset = 0
        for job in self._jobs.values():
            if job.notified and not job.uploaded:
                job.notified = False
                job.delivering_since = 0.0
                _reset += 1
        if _reset:
            log.info(
                "[CCJobStore] Reset %d stale delivery claim(s) after restart — "
                "will be re-delivered by startup poller", _reset
            )

    def _save(self) -> None:
        try:
            os.makedirs(os.path.dirname(_STORE_PATH), exist_ok=True)
            with open(_STORE_PATH, "w", encoding="utf-8") as fh:
                json.dump(
                    {jid: asdict(j) for jid, j in self._jobs.items()},
                    fh, indent=2,
                )
        except Exception as exc:
            log.warning("[CCJobStore] Save error: %s", exc)

    # ── Eviction ──────────────────────────────────────────────

    def _evict(self) -> None:
        now  = time.time()
        dead = [
            jid for jid, j in self._jobs.items()
            if j.status in ("finished", "error")
            and j.finished_at > 0
            and now - j.finished_at > JOB_LINGER
        ]
        for jid in dead:
            self._jobs.pop(jid, None)

    # ── Write API ─────────────────────────────────────────────

    async def add(self, job: CCJob) -> None:
        async with self._lock:
            self._evict()
            self._jobs[job.job_id] = job
            self._save()
        log.info("[CCJobStore] Added job %s uid=%d fname=%s",
                 job.job_id, job.uid, job.fname)

    async def update(self, job_id: str, **kw) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            for k, v in kw.items():
                if hasattr(job, k):
                    setattr(job, k, v)
            job.progress_at = time.time()
            self._save()

    async def finish(
        self, job_id: str,
        export_url: str = "",
        error_msg:  str = "",
    ) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if error_msg:
                job.status    = "error"
                job.error_msg = error_msg
            else:
                job.status     = "finished"
                job.export_url = export_url
            job.finished_at = time.time()
            job.progress_at = time.time()
            self._save()
        log.info("[CCJobStore] Job %s → %s", job_id, "error" if error_msg else "finished")

    async def mark_notified(self, job_id: str) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.notified = True
                self._save()

    _DELIVERY_STALE_AFTER = 300  # 5 min

    async def try_claim_delivery(self, job_id: str) -> bool:
        """
        Atomically claim delivery for a CC job to prevent double-upload when
        the webhook, the poller, and the offline recovery path race on the
        same job_id.

        Returns True only if:
          - job exists
          - job.uploaded is False (user does NOT already have the file)
          - job.notified is False (no active delivery path has claimed)
          - no other path claimed within the last 5 minutes

        On success, sets notified=True and delivering_since=now().
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            if job.uploaded:
                if not job.notified:
                    job.notified = True
                    self._save()
                return False
            if job.notified:
                if time.time() - (job.delivering_since or 0) < self._DELIVERY_STALE_AFTER:
                    return False
                log.warning("[CCJobStore] Stale delivery claim on %s — releasing", job_id)
            job.notified          = True
            job.delivering_since  = time.time()
            self._save()
            return True

    async def mark_uploaded(self, job_id: str) -> None:
        """
        Call this IMMEDIATELY after upload_file() returns successfully.
        Sets uploaded=True so any post-upload exception cannot trigger a
        duplicate delivery on the next poll cycle.
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.uploaded         = True
                job.notified         = True
                job.delivering_since = 0.0
                self._save()

    async def release_claim(self, job_id: str) -> None:
        """
        Release an in-flight claim when delivery fails BEFORE upload completes.
        The job becomes eligible for another delivery attempt via the poller.
        Only resets if uploaded is still False.
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if job and not job.uploaded:
                job.notified         = False
                job.delivering_since = 0.0
                self._save()

    # ── Read API ──────────────────────────────────────────────

    def get(self, job_id: str) -> Optional[CCJob]:
        return self._jobs.get(job_id)

    def jobs_for_user(self, uid: int) -> list[CCJob]:
        self._evict()
        return sorted(
            [j for j in self._jobs.values() if j.uid == uid],
            key=lambda j: j.created_at, reverse=True,
        )

    def active_jobs(self) -> list[CCJob]:
        return [j for j in self._jobs.values() if j.status == "processing"]

    def undelivered_jobs(self) -> list[CCJob]:
        """
        Jobs that finished successfully but have not been uploaded to Telegram yet.
        Excludes jobs with notified=True (already claimed for delivery).
        Excludes uploaded jobs (user already has the file).
        """
        return [
            j for j in self._jobs.values()
            if j.status == "finished" and j.export_url
            and not j.notified and not j.uploaded
        ]

    def all_jobs(self) -> list[CCJob]:
        self._evict()
        return list(self._jobs.values())


# Singleton shared across all plugins
cc_job_store = CCJobStore()
