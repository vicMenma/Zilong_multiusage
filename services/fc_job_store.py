"""
services/fc_job_store.py
Persistent JSON store for FreeConvert jobs — mirrors cc_job_store.py.

Stores hardsub / convert / compress jobs submitted to FreeConvert so the
webhook handler can look them up when FC calls back.

Data file: data/fc_jobs.json  (created automatically)

FIX NEW-JOB-POLLER: add() now fires an optional on_job_added callback.
  This lets plugins/fc_webhook.py register _ensure_fc_poller() as the
  callback so the recurring FC poller is (re)started automatically
  whenever a new job is submitted — without creating a circular import.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Optional

log = logging.getLogger(__name__)

_STORE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "fc_jobs.json")
)
_JOB_TTL = 48 * 3600   # 48 h — FC export URLs expire after 24 h


# ─────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────

@dataclass
class FCJob:
    job_id:      str
    uid:         int
    fname:       str
    output_name: str
    status:      str   = "processing"   # processing | completed | failed
    job_type:    str   = "hardsub"      # hardsub | convert | compress
    sub_fname:   str   = ""
    api_key:     str   = ""
    created_at:  float = field(default_factory=time.time)
    error:       str   = ""
    # Set True once the file is in Telegram — prevents duplicate delivery.
    uploaded:    bool  = False


# ─────────────────────────────────────────────────────────────
# Store
# ─────────────────────────────────────────────────────────────

class FCJobStore:
    def __init__(self, path: str = _STORE_PATH) -> None:
        self._path   = path
        self._jobs: dict[str, FCJob] = {}
        self._lock   = asyncio.Lock()
        self._dirty  = False
        # FIX NEW-JOB-POLLER: callback fired after every add() so the FC
        # poller can be (re)started from fc_webhook.py without a circular import.
        self._on_job_added: Optional[Callable[[], None]] = None

    def set_on_job_added(self, cb: Callable[[], None]) -> None:
        """Register a zero-argument callback invoked each time a job is added."""
        self._on_job_added = cb

    # ── Load / save ───────────────────────────────────────────

    async def load(self) -> None:
        async with self._lock:
            try:
                with open(self._path, encoding="utf-8") as fh:
                    raw = json.load(fh)
                self._jobs = {k: FCJob(**v) for k, v in raw.items()}
                log.info("[FC-Store] Loaded %d job(s) from %s", len(self._jobs), self._path)
            except FileNotFoundError:
                self._jobs = {}
            except Exception as exc:
                log.warning("[FC-Store] Load error (%s) — starting empty", exc)
                self._jobs = {}
            self._evict_expired()
            # Reset any job that was atomically claimed (status="completed") but not
            # fully delivered before the process exited.
            for job in self._jobs.values():
                if job.status == "completed" and not job.uploaded:
                    job.status = "processing"
                    log.info("[FC-Store] Reset undelivered job %s → 'processing'", job.job_id)
                elif job.status == "completed" and job.uploaded:
                    log.info("[FC-Store] Job %s already uploaded — leaving", job.job_id)

    async def _save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        try:
            tmp = self._path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(
                    {k: asdict(v) for k, v in self._jobs.items()},
                    fh, indent=2,
                )
            os.replace(tmp, self._path)
        except Exception as exc:
            log.error("[FC-Store] Save error: %s", exc)

    # ── Eviction ──────────────────────────────────────────────

    def _evict_expired(self) -> None:
        cutoff  = time.time() - _JOB_TTL
        expired = [k for k, v in self._jobs.items() if v.created_at < cutoff]
        for k in expired:
            del self._jobs[k]
        if expired:
            log.info("[FC-Store] Evicted %d expired job(s)", len(expired))

    # ── Public API ────────────────────────────────────────────

    async def add(self, job: FCJob) -> None:
        async with self._lock:
            self._jobs[job.job_id] = job
            await self._save()
        log.debug("[FC-Store] Added job %s  type=%s  uid=%d",
                  job.job_id, job.job_type, job.uid)
        # FIX NEW-JOB-POLLER: notify the registered callback (if any)
        if self._on_job_added is not None:
            try:
                self._on_job_added()
            except Exception as _cb_exc:
                log.debug("[FC-Store] on_job_added callback error: %s", _cb_exc)

    async def get(self, job_id: str) -> Optional[FCJob]:
        async with self._lock:
            return self._jobs.get(job_id)

    async def update(self, job_id: str, **kwargs) -> Optional[FCJob]:
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            for k, v in kwargs.items():
                if hasattr(job, k):
                    setattr(job, k, v)
                else:
                    log.warning("[FC-Store] Unknown field '%s' for %s", k, job_id)
            await self._save()
            return job

    async def remove(self, job_id: str) -> None:
        async with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                await self._save()

    async def try_claim_delivery(self, job_id: str) -> bool:
        """
        Atomically claim this job for delivery (processing → completed).
        Returns True only once; all subsequent callers return False.
        Also refuses if uploaded=True — once in Telegram, never re-deliver.
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.uploaded:
                return False
            if job.status != "processing":
                return False
            job.status = "completed"
            await self._save()
            return True

    async def mark_uploaded(self, job_id: str) -> None:
        """Call IMMEDIATELY after upload_file() succeeds to prevent duplicates."""
        async with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.uploaded = True
                await self._save()

    async def list_by_uid(self, uid: int) -> list[FCJob]:
        async with self._lock:
            return sorted(
                [j for j in self._jobs.values() if j.uid == uid],
                key=lambda j: j.created_at,
                reverse=True,
            )

    async def list_processing(self) -> list[FCJob]:
        async with self._lock:
            return [j for j in self._jobs.values() if j.status == "processing"]

    async def count(self) -> int:
        async with self._lock:
            return len(self._jobs)


# ── Module-level singleton ────────────────────────────────────

fc_job_store = FCJobStore()
