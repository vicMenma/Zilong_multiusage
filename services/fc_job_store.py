"""
services/fc_job_store.py
Persistent JSON store for FreeConvert jobs — mirrors cc_job_store.py.

Stores hardsub / convert / compress jobs submitted to FreeConvert so the
webhook handler can look them up when FC calls back.

Data file: data/fc_jobs.json  (created automatically)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

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
    api_key:     str   = ""             # key used for this job (for re-fetching)
    created_at:  float = field(default_factory=time.time)
    error:       str   = ""


# ─────────────────────────────────────────────────────────────
# Store
# ─────────────────────────────────────────────────────────────

class FCJobStore:
    """
    Thread-safe, asyncio-aware persistent store for FCJob objects.
    All public methods are coroutines and must be awaited.
    """

    def __init__(self, path: str = _STORE_PATH) -> None:
        self._path  = path
        self._jobs: dict[str, FCJob] = {}
        self._lock  = asyncio.Lock()
        self._dirty = False

    # ── Load / save ───────────────────────────────────────────

    async def load(self) -> None:
        """Load jobs from disk.  Call once at startup."""
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

    async def _save(self) -> None:
        """Persist to disk (called internally, lock must be held)."""
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        try:
            tmp = self._path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(
                    {k: asdict(v) for k, v in self._jobs.items()},
                    fh, indent=2,
                )
            os.replace(tmp, self._path)
            self._dirty = False
        except Exception as exc:
            log.error("[FC-Store] Save error: %s", exc)

    # ── Eviction ──────────────────────────────────────────────

    def _evict_expired(self) -> None:
        """Remove jobs older than _JOB_TTL (called with lock held)."""
        cutoff = time.time() - _JOB_TTL
        expired = [k for k, v in self._jobs.items() if v.created_at < cutoff]
        for k in expired:
            del self._jobs[k]
        if expired:
            log.info("[FC-Store] Evicted %d expired job(s)", len(expired))

    # ── Public API ────────────────────────────────────────────

    async def add(self, job: FCJob) -> None:
        """Register a new job."""
        async with self._lock:
            self._jobs[job.job_id] = job
            await self._save()
        log.debug("[FC-Store] Added job %s  type=%s  uid=%d",
                  job.job_id, job.job_type, job.uid)

    async def get(self, job_id: str) -> Optional[FCJob]:
        """Return the FCJob for job_id, or None if not found."""
        async with self._lock:
            return self._jobs.get(job_id)

    async def update(self, job_id: str, **kwargs) -> Optional[FCJob]:
        """
        Update fields on an existing job.  Returns the updated FCJob or None.

        Example:
            await fc_job_store.update(job_id, status="completed")
        """
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            for k, v in kwargs.items():
                if hasattr(job, k):
                    setattr(job, k, v)
                else:
                    log.warning("[FC-Store] Unknown field '%s' in update for %s", k, job_id)
            await self._save()
            return job

    async def remove(self, job_id: str) -> None:
        """Remove a job by ID."""
        async with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                await self._save()

    async def list_by_uid(self, uid: int) -> list[FCJob]:
        """Return all jobs belonging to a user, newest first."""
        async with self._lock:
            return sorted(
                [j for j in self._jobs.values() if j.uid == uid],
                key=lambda j: j.created_at,
                reverse=True,
            )

    async def list_processing(self) -> list[FCJob]:
        """Return all jobs still in 'processing' state."""
        async with self._lock:
            return [j for j in self._jobs.values() if j.status == "processing"]

    async def count(self) -> int:
        async with self._lock:
            return len(self._jobs)


# ── Module-level singleton ────────────────────────────────────

fc_job_store = FCJobStore()
