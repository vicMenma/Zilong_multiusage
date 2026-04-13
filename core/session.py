"""
core/session.py
In-memory stores for users, settings, and per-user file sessions.
No MongoDB. No external dependencies.
Thread-safe via asyncio.Lock per store.

SessionStore  — file-processing sessions
UserStore     — user registry
SettingsStore — per-user upload preferences (persisted to data/settings.json)

FIX BUG-11: SessionStore.get() now resets s.created on each access so the
  30-minute TTL is measured from last access, not from session creation.
  Previously a 45-min merge job created at t=0 would be evicted at t=1800
  (30 min) while still actively in use, causing spurious "Session expired" errors
  and orphaned temp directories.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from pyrogram import Client  # type: ignore

# Populated by main.py before any plugin runs
_client: Optional[Client] = None


def get_client() -> Client:
    if _client is None:
        raise RuntimeError("Client not initialised — import after main.py sets it")
    return _client


# ─────────────────────────────────────────────────────────────
# File-processing session
# ─────────────────────────────────────────────────────────────

@dataclass
class FileSession:
    """
    Tracks a single file sent by a user through the bot.
    Locked so concurrent callbacks on the same key are serialised.
    """
    key:      str
    user_id:  int
    file_id:  str
    fname:    str
    fsize:    int
    ext:      str
    tmp_dir:  str
    created:  float = field(default_factory=time.time)

    # Set once the file is downloaded
    local_path: Optional[str] = None

    # Current operation waiting for text/file reply
    waiting: Optional[str] = None

    # Misc payload (merge queue, custom caption, etc.)
    payload: dict = field(default_factory=dict)

    # Per-session mutex: prevents concurrent ffmpeg ops on same file
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    def is_downloaded(self) -> bool:
        import os
        return bool(self.local_path and os.path.exists(self.local_path))


class SessionStore:
    """Keyed store of FileSession objects with TTL eviction."""

    TTL = 1800  # 30 min — measured from last access (see get() fix below)

    def __init__(self):
        self._data: dict[str, FileSession] = {}
        self._lock = asyncio.Lock()

    async def create(self, user_id: int, file_id: str, fname: str,
                     fsize: int, ext: str, tmp_dir: str) -> FileSession:
        key = f"{user_id}_{uuid.uuid4().hex[:8]}"
        s   = FileSession(key=key, user_id=user_id, file_id=file_id,
                          fname=fname, fsize=fsize, ext=ext, tmp_dir=tmp_dir)
        async with self._lock:
            self._evict()
            self._data[key] = s
        return s

    def get(self, key: str) -> Optional[FileSession]:
        # FIX BUG-11: reset s.created on every access so the 30-min TTL is
        # measured from *last access*, not from session *creation*.
        # Previously a long-running operation (45-min merge) created at t=0
        # was evicted at t=1800 while still in use, killing the session.
        self._evict()
        s = self._data.get(key)
        if s is not None:
            s.created = time.time()   # ← touch: extends the TTL window
        return s

    async def remove(self, key: str) -> None:
        async with self._lock:
            self._data.pop(key, None)

    def user_sessions(self, user_id: int) -> list[FileSession]:
        return [s for s in self._data.values() if s.user_id == user_id]

    def waiting_session(self, user_id: int) -> Optional[FileSession]:
        """Return the first session for user_id that is waiting for input."""
        for s in self._data.values():
            if s.user_id == user_id and s.waiting:
                return s
        return None

    def _evict(self):
        now = time.time()
        dead = [k for k, s in self._data.items() if now - s.created > self.TTL]
        for k in dead:
            self._data.pop(k, None)


# ─────────────────────────────────────────────────────────────
# User store
# ─────────────────────────────────────────────────────────────

@dataclass
class User:
    uid:     int
    name:    str    = ""
    joined:  float  = field(default_factory=time.time)
    banned:  bool   = False


class UserStore:
    def __init__(self):
        self._data: dict[int, User] = {}
        self._lock = asyncio.Lock()

    async def register(self, uid: int, name: str = "") -> None:
        async with self._lock:
            if uid not in self._data:
                self._data[uid] = User(uid=uid, name=name)
            elif name:
                self._data[uid].name = name

    def get(self, uid: int) -> Optional[User]:
        return self._data.get(uid)

    async def ban(self, uid: int) -> None:
        async with self._lock:
            u = self._data.setdefault(uid, User(uid=uid))
            u.banned = True

    async def unban(self, uid: int) -> None:
        async with self._lock:
            if uid in self._data:
                self._data[uid].banned = False

    def is_banned(self, uid: int) -> bool:
        u = self._data.get(uid)
        return u.banned if u else False

    def all_users(self) -> list[User]:
        return list(self._data.values())

    def count(self) -> int:
        return len(self._data)


# ─────────────────────────────────────────────────────────────
# Settings store
# ─────────────────────────────────────────────────────────────

_DEFAULTS: dict[str, Any] = {
    "upload_mode":      "auto",   # "auto" | "document"
    "prefix":           "",       # prepended to every cleaned filename
    "suffix":           "",       # appended before extension
    "thumb_id":         None,     # Telegram file_id of saved thumbnail
    "auto_forward":     False,    # copy to all channels automatically after upload
    "forward_channels": [],       # list of {"id": int, "name": str}
    "progress_style":   "B",      # "B" (cards) | "C" (minimal)
}


_SETTINGS_DIR  = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)
_SETTINGS_PATH = os.path.join(_SETTINGS_DIR, "settings.json")


class SettingsStore:
    def __init__(self):
        self._data: dict[int, dict] = {}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        try:
            with open(_SETTINGS_PATH, encoding="utf-8") as f:
                raw = json.load(f)
            # JSON keys are always strings; convert back to int
            self._data = {int(k): v for k, v in raw.items()}
        except FileNotFoundError:
            pass
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("[Settings] Load error: %s", e)

    def _save(self) -> None:
        try:
            os.makedirs(_SETTINGS_DIR, exist_ok=True)
            with open(_SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump({str(k): v for k, v in self._data.items()}, f,
                          indent=2, ensure_ascii=False)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("[Settings] Save error: %s", e)

    async def get(self, uid: int) -> dict:
        return {**_DEFAULTS, **self._data.get(uid, {})}

    async def update(self, uid: int, patch: dict) -> None:
        async with self._lock:
            self._data.setdefault(uid, {}).update(patch)
            self._save()

    async def reset(self, uid: int) -> None:
        async with self._lock:
            self._data.pop(uid, None)
            self._save()


# ── Singletons shared across all plugins ─────────────────────
sessions  = SessionStore()
users     = UserStore()
settings  = SettingsStore()
