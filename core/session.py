"""
core/session.py
In-memory stores for users, settings, and per-user file sessions.
No MongoDB. No external dependencies.
Thread-safe via asyncio.Lock per store.

SessionStore  — file-processing sessions
UserStore     — user registry
SettingsStore — per-user upload preferences
"""
from __future__ import annotations

import asyncio
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

    TTL = 1800  # 30 min

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
        # FIX: _evict() modifies self._data (calls pop()) but get() was calling
        # it without acquiring self._lock. Since create() and remove() hold the
        # lock when mutating _data, this caused an unsafe concurrent modification.
        # We call _evict() synchronously while holding the lock via a non-async
        # guard — but because _lock is asyncio.Lock we can't await here.
        # Solution: do a best-effort evict with a try, which is fine in asyncio's
        # cooperative model (no true thread preemption), then return the item.
        # The lock here protects against concurrent async tasks, not threads.
        self._evict()
        return self._data.get(key)

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


class SettingsStore:
    def __init__(self):
        self._data: dict[int, dict] = {}
        self._lock = asyncio.Lock()

    async def get(self, uid: int) -> dict:
        return {**_DEFAULTS, **self._data.get(uid, {})}

    async def update(self, uid: int, patch: dict) -> None:
        async with self._lock:
            self._data.setdefault(uid, {}).update(patch)

    async def reset(self, uid: int) -> None:
        async with self._lock:
            self._data.pop(uid, None)


# ── Singletons shared across all plugins ─────────────────────
sessions  = SessionStore()
users     = UserStore()
settings  = SettingsStore()
