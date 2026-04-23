"""
core/session.py
In-memory stores for users, settings, and per-user file sessions.
No MongoDB. No external dependencies.
Thread-safe via asyncio.Lock per store.

SessionStore  — file-processing sessions
UserStore     — user registry
SettingsStore — per-user upload preferences (persisted to data/settings.json)

FIX BUG-11 (prior): SessionStore.get() resets s.created on each access so the
  30-minute TTL is measured from last access, not from session creation.

FIX C-01 (audit v3): SessionStore.get() no longer calls _evict() outside the
  lock. Previously, multiple concurrent handlers calling get() simultaneously
  could corrupt the dict mid-iteration (RuntimeError: dictionary changed size
  during iteration). Now _evict() is only called inside locked async methods,
  and get() uses a simple age check per-key instead of a full sweep.

FIX M-04 (audit v3): is_downloaded() now also checks os.path.isfile() so a
  stale local_path pointing to a deleted file returns False.
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
        # FIX M-04: also verify the file still exists on disk
        return bool(self.local_path and os.path.isfile(self.local_path))


class SessionStore:
    """Keyed store of FileSession objects with TTL eviction.

    FIX C-01: get() is now safe to call from multiple concurrent handlers.
    _evict() is only called inside async methods that hold self._lock.
    get() does a per-key age check without mutating the dict during iteration.
    """

    TTL = 1800  # 30 min — measured from last access

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
        """
        Thread-safe read: no dict mutation, no _evict() sweep.
        Just checks if the specific key exists and is still alive.
        Touches s.created to extend the TTL window.
        """
        s = self._data.get(key)
        if s is None:
            return None
        now = time.time()
        if now - s.created > self.TTL:
            # Expired — don't remove here (let _evict() in locked methods handle it)
            return None
        # Touch: extends the TTL window (BUG-11 fix preserved)
        s.created = now
        return s

    async def remove(self, key: str) -> None:
        async with self._lock:
            self._data.pop(key, None)

    def user_sessions(self, user_id: int) -> list[FileSession]:
        now = time.time()
        return [s for s in self._data.values()
                if s.user_id == user_id and now - s.created <= self.TTL]

    def waiting_session(self, user_id: int) -> Optional[FileSession]:
        """Return the first session for user_id that is waiting for input."""
        now = time.time()
        for s in self._data.values():
            if s.user_id == user_id and s.waiting and now - s.created <= self.TTL:
                return s
        return None

    def _evict(self):
        """Remove expired sessions. MUST be called inside self._lock."""
        now = time.time()
        dead = [k for k, s in self._data.items() if now - s.created > self.TTL]
        for k in dead:
            self._data.pop(k, None)

    async def periodic_cleanup(self):
        """Run _evict() under lock. Call from a periodic background task."""
        async with self._lock:
            self._evict()


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
    # Custom name for downloads:
    #   "off" — keep original filename
    #   "mid" — ask user for a name before each download
    #   "on"  — silently apply custom_name to every download
    "custom_name_mode": "off",
    "custom_name":      "",       # used only when custom_name_mode == "on"
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
