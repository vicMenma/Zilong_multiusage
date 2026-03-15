"""
core/config.py
Single source of truth for all configuration.
Validates at import time — bot won't start with missing credentials.
"""
import os
import sys
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"❌ Missing required env var: {name}", file=sys.stderr)
        sys.exit(1)
    return val


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (ValueError, TypeError):
        return default


@dataclass(frozen=True)
class Config:
    # ── Required ──────────────────────────────────────────────
    api_id:    int    = field(default_factory=lambda: int(_require("API_ID")))
    api_hash:  str    = field(default_factory=lambda: _require("API_HASH"))
    bot_token: str    = field(default_factory=lambda: _require("BOT_TOKEN"))
    owner_id:  int    = field(default_factory=lambda: int(_require("OWNER_ID")))

    # ── Optional ──────────────────────────────────────────────
    download_dir: str = field(default_factory=lambda:
        os.environ.get("DOWNLOAD_DIR", "/tmp/zilong_dl"))

    log_channel: int = field(default_factory=lambda:
        _int_env("LOG_CHANNEL", 0))

    # extra admin IDs (space-separated)
    extra_admins: tuple = field(default_factory=lambda: tuple(
        int(x) for x in os.environ.get("ADMINS", "").split()
        if x.strip().lstrip("-").isdigit()
    ))

    # Aria2
    aria2_host:   str = field(default_factory=lambda:
        os.environ.get("ARIA2_HOST", "http://localhost"))
    aria2_port:   int = field(default_factory=lambda: _int_env("ARIA2_PORT", 6800))
    aria2_secret: str = field(default_factory=lambda:
        os.environ.get("ARIA2_SECRET", ""))

    # File size cap (bytes) — single flat limit, no tiers
    file_limit_mb: int = field(default_factory=lambda: _int_env("FILE_LIMIT_MB", 2048))

    @property
    def file_limit_b(self) -> int:
        return self.file_limit_mb * 1024 * 1024

    @property
    def admins(self) -> set:
        return {self.owner_id} | set(self.extra_admins)

    def __post_init__(self):
        os.makedirs(self.download_dir, exist_ok=True)


cfg = Config()
