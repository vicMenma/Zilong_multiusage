"""
core/config.py
Single source of truth for all configuration.
Validates at import time — bot won't start with missing credentials.

FIX (BUG 9): load_dotenv(override=True) so the values written by
colab_launcher.py into the .env file always win over any stale env vars
that were set earlier in the Colab runtime session. Without override=True,
a stale NGROK_TOKEN exported into os.environ before clone persists and
makes it look like the token is missing even though .env has the right value.
"""
import os
import sys
from dataclasses import dataclass, field
from dotenv import load_dotenv

# FIX: override=True — .env always wins over pre-existing env vars
load_dotenv(override=True)


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
    api_id:    int = field(default_factory=lambda: int(_require("API_ID")))
    api_hash:  str = field(default_factory=lambda: _require("API_HASH"))
    bot_token: str = field(default_factory=lambda: _require("BOT_TOKEN"))
    owner_id:  int = field(default_factory=lambda: int(_require("OWNER_ID")))

    # ── Optional ──────────────────────────────────────────────
    download_dir: str = field(default_factory=lambda:
        os.environ.get("DOWNLOAD_DIR", "/tmp/zilong_dl"))

    log_channel: int = field(default_factory=lambda:
        _int_env("LOG_CHANNEL", 0))

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

    # Google Drive service account JSON path
    gdrive_sa_json: str = field(default_factory=lambda:
        os.environ.get("GDRIVE_SA_JSON", "service_account.json"))

    # File size cap (bytes)
    file_limit_mb: int = field(default_factory=lambda: _int_env("FILE_LIMIT_MB", 2048))

    # CloudConvert webhook integration (optional)
    ngrok_token: str = field(default_factory=lambda:
        os.environ.get("NGROK_TOKEN", ""))
    cc_webhook_secret: str = field(default_factory=lambda:
        os.environ.get("CC_WEBHOOK_SECRET", ""))

    # CloudConvert API key (for /hardsub and /ccstatus)
    cc_api_key: str = field(default_factory=lambda:
        os.environ.get("CC_API_KEY", ""))

    @property
    def file_limit_b(self) -> int:
        return self.file_limit_mb * 1024 * 1024

    @property
    def admins(self) -> set:
        return {self.owner_id} | set(self.extra_admins)

    def __post_init__(self):
        os.makedirs(self.download_dir, exist_ok=True)


cfg = Config()
