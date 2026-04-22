"""
core/config.py
Single source of truth for all configuration.
Validates at import time — bot won't start with missing credentials.

FIX (BUG 9): load_dotenv(override=True) so the values written by
colab_launcher.py into the .env file always win over any stale env vars
that were set earlier in the Colab runtime session. Without override=True,
a stale NGROK_TOKEN exported into os.environ before clone persists and
makes it look like the token is missing even though .env has the right value.

WEBHOOK_BASE_URL: public base URL for the CloudConvert webhook server.
  On Colab, colab_launcher.py opens a Serveo tunnel on port 8765 before
  starting the bot and writes the resulting URL here automatically.
  On a VPS/EC2, set it manually: e.g. http://YOUR_IP:8765
  Leave empty to fall back to the ccstatus poller (~5 s lag).
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

    # Public base URL for the CloudConvert webhook receiver (port 8765).
    # On Colab, colab_launcher.py opens a Serveo tunnel and writes this
    # automatically before the bot starts. On a VPS, set it manually.
    # Leave empty to use the ccstatus poller fallback instead.
    webhook_base_url: str = field(default_factory=lambda:
        os.environ.get("WEBHOOK_BASE_URL", "").strip().rstrip("/"))

    # CloudConvert API key (for /hardsub and /ccstatus)
    cc_api_key: str = field(default_factory=lambda:
        os.environ.get("CC_API_KEY", ""))

    # FreeConvert API key (for /convert, /compress, /fchardsub)
    fc_api_key: str = field(default_factory=lambda:
        os.environ.get("FC_API_KEY", ""))

    # Seedr proxy — HTTP or SOCKS5 URL routed only to add_torrent write calls.
    # Required on cloud IPs (Google Colab, Render, Railway, etc.) because Seedr
    # blocks add_torrent from those IP ranges while read-only calls still work.
    # Examples:
    #   http://user:pass@host:port
    #   socks5://user:pass@host:port   (needs: pip install httpx[socks])
    # Leave empty on a VPS or home server with a clean IP.
    seedr_proxy: str = field(default_factory=lambda:
        os.environ.get("SEEDR_PROXY", "").strip())

    @property
    def file_limit_b(self) -> int:
        return self.file_limit_mb * 1024 * 1024

    @property
    def admins(self) -> set:
        return {self.owner_id} | set(self.extra_admins)

    def __post_init__(self):
        os.makedirs(self.download_dir, exist_ok=True)


cfg = Config()

# ── Mutable runtime state (cfg is frozen, so runtime values go here) ──────────
# tunnel_url: set once the Cloudflare/ngrok tunnel comes up.
# Plugins read this via get_tunnel_url() to embed in per-job FC webhook URLs.
_state: dict = {"tunnel_url": ""}


def get_tunnel_url() -> str:
    """Return the current public tunnel base URL (e.g. https://abc.trycloudflare.com)."""
    return _state.get("tunnel_url", "")


def set_tunnel_url(url: str) -> None:
    """Store the tunnel base URL once it is known at startup."""
    _state["tunnel_url"] = url.rstrip("/") if url else ""
