FROM python:3.11-slim

# ── System dependencies ────────────────────────────────────────────────────────
# ffmpeg        — video/audio processing (transcode, thumbnail, hardsub)
# aria2         — multi-connection HTTP + torrent/magnet downloads
# mediainfo     — media stream metadata (fallback when ffprobe returns nothing)
# p7zip-full    — 7z archive extraction (/archive command)
# unrar-free    — RAR archive extraction (/archive command)
#                 NOTE: unrar-free has limited RAR5 support. For full RAR5,
#                 switch to the non-free "unrar" package (requires multiverse).
# git           — used by some yt-dlp extractors and the deploy workflow
# curl          — healthcheck probe + misc HTTP fetches
# openssh-client— Serveo keep-alive tunnel (services/keep_alive.py)
# build-essential — needed to compile native extensions (TgCrypto, uvloop)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    aria2 \
    mediainfo \
    p7zip-full \
    unrar-free \
    git \
    curl \
    openssh-client \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ───────────────────────────────────────────────────────────
COPY . .

# Persistent directories
# /app/data      — Pyrogram session file (ZilongBot.session). Mount as a volume
#                  so auth survives container restarts/re-deploys.
# /app/downloads — active download workspace. Mount a volume here if you want
#                  downloads to survive container restarts (optional).
RUN mkdir -p /app/data /app/downloads

# ── Ports ─────────────────────────────────────────────────────────────────────
# 8080 — keep-alive health server (UptimeRobot / ELB health checks)
# 8765 — CloudConvert webhook receiver (open this in your EC2 Security Group
#         and set WEBHOOK_BASE_URL=http://<your-ip>:8765 in .env)
EXPOSE 8080 8765

# ── Runtime ────────────────────────────────────────────────────────────────────
CMD ["python", "main.py"]
