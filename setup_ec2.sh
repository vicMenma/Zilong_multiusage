#!/bin/bash
# setup_ec2.sh — one-shot EC2 bootstrap for Zilong Bot
# Tested on Ubuntu 22.04 LTS and Ubuntu 24.04 LTS (arm64 / x86_64).
# Run as the ubuntu (or ec2-user) account — sudo is used internally.
set -e

# ── 0. Colour helpers ──────────────────────────────────────────
RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GRN}[+]${NC} $*"; }
warn()  { echo -e "${YLW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*"; exit 1; }

# ── 1. System packages ─────────────────────────────────────────
info "Installing system packages…"
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
    ffmpeg \
    aria2 \
    mediainfo \
    p7zip-full \
    git \
    curl \
    screen \
    openssh-client

# unrar — prefer the non-free version (full RAR5 support).
# Falls back to unrar-free if multiverse is not available.
if sudo apt-get install -y --no-install-recommends unrar 2>/dev/null; then
    info "Installed non-free unrar (full RAR5 support)."
else
    warn "non-free unrar unavailable — installing unrar-free (limited RAR5 support)."
    warn "To get full RAR5: sudo add-apt-repository multiverse && sudo apt-get install unrar"
    sudo apt-get install -y --no-install-recommends unrar-free
fi

# ── 2. Clone / update repo ─────────────────────────────────────
# ⚠️  UPDATE THIS URL to your actual GitHub repo before running.
REPO_URL="https://github.com/YOUR_USERNAME/YOUR_REPO.git"
BOTDIR="$HOME/zilong_bot"

if [ -d "$BOTDIR/.git" ]; then
    info "Repo already present — pulling latest…"
    git -C "$BOTDIR" pull --ff-only
else
    info "Cloning repo → $BOTDIR"
    git clone "$REPO_URL" "$BOTDIR"
fi
cd "$BOTDIR"

# ── 3. Python virtual environment ─────────────────────────────
info "Setting up Python venv…"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

# ── 4. Persistent directories ──────────────────────────────────
# data/       — Pyrogram session (must survive reboots)
# downloads/  — active download workspace (separate from /tmp)
DOWNLOAD_DIR="$BOTDIR/downloads"
mkdir -p "$BOTDIR/data" "$DOWNLOAD_DIR"
info "Session dir : $BOTDIR/data"
info "Download dir: $DOWNLOAD_DIR"

# ── 5. .env file ───────────────────────────────────────────────
if [ ! -f "$BOTDIR/.env" ]; then
    cp "$BOTDIR/env.example" "$BOTDIR/.env"
    # Patch DOWNLOAD_DIR to the persistent path (not /tmp)
    sed -i "s|DOWNLOAD_DIR=.*|DOWNLOAD_DIR=$DOWNLOAD_DIR|" "$BOTDIR/.env"
    warn "Created .env from env.example — fill in your credentials now:"
    warn "    nano $BOTDIR/.env"
else
    info ".env already exists — skipping copy."
fi

# ── 6. aria2c secret ───────────────────────────────────────────
# Read from .env if already set; otherwise generate one.
ARIA2_SECRET=$(grep -E '^ARIA2_SECRET=' "$BOTDIR/.env" | cut -d= -f2 | tr -d ' ')
if [ -z "$ARIA2_SECRET" ]; then
    ARIA2_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(24))")
    # Write it into .env
    sed -i "s|^ARIA2_SECRET=.*|ARIA2_SECRET=$ARIA2_SECRET|" "$BOTDIR/.env"
    info "Generated aria2c RPC secret and saved to .env"
fi

# ── 7. systemd — aria2c ────────────────────────────────────────
info "Creating aria2c systemd service…"
sudo tee /etc/systemd/system/zilong-aria2.service > /dev/null <<EOF
[Unit]
Description=aria2c RPC daemon for Zilong Bot
After=network.target

[Service]
# RPC is bound to localhost only — never expose port 6800 in your
# EC2 Security Group (it does NOT need to be reachable from outside).
ExecStart=/usr/bin/aria2c \
  --enable-rpc \
  --rpc-listen-all=false \
  --rpc-secret=${ARIA2_SECRET} \
  --max-connection-per-server=16 \
  --split=16 \
  --min-split-size=1M \
  --seed-time=0 \
  --dir=${DOWNLOAD_DIR}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# ── 8. systemd — bot ───────────────────────────────────────────
info "Creating zilongbot systemd service…"
sudo tee /etc/systemd/system/zilongbot.service > /dev/null <<EOF
[Unit]
Description=Zilong Telegram Bot
After=network.target zilong-aria2.service
Requires=zilong-aria2.service

[Service]
WorkingDirectory=${BOTDIR}
EnvironmentFile=${BOTDIR}/.env
ExecStart=${BOTDIR}/venv/bin/python main.py
Restart=on-failure
RestartSec=5
# Logs go to both journald and the local file
StandardOutput=append:${BOTDIR}/zilong.log
StandardError=append:${BOTDIR}/zilong.log

[Install]
WantedBy=multi-user.target
EOF

# ── 9. Enable and start ────────────────────────────────────────
sudo systemctl daemon-reload
sudo systemctl enable zilong-aria2 zilongbot
sudo systemctl start zilong-aria2
# Bot is NOT started yet — user must fill .env first.

# ── 10. Firewall reminder ──────────────────────────────────────
PUBLIC_IP=$(curl -sf http://checkip.amazonaws.com || echo "<your-ec2-ip>")

echo ""
echo -e "${GRN}✅ Setup complete!${NC}"
echo ""
echo "══════════════════════════════════════════════════"
echo "  Next steps"
echo "══════════════════════════════════════════════════"
echo ""
echo "  1. Fill in your credentials:"
echo "     nano $BOTDIR/.env"
echo ""
echo "  Required fields:"
echo "     API_ID, API_HASH   → my.telegram.org"
echo "     BOT_TOKEN          → @BotFather"
echo "     OWNER_ID           → your Telegram user ID"
echo ""
echo "  CloudConvert webhook (optional, faster job delivery):"
echo "     WEBHOOK_BASE_URL=http://${PUBLIC_IP}:8765"
echo "     → Open port 8765 inbound TCP in your EC2 Security Group"
echo "     → Port 8080 (keep-alive) is optional"
echo "     → Port 6800 (aria2 RPC) must stay CLOSED — localhost only"
echo ""
echo "  2. Start the bot:"
echo "     sudo systemctl start zilongbot"
echo ""
echo "  3. Watch logs:"
echo "     sudo journalctl -u zilongbot -f"
echo "     tail -f $BOTDIR/zilong.log"
echo ""
echo "  Handy deploy alias (add to ~/.bash_aliases):"
echo "  alias deploy='cd $BOTDIR && git pull && source venv/bin/activate && pip install -r requirements.txt -q && sudo systemctl restart zilongbot && sudo journalctl -u zilongbot -f'"
echo ""
