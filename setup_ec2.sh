#!/bin/bash
# setup_ec2.sh — one-shot EC2 setup for Zilong Bot (Zilong_v2)
# Service name: zilongbot  (matches the running service on this instance)
set -e

echo "📦 System packages…"
sudo apt-get update -qq
sudo apt-get install -y python3 python3-pip python3-venv \
    ffmpeg aria2 mediainfo p7zip-full \
    unrar-free \
    git screen

# FIX (BUG 8): Ubuntu 22+ ships "unrar-free" not "unrar".
# If you need the non-free unrar (better RAR5 support), enable the
# multiverse repo first:
#   sudo add-apt-repository multiverse && sudo apt-get update
#   sudo apt-get install -y unrar

echo "📁 Cloning repo…"
# FIX (BUG 1 + BUG 8): repo is Zilong_v2, directory is Zilong_v2
git clone https://github.com/vicMenma/Zilong_v2 ~/Zilong_v2
cd ~/Zilong_v2

echo "🐍 Python venv…"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

cp .env.example .env
echo ""
echo "⚠️  Edit .env with your credentials:"
echo "    nano .env"
echo ""

BOTDIR="$(pwd)"

# ── aria2c systemd service ────────────────────────────────────
sudo tee /etc/systemd/system/zilong-aria2.service > /dev/null <<EOF
[Unit]
Description=aria2c RPC for Zilong
After=network.target

[Service]
ExecStart=/usr/bin/aria2c --enable-rpc --rpc-listen-all=true \
  --rpc-allow-origin-all --max-connection-per-server=16 \
  --split=16 --seed-time=0 --dir=/tmp/zilong_dl
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

# ── Bot systemd service ───────────────────────────────────────
# FIX (BUG 8): service name is "zilongbot" (matches what's running on this
# instance and the deploy alias already configured in ~/.bash_aliases)
sudo tee /etc/systemd/system/zilongbot.service > /dev/null <<EOF
[Unit]
Description=Zilong Telegram Bot (Zilong_v2)
After=network.target zilong-aria2.service

[Service]
WorkingDirectory=${BOTDIR}
EnvironmentFile=${BOTDIR}/.env
ExecStart=${BOTDIR}/venv/bin/python main.py
Restart=on-failure
RestartSec=5
StandardOutput=append:${BOTDIR}/zilong.log
StandardError=append:${BOTDIR}/zilong.log

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable zilong-aria2 zilongbot
sudo systemctl start zilong-aria2

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. nano .env           ← fill in credentials"
echo "  2. sudo systemctl start zilongbot"
echo "  3. sudo journalctl -u zilongbot -f  ← watch logs"
echo ""
echo "Useful aliases (add to ~/.bash_aliases):"
echo "  alias deploy='cd ~/Zilong_v2 && git reset --hard HEAD && git pull && venv/bin/pip install -r requirements.txt -q && sudo systemctl restart zilongbot && sudo journalctl -u zilongbot -f'"
