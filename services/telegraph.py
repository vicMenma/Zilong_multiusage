"""
services/telegraph.py
Post MediaInfo to Telegra.ph.

FIX: Token is now stored in data/telegraph.token inside the repo directory
instead of /tmp/zilong_telegraph.token. /tmp is wiped on every EC2 reboot
which caused a new orphaned Telegraph account to be created on each restart.
The data/ directory is already used for bot_name.txt so it's always present.
"""
from __future__ import annotations

import pathlib as _pathlib

import aiohttp
import re

# FIX: use repo-relative path so the token survives reboots on EC2/VPS
_TOKEN_FILE = str(
    _pathlib.Path(__file__).parent.parent / "data" / "telegraph.token"
)
_pathlib.Path(_TOKEN_FILE).parent.mkdir(parents=True, exist_ok=True)

_BASE  = "https://api.telegra.ph"
_token: str = ""


async def _get_token() -> str:
    global _token
    if _token:
        return _token
    try:
        with open(_TOKEN_FILE) as f:
            _token = f.read().strip()
        if _token:
            return _token
    except FileNotFoundError:
        pass
    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{_BASE}/createAccount", json={
            "short_name":  "ZilongBot",
            "author_name": "Zilong MediaInfo",
        }) as r:
            data = await r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegraph createAccount failed: {data}")
    _token = data["result"]["access_token"]
    try:
        with open(_TOKEN_FILE, "w") as f:
            f.write(_token)
    except Exception:
        pass
    return _token


async def post_mediainfo(filename: str, text: str) -> str:
    token = await _get_token()
    title = f"MediaInfo — {filename[:55]}"
    clean = re.sub(r'(Complete name\s*:\s*)/[^\n]*/', r'\1', text)
    clean = re.sub(r'/(?:tmp|content|home)/[^\s]*/([^\s/\n]+)', r'\1', clean)
    if len(clean) > 60_000:
        clean = clean[:60_000] + "\n\n...(truncated)"

    nodes = [
        {"tag": "p", "children": [{"tag": "em", "children": [filename]}]},
    ]

    _SECTION_RE = re.compile(r'^[A-Z][a-zA-Z\s#0-9]+$')

    for line in clean.splitlines():
        stripped = line.rstrip()

        if not stripped:
            nodes.append({"tag": "p", "children": [{"tag": "br", "children": []}]})
            continue

        if _SECTION_RE.match(stripped) and len(stripped) < 40 and ":" not in stripped:
            nodes.append({"tag": "p", "children": [
                {"tag": "strong", "children": [stripped]}
            ]})
            continue

        if ":" in stripped:
            key, _, val = stripped.partition(":")
            key_s = key.rstrip()
            val_s = val.strip()
            children: list = [key_s + " : "]
            if val_s:
                children.append({"tag": "code", "children": [val_s]})
            nodes.append({"tag": "p", "children": children})
        else:
            nodes.append({"tag": "p", "children": [
                {"tag": "code", "children": [stripped]}
            ]})

    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{_BASE}/createPage", json={
            "access_token":   token,
            "title":          title,
            "author_name":    "Zilong Bot",
            "content":        nodes,
            "return_content": False,
        }) as r:
            data = await r.json()

    if data.get("ok"):
        return "https://telegra.ph/" + data["result"]["path"]
    raise RuntimeError(f"Telegraph createPage failed: {data.get('error','unknown')}")
