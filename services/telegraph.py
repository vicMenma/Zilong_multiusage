"""
services/telegraph.py
Post MediaInfo to Telegra.ph.
Token is persisted to /tmp/zilong_telegraph.token so Colab restarts
don't create a new orphaned account every time.
"""
from __future__ import annotations

import aiohttp
import re

_TOKEN_FILE = "/tmp/zilong_telegraph.token"
_BASE       = "https://api.telegra.ph"
_token: str = ""


async def _get_token() -> str:
    global _token
    if _token:
        return _token
    # Try reading from disk
    try:
        with open(_TOKEN_FILE) as f:
            _token = f.read().strip()
        if _token:
            return _token
    except FileNotFoundError:
        pass
    # Create a new account
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
    # Strip absolute paths from output
    clean = re.sub(r'(Complete name\s*:\s*)/[^\n]*/', r'\1', text)
    clean = re.sub(r'/(?:tmp|content|home)/[^\s]*/([^\s/\n]+)', r'\1', clean)
    if len(clean) > 60_000:
        clean = clean[:60_000] + "\n\n...(truncated)"

    nodes = [
        {"tag": "p",   "children": [{"tag": "em", "children": [filename]}]},
        {"tag": "pre", "children": [clean]},
    ]
    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{_BASE}/createPage", json={
            "access_token":  token,
            "title":         title,
            "author_name":   "Zilong Bot",
            "content":       nodes,
            "return_content": False,
        }) as r:
            data = await r.json()

    if data.get("ok"):
        return "https://telegra.ph/" + data["result"]["path"]
    raise RuntimeError(f"Telegraph createPage failed: {data.get('error','unknown')}")
